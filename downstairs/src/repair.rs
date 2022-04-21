// Copyright 2022 Oxide Computer Company
use std::path::PathBuf;
use std::sync::Arc;

use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServerStarter;
use dropshot::RequestContext;
use dropshot::{endpoint, Path};
use http::{Response, StatusCode};
use hyper::Body;
use schemars::JsonSchema;
use serde::Deserialize;

use super::*;
use crate::region::{extent_dir, extent_file_name, extent_path, ExtentType};

/**
 * Our context is the root of the region we want to serve.
 */
pub struct FileServerContext {
    region_dir: PathBuf,
}

/**
 * Build the API.  If requested, dump it to stdout.
 * This allows us to use the resulting output to build the client side.
 */
pub fn build_api(
    show: bool,
) -> Result<ApiDescription<FileServerContext>, String> {
    let mut api = ApiDescription::new();
    api.register(get_extent_file).unwrap();
    api.register(get_files_for_extent).unwrap();

    if show {
        api.openapi("downstairs-repair", "1")
            .write(&mut std::io::stdout())
            .map_err(|e| e.to_string())?;
    }
    Ok(api)
}

pub async fn repair_main(
    ds: &Arc<Mutex<Downstairs>>,
    addr: SocketAddr,
) -> Result<(), String> {
    /*
     * We must specify a configuration with a bind address.
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        tls: None,
    };

    /*
     * For simplicity, configure an "info"-level logger that writes to
     * stderr assuming that it's a terminal.
     */
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };
    let log = config_logging
        .to_logger("example-basic")
        .map_err(|error| format!("failed to create logger: {}", error))?;

    /*
     * Build a description of the API
     */
    let api = build_api(false)?;

    /*
     * Record the region directory where all the extents and metadata
     * files live.
     */
    let ds = ds.lock().await;
    let region_dir = ds.region.dir.clone();
    drop(ds);

    let context = FileServerContext { region_dir };

    println!("Repair listens on {}", addr);
    /*
     * Set up the server.
     */
    let server = HttpServerStarter::new(&config_dropshot, api, context, &log)
        .map_err(|error| format!("failed to create server: {}", error))?
        .start();

    /*
     * Wait for the server to stop.  Note that there's not any code to shut
     * down this server, so we should never get past this point.
     */
    server.await
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Eid {
    eid: u32,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FileType {
    #[serde(rename = "data")]
    Data,
    #[serde(rename = "db")]
    Database,
    #[serde(rename = "db-shm")]
    DatabaseSharedMemory,
    #[serde(rename = "db-wal")]
    DatabaseLog,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FileSpec {
    eid: u32,
    file_type: FileType,
}

#[endpoint {
    method = GET,
    path = "/newextent/{eid}/{fileType}",
}]
async fn get_extent_file(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<FileSpec>,
) -> Result<Response<Body>, HttpError> {
    let fs = path.into_inner();
    let eid = fs.eid;

    let mut extent_path = extent_path(rqctx.context().region_dir.clone(), eid);
    match fs.file_type {
        FileType::Database => {
            extent_path.set_extension("db");
        }
        FileType::DatabaseSharedMemory => {
            extent_path.set_extension("db-wal");
        }
        FileType::DatabaseLog => {
            extent_path.set_extension("db-shm");
        }
        FileType::Data => (),
    };

    get_a_file(extent_path).await
}

async fn get_a_file(path: PathBuf) -> Result<Response<Body>, HttpError> {
    println!("Request for file {:?}", path);
    /*
     * Make sure our file is neither a link nor a directory.
     */
    let m = path
        .symlink_metadata()
        .map_err(|_| HttpError::for_bad_request(None, "ENOENT".to_string()))?;

    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(None, "EMLINK".to_string()))
    } else if path.is_dir() {
        Err(HttpError::for_bad_request(None, "EBADF".to_string()))
    } else {
        let file = tokio::fs::File::open(&path).await.map_err(|_| {
            HttpError::for_bad_request(None, "EBADF".to_string())
        })?;

        let file_stream = hyper_staticfile::FileBytesStream::new(file);
        let content_type = "application/octet-stream".to_string();

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, content_type)
            .body(file_stream.into_body())?)
    }
}

/**
 * Get the list of files related to an extent.
 *
 * For a given extent, return a vec of strings representing the names of
 * the files that exist for that extent.
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/files",
}]
async fn get_files_for_extent(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
    let eid = path.into_inner().eid;
    let extent_dir = extent_dir(rqctx.context().region_dir.clone(), eid);

    // Some sanity checking on the extent path
    let m = extent_dir
        .symlink_metadata()
        .map_err(|_| HttpError::for_bad_request(None, "ENOENT".to_string()))?;

    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(None, "EMLINK".to_string()))
    } else if !extent_dir.is_dir() {
        Err(HttpError::for_bad_request(None, "EBADF".to_string()))
    } else {
        let files = extent_file_list(extent_dir, eid).await?;
        Ok(HttpResponseOk(files))
    }
}

/**
 * Return the list of extent files we have in our region directory
 * that correspond to the given extent.  Return an error if any
 * of the required files are missing.
 */
async fn extent_file_list(
    extent_dir: PathBuf,
    eid: u32,
) -> Result<Vec<String>, HttpError> {
    let mut files = Vec::new();
    let possible_files = vec![
        (extent_file_name(eid, ExtentType::Data), true),
        (extent_file_name(eid, ExtentType::Db), true),
        (extent_file_name(eid, ExtentType::DbShm), false),
        (extent_file_name(eid, ExtentType::DbWal), false),
    ];

    for (file, required) in possible_files.into_iter() {
        let mut fullname = extent_dir.clone();
        fullname.push(file.clone());
        if fullname.exists() {
            files.push(file);
        } else if required {
            println!("Needed file {} is missing", file);
            return Err(HttpError::for_bad_request(None, "EBADF".to_string()));
        }
    }

    Ok(files)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::region::{extent_dir, extent_file_name};
    use tempfile::tempdir;

    fn new_region_options() -> crucible_common::RegionOptions {
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        let block_size = 512;
        region_options.set_block_size(block_size);
        region_options
            .set_extent_size(Block::new(10, block_size.trailing_zeros()));
        region_options
    }

    #[tokio::test]
    async fn extent_expected_files() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect.  This is a hack of sorts as we are hard coding
        // the expected names of files here in that test, rather than
        // determine them through some programmatic means.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Determine the directory and name for expected extent files.
        let ed = extent_dir(&dir, 1);
        let mut ex_files = extent_file_list(ed, 1).await.unwrap();
        ex_files.sort();
        let expected = vec!["001", "001.db", "001.db-shm", "001.db-wal"];
        println!("files: {:?}", ex_files);
        assert_eq!(ex_files, expected);

        Ok(())
    }

    #[tokio::test]
    async fn extent_expected_files_short() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect. In this case we expect the extent data file and
        // the .db file, but not the .db-shm or .db-wal database files.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, 1);

        // Delete db-wal and db-shm
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(1, ExtentType::Data));
        rm_file.set_extension("db-wal");
        std::fs::remove_file(&rm_file).unwrap();
        rm_file.set_extension("db-shm");
        std::fs::remove_file(rm_file).unwrap();

        let mut ex_files = extent_file_list(extent_dir, 1).await.unwrap();
        ex_files.sort();
        let expected = vec!["001", "001.db"];
        println!("files: {:?}", ex_files);
        assert_eq!(ex_files, expected);

        Ok(())
    }

    #[tokio::test]
    async fn extent_expected_files_short_with_close() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect. In this case we expect the extent data file and
        // the .db file, but not the .db-shm or .db-wal database files.
        // We close the extent here first, and on illumos that behaves
        // a little different than elsewhere.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        let ext_one = &mut region.extents[1];
        ext_one.close()?;

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, 1);

        // Delete db-wal and db-shm.  On illumos the close of the extent
        // may remove these for us, so we ignore errors on the removal.
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(1, ExtentType::Data));
        rm_file.set_extension("db-wal");
        let _ = std::fs::remove_file(&rm_file);
        rm_file.set_extension("db-shm");
        let _ = std::fs::remove_file(rm_file);

        let mut ex_files = extent_file_list(extent_dir, 1).await.unwrap();
        ex_files.sort();
        let expected = vec!["001", "001.db"];
        println!("files: {:?}", ex_files);
        assert_eq!(ex_files, expected);

        Ok(())
    }

    #[tokio::test]
    async fn extent_expected_files_fail() -> Result<()> {
        // Verify that we get an error if the expected extent.db file
        // is missing.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, 2);

        // Delete db
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(2, ExtentType::Data));
        rm_file.set_extension("db");
        std::fs::remove_file(&rm_file).unwrap();

        assert!(extent_file_list(extent_dir, 2).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn extent_expected_files_fail_two() -> Result<()> {
        // Verify that we get an error if the expected extent file
        // is missing.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, 1);

        // Delete db
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(1, ExtentType::Data));
        std::fs::remove_file(&rm_file).unwrap();

        assert!(extent_file_list(extent_dir, 1).await.is_err());

        Ok(())
    }
}
