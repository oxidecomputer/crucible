// Copyright 2022 Oxide Computer Company
use std::path::PathBuf;
use std::sync::Arc;

use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::HandlerTaskMode;
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

pub fn write_openapi<W: Write>(f: &mut W) -> Result<()> {
    let api = build_api();
    api.openapi("Downstairs Repair", "0.0.0").write(f)?;
    Ok(())
}

fn build_api() -> ApiDescription<Arc<FileServerContext>> {
    let mut api = ApiDescription::new();
    api.register(get_extent_file).unwrap();
    api.register(get_files_for_extent).unwrap();

    api
}

/// Returns Ok(listen address) if everything launched ok, Err otherwise
pub async fn repair_main(
    ds: &Arc<Mutex<Downstairs>>,
    addr: SocketAddr,
    log: &Logger,
) -> Result<dropshot::HttpServer<Arc<FileServerContext>>, String> {
    /*
     * We must specify a configuration with a bind address.
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
    };

    /*
     * Build a description of the API
     */
    let api = build_api();

    /*
     * Record the region directory where all the extents and metadata
     * files live.
     */
    let ds = ds.lock().await;
    let region_dir = ds.region.dir.clone();
    drop(ds);

    let context = FileServerContext { region_dir };

    info!(log, "Repair will listen on {}", addr);
    /*
     * Set up the server.
     */
    let server =
        HttpServerStarter::new(&config_dropshot, api, context.into(), log)
            .map_err(|error| format!("failed to create server: {}", error))?
            .start();

    Ok(server)
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
    #[serde(rename = "db_shm")]
    DatabaseSharedMemory,
    #[serde(rename = "db_wal")]
    DatabaseLog,
}

#[derive(Deserialize, JsonSchema)]
pub struct FileSpec {
    eid: u32,
    file_type: FileType,
}

#[endpoint {
    method = GET,
    path = "/newextent/{eid}/{file_type}",
}]
async fn get_extent_file(
    rqctx: RequestContext<Arc<FileServerContext>>,
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
    /*
     * Make sure our file is neither a link nor a directory.
     */
    let m = path.symlink_metadata().map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("Failed to get {:?} metadata: {:#}", path, e),
        )
    })?;

    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(
            None,
            "File is symlink".to_string(),
        ))
    } else if path.is_dir() {
        Err(HttpError::for_bad_request(
            None,
            "Expected a file, found a directory".to_string(),
        ))
    } else {
        let file = tokio::fs::File::open(&path).await.map_err(|e| {
            HttpError::for_bad_request(
                None,
                format!("file {:?}: {:#}", path, e),
            )
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
    rqctx: RequestContext<Arc<FileServerContext>>,
    path: Path<Eid>,
) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
    let eid = path.into_inner().eid;
    let extent_dir = extent_dir(rqctx.context().region_dir.clone(), eid);

    // Some sanity checking on the extent path
    let m = extent_dir.symlink_metadata().map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("Failed to get {:?} metadata: {:#}", extent_dir, e),
        )
    })?;
    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(
            None,
            format!("File {:?} is a symlink", extent_dir),
        ))
    } else if !extent_dir.is_dir() {
        Err(HttpError::for_bad_request(
            None,
            format!("Expected {:?} to be a directory", extent_dir),
        ))
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
            return Err(HttpError::for_bad_request(None, "EBADF".to_string()));
        }
    }

    Ok(files)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::region::{extent_dir, extent_file_name};
    use openapiv3::OpenAPI;
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

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    #[tokio::test]
    async fn extent_expected_files() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect.  This is a hack of sorts as we are hard coding
        // the expected names of files here in that test, rather than
        // determine them through some programmatic means.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

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
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

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
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ext_one = &mut region.extents[1];
        ext_one.close().await?;

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
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

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
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, 1);

        // Delete db
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(1, ExtentType::Data));
        std::fs::remove_file(&rm_file).unwrap();

        assert!(extent_file_list(extent_dir, 1).await.is_err());

        Ok(())
    }

    #[test]
    fn test_crucible_repair_openapi() {
        let mut raw = Vec::new();
        write_openapi(&mut raw).unwrap();
        let actual = String::from_utf8(raw).unwrap();

        // Make sure the result parses as a valid OpenAPI spec.
        let spec = serde_json::from_str::<OpenAPI>(&actual)
            .expect("output was not valid OpenAPI");

        // Check for lint errors.
        let errors = openapi_lint::validate(&spec);
        assert!(errors.is_empty(), "{}", errors.join("\n\n"));

        expectorate::assert_contents(
            "../openapi/downstairs-repair.json",
            &actual,
        );
    }
}
