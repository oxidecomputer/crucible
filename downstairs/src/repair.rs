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
use serde::Serialize;

use super::*;
use crate::region::{extent_dir, extent_file_name, extent_path};

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
    api.register(get_extent).unwrap();
    api.register(get_db).unwrap();
    api.register(get_shm).unwrap();
    api.register(get_wal).unwrap();
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

/**
 * Stream the contents of the data file for the given extent ID
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/data",
    unpublished = false,
}]
async fn get_extent(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<Response<Body>, HttpError> {
    let eid = path.into_inner().eid;
    let extent_path = extent_path(rqctx.context().region_dir.clone(), eid);

    get_file(extent_path).await
}

/**
 * Stream the contents of the metadata .db file for the given extent ID
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/db",
    unpublished = false,
}]
async fn get_db(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<Response<Body>, HttpError> {
    let eid = path.into_inner().eid;
    let mut extent_path = extent_path(rqctx.context().region_dir.clone(), eid);
    extent_path.set_extension("db");

    get_file(extent_path).await
}

/**
 * Stream the contents of the metadata .db-shm file for the given extent ID
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/db-shm",
    unpublished = false,
}]
async fn get_shm(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<Response<Body>, HttpError> {
    let eid = path.into_inner().eid;
    let mut extent_path = extent_path(rqctx.context().region_dir.clone(), eid);
    extent_path.set_extension("db-shm");

    get_file(extent_path).await
}

/**
 * Stream the contents of the metadata .db-wal file for the given extent ID
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/db-wal",
    unpublished = false,
}]
async fn get_wal(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<Response<Body>, HttpError> {
    let eid = path.into_inner().eid;
    let mut extent_path = extent_path(rqctx.context().region_dir.clone(), eid);
    extent_path.set_extension("db-wal");

    get_file(extent_path).await
}

async fn get_file(path: PathBuf) -> Result<Response<Body>, HttpError> {
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

#[derive(Deserialize, Serialize, JsonSchema)]
struct ExtentFiles {
    files: Vec<String>,
}

/**
 * For a given extent, return a vec of strings representing the names of
 * the files that exist for that extent.
 */
#[endpoint {
    method = GET,
    path = "/extent/{eid}/files",
    unpublished = false,
}]
async fn get_files_for_extent(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<HttpResponseOk<ExtentFiles>, HttpError> {
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

async fn extent_file_list(
    extent_dir: PathBuf,
    eid: u32,
) -> Result<ExtentFiles, HttpError> {
    let mut full_name = extent_dir;

    let extent_name = extent_file_name(eid, None);
    full_name.push(extent_name.clone());
    let mut files = Vec::new();
    // The data file should always exist
    if !full_name.exists() {
        return Err(HttpError::for_bad_request(None, "EBADF".to_string()));
    }
    files.push(extent_name);
    // The db file should always exist.
    full_name.set_extension("db");
    if !full_name.exists() {
        return Err(HttpError::for_bad_request(None, "EBADF".to_string()));
    }
    files.push(extent_file_name(eid, Some("db")));

    // The db-shm file may exist.
    full_name.set_extension("db-shm");
    if full_name.exists() {
        println!("Exists: {:?}", full_name.file_name().unwrap());
        files.push(extent_file_name(eid, Some("db-shm")));
    }
    // The db-wal file may exist.
    full_name.set_extension("db-wal");
    if full_name.exists() {
        files.push(extent_file_name(eid, Some("db-wal")));
    }
    Ok(ExtentFiles { files })
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
        let extent_dir = extent_dir(&dir, 1);
        let mut ex_files = extent_file_list(extent_dir, 1).await.unwrap();
        ex_files.files.sort();
        let expected = vec!["001", "001.db", "001.db-shm", "001.db-wal"];
        println!("files: {:?}", ex_files.files);
        assert_eq!(ex_files.files, expected);

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
        rm_file.push(extent_file_name(1, None));
        rm_file.set_extension("db-wal");
        std::fs::remove_file(&rm_file).unwrap();
        rm_file.set_extension("db-shm");
        std::fs::remove_file(rm_file).unwrap();

        let mut ex_files = extent_file_list(extent_dir, 1).await.unwrap();
        ex_files.files.sort();
        let expected = vec!["001", "001.db"];
        println!("files: {:?}", ex_files.files);
        assert_eq!(ex_files.files, expected);

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
        rm_file.push(extent_file_name(1, None));
        rm_file.set_extension("db-wal");
        let _ = std::fs::remove_file(&rm_file);
        rm_file.set_extension("db-shm");
        let _ = std::fs::remove_file(rm_file);

        let mut ex_files = extent_file_list(extent_dir, 1).await.unwrap();
        ex_files.files.sort();
        let expected = vec!["001", "001.db"];
        println!("files: {:?}", ex_files.files);
        assert_eq!(ex_files.files, expected);

        Ok(())
    }
}
