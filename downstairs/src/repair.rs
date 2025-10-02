// Copyright 2022 Oxide Computer Company
use std::path::PathBuf;
use std::sync::Arc;

use crucible_downstairs_api::*;
use crucible_downstairs_types::FileType;
use dropshot::{
    Body, ConfigDropshot, HandlerTaskMode, HttpError, HttpResponseOk,
    HttpServerStarter, Path, RequestContext,
};
use hyper::{Response, StatusCode};

use super::*;
use crate::extent::{extent_dir, extent_file_name, extent_path, ExtentType};

/**
 * Our context is the root of the region we want to serve.
 */
pub struct FileServerContext {
    region_dir: PathBuf,
    read_only: bool,
    region_definition: RegionDefinition,
    downstairs: DownstairsHandle,
}

/// Returns Ok(listen address) if everything launched ok, Err otherwise
pub fn repair_main(
    ds: &Downstairs,
    addr: SocketAddr,
    log: &Logger,
) -> Result<(tokio::task::JoinHandle<Result<()>>, SocketAddr), String> {
    /*
     * We must specify a configuration with a bind address.
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        default_request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
    };

    /*
     * Build a description of the API
     */
    let api = crucible_downstairs_repair_api_mod::api_description::<
        CrucibleDownstairsRepairImpl,
    >()
    .unwrap();

    /*
     * Record the region directory where all the extents and metadata
     * files live.
     */
    let region_dir = ds.region.dir.clone();
    let read_only = ds.flags.read_only;
    let region_definition = ds.region.def();
    let handle = ds.handle();

    info!(log, "Repair listens on {} for path:{:?}", addr, region_dir);
    let context = FileServerContext {
        region_dir,
        read_only,
        region_definition,
        downstairs: handle,
    };

    /*
     * Set up the server.
     */
    let server =
        HttpServerStarter::new(&config_dropshot, api, context.into(), log)
            .map_err(|error| format!("failed to create server: {}", error))?
            .start();
    let local_addr = server.local_addr();

    let h = tokio::spawn(async move {
        let r = server.await;
        r.map_err(|e| anyhow::anyhow!("{e}"))
    });

    Ok((h, local_addr))
}

/// Implementation of the Crucible Downstairs Repair API.
pub struct CrucibleDownstairsRepairImpl;

impl CrucibleDownstairsRepairApi for CrucibleDownstairsRepairImpl {
    type Context = Arc<FileServerContext>;

    async fn get_extent_file(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        let fs = path.into_inner();
        let eid = ExtentId(fs.eid);

        let mut extent_path =
            extent_path(rqctx.context().region_dir.clone(), eid);
        match fs.file_type {
            FileType::Database => {
                extent_path.set_extension("db");
            }
            FileType::DatabaseSharedMemory => {
                extent_path.set_extension("db-shm");
            }
            FileType::DatabaseLog => {
                extent_path.set_extension("db-wal");
            }
            // No file extension
            FileType::Data => (),
        };

        get_a_file(extent_path).await
    }

    /// Return true if the provided extent is closed or the region is read only.
    async fn extent_repair_ready(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentPath>,
    ) -> Result<HttpResponseOk<bool>, HttpError> {
        let eid: usize = path.into_inner().eid as usize;
        let downstairs = &rqctx.context().downstairs;

        // If the region is read only, the extent is always ready.
        if rqctx.context().read_only {
            return Ok(HttpResponseOk(true));
        }

        downstairs
            .is_extent_closed(ExtentId(eid as u32))
            .await
            .map(HttpResponseOk)
            .map_err(|e| HttpError::for_internal_error(e.to_string()))
    }

    /// Get the list of files related to an extent.
    ///
    /// For a given extent, return a vec of strings representing the names of
    /// the files that exist for that extent.
    async fn get_files_for_extent(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentPath>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        let eid = ExtentId(path.into_inner().eid);
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
            let files = extent_file_list(extent_dir, eid)?;
            Ok(HttpResponseOk(files))
        }
    }
    /// Return the RegionDefinition describing our region.
    async fn get_region_info(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<crucible_common::RegionDefinition>, HttpError>
    {
        let region_definition = rqctx.context().region_definition;

        Ok(HttpResponseOk(region_definition))
    }

    /// Return the region-mode describing our region.
    async fn get_region_mode(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<bool>, HttpError> {
        let read_only = rqctx.context().read_only;

        Ok(HttpResponseOk(read_only))
    }

    /// Get work queue information.
    async fn get_work(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<bool>, HttpError> {
        let downstairs = &rqctx.context().downstairs;
        downstairs
            .show_work()
            .map(|_| HttpResponseOk(true))
            .map_err(|e| HttpError::for_internal_error(e.to_string()))
    }
}

async fn get_a_file(
    path: PathBuf,
) -> Result<Response<dropshot::Body>, HttpError> {
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

        let file_access = hyper_staticfile::vfs::TokioFileAccess::new(file);
        let file_stream =
            hyper_staticfile::util::FileBytesStream::new(file_access);
        let body = Body::wrap(hyper_staticfile::Body::Full(file_stream));
        let content_type = "application/octet-stream".to_string();

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, content_type)
            .body(body)?)
    }
}

/**
 * Return the list of extent files we have in our region directory
 * that correspond to the given extent.  Return an error if any
 * of the required files are missing.
 */
fn extent_file_list(
    extent_dir: PathBuf,
    eid: ExtentId,
) -> Result<Vec<String>, HttpError> {
    let mut files = Vec::new();
    let possible_files = [
        (ExtentType::Data, true),
        (ExtentType::Db, false),
        (ExtentType::DbShm, false),
        (ExtentType::DbWal, false),
    ];

    for (file, required) in possible_files.into_iter() {
        let mut fullname = extent_dir.clone();
        let file_name = extent_file_name(eid, file);
        fullname.push(file_name.clone());
        if fullname.exists() {
            files.push(file_name);
        } else if required {
            return Err(HttpError::for_bad_request(None, "EBADF".to_string()));
        }
    }

    Ok(files)
}

#[cfg(test)]
mod test {
    use super::*;
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

    #[test]
    fn extent_expected_files() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect.  This is a hack of sorts as we are hard coding
        // the expected names of files here in that test, rather than
        // determine them through some programmatic means.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::default())?;

        // Determine the directory and name for expected extent files.
        let eid = ExtentId(1);
        let ed = extent_dir(&dir, eid);
        let mut ex_files = extent_file_list(ed, eid).unwrap();
        ex_files.sort();
        let expected = vec!["001"];
        println!("files: {:?}", ex_files);
        assert_eq!(ex_files, expected);

        Ok(())
    }

    #[test]
    fn extent_expected_files_with_close() -> Result<()> {
        // Verify that the list of files returned for an extent matches
        // what we expect. In this case we expect the extent data file and
        // nothing else. We close the extent here first, and on illumos that
        // behaves a little different than elsewhere.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::default())?;

        let eid = ExtentId(1);
        region.close_extent(eid).unwrap();

        // Determine the directory and name for expected extent files.
        let extent_dir = extent_dir(&dir, eid);

        let mut ex_files = extent_file_list(extent_dir, eid).unwrap();
        ex_files.sort();
        let expected = vec!["001"];
        println!("files: {:?}", ex_files);
        assert_eq!(ex_files, expected);

        Ok(())
    }

    #[test]
    fn extent_expected_files_fail() -> Result<()> {
        // Verify that we get an error if the expected extent file
        // is missing.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::default())?;

        // Determine the directory and name for expected extent files.
        let eid = ExtentId(1);
        let extent_dir = extent_dir(&dir, eid);

        // Delete the extent file
        let mut rm_file = extent_dir.clone();
        rm_file.push(extent_file_name(eid, ExtentType::Data));
        std::fs::remove_file(&rm_file).unwrap();

        assert!(extent_file_list(extent_dir, eid).is_err());

        Ok(())
    }
}
