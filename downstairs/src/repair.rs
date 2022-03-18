// Copyright 2022 Oxide Computer Company
use std::path::PathBuf;
use std::sync::Arc;

use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpServerStarter;
use dropshot::RequestContext;
use dropshot::{endpoint, Path};
use http::{Response, StatusCode};
use hyper::Body;
use schemars::JsonSchema;
use serde::Deserialize;

use super::*;
use crate::region::{extent_dir, extent_path};
/**
 * Our context is simply the root of the directory we want to serve.
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
     * For simplicity, we'll configure an "info"-level logger that writes to
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
     * Specify the directory we want to serve.  This is the region directory
     * where all the extents and metadata files live.
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

// ZZZ
// We need some query from the remote side where they ask us for all the
// files we need for an extent and we return a list of files or URLs or
// something that they can turn around a use to get the files?
// Maybe just a string we put on the end of the URL?
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Eid {
    eid: u32,
}

/**
 * Return the contents of the data file for the given extent ID
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
    println!("Extent {} has: {:?}", eid, extent_path);

    get_file(extent_path).await
}

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
    /*
     * We explicitly prohibit consumers from following symlinks to prevent
     * showing data outside of the intended directory.
     */
    let m = path
        .symlink_metadata()
        .map_err(|_| HttpError::for_bad_request(None, "ENOENT".to_string()))?;

    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(None, "EMLINK".to_string()))
    } else if path.is_dir() {
        Err(HttpError::for_bad_request(None, "EBADF".to_string()))
    } else {
        println!("get file {:?}", path);
        let file = tokio::fs::File::open(&path).await.map_err(|_| {
            HttpError::for_bad_request(None, "EBADF".to_string())
        })?;
        let file_stream = hyper_staticfile::FileBytesStream::new(file);

        // ZZZ some other type octet something
        /* Derive the MIME type from the file name */
        let content_type = mime_guess::from_path(&path)
            .first()
            .map_or_else(|| "text/plain".to_string(), |m| m.to_string());

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, content_type)
            .body(file_stream.into_body())?)
    }
}

#[endpoint {
    method = GET,
    path = "/extent/{eid}/files",
    unpublished = false,
}]
async fn get_files_for_extent(
    rqctx: Arc<RequestContext<FileServerContext>>,
    path: Path<Eid>,
) -> Result<Response<Body>, HttpError> {
    let eid = path.into_inner().eid;
    let extent_dir = extent_dir(rqctx.context().region_dir.clone(), eid);
    println!("extent {} lives in: {:?}", eid, extent_dir);

    /*
     * We explicitly prohibit consumers from following symlinks to prevent
     * showing data outside of the intended directory.
     */
    let m = extent_dir
        .symlink_metadata()
        .map_err(|_| HttpError::for_bad_request(None, "ENOENT".to_string()))?;

    if m.file_type().is_symlink() {
        Err(HttpError::for_bad_request(None, "EMLINK".to_string()))
    } else if !extent_dir.is_dir() {
        Err(HttpError::for_bad_request(None, "EBADF".to_string()))
    } else {
        println!("get directory listing for {:?}", extent_dir);

        let body = dir_body(extent_dir).await.map_err(|_| {
            HttpError::for_bad_request(None, "EBADF".to_string())
        })?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/html")
            .body(body.into())?)
    }
}

/**
 * Generate a simple HTML listing of files within the directory.
 * See the note below regarding the handling of trailing slashes.
 */
async fn dir_body(dir_path: PathBuf) -> Result<String, std::io::Error> {
    let dir_link = dir_path.to_string_lossy();
    let mut dir = tokio::fs::read_dir(&dir_path).await?;

    let mut body = String::new();

    body.push_str(
        format!(
            "<html>
            <head><title>{}/</title></head>
            <body>
            <h1>{}/</h1>
            ",
            dir_link, dir_link
        )
        .as_str(),
    );
    body.push_str("<ul>\n");
    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        /*
         * Note that Dropshot handles paths with and without trailing slashes
         * as identical. This is important with respect to relative paths as
         * the destination of a relative path is different depending on
         * whether or not a trailing slash is present in the
         * browser's location bar. For example, a relative url of
         * "bar" would go from the location "localhost:123/foo" to
         * "localhost:123/bar" and from the location "localhost:123/
         * foo/" to "localhost:123/foo/bar". More robust
         * handling would require distinct handling of the trailing slash
         * and a redirect in the case of its absence when navigating to a
         * directory.
         */
        body.push_str(
            format!(
                r#"<li><a href="{}{}">{}</a></li>"#,
                name,
                if entry.file_type().await?.is_dir() {
                    "/"
                } else {
                    ""
                },
                name
            )
            .as_str(),
        );
        body.push('\n');
    }
    body.push_str(
        "</ul>
        </body>
        </html>\n",
    );

    Ok(body)
}
