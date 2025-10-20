// Copyright 2021 Oxide Computer Company
use std::sync::Arc;
use tokio::sync::Mutex;
use std::net::SocketAddr;

use anyhow::{bail, Context, Result};
use clap::Parser;

use crucible::*;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HandlerTaskMode;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use dropshot::TypedBody;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Parser)]
#[clap(about = "volume-side storage component")]
pub struct Opt {
    #[clap(short, long, default_value = "127.0.0.1:9999", action)]
    listen: SocketAddr,

    #[clap(short, long, default_value = "127.0.0.1:9000", action)]
    target: Vec<SocketAddr>,

    #[clap(short, long, action)]
    key: Option<String>,

    #[clap(short, long, default_value = "0", action)]
    gen: u64,

    // TLS options
    #[clap(long, action)]
    cert_pem: Option<String>,
    #[clap(long, action)]
    key_pem: Option<String>,
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    // Start upstairs control http server
    #[clap(long, action)]
    control: Option<SocketAddr>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

/*
 * Crucible needs a runtime as it will create several async tasks to handle
 * adding new IOs, communication with the three downstairs instances, and
 * completing IOs.
 */
#[tokio::main]
async fn main() -> Result<()> {

    // For simplicity, we'll configure an "info"-level logger that writes to
    // stderr assuming that it's a terminal.
    let config_logging =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info };
    let log = config_logging
        .to_logger("example-basic")
        .context("Failed to create logger")?;

    let opt = opts()?;
    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: false,
        flush_timeout: None,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
        ..Default::default()
    };

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let (guest, io) = Guest::new(None);

    let _join_handle = up_main(crucible_opts, opt.gen, None, io, None)?;
    println!("Crucible runtime is spawned");

    // Rest server

    let mut cpf = crucible::CruciblePseudoFile::from(guest)?;
    cpf.activate().await?;

    // Build a description of the API.
    let mut api = ApiDescription::new();
    api.register(crucible_api_get_bytes).unwrap();
    api.register(crucible_api_put_bytes).unwrap();
    api.register(crucible_api_put_flush).unwrap();

    // The functions that implement our API endpoints will share this context.
    let api_context = CrucibleContext::new(cpf);

    // Set up the server.
    //
    // We use the default configuration here, which uses 127.0.0.1 since it's
    // always available and won't expose this server outside the host.  It also
    // uses port 0, which allows the operating system to pick any available
    // port.


    let server = ServerBuilder::new(api, api_context, log)
        .config(dropshot::ConfigDropshot {
                bind_address: opt.listen,
                default_request_body_max_bytes: 8 * 1024,
                default_handler_task_mode: HandlerTaskMode::Detached,
                log_headers: vec![]})
        .start()
        .context("failed to create server")?;

    // Wait for the server to stop.  Note that there's not any code to shut down
    // this server, so we should never get past this point.
    let _ = server.await;
    Ok(())
}

/// Application-specific example context (state shared by handler functions)
struct CrucibleContext {
    cpf: Arc<Mutex<CruciblePseudoFile<crucible::Guest>>>,
}

impl CrucibleContext {
    /// Return a new CrucibleContext.
    pub fn new(cpf: CruciblePseudoFile<crucible::Guest>) -> CrucibleContext {
        CrucibleContext { cpf: Arc::new(Mutex::new(cpf)) }
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct ReadRequest {
    offset: u64,
    len: u64,
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct WriteRequest {
    offset: u64,
    bytes: String,
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct ReadResponse {
    bytes: String,
}

/// Read bytes
#[endpoint {
    method = GET,
    path = "/bytes",
}]
async fn crucible_api_get_bytes(
    rqctx: RequestContext<CrucibleContext>,
    req: TypedBody<ReadRequest>,
) -> Result<HttpResponseOk<ReadResponse>, HttpError> {
    let api_context = rqctx.context();
    let req = req.into_inner();

    let mut cpf = api_context.cpf.lock().await;

    match cpf.seek(SeekFrom::Start(req.offset)) {
        Ok(_) => {},
        Err(e) => return Err(HttpError::for_internal_error(
            format!("seek failed: {e}")
        ))
    };
    let mut buffer = vec![0u8; req.len.try_into().unwrap()];
    match cpf.read(&mut buffer) {
        Ok(bytes_read) => Ok(HttpResponseOk(ReadResponse {
            bytes: String::from_utf8_lossy(&buffer[..bytes_read]).to_string()
        })),
        Err(e) => Err(HttpError::for_internal_error(
            format!("read failed: {e}")
        ))
    }
}

/// Write bytes
#[endpoint {
    method = PUT,
    path = "/bytes",
}]
async fn crucible_api_put_bytes(
    rqctx: RequestContext<CrucibleContext>,
    req: TypedBody<WriteRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();
    let req = req.into_inner();

    let mut cpf = api_context.cpf.lock().await;

    match cpf.seek(SeekFrom::Start(req.offset)) {
        Ok(_) => {},
        Err(e) => return Err(HttpError::for_internal_error(
            format!("seek failed: {e}")
        ))
    };
    match cpf.write(&mut req.bytes.as_bytes()) {
        Ok(_bytes_written) => Ok(HttpResponseUpdatedNoContent()),
        Err(e) => return Err(HttpError::for_internal_error(
            format!("write failed: {e}")
        ))
    }
}
/// flush
#[endpoint {
    method = PUT,
    path = "/flush",
}]
async fn crucible_api_put_flush(
    rqctx: RequestContext<CrucibleContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut cpf = api_context.cpf.lock().await;

    match cpf.flush() {
        Ok(_) => Ok(HttpResponseUpdatedNoContent()),
        Err(e) => return Err(HttpError::for_internal_error(
            format!("flush failed: {e}")
        ))
    }
}
