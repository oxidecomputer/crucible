// Copyright 2022 Oxide Computer Company
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServerStarter;
use dropshot::RequestContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

use super::*;

/**
 * Publish some stats on a http port from the upstairs internal struct.
 */
pub async fn start_info(
    up: &Arc<Upstairs>,
    addr: SocketAddr,
) -> Result<(), String> {
    /*
     * Setup dropshot
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        // tls: None,
        ..Default::default()
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
     * Build a description of the API.
     */
    let mut api = ApiDescription::new();
    api.register(upstairs_fill_info).unwrap();

    /*
     * The functions that implement our API endpoints will share this
     * context.
     */
    let api_context = UpstairsInfo::new(up);

    /*
     * Set up the server.
     */
    let server =
        HttpServerStarter::new(&config_dropshot, api, api_context, &log)
            .map_err(|error| format!("failed to create server: {}", error))?
            .start();

    /*
     * Wait for the server to stop.  Note that there's not any code to shut
     * down this server, so we should never get past this point.
     */
    server.await
}

/**
 * The state shared by handler functions
 */
struct UpstairsInfo {
    /**
     * Upstairs structure that is used to gather all the info stats
     */
    up: Arc<Upstairs>,
}

impl UpstairsInfo {
    /**
     * Return a new UpstairsInfo.
     */
    pub fn new(up: &Arc<Upstairs>) -> UpstairsInfo {
        UpstairsInfo { up: up.clone() }
    }
}

/**
 * `UpstairsInfo` holds the information gathered from the upstairs to fill
 * a response to a GET request
 */
#[derive(Deserialize, Serialize, JsonSchema)]
struct UpstairsStats {
    state: UpState,
    up_jobs: usize,
    ds_jobs: usize,
}

/**
 * Fetch the current value for all the stats in the UpstairsStats struct
 */
#[endpoint {
    method = GET,
    path = "/info",
}]
async fn upstairs_fill_info(
    rqctx: Arc<RequestContext<UpstairsInfo>>,
) -> Result<HttpResponseOk<UpstairsStats>, HttpError> {
    let api_context = rqctx.context();

    let act = api_context.up.active.lock().unwrap().up_state;
    let up_jobs = api_context.up.guest.guest_work.lock().unwrap().active.len();
    let ds_jobs = api_context.up.downstairs.lock().unwrap().active.len();

    Ok(HttpResponseOk(UpstairsStats {
        state: act,
        up_jobs,
        ds_jobs,
    }))
}
