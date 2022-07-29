// Copyright 2022 Oxide Computer Company
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpServerStarter;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::{HttpResponseOk, HttpResponseUpdatedNoContent};
use schemars::JsonSchema;
use serde::Deserialize;
//use serde::Serialize;
use std::sync::Arc;

use super::*;

pub(crate) fn build_api() -> ApiDescription<DownstairsControl> {
    let mut api = ApiDescription::new();
    api.register(dsc_get_state).unwrap();
    api.register(dsc_get_pid).unwrap();
    api.register(dsc_stop).unwrap();
    api.register(dsc_stop_all).unwrap();
    api.register(dsc_stop_rand).unwrap();
    api.register(dsc_start).unwrap();
    api.register(dsc_start_all).unwrap();
    api.register(dsc_disable_restart).unwrap();
    api.register(dsc_disable_restart_all).unwrap();
    api.register(dsc_enable_restart).unwrap();
    api.register(dsc_enable_restart_all).unwrap();
    api.register(dsc_shutdown).unwrap();

    api
}

/**
 * Start up a dropshot server. This offers a way to control which downstairs
 * are running and determine their state.
 */
pub async fn begin(dsci: Arc<DscInfo>, addr: SocketAddr) -> Result<(), String> {
    // Setup dropshot
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        tls: None,
    };
    println!("start access at:{:?}", addr);

    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };
    let log = config_logging
        .to_logger("example-basic")
        .map_err(|error| format!("failed to create logger: {}", error))?;

    // Build a description of the API.
    let api = build_api();

    // The functions that implement our API endpoints will share this
    // context.
    let api_context = DownstairsControl::new(&dsci);

    /*
     * Set up the server.
     */
    let server =
        HttpServerStarter::new(&config_dropshot, api, api_context, &log)
            .map_err(|error| format!("failed to create server: {}", error))?
            .start();

    println!("Control access at:{:?}", addr);
    /*
     * Wait for the server to stop.  Note that there's not any code to shut
     * down this server, so we should never get past this point.
     */
    server.await
}

/**
 * The state shared by handler functions
 */
pub struct DownstairsControl {
    /**
     * The dsc struct that holds info on what the dsc is doing.
     */
    dsci: Arc<DscInfo>,
}

impl DownstairsControl {
    /**
     * Return a new DownstairsControl.
     */
    pub fn new(dsci: &Arc<DscInfo>) -> DownstairsControl {
        DownstairsControl { dsci: dsci.clone() }
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cid {
    cid: usize,
}

fn cid_bad(dsci: &DscInfo, cid: usize) -> bool {
    let rs = dsci.rs.lock().unwrap();
    rs.ds_state.len() <= cid
}

/**
 * Fetch the reported pid for the requested client_id
 */
#[endpoint {
    method = GET,
    path = "/pid/cid/{cid}",
}]
async fn dsc_get_pid(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<Option<u32>>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            Some(String::from("BadInput")),
            format!("Invalid client id: {}", cid),
        ));
    }
    let ds_pid = api_context.dsci.get_ds_pid(cid).map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to state for downstairs {}: {:#}", 0, e),
        )
    })?;

    Ok(HttpResponseOk(ds_pid))
}

/**
 * Fetch the current state for the requested client_id
 */
#[endpoint {
    method = GET,
    path = "/state/cid/{cid}",
}]
async fn dsc_get_state(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<DownstairsState>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let ds_state = api_context.dsci.get_ds_state(cid).map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to state for downstairs {}: {:#}", 0, e),
        )
    })?;

    Ok(HttpResponseOk(ds_state))
}

/**
 * Stop the downstairs at the given client_id
 */
#[endpoint {
    method = GET,
    path = "/stop/cid/{cid}",
}]
async fn dsc_stop(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::Stop(cid));
    Ok(HttpResponseUpdatedNoContent())
}
/**
 * Stop all downstairs
 */
#[endpoint {
    method = POST,
    path = "/stop/all",
}]
async fn dsc_stop_all(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::StopAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Stop a random downstairs
 */
#[endpoint {
    method = GET,
    path = "/stop/rand",
}]
async fn dsc_stop_rand(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::StopRand);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Start the downstairs at the given client_id
 */
#[endpoint {
    method = GET,
    path = "/start/cid/{cid}",
}]
async fn dsc_start(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::Start(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Start all the downstairs
 */
#[endpoint {
    method = GET,
    path = "/start/all",
}]
async fn dsc_start_all(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::StartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Disable automatic restart on the given client_id
 */
#[endpoint {
    method = GET,
    path = "/disablerestart/cid/{cid}",
}]
async fn dsc_disable_restart(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::DisableRestart(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Disable automatic restart on all downstairs
 */
#[endpoint {
    method = GET,
    path = "/disablerestart/all",
}]
async fn dsc_disable_restart_all(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::DisableRestartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Enable automatic restart on the given client_id
 */
#[endpoint {
    method = GET,
    path = "/enablerestart/cid/{cid}",
}]
async fn dsc_enable_restart(
    rqctx: Arc<RequestContext<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid) {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::EnableRestart(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Enable automatic restart on all downstairs
 */
#[endpoint {
    method = GET,
    path = "/enablerestart/all",
}]
async fn dsc_enable_restart_all(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::EnableRestartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Stop all downstairs, then stop ourselves.
 */
#[endpoint {
    method = POST,
    path = "/shutdown",
}]
async fn dsc_shutdown(
    rqctx: Arc<RequestContext<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().unwrap();
    dsc_work.add_cmd(DscCmd::Shutdown);
    Ok(HttpResponseUpdatedNoContent())
}

#[cfg(test)]
mod test {
    use openapiv3::OpenAPI;

    use super::build_api;

    #[test]
    fn test_dsc_openapi() {
        let api = build_api();
        let mut raw = Vec::new();
        api.openapi("DownStairs Control", "0.0.0")
            .write(&mut raw)
            .unwrap();
        let actual = String::from_utf8(raw).unwrap();

        // Make sure the result parses as a valid OpenAPI spec.
        let spec = serde_json::from_str::<OpenAPI>(&actual)
            .expect("output was not valid OpenAPI");

        // Check for lint errors.
        let errors = openapi_lint::validate(&spec);
        assert!(errors.is_empty(), "{}", errors.join("\n\n"));

        expectorate::assert_contents("../openapi/dsc-control.json", &actual);
    }
}
