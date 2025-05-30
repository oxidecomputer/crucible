// Copyright 2022 Oxide Computer Company
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HandlerTaskMode;
use dropshot::HttpError;
use dropshot::HttpServerStarter;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::{HttpResponseOk, HttpResponseUpdatedNoContent};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

use super::*;

pub(crate) fn build_api() -> ApiDescription<Arc<DownstairsControl>> {
    let mut api = ApiDescription::new();
    api.register(dsc_all_running).unwrap();
    api.register(dsc_all_stopped).unwrap();
    api.register(dsc_get_ds_state).unwrap();
    api.register(dsc_get_pid).unwrap();
    api.register(dsc_get_port).unwrap();
    api.register(dsc_get_region_count).unwrap();
    api.register(dsc_get_region_info).unwrap();
    api.register(dsc_get_uuid).unwrap();
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
    api.register(dsc_enable_random_stop).unwrap();
    api.register(dsc_disable_random_stop).unwrap();
    api.register(dsc_enable_random_min).unwrap();
    api.register(dsc_enable_random_max).unwrap();

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
        default_request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
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
        HttpServerStarter::new(&config_dropshot, api, api_context.into(), &log)
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
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Min {
    min: u64,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Max {
    max: u64,
}

async fn cid_bad(dsci: &DscInfo, cid: usize) -> bool {
    let rs = dsci.rs.lock().await;
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
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<Option<u32>>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            Some(String::from("BadInput")),
            format!("Invalid client id: {}", cid),
        ));
    }
    let ds_pid = api_context.dsci.get_ds_pid(cid).await.map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to get PID for downstairs {}: {:#}", cid, e),
        )
    })?;

    Ok(HttpResponseOk(ds_pid))
}

/**
 * Fetch the port for the requested client_id
 */
#[endpoint {
    method = GET,
    path = "/port/cid/{cid}",
}]
async fn dsc_get_port(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<u32>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            Some(String::from("BadInput")),
            format!("Invalid client id: {}", cid),
        ));
    }
    let ds_port = api_context.dsci.get_ds_port(cid).await.map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to get port for downstairs {}: {:#}", cid, e),
        )
    })?;

    Ok(HttpResponseOk(ds_port))
}

/**
 * Return true if all downstairs are running
 */
#[endpoint {
    method = GET,
    path = "/allrunning",
}]
async fn dsc_all_running(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseOk<bool>, HttpError> {
    let api_context = rqctx.context();

    let all_state = api_context.dsci.all_running().await;
    Ok(HttpResponseOk(all_state))
}

/**
 * Return true if all downstairs are stopped
 */
#[endpoint {
    method = GET,
    path = "/allstopped",
}]
async fn dsc_all_stopped(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseOk<bool>, HttpError> {
    let api_context = rqctx.context();

    let all_state = api_context.dsci.all_stopped().await;
    Ok(HttpResponseOk(all_state))
}

/**
 * Fetch the current state for the requested client_id
 */
#[endpoint {
    method = GET,
    path = "/state/cid/{cid}",
}]
async fn dsc_get_ds_state(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<DownstairsState>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let ds_state = api_context.dsci.get_ds_state(cid).await.map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to get state for downstairs {}: {:#}", cid, e),
        )
    })?;

    Ok(HttpResponseOk(ds_state))
}

/**
 * Fetch the UUID for the requested client_id
 */
#[endpoint {
    method = GET,
    path = "/uuid/cid/{cid}",
}]
async fn dsc_get_uuid(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseOk<Uuid>, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            Some(String::from("BadInput")),
            format!("Invalid client id: {}", cid),
        ));
    }
    let uuid = api_context.dsci.get_ds_uuid(cid).await.map_err(|e| {
        HttpError::for_bad_request(
            None,
            format!("failed to get UUID for downstairs {}: {:#}", cid, e),
        )
    })?;

    Ok(HttpResponseOk(uuid))
}

/**
 * Stop the downstairs at the given client_id
 */
#[endpoint {
    method = POST,
    path = "/stop/cid/{cid}",
}]
async fn dsc_stop(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().await;
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
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::StopAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Stop a random downstairs
 */
#[endpoint {
    method = POST,
    path = "/stop/rand",
}]
async fn dsc_stop_rand(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::StopRand);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Start the downstairs at the given client_id
 */
#[endpoint {
    method = POST,
    path = "/start/cid/{cid}",
}]
async fn dsc_start(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::Start(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Start all the downstairs
 */
#[endpoint {
    method = POST,
    path = "/start/all",
}]
async fn dsc_start_all(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::StartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Disable automatic restart on the given client_id
 */
#[endpoint {
    method = POST,
    path = "/disablerestart/cid/{cid}",
}]
async fn dsc_disable_restart(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::DisableRestart(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Disable automatic restart on all downstairs
 */
#[endpoint {
    method = POST,
    path = "/disablerestart/all",
}]
async fn dsc_disable_restart_all(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::DisableRestartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Enable automatic restart on the given client_id
 */
#[endpoint {
    method = POST,
    path = "/enablerestart/cid/{cid}",
}]
async fn dsc_enable_restart(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Cid>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let cid = path.cid;
    let api_context = rqctx.context();

    if cid_bad(&api_context.dsci, cid).await {
        return Err(HttpError::for_bad_request(
            None,
            format!("Invalid client id: {}", cid),
        ));
    }
    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::EnableRestart(cid));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Enable automatic restart on all downstairs
 */
#[endpoint {
    method = POST,
    path = "/enablerestart/all",
}]
async fn dsc_enable_restart_all(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::EnableRestartAll);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Get the count of regions.
 */
#[endpoint {
    method = GET,
    path = "/regioncount",
}]
async fn dsc_get_region_count(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseOk<usize>, HttpError> {
    let api_context = rqctx.context();

    let region_count = api_context.dsci.get_region_count().await;
    Ok(HttpResponseOk(region_count))
}

/**
 * Fetch the region info for our downstairs
 */
#[endpoint {
    method = GET,
    path = "/regioninfo",
}]
async fn dsc_get_region_info(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseOk<RegionExtentInfo>, HttpError> {
    let api_context = rqctx.context();

    let region_info =
        api_context.dsci.get_region_info().await.map_err(|e| {
            HttpError::for_bad_request(
                None,
                format!("failed to get region info {:#}", e),
            )
        })?;

    Ok(HttpResponseOk(region_info))
}

/**
 * Stop all downstairs, then stop ourselves.
 */
#[endpoint {
    method = POST,
    path = "/shutdown",
}]
async fn dsc_shutdown(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::Shutdown);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Enable stopping a random downstairs every [min-max] seconds
 */
#[endpoint {
    method = POST,
    path = "/randomstop/enable",
}]
async fn dsc_enable_random_stop(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::EnableRandomStop);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Disable the random stopping of a downstairs
 */
#[endpoint {
    method = POST,
    path = "/randomstop/disable",
}]
async fn dsc_disable_random_stop(
    rqctx: RequestContext<Arc<DownstairsControl>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::DisableRandomStop);
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Set the minimum time between random stopping requests
 */
#[endpoint {
    method = POST,
    path = "/randomstop/min/{min}",
}]
async fn dsc_enable_random_min(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Min>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let min = path.min;
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::RandomStopMin(min));
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Set the maximum time between random stopping requests
 */
#[endpoint {
    method = POST,
    path = "/randomstop/max/{max}",
}]
async fn dsc_enable_random_max(
    rqctx: RequestContext<Arc<DownstairsControl>>,
    path: Path<Max>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let max = path.max;
    let api_context = rqctx.context();

    let mut dsc_work = api_context.dsci.work.lock().await;
    dsc_work.add_cmd(DscCmd::RandomStopMax(max));
    Ok(HttpResponseUpdatedNoContent())
}

#[cfg(test)]
mod test {
    use openapiv3::OpenAPI;
    use semver::Version;

    use super::build_api;

    #[test]
    fn test_dsc_openapi() {
        let api = build_api();
        let mut raw = Vec::new();
        api.openapi("DownStairs Control", Version::new(0, 0, 1))
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
