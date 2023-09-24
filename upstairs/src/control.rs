// Copyright 2022 Oxide Computer Company
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::HandlerTaskMode;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServerStarter;
use dropshot::RequestContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

use super::*;

pub(crate) fn build_api() -> ApiDescription<Arc<UpstairsInfo>> {
    let mut api = ApiDescription::new();
    api.register(upstairs_fill_info).unwrap();
    api.register(downstairs_work_queue).unwrap();

    api
}

/**
 * Start up a dropshot server along side the Upstairs.  This interface
 * provides access to upstairs information and downstairs work queues.
 */
pub async fn start(up: &Arc<Upstairs>, addr: SocketAddr) -> Result<(), String> {
    /*
     * Setup dropshot
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
    };

    let log = up.log.new(o!("task" => "control".to_string()));

    /*
     * Build a description of the API.
     */
    let api = build_api();

    /*
     * The functions that implement our API endpoints will share this
     * context.
     */
    let api_context = Arc::new(UpstairsInfo::new(up));

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
pub struct UpstairsInfo {
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
    ds_state: Vec<DsState>,
    up_jobs: usize,
    ds_jobs: usize,
    repair_done: usize,
    repair_needed: usize,
    extents_repaired: Vec<usize>,
    extents_confirmed: Vec<usize>,
    extent_limit: Vec<Option<usize>>,
    live_repair_completed: Vec<usize>,
    live_repair_aborted: Vec<usize>,
}

/**
 * Fetch the current value for all the stats in the UpstairsStats struct
 */
#[endpoint {
    method = GET,
    path = "/info",
    unpublished = false,
}]
async fn upstairs_fill_info(
    rqctx: RequestContext<Arc<UpstairsInfo>>,
) -> Result<HttpResponseOk<UpstairsStats>, HttpError> {
    let api_context = rqctx.context();

    let act = api_context.up.active.lock().await.up_state;
    let ds_state = api_context.up.ds_state_copy().await;
    let up_jobs = api_context.up.guest.guest_work.lock().await.active.len();
    let ds = api_context.up.downstairs.lock().await;
    let ds_jobs = ds.ds_active.len();
    let repair_done = ds.reconcile_repaired;
    let repair_needed = ds.reconcile_repair_needed;
    let extents_repaired = ds.extents_repaired;
    let extents_confirmed = ds.extents_confirmed;
    let extent_limit = ds.extent_limit;
    let live_repair_completed = ds.live_repair_completed;
    let live_repair_aborted = ds.live_repair_aborted;

    // Convert from a map of extent limits to a Vec<Option<usize>>
    let extent_limit = ClientId::iter()
        .map(|i| extent_limit.get(&i).cloned())
        .collect();

    Ok(HttpResponseOk(UpstairsStats {
        state: act,
        ds_state: ds_state.0.to_vec(),
        up_jobs,
        ds_jobs,
        repair_done,
        repair_needed,
        extents_repaired: extents_repaired.0.to_vec(),
        extents_confirmed: extents_confirmed.0.to_vec(),
        extent_limit,
        live_repair_completed: live_repair_completed.0.to_vec(),
        live_repair_aborted: live_repair_aborted.0.to_vec(),
    }))
}

/**
 * `DownstairsWork` holds the information gathered from the downstairs
 */
#[derive(Deserialize, Serialize, JsonSchema)]
struct DownstairsWork {
    jobs: Vec<WorkSummary>,
    completed: Vec<WorkSummary>,
}

async fn build_downstairs_job_list(up: &Arc<Upstairs>) -> Vec<WorkSummary> {
    let ds = up.downstairs.lock().await;
    let mut kvec: Vec<_> = ds.ds_active.keys().cloned().collect();
    kvec.sort_unstable();

    let mut jobs = Vec::new();
    for id in kvec.iter() {
        let job = ds.ds_active.get(id).unwrap();
        let work_summary = job.io_summarize();
        jobs.push(work_summary);
    }
    jobs
}

/**
 * Fetch the current downstairs work queue and populate a WorkSummary
 * struct for each job we find.
 */
#[endpoint {
    method = GET,
    path = "/work",
    unpublished = false,
}]
async fn downstairs_work_queue(
    rqctx: RequestContext<Arc<UpstairsInfo>>,
) -> Result<HttpResponseOk<DownstairsWork>, HttpError> {
    let api_context = rqctx.context();

    let jobs = build_downstairs_job_list(&api_context.up.clone()).await;
    let ds = api_context.up.downstairs.lock().await;
    let completed = ds.completed_jobs.to_vec();

    Ok(HttpResponseOk(DownstairsWork { jobs, completed }))
}

#[cfg(test)]
mod test {
    use openapiv3::OpenAPI;

    use super::build_api;

    #[test]
    fn test_crucible_control_openapi() {
        let api = build_api();
        let mut raw = Vec::new();
        api.openapi("Crucible Control", "0.0.0")
            .write(&mut raw)
            .unwrap();
        let actual = String::from_utf8(raw).unwrap();

        // Make sure the result parses as a valid OpenAPI spec.
        let spec = serde_json::from_str::<OpenAPI>(&actual)
            .expect("output was not valid OpenAPI");

        // Check for lint errors.
        let errors = openapi_lint::validate(&spec);
        assert!(errors.is_empty(), "{}", errors.join("\n\n"));

        expectorate::assert_contents(
            "../openapi/crucible-control.json",
            &actual,
        );
    }
}
