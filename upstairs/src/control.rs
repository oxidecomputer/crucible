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

use super::*;

pub(crate) fn build_api() -> ApiDescription<UpstairsInfo> {
    let mut api = ApiDescription::new();
    api.register(upstairs_fill_info).unwrap();
    api.register(downstairs_work_queue).unwrap();

    api
}

/**
 * Start up a dropshot server along side the Upstairs.  This interface
 * provides access to upstairs information and downstairs work queues.
 */
pub async fn start(
    up: UpstairsInfo,
    log: Logger,
    addr: SocketAddr,
) -> Result<(), String> {
    /*
     * Setup dropshot
     */
    let config_dropshot = ConfigDropshot {
        bind_address: addr,
        request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
    };

    /*
     * Build a description of the API.
     */
    let api = build_api();

    /*
     * The functions that implement our API endpoints will share this
     * context.
     */
    let api_context = up;

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

/// Request sent to the upstairs by the control server
///
/// Values are returned through the provided oneshot channel
pub(crate) enum ControlRequest {
    DownstairsWorkQueue(oneshot::Sender<DownstairsWork>),
    UpstairsStats(oneshot::Sender<UpstairsStats>),
}

impl std::fmt::Debug for ControlRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlRequest::DownstairsWorkQueue(..) => {
                f.debug_struct("DownstairsWorkQueue").finish()
            }
            ControlRequest::UpstairsStats(..) => {
                f.debug_struct("UpstairsStats").finish()
            }
        }
    }
}

/**
 * The state shared by handler functions
 */
pub struct UpstairsInfo {
    /// Channel used to send messages to the upstairs
    up: mpsc::Sender<ControlRequest>,
}

impl UpstairsInfo {
    /**
     * Return a new UpstairsInfo.
     */
    pub(crate) fn new(up: &crate::upstairs::Upstairs) -> UpstairsInfo {
        UpstairsInfo {
            up: up.control_tx.clone(),
        }
    }
}

/**
 * `UpstairsInfo` holds the information gathered from the upstairs to fill
 * a response to a GET request
 */
#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct UpstairsStats {
    pub state: UpState,
    pub ds_state: Vec<DsState>,
    pub up_jobs: usize,
    pub ds_jobs: usize,
    pub repair_done: usize,
    pub repair_needed: usize,
    pub extents_repaired: Vec<usize>,
    pub extents_confirmed: Vec<usize>,
    pub extent_limit: Vec<Option<usize>>,
    pub live_repair_completed: Vec<usize>,
    pub live_repair_aborted: Vec<usize>,
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
    rqctx: RequestContext<UpstairsInfo>,
) -> Result<HttpResponseOk<UpstairsStats>, HttpError> {
    let api_context = rqctx.context();

    let (tx, rx) = oneshot::channel();
    api_context
        .up
        .send(ControlRequest::UpstairsStats(tx))
        .await
        .unwrap();

    let out = rx.await.unwrap();
    Ok(HttpResponseOk(out))
}

/**
 * `DownstairsWork` holds the information gathered from the downstairs
 */
#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct DownstairsWork {
    pub jobs: Vec<WorkSummary>,
    pub completed: Vec<WorkSummary>,
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
    rqctx: RequestContext<UpstairsInfo>,
) -> Result<HttpResponseOk<DownstairsWork>, HttpError> {
    let api_context = rqctx.context();

    let (tx, rx) = oneshot::channel();
    api_context
        .up
        .send(ControlRequest::DownstairsWorkQueue(tx))
        .await
        .unwrap();

    let out = rx.await.unwrap();
    Ok(HttpResponseOk(out))
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
