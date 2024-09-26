// Copyright 2023 Oxide Computer Company
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
// use dropshot::HttpResponseCreated;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::HttpServerStarter;
use dropshot::Path;
use dropshot::RequestContext;
// use dropshot::TypedBody;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

use super::*;

pub(crate) fn build_api() -> ApiDescription<Arc<GuestInfo>> {
    let mut api = ApiDescription::new();
    api.register(guest_ping).unwrap();
    api.register(guest_activate).unwrap();

    api
}

/**
 * Start up a dropshot server with the guest structure
 */
pub async fn start_crutest_server(guest: &Arc<Guest>, addr: SocketAddr) -> Result<(), String> {
    /*
     * Setup dropshot
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
     * Build a description of the API.
     */
    let api = build_api();

    /*
     * The functions that implement our API endpoints will share this
     * context.
     */
    let api_context = Arc::new(GuestInfo::new(guest));

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
pub struct GuestInfo {
    /**
     * Guest structure that is used to gather all the info stats
     */
    guest: Arc<Guest>,
}

impl GuestInfo {
    /**
     * Return a new GuestInfo.
     */
    pub fn new(guest: &Arc<Guest>) -> GuestInfo {
        GuestInfo { guest: guest.clone() }
    }
}

/**
 * `GuestInfo` holds
 */
#[derive(Deserialize, Serialize, JsonSchema)]
struct GuestStats {
    blah: u32,
}

/**
 * Does the endpoint work?
 */
#[endpoint {
    method = GET,
    path = "/info",
    unpublished = false,
}]
async fn guest_ping(
    rqctx: RequestContext<Arc<GuestInfo>>,
) -> Result<HttpResponseOk<GuestStats>, HttpError> {
    let api_context = rqctx.context();

    let _guest = &api_context.guest;

    Ok(HttpResponseOk(GuestStats {
        blah: 32,
    }))
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Generation {
    generation: u64,
}

#[endpoint {
    method = POST,
    path = "/activate/{generation}",
    unpublished = false,
}]
async fn guest_activate(
    rqctx: RequestContext<Arc<GuestInfo>>,
    path: Path<Generation>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let api_context = rqctx.context();
    let path = path.into_inner();
    let generation = path.generation;

    let guest = &api_context.guest;
    guest
        .activate_with_gen(generation)
        .await
        .map_err(|e| {
            HttpError::for_bad_request(
                None,
                format!("Activate failed with {:#}", e),
            )
        })?;

    Ok(HttpResponseUpdatedNoContent())
}

#[cfg(test)]
mod test {
    use openapiv3::OpenAPI;

    use super::build_api;

    #[test]
    fn test_crutest_control_openapi() {
        let api = build_api();
        let mut raw = Vec::new();
        api.openapi("Crutest Control", "0.0.0")
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
            "../openapi/crutest-control.json",
            &actual,
        );
    }
}
