// Copyright 2022 Oxide Computer Company
use super::*;

use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, HttpError, HttpResponseCreated,
    HttpServerStarter, Path, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub struct ServerContext {
    downstairs: Mutex<HashMap<Uuid, Arc<Mutex<Downstairs>>>>,
}

#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsForRegionParams {
    address: IpAddr,
    data: PathBuf,
    oximeter: Option<SocketAddr>,
    lossy: bool,
    port: u16,
    return_errors: bool,
    cert_pem: Option<String>,
    key_pem: Option<String>,
    root_cert_pem: Option<String>,
    read_only: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsforRegionPath {
    uuid: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct DownstairsRunningResponse {
    uuid: Uuid,
}

#[endpoint {
    method = POST,
    path = "/regions/{uuid}/downstairs"
}]
pub async fn run_downstairs_for_region(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    run_params: TypedBody<RunDownstairsForRegionParams>,
    path_param: Path<RunDownstairsforRegionPath>,
) -> Result<HttpResponseCreated<DownstairsRunningResponse>, HttpError> {
    let apictx = rqctx.context();
    let run_params = run_params.into_inner();
    let uuid = path_param.into_inner().uuid;

    let mut downstairs = apictx.downstairs.lock().await;

    if downstairs.contains_key(&uuid) {
        return Err(HttpError::for_bad_request(
            Some(String::from("BadInput")),
            format!("downstairs {} running already", uuid),
        ));
    }

    let d = create_downstairs(
        &run_params.data,
        run_params.lossy,
        run_params.return_errors,
        run_params.read_only,
    )
    .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    let dd = d.clone();
    tokio::spawn(async move {
        // XXX result eaten here!
        let _ = start_downstairs(
            dd,
            run_params.address,
            run_params.oximeter,
            run_params.port,
            run_params.cert_pem,
            run_params.key_pem,
            run_params.root_cert_pem,
        )
        .await;
    });

    downstairs.insert(uuid, d);

    Ok(HttpResponseCreated(DownstairsRunningResponse { uuid }))
}

fn register_endpoints(
    api_description: &mut ApiDescription<Arc<ServerContext>>,
) -> Result<(), String> {
    api_description.register(run_downstairs_for_region)?;

    Ok(())
}

pub async fn run_dropshot(
    bind_address: SocketAddr,
    log: &slog::Logger,
) -> Result<()> {
    let config = ConfigDropshot {
        bind_address,
        ..Default::default()
    };

    let mut api_description = ApiDescription::<Arc<ServerContext>>::new();

    if let Err(s) = register_endpoints(&mut api_description) {
        anyhow::bail!("Error from register_endpoints: {}", s);
    }

    let ctx = Arc::new(ServerContext {
        downstairs: Mutex::new(HashMap::default()),
    });

    let http_server =
        HttpServerStarter::new(&config, api_description, Arc::clone(&ctx), log);

    if let Err(e) = http_server {
        anyhow::bail!("Error from HttpServerStarter::new: {:?}", e);
    }

    if let Err(s) = http_server.unwrap().start().await {
        anyhow::bail!("Error from start(): {}", s);
    }

    Ok(())
}
