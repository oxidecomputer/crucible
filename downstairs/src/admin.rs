// Copyright 2022 Oxide Computer Company
use super::*;

use crucible_downstairs_admin_api::*;
use crucible_downstairs_types::RunDownstairsForRegionParams;
use dropshot::{
    ConfigDropshot, HttpError, HttpResponseCreated, HttpServerStarter, Path,
    RequestContext, TypedBody,
};

pub struct ServerContext {
    // Region UUID -> a running Downstairs
    downstairs: Mutex<HashMap<Uuid, DownstairsHandle>>,
}

/// Implementation of the Crucible Downstairs Admin API.
pub struct CrucibleDownstairsAdminImpl;

impl CrucibleDownstairsAdminApi for CrucibleDownstairsAdminImpl {
    type Context = Arc<ServerContext>;

    /// Start a downstairs instance for a specific region.
    async fn run_downstairs_for_region(
        rqctx: RequestContext<Self::Context>,
        path_param: Path<RunDownstairsForRegionPath>,
        run_params: TypedBody<RunDownstairsForRegionParams>,
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

        let certs = match (
            run_params.cert_pem,
            run_params.key_pem,
            run_params.root_cert_pem,
        ) {
            (Some(cert_pem), Some(key_pem), Some(root_cert_pem)) => {
                Some(DownstairsClientCerts {
                    cert_pem,
                    key_pem,
                    root_cert_pem,
                })
            }
            (None, None, None) => None,
            _ => {
                return Err(HttpError::for_bad_request(
                    Some(String::from("BadInput")),
                    "must provide all of cert_pem, key_pem, root_cert_pem \
                     if any are provided"
                        .to_owned(),
                ))
            }
        };

        let d = Downstairs::new_builder(&run_params.data, run_params.read_only)
            .set_lossy(run_params.lossy)
            .set_test_errors(
                run_params.read_errors,
                run_params.write_errors,
                run_params.flush_errors,
            )
            .build()
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        let handle = d.handle();
        let _join_handle = DownstairsClient::spawn(
            d,
            DownstairsClientSettings {
                address: run_params.address,
                oximeter: run_params.oximeter,
                port: run_params.port,
                rport: run_params.rport,
                certs,
            },
        )
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        // past here, the downstairs has started successfully.

        downstairs.insert(uuid, handle);

        Ok(HttpResponseCreated(DownstairsRunningResponse { uuid }))
    }
}

pub async fn run_dropshot(
    bind_address: SocketAddr,
    log: &slog::Logger,
) -> Result<()> {
    let config = ConfigDropshot {
        bind_address,
        ..Default::default()
    };

    let api_description = crucible_downstairs_admin_api_mod::api_description::<
        CrucibleDownstairsAdminImpl,
    >()?;

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
