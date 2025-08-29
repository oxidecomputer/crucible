// Copyright 2024 Oxide Computer Company
use super::datafile::DataFile;
use anyhow::{anyhow, Result};
use crucible_agent_api::*;
use crucible_agent_types::{region, snapshot};
use dropshot::{
    HandlerTaskMode, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
};
use slog::{o, Logger};
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct CrucibleAgentImpl;

impl CrucibleAgentApi for CrucibleAgentImpl {
    type Context = Arc<DataFile>;

    async fn region_list(
        rqctx: RequestContext<Self::Context>,
    ) -> SResult<HttpResponseOk<Vec<region::Region>>, HttpError> {
        Ok(HttpResponseOk(rqctx.context().regions()))
    }

    async fn region_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<region::CreateRegion>,
    ) -> SResult<HttpResponseOk<region::Region>, HttpError> {
        let create = body.into_inner();

        match rqctx.context().create_region_request(create) {
            Ok(r) => Ok(HttpResponseOk(r)),
            Err(e) => Err(HttpError::for_internal_error(format!(
                "region create failure: {:?}",
                e
            ))),
        }
    }

    async fn region_get(
        rc: RequestContext<Self::Context>,
        path: TypedPath<RegionPath>,
    ) -> SResult<HttpResponseOk<region::Region>, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(r) => Ok(HttpResponseOk(r)),
            None => Err(HttpError::for_not_found(
                None,
                format!("region {:?} not found", p.id),
            )),
        }
    }

    async fn region_delete(
        rc: RequestContext<Self::Context>,
        path: TypedPath<RegionPath>,
    ) -> SResult<HttpResponseDeleted, HttpError> {
        let p = path.into_inner();

        // Cannot delete a region that's backed by a ZFS dataset if there are
        // snapshots.

        let snapshots = match rc.context().get_snapshots_for_region(&p.id) {
            Ok(results) => results,
            Err(e) => {
                return Err(HttpError::for_internal_error(e.to_string()));
            }
        };

        if !snapshots.is_empty() {
            return Err(HttpError::for_bad_request(
                None,
                "must delete snapshots first!".to_string(),
            ));
        }

        match rc.context().destroy(&p.id) {
            Ok(_) => Ok(HttpResponseDeleted()),
            Err(e) => Err(HttpError::for_bad_request(None, e.to_string())),
        }
    }

    async fn region_get_snapshots(
        rc: RequestContext<Arc<DataFile>>,
        path: TypedPath<RegionPath>,
    ) -> Result<HttpResponseOk<GetSnapshotResponse>, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(_) => (),
            None => {
                return Err(HttpError::for_not_found(
                    None,
                    format!("region {:?} not found", p.id),
                ));
            }
        }

        let snapshots = match rc.context().get_snapshots_for_region(&p.id) {
            Ok(results) => results,
            Err(e) => {
                return Err(HttpError::for_internal_error(e.to_string()));
            }
        };

        let running_snapshots = rc
            .context()
            .running_snapshots()
            .get(&p.id)
            .cloned()
            .unwrap_or_default();

        Ok(HttpResponseOk(GetSnapshotResponse {
            snapshots,
            running_snapshots,
        }))
    }

    async fn region_get_snapshot(
        rc: RequestContext<Self::Context>,
        path: TypedPath<GetSnapshotPath>,
    ) -> Result<HttpResponseOk<snapshot::Snapshot>, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(_) => (),
            None => {
                return Err(HttpError::for_not_found(
                    None,
                    format!("region {:?} not found", p.id),
                ));
            }
        }

        let snapshots_for_region =
            match rc.context().get_snapshots_for_region(&p.id) {
                Ok(results) => results,
                Err(e) => {
                    return Err(HttpError::for_internal_error(e.to_string()));
                }
            };

        for snapshot in &snapshots_for_region {
            if snapshot.name == p.name {
                return Ok(HttpResponseOk(snapshot.clone()));
            }
        }

        Err(HttpError::for_not_found(
            None,
            format!("region {:?} snapshot {:?} not found", p.id, p.name),
        ))
    }

    async fn region_delete_snapshot(
        rc: RequestContext<Self::Context>,
        path: TypedPath<DeleteSnapshotPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(_) => (),
            None => {
                return Err(HttpError::for_not_found(
                    None,
                    format!("region {:?} not found", p.id),
                ));
            }
        }

        let request = snapshot::DeleteSnapshotRequest {
            id: p.id.clone(),
            name: p.name,
        };

        match rc.context().delete_snapshot(request) {
            Ok(_) => Ok(HttpResponseDeleted()),
            Err(e) => Err(HttpError::for_internal_error(e.to_string())),
        }
    }

    async fn region_run_snapshot(
        rc: RequestContext<Arc<DataFile>>,
        path: TypedPath<RunSnapshotPath>,
    ) -> Result<HttpResponseOk<snapshot::RunningSnapshot>, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(_) => (),
            None => {
                return Err(HttpError::for_not_found(
                    None,
                    format!("region {:?} not found", p.id),
                ));
            }
        }

        let snapshots = match rc.context().get_snapshots_for_region(&p.id) {
            Ok(results) => results,
            Err(e) => {
                return Err(HttpError::for_internal_error(e.to_string()));
            }
        };

        let snapshot_names: Vec<String> =
            snapshots.iter().map(|s| s.name.clone()).collect();

        if !snapshot_names.contains(&p.name) {
            return Err(HttpError::for_not_found(
                None,
                format!("snapshot {:?} not found", p.name),
            ));
        }

        // TODO support running snapshots with their own X509 creds
        let create = snapshot::CreateRunningSnapshotRequest {
            id: p.id,
            name: p.name,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
        };

        match rc.context().create_running_snapshot_request(create) {
            Ok(r) => Ok(HttpResponseOk(r)),
            Err(e) => Err(HttpError::for_internal_error(format!(
                "running snapshot create failure: {:?}",
                e
            ))),
        }
    }

    async fn region_delete_running_snapshot(
        rc: RequestContext<Self::Context>,
        path: TypedPath<RunSnapshotPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(_) => (),
            None => {
                return Err(HttpError::for_not_found(
                    None,
                    format!("region {:?} not found", p.id),
                ));
            }
        }

        let request = snapshot::DeleteRunningSnapshotRequest {
            id: p.id,
            name: p.name,
        };

        match rc.context().delete_running_snapshot_request(request) {
            Ok(_) => Ok(HttpResponseDeleted()),
            Err(e) => Err(HttpError::for_internal_error(format!(
                "running snapshot create failure: {:?}",
                e
            ))),
        }
    }
}

pub async fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
    df: Arc<DataFile>,
) -> Result<()> {
    let api = crucible_agent_api_mod::api_description::<CrucibleAgentImpl>()?;

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address,
            default_request_body_max_bytes: 1024 * 10,
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
        },
        api,
        df,
        &log.new(o!("component" => "dropshot")),
    )
    .map_err(|e| anyhow!("creating server: {:?}", e))?
    .start();

    server
        .await
        .map_err(|e| anyhow!("starting server: {:?}", e))
}
