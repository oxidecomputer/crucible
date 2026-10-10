// Copyright 2026 Oxide Computer Company
use super::datafile::DataFile;
use anyhow::{Result, anyhow};
use crucible_agent_api::*;
use crucible_agent_types::{
    region::{CreateRegion, Region, RegionPath},
    snapshot::{
        CreateRunningSnapshotRequest, DeleteRunningSnapshotRequest,
        DeleteSnapshotPath, DeleteSnapshotRequest, GetSnapshotPath,
        GetSnapshotResponse, RunSnapshotPath, RunningSnapshot, Snapshot,
    },
};
use dropshot::{
    ClientSpecifiesVersionInHeader, HandlerTaskMode, HttpError,
    HttpResponseDeleted, HttpResponseOk, Path as TypedPath, RequestContext,
    TypedBody, VersionPolicy,
};
use slog::{Logger, o};
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct CrucibleAgentImpl;

impl CrucibleAgentApi for CrucibleAgentImpl {
    type Context = Arc<DataFile>;

    async fn region_list(
        rqctx: RequestContext<Self::Context>,
    ) -> SResult<HttpResponseOk<Vec<Region>>, HttpError> {
        let regions = rqctx
            .context()
            .regions()
            .into_iter()
            .map(|r| r.into())
            .collect();

        Ok(HttpResponseOk(regions))
    }

    async fn region_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<CreateRegion>,
    ) -> SResult<HttpResponseOk<Region>, HttpError> {
        let create = body.into_inner();

        match rqctx.context().create_region_request(create) {
            Ok(r) => Ok(HttpResponseOk(r.into())),
            Err(e) => Err(HttpError::for_internal_error(format!(
                "region create failure: {:?}",
                e
            ))),
        }
    }

    async fn region_get(
        rc: RequestContext<Self::Context>,
        path: TypedPath<RegionPath>,
    ) -> SResult<HttpResponseOk<Region>, HttpError> {
        let p = path.into_inner();

        match rc.context().get(&p.id) {
            Some(r) => Ok(HttpResponseOk(r.into())),
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

        rc.context().destroy(&p.id)?;

        Ok(HttpResponseDeleted())
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
            running_snapshots: running_snapshots
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }))
    }

    async fn region_get_snapshot(
        rc: RequestContext<Self::Context>,
        path: TypedPath<GetSnapshotPath>,
    ) -> Result<HttpResponseOk<Snapshot>, HttpError> {
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

        let request = DeleteSnapshotRequest {
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
    ) -> Result<HttpResponseOk<RunningSnapshot>, HttpError> {
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
        let create = CreateRunningSnapshotRequest {
            id: p.id,
            name: p.name,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
        };

        match rc.context().create_running_snapshot_request(create) {
            Ok(r) => Ok(HttpResponseOk(r.into())),
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

        let request = DeleteRunningSnapshotRequest {
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

    let server = dropshot::ServerBuilder::new(
        api,
        df,
        log.new(o!("component" => "dropshot")),
    )
    .config(dropshot::ConfigDropshot {
        bind_address,
        default_request_body_max_bytes: 1024 * 10,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
        compression: dropshot::CompressionConfig::None,
    })
    .version_policy(VersionPolicy::Dynamic(Box::new(
        ClientSpecifiesVersionInHeader::new(
            omicron_common::api::VERSION_HEADER,
            crucible_agent_api::latest_version(),
        ),
    )))
    .build_starter()
    .map_err(|e| anyhow!("creating server: {:?}", e))?
    .start();

    server
        .await
        .map_err(|e| anyhow!("starting server: {:?}", e))
}
