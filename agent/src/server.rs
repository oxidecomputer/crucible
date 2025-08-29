// Copyright 2024 Oxide Computer Company
use super::datafile::{DataFile, JobInfo};
use super::model;
use anyhow::{anyhow, Result};
use dropshot::{
    endpoint, HandlerTaskMode, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{o, Logger};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;

#[endpoint {
    method = GET,
    path = "/crucible/0/regions",
}]
async fn region_list(
    rc: RequestContext<Arc<DataFile>>,
) -> SResult<HttpResponseOk<Vec<model::Region>>, HttpError> {
    Ok(HttpResponseOk(rc.context().regions()))
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions",
}]
async fn region_create(
    rc: RequestContext<Arc<DataFile>>,
    body: TypedBody<model::CreateRegion>,
) -> SResult<HttpResponseOk<model::Region>, HttpError> {
    let create = body.into_inner();

    match rc.context().create_region_request(create) {
        Ok(r) => Ok(HttpResponseOk(r)),
        Err(e) => Err(HttpError::for_internal_error(format!(
            "region create failure: {:?}",
            e
        ))),
    }
}

#[derive(Deserialize, JsonSchema)]
struct RegionPath {
    id: model::RegionId,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}",
}]
async fn region_get(
    rc: RequestContext<Arc<DataFile>>,
    path: TypedPath<RegionPath>,
) -> SResult<HttpResponseOk<model::Region>, HttpError> {
    let p = path.into_inner();

    match rc.context().get(&p.id) {
        Some(r) => Ok(HttpResponseOk(r)),
        None => Err(HttpError::for_not_found(
            None,
            format!("region {:?} not found", p.id),
        )),
    }
}

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}",
}]
async fn region_delete(
    rc: RequestContext<Arc<DataFile>>,
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

#[derive(Serialize, JsonSchema)]
pub struct GetSnapshotResponse {
    snapshots: Vec<model::Snapshot>,
    running_snapshots: BTreeMap<String, model::RunningSnapshot>,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}/snapshots",
}]
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

#[derive(Deserialize, JsonSchema)]
struct GetSnapshotPath {
    id: model::RegionId,
    name: String,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions/{id}/snapshots/{name}",
}]
async fn region_get_snapshot(
    rc: RequestContext<Arc<DataFile>>,
    path: TypedPath<GetSnapshotPath>,
) -> Result<HttpResponseOk<model::Snapshot>, HttpError> {
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

#[derive(Deserialize, JsonSchema)]
struct DeleteSnapshotPath {
    id: model::RegionId,
    name: String,
}

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}/snapshots/{name}",
}]
async fn region_delete_snapshot(
    rc: RequestContext<Arc<DataFile>>,
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

    let request = model::DeleteSnapshotRequest {
        id: p.id.clone(),
        name: p.name,
    };

    match rc.context().delete_snapshot(request) {
        Ok(_) => Ok(HttpResponseDeleted()),
        Err(e) => Err(HttpError::for_internal_error(e.to_string())),
    }
}

#[derive(Deserialize, JsonSchema)]
struct RunSnapshotPath {
    id: model::RegionId,
    name: String,
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions/{id}/snapshots/{name}/run",
}]
async fn region_run_snapshot(
    rc: RequestContext<Arc<DataFile>>,
    path: TypedPath<RunSnapshotPath>,
) -> Result<HttpResponseOk<model::RunningSnapshot>, HttpError> {
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
    let create = model::CreateRunningSnapshotRequest {
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

#[endpoint {
    method = DELETE,
    path = "/crucible/0/regions/{id}/snapshots/{name}/run",
}]
async fn region_delete_running_snapshot(
    rc: RequestContext<Arc<DataFile>>,
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

    let request = model::DeleteRunningSnapshotRequest {
        id: p.id,
        name: p.name,
    };

    match rc.context().delete_running_snapshot_request(request) {
        Ok(_) => Ok(HttpResponseDeleted()),
        Err(e) => Err(HttpError::for_internal_error(format!(
            "running snapshot delete failure: {:?}",
            e
        ))),
    }
}

#[endpoint {
    method = GET,
    path = "/crucible/0/work",
}]
async fn region_get_work_queue(
    rc: RequestContext<Arc<DataFile>>,
) -> SResult<HttpResponseOk<Vec<JobInfo>>, HttpError> {
    Ok(HttpResponseOk(rc.context().get_work_queue()))
}

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<DataFile>>> {
    let mut api = dropshot::ApiDescription::new();

    api.register(region_list)?;
    api.register(region_create)?;
    api.register(region_get)?;
    api.register(region_delete)?;

    api.register(region_get_snapshots)?;
    api.register(region_get_snapshot)?;
    api.register(region_delete_snapshot)?;

    api.register(region_run_snapshot)?;
    api.register(region_delete_running_snapshot)?;

    api.register(region_get_work_queue)?;
    Ok(api)
}

pub async fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
    df: Arc<DataFile>,
) -> Result<()> {
    let api = make_api()?;

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
