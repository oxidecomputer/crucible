// Copyright 2022 Oxide Computer Company

use super::pantry::Pantry;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use dropshot::endpoint;
use dropshot::HttpError;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path as TypedPath;
use dropshot::RequestContext;
use dropshot::TypedBody;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{info, o, Logger};

use crucible::VolumeConstructionRequest;

#[derive(Deserialize, JsonSchema)]
struct VolumePath {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
struct AttachRequest {
    pub volume_construction_request: VolumeConstructionRequest,
}

#[derive(Serialize, JsonSchema)]
struct AttachResult {
    pub id: String,
}

/// Construct a volume from a VolumeConstructionRequest, storing the result in
/// the Pantry.
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn attach(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<AttachRequest>,
) -> Result<HttpResponseOk<AttachResult>, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry
        .attach(path.id.clone(), body.volume_construction_request)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(AttachResult { id: path.id }))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub enum ExpectedDigest {
    Sha256(String),
}

#[derive(Deserialize, JsonSchema)]
struct ImportFromUrlRequest {
    pub url: String,
    pub expected_digest: Option<ExpectedDigest>,
}

/// Import data from a URL into a volume
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/import_from_url",
}]
async fn import_from_url(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<ImportFromUrlRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry
        .import_from_url(path.id.clone(), body.url, body.expected_digest)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
struct SnapshotRequest {
    pub snapshot_id: String,
}

/// Take a snapshot of a volume
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/snapshot",
}]
async fn snapshot(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<SnapshotRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry
        .snapshot(path.id.clone(), body.snapshot_id)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
struct BulkWriteRequest {
    pub offset: u64,

    pub base64_encoded_data: String,
}

/// Bulk write data into a volume at a specified offset
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/bulk_write",
}]
async fn bulk_write(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<BulkWriteRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    let data = base64::decode(body.base64_encoded_data)
        .map_err(|e| HttpError::for_bad_request(None, e.to_string()))?;

    pantry
        .bulk_write(path.id.clone(), body.offset, data)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Flush and close a volume, removing it from the Pantry
#[endpoint {
    method = DELETE,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn detach(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    pantry
        .detach(path.id)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseDeleted())
}

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<Pantry>>, String> {
    let mut api = dropshot::ApiDescription::new();

    api.register(attach)?;
    api.register(import_from_url)?;
    api.register(snapshot)?;
    api.register(bulk_write)?;
    api.register(detach)?;

    Ok(api)
}

pub async fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
    df: Arc<Pantry>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<(), String>>)> {
    let api = make_api().map_err(|e| anyhow!(e))?;

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address,
            // max import, multiplied by worst case base64 overhead, plus room
            // for metadata
            request_body_max_bytes: 1024
                + crate::pantry::PantryEntry::MAX_CHUNK_SIZE * 2,
            ..Default::default()
        },
        api,
        df,
        &log.new(o!("component" => "dropshot")),
    )
    .map_err(|e| anyhow!("creating server: {:?}", e))?
    .start();

    let local_addr = server.local_addr();
    info!(log, "listen IP: {:?}", local_addr);

    let join_handle = tokio::spawn(async move { server.await });

    Ok((local_addr, join_handle))
}
