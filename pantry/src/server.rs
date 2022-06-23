// Copyright 2022 Oxide Computer Company

use super::pantry::Pantry;
use anyhow::{anyhow, Result};
use dropshot::{
    endpoint, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody, HttpResponseCreated, HttpResponseUpdatedNoContent,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{o, Logger};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;

type DSResult<T> = SResult<T, HttpError>;

use crucible::{
    VolumeConstructionRequest,
};

trait AnyhowFromString<T> {
    fn or_bail(self, msg: &str) -> Result<T>;
    fn or_regfail(self) -> Result<T>;
}

impl<T> AnyhowFromString<T> for SResult<T, String> {
    fn or_bail(self, msg: &str) -> Result<T> {
        self.map_err(|e| anyhow!("{}: {:?}", msg, e))
    }

    fn or_regfail(self) -> Result<T> {
        self.or_bail("registration failure")
    }
}

#[derive(Deserialize, JsonSchema)]
struct VolumePath {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
struct AttachRequest {
    pub read_only: bool,
    pub gen: u64,
    pub volume_construction_request: VolumeConstructionRequest,
}

#[endpoint {
    method = PUT,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn attach(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<AttachRequest>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn detach(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumePath>,
) -> DSResult<HttpResponseDeleted> {
    Ok(HttpResponseDeleted())
}

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<Pantry>>> {
    let mut api = dropshot::ApiDescription::new();

    api.register(attach).or_regfail()?;
    api.register(detach).or_regfail()?;

    Ok(api)
}

pub async fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
    df: Arc<Pantry>,
) -> Result<()> {
    let api = make_api()?;

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address,
            request_body_max_bytes: 1024 * 10,
            ..Default::default()
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
