// Copyright 2021 Oxide Computer Company

use super::datafile::DataFile;
use super::model;
use anyhow::{anyhow, Result};
use dropshot::{
    endpoint, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::Deserialize;
use slog::{o, Logger};
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;

trait AnyhowFromString<T> {
    fn or_bail(self, msg: &str) -> Result<T>;
}

impl<T> AnyhowFromString<T> for SResult<T, String> {
    fn or_bail(self, msg: &str) -> Result<T> {
        self.map_err(|e| anyhow!("{}: {:?}", msg, e))
    }
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions",
}]
async fn region_list(
    rc: Arc<RequestContext<Arc<DataFile>>>,
) -> SResult<HttpResponseOk<Vec<model::Region>>, HttpError> {
    Ok(HttpResponseOk(rc.context().all()))
}

#[endpoint {
    method = POST,
    path = "/crucible/0/regions",
}]
async fn region_create(
    rc: Arc<RequestContext<Arc<DataFile>>>,
    body: TypedBody<model::CreateRegion>,
) -> SResult<HttpResponseOk<model::Region>, HttpError> {
    let create = body.into_inner();

    match rc.context().request(create) {
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
    rc: Arc<RequestContext<Arc<DataFile>>>,
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
    rc: Arc<RequestContext<Arc<DataFile>>>,
    path: TypedPath<RegionPath>,
) -> SResult<HttpResponseDeleted, HttpError> {
    let p = path.into_inner();

    match rc.context().destroy(&p.id) {
        Ok(_) => Ok(HttpResponseDeleted()),
        Err(e) => Err(HttpError::for_bad_request(None, e.to_string())),
    }
}

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<DataFile>>> {
    let mut api = dropshot::ApiDescription::new();
    api.register(region_list).or_bail("registration failure")?;
    api.register(region_create)
        .or_bail("registration failure")?;
    api.register(region_get).or_bail("registration failure")?;
    api.register(region_delete)
        .or_bail("registration failure")?;
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
