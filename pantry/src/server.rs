// Copyright 2022 Oxide Computer Company

use super::pantry::Pantry;
use anyhow::{anyhow, Result};
use dropshot::{
    endpoint, HttpError, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path as TypedPath,
    RequestContext, TypedBody,
};
use http::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Response,
};
use hyper::Body;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, Logger};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::result::Result as SResult;
use std::sync::Arc;
use tokio::task::block_in_place;

type DSResult<T> = SResult<T, HttpError>;

use crucible::{BlockIO, Buffer, VolumeConstructionRequest};

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

trait ToHttpErrors<T> {
    fn or_404(self) -> DSResult<T>;
    fn or_400(self) -> DSResult<T>;
}

impl<T> ToHttpErrors<T> for Result<T> {
    fn or_404(self) -> DSResult<T> {
        self.map_err(|e| {
            HttpError::for_not_found(None, "not found".to_string())
        })
    }

    fn or_400(self) -> DSResult<T> {
        self.map_err(|e| {
            HttpError::for_bad_request(None, "bad things!".to_string())
        })
    }
}

impl<T> ToHttpErrors<T> for SResult<T, crucible::CrucibleError> {
    fn or_404(self) -> DSResult<T> {
        self.map_err(|e| {
            HttpError::for_not_found(None, "not found".to_string())
        })
    }

    fn or_400(self) -> DSResult<T> {
        self.map_err(|e| {
            HttpError::for_bad_request(None, "bad things!".to_string())
        })
    }
}

#[derive(Deserialize, JsonSchema)]
struct VolumePath {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
struct VolumeOffsetPath {
    pub id: String,
    pub offset: u64,
}

#[derive(Deserialize, Debug, JsonSchema)]
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
    let log = &rc.log;
    let p = rc.context();

    let id = path.into_inner().id;
    let b = body.into_inner();

    info!(log, "attach request: {} -> {:?}", id, b);

    if let Err(e) = p.attach(id, b.volume_construction_request, b.gen).await {
        error!(log, "attach fail: {:?}", e);
        Err(HttpError::for_internal_error(format!("attach fail: {}", e)))
    } else {
        info!(log, "attach ok!");
        Ok(HttpResponseUpdatedNoContent())
    }
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

#[endpoint {
    method = GET,
    path = "/crucible/pantry/0/volume/{id}/get/{offset}",
}]
async fn chunk_read(
    rc: Arc<RequestContext<Arc<Pantry>>>,
    path: TypedPath<VolumeOffsetPath>,
) -> DSResult<Response<Body>> {
    let log = &rc.log;
    let p = rc.context();

    let path = path.into_inner();

    /*
     * Look for the requested volume first.
     */
    let a = p.lookup(&path.id).await.or_404()?;

    info!(log, "volume {} offset {} get", path.id, path.offset);
    let mut buf = Buffer::new(512 * 1024);
    let mut io = if let Some(vol) = a.lock().await.volume() {
        let offset = block_in_place(|| vol.byte_offset_to_block(path.offset))
            .or_400()?;
        block_in_place(|| vol.read(offset, buf.clone())).or_400()?
    } else {
        return Err(HttpError::for_bad_request(
            None,
            "volume not available yet".into(),
        ));
    };

    match block_in_place(|| io.block_wait()) {
        Ok(()) => {
            /*
             * Ok, we got the data!
             */
            let mut res = Response::builder();
            res = res.header(CONTENT_TYPE, "application/octet-stream");
            res = res.header(CONTENT_LENGTH, buf.len());
            let data = buf.as_vec().clone();
            Ok(res.body(data.into())?)
        }
        Err(e) => {
            error!(log, "crucible error: {:?}", e);
            Err(HttpError::for_internal_error(format!(
                "crucible error: {}",
                e
            )))
        }
    }
}

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<Pantry>>> {
    let mut api = dropshot::ApiDescription::new();

    api.register(attach).or_regfail()?;
    api.register(detach).or_regfail()?;
    api.register(chunk_read).or_regfail()?;

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
