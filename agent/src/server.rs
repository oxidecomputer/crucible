use anyhow::{bail, anyhow, Result};
use std::net::SocketAddr;
use slog::{Logger, o};
use dropshot::{
    RequestContext,
    Path as TypedPath,
    TypedBody,
    HttpResponseOk,
    HttpError,
    endpoint,
};
use std::result::Result as SResult;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use schemars::JsonSchema;

trait AnyhowFromString<T> {
    fn or_bail(self, msg: &str) -> Result<T>;
}

impl<T> AnyhowFromString<T> for SResult<T, String> {
    fn or_bail(self, msg: &str) -> Result<T> {
        self.map_err(|e| anyhow!("{}: {:?}", msg, e))
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct Region {
    id: String,
    block_size: u32,
    block_count: u32,
}

#[endpoint {
    method = GET,
    path = "/crucible/0/regions",
}]
async fn regions(
    rc: Arc<RequestContext<()>>,
) -> SResult<HttpResponseOk<Vec<Region>>, HttpError> {
    unimplemented!();
}

pub async fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
) -> Result<()> {
    let mut api = dropshot::ApiDescription::new();
    api.register(regions).or_bail("registration failure")?;

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address,
            ..Default::default()
        },
        api,
        (),
        &log.new(o!("component" => "dropshot")),
    )
    .map_err(|e| anyhow!("creating server: {:?}", e))?
    .start();

    server.await.map_err(|e| anyhow!("starting server: {:?}", e))
}
