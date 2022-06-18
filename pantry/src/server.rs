// Copyright 2022 Oxide Computer Company

use super::pantry::Pantry;
use anyhow::{anyhow, Result};
use dropshot::{
    endpoint, HttpError, HttpResponseDeleted, HttpResponseOk,
    Path as TypedPath, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{o, Logger};
use std::collections::BTreeMap;
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

pub fn make_api() -> Result<dropshot::ApiDescription<Arc<Pantry>>> {
    let mut api = dropshot::ApiDescription::new();

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
