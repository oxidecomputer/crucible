// Copyright 2026 Oxide Computer Company

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsForRegionPath {
    pub uuid: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct DownstairsRunningResponse {
    pub uuid: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsForRegionParams {
    pub address: IpAddr,
    pub data: PathBuf,
    pub oximeter: Option<SocketAddr>,
    pub lossy: bool,
    pub port: u16,
    pub rport: u16,
    pub read_errors: bool,
    pub write_errors: bool,
    pub flush_errors: bool,
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_cert_pem: Option<String>,
    pub read_only: bool,
}
