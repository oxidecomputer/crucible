// Copyright 2025 Oxide Computer Company

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::Deserialize;

// Repair API types
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Eid {
    pub eid: u32,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FileType {
    #[serde(rename = "data")]
    Data,
    #[serde(rename = "db")]
    Database,
    #[serde(rename = "db_shm")]
    DatabaseSharedMemory,
    #[serde(rename = "db_wal")]
    DatabaseLog,
}

#[derive(Deserialize, JsonSchema)]
pub struct FileSpec {
    pub eid: u32,
    pub file_type: FileType,
}

// Admin API types
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
