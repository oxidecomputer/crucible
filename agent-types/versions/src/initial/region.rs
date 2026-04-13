// Copyright 2026 Oxide Computer Company

use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Requested,
    Created,
    Tombstoned,
    Destroyed,
    Failed,
}

// If not provided, select None as the default for source.
fn source_default() -> Option<SocketAddr> {
    None
}

// If not provided, select false as the default for read only.
fn read_only_default() -> bool {
    false
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct Region {
    pub id: RegionId,
    pub state: State,

    // Creation parameters
    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u32,
    pub encrypted: bool,

    // Run-time parameters
    pub port_number: u16,
    pub cert_pem: Option<String>,

    // TODO should skip serializing this on list regions response, but this
    // causes crucible.json to not have it
    // #[serde(skip_serializing)]
    pub key_pem: Option<String>,

    pub root_pem: Option<String>,
    // If this region was created as part of a clone.
    #[serde(default = "source_default")]
    pub source: Option<SocketAddr>,

    // If this region is read only
    #[serde(default = "read_only_default")]
    pub read_only: bool,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct CreateRegion {
    pub id: RegionId,

    pub block_size: u64,
    pub extent_size: u64,
    pub extent_count: u32,
    pub encrypted: bool,

    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_pem: Option<String>,
    // TODO base64 encoded der too?
    /// If requested, copy the extent contents from the provided IP:Port
    ///
    /// Regions created from a source will be started read_only
    pub source: Option<SocketAddr>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(
    Serialize,
    Deserialize,
    JsonSchema,
    Debug,
    PartialEq,
    Eq,
    Clone,
    PartialOrd,
    Ord,
)]
pub struct RegionId(pub String);

#[derive(Deserialize, JsonSchema)]
pub struct RegionPath {
    pub id: RegionId,
}
