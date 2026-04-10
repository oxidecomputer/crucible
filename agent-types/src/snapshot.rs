// Copyright 2025 Oxide Computer Company

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::region::{RegionId, State};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct Snapshot {
    pub name: String,
    pub created: DateTime<Utc>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct RunningSnapshot {
    pub id: RegionId,
    pub name: String,
    pub port_number: u16,
    pub state: State,
}

pub struct CreateRunningSnapshotRequest {
    pub id: RegionId,
    pub name: String,
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
    pub root_pem: Option<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct DeleteRunningSnapshotRequest {
    pub id: RegionId,
    pub name: String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Serialize, Deserialize, JsonSchema, Debug, PartialEq, Clone)]
pub struct DeleteSnapshotRequest {
    pub id: RegionId,
    pub name: String,
}
