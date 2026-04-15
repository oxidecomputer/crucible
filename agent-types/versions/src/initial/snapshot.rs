// Copyright 2026 Oxide Computer Company

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::region::RegionId;

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
    pub state: super::region::State,
}

#[derive(Serialize, JsonSchema)]
pub struct GetSnapshotResponse {
    pub snapshots: Vec<Snapshot>,
    pub running_snapshots: BTreeMap<String, RunningSnapshot>,
}

#[derive(Deserialize, JsonSchema)]
pub struct GetSnapshotPath {
    pub id: RegionId,
    pub name: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct DeleteSnapshotPath {
    pub id: RegionId,
    pub name: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct RunSnapshotPath {
    pub id: RegionId,
    pub name: String,
}
