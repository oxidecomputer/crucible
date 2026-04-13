// Copyright 2026 Oxide Computer Company

pub use crucible_agent_types_versions::latest::snapshot::*;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::region::RegionId;

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
