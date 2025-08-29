// Copyright 2025 Oxide Computer Company

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, JsonSchema)]
pub struct PantryStatus {
    /// Which volumes does this Pantry know about? Note this may include volumes
    /// that are no longer active, and haven't been garbage collected yet.
    pub volumes: Vec<String>,

    /// How many job handles?
    pub num_job_handles: usize,
}

#[derive(Serialize, JsonSchema)]
pub struct VolumeStatus {
    /// Is the Volume currently active?
    pub active: bool,

    /// Has the Pantry ever seen this Volume active?
    pub seen_active: bool,

    /// How many job handles are there for this Volume?
    pub num_job_handles: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedDigest {
    Sha256(String),
}
