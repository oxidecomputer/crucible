// Copyright 2026 Oxide Computer Company

use schemars::JsonSchema;
use serde::Serialize;

#[derive(Serialize, JsonSchema)]
pub struct VolumeStatus {
    /// Is the Volume currently active?
    pub active: bool,

    /// Has the Pantry ever seen this Volume active?
    pub seen_active: bool,

    /// How many job handles are there for this Volume?
    pub num_job_handles: usize,

    pub info: crucible_client_types::VolumeInfo,
}
