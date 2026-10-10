// Copyright 2026 Oxide Computer Company

use crucible_client_types::VolumeConstructionRequest;
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

#[derive(Deserialize, JsonSchema)]
pub struct VolumePath {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct AttachRequest {
    pub volume_construction_request: VolumeConstructionRequest,
}

#[derive(Serialize, JsonSchema)]
pub struct AttachResult {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct AttachBackgroundRequest {
    pub volume_construction_request: VolumeConstructionRequest,
    pub job_id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ReplaceRequest {
    pub volume_construction_request: VolumeConstructionRequest,
}

#[derive(Deserialize, JsonSchema)]
pub struct JobPath {
    pub id: String,
}

#[derive(Serialize, JsonSchema)]
pub struct JobPollResponse {
    pub job_is_finished: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct JobResultOkResponse {
    pub job_result_ok: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct ImportFromUrlRequest {
    pub url: String,
    pub expected_digest: Option<ExpectedDigest>,
}

#[derive(Serialize, JsonSchema)]
pub struct ImportFromUrlResponse {
    pub job_id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct SnapshotRequest {
    pub snapshot_id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct BulkWriteRequest {
    pub offset: u64,
    pub base64_encoded_data: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct BulkReadRequest {
    pub offset: u64,
    pub size: usize,
}

#[derive(Serialize, JsonSchema)]
pub struct BulkReadResponse {
    pub base64_encoded_data: String,
}

#[derive(Serialize, JsonSchema)]
pub struct ScrubResponse {
    pub job_id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ValidateRequest {
    pub expected_digest: ExpectedDigest,

    // Size to validate in bytes, starting from offset 0. If not specified, the
    // total volume size is used.
    pub size_to_validate: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct ValidateResponse {
    pub job_id: String,
}
