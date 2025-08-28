// Copyright 2025 Oxide Computer Company

use crucible_client_types::{ReplaceResult, VolumeConstructionRequest};
use crucible_pantry_types::*;
use dropshot::{
    HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[dropshot::api_description]
pub trait CruciblePantryApi {
    type Context;

    /// Get the Pantry's status
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0",
    }]
    async fn pantry_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<PantryStatus>, HttpError>;

    /// Get a current Volume's status
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn volume_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
    ) -> Result<HttpResponseOk<VolumeStatus>, HttpError>;

    /// Construct a volume from a VolumeConstructionRequest, storing the result in
    /// the Pantry.
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn attach(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<AttachRequest>,
    ) -> Result<HttpResponseOk<AttachResult>, HttpError>;

    /// Construct a volume from a VolumeConstructionRequest, storing the result in
    /// the Pantry. Activate in a separate job so as not to block the request.
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/background",
    }]
    async fn attach_activate_background(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<AttachBackgroundRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Call a volume's target_replace function
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/replace",
    }]
    async fn replace(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<ReplaceRequest>,
    ) -> Result<HttpResponseOk<ReplaceResult>, HttpError>;

    /// Poll to see if a Pantry background job is done
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/job/{id}/is-finished",
    }]
    async fn is_job_finished(
        rqctx: RequestContext<Self::Context>,
        path: Path<JobPath>,
    ) -> Result<HttpResponseOk<JobPollResponse>, HttpError>;

    /// Block on returning a Pantry background job result, then return 200 OK if the
    /// job executed OK, 500 otherwise.
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/job/{id}/ok",
    }]
    async fn job_result_ok(
        rqctx: RequestContext<Self::Context>,
        path: Path<JobPath>,
    ) -> Result<HttpResponseOk<JobResultOkResponse>, HttpError>;

    /// Import data from a URL into a volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/import-from-url",
    }]
    async fn import_from_url(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<ImportFromUrlRequest>,
    ) -> Result<HttpResponseOk<ImportFromUrlResponse>, HttpError>;

    /// Take a snapshot of a volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/snapshot",
    }]
    async fn snapshot(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<SnapshotRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Bulk write data into a volume at a specified offset
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/bulk-write",
    }]
    async fn bulk_write(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<BulkWriteRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Bulk read data from a volume at a specified offset
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/bulk-read",
    }]
    async fn bulk_read(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<BulkReadRequest>,
    ) -> Result<HttpResponseOk<BulkReadResponse>, HttpError>;

    /// Scrub the volume (copy blocks from read-only parent to subvolumes)
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/scrub",
    }]
    async fn scrub(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
    ) -> Result<HttpResponseOk<ScrubResponse>, HttpError>;

    /// Validate the digest of a whole volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/validate",
    }]
    async fn validate(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
        body: TypedBody<ValidateRequest>,
    ) -> Result<HttpResponseOk<ValidateResponse>, HttpError>;

    /// Deactivate a volume, removing it from the Pantry
    #[endpoint {
        method = DELETE,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn detach(
        rqctx: RequestContext<Self::Context>,
        path: Path<VolumePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;
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
