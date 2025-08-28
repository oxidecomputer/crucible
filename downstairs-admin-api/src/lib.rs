// Copyright 2025 Oxide Computer Company

use crucible_downstairs_types::RunDownstairsForRegionParams;
use dropshot::{
    HttpError, HttpResponseCreated, Path, RequestContext, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// API trait for the downstairs admin server.
#[dropshot::api_description]
pub trait CrucibleDownstairsAdminApi {
    type Context;

    /// Start a downstairs instance for a specific region.
    #[endpoint {
        method = POST,
        path = "/regions/{uuid}/downstairs"
    }]
    async fn run_downstairs_for_region(
        rqctx: RequestContext<Self::Context>,
        path_param: Path<RunDownstairsForRegionPath>,
        run_params: TypedBody<RunDownstairsForRegionParams>,
    ) -> Result<HttpResponseCreated<DownstairsRunningResponse>, HttpError>;
}

#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsForRegionPath {
    pub uuid: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct DownstairsRunningResponse {
    pub uuid: Uuid,
}
