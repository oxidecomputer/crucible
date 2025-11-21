// Copyright 2025 Oxide Computer Company

use std::collections::BTreeMap;

use crucible_agent_types::{
    region::{CreateRegion, Region, RegionId},
    snapshot::{RunningSnapshot, Snapshot},
};
use dropshot::{
    HttpError, HttpResponseDeleted, HttpResponseOk, Path, RequestContext,
    TypedBody,
};
use dropshot_api_manager_types::api_versions;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

api_versions!([
    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (1, INITIAL)
//
// Then you could use `VERSION_INITIAL` as the version in which endpoints were
// added or removed.

#[dropshot::api_description]
pub trait CrucibleAgentApi {
    type Context;

    #[endpoint {
        method = GET,
        path = "/crucible/0/regions",
    }]
    async fn region_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<Region>>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/crucible/0/regions",
    }]
    async fn region_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<CreateRegion>,
    ) -> Result<HttpResponseOk<Region>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/crucible/0/regions/{id}",
    }]
    async fn region_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<RegionPath>,
    ) -> Result<HttpResponseOk<Region>, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/crucible/0/regions/{id}",
    }]
    async fn region_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<RegionPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = GET,
        path = "/crucible/0/regions/{id}/snapshots",
    }]
    async fn region_get_snapshots(
        rqctx: RequestContext<Self::Context>,
        path: Path<RegionPath>,
    ) -> Result<HttpResponseOk<GetSnapshotResponse>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/crucible/0/regions/{id}/snapshots/{name}",
    }]
    async fn region_get_snapshot(
        rqctx: RequestContext<Self::Context>,
        path: Path<GetSnapshotPath>,
    ) -> Result<HttpResponseOk<Snapshot>, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/crucible/0/regions/{id}/snapshots/{name}",
    }]
    async fn region_delete_snapshot(
        rqctx: RequestContext<Self::Context>,
        path: Path<DeleteSnapshotPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = POST,
        path = "/crucible/0/regions/{id}/snapshots/{name}/run",
    }]
    async fn region_run_snapshot(
        rqctx: RequestContext<Self::Context>,
        path: Path<RunSnapshotPath>,
    ) -> Result<HttpResponseOk<RunningSnapshot>, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/crucible/0/regions/{id}/snapshots/{name}/run",
    }]
    async fn region_delete_running_snapshot(
        rc: RequestContext<Self::Context>,
        path: Path<RunSnapshotPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;
}

#[derive(Deserialize, JsonSchema)]
pub struct RegionPath {
    pub id: RegionId,
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
