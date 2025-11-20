// Copyright 2025 Oxide Computer Company

//! API traits for Crucible downstairs operations.

use crucible_common::RegionDefinition;
use crucible_downstairs_types::{FileType, RunDownstairsForRegionParams};
use dropshot::{
    Body, HttpError, HttpResponseCreated, HttpResponseOk, Path, RequestContext,
    TypedBody,
};
use dropshot_api_manager_types::api_versions;
use hyper::Response;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// API trait for the downstairs repair server.
#[dropshot::api_description]
pub trait CrucibleDownstairsRepairApi {
    type Context;

    /// Get a specific extent file (data, database, or log files).
    #[endpoint {
        method = GET,
        path = "/newextent/{eid}/{file_type}",
    }]
    async fn get_extent_file(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentFilePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Return true if the provided extent is closed or the region is read only.
    #[endpoint {
        method = GET,
        path = "/extent/{eid}/repair-ready",
    }]
    async fn extent_repair_ready(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentPath>,
    ) -> Result<HttpResponseOk<bool>, HttpError>;

    /// Get the list of files related to an extent.
    ///
    /// For a given extent, return a vec of strings representing the names of
    /// the files that exist for that extent.
    #[endpoint {
        method = GET,
        path = "/extent/{eid}/files",
    }]
    async fn get_files_for_extent(
        rqctx: RequestContext<Self::Context>,
        path: Path<ExtentPath>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError>;

    /// Return the RegionDefinition describing our region.
    #[endpoint {
        method = GET,
        path = "/region-info",
    }]
    async fn get_region_info(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RegionDefinition>, HttpError>;

    /// Return the region-mode describing our region.
    #[endpoint {
        method = GET,
        path = "/region-mode",
    }]
    async fn get_region_mode(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<bool>, HttpError>;

    /// Work queue
    #[endpoint {
        method = GET,
        path = "/work",
    }]
    async fn get_work(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<bool>, HttpError>;
}

// Admin API types
#[derive(Deserialize, JsonSchema)]
pub struct RunDownstairsForRegionPath {
    pub uuid: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct DownstairsRunningResponse {
    pub uuid: Uuid,
}

// Repair API types
#[derive(Deserialize, JsonSchema)]
pub struct ExtentPath {
    pub eid: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct ExtentFilePath {
    pub eid: u32,
    pub file_type: FileType,
}

#[derive(Deserialize, JsonSchema)]
pub struct JobPath {
    pub id: String,
}
