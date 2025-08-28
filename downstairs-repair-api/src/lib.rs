// Copyright 2025 Oxide Computer Company

use crucible_common::RegionDefinition;
use crucible_downstairs_types::FileType;
use dropshot::{Body, HttpError, HttpResponseOk, Path, RequestContext};
use hyper::Response;
use schemars::JsonSchema;
use serde::Deserialize;

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
