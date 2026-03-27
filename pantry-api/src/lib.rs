// Copyright 2026 Oxide Computer Company

use crucible_pantry_types_versions::latest;
use dropshot::{
    HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, RequestContext, TypedBody,
};
use dropshot_api_manager_types::api_versions;

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
pub trait CruciblePantryApi {
    type Context;

    /// Get the Pantry's status
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0",
    }]
    async fn pantry_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::pantry::PantryStatus>, HttpError>;

    /// Get a current Volume's status
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn volume_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
    ) -> Result<HttpResponseOk<latest::pantry::VolumeStatus>, HttpError>;

    /// Construct a volume from a VolumeConstructionRequest, storing the result in
    /// the Pantry.
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn attach(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::AttachRequest>,
    ) -> Result<HttpResponseOk<latest::pantry::AttachResult>, HttpError>;

    /// Construct a volume from a VolumeConstructionRequest, storing the result in
    /// the Pantry. Activate in a separate job so as not to block the request.
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/background",
    }]
    async fn attach_activate_background(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::AttachBackgroundRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Call a volume's target_replace function
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/replace",
    }]
    async fn replace(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::ReplaceRequest>,
    ) -> Result<HttpResponseOk<latest::pantry::ReplaceResult>, HttpError>;

    /// Poll to see if a Pantry background job is done
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/job/{id}/is-finished",
    }]
    async fn is_job_finished(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::JobPath>,
    ) -> Result<HttpResponseOk<latest::pantry::JobPollResponse>, HttpError>;

    /// Block on returning a Pantry background job result, then return 200 OK if the
    /// job executed OK, 500 otherwise.
    #[endpoint {
        method = GET,
        path = "/crucible/pantry/0/job/{id}/ok",
    }]
    async fn job_result_ok(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::JobPath>,
    ) -> Result<HttpResponseOk<latest::pantry::JobResultOkResponse>, HttpError>;

    /// Import data from a URL into a volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/import-from-url",
    }]
    async fn import_from_url(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::ImportFromUrlRequest>,
    ) -> Result<HttpResponseOk<latest::pantry::ImportFromUrlResponse>, HttpError>;

    /// Take a snapshot of a volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/snapshot",
    }]
    async fn snapshot(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::SnapshotRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Bulk write data into a volume at a specified offset
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/bulk-write",
    }]
    async fn bulk_write(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::BulkWriteRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Bulk read data from a volume at a specified offset
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/bulk-read",
    }]
    async fn bulk_read(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::BulkReadRequest>,
    ) -> Result<HttpResponseOk<latest::pantry::BulkReadResponse>, HttpError>;

    /// Scrub the volume (copy blocks from read-only parent to subvolumes)
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/scrub",
    }]
    async fn scrub(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
    ) -> Result<HttpResponseOk<latest::pantry::ScrubResponse>, HttpError>;

    /// Validate the digest of a whole volume
    #[endpoint {
        method = POST,
        path = "/crucible/pantry/0/volume/{id}/validate",
    }]
    async fn validate(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
        body: TypedBody<latest::pantry::ValidateRequest>,
    ) -> Result<HttpResponseOk<latest::pantry::ValidateResponse>, HttpError>;

    /// Deactivate a volume, removing it from the Pantry
    #[endpoint {
        method = DELETE,
        path = "/crucible/pantry/0/volume/{id}",
    }]
    async fn detach(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::pantry::VolumePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;
}
