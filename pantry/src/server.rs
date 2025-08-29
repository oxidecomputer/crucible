// Copyright 2025 Oxide Computer Company
use super::pantry::Pantry;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use base64::{engine, Engine};
use crucible_pantry_api::*;
use crucible_pantry_types::*;
use dropshot::{
    HandlerTaskMode, HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path as TypedPath, RequestContext, TypedBody,
};
use slog::{info, o, Logger};
use std::result::Result as SResult;

#[derive(Debug)]
pub(crate) struct CruciblePantryImpl;

impl CruciblePantryApi for CruciblePantryImpl {
    type Context = Arc<Pantry>;

    async fn pantry_status(
        rqctx: RequestContext<Self::Context>,
    ) -> SResult<HttpResponseOk<PantryStatus>, HttpError> {
        let pantry = rqctx.context();

        let status = pantry.status().await?;

        Ok(HttpResponseOk(status))
    }

    async fn volume_status(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
    ) -> SResult<HttpResponseOk<VolumeStatus>, HttpError> {
        let path = path.into_inner();
        let pantry = rqctx.context();

        let status = pantry.volume_status(path.id.clone()).await?;

        Ok(HttpResponseOk(status))
    }

    async fn attach(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<AttachRequest>,
    ) -> SResult<HttpResponseOk<AttachResult>, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        pantry
            .attach(path.id.clone(), body.volume_construction_request)
            .await?;

        Ok(HttpResponseOk(AttachResult { id: path.id }))
    }

    async fn attach_activate_background(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<AttachBackgroundRequest>,
    ) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        pantry
            .attach_activate_background(
                path.id.clone(),
                body.job_id,
                body.volume_construction_request,
            )
            .await?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn replace(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<ReplaceRequest>,
    ) -> SResult<HttpResponseOk<crucible::ReplaceResult>, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        let result = pantry
            .replace(path.id.clone(), body.volume_construction_request)
            .await?;

        Ok(HttpResponseOk(result))
    }

    async fn is_job_finished(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<JobPath>,
    ) -> SResult<HttpResponseOk<JobPollResponse>, HttpError> {
        let path = path.into_inner();
        let pantry = rqctx.context();

        let job_is_finished = pantry.is_job_finished(path.id).await?;

        Ok(HttpResponseOk(JobPollResponse { job_is_finished }))
    }

    async fn job_result_ok(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<JobPath>,
    ) -> SResult<HttpResponseOk<JobResultOkResponse>, HttpError> {
        let path = path.into_inner();
        let pantry = rqctx.context();

        match pantry.get_job_result(path.id).await {
            Ok(result) => {
                // The inner result is from the tokio task itself.
                Ok(HttpResponseOk(JobResultOkResponse {
                    job_result_ok: result.is_ok(),
                }))
            }

            // Here is where get_job_result will return 404 if the job id is not
            // found, or a 500 if the join_handle.await didn't work.
            Err(e) => Err(e),
        }
    }

    async fn import_from_url(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<ImportFromUrlRequest>,
    ) -> SResult<HttpResponseOk<ImportFromUrlResponse>, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        let job_id = pantry
            .import_from_url(path.id.clone(), body.url, body.expected_digest)
            .await?;

        Ok(HttpResponseOk(ImportFromUrlResponse { job_id }))
    }

    async fn snapshot(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<SnapshotRequest>,
    ) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        pantry.snapshot(path.id.clone(), body.snapshot_id).await?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn bulk_write(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<BulkWriteRequest>,
    ) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        let data = engine::general_purpose::STANDARD
            .decode(body.base64_encoded_data)
            .map_err(|e| HttpError::for_bad_request(None, e.to_string()))?;

        pantry
            .bulk_write(path.id.clone(), body.offset, data)
            .await?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn bulk_read(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<BulkReadRequest>,
    ) -> SResult<HttpResponseOk<BulkReadResponse>, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        let data = pantry
            .bulk_read(path.id.clone(), body.offset, body.size)
            .await?;

        Ok(HttpResponseOk(BulkReadResponse {
            base64_encoded_data: engine::general_purpose::STANDARD.encode(data),
        }))
    }

    async fn scrub(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
    ) -> SResult<HttpResponseOk<ScrubResponse>, HttpError> {
        let path = path.into_inner();
        let pantry = rqctx.context();

        let job_id = pantry.scrub(path.id.clone()).await?;

        Ok(HttpResponseOk(ScrubResponse { job_id }))
    }

    async fn validate(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
        body: TypedBody<ValidateRequest>,
    ) -> SResult<HttpResponseOk<ValidateResponse>, HttpError> {
        let path = path.into_inner();
        let body = body.into_inner();
        let pantry = rqctx.context();

        let job_id = pantry
            .validate(
                path.id.clone(),
                body.expected_digest,
                body.size_to_validate,
            )
            .await?;

        Ok(HttpResponseOk(ValidateResponse { job_id }))
    }

    async fn detach(
        rqctx: RequestContext<Self::Context>,
        path: TypedPath<VolumePath>,
    ) -> SResult<HttpResponseDeleted, HttpError> {
        let path = path.into_inner();
        let pantry = rqctx.context();

        pantry.detach(path.id).await?;

        Ok(HttpResponseDeleted())
    }
}

pub fn run_server(
    log: &Logger,
    bind_address: SocketAddr,
    df: &Arc<Pantry>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<(), String>>)> {
    let api = crucible_pantry_api_mod::api_description::<CruciblePantryImpl>()?;

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address,
            // max import, multiplied by worst case base64 overhead, plus room
            // for metadata
            default_request_body_max_bytes: 1024
                + crate::pantry::PantryEntry::MAX_CHUNK_SIZE * 2,
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
        },
        api,
        df.clone(),
        &log.new(o!("component" => "dropshot")),
    )
    .map_err(|e| anyhow!("creating server: {:?}", e))?
    .start();

    let local_addr = server.local_addr();
    info!(log, "listen IP: {:?}", local_addr);

    let join_handle = tokio::spawn(server);

    Ok((local_addr, join_handle))
}
