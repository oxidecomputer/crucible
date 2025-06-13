// Copyright 2024 Oxide Computer Company
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    io_limits::{IOLimitView, IOLimits},
    BlockIO, BlockOp, BlockOpWaiter, BlockRes, Buffer, ReadBlockContext,
    ReplaceResult, UpstairsAction,
};
use crucible_client_types::RegionExtentInfo;
use crucible_common::{build_logger, BlockIndex, CrucibleError};
use crucible_protocol::SnapshotDetails;

use async_trait::async_trait;
use bytes::BytesMut;
use slog::{info, warn, Logger};
use tokio::sync::mpsc;
use tracing::{instrument, span, Level};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum GuestBlockRes {
    /// Reads must go into a buffer, and will return that buffer
    Read(Buffer, BlockRes<Buffer, (Buffer, CrucibleError)>),

    /// Other operations send an empty tuple to indicate completion
    Other(BlockRes),

    /// The given job has already been acked
    Acked,
}

impl GuestBlockRes {
    /// Copy data to guest buffers and notify the guest
    ///
    /// The `downstairs_response` must be present if this was a `Read` job.  In
    /// this case, the `&mut BytesMut` argument may be taken (to reduce `memcpy`
    /// overhead).
    #[instrument]
    pub(crate) fn transfer_and_notify(
        self,
        downstairs_response: Option<(&[ReadBlockContext], &mut BytesMut)>,
        result: Result<(), CrucibleError>,
    ) {
        /*
         * If present, send the result to the guest. If this is a flush
         * issued on behalf of crucible, then there is no place to send
         * a result to.
         *
         * XXX: If the guest is no longer listening and this returns an
         * error, do we care?  This could happen if the guest has
         * given up because an IO took too long, or other possible
         * guest side reasons.
         */
        match self {
            GuestBlockRes::Read(mut buffer, res) => {
                if let Some((blocks, data)) = downstairs_response {
                    // XXX don't do if result.is_err()?
                    // Copy over into guest memory.
                    let _ignored =
                        span!(Level::TRACE, "copy to guest buffer").entered();

                    buffer.write_read_response(blocks, data);
                } else {
                    // Should this panic?  If the caller is requesting a
                    // transfer, the guest_buffer should exist. If it does not
                    // exist, then either there is a real problem, or the
                    // operation was a write or flush and why are we requesting
                    // a transfer for those.
                    //
                    // However, dropping a Guest before receiving a downstairs
                    // response will trigger this, so eat it for now.
                }
                match result {
                    Ok(()) => res.send_ok(buffer),
                    Err(e) => res.send_err((buffer, e)),
                }
            }
            GuestBlockRes::Other(res) => {
                // Should we panic if someone provided downstairs_responses?
                res.send_result(result)
            }
            GuestBlockRes::Acked => (),
        }
    }
}

/// IO handles used by the guest to pass work into Crucible proper
///
/// This data structure is the counterpart to the [`GuestIoHandle`], which
/// receives work from the guest and is exclusively owned by the
/// [`upstairs::Upstairs`]
///
/// Requests from the guest are put into the `req_tx` queue by the guest, and
/// received by the [`GuestIoHandle::req_rx`] side.
#[derive(Debug)]
pub struct Guest {
    /// New requests from outside go into this queue
    req_tx: mpsc::Sender<BlockOp>,

    /// Local cache for block size
    ///
    /// This is 0 when unpopulated, and non-zero otherwise; storing it locally
    /// saves a round-trip through the `reqs` queue, and using an atomic means
    /// it can be read from a `&self` reference.
    block_size: AtomicU64,

    /// View into global IO limits
    io_limits: IOLimitView,

    /// Logger for the guest
    log: Logger,
}

/*
 * These methods are how to add or checking for new work on the Guest struct
 */
impl Guest {
    pub fn new(log: Option<Logger>) -> (Guest, GuestIoHandle) {
        let log = log.unwrap_or_else(build_logger);

        // The channel size is chosen arbitrarily here.  The `req_rx` side
        // is running independently and will constantly be processing messages,
        // so we don't expect the queue to become full.  The `req_tx` side is
        // only ever used in `Guest::send`, which waits for acknowledgement from
        // the other side of the queue; there are no places where we put stuff
        // into the queue without awaiting a response.
        //
        // Together, these facts mean that the queue should remain relatively
        // small.  The exception is if someone spawns a zillion tasks, all of
        // which call `Guest` APIs simultaneously.  In that case, having the
        // queue be full will just look like another source of backpressure (and
        // will in fact be invisible to the caller, since they can't distinguish
        // time spent waiting for the queue versus time spent in Upstairs code).
        let (req_tx, req_rx) = mpsc::channel(500);

        // We have to set limits above `IO_OUTSTANDING_MAX_JOBS/BYTES`:
        // an `Offline` downstairs must hit that threshold to transition to
        // `Faulted`, so we can't be IO-limited before that point.
        let io_limits = IOLimits::new(
            crate::IO_OUTSTANDING_MAX_JOBS * 3 / 2,
            crate::IO_OUTSTANDING_MAX_BYTES as usize * 3 / 2,
        );
        let io_limits_view = io_limits.view();

        let io = GuestIoHandle {
            req_rx,

            io_limits,

            #[cfg(test)]
            disable_backpressure: false,

            log: log.clone(),
        };
        let guest = Guest {
            req_tx,

            block_size: AtomicU64::new(0),
            io_limits: io_limits_view,

            log,
        };
        (guest, io)
    }

    /*
     * This is used to submit a new BlockOp IO request to Crucible.
     *
     * It's public for testing, but shouldn't be called
     */
    async fn send(&self, op: BlockOp) {
        if let Err(e) = self.req_tx.send(op).await {
            // This could happen during shutdown, if the up_main task is
            // destroyed while the Guest is still trying to do work.
            //
            // If this happens, then the BlockOpWaiter will immediately return
            // with CrucibleError::RecvDisconnected (since the oneshot::Sender
            // will have been dropped into the void).
            warn!(self.log, "failed to send op to guest: {e}");
        }
    }

    /// Helper function to build a `BlockOp`, send it, and await the result
    async fn send_and_wait<T, F>(&self, f: F) -> Result<T, CrucibleError>
    where
        F: FnOnce(BlockRes<T>) -> BlockOp,
    {
        let (rx, done) = BlockOpWaiter::pair();
        let op = f(done);
        self.send(op).await;
        rx.wait().await
    }

    #[cfg(test)]
    pub async fn downstairs_state(
        &self,
    ) -> Result<crate::ClientData<crate::DsStateTag>, CrucibleError> {
        self.send_and_wait(|done| BlockOp::GetDownstairsState { done })
            .await
    }

    /// Mark a particular downstairs as faulted
    ///
    /// This is used in tests to trigger live-repair
    #[cfg(test)]
    pub async fn fault_downstairs(
        &self,
        client_id: crate::ClientId,
    ) -> Result<(), CrucibleError> {
        self.send_and_wait(|done| BlockOp::FaultDownstairs { client_id, done })
            .await
    }
}

#[async_trait]
impl BlockIO for Guest {
    async fn activate(&self) -> Result<(), CrucibleError> {
        let (rx, done) = BlockOpWaiter::pair();
        self.send(BlockOp::GoActive { done }).await;
        info!(self.log, "The guest has requested activation");

        rx.wait().await?;

        info!(self.log, "The guest has finished waiting for activation");
        Ok(())
    }

    async fn activate_with_gen(&self, gen: u64) -> Result<(), CrucibleError> {
        let (rx, done) = BlockOpWaiter::pair();
        self.send(BlockOp::GoActiveWithGen { gen, done }).await;
        info!(
            self.log,
            "The guest has requested activation with gen:{}", gen
        );

        rx.wait().await?;

        info!(
            self.log,
            "The guest has finished waiting for activation with:{}", gen
        );

        Ok(())
    }

    /// Disable any more IO from this guest and deactivate the downstairs.
    async fn deactivate(&self) -> Result<(), CrucibleError> {
        self.send_and_wait(|done| BlockOp::Deactivate { done })
            .await
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        self.send_and_wait(|done| BlockOp::QueryGuestIOReady { done })
            .await
    }

    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        self.send_and_wait(|done| BlockOp::QueryWorkQueue { done })
            .await
    }
    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError> {
        let ei = self
            .send_and_wait(|done| BlockOp::QueryExtentInfo { done })
            .await?;
        Ok(Some(ei))
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        self.send_and_wait(|done| BlockOp::QueryTotalSize { done })
            .await
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        let bs = self.block_size.load(Ordering::Relaxed);
        if bs == 0 {
            let bs = self
                .send_and_wait(|done| BlockOp::QueryBlockSize { done })
                .await?;

            self.block_size.store(bs, Ordering::Relaxed);
            Ok(bs)
        } else {
            Ok(bs)
        }
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        self.send_and_wait(|done| BlockOp::QueryUpstairsUuid { done })
            .await
    }

    async fn read(
        &self,
        mut offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        let bs = self.check_data_size(data.len()).await?;

        if data.is_empty() {
            return Ok(());
        }

        // We split reads into chunks to bound the maximum (typical) latency of
        // any single `BlockOp::Read`.
        const MDTS: usize = 1024 * 1024; // 1 MiB

        // Leave `data` as a 0-byte buffer rooted at the original address.
        // `buffer` contains data that will be actively processed.
        //
        // [][-------------buffer---------------]
        // ^ data
        let mut buffer = data.split_off(0);

        while !buffer.is_empty() {
            // Split this particular chunk from the front of `buffer:
            //
            // [][-chunk-][--------buffer-------]
            // ^ data
            let num_bytes = MDTS.min(buffer.len());
            assert_eq!(num_bytes % bs as usize, 0);
            let chunk = buffer.split_to(num_bytes / bs as usize);
            assert_eq!(chunk.len() as u64 % bs, 0);

            let offset_change = chunk.len() as u64 / bs;
            let io_guard =
                self.io_limits.claim(chunk.len() as u32).await.map_err(
                    |e| {
                        CrucibleError::IoError(format!(
                            "could not get IO guard for Read: {e:?}"
                        ))
                    },
                )?;
            let (rx, done) = BlockOpWaiter::pair();
            let rio = BlockOp::Read {
                offset,
                data: chunk,
                done,
                io_guard,
            };

            // Our return value always includes the buffer, so we can splice it
            // back onto our existing chunk of data using `unsplit`
            self.send(rio).await;
            let reply = rx.wait_raw().await;
            let err = match reply {
                Some(Ok(buffer)) => {
                    // Reattach the chunk to `data`
                    //
                    // [---data---][--------buffer-------]
                    data.unsplit(buffer);
                    None
                }
                Some(Err((buffer, err))) => {
                    data.unsplit(buffer);
                    Some(err)
                }
                None => Some(CrucibleError::RecvDisconnected),
            };

            // If this is an error, then reattach the rest of the buffer so that
            // the caller doesn't have to reallocate anything.  Otherwise, the
            // buffer will be reattached piece by piece as we loop here.
            if let Some(e) = err {
                data.unsplit(buffer);
                return Err(e);
            }

            offset.0 += offset_change;
        }

        Ok(())
    }

    async fn write(
        &self,
        mut offset: BlockIndex,
        mut data: BytesMut,
    ) -> Result<(), CrucibleError> {
        let bs = self.check_data_size(data.len()).await?;

        if data.is_empty() {
            return Ok(());
        }

        // We split writes into chunks to bound the maximum (typical) latency of
        // any single `BlockOp::Write`.  This makes the system's performance
        // characteristics easier to think about.
        const MDTS: usize = 1024 * 1024; // 1 MiB

        while !data.is_empty() {
            let buf = data.split_to(MDTS.min(data.len()));
            assert_eq!(buf.len() as u64 % bs, 0);
            let offset_change = buf.len() as u64 / bs;

            let io_guard =
                self.io_limits.claim(buf.len() as u32).await.map_err(|e| {
                    CrucibleError::IoError(format!(
                        "could not get IO guard for Write: {e:?}"
                    ))
                })?;

            let reply = self
                .send_and_wait(|done| BlockOp::Write {
                    offset,
                    data: buf,
                    done,
                    io_guard,
                })
                .await;
            reply?;
            offset.0 += offset_change;
        }

        Ok(())
    }

    async fn write_unwritten(
        &self,
        offset: BlockIndex,
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        let _bs = self.check_data_size(data.len()).await?;

        if data.is_empty() {
            return Ok(());
        }

        let io_guard =
            self.io_limits.claim(data.len() as u32).await.map_err(|e| {
                CrucibleError::IoError(format!(
                    "could not get IO guard for WriteUnwritten: {e:?}"
                ))
            })?;
        self.send_and_wait(|done| BlockOp::WriteUnwritten {
            offset,
            data,
            done,
            io_guard,
        })
        .await
    }

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        let io_guard = self.io_limits.claim(0).await.map_err(|e| {
            CrucibleError::IoError(format!(
                "could not get IO guard for flush: {e:?}"
            ))
        })?;
        self.send_and_wait(|done| BlockOp::Flush {
            snapshot_details,
            done,
            io_guard,
        })
        .await
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        // Note: for this implementation, BlockOp::ShowWork will be sent and
        // processed by the Upstairs even if it isn't active.
        self.send_and_wait(|done| BlockOp::ShowWork { done }).await
    }

    async fn replace_downstairs(
        &self,
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
    ) -> Result<ReplaceResult, CrucibleError> {
        self.send_and_wait(|done| BlockOp::ReplaceDownstairs {
            id,
            old,
            new,
            done,
        })
        .await
    }
}

/// Handle for receiving requests from the guest
///
/// This is the counterpart to the [`Guest`], which sends requests.  It includes
/// the receiving side of the request queue, along with infrastructure for
/// bandwidth and IOP limiting.
///
/// In addition, it contains information about the mapping from guest to
/// downstairs data structures, in the form of the [`GuestWork`] map.
///
/// The life-cycle of a request is roughly the following:
///
/// * Pop the request off the reqs queue.
///
/// * Copy (and optionally encrypt) any data buffers provided to us by the
///   Guest.
///
/// * Create one or more downstairs DownstairsIO structures.
///
/// * Create a GtoS tracking structure with the id's for each downstairs task
///   and the read result buffer if required.
///
/// * Add the GtoS struct to the in GuestWork active work hashmap.
///
/// * Put all the DownstairsIO structures on the downstairs work queue
///
/// * Wait for them to complete, then notify the guest through oneshot channels
pub struct GuestIoHandle {
    /// Queue to receive new blockreqs
    req_rx: mpsc::Receiver<BlockOp>,

    /// IO limiting (shared with the `Guest`)
    io_limits: IOLimits,

    /// Log handle, mainly to pass it into the [`Upstairs`]
    pub log: Logger,

    /// Flag to disable backpressure during unit tests
    #[cfg(test)]
    disable_backpressure: bool,
}

impl GuestIoHandle {
    /// Listen for new work, returning the next value from the `BlockOp` queue
    pub(crate) async fn recv(&mut self) -> UpstairsAction {
        if let Some(req) = self.req_rx.recv().await {
            UpstairsAction::Guest(req)
        } else {
            warn!(self.log, "Guest handle has been dropped");
            UpstairsAction::GuestDropped
        }
    }

    #[cfg(test)]
    pub fn disable_backpressure(&mut self) {
        self.disable_backpressure = true;
    }

    #[cfg(test)]
    pub fn is_backpressure_disabled(&self) -> bool {
        self.disable_backpressure
    }

    pub(crate) fn io_limits(&self) -> &IOLimits {
        &self.io_limits
    }
}

/*
 * Work Queue Counts, for debug ShowWork IO type
 */
#[derive(Debug, Copy, Clone)]
pub struct WQCounts {
    pub up_count: usize,
    pub ds_count: usize,
    pub active_count: usize,
}
