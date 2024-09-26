// Copyright 2024 Oxide Computer Company
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::{
    backpressure::{
        BackpressureAmount, BackpressureConfig, SharedBackpressureAmount,
    },
    BlockIO, BlockOp, BlockOpWaiter, BlockRes, Buffer, JobId, RawReadResponse,
    ReplaceResult, UpstairsAction,
};
use crucible_common::{build_logger, BlockIndex, CrucibleError, ExtentInfo};
use crucible_protocol::SnapshotDetails;

use async_trait::async_trait;
use bytes::BytesMut;
use ringbuffer::{AllocRingBuffer, RingBuffer};
use slog::{error, info, warn, Logger};
use tokio::sync::{mpsc, Mutex};
use tracing::{instrument, span, Level};
use uuid::Uuid;

/*
 * This structure is for tracking the underlying storage side operations
 * that map to a single Guest IO request. G to S stands for Guest
 * to Storage.
 *
 * The submitted hashmap is indexed by the request number (ds_id) for the
 * downstairs requests issued on behalf of this request.
 */
#[derive(Debug)]
struct GtoS {
    /*
     * Job we sent on to the downstairs.
     */
    ds_id: JobId,

    /*
     * Handle to notify the guest when we're done
     *
     * This is an Option for the case where we want to send an IO on behalf
     * of the Upstairs (not guest driven). Right now the only case where we
     * need that is to flush data to downstairs when the guest has not sent
     * us a flush in some time.  This allows us to free internal buffers.
     * If the sender is None, we know it's a request from the Upstairs and
     * we don't have to ACK it to anyone.
     */
    res: Option<GuestBlockRes>,
}

#[derive(Debug)]
pub(crate) enum GuestBlockRes {
    /// Reads must go into a buffer, and will return that buffer
    Read(Buffer, BlockRes<Buffer, (Buffer, CrucibleError)>),

    /// Other operations send an empty tuple to indicate completion
    Other(BlockRes),

    /// The given job has already been acked
    Acked,
}

impl GtoS {
    /// Create a new GtoS object where one Guest IO request maps to one
    /// downstairs operation.
    pub fn new(ds_id: JobId, res: Option<GuestBlockRes>) -> GtoS {
        GtoS { ds_id, res }
    }

    /*
     * When all downstairs jobs have completed, and all buffers have been
     * attached to the GtoS struct, we can do the final copy of the data
     * from upstairs memory back to the guest's memory. Notify corresponding
     * BlockOpWaiter if required
     */
    #[instrument]
    fn transfer_and_notify(
        self,
        downstairs_response: Option<RawReadResponse>,
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
        match self.res {
            Some(GuestBlockRes::Read(mut buffer, res)) => {
                if let Some(downstairs_response) = downstairs_response {
                    // XXX don't do if result.is_err()?
                    // Copy over into guest memory.
                    let _ignored =
                        span!(Level::TRACE, "copy to guest buffer").entered();

                    buffer.write_read_response(downstairs_response);
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
            Some(GuestBlockRes::Other(res)) => {
                // Should we panic if someone provided downstairs_responses?
                res.send_result(result)
            }
            Some(GuestBlockRes::Acked) => (),
            None => (),
        }
    }
}

/// Strongly-typed ID for guest work (stored in the [`GuestWork`] map)
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct GuestWorkId(pub u64);

impl std::fmt::Display for GuestWorkId {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/**
 * This structure keeps track of work that Crucible has accepted from the
 * "Guest", aka, Propolis.
 *
 * The active is a hashmap of GtoS structures for all I/Os that are
 * outstanding. Either just created or in progress operations. The key
 * for a new job comes from next_gw_id and should always increment.
 *
 * Once we have decided enough downstairs requests are finished, we remove
 * the entry from the active and add the gw_id to the completed vec.
 *
 * TODO: The completed needs to implement some notify back to the Guest, and
 * it should probably be a ring buffer.
 */
#[derive(Debug)]
pub struct GuestWork {
    active: HashMap<GuestWorkId, GtoS>,
    next_gw_id: u64,
    completed: AllocRingBuffer<GuestWorkId>,
}

impl GuestWork {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    pub fn len(&self) -> usize {
        self.active.len()
    }

    /// Helper function to install new work into the map
    pub(crate) fn submit_job<F: FnOnce(GuestWorkId) -> JobId>(
        &mut self,
        f: F,
        res: Option<GuestBlockRes>,
    ) -> (GuestWorkId, JobId) {
        let gw_id = self.next_gw_id();
        let ds_id = f(gw_id);

        if matches!(res, Some(GuestBlockRes::Acked)) {
            self.completed.push(gw_id);
        } else {
            self.active.insert(gw_id, GtoS::new(ds_id, res));
        }
        (gw_id, ds_id)
    }

    /// Low-level function to get next guest work ID
    ///
    /// Normally, `submit_job` should be called instead; this function should
    /// only be used to reserve `GuestWorkId`s in advance of submitting the
    /// jobs.
    pub(crate) fn next_gw_id(&mut self) -> GuestWorkId {
        let id = self.next_gw_id;
        self.next_gw_id += 1;
        GuestWorkId(id)
    }

    /// Low-level function to insert work into the map
    ///
    /// Normally, `submit_job` should be called instead; this function should
    /// only be used if we have reserved the `GuestWorkId` and `JobId` in
    /// advance.
    pub(crate) fn insert(&mut self, gw_id: GuestWorkId, ds_id: JobId) {
        let new_gtos = GtoS::new(ds_id, None);
        self.active.insert(gw_id, new_gtos);
    }

    /*
     * When the required number of completions for a downstairs
     * ds_id have arrived, we call this method on the parent GuestWork
     * that requested them and include the Option<Bytes> from the IO.
     *
     * If this operation was a read, then we attach the Bytes read to the
     * GtoS struct for later transfer.
     *
     * A single GtoS job may have multiple downstairs jobs it created, so
     * we may not be done yet. When the required number of completions have
     * arrived from all the downstairs jobs we created, then we
     * can move forward with finishing up the guest work operation.
     * This may include moving data buffers from completed reads.
     */
    #[instrument]
    pub(crate) fn gw_ds_complete(
        &mut self,
        gw_id: GuestWorkId,
        ds_id: JobId,
        data: Option<RawReadResponse>,
        result: Result<(), CrucibleError>,
    ) {
        if let Some(gtos_job) = self.active.remove(&gw_id) {
            assert_eq!(gtos_job.ds_id, ds_id);

            /*
             * Copy (if present) read data back to the guest buffer they
             * provided to us, and notify any waiters.
             */
            gtos_job.transfer_and_notify(data, result);

            self.completed.push(gw_id);
        } else {
            /*
             * XXX This is just so I can see if ever does happen.
             */
            panic!("gw_id {} for job {} not on active list", gw_id, ds_id);
        }
    }

    pub fn print_last_completed(&self, n: usize) {
        print!("Upstairs last five completed:  ");
        for j in self.completed.iter().rev().take(n) {
            print!(" {:4}", j);
        }
    }
}

impl Default for GuestWork {
    fn default() -> Self {
        Self {
            active: HashMap::new(), // GtoS
            next_gw_id: 1,
            completed: AllocRingBuffer::new(2048),
        }
    }
}

/// IO handles used by the guest uses to pass work into Crucible proper
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

    /// Backpressure is implemented as a delay on host write operations
    ///
    /// It is stored in an `Arc` so that the `GuestIoHandle` can update it from
    /// the IO task.
    backpressure: SharedBackpressureAmount,

    /// Lock held during backpressure delay
    ///
    /// Without this lock, multiple tasks could submit jobs to the upstairs and
    /// wait in parallel, which defeats the purpose of backpressure (since you
    /// could send arbitrarily many jobs at high speed by sending them from
    /// different tasks).
    backpressure_lock: Mutex<()>,

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

        let backpressure = SharedBackpressureAmount::new();
        let limits = GuestLimits {
            iop_limit: None,
            bw_limit: None,
        };
        let io = GuestIoHandle {
            req_rx,
            req_head: None,
            req_limited: false,
            limits,

            guest_work: GuestWork {
                active: HashMap::new(), // GtoS
                next_gw_id: 1,
                completed: AllocRingBuffer::new(2048),
            },

            iop_tokens: 0,
            bw_tokens: 0,
            backpressure: backpressure.clone(),
            backpressure_config: BackpressureConfig::default(),
            log: log.clone(),
        };
        let guest = Guest {
            req_tx,

            block_size: AtomicU64::new(0),

            backpressure,
            backpressure_lock: Mutex::new(()),
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

    pub async fn query_extent_info(
        &self,
    ) -> Result<Vec<ExtentInfo>, CrucibleError> {
        let block = self
            .send_and_wait(|done| BlockOp::QueryExtentInfo { done })
            .await?;

        Ok(vec![block])
    }

    pub async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        self.send_and_wait(|done| BlockOp::QueryWorkQueue { done })
            .await
    }

    // Maybe this can just be a guest specific thing, not a BlockIO
    pub async fn activate_with_gen(
        &self,
        gen: u64,
    ) -> Result<(), CrucibleError> {
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

    /// Sleeps for a backpressure-dependent amount, holding the lock
    ///
    /// If backpressure is saturated, logs and returns an error.
    async fn backpressure_sleep(&self) -> Result<(), CrucibleError> {
        let bp = self.backpressure.load();
        match bp {
            BackpressureAmount::Saturated => {
                let err = "write queue is saturated";
                error!(self.log, "{err}");
                Err(CrucibleError::IoError(err.to_owned()))
            }
            BackpressureAmount::Duration(d) => {
                if d > Duration::ZERO {
                    let _guard = self.backpressure_lock.lock().await;
                    tokio::time::sleep(d).await;
                    drop(_guard);
                }
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub async fn downstairs_state(
        &self,
    ) -> Result<crate::ClientData<crate::DsState>, CrucibleError> {
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
    ) -> Result<Vec<ExtentInfo>, CrucibleError> {
        let block = self
            .send_and_wait(|done| BlockOp::QueryExtentInfo { done })
            .await?;

        Ok(vec![block])
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
            let (rx, done) = BlockOpWaiter::pair();
            let rio = BlockOp::Read {
                offset,
                data: chunk,
                done,
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
        // any single `BlockOp::Write`.  Otherwise, the host could send writes
        // which are large enough that our maximum backpressure delay wouldn't
        // compensate for them.
        const MDTS: usize = 1024 * 1024; // 1 MiB

        while !data.is_empty() {
            let buf = data.split_to(MDTS.min(data.len()));
            assert_eq!(buf.len() as u64 % bs, 0);
            let offset_change = buf.len() as u64 / bs;

            self.backpressure_sleep().await?;

            let reply = self
                .send_and_wait(|done| BlockOp::Write {
                    offset,
                    data: buf,
                    done,
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

        self.backpressure_sleep().await?;
        self.send_and_wait(|done| BlockOp::WriteUnwritten {
            offset,
            data,
            done,
        })
        .await
    }

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        self.send_and_wait(|done| BlockOp::Flush {
            snapshot_details,
            done,
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

/// Configuration for iops-per-second limiting
#[derive(Copy, Clone, Debug)]
pub struct IopLimit {
    bytes_per_iop: usize,
    iop_limit: usize,
}

/// Configuration for guest limits
#[derive(Copy, Clone, Debug)]
pub struct GuestLimits {
    iop_limit: Option<IopLimit>,
    bw_limit: Option<usize>,
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

    /// Guest IO and bandwidth limits
    limits: GuestLimits,

    /// `BlockOp` that is at the head of the queue
    ///
    /// If a `BlockOp` was pulled from the queue but couldn't be used due to
    /// IOP or bandwidth limiting, it's stored here instead (and we check this
    /// before awaiting the queue).
    req_head: Option<BlockOp>,

    /// Are we currently IOP or bandwidth limited?
    ///
    /// If so, we don't return anything in `recv()`
    req_limited: bool,

    /// Current number of IOP tokens
    iop_tokens: usize,

    /// Current backpressure (shared with the `Guest`)
    backpressure: SharedBackpressureAmount,

    /// Backpressure configuration, as a starting point and max delay
    backpressure_config: BackpressureConfig,

    /// Bandwidth tokens (in bytes)
    bw_tokens: usize,

    /// Active work from the guest
    ///
    /// When the crucible listening task has noticed a new IO request, it
    /// will pull it from the reqs queue and create an GuestWork struct
    /// as well as convert the new IO request into the matching
    /// downstairs request(s). Each new GuestWork request will get a
    /// unique gw_id, which is also the index for that operation into the
    /// hashmap.
    ///
    /// It is during this process that data will encrypted. For a read, the
    /// data is decrypted back to the guest provided buffer after all the
    /// required downstairs operations are completed.
    pub guest_work: GuestWork,

    /// Log handle, mainly to pass it into the [`Upstairs`]
    pub log: Logger,
}

impl GuestIoHandle {
    pub fn iop_tokens(&self) -> usize {
        self.iop_tokens
    }

    /// Leaks IOP and BW tokens
    pub fn leak_check(&mut self, leak_ms: usize) {
        if let Some(iop_limit_cfg) = self.limits.iop_limit {
            let tokens = iop_limit_cfg.iop_limit / (1000 / leak_ms);
            self.leak_iop_tokens(tokens);
        }

        if let Some(bw_limit) = self.limits.bw_limit {
            let tokens = bw_limit / (1000 / leak_ms);
            self.leak_bw_tokens(tokens);
        }
    }

    /// Leak IOPs tokens
    fn leak_iop_tokens(&mut self, tokens: usize) {
        self.iop_tokens = self.iop_tokens.saturating_sub(tokens);
        self.req_limited = false;
    }

    /// Leak bytes from bandwidth tokens
    fn leak_bw_tokens(&mut self, bytes: usize) {
        self.bw_tokens = self.bw_tokens.saturating_sub(bytes);
        self.req_limited = false;
    }

    /// Listen for new work
    ///
    /// This will wait forever if we are currently IOP / BW limited; otherwise,
    /// it will return the next value from the `BlockOp` queue.
    ///
    /// To avoid being stuck forever, this function should be called as **a
    /// branch** of a `select!` statement that _also_ includes at least one
    /// timeout; we should use that timeout to periodically service the IOP / BW
    /// token counters, which will unblock the `GuestIoHandle` in future calls.
    pub(crate) async fn recv(&mut self) -> UpstairsAction {
        let req = if self.req_limited {
            futures::future::pending().await
        } else if let Some(req) = self.req_head.take() {
            req
        } else if let Some(req) = self.req_rx.recv().await {
            // NOTE: once we take this req from the queue, we must be cancel
            // safe!  In other words, we cannot yield until either (1) returning
            // the req or (2) storing it in self.req_head for safe-keeping.
            req
        } else {
            warn!(self.log, "Guest handle has been dropped");
            return UpstairsAction::GuestDropped;
        };

        // Check if we can consume right away
        let iop_limit_applies =
            self.limits.iop_limit.is_some() && req.consumes_iops();
        let bw_limit_applies =
            self.limits.bw_limit.is_some() && req.sz().is_some();

        if !iop_limit_applies && !bw_limit_applies {
            return UpstairsAction::Guest(req);
        }

        // Check bandwidth limit before IOP limit, but make sure only to consume
        // tokens if both checks pass!

        let mut bw_check_ok = true;
        let mut iop_check_ok = true;

        // When checking tokens vs the limit, do not check by checking if adding
        // the block request's values to the applicable limit: this would create
        // a scenario where a large IO enough would stall the pipeline (see
        // test_impossible_io). Instead, check if the limits are already
        // reached.

        if let Some(bw_limit) = self.limits.bw_limit {
            if req.sz().is_some() && self.bw_tokens >= bw_limit {
                bw_check_ok = false;
            }
        }

        if let Some(iop_limit_cfg) = &self.limits.iop_limit {
            let bytes_per_iops = iop_limit_cfg.bytes_per_iop;
            if req.iops(bytes_per_iops).is_some()
                && self.iop_tokens >= iop_limit_cfg.iop_limit
            {
                iop_check_ok = false;
            }
        }

        // If both checks pass, consume appropriate resources and return the
        // block req
        if bw_check_ok && iop_check_ok {
            if self.limits.bw_limit.is_some() {
                if let Some(sz) = req.sz() {
                    self.bw_tokens += sz;
                }
            }

            if let Some(cfg) = &self.limits.iop_limit {
                if let Some(req_iops) = req.iops(cfg.bytes_per_iop) {
                    self.iop_tokens += req_iops;
                }
            }

            UpstairsAction::Guest(req)
        } else {
            assert!(self.req_head.is_none());
            self.req_head = Some(req);
            futures::future::pending().await
        }
    }

    #[cfg(test)]
    pub fn disable_queue_backpressure(&mut self) {
        self.backpressure_config.queue.delay_scale = Duration::ZERO;
    }

    #[cfg(test)]
    pub fn disable_byte_backpressure(&mut self) {
        self.backpressure_config.bytes.delay_scale = Duration::ZERO;
    }

    #[cfg(test)]
    pub fn is_queue_backpressure_disabled(&self) -> bool {
        self.backpressure_config.queue.delay_scale == Duration::ZERO
    }

    /// Set `self.backpressure` based on outstanding IO ratio
    pub fn set_backpressure(&self, bytes: u64, jobs: u64) {
        let bp = self.backpressure_config.get_backpressure(bytes, jobs);
        self.backpressure.store(bp);
    }

    pub fn set_iop_limit(&mut self, bytes_per_iop: usize, limit: usize) {
        self.limits.iop_limit = Some(IopLimit {
            bytes_per_iop,
            iop_limit: limit,
        });
    }

    pub fn set_bw_limit(&mut self, bytes_per_second: usize) {
        self.limits.bw_limit = Some(bytes_per_second);
    }

    /// Returns the number of active jobs
    pub fn active_count(&self) -> usize {
        self.guest_work.active.len()
    }

    /// Looks up current backpressure
    pub fn get_backpressure(&self) -> BackpressureAmount {
        self.backpressure.load()
    }

    /// Debug function to dump the guest work structure.
    ///
    /// TODO: make this one big dump, where we include the up.work.active
    /// printing for each guest_work. It will be much more dense, but require
    /// holding both locks for the duration.
    pub(crate) fn show_work(&self) -> usize {
        println!("Guest work:  Active and Completed Jobs:");
        let gw = &self.guest_work;
        let mut kvec: Vec<_> = gw.active.keys().cloned().collect();
        kvec.sort_unstable();
        for id in kvec.iter() {
            let job = gw.active.get(id).unwrap();
            println!("GW_JOB active:[{:04}] D:{:?} ", id, job.ds_id);
        }
        let done = gw.completed.to_vec();
        println!("GW_JOB completed count:{:?} ", done.len());
        kvec.len()
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

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;

    async fn assert_consumed(io: &mut GuestIoHandle) {
        tokio::select! {
            _ = io.recv() => {
                // correct!
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {
                panic!("timed out while waiting for message");
            }
        }
    }

    async fn assert_none_consumed(io: &mut GuestIoHandle) {
        tokio::select! {
            _ = io.recv() => {
                panic!("got message when expecting nothing")
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {
                // nothing to do here
            }
        }
    }

    #[tokio::test]
    async fn test_no_iop_limit() -> Result<()> {
        let (guest, mut io) = Guest::new(None);
        assert_none_consumed(&mut io).await;

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(1, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(8, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(32, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;

        // With no IOP limit, all requests are consumed immediately
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;

        assert_none_consumed(&mut io).await;

        // If no IOP limit set, don't track it
        assert_eq!(io.iop_tokens, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_iop_limit() -> Result<()> {
        let (guest, mut io) = Guest::new(None);
        io.set_iop_limit(16000, 2);

        assert_none_consumed(&mut io).await;

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(1, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(8, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(31, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;

        // First two reads succeed
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;

        // Next cannot be consumed until there's available IOP tokens so it
        // remains in the queue.  Strictly speaking, it's been popped to the
        // `req_head` position.
        assert_none_consumed(&mut io).await;
        assert!(io.req_rx.try_recv().is_err());
        assert_eq!(io.iop_tokens, 2);
        assert!(io.req_head.is_some());

        // Replenish one token, meaning next read can be consumed
        io.leak_iop_tokens(1);
        assert_eq!(io.iop_tokens, 1);

        assert_consumed(&mut io).await;
        assert!(io.req_rx.try_recv().is_err());
        assert!(io.req_head.is_none());
        assert_eq!(io.iop_tokens, 2);

        io.leak_iop_tokens(2);
        assert_eq!(io.iop_tokens, 0);

        io.leak_iop_tokens(16000);
        assert_eq!(io.iop_tokens, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_does_not_consume_iops() -> Result<()> {
        let (guest, mut io) = Guest::new(None);

        // Set 0 as IOP limit
        io.set_iop_limit(16000, 0);
        assert_none_consumed(&mut io).await;

        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;

        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;

        assert_none_consumed(&mut io).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_set_bw_limit() -> Result<()> {
        let (guest, mut io) = Guest::new(None);
        io.set_bw_limit(1024 * 1024); // 1 KiB

        assert_none_consumed(&mut io).await;

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(1024, 512), // 512 KiB
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(1024, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(1024, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;

        // First two reads succeed
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;

        // Next cannot be consumed until there's available BW tokens so it
        // remains in the queue.
        assert_none_consumed(&mut io).await;
        assert!(io.req_rx.try_recv().is_err());
        assert!(io.req_head.is_some());
        assert_eq!(io.bw_tokens, 1024 * 1024);

        // Replenish enough tokens, meaning next read can be consumed
        io.leak_bw_tokens(1024 * 1024 / 2);
        assert_eq!(io.bw_tokens, 1024 * 1024 / 2);

        assert_consumed(&mut io).await;
        assert!(io.req_rx.try_recv().is_err());
        assert!(io.req_head.is_none());
        assert_eq!(io.bw_tokens, 1024 * 1024);

        io.leak_bw_tokens(1024 * 1024);
        assert_eq!(io.bw_tokens, 0);

        io.leak_bw_tokens(1024 * 1024 * 1024);
        assert_eq!(io.bw_tokens, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_does_not_consume_bw() -> Result<()> {
        let (guest, mut io) = Guest::new(None);

        // Set 0 as bandwidth limit
        io.set_bw_limit(0);
        assert_none_consumed(&mut io).await;

        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
                done: BlockOpWaiter::pair().1,
            })
            .await;

        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;
        assert_consumed(&mut io).await;

        assert_none_consumed(&mut io).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_iop_and_bw_limit() -> Result<()> {
        let (guest, mut io) = Guest::new(None);

        io.set_iop_limit(16384, 500); // 1 IOP is 16 KiB
        io.set_bw_limit(6400 * 1024); // 16384 B * 400 = 6400 KiB/s
        assert_none_consumed(&mut io).await;

        // Don't use guest.read, that will send a block size query that will
        // never be answered.

        // Validate that BW limit activates by sending two 7000 KiB IOs. 7000
        // KiB is only 437.5 IOPs

        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(14000, 512), // 7000 KiB
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(14000, 512), // 7000 KiB
                done: BlockOpWaiter::pair().1,
            })
            .await;

        assert_consumed(&mut io).await;
        assert_none_consumed(&mut io).await;

        // Assert we've hit the BW limit before IOPS
        assert_eq!(io.iop_tokens, 438); // 437.5 rounded up
        assert_eq!(io.bw_tokens, 7000 * 1024);

        io.leak_iop_tokens(438);
        io.leak_bw_tokens(7000 * 1024);

        assert_consumed(&mut io).await;

        // Everything should be empty now
        assert!(io.req_rx.try_recv().is_err());
        assert!(io.req_head.is_none());

        // Back to zero
        io.leak_iop_tokens(438);
        io.leak_bw_tokens(7000 * 1024);

        assert_eq!(io.iop_tokens, 0);
        assert_eq!(io.bw_tokens, 0);

        // Validate that IOP limit activates by sending 501 1024b IOs
        for _ in 0..500 {
            let _ = guest
                .send(BlockOp::Read {
                    offset: BlockIndex(0),
                    data: Buffer::new(2, 512),
                    done: BlockOpWaiter::pair().1,
                })
                .await;
            assert_consumed(&mut io).await;
        }

        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(2, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;
        assert_none_consumed(&mut io).await;

        // Assert we've hit the IOPS limit
        assert_eq!(io.iop_tokens, 500);
        assert_eq!(io.bw_tokens, 500 * 1024);

        // Back to zero
        io.leak_iop_tokens(500);
        io.leak_bw_tokens(500 * 1024);

        // Remove the 501st request
        assert!(io.req_head.take().is_some());
        assert!(io.req_rx.try_recv().is_err());
        assert_eq!(io.iop_tokens, 0);
        assert_eq!(io.bw_tokens, 0);

        // From
        // https://aws.amazon.com/premiumsupport/knowledge-center/ebs-calculate-optimal-io-size/:
        //
        // Amazon EBS calculates the optimal I/O size using the following
        // equation: throughput / number of IOPS = optimal I/O size.

        let optimal_io_size: usize = 6400 * 1024 / 500;

        // Round down to the nearest size in blocks
        let optimal_io_size = (optimal_io_size / 512) * 512;

        // Make sure this is <= an IOP size
        assert!(optimal_io_size <= 16384);

        // I mean, it makes sense: now we submit 500 of those to reach both
        // limits at the same time.
        for i in 0..500 {
            assert_eq!(io.iop_tokens, i);
            assert_eq!(io.bw_tokens, i * optimal_io_size);
            assert_eq!(optimal_io_size % 512, 0);

            let _ = guest
                .send(BlockOp::Read {
                    offset: BlockIndex(0),
                    data: Buffer::new(optimal_io_size / 512, 512),
                    done: BlockOpWaiter::pair().1,
                })
                .await;

            assert_consumed(&mut io).await;
        }

        assert_eq!(io.iop_tokens, 500);
        assert_eq!(io.bw_tokens, 500 * optimal_io_size);

        Ok(())
    }

    // Is it possible to submit an IO that will never be sent? It shouldn't be!
    #[tokio::test]
    async fn test_impossible_io() -> Result<()> {
        let (guest, mut io) = Guest::new(None);

        io.set_iop_limit(1024 * 1024 / 2, 10); // 1 IOP is half a KiB
        io.set_bw_limit(1024 * 1024); // 1 KiB
        assert_none_consumed(&mut io).await;

        // Sending an IO of 10 MiB is larger than the bandwidth limit and
        // represents 20 IOPs, larger than the IOP limit.
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(20480, 512), // 10 MiB
                done: BlockOpWaiter::pair().1,
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: BlockIndex(0),
                data: Buffer::new(0, 512),
                done: BlockOpWaiter::pair().1,
            })
            .await;

        assert_eq!(io.iop_tokens, 0);
        assert_eq!(io.bw_tokens, 0);

        // Even though the first IO is larger than the bandwidth and IOP limit,
        // it should still succeed. The next IO should not, even if it consumes
        // nothing, because the iops and bw tokens will be larger than the limit
        // for a while (until they leak enough).

        assert_consumed(&mut io).await;
        assert_none_consumed(&mut io).await;

        assert_eq!(io.iop_tokens, 20);
        assert_eq!(io.bw_tokens, 10 * 1024 * 1024);

        // Bandwidth trigger is going to be larger and need more leaking to get
        // down to a point where the zero sized IO can fire.
        for _ in 0..9 {
            io.leak_iop_tokens(10);
            io.leak_bw_tokens(1024 * 1024);

            assert_none_consumed(&mut io).await;
        }

        assert_eq!(io.iop_tokens, 0);
        assert_eq!(io.bw_tokens, 1024 * 1024);

        assert_none_consumed(&mut io).await;

        io.leak_iop_tokens(10);
        io.leak_bw_tokens(1024 * 1024);

        // We've leaked 10 KiB worth, it should fire now!
        assert_consumed(&mut io).await;

        Ok(())
    }
}
