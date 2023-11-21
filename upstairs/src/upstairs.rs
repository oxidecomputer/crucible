#![allow(dead_code)] // TODO remove this

use crate::{
    cdt,
    client::{ClientAction, ClientRunResult, ClientStopReason},
    downstairs::{Downstairs, DownstairsAction},
    extent_from_offset, integrity_hash, Block, BlockContext, BlockOp, BlockReq,
    Buffer, Bytes, ClientId, DsState, EncryptionContext, GtoS, Guest, Message,
    RegionDefinitionStatus, SnapshotDetails, UpCountStat, WQCounts,
};
use crucible_common::CrucibleError;

use std::sync::Arc;

use ringbuffer::RingBuffer;
use slog::{error, info, warn, Logger};
use tokio::{
    sync::oneshot,
    time::{sleep_until, Instant},
};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum UpstairsState {
    /// The upstairs is just coming online
    ///
    /// We can send IO on behalf of the upstairs (e.g. to handle negotiation),
    /// but not from the guest.
    Initializing,

    /// The guest has requested that the upstairs go active
    ///
    /// We should reply on the provided channel
    GoActive {
        reply: oneshot::Sender<Result<(), CrucibleError>>,
    },

    /// The upstairs is fully online and accepting guest IO
    Active,

    /// The upstairs is deactivating
    ///
    /// In-flight IO continues, but no new IO is allowed.  When all IO has been
    /// completed (including the final flush), the downstairs task should stop;
    /// when all three Downstairs have stopped, the upstairs should enter
    /// `UpstairsState::Initializing`.
    Deactivating,
}

struct Upstairs {
    /// Current state
    state: UpstairsState,

    /// Downstairs jobs and per-client state
    downstairs: Downstairs,

    /// Upstairs UUID
    uuid: Uuid,

    /// Unique session ID
    session_id: Uuid,

    /// Upstairs generation number
    ///
    /// This increases each time an Upstairs starts
    generation: u64,

    /// The guest struct keeps track of jobs accepted from the Guest
    ///
    /// A single job submitted can produce multiple downstairs requests.
    guest: Arc<Guest>,

    /// Current flush number
    ///
    /// This is the highest flush number from all three downstairs on startup,
    /// and increments by one each time the guest sends a flush (including
    /// automatic flushes).
    next_flush: u64,

    /// Region definition
    ///
    /// This is (optionally) provided on startup, and checked for consistency
    /// between all three Downstairs.
    ///
    /// The region definition allows us to translate an LBA to an extent and
    /// block offset.
    ddef: RegionDefinitionStatus,

    /// Encryption context, if present
    ///
    /// This is `Some(..)` if a key is provided in the `CrucibleOpts`
    encryption_context: Option<Arc<EncryptionContext>>,

    /// Marks whether a flush is needed
    ///
    /// The Upstairs keeps all IOs in memory until a flush is ACK'd back from
    /// all three downstairs.  If there are IOs we have accepted into the work
    /// queue that don't end with a flush, then we set this to indicate that the
    /// upstairs may need to issue a flush of its own to be sure that data is
    /// pushed to disk.  Note that this is not an indication of an ACK'd flush,
    /// just that the last IO command we put on the work queue was not a flush.
    need_flush: bool,

    /// Statistics for this upstairs
    stats: UpCountStat,

    /// Operate in read-only mode
    read_only: bool,

    /// Logger used by the upstairs
    log: Logger,

    /// Does this Upstairs throw random errors?
    lossy: bool,

    /// Next time to check for repairs
    repair_check_interval: Option<Instant>,

    /// Next time to leak IOP / bandwidth tokens from the Guest
    leak_deadline: Instant,

    /// Next time to trigger an automatic flush
    flush_deadline: Instant,
}

#[derive(Debug)]
enum UpstairsAction {
    Downstairs(DownstairsAction),
    Guest(BlockReq),
    LeakCheck,
    FlushCheck,
    RepairCheck,
}

impl Upstairs {
    /// Select an event from possible actions
    async fn select(&mut self) -> UpstairsAction {
        tokio::select! {
            d = self.downstairs.select() => {
                UpstairsAction::Downstairs(d)
            }
            d = self.guest.recv() => {
                UpstairsAction::Guest(d)
            }
            _ = async {
                if let Some(r) = self.repair_check_interval {
                    sleep_until(r).await
                } else {
                    futures::future::pending().await
                }
            } => {
                UpstairsAction::RepairCheck
            }
            _ = sleep_until(self.leak_deadline) => {
                UpstairsAction::LeakCheck
            }
            _ = sleep_until(self.flush_deadline) => {
                UpstairsAction::FlushCheck
            }
        }
    }

    /// Apply an action returned from [`Upstairs::select`]
    async fn apply(&mut self, action: UpstairsAction) {
        match action {
            UpstairsAction::Downstairs(d) => {
                self.apply_downstairs_action(d).await
            }
            UpstairsAction::Guest(b) => self.apply_guest_request(b).await,
            UpstairsAction::LeakCheck => {
                const LEAK_MS: usize = 1000;
                let leak_tick =
                    tokio::time::Duration::from_millis(LEAK_MS as u64);
                if let Some(iop_limit) = self.guest.get_iop_limit() {
                    let tokens = iop_limit / (1000 / LEAK_MS);
                    self.guest.leak_iop_tokens(tokens);
                }

                if let Some(bw_limit) = self.guest.get_bw_limit() {
                    let tokens = bw_limit / (1000 / LEAK_MS);
                    self.guest.leak_bw_tokens(tokens);
                }

                self.leak_deadline =
                    Instant::now().checked_add(leak_tick).unwrap();
            }
            UpstairsAction::FlushCheck => {
                if self.need_flush {
                    self.submit_flush(None, None).await;
                }
            }
            UpstairsAction::RepairCheck => unimplemented!(),
        }
    }

    /// Returns `true` if we're ready to accept guest IO
    fn guest_io_ready(&self) -> bool {
        matches!(self.state, UpstairsState::Active)
    }

    /// Apply a guest request
    async fn apply_guest_request(&mut self, req: BlockReq) {
        // If any of the submit_* functions fail to send to the downstairs, they
        // return an error.  These are reported to the Guest.
        match req.op() {
            // These three options can be handled by this task directly,
            // and don't require the upstairs to be fully online.
            BlockOp::GoActive => {
                self.set_active_request(req.take_sender()).await;
            }
            BlockOp::GoActiveWithGen { gen } => {
                self.generation = gen;
                self.set_active_request(req.take_sender()).await;
            }
            BlockOp::QueryGuestIOReady { data } => {
                *data.lock().await = self.guest_io_ready();
                req.send_ok();
            }
            BlockOp::QueryUpstairsUuid { data } => {
                *data.lock().await = self.uuid;
                req.send_ok();
            }
            BlockOp::Deactivate => {
                self.set_deactivate(req).await;
            }
            BlockOp::RepairOp => {
                warn!(self.log, "Ignoring external BlockOp::RepairOp");
            }

            // Query ops
            BlockOp::QueryBlockSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.block_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Block size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "block size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryTotalSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.total_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Total size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "total size".to_string(),
                        ));
                    }
                };
            }
            // Testing options
            BlockOp::QueryExtentSize { data } => {
                // Yes, test only
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.extent_size();
                        req.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Extent size not available (active: {})",
                            self.guest_io_ready()
                        );
                        req.send_err(CrucibleError::PropertyNotAvailable(
                            "extent size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryWorkQueue { data } => {
                // TODO should this first check if the Upstairs is active?
                let active_count = self
                    .downstairs
                    .clients
                    .iter()
                    .filter(|c| c.state() == DsState::Active)
                    .count();
                *data.lock().await = WQCounts {
                    up_count: self.guest.guest_work.lock().await.active.len(),
                    ds_count: self.downstairs.active_count(),
                    active_count,
                };
                req.send_ok();
            }

            BlockOp::ShowWork { data } => {
                // TODO should this first check if the Upstairs is active?
                *data.lock().await = self.show_all_work().await;
                req.send_ok();
            }

            BlockOp::Commit => {
                if !self.guest_io_ready() {
                    req.send_err(CrucibleError::UpstairsInactive);
                }
                // Nothing to do here; we always check for new work in `select!`
            }

            BlockOp::Read { offset, data } => {
                self.submit_read(offset, data, req).await
            }
            BlockOp::Write { offset, data } => {
                self.submit_write(offset, data, req, false).await
            }
            BlockOp::WriteUnwritten { offset, data } => {
                self.submit_write(offset, data, req, false).await
            }
            BlockOp::Flush { snapshot_details } => {
                /*
                 * Submit for read and write both check if the upstairs is
                 * ready for guest IO or not.  Because the Upstairs itself can
                 * call submit_flush, we have to check here that it is okay
                 * to accept IO from the guest before calling a guest requested
                 * flush command.
                 */
                if !self.guest_io_ready() {
                    req.send_err(CrucibleError::UpstairsInactive);
                    return;
                }
                self.submit_flush(Some(req), snapshot_details).await;
            }
            BlockOp::ReplaceDownstairs {
                id,
                old,
                new,
                result,
            } => match self.downstairs.replace(id, old, new, &self.state).await
            {
                Ok(v) => {
                    *result.lock().await = v;
                    req.send_ok();
                }
                Err(e) => req.send_err(e),
            },
        }
    }

    async fn show_all_work(&self) -> WQCounts {
        let gior = self.guest_io_ready();
        let up_count = self.guest.guest_work.lock().await.active.len();

        let ds_count = self.downstairs.active_count();

        println!(
            "----------------------------------------------------------------"
        );
        println!(
            " Crucible gen:{} GIO:{} work queues:  Upstairs:{}  downstairs:{}",
            self.generation, gior, up_count, ds_count,
        );
        if ds_count == 0 {
            if up_count != 0 {
                crate::show_guest_work(&self.guest).await;
            }
        } else {
            self.downstairs.show_all_work()
        }

        print!("Downstairs last five completed:");
        self.downstairs.print_last_completed(5);
        println!();

        let active_count = self
            .downstairs
            .clients
            .iter()
            .filter(|c| c.state() == DsState::Active)
            .count();

        // TODO this is a ringbuffer, why are we turning it to a Vec to look at
        // the last five items?
        let up_done = self.guest.guest_work.lock().await.completed.to_vec();
        print!("Upstairs last five completed:  ");
        for j in up_done.iter().rev().take(5) {
            print!(" {:4}", j);
        }
        println!();

        WQCounts {
            up_count,
            ds_count,
            active_count,
        }
    }

    /// Request that the Upstairs go active
    async fn set_active_request(
        &mut self,
        reply: oneshot::Sender<Result<(), CrucibleError>>,
    ) {
        match self.state {
            UpstairsState::Initializing => {
                self.state = UpstairsState::GoActive { reply };
                info!(self.log, "{} active request set", self.uuid);
            }
            UpstairsState::GoActive { .. } => {
                panic!("set_active_request called while already going active");
            }
            UpstairsState::Deactivating => {
                warn!(
                    self.log,
                    "{} active denied while Deactivating", self.uuid
                );
                let _ = reply.send(Err(CrucibleError::UpstairsDeactivating));
            }
            UpstairsState::Active => {
                info!(
                    self.log,
                    "{} Request to activate upstairs already active", self.uuid
                );
                let _ = reply.send(Err(CrucibleError::UpstairsAlreadyActive));
            }
        }
        // Notify all clients that they should go active when they hit an
        // appropriate state in their negotiation.
        for c in self.downstairs.clients.iter_mut() {
            c.set_active_request(self.generation).await;
        }
    }

    /// Request that the Upstairs deactivate
    ///
    /// This will return immediately if all of the Downstairs clients are done;
    /// otherwise, it will schedule a final flush that triggers deactivation
    /// when complete.
    ///
    /// In either case, `self.state` is set to `UpstairsState::Deactivating`
    async fn set_deactivate(&mut self, req: BlockReq) {
        info!(self.log, "Request to deactivate this guest");
        match self.state {
            UpstairsState::Initializing | UpstairsState::GoActive { .. } => {
                req.send_err(CrucibleError::UpstairsInactive);
                return;
            }
            UpstairsState::Deactivating => {
                req.send_err(CrucibleError::UpstairsDeactivating);
                return;
            }
            UpstairsState::Active => (),
        }
        self.state = UpstairsState::Deactivating;

        if self.downstairs.set_deactivate() {
            req.send_ok();
        } else {
            self.submit_flush(Some(req), None).await
        }
    }

    /// Returns the next flush number
    ///
    /// If we are doing a flush, the flush number and the rn number
    /// must both go up together. We don't want a lower next_id
    /// with a higher flush_number to be possible, as that can introduce
    /// dependency deadlock.
    fn next_flush_id(&mut self) -> u64 {
        let out = self.next_flush;
        self.next_flush += 1;
        out
    }

    async fn submit_flush(
        &mut self,
        req: Option<BlockReq>,
        snapshot_details: Option<SnapshotDetails>,
    ) {
        // Notice that unlike submit_read and submit_write, we do not check for
        // guest_io_ready here. The upstairs itself can call submit_flush
        // (without the guest being involved), so the check is handled at the
        // BlockOp::Flush level above.

        self.need_flush = false;
        let next_flush = self.next_flush_id();

        /*
         * Get the next ID for our new guest work job. Note that the flush
         * ID and the next_id are connected here, in that all future writes
         * should be flushed at the next flush ID.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let gw_id: u64 = gw.next_gw_id();
        cdt::gw__flush__start!(|| (gw_id));

        if snapshot_details.is_some() {
            info!(self.log, "flush with snap requested");
        }

        let next_id = self.downstairs.submit_flush(
            gw_id,
            next_flush,
            self.generation,
            snapshot_details,
        );

        let new_gtos = GtoS::new(next_id, None, req);
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__flush__start!(|| (gw_id));
    }

    /// Submit a read job to the downstairs
    async fn submit_read(
        &mut self,
        offset: Block,
        data: Buffer,
        req: BlockReq,
    ) {
        if !self.guest_io_ready() {
            req.send_err(CrucibleError::UpstairsInactive);
            return;
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let ddef = self.ddef.get_def().unwrap();

        /*
         * Verify IO is in range for our region
         */
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            req.send_err(e);
            return;
        }

        self.need_flush = true;

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span many extents, and eventually, TODO, regions.
         */
        let impacted_blocks = crate::extent_from_offset(
            &ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        );

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        cdt::gw__read__start!(|| (gw_id));

        let next_id = self.downstairs.submit_read(gw_id, impacted_blocks, ddef);

        // New work created, add to the guest_work HM.  It's fine to do this
        // after submitting the job to the downstairs, because no one else is
        // modifying the Upstairs right now; even if the job finishes
        // instantaneously, it can't interrupt this function.
        let new_gtos = GtoS::new(next_id, Some(data), Some(req));
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__read__start!(|| (gw_id));
    }

    async fn submit_write(
        &mut self,
        offset: Block,
        data: Bytes,
        req: BlockReq,
        is_write_unwritten: bool,
    ) {
        if !self.guest_io_ready() {
            req.send_err(CrucibleError::UpstairsInactive);
            return;
        }
        if self.read_only {
            req.send_err(CrucibleError::ModifyingReadOnlyRegion);
            return;
        }

        /*
         * Verify IO is in range for our region
         */
        let ddef = self.ddef.get_def().unwrap();
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            req.send_err(e);
            return;
        }

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span two extents.
         */
        let impacted_blocks = extent_from_offset(
            &ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        );

        // Build up all of the Write operations, encrypting data here
        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(impacted_blocks.len(&ddef));

        let mut cur_offset: usize = 0;
        for (eid, offset) in impacted_blocks.blocks(&ddef) {
            let byte_len: usize = ddef.block_size() as usize;

            let (sub_data, encryption_context, hash) = if let Some(context) =
                &self.encryption_context
            {
                // Encrypt here
                let mut mut_data =
                    data.slice(cur_offset..(cur_offset + byte_len)).to_vec();

                let (nonce, tag, hash) =
                    match context.encrypt_in_place(&mut mut_data[..]) {
                        Err(e) => {
                            req.send_err(CrucibleError::EncryptionError(
                                e.to_string(),
                            ));
                            return;
                        }

                        Ok(v) => v,
                    };

                (
                    Bytes::copy_from_slice(&mut_data),
                    Some(crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    }),
                    hash,
                )
            } else {
                // Unencrypted
                let sub_data = data.slice(cur_offset..(cur_offset + byte_len));
                let hash = integrity_hash(&[&sub_data[..]]);

                (sub_data, None, hash)
            };

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: sub_data,
                block_context: BlockContext {
                    hash,
                    encryption_context,
                },
            });

            cur_offset += byte_len;
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        self.need_flush = true;

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        if is_write_unwritten {
            cdt::gw__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::gw__write__start!(|| (gw_id));
        }

        let next_id = self.downstairs.submit_write(
            gw_id,
            impacted_blocks,
            writes,
            is_write_unwritten,
        );
        let new_gtos = GtoS::new(next_id, None, Some(req));

        // New work created, add to the guest_work HM
        gw.active.insert(gw_id, new_gtos);

        if is_write_unwritten {
            cdt::up__to__ds__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::up__to__ds__write__start!(|| (gw_id));
        }
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_downstairs_action(&mut self, d: DownstairsAction) {
        match d {
            DownstairsAction::Client { client_id, action } => {
                self.apply_client_action(client_id, action).await;
            }
            DownstairsAction::AckReady => {
                self.ack_ready().await;
            }
        }
    }

    async fn ack_ready(&mut self) {
        let mut gw = self.guest.guest_work.lock().await;
        self.downstairs.ack_jobs(&mut gw, &mut self.stats).await;
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_client_action(
        &mut self,
        client_id: ClientId,
        action: ClientAction,
    ) {
        match action {
            ClientAction::Ping => {
                self.downstairs.clients[client_id].send_ping().await;
            }
            ClientAction::Timeout => {
                // Ask the downstairs client task to stop, because the client
                // has hit a Crucible timeout.
                //
                // This will come back to `TaskStopped`, at which point we'll
                // clear out the task and restart it.
                self.downstairs.clients[client_id]
                    .halt_io_task(ClientStopReason::Timeout)
                    .await;
            }
            ClientAction::Response(m) => {
                self.on_client_message(client_id, m).await;
            }
            ClientAction::TaskStopped(r) => {
                self.on_client_task_stopped(client_id, r);
            }
            ClientAction::Work | ClientAction::MoreWork => {
                self.downstairs.io_send(client_id, self.lossy).await;
            }
        }
    }

    async fn on_client_message(&mut self, client_id: ClientId, m: Message) {
        // We have received a message, so reset the timeout watchdog for this
        // particular client.
        self.downstairs.clients[client_id].reset_timeout();
        match m {
            Message::Imok => {
                // Nothing to do here, glad to hear that you're okay
            }

            // IO operation replies
            //
            // This may cause jobs to become ackable!
            Message::WriteAck { .. }
            | Message::WriteUnwrittenAck { .. }
            | Message::FlushAck { .. }
            | Message::ReadResponse { .. }
            | Message::ExtentLiveCloseAck { .. }
            | Message::ExtentLiveAckId { .. }
            | Message::ExtentLiveRepairAckId { .. }
            | Message::ErrorReport { .. } => {
                let r = self.downstairs.process_io_completion(
                    client_id,
                    m,
                    &self.encryption_context,
                    &self.state,
                );
                if let Err(e) = r {
                    warn!(
                        self.downstairs.clients[client_id].log,
                        "Error processing message: {}", e
                    );
                }
            }

            Message::YesItsMe { .. }
            | Message::VersionMismatch { .. }
            | Message::EncryptedMismatch { .. }
            | Message::ReadOnlyMismatch { .. }
            | Message::YouAreNowActive { .. }
            | Message::RegionInfo { .. }
            | Message::LastFlushAck { .. }
            | Message::ExtentVersions { .. } => {
                // negotiation and initial reconciliation
                let r = self.downstairs.clients[client_id]
                    .continue_negotiation(
                        m,
                        &self.state,
                        self.generation,
                        &mut self.ddef,
                    )
                    .await;

                match r {
                    // continue_negotiation returns an error if the upstairs
                    // should go inactive!
                    Err(e) => self.set_inactive(e),
                    Ok(false) => (),
                    Ok(true) => {
                        // Negotiation succeeded for this Downstairs, let's see
                        // what we can do from here
                        match self.downstairs.clients[client_id].state() {
                            DsState::Active => (),
                            DsState::WaitQuorum => {
                                // See if we have a quorum
                                self.connect_region_set().await
                            }
                            DsState::LiveRepairReady => {
                                // See if we can do live-repair
                                // ???
                            }
                            s => panic!("bad state after negotiation: {s:?}"),
                        }
                    }
                }
            }

            Message::ExtentError { .. } => {
                self.downstairs
                    .on_reconciliation_failed(client_id, m, &self.state)
                    .await;
            }
            Message::RepairAckId { .. } => {
                if self
                    .downstairs
                    .on_reconciliation_ack(client_id, m, &self.state)
                    .await
                {
                    // reconciliation is done, great work everyone
                    self.on_reconciliation_done(DsState::Repair);
                }
            }

            Message::YouAreNoLongerActive { .. } => {
                self.on_no_longer_active(client_id, m).await;
            }

            Message::UuidMismatch { .. } => {
                self.on_uuid_mismatch(client_id, m).await;
            }

            // These are all messages that we send out, so we shouldn't see them
            Message::HereIAm { .. }
            | Message::Ruok
            | Message::Flush { .. }
            | Message::LastFlush { .. }
            | Message::Write { .. }
            | Message::WriteUnwritten { .. }
            | Message::ReadRequest { .. }
            | Message::RegionInfoPlease { .. }
            | Message::ExtentLiveFlushClose { .. }
            | Message::ExtentLiveClose { .. }
            | Message::ExtentLiveRepair { .. }
            | Message::ExtentLiveNoOp { .. }
            | Message::ExtentLiveReopen { .. }
            | Message::ExtentClose { .. }
            | Message::ExtentFlush { .. }
            | Message::ExtentRepair { .. }
            | Message::ExtentReopen { .. }
            | Message::ExtentVersionsPlease
            | Message::PromoteToActive { .. }
            | Message::Unknown(..) => {
                panic!("invalid response {m:?}")
            }
        }
    }

    /// Checks whether we can connect all three regions
    ///
    /// Returns `false` if we aren't ready, or if things failed.  If there's a
    /// failure, then we also update the client state.
    ///
    /// If we have enough downstairs and we can activate, then we should inform
    /// the requestor of activation; similarly, if we have enough downstairs and
    /// **can't** activate, then we should notify the requestor of failure.
    ///
    /// If we have a problem here, we can't activate the upstairs.
    async fn connect_region_set(&mut self) {
        /*
         * Reconciliation only happens during initialization.
         * Look at all three downstairs region information collected.
         * Determine the highest flush number and make sure our generation
         * is high enough.
         */
        let collate_status = {
            if !matches!(self.state, UpstairsState::GoActive { .. }) {
                info!(
                    self.log,
                    "could not connect region set due to bad state: {:?}",
                    self.state
                );
                return;
            }
            /*
             * Make sure all downstairs are in the correct state before we
             * proceed.
             */
            let not_ready = self
                .downstairs
                .clients
                .iter()
                .filter(|c| c.state() != DsState::WaitQuorum)
                .count();
            if not_ready > 0 {
                info!(
                    self.log,
                    "Waiting for {} more clients to be ready", not_ready
                );
                return;
            }

            /*
             * While holding the downstairs lock, we figure out if there is
             * any reconciliation to do, and if so, we build the list of
             * operations that will repair the extents that are not in sync.
             *
             * If we fail to collate, then we need to kick out all the
             * downstairs out, forget any activation requests, and the
             * upstairs goes back to waiting for another activation request.
             */
            self.downstairs
                .collate(self.generation, &mut self.next_flush, &self.state)
                .await
        };

        match collate_status {
            Err(e) => {
                error!(self.log, "Failed downstairs collate with: {}", e);
                // We failed to collate the three downstairs, so we need
                // to reset that activation request.  The downstairs were
                // already set to FailedRepair in the call to
                // `Downstairs::collate`
                self.set_inactive(e);
            }
            Ok(true) => {
                // We have populated all of the reconciliation requests in
                // `Downstairs::reconcile_task_list`.  Start reconciliation by
                // sending the first request.
                self.downstairs.send_next_reconciliation_req().await;
            }
            Ok(false) => {
                info!(self.log, "No downstairs repair required");
                self.on_reconciliation_done(DsState::WaitQuorum);
                info!(self.log, "Set Active after no repair");
            }
        }
    }

    /// Called when reconciliation is complete
    fn on_reconciliation_done(&mut self, from_state: DsState) {
        // This should only ever be called if reconciliation completed
        // successfully; make some assertions to that effect.
        self.downstairs.on_reconciliation_done(from_state);

        info!(self.log, "All required repair work is completed");
        info!(self.log, "Set Downstairs and Upstairs active after repairs");

        let UpstairsState::GoActive { reply } =
            std::mem::replace(&mut self.state, UpstairsState::Active)
        else {
            panic!("invalid state active state: {:?}", self.state);
        };
        reply.send(Ok(())).unwrap();
        info!(
            self.log,
            "{} is now active with session: {}", self.uuid, self.session_id
        );
        self.stats.add_activation();
    }

    async fn on_no_longer_active(&mut self, client_id: ClientId, m: Message) {
        let Message::YouAreNoLongerActive {
            new_upstairs_id,
            new_session_id,
            new_gen,
        } = m else {
            panic!("called on_no_longer_active on invalid message {m:?}");
        };

        let client_log = &self.downstairs.clients[client_id].log;
        error!(
            client_log,
            "received NoLongerActive {:?} {:?} {}",
            new_upstairs_id,
            new_session_id,
            new_gen,
        );

        // Before we restart the downstairs client, let's print a specific
        // warning or error message

        // What if the newly active upstairs has the same UUID?
        let uuid_desc = if self.uuid == new_upstairs_id {
            "same upstairs UUID".to_owned()
        } else {
            format!("different upstairs UUID {new_upstairs_id:?}")
        };

        if new_gen <= self.generation {
            // Here, our generation number is greater than or equal to the newly
            // active Upstairs, which shares our UUID. We shouldn't have
            // received this message. The downstairs is confused.
            error!(
                client_log,
                "bad YouAreNoLongerActive with our gen {} >= {new_gen} \
                 and {uuid_desc}",
                self.generation
            );
        } else {
            // The next generation of this Upstairs connected, which is fine.
            warn!(
                client_log,
                "saw YouAreNoLongerActive with our gen {} < {new_gen} and \
                 {uuid_desc}",
                self.generation
            );
        };

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id]
            .disable(&self.state)
            .await;
        self.set_inactive(CrucibleError::NoLongerActive);
    }

    async fn on_uuid_mismatch(&mut self, client_id: ClientId, m: Message) {
        let Message::UuidMismatch {
            expected_id
        } = m else {
            panic!("called on_uuid_mismatch on invalid message {m:?}");
        };

        let client_log = &self.downstairs.clients[client_id].log;
        error!(
            client_log,
            "received UuidMismatch, expecting {expected_id:?}!"
        );

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id]
            .disable(&self.state)
            .await;
        self.set_inactive(CrucibleError::UuidMismatch);
    }

    fn set_inactive(&mut self, err: CrucibleError) {
        let prev =
            std::mem::replace(&mut self.state, UpstairsState::Initializing);
        if let UpstairsState::GoActive { reply } = prev {
            let _ = reply.send(Err(err));
        }
        info!(self.log, "setting inactive!");
    }

    fn on_client_task_stopped(
        &mut self,
        client_id: ClientId,
        reason: ClientRunResult,
    ) {
        warn!(
            self.log,
            "downstairs task for {client_id} stopped due to {reason:?}"
        );

        // If the connection goes down here, we need to know what state we were
        // in to decide what state to transition to.  The ds_missing method will
        // do that for us!
        self.downstairs.clients[client_id].on_missing();

        // If we are deactivating, then check and see if this downstairs was the
        // final one required to deactivate; if so, switch the upstairs back to
        // initializing.
        self.deactivate_transition_check();

        // Restart the downstairs task.  If the upstairs is already active, then
        // the downstairs should automatically call PromoteToActive when it
        // reaches the relevant state.
        let auto_promote = match self.state {
            UpstairsState::Active | UpstairsState::GoActive { .. } => {
                // XXX is is correct to auto-promote if we're in GoActive?
                Some(self.generation)
            }
            UpstairsState::Initializing | UpstairsState::Deactivating => None,
        };
        self.downstairs.reinitialize(client_id, auto_promote);
    }

    fn deactivate_transition_check(&mut self) {
        if matches!(self.state, UpstairsState::Deactivating) {
            info!(self.log, "deactivate transition checking...");
            if self
                .downstairs
                .clients
                .iter()
                .all(|c| c.ready_to_deactivate())
            {
                info!(self.log, "All DS in the proper state! -> INIT");
                self.state = UpstairsState::Initializing;
            }
        }
    }
}
