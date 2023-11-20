#![allow(dead_code)] // TODO remove this
                     //
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use crate::{
    cdt,
    client::{ClientAction, DownstairsClient},
    upstairs::UpstairsState,
    AckStatus, ActiveJobs, AllocRingBuffer, ClientData, ClientId,
    CrucibleError, DownstairsIO, DownstairsMend, DsState, EncryptionContext,
    ExtentFix, ExtentInfo, ExtentRepairIDs, IOState, IOStateCount, IOop,
    ImpactedBlocks, JobId, Message, ReadRequest, ReadResponse, ReconcileIO,
    ReconciliationId, RegionDefinition, ReplaceResult, SnapshotDetails,
    WorkSummary,
};
use crucible_common::MAX_ACTIVE_COUNT;

use rand::prelude::*;
use ringbuffer::RingBuffer;
use slog::{debug, error, info, o, warn, Logger};
use uuid::Uuid;

/*
 * The structure that tracks information about the three downstairs
 * connections as well as the work that each is doing.
 */
#[derive(Debug)]
pub(crate) struct Downstairs {
    /// Per-client data
    pub(crate) clients: ClientData<DownstairsClient>,

    /// Upstairs UUID, copied here for ease of use
    upstairs_id: Uuid,

    /// Upstairs session UUID, copied here for ease of use
    session_id: Uuid,

    /// The active list of IO for the downstairs.
    ds_active: ActiveJobs,

    /// The number of write bytes that haven't finished yet
    ///
    /// This is used to configure backpressure to the host, because writes
    /// (uniquely) will return before actually being completed by a Downstairs
    /// and can clog up the queues.
    ///
    /// It is stored in the Downstairs because from the perspective of the
    /// Upstairs, writes complete immediately; only the Downstairs is actually
    /// tracking the pending jobs.
    write_bytes_outstanding: u64,

    /// The next Job ID this Upstairs should use for downstairs work.
    next_id: JobId,

    /// Ringbuf of completed downstairs job IDs.
    completed: AllocRingBuffer<JobId>,

    /// Ringbuf of a summary of each recently completed downstairs IO.
    completed_jobs: AllocRingBuffer<WorkSummary>,

    /// Current piece of reconcile work that the downstairs are working on
    ///
    /// It can be New, InProgress, Skipped, or Done.
    reconcile_current_work: Option<ReconcileIO>,

    /// Remaining reconciliation work
    ///
    /// This queue holds the remaining work required to make all three
    /// downstairs in a region set the same.
    reconcile_task_list: VecDeque<ReconcileIO>,

    /// Number of extents repaired during initial activation
    reconcile_repaired: usize,

    /// Number of extents needing repair during initial activation
    reconcile_repair_needed: usize,

    /// The logger for messages sent from downstairs methods.
    log: Logger,

    /// Counters for the in flight work for the downstairs
    io_state_count: IOStateCount,

    /**
     * Live Repair Job IDs
     * If, while running live repair, we have an IO that spans repaired
     * extents and not yet repaired extents, we will reserve job IDs for the
     * future repair work and store them in this hash map.  When it comes time
     * to start a repair and allocate the job IDs we will require, we first
     * check this hash map to see if the IDs were already repaired.
     *
     * The key is the extent being repaired; the value is a tuple of reserved
     * IDs and existing dependencies for the first job in the repair.
     */
    repair_job_ids: BTreeMap<u64, (ExtentRepairIDs, Vec<JobId>)>,

    /**
     * When repairing, this will be the minimum job ID the downstairs under
     * repair needs to consider for dependencies.  This being `Some` also
     * indicates a live repair task is running and being `Some` is used to
     * prevent more than one repair task from running at the same time.
     */
    repair_min_id: Option<JobId>,
}

#[derive(Debug)]
pub(crate) enum DownstairsAction {
    Client {
        client_id: ClientId,
        action: ClientAction,
    },
}

impl Downstairs {
    /// Choose which `DownstairsAction` to apply
    ///
    /// This function is called from within a top-level `select!`, so not only
    /// must the select expressions be cancel safe, but the **bodies** must also
    /// be cancel-safe.  This is why we simply return a single value in the body
    /// of each statement.
    pub(crate) async fn select(&mut self) -> DownstairsAction {
        // Split borrow of the clients
        let [ca, cb, cc] = &mut self.clients.0;
        tokio::select! {
            action = ca.select() => {
                DownstairsAction::Client {
                    client_id: ClientId::new(0),
                    action
                }
            }
            action = cb.select() => {
                DownstairsAction::Client {
                    client_id: ClientId::new(1),
                    action
                }
            }
            action = cc.select() => {
                DownstairsAction::Client {
                    client_id: ClientId::new(2),
                    action
                }
            }
        }
    }

    pub(crate) async fn perform_work(
        &mut self,
        client_id: ClientId,
        lossy: bool,
    ) {
        let more = self.io_send(client_id, lossy).await;
        self.clients[client_id].set_more_work(more);
    }

    pub(crate) async fn io_send(
        &mut self,
        client_id: ClientId,
        lossy: bool,
    ) -> bool {
        /*
         * Build ourselves a list of all the jobs on the work hashmap that
         * have the job state for our client id in the IOState::New
         *
         * The length of this list (new work for a downstairs) can give us
         * an idea of how that downstairs is doing. If the number of jobs
         * to be submitted is too big (for some value of big) then there is
         * a problem. All sorts of back pressure information can be
         * gathered here. As (for the moment) the same task does both
         * transmit and receive, we can starve the receive side by spending
         * all our time sending work.
         *
         * This XXX is for coming back here and making a better job of
         * flow control.
         */
        let client = &mut self.clients[client_id];
        let (new_work, flow_control) = {
            let active_count =
                self.io_state_count.in_progress[client_id] as usize;
            if active_count > MAX_ACTIVE_COUNT {
                // Can't do any work
                client.stats.flow_control += 1;
                return true;
            } else {
                let n = MAX_ACTIVE_COUNT - active_count;
                let (new_work, flow_control) = client.new_work(n);
                if flow_control {
                    client.stats.flow_control += 1;
                }
                (new_work, flow_control)
            }
        };

        /*
         * Now we have a list of all the job IDs that are new for our client id.
         * Walk this list and process each job, marking it InProgress as we
         * do the work. We do this in two loops because we can't hold the
         * lock for the hashmap while we do work, and if we release the lock
         * to do work, we would have to start over and look at all jobs in the
         * map to see if they are new.
         */
        for new_id in new_work {
            /*
             * Walk the list of work to do, update its status as in progress
             * and send the details to our downstairs.
             */
            if lossy && random() && random() {
                /*
                 * Requeue this work so it isn't completely lost.
                 */
                self.clients[client_id].requeue_one(new_id);
                continue;
            }

            /*
             * If in_progress returns None, it means that this job on this
             * client should be skipped.
             */
            let Some(job) = self.in_progress(new_id, client_id)
                else { continue; };

            let message = match job {
                IOop::Write {
                    dependencies,
                    writes,
                } => {
                    cdt::ds__write__io__start!(|| (new_id.0, client_id.get()));
                    Message::Write {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                        writes,
                    }
                }
                IOop::WriteUnwritten {
                    dependencies,
                    writes,
                } => {
                    cdt::ds__write__unwritten__io__start!(|| (
                        new_id.0,
                        client_id.get()
                    ));
                    Message::WriteUnwritten {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                        writes,
                    }
                }
                IOop::Flush {
                    dependencies,
                    flush_number,
                    gen_number,
                    snapshot_details,
                    extent_limit,
                } => {
                    cdt::ds__flush__io__start!(|| (new_id.0, client_id.get()));
                    Message::Flush {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                        flush_number,
                        gen_number,
                        snapshot_details,
                        extent_limit,
                    }
                }
                IOop::Read {
                    dependencies,
                    requests,
                } => {
                    cdt::ds__read__io__start!(|| (new_id.0, client_id.get()));
                    Message::ReadRequest {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                        requests,
                    }
                }
                IOop::ExtentClose {
                    dependencies: _,
                    extent: _,
                } => {
                    // This command should never exist on the upstairs side.
                    // We only construct it for downstairs IO, after
                    // receiving ExtentLiveClose from the upstairs.
                    panic!(
                        "[{}] Received illegal IOop::ExtentClose",
                        client_id
                    );
                }
                IOop::ExtentFlushClose {
                    dependencies,
                    extent,
                    flush_number,
                    gen_number,
                    source_downstairs: _,
                    repair_downstairs,
                } => {
                    cdt::ds__close__start!(|| (
                        new_id.0,
                        client_id.get(),
                        extent
                    ));
                    if repair_downstairs.contains(&client_id) {
                        // We are the downstairs being repaired, so just close.
                        Message::ExtentLiveClose {
                            upstairs_id: self.upstairs_id,
                            session_id: self.session_id,
                            job_id: new_id,
                            dependencies,
                            extent_id: extent,
                        }
                    } else {
                        Message::ExtentLiveFlushClose {
                            upstairs_id: self.upstairs_id,
                            session_id: self.session_id,
                            job_id: new_id,
                            dependencies,
                            extent_id: extent,
                            flush_number,
                            gen_number,
                        }
                    }
                }
                IOop::ExtentLiveRepair {
                    dependencies,
                    extent,
                    source_downstairs,
                    source_repair_address,
                    repair_downstairs,
                } => {
                    cdt::ds__repair__start!(|| (
                        new_id.0,
                        client_id.get(),
                        extent
                    ));
                    if repair_downstairs.contains(&client_id) {
                        Message::ExtentLiveRepair {
                            upstairs_id: self.upstairs_id,
                            session_id: self.session_id,
                            job_id: new_id,
                            dependencies,
                            extent_id: extent,
                            source_client_id: source_downstairs,
                            source_repair_address,
                        }
                    } else {
                        Message::ExtentLiveNoOp {
                            upstairs_id: self.upstairs_id,
                            session_id: self.session_id,
                            job_id: new_id,
                            dependencies,
                        }
                    }
                }
                IOop::ExtentLiveReopen {
                    dependencies,
                    extent,
                } => {
                    cdt::ds__reopen__start!(|| (
                        new_id.0,
                        client_id.get(),
                        extent
                    ));
                    Message::ExtentLiveReopen {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                        extent_id: extent,
                    }
                }
                IOop::ExtentLiveNoOp { dependencies } => {
                    cdt::ds__noop__start!(|| (new_id.0, client_id.get()));
                    Message::ExtentLiveNoOp {
                        upstairs_id: self.upstairs_id,
                        session_id: self.session_id,
                        job_id: new_id,
                        dependencies,
                    }
                }
            };
            self.clients[client_id].send(message).await
        }
        flow_control
    }

    /// Mark this request as in progress for this client, and return the
    /// relevant [`IOOp`] with updated dependencies.
    ///
    /// If the job state is already [`IOState::Skipped`], then this task
    /// has no work to do, so return `None`.
    fn in_progress(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
    ) -> Option<IOop> {
        let mut handle = match self.ds_active.get_mut(&ds_id) {
            Some(handle) => handle,
            None => {
                // This job, that we thought was good, is not.  As we don't
                // keep the lock when gathering job IDs to work on, it is
                // possible to have a out of date work list.
                warn!(self.log, "[{client_id}] Job {ds_id} not on active list");
                return None;
            }
        };
        let job = handle.job();

        // If current state is Skipped, then we have nothing to do here.
        if matches!(job.state[client_id], IOState::Skipped) {
            return None;
        }

        Some(self.clients[client_id].in_progress(job, self.repair_min_id))
    }

    /// Reinitialize the given client
    pub(crate) fn reinitialize(
        &mut self,
        client_id: ClientId,
        auto_promote: Option<u64>,
    ) {
        // If this client is coming back from being offline, then replay all of
        // its jobs.
        if self.clients[client_id].state() == DsState::Offline {
            self.replay_jobs(client_id);
        }
        // Restart the IO task
        self.clients[client_id].reinitialize(auto_promote);
    }

    /// Tries to deactivate all of the Downstairs clients
    ///
    /// Returns true if we succeeded; otherwise returns false
    pub(crate) fn set_deactivate(&mut self) -> bool {
        /*
         * If any downstairs are currently offline, then we are going
         * to lock the door behind them and not let them back in until
         * all non-offline downstairs have deactivated themselves.
         *
         * However: TODO: This is not done yet.
         */
        let offline_ds: Vec<ClientId> = self
            .clients
            .iter()
            .enumerate()
            .filter(|(_i, c)| c.state() == DsState::Offline)
            .map(|(i, _c)| ClientId::new(i as u8))
            .collect();

        /*
         * TODO: Handle deactivation when a downstairs is offline.
         */
        if !offline_ds.is_empty() {
            panic!("Can't deactivate with downstairs offline (yet)");
        }

        // If there are no jobs, then we're immediately done!
        self.ds_active.is_empty()
    }

    /// Assign a new downstairs job ID.
    pub(crate) fn next_id(&mut self) -> JobId {
        let id = self.next_id;
        self.next_id.0 += 1;
        id
    }

    /// Moves all pending jobs back to the `new_jobs` queue
    ///
    /// Jobs are pending if they have not yet been flushed by this client.
    fn replay_jobs(&mut self, client_id: ClientId) {
        let lf = self.clients[client_id].last_flush();

        info!(
            self.log,
            "[{client_id}] client re-new {} jobs since flush {lf}",
            self.ds_active.len(),
        );

        self.ds_active.for_each(|ds_id, job| {
            // We don't need to send anything before our last good flush
            if *ds_id <= lf {
                assert_eq!(IOState::Done, job.state[client_id]);
                return;
            }

            self.clients[client_id].replay_job(job);
        });
    }

    /// Compare downstairs region metadata and based on the results:
    ///
    /// Determine the global flush number for this region set.
    /// Verify the guest given gen number is highest.
    /// Decide if we need repair, and if so create the repair list
    pub(crate) async fn collate(
        &mut self,
        gen: u64,
        next_flush: &mut u64,
        up_state: &UpstairsState,
    ) -> Result<bool, CrucibleError> {
        let r = self.collate_inner(gen, next_flush);
        if r.is_err() {
            // While collating, downstairs should all be DsState::Repair.
            //
            // Any downstairs still repairing should be moved to
            // failed repair.
            assert_eq!(self.ds_active.len(), 0);
            assert_eq!(self.reconcile_task_list.len(), 0);

            for (i, c) in self.clients.iter_mut().enumerate() {
                match c.state() {
                    DsState::WaitQuorum => {
                        // Set this task as FailedRepair then restart it
                        c.set_failed_repair(up_state).await;
                        warn!(
                            self.log,
                            "Mark {i} as FAILED Collate in final check"
                        );
                    }
                    s => {
                        warn!(
                            self.log,
                            "downstairs in state {s} after failed collate"
                        );
                    }
                }
            }
        }
        r
    }

    fn collate_inner(
        &mut self,
        gen: u64,
        next_flush: &mut u64,
    ) -> Result<bool, CrucibleError> {
        /*
         * Show some (or all if small) of the info from each region.
         *
         * This loop is load bearing, we use this loop to get the
         * max flush number.  Eventually the max flush will come after
         * we have done any repair we need to do.  Since we don't have
         * that code yet, we are making use of this loop to find our
         * max.
         */
        let mut max_flush = 0;
        let mut max_gen = 0;
        for (cid, rec) in self
            .clients
            .iter()
            .map(|c| c.region_metadata.as_ref().unwrap())
            .enumerate()
        {
            let mf = rec.flush_numbers.iter().max().unwrap() + 1;
            if mf > max_flush {
                max_flush = mf;
            }
            let mg = rec.generation.iter().max().unwrap() + 1;
            if mg > max_gen {
                max_gen = mg;
            }
            if rec.flush_numbers.len() > 12 {
                info!(
                    self.log,
                    "[{}]R flush_numbers[0..12]: {:?}",
                    cid,
                    rec.flush_numbers[0..12].to_vec()
                );
                info!(
                    self.log,
                    "[{}]R generation[0..12]: {:?}",
                    cid,
                    rec.generation[0..12].to_vec()
                );
                info!(
                    self.log,
                    "[{}]R dirty[0..12]: {:?}",
                    cid,
                    rec.dirty[0..12].to_vec()
                );
            } else {
                info!(
                    self.log,
                    "[{}]R  flush_numbers: {:?}", cid, rec.flush_numbers
                );
                info!(self.log, "[{}]R  generation: {:?}", cid, rec.generation);
                info!(self.log, "[{}]R  dirty: {:?}", cid, rec.dirty);
            }
        }

        info!(self.log, "Max found gen is {}", max_gen);
        /*
         * Verify that the generation number that the guest has requested
         * is higher than what we have from the three downstairs.
         */
        let requested_gen = gen;
        if requested_gen == 0 {
            error!(self.log, "generation number should be at least 1");
            return Err(CrucibleError::GenerationNumberTooLow(
                "Generation 0 illegal".to_owned(),
            ));
        } else if requested_gen < max_gen {
            /*
             * We refuse to connect. The provided generation number is not
             * high enough to let us connect to these downstairs.
             */
            error!(
                self.log,
                "found generation number {}, larger than requested: {}",
                max_gen,
                requested_gen,
            );
            return Err(CrucibleError::GenerationNumberTooLow(format!(
                "found generation number {}, larger than requested: {}",
                max_gen, requested_gen,
            )));
        } else {
            info!(
                self.log,
                "Generation requested: {} >= found:{}", requested_gen, max_gen,
            );
        }

        /*
         * Set the next flush ID so we have if we need to repair.
         */
        *next_flush = max_flush;
        info!(self.log, "Next flush: {}", max_flush);

        /*
         * Determine what extents don't match and what to do
         * about that
         */
        if let Some(reconcile_list) = self.mismatch_list() {
            for c in self.clients.iter_mut() {
                c.begin_reconcile();
            }

            info!(
                self.log,
                "Found {:?} extents that need repair",
                reconcile_list.mend.len()
            );
            self.convert_rc_to_messages(
                reconcile_list.mend,
                max_flush,
                max_gen,
            );
            self.reconcile_repair_needed = self.reconcile_task_list.len();
            Ok(true)
        } else {
            info!(self.log, "All extents match");
            Ok(false)
        }
    }

    /// Take a hashmap with extents we need to fix and convert that to
    /// a queue of Crucible messages we need to execute to perform the fix.
    ///
    /// The order of messages in the queue shall be the order they are
    /// performed, and no message can start until the previous message
    /// has been ack'd by all three downstairs.
    fn convert_rc_to_messages(
        &mut self,
        mut rec_list: HashMap<usize, ExtentFix>,
        max_flush: u64,
        max_gen: u64,
    ) {
        let mut rep_id = ReconciliationId(0);
        info!(self.log, "Full repair list: {:?}", rec_list);
        for (ext, ef) in rec_list.drain() {
            /*
             * For each extent needing repair, we put the following
             * tasks on the reconcile task list.
             * Flush (the source) extent with latest gen/flush#.
             * Close extent (on all ds)
             * Send repair command to bad extents
             * Reopen extent.
             */
            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentFlush {
                    repair_id: rep_id,
                    extent_id: ext,
                    client_id: ef.source,
                    flush_number: max_flush,
                    gen_number: max_gen,
                },
            ));
            rep_id.0 += 1;

            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id.0 += 1;

            let repair = self.clients[ef.source].repair_addr.unwrap();
            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentRepair {
                    repair_id: rep_id,
                    extent_id: ext,
                    source_client_id: ef.source,
                    source_repair_address: repair,
                    dest_clients: ef.dest,
                },
            ));
            rep_id.0 += 1;

            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentReopen {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id.0 += 1;
        }

        info!(self.log, "Task list: {:?}", self.reconcile_task_list);
    }

    /// Takes the next task from `self.reconcile_task_list` and runs it
    ///
    /// The resulting work is stored in `self.reconcile_current_work`
    ///
    /// Returns `true` if reconciliation is complete (i.e. the task list is
    /// empty).
    ///
    /// # Panics
    /// If `self.reconcile_current_work` is not `None`
    pub(crate) async fn send_next_reconciliation_req(&mut self) -> bool {
        assert!(self.reconcile_current_work.is_none());
        let Some(mut next) = self.reconcile_task_list.pop_front() else {
            info!(self.log, "done with reconciliation");
            return true;
        };
        for c in self.clients.iter_mut() {
            c.send_next_reconciliation_req(&mut next).await;
        }
        self.reconcile_current_work = Some(next);
        false
    }

    /// Handles an ack from a repair job
    ///
    /// Returns `true` if reconciliation is complete
    ///
    /// If any of the downstairs clients are in an invalid state for
    /// reconciliation to continue, mark them as `FailedRepair` and restart
    /// them, clearing out the current reconciliation state.
    pub(crate) async fn on_reconciliation_ack(
        &mut self,
        client_id: ClientId,
        m: Message,
        up_state: &UpstairsState,
    ) -> bool {
        // Check to make sure that we're still in a repair-ready state
        //
        // If any client have dropped out of repair-readiness (e.g. due to
        // failed reconciliation, timeouts, etc), then we have to kick
        // everything else back to the beginning.
        if self.clients.iter().any(|c| c.state() != DsState::Repair) {
            // Something has changed, so abort this repair.
            // Mark any downstairs that have not changed as failed and disable
            // them so that they restart.
            self.abort_reconciliation(up_state).await;
            return false;
        }

        let Some(next) = self.reconcile_current_work.as_mut() else {
            // XXX what if we get a delayed ack?
            panic!("got reconciliation ack with no current work"); 
        };
        let Message::RepairAckId {repair_id} = m else {
            panic!("invalid message {m:?} for on_reconciliation_ack");
        };

        if self.clients[client_id].on_reconciliation_job_done(repair_id, next) {
            self.reconcile_current_work = None;
            self.send_next_reconciliation_req().await
        } else {
            false
        }
    }

    /// Handles an `ExtentError` message during reconciliation
    ///
    /// Right now, we completely abort the repair operation on all clients if
    /// this happens, and let the Upstairs sort it out once the IO tasks close.
    pub(crate) async fn on_reconciliation_failed(
        &mut self,
        client_id: ClientId,
        m: Message,
        up_state: &UpstairsState,
    ) {
        let Message::ExtentError { repair_id, extent_id, error } = m else {
            panic!("invalid message {m:?}");
        };
        error!(
            self.clients[client_id].log,
            "extent {extent_id} error on job {repair_id}: {error}"
        );
        self.abort_reconciliation(up_state).await;
    }

    async fn abort_reconciliation(&mut self, up_state: &UpstairsState) {
        warn!(self.log, "aborting reconciliation");
        // Something has changed, so abort this repair.
        // Mark any downstairs that have not changed as failed and disable
        // them so that they restart.
        for (i, c) in self.clients.iter_mut().enumerate() {
            if c.state() == DsState::Repair {
                // Restart the IO task.  This will cause the Upstairs to
                // deactivate through a ClientAction::TaskStopped.
                c.set_failed_repair(up_state).await;
                error!(self.log, "Mark {} as FAILED REPAIR", i);
            }
        }
        info!(self.log, "Clear out existing repair work queue");
        self.reconcile_task_list = VecDeque::new();
        self.reconcile_current_work = None;
    }

    /// Asserts that initial reconciliation is done, and sets clients as Active
    ///
    /// # Panics
    /// If that isn't the case!
    pub(crate) fn on_reconciliation_done(&mut self, from_state: DsState) {
        assert_eq!(self.ds_active.len(), 0);
        assert_eq!(self.reconcile_task_list.len(), 0);

        for (i, c) in self.clients.iter_mut().enumerate() {
            assert_eq!(c.state(), from_state, "invalid state for client {i}");
            c.set_active();
        }
    }

    /// Compares region metadata from all three clients and builds a mend list
    ///
    /// # Panics
    /// If any downstairs client does not have region metadata populated
    fn mismatch_list(&self) -> Option<DownstairsMend> {
        let c = |i| {
            self.clients[ClientId::new(i)]
                .region_metadata
                .as_ref()
                .unwrap()
        };

        let log = self.log.new(o!("" => "mend".to_string()));
        DownstairsMend::new(c(0), c(1), c(2), log)
    }

    pub(crate) fn submit_flush(
        &mut self,
        gw_id: u64,
        flush_id: u64,
        gen: u64,
        snapshot_details: Option<SnapshotDetails>,
    ) -> JobId {
        let next_id = self.next_id();
        let dep = self.ds_active.deps_for_flush(next_id);
        debug!(self.log, "IO Flush {} has deps {:?}", next_id, dep);

        /*
         * TODO: Walk the list of guest work structs and build the same list
         * and make sure it matches.
         */

        let extent_under_repair =
            self.get_extent_under_repair().map(|v| *v.end() as usize);

        /*
         * Build the flush request, and take note of the request ID that
         * will be assigned to this new piece of work.
         */
        let fl = crate::create_flush(
            next_id,
            dep,
            flush_id,
            gw_id,
            gen,
            snapshot_details,
            extent_under_repair,
        );

        self.enqueue(fl);
        next_id
    }

    /// Reserves repair IDs if impacted blocks overlap our extent under repair
    fn check_repair_ids_for_range(&mut self, impacted_blocks: ImpactedBlocks) {
        let Some(eur) = self.get_extent_under_repair() else { return; };
        let mut future_repair = false;
        for eid in impacted_blocks.extents().into_iter().flatten() {
            if eid == *eur.start() {
                future_repair = true;
            } else if eid > *eur.start() && (eid <= *eur.end() || future_repair)
            {
                self.reserve_repair_ids_for_extent(eid);
                future_repair = true;
            }
        }
    }

    /// Reserve some job IDs for future repair work on the given extent
    ///
    /// This is a no-op if repair IDs were already reserved for this extent
    fn reserve_repair_ids_for_extent(&mut self, eid: u64) {
        if self.repair_job_ids.contains_key(&eid) {
            debug!(
                self.log,
                "reserve already has existing job ids for {}", eid
            );
            return;
        }

        debug!(
            self.log,
            "Reserve Created new job ids for {}, save them", eid
        );
        let close_id = self.next_id();
        let repair_id = self.next_id();
        let noop_id = self.next_id();
        let reopen_id = self.next_id();
        let repair_ids = ExtentRepairIDs {
            close_id,
            repair_id,
            noop_id,
            reopen_id,
        };
        let deps = self.ds_active.deps_for_repair(repair_ids, eid);
        info!(
            self.log,
            "inserting repair IDs {repair_ids:?}; got dep {deps:?}"
        );
        self.repair_job_ids.insert(eid, (repair_ids, deps));
    }

    /// Create and submit a read job to the three clients
    pub(crate) fn submit_read(
        &mut self,
        guest_id: u64,
        blocks: ImpactedBlocks,
        ddef: RegionDefinition,
    ) -> JobId {
        // If there is a live-repair in progress that intersects with this read,
        // then reserve job IDs for those jobs.
        self.check_repair_ids_for_range(blocks);

        /*
         * Create the tracking info for downstairs request numbers (ds_id) we
         * will create on behalf of this guest job.
         */
        let ds_id = self.next_id();

        let dependencies = self.ds_active.deps_for_read(ds_id, blocks);
        debug!(self.log, "IO Read  {} has deps {:?}", ds_id, dependencies);

        /*
         * Now create a downstairs work job for each (eid, bo) returned
         * from extent_from_offset.
         */
        let mut requests: Vec<ReadRequest> =
            Vec::with_capacity(blocks.len(&ddef));

        for (eid, offset) in blocks.blocks(&ddef) {
            requests.push(ReadRequest { eid, offset });
        }

        let aread = IOop::Read {
            dependencies,
            requests,
        };

        let io = DownstairsIO {
            ds_id,
            guest_id,
            work: aread,
            state: ClientData::new(IOState::New),
            ack_status: AckStatus::NotAcked,
            replay: false,
            data: None,
            read_response_hashes: Vec::new(),
        };

        self.enqueue(io);

        ds_id
    }

    pub(crate) fn submit_write(
        &mut self,
        guest_id: u64,
        blocks: ImpactedBlocks,
        writes: Vec<crucible_protocol::Write>,
        is_write_unwritten: bool,
    ) -> JobId {
        // If there is a live-repair in progress that intersects with this read,
        // then reserve job IDs for those jobs.
        self.check_repair_ids_for_range(blocks);

        let ds_id = self.next_id();
        let dependencies = self.ds_active.deps_for_write(ds_id, blocks);
        cdt::gw__write__deps!(|| (
            self.ds_active.len() as u64,
            dependencies.len() as u64
        ));
        debug!(self.log, "IO Write {} has deps {:?}", ds_id, dependencies);

        let awrite = if is_write_unwritten {
            IOop::WriteUnwritten {
                dependencies,
                writes,
            }
        } else {
            IOop::Write {
                dependencies,
                writes,
            }
        };
        let io = DownstairsIO {
            ds_id,
            guest_id,
            work: awrite,
            state: ClientData::new(IOState::New),
            ack_status: AckStatus::NotAcked,
            replay: false,
            data: None,
            read_response_hashes: Vec::new(),
        };
        self.enqueue(io);
        ds_id
    }

    fn enqueue(&mut self, mut io: DownstairsIO) {
        let mut skipped = 0;
        let last_repair_id =
            self.repair_job_ids.last_key_value().map(|(k, _)| *k);

        // Send the job to each client!
        for cid in ClientId::iter() {
            let job_state = self.clients[cid].enqueue(&mut io, last_repair_id);
            if matches!(job_state, IOState::Skipped) {
                skipped += 1;
            }
        }

        // If this is a write (which will be fast-acked), increment our byte
        // counter for backpressure calculations.
        match &io.work {
            IOop::Write { writes, .. }
            | IOop::WriteUnwritten { writes, .. } => {
                self.write_bytes_outstanding +=
                    writes.iter().map(|w| w.data.len() as u64).sum::<u64>();
            }
            _ => (),
        };
        let is_write = matches!(io.work, IOop::Write { .. });

        // Puts the IO onto the downstairs work queue.
        let ds_id = io.ds_id;
        self.ds_active.insert(ds_id, io);

        if skipped == 3 {
            warn!(self.log, "job {} skipped on all downstairs", &ds_id);
        }

        if skipped == 3 || is_write {
            let mut handle = self.ds_active.get_mut(&ds_id).unwrap();
            let job = handle.job();
            assert_eq!(job.ack_status, AckStatus::NotAcked);
            job.ack_status = AckStatus::AckReady;
        }
    }

    /// Returns the current extent under repair (from `self.extent_limit`)
    ///
    /// # Panics
    /// If the different downstairs have different extents under repair (which
    /// is not allowed)
    fn get_extent_under_repair(&self) -> Option<std::ops::RangeInclusive<u64>> {
        let mut extent_under_repair = None;
        for cid in ClientId::iter() {
            if let Some(eur) = self.clients[cid].extent_limit {
                if extent_under_repair.is_none() {
                    extent_under_repair = Some(eur as u64);
                } else {
                    // We only support one extent being repaired at a time
                    assert_eq!(Some(eur as u64), extent_under_repair);
                }
            }
        }
        extent_under_repair.map(|extent_under_repair| {
            extent_under_repair
                ..=self
                    .repair_job_ids
                    .last_key_value()
                    .map(|(k, _)| *k)
                    .unwrap_or(extent_under_repair)
        })
    }

    pub(crate) async fn replace(
        &mut self,
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
        up_state: &UpstairsState,
    ) -> Result<ReplaceResult, CrucibleError> {
        warn!(
            self.log,
            "{id} request to replace downstairs {old} with {new}"
        );
        warn!(
            self.log,
            "{id} request to replace downstairs {old} with {new}"
        );

        // We check all targets first to not only find our current target,
        // but to be sure our new target is not an already active target
        // for a different downstairs.
        let mut new_client_id: Option<ClientId> = None;
        let mut old_client_id: Option<ClientId> = None;
        for (i, c) in self.clients.iter().enumerate() {
            if c.target_addr == Some(new) {
                new_client_id = Some(ClientId::new(i as u8));
                info!(self.log, "{id} found new target: {new} at {i}");
            }
            if c.target_addr == Some(old) {
                old_client_id = Some(ClientId::new(i as u8));
                info!(self.log, "{id} found old target: {old} at {i}");
            }
        }

        if let Some(new_client_id) = new_client_id {
            // Our new downstairs already exists.
            if old_client_id.is_some() {
                // New target is present, but old is present too, so this is not
                // a valid replacement request.
                return Err(CrucibleError::ReplaceRequestInvalid(format!(
                    "Both old {old} and new {new} targets are in use",
                )));
            }

            // We don't really know if the "old" matches what was old,
            // as that info is gone to us now, so assume it was true.
            match self.clients[new_client_id].state() {
                DsState::Replacing
                | DsState::Replaced
                | DsState::LiveRepairReady
                | DsState::LiveRepair => {
                    // These states indicate a replacement is in progress.
                    return Ok(ReplaceResult::StartedAlready);
                }
                _ => {
                    // Any other state, we assume it is done.
                    return Ok(ReplaceResult::CompletedAlready);
                }
            }
        }

        // We put the check for the old downstairs after checking for the
        // new because we want to be able to check if a replacement has
        // already happened and return status for that first.
        let Some(old_client_id) = old_client_id else {
            warn!(self.log, "{id} downstairs {old} not found");
            return Ok(ReplaceResult::Missing);
        };

        // Check for and Block a replacement if any (other) downstairs are
        // in any of these states as we don't want to take more than one
        // downstairs offline at the same time.
        for client_id in ClientId::iter() {
            if client_id == old_client_id {
                continue;
            }
            match self.clients[client_id].state() {
                DsState::Replacing
                | DsState::Replaced
                | DsState::LiveRepairReady
                | DsState::LiveRepair => {
                    return Err(CrucibleError::ReplaceRequestInvalid(format!(
                        "Replace {old} failed, downstairs {client_id} is {:?}",
                        self.clients[client_id].state(),
                    )));
                }
                _ => {}
            }
        }

        // Now we have found our old downstairs, verified the new is not in use
        // elsewhere, verified no other downstairs are in a bad state, we can
        // move forward with the replacement.
        info!(self.log, "{id} replacing old: {old} at {old_client_id}");

        // Skip all outstanding jobs for this client
        self.skip_all_jobs(old_client_id);

        // Clear the client state and restart the IO task
        self.clients[old_client_id].replace(up_state, new).await;

        Ok(ReplaceResult::Started)
    }

    /// Move all `New` and `InProgress` jobs for the given client to `Skipped`
    ///
    /// This may lead to jobs being marked as ackable, since a skipped job
    /// counts as complete in some circumstances.
    fn skip_all_jobs(&mut self, client_id: ClientId) {
        info!(
            self.log,
            "[{client_id}] client skip {} in process jobs because fault",
            self.ds_active.len(),
        );

        let mut retire_check = vec![];
        let mut number_jobs_skipped = 0;

        self.ds_active.for_each(|ds_id, job| {
            let state = &job.state[client_id];

            if matches!(state, IOState::InProgress | IOState::New) {
                self.clients[client_id].skip_job(job);
                number_jobs_skipped += 1;

                // Check to see if this being skipped means we can ACK
                // the job back to the guest.
                if job.ack_status == AckStatus::Acked {
                    // Push this onto a queue to do the retire check when
                    // we aren't doing a mutable iteration.
                    retire_check.push(*ds_id);
                } else if job.ack_status == AckStatus::NotAcked {
                    let wc = job.state_count();
                    if (wc.error + wc.skipped + wc.done) == 3 {
                        info!(
                            self.log,
                            "[{}] notify = true for {}", client_id, ds_id
                        );
                        job.ack_status = AckStatus::AckReady;
                    }
                } else {
                    info!(
                        self.log,
                        "[{}] job {} middle: {}",
                        client_id,
                        ds_id,
                        job.ack_status
                    );
                }
            }
        });

        info!(
            self.log,
            "[{}] changed {} jobs to fault skipped",
            client_id,
            number_jobs_skipped
        );

        for ds_id in retire_check {
            self.retire_check(ds_id);
        }

        // We have eliminated all of our jobs in IOState::New above; flush
        // our cache to reflect that.
        self.clients[client_id].clear_new_jobs();

        // As this downstairs is now faulted, we clear the extent_limit.
        self.clients[client_id].extent_limit = None;
    }

    /// Check if an active job is a flush or not.
    fn is_flush(&self, ds_id: JobId) -> bool {
        let job = self.ds_active.get(&ds_id).expect("checked missing job");

        matches!(job.work, IOop::Flush { .. })
    }

    /// This request is now complete on all peers, but is it ready to retire?
    /// Only when a flush is complete on all three downstairs do we check to
    /// see if we can remove jobs. Double check that all write jobs have
    /// finished and panic if not.
    ///
    /// Note we don't retire jobs until all three downstairs have returned
    /// from the same flush because the Upstairs replays all jobs since
    /// the last flush if a downstairs goes away and then comes back.
    /// This includes reads because they can be in the deps list for
    /// writes and if they aren't included in replay then the write will
    /// never start.
    fn retire_check(&mut self, ds_id: JobId) {
        if !self.is_flush(ds_id) {
            return;
        }

        // Only a completed flush will remove jobs from the active queue -
        // currently we have to keep everything around for use during replay
        let wc = self.ds_active.get(&ds_id).unwrap().state_count();
        if (wc.error + wc.skipped + wc.done) == 3 {
            assert!(!self.completed.contains(&ds_id));
            assert_eq!(wc.active, 0);

            // Retire all the jobs that happened before and including this
            // flush, with a few exceptions.  Because we can't iterate and
            // modify the list simultaneously, we mark to-be-retired jobs in
            // `retired`, then remove them in bulk after checking the list.
            let mut retired = Vec::new();

            for (&id, job) in &self.ds_active {
                if id > ds_id {
                    break;
                };
                assert!(id <= ds_id);

                // While we don't expect any jobs to still be in progress,
                // there is nothing to prevent a flush ACK from getting
                // ahead of the ACK from something that flush depends on.
                // The downstairs does handle the dependency.
                let wc = job.state_count();
                if wc.active != 0 || job.ack_status != AckStatus::Acked {
                    warn!(
                        self.log,
                        "[rc] leave job {} on the queue when removing {} {:?}",
                        job.ds_id,
                        ds_id,
                        wc,
                    );
                    continue;
                }

                // Assert the job is actually done, then complete it
                assert_eq!(wc.error + wc.skipped + wc.done, 3);
                assert!(!self.completed.contains(&id));
                assert_eq!(job.ack_status, AckStatus::Acked);
                assert_eq!(job.ds_id, id);

                retired.push(job.ds_id);
                self.completed.push(id);
                let summary = job.io_summarize();
                self.completed_jobs.push(summary);
                for cid in ClientId::iter() {
                    let old_state = &job.state[cid];
                    self.clients[cid].io_state_count.decr(old_state);
                }
            }
            // Now that we've collected jobs to retire, remove them from the map
            for &id in &retired {
                let job = self.ds_active.remove(&id);
                // Update pending bytes when this job is retired
                match &job.work {
                    IOop::Write { writes, .. }
                    | IOop::WriteUnwritten { writes, .. } => {
                        self.write_bytes_outstanding = self
                            .write_bytes_outstanding
                            .checked_sub(
                                writes
                                    .iter()
                                    .map(|w| w.data.len() as u64)
                                    .sum::<u64>(),
                            )
                            .unwrap();
                    }
                    _ => (),
                }
            }

            debug!(self.log, "[rc] retire {} clears {:?}", ds_id, retired);
            // Only keep track of skipped jobs at or above the flush.
            for cid in ClientId::iter() {
                self.clients[cid].skipped_jobs.retain(|&x| x >= ds_id);
            }
        }
    }

    /// Returns the number of active jobs
    pub(crate) fn active_count(&self) -> usize {
        self.ds_active.len()
    }

    /// Prints a summary of active work to `stdout`
    pub(crate) fn show_all_work(&self) {
        println!(
            "{0:>5} {1:>8} {2:>5} {3:>7} {4:>7} {5:>5} {6:>5} {7:>5} {8:>7}",
            "GW_ID",
            "ACK",
            "DSID",
            "TYPE",
            "BLOCKS",
            "DS:0",
            "DS:1",
            "DS:2",
            "REPLAY",
        );

        for (id, job) in &self.ds_active {
            let ack = job.ack_status;

            let (job_type, num_blocks): (String, usize) = match &job.work {
                IOop::Read {
                    dependencies: _dependencies,
                    requests,
                } => {
                    let job_type = "Read".to_string();
                    let num_blocks = requests.len();
                    (job_type, num_blocks)
                }
                IOop::Write {
                    dependencies: _dependencies,
                    writes,
                } => {
                    let job_type = "Write".to_string();
                    let mut num_blocks = 0;

                    for write in writes {
                        let block_size = write.offset.block_size_in_bytes();
                        num_blocks += write.data.len() / block_size as usize;
                    }

                    (job_type, num_blocks)
                }
                IOop::WriteUnwritten {
                    dependencies: _dependencies,
                    writes,
                } => {
                    let job_type = "WriteU".to_string();
                    let mut num_blocks = 0;

                    for write in writes {
                        let block_size = write.offset.block_size_in_bytes();
                        num_blocks += write.data.len() / block_size as usize;
                    }

                    (job_type, num_blocks)
                }
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                    gen_number: _gen_number,
                    snapshot_details: _,
                    extent_limit: _,
                } => {
                    let job_type = "Flush".to_string();
                    (job_type, 0)
                }
                IOop::ExtentClose {
                    dependencies: _,
                    extent,
                } => {
                    let job_type = "EClose".to_string();
                    (job_type, *extent)
                }
                IOop::ExtentFlushClose {
                    dependencies: _,
                    extent,
                    flush_number: _,
                    gen_number: _,
                    source_downstairs: _,
                    repair_downstairs: _,
                } => {
                    let job_type = "FClose".to_string();
                    (job_type, *extent)
                }
                IOop::ExtentLiveRepair {
                    dependencies: _,
                    extent,
                    source_downstairs: _,
                    source_repair_address: _,
                    repair_downstairs: _,
                } => {
                    let job_type = "Repair".to_string();
                    (job_type, *extent)
                }
                IOop::ExtentLiveReopen {
                    dependencies: _,
                    extent,
                } => {
                    let job_type = "Reopen".to_string();
                    (job_type, *extent)
                }
                IOop::ExtentLiveNoOp { dependencies: _ } => {
                    let job_type = "NoOp".to_string();
                    (job_type, 0)
                }
            };

            print!(
                "{0:>5} {1:>8} {2:>5} {3:>7} {4:>7}",
                job.guest_id, ack, id, job_type, num_blocks
            );

            for cid in ClientId::iter() {
                let state = &job.state[cid];
                // XXX I have no idea why this is two spaces instead of
                // one...
                print!("  {0:>5}", state);
            }
            print!(" {0:>6}", job.replay);

            println!();
        }
        self.io_state_count.show_all();
        print!("Last Flush: ");
        for c in self.clients.iter() {
            print!("{} ", c.last_flush());
        }
        println!();
    }

    /// Prints the last `n` completed jobs to `stdout`
    pub(crate) fn print_last_completed(&self, n: usize) {
        // TODO this is a ringbuffer, why are we turning it to a Vec to look at
        // the last five items?
        let done = self.completed.to_vec();
        for j in done.iter().rev().take(n) {
            print!(" {:4}", j);
        }
    }

    pub(crate) fn process_io_completion(
        &mut self,
        client_id: ClientId,
        m: Message,
        encryption_context: &Option<Arc<EncryptionContext>>,
        up_state: &UpstairsState,
    ) -> Result<(), CrucibleError> {
        let (upstairs_id, session_id, ds_id, read_data, extent_info) = match &m
        {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__write__io__done!(|| (job_id.0, client_id.get()));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    None,
                )
            }
            Message::WriteUnwrittenAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__write__unwritten__io__done!(|| (
                    job_id.0,
                    client_id.get()
                ));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    None,
                )
            }
            Message::FlushAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__flush__io__done!(|| (job_id.0, client_id.get()));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    None,
                )
            }
            Message::ReadResponse {
                upstairs_id,
                session_id,
                job_id,
                responses,
            } => {
                cdt::ds__read__io__done!(|| (job_id.0, client_id.get()));
                (*upstairs_id, *session_id, *job_id, responses.clone(), None)
            }

            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                // Take the result from the live close and store it away.
                let extent_info = match result {
                    Ok((g, f, d)) => {
                        debug!(
                            self.log,
                            "[{}] ELC got g:{} f:{} d:{}", client_id, g, f, d
                        );
                        Some(ExtentInfo {
                            generation: *g,
                            flush_number: *f,
                            dirty: *d,
                        })
                    }
                    Err(e) => {
                        panic!(
                            "[{}] ELC-Ack {} returned error {:?}",
                            client_id, job_id, e
                        );
                    }
                };
                cdt::ds__close__done!(|| (job_id.0, client_id.get()));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    extent_info,
                )
            }
            Message::ExtentLiveAckId {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__noop__done!(|| (job_id.0, client_id.get()));
                cdt::ds__reopen__done!(|| (job_id.0, client_id.get()));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    None,
                )
            }
            Message::ExtentLiveRepairAckId {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__repair__done!(|| (job_id.0, client_id.get()));
                (
                    *upstairs_id,
                    *session_id,
                    *job_id,
                    result.clone().map(|_| Vec::new()),
                    None,
                )
            }
            Message::ErrorReport {
                upstairs_id,
                session_id,
                job_id,
                error,
            } => {
                // The Upstairs should not consider a job completed until it has
                // returned an Ok result, and should therefore log and eat all
                // ErrorReport messages here. This will change in the future when
                // the Upstairs tracks the number of errors per Downstairs, and acts
                // on that information somehow.
                error!(
                    self.clients[client_id].log,
                    "job id {} saw error {:?}", job_id, error
                );

                // However, there is one case (see `check_message_for_abort` in
                // downstairs/src/lib.rs) where the Upstairs **does** need to
                // act: when a repair job in the Downstairs fails, that Downstairs
                // aborts itself and reconnects.
                if let Some(job) = self.ds_active.get(job_id) {
                    if job.work.is_repair() {
                        // Return the error and let the previously written error
                        // processing code work.
                        cdt::ds__repair__done!(|| (job_id.0, client_id.get()));
                        (
                            *upstairs_id,
                            *session_id,
                            *job_id,
                            Err(error.clone()),
                            None,
                        )

                        // XXX return Ok(()) here to make the upstairs stuck in
                        // test_error_during_live_repair_no_halt
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
            m => panic!("called on_io_completion with invalid message {m:?}"),
        };

        if self.upstairs_id != upstairs_id {
            warn!(
                self.clients[client_id].log,
                "upstairs_id {:?} != job {} upstairs_id {:?}!",
                self.upstairs_id,
                ds_id,
                upstairs_id,
            );

            return Err(CrucibleError::UuidMismatch);
        }

        if self.session_id != session_id {
            warn!(
                self.clients[client_id].log,
                "[{}] u.session_id {:?} != job {} session_id {:?}!",
                client_id,
                self.session_id,
                ds_id,
                session_id,
            );

            return Err(CrucibleError::UuidMismatch);
        }

        /*
         * We can finish the job if the downstairs has gone away, but
         * not if it has gone away then come back, because when it comes
         * back, it will have to replay everything.
         * While the downstairs is away, it's OK to act on the result that
         * we already received, because it may never come back.
         */
        let ds_state = self.clients[client_id].state();
        match ds_state {
            DsState::Active | DsState::Repair | DsState::LiveRepair => {}
            DsState::Faulted => {
                error!(
                    self.clients[client_id].log,
                    "Dropping job {}, this downstairs is faulted", ds_id,
                );
                return Err(CrucibleError::NoLongerActive);
            }
            _ => {
                warn!(
                    self.clients[client_id].log,
                    "{} WARNING finish job {} when downstairs state: {}",
                    self.upstairs_id,
                    ds_id,
                    ds_state
                );
            }
        }

        self.process_io_completion_inner(
            ds_id,
            client_id,
            read_data,
            encryption_context,
            up_state,
            extent_info,
        );

        // Decide what to do when we have an error from this IO.
        // Mark this downstairs as bad if this was a write or flush
        match self.client_error(ds_id, client_id) {
            Some(CrucibleError::UpstairsInactive) => {
                error!(
                    self.log,
                    "Saw CrucibleError::UpstairsInactive on client {}!",
                    client_id
                );
                self.clients[client_id]
                    .checked_state_transition(up_state, DsState::Disabled);
            }
            Some(CrucibleError::DecryptionError) => {
                // We should always be able to decrypt the data.  If we
                // can't, then we have the wrong key, or the data (or key)
                // is corrupted.
                error!(
                    self.clients[client_id].log,
                    "Authenticated decryption failed on job: {:?}", ds_id
                );
                panic!(
                    "[{}] Authenticated decryption failed on job: {:?}",
                    client_id, ds_id
                );
            }
            Some(CrucibleError::SnapshotExistsAlready(_)) => {
                // This is fine, nothing to worry about
            }
            _ => {
                let Some(job) = self.ds_active.get(&ds_id) else {
                    panic!("I don't think we should be here");
                };
                if matches!(
                    job.work,
                    IOop::Write { .. }
                        | IOop::Flush { .. }
                        | IOop::WriteUnwritten { .. }
                        | IOop::ExtentFlushClose { .. }
                        | IOop::ExtentLiveRepair { .. }
                        | IOop::ExtentLiveNoOp { .. }
                        | IOop::ExtentLiveReopen { .. }
                ) {
                    // This error means the downstairs will go to Faulted.
                    // Walk the active job list and mark any that were
                    // new or in progress to skipped.
                    self.skip_all_jobs(client_id);
                    self.clients[client_id]
                        .checked_state_transition(up_state, DsState::Faulted);
                }
            }
        }
        Ok(())
    }

    /// Returns a client error associated with the given job
    ///
    /// # Panics
    /// If that job is not active
    fn client_error(
        &self,
        ds_id: JobId,
        client_id: ClientId,
    ) -> Option<CrucibleError> {
        let Some(job) = self.ds_active.get(&ds_id) else {
            panic!("reqid {ds_id} is not active");
        };

        let state = &job.state[client_id];

        if let IOState::Error(e) = state {
            Some(e.clone())
        } else {
            None
        }
    }

    fn process_io_completion_inner(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        responses: Result<Vec<ReadResponse>, CrucibleError>,
        encryption_context: &Option<Arc<EncryptionContext>>,
        up_state: &UpstairsState,
        extent_info: Option<ExtentInfo>,
    ) {
        /*
         * Assume we don't have enough completed jobs, and only change
         * it if we have the exact amount required
         */
        let deactivate = matches!(up_state, UpstairsState::Deactivating);

        let Some(mut handle) = self.ds_active.get_mut(&ds_id) else {
            panic!("reqid {ds_id} is not active");
        };
        let job = handle.job();

        self.clients[client_id].process_io_completion(
            job,
            responses,
            encryption_context,
            deactivate,
            extent_info,
        );

        /*
         * If all 3 jobs are done, we can check here to see if we can
         * remove this job from the DS list. If we have completed the ack
         * to the guest, then there will be no more work on this job
         * but messages may still be unprocessed.
         */
        if job.ack_status == AckStatus::Acked {
            drop(handle);
            self.retire_check(ds_id);
        } else if job.ack_status == AckStatus::NotAcked {
            // If we reach this then the job probably has errors and
            // hasn't acked back yet. We check for NotAcked so we don't
            // double count three done and return true if we already have
            // AckReady set.
            let wc = job.state_count();

            // If we are a write or a flush with one success, then
            // we must switch our state to failed.  This condition is
            // handled in Downstairs::result()
            if (wc.error + wc.skipped + wc.done) == 3 {
                job.ack_status = AckStatus::AckReady;
                debug!(self.log, "[{}] Set AckReady {}", client_id, job.ds_id);
            }
        }
    }
}
