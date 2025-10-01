// Copyright 2023 Oxide Computer Company
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use crate::{
    cdt,
    client::{
        ClientAction, ClientFaultReason, ClientNegotiationFailed,
        ClientRunResult, ClientStopReason, DownstairsClient, EnqueueResult,
        NegotiationStateData, ShouldSendError,
    },
    format_job_list,
    guest::GuestBlockRes,
    io_limits::{IOLimitGuard, IOLimits},
    live_repair::ExtentInfo,
    notify::{NotifyQueue, NotifyRequest},
    stats::DownstairsStatOuter,
    upstairs::{UpstairsConfig, UpstairsState},
    AckStatus, ActiveJobs, AllocRingBuffer, BlockRes, Buffer, ClientData,
    ClientIOStateCount, ClientId, ClientMap, ConnectionMode, CrucibleError,
    DownstairsIO, DownstairsMend, DsState, DsStateData, ExtentFix,
    ExtentRepairIDs, IOState, IOStateCount, IOop, ImpactedBlocks, JobId,
    Message, NegotiationState, RawReadResponse, RawWrite, ReconcileIO,
    ReconciliationId, RegionDefinition, ReplaceResult, SnapshotDetails,
    WorkSummary,
};
use crucible_common::{BlockIndex, ExtentId, NegotiationError};
use crucible_protocol::WriteHeader;

use ringbuffer::RingBuffer;
use slog::{debug, error, info, o, warn, Logger};
use uuid::Uuid;

/// Downstairs data
///
/// This data structure is responsible for tracking outstanding jobs from the
/// perspective of the (3x) downstairs.  It contains a list of all active jobs,
/// as well as three `DownstairsClient` with per-client data.
pub(crate) struct Downstairs {
    /// Shared configuration
    cfg: Arc<UpstairsConfig>,

    /// Per-client data
    pub(crate) clients: ClientData<DownstairsClient>,

    /// Per-client backpressure configuration
    backpressure_config: DownstairsBackpressureConfig,

    /// The active list of IO for the downstairs.
    pub(crate) ds_active: ActiveJobs,

    /// Set of jobs that are active from the guest's perspective
    ///
    /// (i.e. have not yet been acked)
    pub(crate) gw_active: HashSet<JobId>,

    /// The next Job ID this Upstairs should use for downstairs work.
    next_id: JobId,

    /// Current flush number
    ///
    /// This starts as the highest flush number from all three downstairs on
    /// startup, and increments by one each time the guest sends a flush
    /// (including automatic flushes).
    next_flush: u64,

    /// Indicates whether we are eligible for replay
    ///
    /// We are only eligible for replay if all jobs since the last flush are
    /// buffered (i.e. none have been retired by a `Barrier` operation, or we
    /// have not performed or started a LiveRepair while this downstairs was
    /// offline).
    can_replay: bool,

    /// How many `Flush` or `Barrier` operations are pending?
    ///
    /// We only want to send a `Barrier` if there isn't already one pending, so
    /// we track it here (incrementing in `submit_flush` / `submit_barrier` and
    /// decrementing in `retire_check`).
    pending_barrier: usize,

    /// Ringbuf of recently acked job IDs.
    acked_ids: AllocRingBuffer<JobId>,

    /// Ringbuf of recently retired job IDs.
    retired_ids: AllocRingBuffer<JobId>,

    /// Ringbuf of a summary of each recently retired downstairs IO.
    retired_jobs: AllocRingBuffer<WorkSummary>,

    /// Data for an in-progress reconciliation
    reconcile: Option<ReconcileData>,

    /// Number of failing attempts to reconcile the downstairs
    ///
    /// This is stored outside of `reconcile` because it's an independent
    /// statistic that isn't associated with a particular activation.
    reconcile_repair_aborted: usize,

    /// Number of extents repaired during initial activation
    ///
    /// This is stored outside of `reconcile` because it can be read afterwards
    reconcile_repaired: usize,

    /// The logger for messages sent from downstairs methods.
    log: Logger,

    /// Data for an in-progress live repair
    repair: Option<LiveRepairData>,

    /// Region definition (copied from the upstairs)
    ddef: Option<RegionDefinition>,

    /// Handle for stats
    stats: DownstairsStatOuter,

    /// A queue to send notifications to Nexus
    notify: Option<NotifyQueue>,
}

/// Tuple storing a job and an optional result
#[derive(Debug)]
pub(crate) struct PendingJob {
    id: JobId,
    result: Option<Result<(), CrucibleError>>,
}

impl PendingJob {
    fn new(id: JobId) -> Self {
        Self { id, result: None }
    }
}

/// State machine for a live-repair operation
///
/// We pass through all states (except `FinalFlush`) in order for each extent,
/// then pass through the `FinalFlush` state before completion.  In each state,
/// we're waiting for a particular job to finish.  We store a `PendingJob` for
/// every pending job, because it may be possible for more than one to finish
/// before checking live-repair state.
#[derive(Debug)]
pub(crate) enum LiveRepairState {
    Closing {
        close_job: PendingJob,
        repair_job: PendingJob,
        noop_job: PendingJob,
        reopen_job: PendingJob,
    },
    Repairing {
        repair_job: PendingJob,
        noop_job: PendingJob,
        reopen_job: PendingJob,
    },
    Noop {
        noop_job: PendingJob,
        reopen_job: PendingJob,
    },
    Reopening {
        reopen_job: PendingJob,
    },
    FinalFlush {
        flush_job: PendingJob,
    },
}

impl LiveRepairState {
    fn dummy() -> Self {
        LiveRepairState::Noop {
            noop_job: PendingJob::new(JobId(0)),
            reopen_job: PendingJob::new(JobId(0)),
        }
    }

    /// Returns the result for the job that we're waiting on at the moment
    ///
    /// This is a consuming function; the result is removed from the object.
    #[must_use]
    fn active_job_result(&mut self) -> Option<Result<(), CrucibleError>> {
        match self {
            LiveRepairState::Closing { close_job: j, .. }
            | LiveRepairState::Repairing { repair_job: j, .. }
            | LiveRepairState::Noop { noop_job: j, .. }
            | LiveRepairState::Reopening { reopen_job: j, .. }
            | LiveRepairState::FinalFlush { flush_job: j } => j.result.take(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LiveRepairData {
    /// An ID uniquely identifying this repair
    id: Uuid,

    /// Total number of extents that need checking
    extent_count: u32,

    /// Extent being repaired
    pub active_extent: ExtentId,

    /// Minimum job ID the downstairs under repair needs to consider for deps
    min_id: JobId,

    /// Reserved live-repair job IDs
    ///
    /// If, while running live repair, we have an IO that spans repaired extents
    /// and not yet repaired extents, we will reserve job IDs for the future
    /// repair work and store them in this hash map.  When it comes time to
    /// start a repair and allocate the job IDs we will require, we first check
    /// this hash map to see if the IDs were already repaired.
    ///
    /// The key is the extent being repaired; the value is a tuple of reserved
    /// IDs and existing dependencies for the first job in the repair.
    repair_job_ids: BTreeMap<ExtentId, (ExtentRepairIDs, Vec<JobId>)>,

    /// Downstairs providing repair info
    source_downstairs: ClientId,

    /// Downstairs being repaired
    repair_downstairs: Vec<ClientId>,

    /// Repair is being aborted
    ///
    /// This means that new operations are replaced with `LiveRepairNoOp`, so
    /// that previously submitted operations that depend on them will still
    /// happen.  We'll stop repairing after the current extent (and all reserved
    /// jobs) are handled, then send a final flush.
    aborting_repair: bool,

    /// Current state
    state: LiveRepairState,
}

impl LiveRepairData {
    /// If the given job is one that we're waiting for, store its result
    fn on_job_complete(&mut self, ds_id: JobId, io: &DownstairsIO) {
        // Build an array of `Option<&mut PendingJob>` to check
        let jobs = match &mut self.state {
            LiveRepairState::Closing {
                close_job,
                repair_job,
                noop_job,
                reopen_job,
            } => [
                Some(close_job),
                Some(repair_job),
                Some(noop_job),
                Some(reopen_job),
            ],
            LiveRepairState::Repairing {
                repair_job,
                noop_job,
                reopen_job,
            } => [Some(repair_job), Some(noop_job), Some(reopen_job), None],
            LiveRepairState::Noop {
                noop_job,
                reopen_job,
            } => [Some(noop_job), Some(reopen_job), None, None],
            LiveRepairState::Reopening { reopen_job } => {
                [Some(reopen_job), None, None, None]
            }
            LiveRepairState::FinalFlush { flush_job } => {
                [Some(flush_job), None, None, None]
            }
        };
        for pending in jobs.into_iter().flatten() {
            if pending.id == ds_id {
                assert!(pending.result.is_none());
                pending.result = Some(io.result())
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct CollateData {
    max_flush: u64,
    max_gen: u64,
}

#[derive(Debug)]
pub(crate) struct ReconcileData {
    /// An ID uniquely identifying this reconciliation
    id: Uuid,

    /// Current piece of reconcile work that the downstairs are working on
    ///
    /// It can be `InProgress`, `Skipped`, or `Done`.
    current_work: Option<ReconcileIO>,

    /// Pending reconciliation work
    task_list: VecDeque<ReconcileIO>,

    /// Number of extents needing repair during initial activation
    reconcile_repair_needed: usize,
}

impl ReconcileData {
    fn new<V: Into<VecDeque<ReconcileIO>>>(task_list: V) -> Self {
        let task_list = task_list.into();
        Self {
            id: Uuid::new_v4(),
            current_work: None,
            reconcile_repair_needed: task_list.len(),
            task_list,
        }
    }
}

#[derive(Debug)]
pub(crate) enum DownstairsAction {
    /// We received a client action from the given client
    Client {
        client_id: ClientId,
        action: ClientAction,
    },
}

impl Downstairs {
    pub(crate) fn new(
        cfg: Arc<UpstairsConfig>,
        ds_target: ClientMap<SocketAddr>,
        tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
        stats: DownstairsStatOuter,
        io_limits: &IOLimits,
        notify: Option<NotifyQueue>,
        log: Logger,
    ) -> Self {
        let mut clients = [None, None, None];
        for i in ClientId::iter() {
            clients[i.get() as usize] = Some(DownstairsClient::new(
                i,
                cfg.clone(),
                ds_target.get(&i).copied(),
                io_limits.client_limits(i),
                log.new(o!("client" => i.get().to_string())),
                tls_context.clone(),
            ));
        }

        let clients = clients.map(Option::unwrap);
        Self {
            clients: ClientData(clients),
            backpressure_config: DownstairsBackpressureConfig {
                // start byte-based backpressure at 25 MiB of difference
                bytes_start: 25 * 1024 * 1024,
                // bytes_scale is chosen to have 1.5 ms of delay at 100 MiB of
                // discrepancy
                bytes_scale: 5e-7,

                // start job-based backpressure at 100 jobs of discrepancy
                jobs_start: 100,
                // job_scale is chosen to have 1 ms of of per-job delay at 900
                // jobs of discrepancy
                jobs_scale: 0.04,

                // max delay is 100 ms, chosen experimentally to keep downstairs
                // in sync even in heavily loaded systems
                max_delay: Duration::from_millis(100),
            },
            cfg,
            next_flush: 0,
            can_replay: true,
            pending_barrier: 0,
            ds_active: ActiveJobs::new(),
            gw_active: HashSet::new(),
            acked_ids: AllocRingBuffer::new(2048),
            retired_ids: AllocRingBuffer::new(2048),
            retired_jobs: AllocRingBuffer::new(8),
            next_id: JobId(1000),
            reconcile: None,
            reconcile_repaired: 0,
            reconcile_repair_aborted: 0,
            log: log.new(o!("" => "downstairs".to_string())),
            repair: None,
            ddef: None,
            stats,
            notify,
        }
    }

    /// Build a `Downstairs` for simple tests
    ///
    /// Note that this `Downstairs` does not have valid socket addresses, so the
    /// client tasks won't start!
    #[cfg(test)]
    pub fn test_default() -> Self {
        let log = crucible_common::build_logger();
        let cfg = Arc::new(UpstairsConfig {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            generation: std::sync::atomic::AtomicU64::new(1),
            read_only: false,
            encryption_context: None,
        });
        let stats = DownstairsStatOuter::default();

        // Build a set of fake IO limits that we won't hit
        let io_limits = IOLimits::new(u32::MAX as usize, u32::MAX as usize);

        let mut ds = Self::new(
            cfg,
            ClientMap::new(),
            None,
            stats,
            &io_limits,
            None,
            log,
        );

        // Create a fake repair address so this field is populated.
        for cid in ClientId::iter() {
            ds.clients[cid].repair_addr =
                Some("127.0.0.1:1234".parse().unwrap());
        }

        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(crate::Block::new_512(3));
        ddef.set_extent_count(u32::MAX);
        ds.ddef = Some(ddef);

        ds
    }

    /// Helper function to set all 3x clients as active, legally
    #[cfg(test)]
    pub fn force_active(&mut self) {
        let up_state = UpstairsState::GoActive(BlockRes::dummy());
        for cid in ClientId::iter() {
            for state in [
                NegotiationStateData::Start,
                NegotiationStateData::WaitForPromote,
                NegotiationStateData::WaitForRegionInfo,
                NegotiationStateData::GetExtentVersions,
                NegotiationStateData::WaitQuorum(Default::default()),
                NegotiationStateData::Reconcile,
            ] {
                self.clients[cid].checked_state_transition(
                    &up_state,
                    DsStateData::Connecting {
                        state,
                        mode: ConnectionMode::New,
                    },
                );
            }
            self.clients[cid]
                .checked_state_transition(&up_state, DsStateData::Active);
        }
    }

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

    /// Checks whether an ack and/or a retire check is necessary for the job
    fn ack_check(&mut self, ds_id: JobId, deactivate: bool) {
        let job = self.ds_active.get(&ds_id).unwrap();

        // Find the number of `Done` jobs, which determines when we ack back to
        // the Guest.  In addition, we always ack (and perform a retire check)
        // if the job is complete on all 3x downstairs.
        let okay = job
            .state
            .iter()
            .filter(|s| matches!(s, IOState::Done))
            .count();

        let complete = job.state.iter().all(|s| {
            matches!(s, IOState::Done | IOState::Error(..) | IOState::Skipped)
        });

        // Decide if we're ready to ack this job
        let ack_ready = match &job.work {
            IOop::Read { .. } => okay == 1,
            IOop::Write { .. } | IOop::WriteUnwritten { .. } => okay == 2,
            IOop::Flush {
                snapshot_details, ..
            } => {
                let n = if deactivate || snapshot_details.is_some() {
                    3
                } else {
                    2
                };
                okay == n
            }
            IOop::Barrier { .. }
            | IOop::ExtentFlushClose { .. }
            | IOop::ExtentLiveRepair { .. }
            | IOop::ExtentLiveReopen { .. }
            | IOop::ExtentLiveNoOp { .. } => okay == 3,
        };

        // Do logging and ack the job
        if !job.acked && (ack_ready || complete) {
            match &job.work {
                IOop::Flush { .. } => {
                    if deactivate {
                        debug!(self.log, "deactivate flush {ds_id} done");
                    }
                }
                IOop::ExtentFlushClose { .. } => {
                    debug!(self.log, "ExtentFlushClose {ds_id} AckReady");
                }
                IOop::ExtentLiveRepair { .. } => {
                    debug!(self.log, "ExtentLiveRepair AckReady {ds_id}");
                }
                IOop::ExtentLiveReopen { .. } => {
                    debug!(self.log, "ExtentLiveReopen AckReady {ds_id}");
                }
                IOop::ExtentLiveNoOp { .. } => {
                    debug!(self.log, "ExtentLiveNoOp AckReady {ds_id}");
                }
                _ => (),
            }
            self.ack_job(ds_id);
        } else if ack_ready && job.work.is_write() {
            // We already acked this job, but, we should update dtrace probes
            Self::update_io_done_stats(
                &self.stats,
                job.work.clone(),
                ds_id,
                job.io_size(),
            );
        }

        if complete {
            self.retire_check(ds_id);
        }
    }

    /// Send the ack for a single job back upstairs through `GuestWork`
    ///
    /// Update stats for the upstairs as well
    fn ack_job(&mut self, ds_id: JobId) {
        debug!(self.log, "ack_jobs process {}", ds_id);

        let done = self.ds_active.get_mut(&ds_id).unwrap();
        assert!(!done.acked);

        done.acked = true;
        let r = done.result();

        // Fire DTrace probes and update stats
        let io_size = done.io_size();
        let work = done.work.clone();
        Self::update_io_done_stats(&self.stats, work, ds_id, io_size);

        debug!(self.log, "[A] ack job {}", ds_id);

        if let Some(r) = &mut self.repair {
            r.on_job_complete(ds_id, done);
        }

        // Copy (if present) read data back to the guest buffer they
        // provided to us, and notify any waiters.
        if let Some(res) = done.res.take() {
            let data = done
                .data
                .as_mut()
                .map(|v| (v.blocks.as_slice(), &mut v.data));
            res.transfer_and_notify(data, r);
        }

        if self.gw_active.remove(&ds_id) {
            self.acked_ids.push(ds_id);
        } else {
            panic!("job {ds_id} not on gw_active list");
        }
    }

    /// Update oximeter stats for a write operation.
    pub fn update_write_done_metrics(&mut self, size: usize) {
        self.stats.add_write(size as i64);
    }

    /// Update dtrace and oximeter metrics for a completed IO
    pub fn update_io_done_stats(
        stats: &DownstairsStatOuter,
        work: IOop,
        ds_id: JobId,
        io_size: usize,
    ) {
        match work {
            IOop::Read { .. } => {
                cdt::gw__read__done!(|| (ds_id.0));
                stats.add_read(io_size as i64);
            }
            IOop::Write { .. } => {
                cdt::gw__write__done!(|| (ds_id.0));
                // We already updated metrics right after the fast ack.
            }
            IOop::WriteUnwritten { .. } => {
                cdt::gw__write__unwritten__done!(|| (ds_id.0));
                // We don't include WriteUnwritten operation in the
                // metrics for this guest.
            }
            IOop::Flush { .. } => {
                cdt::gw__flush__done!(|| (ds_id.0));
                stats.add_flush();
            }
            IOop::Barrier { .. } => {
                cdt::gw__barrier__done!(|| (ds_id.0));
                stats.add_barrier();
            }
            IOop::ExtentFlushClose { extent, .. } => {
                cdt::gw__close__done!(|| (ds_id.0, extent.0));
                stats.add_flush_close();
            }
            IOop::ExtentLiveRepair { extent, .. } => {
                cdt::gw__repair__done!(|| (ds_id.0, extent.0));
                stats.add_extent_repair();
            }
            IOop::ExtentLiveNoOp { .. } => {
                cdt::gw__noop__done!(|| (ds_id.0));
                stats.add_extent_noop();
            }
            IOop::ExtentLiveReopen { extent, .. } => {
                cdt::gw__reopen__done!(|| (ds_id.0, extent.0));
                stats.add_extent_reopen();
            }
        }
    }

    /// Helper function to calculate pruned deps for a given job
    #[cfg(test)]
    fn get_pruned_deps(&self, ds_id: JobId, client_id: ClientId) -> Vec<JobId> {
        let job = self.ds_active.get(&ds_id).unwrap();
        self.clients[client_id]
            .prune_deps(
                ds_id,
                job.work.clone(),
                self.repair.as_ref().map(|r| r.min_id),
            )
            .deps()
            .to_vec()
    }

    /// Reinitialize the given client
    pub(crate) fn reinitialize(
        &mut self,
        client_id: ClientId,
        up_state: &UpstairsState,
    ) {
        // Check whether we asked the IO task to stop ourselves
        let stopped_due_to_fault = matches!(
            self.clients[client_id].state(),
            DsState::Stopping(ClientStopReason::Fault(..))
        );

        // Restart the IO task for that specific client, transitioning to a new
        // state.
        self.clients[client_id].reinitialize(up_state, self.can_replay);

        // If the IO task stops on its own, then under certain circumstances,
        // we want to skip all of its jobs.  (If we requested that the IO task
        // stop, then whoever made that request is responsible for skipping jobs
        // if necessary).
        //
        // Specifically, we want to skip jobs if the only path back online for
        // that client goes through live-repair; if that client can come back
        // through replay, then the jobs must remain live.
        let now_faulted = matches!(
            self.clients[client_id].state(),
            DsState::Connecting {
                mode: ConnectionMode::Faulted,
                ..
            }
        );
        if now_faulted && !stopped_due_to_fault {
            self.skip_all_jobs(client_id);
        }

        for i in ClientId::iter() {
            // Clear per-client delay, because we're starting a new session
            self.clients[i].set_delay_us(0);
        }

        // Special-case: if a Downstairs goes away midway through initial
        // reconciliation, then we have to manually abort reconciliation.
        if self.clients.iter().any(|c| {
            matches!(
                c.state(),
                DsState::Connecting {
                    state: NegotiationState::Reconcile,
                    ..
                }
            )
        }) {
            self.abort_reconciliation(up_state);
        }
    }

    /// Returns true if we can deactivate immediately
    ///
    /// This is the case if there are no pending jobs in the queue.
    pub(crate) fn can_deactivate_immediately(&self) -> bool {
        // If there are no jobs, then we're immediately done!
        self.ds_active.is_empty()
    }

    /// Tries to deactivate the given client
    ///
    /// If successful, the client state will be set to `Deactivated` and the IO
    /// task will be stopped.  This will trigger a `ClientAction::TaskStopped`
    /// in the main task, which will in turn restart the IO task.
    ///
    /// Returns `true` on success, `false` otherwise
    ///
    /// # Panics
    /// If `up_state` is not `UpstairsState::Deactivating`
    pub(crate) fn try_deactivate(
        &mut self,
        client_id: ClientId,
        up_state: &UpstairsState,
    ) -> bool {
        assert!(
            matches!(up_state, UpstairsState::Deactivating { .. }),
            "up_state must be Deactivating, not {up_state:?}"
        );
        if self.ds_active.is_empty() {
            info!(self.log, "[{}] deactivate, no work so YES", client_id);
            self.clients[client_id].deactivate(up_state);
            return true;
        }
        // If this client is offline, then move it to faulted after skipping
        // all the outstanding jobs.  This will shut the door on this client
        // till the deactivation has finished on all downstairs and the
        // upstairs has completed deactivation.  After skipping jobs and
        // setting faulted, we return false here and let the faulting framework
        // take care of clearing out the skipped jobs.  This then allows the
        // requested deactivation to finish.
        if matches!(
            self.clients[client_id].state(),
            DsState::Connecting {
                mode: ConnectionMode::Offline,
                ..
            }
        ) {
            info!(self.log, "[{}] Offline client moved to Faulted", client_id);
            self.fault_client(
                client_id,
                up_state,
                ClientFaultReason::OfflineDeactivated,
            );
            return false;
        }
        // If there are jobs in the queue, then we have to check them!
        let last_id = self.ds_active.keys().next_back().unwrap();

        /*
         * The last job must be a flush.  It's possible to get
         * here right after deactivating is set, but before the final
         * flush happens.
         */
        if !self.is_flush(*last_id) {
            info!(
                self.log,
                "[{}] deactivate last job {} not flush, NO", client_id, last_id
            );
            return false;
        }
        /*
         * Now count our jobs.  Any job not done or skipped means
         * we are not ready to deactivate.
         */
        for (id, job) in &self.ds_active {
            let state = &job.state[client_id];
            if state == &IOState::InProgress {
                info!(
                    self.log,
                    "[{}] cannot deactivate, job {} in state {:?}",
                    client_id,
                    id,
                    state
                );
                return false;
            }
        }
        /*
         * To arrive here, we verified our most recent job is a flush, and
         * none of the jobs that are on our active job list are New or
         * InProgress (either error, skipped, or done)
         */
        info!(self.log, "[{}] check deactivate YES", client_id);
        self.clients[client_id].deactivate(up_state);
        true
    }

    /// Assign a new downstairs job ID.
    fn next_id(&mut self) -> JobId {
        let id = self.next_id;
        self.next_id.0 += 1;
        id
    }

    /// What would next_id return, if we called it?
    pub(crate) fn peek_next_id(&self) -> JobId {
        self.next_id
    }

    /// Sends all pending jobs for the given client
    ///
    /// Jobs are pending if they have not yet been flushed by this client.
    pub(crate) fn replay_jobs(&mut self, client_id: ClientId) {
        let lf = self.clients[client_id].last_flush();

        info!(
            self.log,
            "[{client_id}] client re-new {} jobs since flush {lf:?}",
            self.ds_active.len(),
        );

        let mut to_send = vec![];
        self.ds_active.for_each(|ds_id, job| {
            // We don't need to send anything before our last good flush
            if lf.is_some_and(|lf| *ds_id <= lf) {
                assert_eq!(IOState::Done, job.state[client_id]);
                return;
            }

            debug!(self.log, "REPLAYING {ds_id} on {client_id}");
            self.clients[client_id].replay_job(job);
            to_send.push((*ds_id, job.work.clone()));
        });
        let count = to_send.len();
        info!(
            self.log,
            "[{client_id}] Replayed {count} jobs since flush: {lf:?}"
        );
        for (ds_id, io) in to_send {
            self.send(ds_id, io, client_id);
        }
    }

    /// Compare downstairs region metadata and based on the results:
    ///
    /// Determine the global flush number for this region set.
    /// Verify the guest given gen number is highest.
    /// Decide if we need repair, and if so create the repair list
    ///
    /// Returns `true` if repair is needed, `false` otherwise
    pub(crate) fn collate(&mut self) -> Result<bool, NegotiationError> {
        let r = self.check_region_metadata()?;
        Ok(self.start_reconciliation(r))
    }

    /// Checks that region metadata is valid
    ///
    /// # Panics
    /// If no regions are in `NegotiationState::WaitQuorum`
    fn check_region_metadata(&self) -> Result<CollateData, NegotiationError> {
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
        for (cid, rec) in ClientId::iter().filter_map(|i| {
            if let DsStateData::Connecting {
                state: NegotiationStateData::WaitQuorum(r),
                ..
            } = self.clients[i].state_data()
            {
                Some((i, r))
            } else {
                None
            }
        }) {
            // We log the first 12 pieces of extent metadata
            const MAX_LOG: usize = 12;
            let mut flush_log = Vec::with_capacity(MAX_LOG);
            let mut gen_log = Vec::with_capacity(MAX_LOG);
            let mut dirty_log = Vec::with_capacity(MAX_LOG);

            for (i, m) in rec.iter().enumerate() {
                max_flush = max_flush.max(m.flush + 1);
                max_gen = max_gen.max(m.gen + 1);
                if i < MAX_LOG {
                    flush_log.push(m.flush);
                    gen_log.push(m.gen);
                    dirty_log.push(m.dirty);
                }
            }
            let slice = if rec.len() > MAX_LOG {
                format!("[0..{MAX_LOG}]")
            } else {
                "".to_owned()
            };
            info!(self.log, "[{cid}]R flush_numbers{slice}: {flush_log:?}",);
            info!(self.log, "[{cid}]R generation{slice}: {gen_log:?}",);
            info!(self.log, "[{cid}]R dirty{slice}: {dirty_log:?}",);
        }

        info!(self.log, "Max found gen is {}", max_gen);
        /*
         * Verify that the generation number that the guest has requested
         * is higher than what we have from the three downstairs.
         */
        let requested_gen = self.cfg.generation();
        if requested_gen == 0 {
            error!(self.log, "generation number should be at least 1");
            return Err(NegotiationError::GenerationZeroIsIllegal);
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
            return Err(NegotiationError::GenerationNumberTooLow {
                requested: requested_gen,
                actual: max_gen,
            });
        } else {
            info!(
                self.log,
                "Generation requested: {} >= found:{}", requested_gen, max_gen,
            );
        }

        Ok(CollateData { max_flush, max_gen })
    }

    /// Begins reconciliation, using the given collation data
    #[must_use]
    fn start_reconciliation(&mut self, data: CollateData) -> bool {
        let CollateData { max_flush, max_gen } = data;

        /*
         * Set the next flush ID so we have if we need to repair.
         */
        self.next_flush = max_flush;
        info!(self.log, "Next flush: {}", max_flush);

        /*
         * Determine what extents don't match and what to do
         * about that
         */
        if let Some(reconcile_list) = self.mismatch_list() {
            for c in self.clients.iter_mut() {
                c.begin_reconcile();
            }

            let task_list = self.convert_rc_to_messages(
                reconcile_list.mend,
                max_flush,
                max_gen,
            );

            let reconcile = ReconcileData::new(task_list);

            info!(
                self.log,
                "starting reconciliation {}: found {:?} extents that need repair",
                reconcile.id,
                reconcile.reconcile_repair_needed,
            );

            self.notify_reconcile_start(&reconcile);
            self.reconcile = Some(reconcile);
            self.reconcile_repaired = 0;

            true
        } else {
            info!(self.log, "All extents match");
            false
        }
    }

    /// Checks whether a live-repair is in progress
    pub(crate) fn live_repair_in_progress(&self) -> bool {
        // A live-repair is in progress if any client is in the LiveRepair
        // state, _or_ if `self.repair.is_some()`.  The latter is necessary
        // because if a live-repair encountered an error, that downstairs will
        // be marked as Faulted, but we still need to keep going through the
        // existing live-repair before retrying.
        self.clients
            .iter()
            .any(|c| c.state() == DsState::LiveRepair)
            || self.repair.is_some()
    }

    /// Tries to start live-repair
    ///
    /// This function is idempotent; it returns without doing anything if
    /// live-repair either can't be started or is already running.
    pub(crate) fn check_live_repair_start(&mut self, up_state: &UpstairsState) {
        // If we're already doing live-repair, then we can't start live-repair
        if self.live_repair_in_progress() {
            return;
        }

        // Begin setting up live-repair state
        let mut repair_downstairs = vec![];
        let mut source_downstairs = None;
        for cid in ClientId::iter() {
            match self.clients[cid].state() {
                DsState::Connecting {
                    state: NegotiationState::LiveRepairReady,
                    ..
                } => {
                    repair_downstairs.push(cid);
                }
                DsState::Active => {
                    source_downstairs = Some(cid);
                }
                _state => {
                    // ignore Downstairs in irrelevant states
                }
            }
        }

        // Can't start live-repair if no one is LiveRepairReady
        if repair_downstairs.is_empty() {
            return;
        }

        // Can't start live-repair if we don't have a source downstairs
        let Some(source_downstairs) = source_downstairs else {
            warn!(self.log, "No source, no Live Repair possible");
            if repair_downstairs.len() == 3 {
                warn!(self.log, "All three downstairs require repair");
            }
            return;
        };

        // Move the upstairs that were LiveRepairReady to LiveRepair
        //
        // After this point, we must call `abort_repair` if something goes wrong
        // to abort the repair on the troublesome clients.
        for &cid in &repair_downstairs {
            self.clients[cid].start_live_repair(up_state);
        }

        // Any offline downstairs should now be moved to faulted, as we don't
        // or can't replay to them, so they must come back through LR
        for cid in ClientId::iter() {
            match self.clients[cid].state() {
                DsState::Connecting {
                    mode: ConnectionMode::Offline,
                    ..
                } => {
                    warn!(self.log, "Forcing offline downstairs {cid} to faulted");
                    self.clients[cid].set_connection_mode_faulted();
                }
                _state => {
                    // ignore Downstairs in irrelevant states
                }
            }
        }

        // Submit the initial repair jobs, which kicks everything off
        self.start_live_repair(&repair_downstairs, source_downstairs, up_state);

        info!(
            self.log,
            "started repair {}",
            self.repair.as_ref().unwrap().id
        );

        let repair = self.repair.as_ref().unwrap();
        self.notify_live_repair_start(repair);
    }

    /// Checks whether live-repair can continue
    ///
    /// If live-repair can continue, returns the relevant job result, which
    /// should be passed into `self.continue_live_repair(..)`.
    #[must_use]
    fn get_live_repair_job(&mut self) -> Option<Result<(), CrucibleError>> {
        self.repair
            .as_mut()
            .and_then(|r| r.state.active_job_result())
    }

    /// Pushes live-repair forward, if possible
    ///
    /// It's possible that handling the current live-repair job will cause
    /// subsequent live-repair jobs to be completed immediately
    /// (`test_repair_extent_fail_noop_out_of_order` exercises this case).  As
    /// such, this function will continue running until the next live-repair job
    /// is not ready.
    pub(crate) fn check_and_continue_live_repair(
        &mut self,
        up_state: &UpstairsState,
    ) {
        while let Some(r) = self.get_live_repair_job() {
            self.continue_live_repair(r, up_state);
        }
    }

    fn continue_live_repair(
        &mut self,
        r: Result<(), CrucibleError>,
        up_state: &UpstairsState,
    ) {
        let Some(repair) = &self.repair else {
            panic!("cannot continue live-repair without self.repair");
        };

        match &r {
            Ok(()) => {
                // keep going
                info!(self.log, "got repair ok in {:?}", repair.state);
            }
            Err(e) => {
                error!(self.log, "got repair error {e} in {:?}", repair.state);

                // We keep going here, because we need to submit no-op jobs to
                // avoid things getting clogged up.
                if !repair.aborting_repair {
                    warn!(self.log, "aborting live-repair due to IO error");
                    self.abort_repair(up_state);
                }
            }
        }

        // It's possible for the Downstairs to have changed state here, if it
        // disconnected.  We'll check that as well and start aborting the repair
        // if that's the case.
        let repair = self.repair.as_ref().unwrap(); // reborrow
        if !repair.aborting_repair
            && (repair
                .repair_downstairs
                .iter()
                .any(|&cid| self.clients[cid].state() != DsState::LiveRepair)
                || self.clients[repair.source_downstairs].state()
                    != DsState::Active)
        {
            warn!(self.log, "aborting live-repair due to invalid state");
            self.abort_repair(up_state);
        }

        // Each of these branches should update `repair.state`, and may or may
        // not call functions on `&mut self`.  This is somewhat awkward, because
        // the borrow of `self.repair.as_mut().unwrap()` can't be held when
        // calling such functions; we have to extract everything we want from
        // the `LiveRepairData` before calling anything on `&mut self`.
        let repair = self.repair.as_mut().unwrap(); // reborrow

        // steal `repair.state`, because we're about to replace it
        match std::mem::replace(&mut repair.state, LiveRepairState::dummy()) {
            LiveRepairState::Closing {
                close_job,
                repair_job,
                noop_job,
                reopen_job,
            } => {
                info!(
                    self.log,
                    "RE:{} Wait for result from repair command {}",
                    repair.active_extent,
                    repair_job.id,
                );
                let close_id = close_job.id;
                let repair_id = repair_job.id;
                repair.state = LiveRepairState::Repairing {
                    repair_job,
                    noop_job,
                    reopen_job,
                };
                if repair.aborting_repair {
                    self.create_and_enqueue_noop_io(vec![close_id], repair_id);
                } else {
                    let repair_downstairs = repair.repair_downstairs.clone();
                    let active_extent = repair.active_extent;
                    let source_downstairs = repair.source_downstairs;
                    self.create_and_enqueue_repair_io(
                        active_extent,
                        vec![close_id],
                        repair_id,
                        source_downstairs,
                        &repair_downstairs,
                    );
                };
            }
            LiveRepairState::Repairing {
                repair_job,
                noop_job,
                reopen_job,
            } => {
                info!(
                    self.log,
                    "RE:{} Wait for result from NoOp command {}",
                    repair.active_extent,
                    noop_job.id,
                );
                let repair_id = repair_job.id;
                let noop_id = noop_job.id;
                repair.state = LiveRepairState::Noop {
                    noop_job,
                    reopen_job,
                };
                self.create_and_enqueue_noop_io(vec![repair_id], noop_id);
            }
            LiveRepairState::Noop { reopen_job, .. } => {
                info!(
                    self.log,
                    "RE:{} Wait for result from reopen command {}",
                    repair.active_extent,
                    reopen_job.id,
                );
                // The reopen job was already queued, so just change state
                repair.state = LiveRepairState::Reopening { reopen_job };
            }
            LiveRepairState::Reopening { .. } => {
                // It's possible that we've reached the end of our extents!
                let next_extent = repair.active_extent + 1;
                let finished = next_extent.0 == repair.extent_count;

                // If we have reserved jobs for this extent, then we have to
                // keep doing (sending no-ops) because otherwise dependencies
                // will never be resolved.
                let have_reserved_jobs =
                    repair.repair_job_ids.contains_key(&next_extent);

                // We return the previous state here, because the new state must
                // be constructed by calling functions on `&mut self`.  The
                // reassignment is handled below.
                if finished || (repair.aborting_repair && !have_reserved_jobs) {
                    // We're done, submit a final flush!
                    let flush_id = self.submit_flush(None, None, None);

                    info!(self.log, "LiveRepair final flush submitted");
                    cdt::up__to__ds__flush__start!(|| (flush_id.0));

                    // The borrow was dropped earlier, so reborrow `self.repair`
                    self.repair.as_mut().unwrap().state =
                        LiveRepairState::FinalFlush {
                            flush_job: PendingJob::new(flush_id),
                        }
                } else {
                    let repair_id = repair.id;
                    self.notify_live_repair_progress(repair_id, next_extent);
                    self.send_next_live_repair(up_state);
                };
            }
            LiveRepairState::FinalFlush { .. } => {
                info!(self.log, "LiveRepair final flush returned {r:?}");
                if repair.aborting_repair {
                    info!(self.log, "live-repair aborted");
                    // Clients were already cleaned up when we first set
                    // `repair.aborting_repair = true`, so no cleanup here
                } else {
                    info!(self.log, "live-repair completed successfully");
                    for c in &repair.repair_downstairs {
                        self.clients[*c].finish_repair(up_state);
                    }
                }

                let repair = self.repair.as_ref().unwrap();
                self.notify_live_repair_finish(repair);

                // Set `self.repair` to `None` on our way out the door (because
                // repair is done, one way or the other)
                self.repair = None;
            }
        }
    }

    fn create_and_enqueue_noop_io(&mut self, deps: Vec<JobId>, noop_id: JobId) {
        let nio = IOop::ExtentLiveNoOp { dependencies: deps };

        cdt::gw__noop__start!(|| (noop_id.0));
        self.enqueue(noop_id, nio, None, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_and_enqueue_repair_io(
        &mut self,
        eid: ExtentId,
        deps: Vec<JobId>,
        repair_id: JobId,
        source: ClientId,
        repair: &[ClientId],
    ) {
        let repair_io = self.repair_or_noop(eid, deps, source, repair);

        cdt::gw__repair__start!(|| (repair_id.0, eid.0));

        self.enqueue(repair_id, repair_io, None, None)
    }

    fn repair_or_noop(
        &mut self,
        extent: ExtentId,
        repair_deps: Vec<JobId>,
        source: ClientId,
        repair: &[ClientId],
    ) -> IOop {
        assert!(repair.len() < 3);

        let mut need_repair = Vec::new();
        debug!(self.log, "Get repair info for {} source", source);
        let good_ei = self.clients[source].repair_info.take().unwrap();
        for broken_extent in repair.iter() {
            // TODO: should this implementation be in DownstairsClient?
            debug!(self.log, "Get repair info for {} bad", broken_extent);
            let repair_ei =
                self.clients[*broken_extent].repair_info.take().unwrap();

            let repair = if repair_ei.dirty
                || repair_ei.generation != good_ei.generation
            {
                true
            } else {
                repair_ei.flush_number != good_ei.flush_number
            };

            if repair {
                need_repair.push(*broken_extent);
            }
        }

        // Now that we have consumed the contents, be sure to clear
        // out anything we did not look at.  There could be something left
        for c in self.clients.iter_mut() {
            c.repair_info = None;
        }

        if need_repair.is_empty() {
            info!(self.log, "No repair needed for extent {}", extent);
            for &cid in repair.iter() {
                self.clients[cid].stats.extents_confirmed += 1;
            }
            IOop::ExtentLiveNoOp {
                dependencies: repair_deps,
            }
        } else {
            info!(
                self.log,
                "Repair for extent {} s:{} d:{:?}", extent, source, need_repair
            );
            for &cid in repair.iter() {
                self.clients[cid].stats.extents_repaired += 1;
            }
            let repair_address = self.clients[source].repair_addr.unwrap();

            IOop::ExtentLiveRepair {
                dependencies: repair_deps,
                extent,
                source_downstairs: source,
                source_repair_address: repair_address,
                repair_downstairs: need_repair,
            }
        }
    }

    /// Begins live-repair
    ///
    /// Claims initial IDs and submits initial jobs, initializing `self.repair`
    ///
    /// # Panics
    /// If the upstairs is not in `UpstairsState::Active`, or either (1) the
    /// source downstairs is not `DsState::Active`, or (2) the repair downstairs
    /// are not all `DsState::LiveRepair`.
    fn start_live_repair(
        &mut self,
        repair_downstairs: &[ClientId],
        source_downstairs: ClientId,
        up_state: &UpstairsState,
    ) {
        let extent_count = self.ddef.unwrap().extent_count();
        // Load a partially valid repair state, with a dummy `state` (to be
        // updated in `send_live_repair_jobs`).
        self.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count,
            repair_downstairs: repair_downstairs.to_vec(),
            source_downstairs,
            aborting_repair: false,
            active_extent: ExtentId(0),
            min_id: self.peek_next_id(), // upcoming close_id
            repair_job_ids: BTreeMap::new(),
            state: LiveRepairState::dummy(), // fixed when sending jobs
        });
        self.send_live_repair_jobs(up_state);
    }

    /// Increments `self.repair.active_extent` and sends new jobs
    ///
    /// # Panics
    /// If any state is invalid, or `self.repair` is `None`
    fn send_next_live_repair(&mut self, up_state: &UpstairsState) {
        // This invalidates repair state, but we're about to update it
        let repair = self.repair.as_mut().unwrap();
        repair.active_extent += 1;
        self.send_live_repair_jobs(up_state);
    }

    /// Begins live-repair for the next extent, based on `self.repair`
    ///
    /// Claims initial IDs and submits live-repair jobs, updating
    /// `self.repair.state` with the new state.  If `self.repair.aborting` is
    /// true, then all of the submitted jobs are no-ops.
    ///
    /// # Panics
    /// - If `self.repair` is `None`
    /// - If the upstairs is not in `UpstairsState::Active`, or we _are not_
    ///   aborting the repair but either (1) the source downstairs is not
    ///   `DsState::Active`, or (2) the repair downstairs are not all
    ///   `DsState::LiveRepair`.
    fn send_live_repair_jobs(&mut self, up_state: &UpstairsState) {
        // Keep going!
        let repair = self.repair.as_mut().unwrap();

        let repair_downstairs = repair.repair_downstairs.clone();
        let extent = repair.active_extent;
        let aborting = repair.aborting_repair;
        let source_downstairs = repair.source_downstairs;

        // Invariant checking to begin
        assert!(
            matches!(up_state, UpstairsState::Active),
            "bad upstairs state: {up_state:?}"
        );
        // These assertions are only valid if we aren't aborting a repair; if we
        // *are* aborting a repair, some/all of the downstairs may change state
        // but we may still pass through this function to clean up reserved jobs
        if !aborting {
            assert_eq!(
                self.clients[source_downstairs].state(),
                DsState::Active
            );
            for &c in &repair_downstairs {
                assert_eq!(self.clients[c].state(), DsState::LiveRepair);
            }
        }

        // The work IDs for the downstairs side of things.
        let (extent_repair_ids, close_deps) = self.get_repair_ids(extent);
        let close_id = extent_repair_ids.close_id;
        let repair_id = extent_repair_ids.repair_id;
        let noop_id = extent_repair_ids.noop_id;
        let reopen_id = extent_repair_ids.reopen_id;

        //  Note that `close_deps` (the list of dependencies) will potentially
        //  include skipped jobs for some downstairs.  The list of dependencies
        //  can be further altered when we are about to send IO to an individual
        //  downstairs that is under repair. At that time, we go through the
        //  list of dependencies and remove jobs that we skipped or finished for
        //  that specific downstairs before we send the repair IO over the wire.

        info!(
            self.log,
            "RE:{} repair extent with ids {},{},{},{} deps:{}",
            extent,
            close_id,
            repair_id,
            noop_id,
            reopen_id,
            format_job_list(&close_deps),
        );

        let repair = self.repair.as_mut().unwrap(); // reborrow
        repair.state = LiveRepairState::Closing {
            close_job: PendingJob::new(close_id),
            repair_job: PendingJob::new(repair_id),
            reopen_job: PendingJob::new(reopen_id),
            noop_job: PendingJob::new(noop_id),
        };

        if aborting {
            self.create_and_enqueue_noop_io(vec![noop_id], reopen_id);
            self.create_and_enqueue_noop_io(close_deps, close_id);
        } else {
            self.create_and_enqueue_reopen_io(extent, vec![noop_id], reopen_id);
            self.create_and_enqueue_close_io(
                extent,
                close_deps,
                close_id,
                &repair_downstairs,
            )
        };
    }

    /// Creates a [DownstairsIO] job for an [IOop::ExtentLiveReopen], and
    /// adds it to the work queue.
    fn create_and_enqueue_reopen_io(
        &mut self,
        eid: ExtentId,
        deps: Vec<JobId>,
        reopen_id: JobId,
    ) {
        let reopen_io = IOop::ExtentLiveReopen {
            dependencies: deps,
            extent: eid,
        };

        cdt::gw__reopen__start!(|| (reopen_id.0, eid.0));

        self.enqueue(reopen_id, reopen_io, None, None)
    }

    #[cfg(test)]
    fn create_and_enqueue_generic_read_eob(&mut self) -> JobId {
        let iblocks = ImpactedBlocks::new(BlockIndex(7), BlockIndex(7));

        let ds_id = self.next_id();
        let dependencies = self.ds_active.deps_for_read(ds_id, iblocks);
        debug!(self.log, "IO Read  {} has deps {:?}", ds_id, dependencies);

        let aread = IOop::Read {
            dependencies,
            start_block: BlockIndex(7),
            count: 1,
            block_size: 512,
        };

        self.enqueue(ds_id, aread, None, None);
        ds_id
    }

    #[cfg(test)]
    fn create_and_enqueue_generic_write_eob(
        &mut self,
        is_write_unwritten: bool,
    ) -> JobId {
        use crucible_protocol::BlockContext;
        let request = RawWrite {
            data: bytes::BytesMut::from(vec![1].as_slice()),
            blocks: vec![BlockContext {
                encryption_context: None,
                hash: 0,
            }],
        };
        let iblocks = ImpactedBlocks::new(BlockIndex(7), BlockIndex(7));
        self.create_and_enqueue_write_eob(
            iblocks,
            request,
            is_write_unwritten,
            IOLimitGuard::dummy(),
        )
    }

    fn create_and_enqueue_write_eob(
        &mut self,
        blocks: ImpactedBlocks,
        write: RawWrite,
        is_write_unwritten: bool,
        io_guard: IOLimitGuard,
    ) -> JobId {
        let ds_id = self.next_id();
        if is_write_unwritten {
            cdt::gw__write__unwritten__start!(|| (ds_id.0));
        } else {
            cdt::gw__write__start!(|| (ds_id.0));
        }

        let dependencies = self.ds_active.deps_for_write(ds_id, blocks);
        cdt::gw__write__deps!(|| (
            self.ds_active.len() as u64,
            dependencies.len() as u64
        ));
        debug!(self.log, "IO Write {} has deps {:?}", ds_id, dependencies);

        // TODO: can anyone actually give us an empty write?
        let start = blocks.start().unwrap_or(BlockIndex(0));
        let awrite = if is_write_unwritten {
            IOop::WriteUnwritten {
                dependencies,
                start_block: start,
                data: write.data.freeze(),
                blocks: write.blocks,
            }
        } else {
            IOop::Write {
                dependencies,
                start_block: start,
                data: write.data.freeze(),
                blocks: write.blocks,
            }
        };

        self.enqueue(
            ds_id,
            awrite,
            Some(GuestBlockRes::Acked), // writes are always acked
            Some(io_guard),
        );
        ds_id
    }

    fn create_close_io(
        &mut self,
        eid: ExtentId,
        dependencies: Vec<JobId>,
        repair: Vec<ClientId>,
    ) -> IOop {
        IOop::ExtentFlushClose {
            dependencies,
            extent: eid,
            flush_number: self.next_flush_id(),
            gen_number: self.cfg.generation(),
            repair_downstairs: repair,
        }
    }

    fn create_and_enqueue_close_io(
        &mut self,
        eid: ExtentId,
        deps: Vec<JobId>,
        close_id: JobId,
        repair: &[ClientId],
    ) {
        let close_io = self.create_close_io(eid, deps, repair.to_vec());

        cdt::gw__close__start!(|| (close_id.0, eid.0));
        self.enqueue(close_id, close_io, None, None)
    }

    /// Get the repair IDs and dependencies for this extent.
    ///
    /// If they were already reserved, then use those values (removing them from
    /// the list of reserved IDs), otherwise, go get the next set of job IDs.
    fn get_repair_ids(
        &mut self,
        eid: ExtentId,
    ) -> (ExtentRepairIDs, Vec<JobId>) {
        if let Some((eri, deps)) = self
            .repair
            .as_mut()
            .map(|r| r.repair_job_ids.remove(&eid))
            .unwrap_or(None)
        {
            info!(
                self.log,
                "Return existing job ids for {} GG: {eri:?} {deps:?}", eid
            );
            (eri, deps)
        } else {
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
            info!(self.log, "Create new job ids for {}: {repair_ids:?}", eid);

            let ddef = self.ddef.as_ref().unwrap();
            let deps = self.ds_active.deps_for_repair(repair_ids, eid, ddef);
            (repair_ids, deps)
        }
    }

    /// Returns the next flush number (post-incrementing `self.next_flush`)
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

    /// Take a hashmap with extents we need to fix and convert that to
    /// a queue of Crucible messages we need to execute to perform the fix.
    ///
    /// The order of messages in the queue shall be the order they are
    /// performed, and no message can start until the previous message
    /// has been ack'd by all three downstairs.
    fn convert_rc_to_messages(
        &self,
        mut rec_list: HashMap<ExtentId, ExtentFix>,
        max_flush: u64,
        max_gen: u64,
    ) -> VecDeque<ReconcileIO> {
        let mut rep_id = ReconciliationId(0);
        let mut out = VecDeque::with_capacity(rec_list.len());
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
            out.push_back(ReconcileIO::new(
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

            out.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id.0 += 1;

            let repair = self.clients[ef.source].repair_addr.unwrap();
            out.push_back(ReconcileIO::new(
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

            out.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentReopen {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id.0 += 1;
        }

        info!(self.log, "Task list: {:?}", out);
        out
    }

    /// Takes the next task from `self.reconcile_task_list` and runs it
    ///
    /// The resulting work is stored in `self.reconcile_current_work`
    ///
    /// Returns `true` if reconciliation is complete (i.e. the task list is
    /// empty).
    ///
    /// # Panics
    /// If `self.reconcile_current_work` is not `None`, or if `self.reconcile`
    /// is `None`.
    pub(crate) fn send_next_reconciliation_req(&mut self) -> bool {
        let Some(reconcile) = &mut self.reconcile else {
            panic!("`self.reconcile` must be Some during reconciliation");
        };
        assert!(reconcile.current_work.is_none());

        let Some(mut next) = reconcile.task_list.pop_front() else {
            info!(self.log, "done with reconciliation");
            return true;
        };

        debug!(
            self.log,
            "reconciliation {}: on task {} of {}",
            reconcile.id,
            self.reconcile_repaired,
            reconcile.reconcile_repair_needed,
        );

        for c in self.clients.iter_mut() {
            c.send_next_reconciliation_req(&mut next);
        }

        reconcile.current_work = Some(next);

        let reconcile_id = reconcile.id;

        // `on_reconciliation_job_done` increments one of these and
        // decrements the other, so add them together to get total task
        // count to send to Nexus.
        let task_count =
            self.reconcile_repaired + reconcile.reconcile_repair_needed;

        self.notify_reconcile_progress(
            reconcile_id,
            self.reconcile_repaired,
            task_count,
        );

        false
    }

    /// Handles an ack from a reconciliation job
    ///
    /// Returns `true` if reconciliation is complete
    ///
    /// If any of the downstairs clients are in an invalid state for
    /// reconciliation to continue, mark them as `FailedRepair` and restart
    /// them, clearing out the current reconciliation state.
    pub(crate) fn on_reconciliation_ack(
        &mut self,
        client_id: ClientId,
        repair_id: ReconciliationId,
        up_state: &UpstairsState,
    ) -> bool {
        if self.reconcile.is_none() {
            // This can happen if reconciliation is cancelled (e.g. one
            // Downstairs died) but reconciliation acks are still coming through
            // from the other downstairs.
            //
            // We can't do much about it, so log it and continue.
            warn!(self.log, "got reconciliation ack without active work");
            // TODO: should we have a stronger state here, something like
            // WaitingForCancelledReconciliation?
            return false;
        };

        // Check to make sure that we're still in a repair-ready state
        //
        // If any client have dropped out of repair-readiness (e.g. due to
        // failed reconciliation, timeouts, etc), then we have to kick
        // everything else back to the beginning.
        if self.clients.iter().any(|c| {
            !matches!(
                c.state(),
                DsState::Connecting {
                    state: NegotiationState::Reconcile,
                    ..
                }
            )
        }) {
            // Something has changed, so abort this repair.
            // Mark any downstairs that have not changed as failed and disable
            // them so that they restart.
            self.abort_reconciliation(up_state);
            return false;
        }

        let Some(reconcile) = self.reconcile.as_mut() else {
            unreachable!(); // checked above
        };

        let next = reconcile.current_work.as_mut().unwrap();
        if self.clients[client_id].on_reconciliation_job_done(repair_id, next) {
            reconcile.current_work = None;
            reconcile.reconcile_repair_needed -= 1;
            self.reconcile_repaired += 1;
            self.send_next_reconciliation_req()
        } else {
            false
        }
    }

    /// Handles an `ExtentError` message during reconciliation
    ///
    /// Right now, we completely abort the repair operation on all clients if
    /// this happens, and let the Upstairs sort it out once the IO tasks close.
    pub(crate) fn on_reconciliation_failed(
        &mut self,
        client_id: ClientId,
        repair_id: ReconciliationId,
        extent_id: ExtentId,
        error: CrucibleError,
        up_state: &UpstairsState,
    ) {
        error!(
            self.clients[client_id].log,
            "extent {extent_id} error on job {repair_id}: {error}"
        );
        self.abort_reconciliation(up_state);
    }

    pub(crate) fn abort_reconciliation(&mut self, up_state: &UpstairsState) {
        warn!(self.log, "aborting reconciliation");
        // Something has changed, so abort this repair.
        // Mark any downstairs that have not changed as failed and disable
        // them so that they restart.
        for (i, c) in self.clients.iter_mut().enumerate() {
            if matches!(
                c.state(),
                DsState::Connecting {
                    state: NegotiationState::Reconcile,
                    ..
                }
            ) {
                // Restart the IO task.  This will cause the Upstairs to
                // deactivate through a ClientAction::TaskStopped.
                c.abort_negotiation(
                    up_state,
                    ClientNegotiationFailed::FailedReconcile,
                );
                error!(self.log, "Mark {} as FAILED REPAIR", i);
            }
        }

        info!(self.log, "Clear out existing repair work queue");

        if let Some(r) = self.reconcile.take() {
            self.notify_reconcile_finished(&r, true /* aborted */);
            self.reconcile_repaired = 0;
        } else {
            // If reconcile is None, then these should also be cleared
            assert_eq!(self.reconcile_repaired, 0);
        }
        self.reconcile_repair_aborted += 1;
    }

    /// Initial reconciliation was skipped, sets all WQ clients as Active
    pub(crate) fn on_reconciliation_skipped(&mut self, go_active: bool) {
        assert!(self.reconcile.is_none());
        if go_active {
            assert!(self.ds_active.is_empty());
        }

        for (i, c) in self.clients.iter_mut().enumerate() {
            if matches!(
                c.state(),
                DsState::Connecting {
                    state: NegotiationState::WaitQuorum,
                    ..
                }
            ) {
                c.set_active();
            } else {
                warn!(
                    self.log,
                    "client {} is in state {:?} not ready for activation",
                    i,
                    c.state(),
                );
            }
        }
    }

    /// Asserts that initial reconciliation is done, and sets clients as Active
    ///
    /// # Panics
    /// If that isn't the case!
    pub(crate) fn on_reconciliation_done(&mut self, did_work: bool) {
        assert!(self.ds_active.is_empty());

        for c in self.clients.iter_mut() {
            c.set_active();
        }

        if did_work {
            // reconciliation completed
            let r = self.reconcile.take().unwrap();
            assert!(r.task_list.is_empty());
            self.notify_reconcile_finished(&r, false /* aborted */);
        } else {
            // no reconciliation was required
            assert!(self.reconcile.is_none());
        }
    }

    /// Compares region metadata from all three clients and builds a mend list
    fn mismatch_list(&self) -> Option<DownstairsMend> {
        let log = self.log.new(o!("" => "mend".to_string()));
        let mut meta = ClientMap::new();
        for i in ClientId::iter() {
            let DsStateData::Connecting {
                state: NegotiationStateData::WaitQuorum(r),
                ..
            } = self.clients[i].state_data()
            else {
                panic!("client {i} is not in WaitQuorum");
            };
            meta.insert(i, r);
        }
        DownstairsMend::new(&meta, log)
    }

    pub(crate) fn submit_flush(
        &mut self,
        snapshot_details: Option<SnapshotDetails>,
        res: Option<BlockRes>,
        io_guard: Option<IOLimitGuard>,
    ) -> JobId {
        let next_id = self.next_id();
        cdt::gw__flush__start!(|| (next_id.0));

        let flush_id = self.next_flush_id();
        let dep = self.ds_active.deps_for_flush(next_id);
        debug!(self.log, "IO Flush {} has deps {:?}", next_id, dep);

        /*
         * TODO: Walk the list of guest work structs and build the same list
         * and make sure it matches.
         */

        // Find the farthest extent under repair
        let extent_under_repair = self.last_repair_extent();

        /*
         * Build the flush request, and take note of the request ID that
         * will be assigned to this new piece of work.
         */
        let flush = IOop::Flush {
            dependencies: dep,
            flush_number: flush_id,
            gen_number: self.cfg.generation(),
            snapshot_details,
            extent_limit: extent_under_repair,
        };

        self.pending_barrier += 1;
        self.enqueue(next_id, flush, res.map(GuestBlockRes::Other), io_guard);
        next_id
    }

    /// Checks to see whether a `Barrier` operation is needed
    ///
    /// A `Barrier` is needed if we have buffered more than
    /// `IO_CACHED_MAX_BYTES/JOBS` worth of complete jobs, and there are no
    /// other barrier (or flush) operations in flight
    pub(crate) fn needs_barrier(&self) -> bool {
        if self.pending_barrier > 0 {
            return false;
        }

        // n.b. This may not be 100% reliable: if different Downstairs have
        // finished a different subset of jobs, then it's theoretically possible
        // for each DownstairsClient to be under our limits, but for the true
        // number of cached bytes/jobs to be over the limits.
        //
        // It's hard to imagine how we could encounter such a situation, given
        // job dependencies and no out-of-order execution, so this is more of a
        // "fun fact" and less an actual concern.
        let max_jobs = self
            .clients
            .iter()
            .map(|c| {
                let i = c.io_state_job_count();
                i.skipped + i.done + i.error
            })
            .max()
            .unwrap();
        let max_bytes = self
            .clients
            .iter()
            .map(|c| {
                let i = c.io_state_byte_count();
                i.skipped + i.done + i.error
            })
            .max()
            .unwrap();

        max_jobs as u64 >= crate::IO_CACHED_MAX_JOBS
            || max_bytes >= crate::IO_CACHED_MAX_BYTES
    }

    pub(crate) fn submit_barrier(&mut self) -> JobId {
        let next_id = self.next_id();
        cdt::gw__barrier__start!(|| (next_id.0));

        // A barrier has the same deps as a flush (namely, it depends on all
        // previous jobs and acts as the only dependency for all subsequent
        // jobs)
        let dependencies = self.ds_active.deps_for_flush(next_id);
        debug!(self.log, "IO Barrier {next_id} has deps {dependencies:?}");

        self.pending_barrier += 1;
        self.enqueue(next_id, IOop::Barrier { dependencies }, None, None);
        next_id
    }

    /// Reserves repair IDs if impacted blocks overlap our extent under repair
    fn check_repair_ids_for_range(&mut self, impacted_blocks: ImpactedBlocks) {
        let Some(eur) = self.get_extent_under_repair() else {
            return;
        };
        let ddef = self.ddef.as_ref().unwrap();
        let mut future_repair = false;
        for eid in impacted_blocks.extents(ddef) {
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
    ///
    /// # Panics
    /// If we are not undergoing live-repair
    pub(crate) fn reserve_repair_ids_for_extent(&mut self, eid: ExtentId) {
        if self
            .repair
            .as_mut()
            .unwrap()
            .repair_job_ids
            .contains_key(&eid)
        {
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
        let ddef = self.ddef.as_ref().unwrap();
        let deps = self.ds_active.deps_for_repair(repair_ids, eid, ddef);
        info!(
            self.log,
            "reserving repair IDs for {eid}: {repair_ids:?}; got dep {deps:?}"
        );
        self.repair
            .as_mut()
            .unwrap()
            .repair_job_ids
            .insert(eid, (repair_ids, deps));
    }

    /// Create and submit a read job to the three clients
    pub(crate) fn submit_read(
        &mut self,
        blocks: ImpactedBlocks,
        data: Buffer,
        res: BlockRes<Buffer, (Buffer, CrucibleError)>,
        io_guard: IOLimitGuard,
    ) -> JobId {
        // If there is a live-repair in progress that intersects with this read,
        // then reserve job IDs for those jobs.  We must do this before
        // reserving the job ID for the read, to keep jobs ordered!
        self.check_repair_ids_for_range(blocks);

        // Create the tracking info for downstairs request numbers (ds_id) we
        // will create on behalf of this guest job.
        let ds_id = self.next_id();
        cdt::gw__read__start!(|| ds_id.0);

        let dependencies = self.ds_active.deps_for_read(ds_id, blocks);
        debug!(self.log, "IO Read  {} has deps {:?}", ds_id, dependencies);

        // TODO: can anyone actually give us an empty write?
        let start = blocks.start().unwrap_or(BlockIndex(0));
        let ddef = self.ddef.unwrap();
        let aread = IOop::Read {
            dependencies,
            start_block: start,
            count: blocks.blocks().len() as u64,
            block_size: ddef.block_size(),
        };

        let res = GuestBlockRes::Read(data, res);
        self.enqueue(ds_id, aread, Some(res), Some(io_guard));

        ds_id
    }

    pub(crate) fn submit_write(
        &mut self,
        blocks: ImpactedBlocks,
        write: RawWrite,
        is_write_unwritten: bool,
        io_guard: IOLimitGuard,
    ) -> JobId {
        // If there is a live-repair in progress that intersects with this read,
        // then reserve job IDs for those jobs.
        self.check_repair_ids_for_range(blocks);

        self.create_and_enqueue_write_eob(
            blocks,
            write,
            is_write_unwritten,
            io_guard,
        )
    }

    /// Returns the currently-active repair extent
    ///
    /// Note that this isn't the _last_ extent for which we've reserved repair
    /// IDs; it's simply the extent being repaired right now.  See
    /// [`Downstairs::last_repair_extent`] for another perspective.
    pub(crate) fn active_repair_extent(&self) -> Option<ExtentId> {
        self.repair.as_ref().map(|r| r.active_extent)
    }

    /// Returns the most recent extent under repair, or `None`
    ///
    /// This includes extents for which repairs have been reserved but not yet
    /// started, because job dependency tracking should maintain proper
    /// dependencies for reserved jobs.
    pub(crate) fn last_repair_extent(&self) -> Option<ExtentId> {
        self.repair
            .as_ref()
            .and_then(|r| r.repair_job_ids.last_key_value().map(|(k, _)| *k))
            .or(self.active_repair_extent())
    }

    /// Returns the range of extents under repair
    ///
    /// This range spans from the currently-under-repair extent to the last
    /// extent for which we have reserved job IDs.
    fn get_extent_under_repair(
        &self,
    ) -> Option<std::ops::RangeInclusive<ExtentId>> {
        if let Some(eur) = self.active_repair_extent() {
            let end = self.last_repair_extent().unwrap();
            Some(eur..=end)
        } else {
            None
        }
    }

    /// Enqueue a [IOop] job:
    ///
    /// - enqueue the job in each of [Self::clients] (clients may skip the job)
    /// - add the job to [Self::ds_active]
    /// - Ack the job immediately if it was skipped by all downstairs
    /// - Check that the job was already acked if it's a write (the "fast ack"
    ///   optimization, which is performed elsewhere)
    /// - Send the job to each downstairs client task (if not skipped)
    fn enqueue(
        &mut self,
        ds_id: JobId,
        io: IOop,
        res: Option<GuestBlockRes>,
        io_limits: Option<IOLimitGuard>,
    ) {
        let mut skipped = 0;
        let last_repair_extent = self.last_repair_extent();

        let mut io_limits = io_limits
            .map(ClientMap::from)
            .unwrap_or_else(ClientMap::new);

        // Send the job to each client!
        let state = ClientData::from_fn(|cid| {
            let client = &mut self.clients[cid];
            let r = match client.should_send() {
                Ok(r) => r,
                Err(ShouldSendError::InLiveRepair) => {
                    if io.send_io_live_repair(
                        last_repair_extent,
                        self.ddef.as_ref().unwrap(),
                    ) {
                        EnqueueResult::Send
                    } else {
                        EnqueueResult::Skip
                    }
                }
            };
            client.apply_enqueue_result(ds_id, &io, r);
            match r {
                EnqueueResult::Send => self.send(ds_id, io.clone(), cid),
                EnqueueResult::Hold => (),
                EnqueueResult::Skip => {
                    // Take and drop the ClientIOLimitGuard, freeing up tokens
                    let _ = io_limits.take(&cid);
                    skipped += 1;
                }
            }
            r.state()
        });

        // Writes are acked immediately, before being enqueued
        let acked = io.is_write();

        // Puts the IO onto the downstairs work queue.
        self.ds_active.insert(
            ds_id,
            DownstairsIO {
                work: io,
                state,
                acked,
                res,
                replay: false,
                data: None,
                io_limits,
            },
        );

        if acked {
            self.acked_ids.push(ds_id)
        } else {
            self.gw_active.insert(ds_id);
        }

        // Ack the job immediately if it was skipped on all 3x downstairs
        // (and wasn't previously acked, i.e. isn't a write)
        if skipped == 3 {
            self.ack_check(ds_id, false);
            debug!(self.log, "job {ds_id} skipped on all downstairs");
        }
    }

    /// Sends the given job to the given client
    fn send(&mut self, ds_id: JobId, io: IOop, client_id: ClientId) {
        let job = self.clients[client_id].prune_deps(
            ds_id,
            io,
            self.repair.as_ref().map(|r| r.min_id),
        );
        let message = match job {
            IOop::Write {
                dependencies,
                start_block,
                blocks,
                data,
            } => {
                cdt::ds__write__client__start!(|| (ds_id.0, client_id.get()));
                Message::Write {
                    header: WriteHeader {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
                        dependencies,
                        start: start_block,
                        contexts: blocks,
                    },
                    data,
                }
            }
            IOop::WriteUnwritten {
                start_block,
                blocks,
                dependencies,
                data,
            } => {
                cdt::ds__write__unwritten__client__start!(|| (
                    ds_id.0,
                    client_id.get()
                ));
                Message::WriteUnwritten {
                    header: WriteHeader {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
                        dependencies,
                        start: start_block,
                        contexts: blocks,
                    },
                    data,
                }
            }
            IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            } => {
                cdt::ds__flush__client__start!(|| (ds_id.0, client_id.get()));
                Message::Flush {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    job_id: ds_id,
                    dependencies,
                    flush_number,
                    gen_number,
                    snapshot_details,
                    extent_limit,
                }
            }
            IOop::Barrier { dependencies } => {
                cdt::ds__barrier__client__start!(|| (ds_id.0, client_id.get()));
                Message::Barrier {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    job_id: ds_id,
                    dependencies,
                }
            }
            IOop::Read {
                dependencies,
                start_block,
                count,
                ..
            } => {
                cdt::ds__read__client__start!(|| (ds_id.0, client_id.get()));
                Message::ReadRequest {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    job_id: ds_id,
                    dependencies,
                    start: start_block,
                    count,
                }
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
                repair_downstairs,
            } => {
                cdt::ds__close__start!(|| {
                    (ds_id.0, client_id.get(), extent.0)
                });
                if repair_downstairs.contains(&client_id) {
                    // We are the downstairs being repaired, so just close.
                    Message::ExtentLiveClose {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
                        dependencies,
                        extent_id: extent,
                    }
                } else {
                    Message::ExtentLiveFlushClose {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
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
                cdt::ds__repair__start!(|| {
                    (ds_id.0, client_id.get(), extent.0)
                });
                if repair_downstairs.contains(&client_id) {
                    Message::ExtentLiveRepair {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
                        dependencies,
                        extent_id: extent,
                        source_client_id: source_downstairs,
                        source_repair_address,
                    }
                } else {
                    Message::ExtentLiveNoOp {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        job_id: ds_id,
                        dependencies,
                    }
                }
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                cdt::ds__reopen__start!(|| {
                    (ds_id.0, client_id.get(), extent.0)
                });
                Message::ExtentLiveReopen {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    job_id: ds_id,
                    dependencies,
                    extent_id: extent,
                }
            }
            IOop::ExtentLiveNoOp { dependencies } => {
                cdt::ds__noop__start!(|| (ds_id.0, client_id.get()));
                Message::ExtentLiveNoOp {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    job_id: ds_id,
                    dependencies,
                }
            }
        };
        self.clients[client_id].send(message)
    }

    pub(crate) fn replace(
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
                DsState::Stopping(ClientStopReason::Replacing)
                | DsState::Connecting {
                    mode: ConnectionMode::Replaced,
                    ..
                }
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
                DsState::Stopping(..)
                | DsState::LiveRepair
                | DsState::Connecting {
                    mode:
                        ConnectionMode::Replaced
                        | ConnectionMode::Offline
                        | ConnectionMode::Faulted,
                    ..
                } => {
                    return Err(CrucibleError::ReplaceRequestInvalid(format!(
                        "Replace {old} failed, downstairs {client_id} is {:?}",
                        self.clients[client_id].state(),
                    )));
                }
                DsState::Active
                | DsState::Connecting {
                    mode: ConnectionMode::New,
                    ..
                } => {}
            }
        }

        // Now we have found our old downstairs, verified the new is not in use
        // elsewhere, verified no other downstairs are in a bad state, we can
        // move forward with the replacement.
        info!(self.log, "{id} replacing old: {old} at {old_client_id}");

        // Skip all outstanding jobs for this client
        self.skip_all_jobs(old_client_id);

        // Clear the client state and restart the IO task
        self.clients[old_client_id].replace(up_state, new);

        Ok(ReplaceResult::Started)
    }

    /// Checks whether the given client state should go from Offline -> Faulted
    ///
    /// # Panics
    /// If the given client is not in the `Offline` state
    pub(crate) fn check_gone_too_long(&mut self, client_id: ClientId) {
        assert!(matches!(
            self.clients[client_id].state(),
            DsState::Connecting {
                mode: ConnectionMode::Offline,
                ..
            }
        ));

        let byte_count = self.clients[client_id].total_bytes_outstanding();
        let work_count = self.clients[client_id].total_live_work();
        let failed = if work_count > crate::IO_OUTSTANDING_MAX_JOBS {
            warn!(
                self.log,
                "downstairs failed, too many outstanding jobs {work_count}"
            );
            true
        } else if byte_count as u64 > crate::IO_OUTSTANDING_MAX_BYTES {
            warn!(
                self.log,
                "downstairs failed, too many outstanding bytes {byte_count}"
            );
            true
        } else if !self.can_replay {
            // XXX can this actually happen?
            warn!(
                self.log,
                "downstairs became ineligible for replay while offline"
            );
            true
        } else if self.live_repair_in_progress() {
            warn!(
                self.log,
                "downstairs {client_id} no replay while offline because LR"
            );
            true
        } else {
            false
        };

        if failed {
            self.skip_all_jobs(client_id);
            self.clients[client_id].set_connection_mode_faulted();
        }
    }

    /// Marks a client as faulted, skipping pending IO and stopping the worker
    pub(crate) fn fault_client(
        &mut self,
        client_id: ClientId,
        up_state: &UpstairsState,
        err: ClientFaultReason,
    ) {
        self.skip_all_jobs(client_id);
        self.clients[client_id].fault(up_state, err);
    }

    /// Move all `New` and `InProgress` jobs for the given client to `Skipped`
    ///
    /// This may lead to jobs being acked, since a skipped job counts as
    /// complete in some circumstances.
    pub(crate) fn skip_all_jobs(&mut self, client_id: ClientId) {
        info!(
            self.log,
            "[{client_id}] client skip {} in process jobs because fault",
            self.ds_active.len(),
        );

        let mut ack_check = vec![];
        let mut number_jobs_skipped = 0;

        self.ds_active.for_each(|ds_id, job| {
            let state = &job.state[client_id];

            if matches!(state, IOState::InProgress) {
                self.clients[client_id].skip_job(*ds_id, job);
                number_jobs_skipped += 1;
                ack_check.push(*ds_id);
            }
        });

        info!(
            self.log,
            "[{}] changed {} jobs to fault skipped",
            client_id,
            number_jobs_skipped
        );

        for ds_id in ack_check {
            self.ack_check(ds_id, false);
        }
    }

    /// Aborts an in-progress live-repair
    ///
    /// The live-repair may continue after this point to clean up reserved jobs,
    /// to avoid blocking dependencies, but jobs are replaced with no-ops.
    fn abort_repair(&mut self, up_state: &UpstairsState) {
        let mut found_valid_state = false;
        for i in ClientId::iter() {
            match self.clients[i].state() {
                DsState::LiveRepair => {
                    found_valid_state = true;
                    self.fault_client(
                        i,
                        up_state,
                        ClientFaultReason::FailedLiveRepair,
                    );
                }
                // If just a single IO reported failure, we will fault this
                // downstairs and it won't yet have had a chance to move back
                // around to Connecting yet.
                DsState::Stopping(
                    ClientStopReason::Replacing
                    | ClientStopReason::Disabled
                    | ClientStopReason::Deactivated
                    | ClientStopReason::Fault(..),
                ) => {
                    found_valid_state = true;
                }

                // If connection aborted, and restarted, then the re-negotiation
                // could have won this race, and transitioned the reconnecting
                // downstairs from LiveRepair to Stopping to Connecting.
                DsState::Connecting {
                    mode: ConnectionMode::Faulted,
                    ..
                } => found_valid_state = true,

                // Other states are invalid
                _ => {}
            }
            // Set repair_info to None, so that the next ExtentFlushClose sees
            // it empty (as expected). repair_info is set on all clients, even
            // those not directly participating in live-repair, so we have to
            // always clear it.
            self.clients[i].clear_repair_state();
        }
        assert!(
            found_valid_state,
            "abort_repair called without a valid client state: {:?}",
            self.clients.iter().map(|c| c.state()).collect::<Vec<_>>(),
        );

        if let Some(repair) = &mut self.repair {
            repair.aborting_repair = true;
        }
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
        let job = self.ds_active.get(&ds_id).expect("checked missing job");
        let can_replay = match job.work {
            IOop::Flush { .. } => true,
            IOop::Barrier { .. } => false,
            _ => return,
        };

        // Only a completed flush will remove jobs from the active queue -
        // currently we have to keep everything around for use during replay
        let wc = self.ds_active.get(&ds_id).unwrap().state_count();
        if (wc.error + wc.skipped + wc.done) == 3 {
            assert!(!self.retired_ids.contains(&ds_id));
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
                if wc.active != 0 || !job.acked {
                    warn!(
                        self.log,
                        "[rc] leave job {} on the queue when removing {} {:?}",
                        id,
                        ds_id,
                        wc,
                    );
                    continue;
                }

                // Assert the job is actually done, then complete it
                assert_eq!(wc.error + wc.skipped + wc.done, 3);
                assert!(!self.retired_ids.contains(&id));
                assert!(job.acked);

                retired.push(id);
                self.retired_ids.push(id);
                let summary = job.io_summarize(id);
                self.retired_jobs.push(summary);
                for cid in ClientId::iter() {
                    self.clients[cid].retire_job(job);
                }
            }
            // Now that we've collected jobs to retire, remove them from the map
            for &id in &retired {
                let job = self.ds_active.remove(&id);

                // Update our barrier count for the removed job
                if matches!(job.work, IOop::Flush { .. } | IOop::Barrier { .. })
                {
                    self.pending_barrier =
                        self.pending_barrier.checked_sub(1).unwrap();
                }

                // Jobs should have their backpressure contribution removed when
                // they are completed (in `process_io_completion_inner`),
                // **not** when they are retired.  We'll do a sanity check here
                // and print a warning if that's not the case.
                for c in ClientId::iter() {
                    if job.io_limits.contains(&c) {
                        warn!(
                            self.log,
                            "job {ds_id} had pending io limits \
                             for client {c}"
                        );
                        // Backpressure is decremented on drop
                    }
                }
            }

            debug!(self.log, "[rc] retire {} clears {:?}", ds_id, retired);
            // Only keep track of skipped jobs at or above the flush.
            for cid in ClientId::iter() {
                self.clients[cid].skipped_jobs.retain(|&x| x >= ds_id);
            }

            // Update the flag indicating whether replay is allowed
            self.can_replay = can_replay;
        }
    }

    /// Returns the number of active jobs
    pub(crate) fn active_count(&self) -> usize {
        self.ds_active.len()
    }

    /// Prints a summary of active work to `stdout`
    pub(crate) fn show_all_work(&self) {
        print!("States:");
        for cid in ClientId::iter() {
            print!(" {}", self.clients[cid].state());
        }
        println!();
        println!(
            "{0:>5} {1:>8} {2:>7} {3:>7} {4:>5} {5:>5} {6:>5} {7:>7}",
            "JOBID", "ACK", "TYPE", "BKS/EXT", "DS:0", "DS:1", "DS:2", "REPLAY",
        );

        for (id, job) in &self.ds_active {
            let ack = if job.acked {
                AckStatus::Acked
            } else {
                AckStatus::NotAcked
            };

            let (job_type, blocks_or_extent): (String, usize) = match &job.work
            {
                IOop::Read { count, .. } => {
                    let job_type = "Read".to_string();
                    (job_type, *count as usize)
                }
                IOop::Write { blocks, .. } => {
                    let job_type = "Write".to_string();
                    (job_type, blocks.len())
                }
                IOop::WriteUnwritten { blocks, .. } => {
                    let job_type = "WriteU".to_string();
                    (job_type, blocks.len())
                }
                IOop::Flush { .. } => {
                    let job_type = "Flush".to_string();
                    (job_type, 0)
                }
                IOop::Barrier { .. } => {
                    let job_type = "Barrier".to_string();
                    (job_type, 0)
                }
                IOop::ExtentFlushClose { extent, .. } => {
                    let job_type = "FClose".to_string();
                    (job_type, extent.0 as usize)
                }
                IOop::ExtentLiveRepair { extent, .. } => {
                    let job_type = "Repair".to_string();
                    (job_type, extent.0 as usize)
                }
                IOop::ExtentLiveReopen { extent, .. } => {
                    let job_type = "Reopen".to_string();
                    (job_type, extent.0 as usize)
                }
                IOop::ExtentLiveNoOp { .. } => {
                    let job_type = "NoOp".to_string();
                    (job_type, 0)
                }
            };

            print!(
                "{0:>5} {1:>8} {2:>7} {3:>7}",
                id, ack, job_type, blocks_or_extent
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
        self.io_state_count().show_all();
        print!("Last Flush: ");
        for c in self.clients.iter() {
            print!("{:?} ", c.last_flush());
        }
        println!();
    }

    /// Prints a summary of guest work to `stdout`
    pub fn show_guest_work(&self) -> usize {
        println!("Guest work:  Active and Completed Jobs:");
        let mut kvec: Vec<_> = self.gw_active.iter().cloned().collect();
        kvec.sort_unstable();
        for id in kvec.iter() {
            println!("JOB active:[{id:?}]");
        }
        println!("JOB completed count:{:?} ", self.acked_ids.len());
        kvec.len()
    }

    /// Collects stats from the three `DownstairsClient`s
    pub fn collect_stats<T, F: Fn(&DownstairsClient) -> T>(
        &self,
        f: F,
    ) -> [T; 3] {
        [
            f(&self.clients[ClientId::new(0)]),
            f(&self.clients[ClientId::new(1)]),
            f(&self.clients[ClientId::new(2)]),
        ]
    }

    pub fn io_state_count(&self) -> IOStateCount {
        let d = self.collect_stats(|c| c.io_state_job_count());
        let f = |g: fn(ClientIOStateCount) -> u32| {
            ClientData([g(d[0]), g(d[1]), g(d[2])])
        };
        IOStateCount {
            in_progress: f(|d| d.in_progress),
            done: f(|d| d.done),
            skipped: f(|d| d.skipped),
            error: f(|d| d.error),
        }
    }

    /// Prints the last `n` retired jobs to `stdout`
    pub(crate) fn print_last_retired(&self, n: usize) {
        for j in self.retired_ids.iter().rev().take(n) {
            print!(" {:4}", j);
        }
    }

    /// Prints the last `n` acked jobs to `stdout`
    pub(crate) fn print_last_acked(&self, n: usize) {
        for j in self.acked_ids.iter().rev().take(n) {
            print!(" {:4}", j);
        }
    }

    pub(crate) fn process_io_completion(
        &mut self,
        client_id: ClientId,
        m: Message,
        up_state: &UpstairsState,
    ) -> Result<(), CrucibleError> {
        let (upstairs_id, session_id, ds_id, read_data, extent_info) = match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__write__client__done!(|| (job_id.0, client_id.get()));
                (
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
                    None,
                )
            }
            Message::WriteUnwrittenAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__write__unwritten__client__done!(|| (
                    job_id.0,
                    client_id.get()
                ));
                (
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
                    None,
                )
            }
            Message::FlushAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__flush__client__done!(|| (job_id.0, client_id.get()));
                (
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
                    None,
                )
            }
            Message::BarrierAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                cdt::ds__barrier__client__done!(|| (job_id.0, client_id.get()));
                (
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
                    None,
                )
            }
            Message::ReadResponse { header, data } => {
                cdt::ds__read__client__done!(|| (
                    header.job_id.0,
                    client_id.get()
                ));
                let upstairs_id = header.upstairs_id;
                let session_id = header.session_id;
                let job_id = header.job_id;

                (
                    upstairs_id,
                    session_id,
                    job_id,
                    header
                        .blocks
                        .map(|blocks| RawReadResponse { blocks, data }),
                    None,
                )
            }

            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                result,
            } => {
                // Take the result from the live close and store it away.
                let extent_info = match &result {
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
                        error!(
                            self.log,
                            "ELC-Ack returned error {e:?}, \
                             cannot get extent info"
                        );
                        None
                    }
                };
                cdt::ds__close__done!(|| (job_id.0, client_id.get()));
                (
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
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
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
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
                    upstairs_id,
                    session_id,
                    job_id,
                    result.map(|_| Default::default()),
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
                // ErrorReport messages here. This will change in the future
                // when the Upstairs tracks the number of errors per Downstairs,
                // and acts on that information somehow.
                error!(
                    self.clients[client_id].log,
                    "job id {} saw error {:?}", job_id, error
                );

                // However, there is one case (see `check_message_for_abort` in
                // downstairs/src/lib.rs) where the Upstairs **does** need to
                // act: when a repair job in the Downstairs fails, that
                // Downstairs aborts itself and reconnects.
                if let Some(job) = self.ds_active.get(&job_id) {
                    if job.work.is_repair() {
                        // Return the error and let the previously written error
                        // processing code work.
                        cdt::ds__repair__done!(|| (job_id.0, client_id.get()));
                        (upstairs_id, session_id, job_id, Err(error), None)
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
            m => panic!("called on_io_completion with invalid message {m:?}"),
        };

        if self.cfg.upstairs_id != upstairs_id {
            warn!(
                self.clients[client_id].log,
                "upstairs_id {:?} != job {} upstairs_id {:?}!",
                self.cfg.upstairs_id,
                ds_id,
                upstairs_id,
            );
            // TODO should these errors cause more behavior?  Right now, they're
            // only logged in the caller and then we move on.
            return Err(CrucibleError::UuidMismatch);
        }

        if self.cfg.session_id != session_id {
            warn!(
                self.clients[client_id].log,
                "u.session_id {:?} != job {} session_id {:?}!",
                self.cfg.session_id,
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
            DsState::Active | DsState::LiveRepair => {}
            DsState::Stopping(ClientStopReason::Fault(..))
            | DsState::Connecting {
                mode: ConnectionMode::Faulted,
                ..
            } => {
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
                    self.cfg.upstairs_id,
                    ds_id,
                    ds_state
                );
            }
        }

        self.process_io_completion_inner(
            ds_id,
            client_id,
            read_data,
            up_state,
            extent_info,
        );

        Ok(())
    }

    /// Returns a client error associated with the given job
    ///
    /// Returns `None` if that job is no longer active or has no error
    /// associated with it. For example, this can happen for the third (out of
    /// three) FlushAck messages, since the job is retired upon 2/3 flushes
    /// returning okay.
    fn client_error(
        &self,
        ds_id: JobId,
        client_id: ClientId,
    ) -> Option<CrucibleError> {
        let job = self.ds_active.get(&ds_id)?;
        let state = &job.state[client_id];

        if let IOState::Error(e) = state {
            Some(e.clone())
        } else {
            None
        }
    }

    /// Returns the number of clients that can accept IO
    ///
    /// A client can accept IO if it is in the `Active` or `LiveRepair` state.
    pub fn active_client_count(&self) -> usize {
        self.clients.iter().filter(|c| c.is_accepting_io()).count()
    }

    /// Wrapper for marking a single job as done from the given client
    ///
    /// This can be used to test handling of job acks, etc
    ///
    /// Returns true if the given job has gone from not acked to acked.  This is
    /// for historical reasons, because it's often used in existing tests.
    #[cfg(test)]
    pub fn process_ds_completion(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        responses: Result<RawReadResponse, CrucibleError>,
        up_state: &UpstairsState,
        extent_info: Option<ExtentInfo>,
    ) -> bool {
        let was_acked = self.ds_active.get(&ds_id).unwrap().acked;

        self.process_io_completion_inner(
            ds_id,
            client_id,
            responses,
            up_state,
            extent_info,
        );
        // It's possible for the IO completion to have retired this job; in that
        // case, it must have been acked before being retired.
        let now_acked =
            self.ds_active.get(&ds_id).map(|j| j.acked).unwrap_or(true);
        !was_acked && now_acked
    }

    fn process_io_completion_inner(
        &mut self,
        ds_id: JobId,
        client_id: ClientId,
        responses: Result<RawReadResponse, CrucibleError>,
        up_state: &UpstairsState,
        extent_info: Option<ExtentInfo>,
    ) {
        /*
         * Assume we don't have enough completed jobs, and only change
         * it if we have the exact amount required
         */
        let deactivate = matches!(up_state, UpstairsState::Deactivating { .. });

        let Some(job) = self.ds_active.get_mut(&ds_id) else {
            error!(
                self.clients[client_id].log,
                "IO completion error: missing {ds_id} "
            );
            /*
             * This assertion is only true for a limited time after
             * the downstairs has failed.  An old in-flight IO
             * could, in theory, ack back to us at some time
             * in the future after we cleared the completed.
             * I also think this path could be  possible if we
             * are in failure mode for LiveRepair, as we could
             * get an ack back from a job after we failed the DS
             * (from the upstairs side) and flushed the job away.
             */
            assert!(self.retired_ids.contains(&ds_id));
            return;
        };

        self.clients[client_id].process_io_completion(
            ds_id,
            job,
            responses,
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
                // XXX should we also change the upstairs state here?
                self.clients[client_id].disable(up_state);
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
            Some(_err) => {
                let Some(job) = self.ds_active.get(&ds_id) else {
                    panic!("I don't think we should be here");
                };
                if matches!(
                    job.work,
                    IOop::Write { .. }
                        | IOop::Flush { .. }
                        | IOop::Barrier { .. }
                        | IOop::WriteUnwritten { .. }
                        | IOop::ExtentFlushClose { .. }
                        | IOop::ExtentLiveRepair { .. }
                        | IOop::ExtentLiveNoOp { .. }
                        | IOop::ExtentLiveReopen { .. }
                ) {
                    // Send this Downstairs to faulted
                    self.fault_client(
                        client_id,
                        up_state,
                        ClientFaultReason::IOError,
                    );
                }
            }
            None => {
                // Nothing to do here, no error!
            }
        }

        // Check whether this job needs to be acked or retired
        self.ack_check(ds_id, deactivate);
    }

    /// Accessor for [`Downstairs::reconcile_repaired`]
    pub(crate) fn reconcile_repaired(&self) -> usize {
        self.reconcile_repaired
    }

    /// Accessor for [`Downstairs::reconcile_repair_needed`]
    pub(crate) fn reconcile_repair_needed(&self) -> usize {
        self.reconcile
            .as_ref()
            .map(|r| r.reconcile_repair_needed)
            .unwrap_or(0)
    }

    /// Accessor for [`Downstairs::reconcile_repair_aborted`]
    pub(crate) fn reconcile_repair_aborted(&self) -> usize {
        self.reconcile_repair_aborted
    }

    pub(crate) fn get_work_summary(&self) -> crate::control::DownstairsWork {
        let mut kvec: Vec<_> = self.ds_active.keys().cloned().collect();
        kvec.sort_unstable();

        let mut jobs = Vec::new();
        for id in kvec.iter() {
            let job = self.ds_active.get(id).unwrap();
            let work_summary = job.io_summarize(*id);
            jobs.push(work_summary);
        }
        let completed = self.retired_jobs.to_vec();
        crate::control::DownstairsWork { jobs, completed }
    }

    pub(crate) fn write_bytes_outstanding(&self) -> u64 {
        // XXX this overlaps with io_state_byte_count
        self.clients
            .iter()
            .filter(|c| matches!(c.state(), DsState::Active))
            .map(|c| c.io_state_byte_count().in_progress)
            .max()
            .unwrap_or(0)
    }

    /// Returns all jobs in sorted order by [`JobId`]
    ///
    /// This function is used in unit tests for the Upstairs
    #[cfg(test)]
    pub(crate) fn get_all_jobs(&self) -> Vec<JobId> {
        // This is a BTreeMap, so it's already sorted
        self.ds_active.keys().cloned().collect()
    }

    /// Return the block range covered by the given job
    ///
    /// # Panics
    /// If the job is not stored in our `Downstairs`
    #[cfg(test)]
    pub fn get_blocks_for(&self, ds_id: JobId) -> ImpactedBlocks {
        self.ds_active.get_blocks_for(ds_id)
    }

    /// Returns a list of extents covered by the given job
    ///
    /// # Panics
    /// If the job is not stored in our `Downstairs`
    #[cfg(test)]
    pub fn get_extents_for(&self, ds_id: JobId) -> Vec<ExtentId> {
        let ddef = self.ddef.as_ref().unwrap();
        self.get_blocks_for(ds_id).extents(ddef).collect()
    }

    #[cfg(test)]
    pub fn completed(&self) -> &AllocRingBuffer<JobId> {
        &self.retired_ids
    }

    #[cfg(test)]
    pub(crate) fn repair(&self) -> &Option<LiveRepairData> {
        &self.repair
    }

    #[cfg(test)]
    pub(crate) fn get_job(&self, ds_id: &JobId) -> Option<&DownstairsIO> {
        self.ds_active.get(ds_id)
    }

    #[cfg(test)]
    pub(crate) fn get_deps(&self, ds_id: JobId) -> &[JobId] {
        self.get_job(&ds_id).unwrap().work.deps()
    }

    /// Submit a write to this downstairs. Use when you don't care about what
    /// the data you're writing is, and only care about getting some write-jobs
    /// enqueued. The write will be to a single block, as specified by `eid` and
    /// `block`
    #[cfg(test)]
    fn submit_test_write_block(
        &mut self,
        eid: ExtentId,
        block: crucible_common::BlockOffset,
        is_write_unwritten: bool,
    ) -> JobId {
        let extent_size = self.ddef.unwrap().extent_size().value;
        let block = BlockIndex(u64::from(eid.0) * extent_size + block.0);
        let blocks = ImpactedBlocks::new(block, block);

        // Extent size doesn't matter as long as it can contain our write
        self.submit_test_write(blocks, is_write_unwritten)
    }

    /// Submit a write to this downstairs. Use when you don't care about what
    /// the data you're writing is, and only care about getting some write-jobs
    /// enqueued. The write will be to a range of blocks, as specified by
    /// `blocks`
    #[cfg(test)]
    fn submit_test_write(
        &mut self,
        blocks: ImpactedBlocks,
        is_write_unwritten: bool,
    ) -> JobId {
        use bytes::BytesMut;
        use crucible_protocol::BlockContext;

        let write_blocks: Vec<_> = blocks
            .blocks()
            .map(|_b| BlockContext {
                hash: crucible_common::integrity_hash(&[&vec![0xff; 512]]),
                encryption_context: None,
            })
            .collect();
        let data =
            BytesMut::from(vec![0xff; 512 * write_blocks.len()].as_slice());

        self.submit_write(
            blocks,
            RawWrite {
                blocks: write_blocks,
                data,
            },
            is_write_unwritten,
            IOLimitGuard::dummy(),
        )
    }

    /// Submit a read to this downstairs. Use when you don't care about what
    /// the data you're read is, and only care about getting some read-jobs
    /// enqueued. The read will be to a single block, as specified by `eid` and
    /// `block`
    #[cfg(test)]
    fn submit_read_block(
        &mut self,
        eid: ExtentId,
        block: crucible_common::BlockOffset,
    ) -> JobId {
        let extent_size = self.ddef.unwrap().extent_size().value;
        let block = BlockIndex(u64::from(eid.0) * extent_size + block.0);
        let blocks = ImpactedBlocks::new(block, block);
        self.submit_dummy_read(blocks)
    }

    #[cfg(test)]
    fn submit_dummy_read(&mut self, blocks: ImpactedBlocks) -> JobId {
        let ddef = self.ddef.as_ref().unwrap();
        let block_count = blocks.blocks().len();
        let buf = Buffer::new(block_count, ddef.block_size() as usize);
        self.submit_read(blocks, buf, BlockRes::dummy(), IOLimitGuard::dummy())
    }

    /// Create a test downstairs which has all clients Active
    ///
    /// The test downstairs is configured with 512-byte blocks and 3 blocks per
    /// extent.
    #[cfg(test)]
    fn repair_test_all_active() -> Self {
        let mut ds = Self::test_default();

        ds.force_active();

        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(crate::Block::new_512(3));
        ddef.set_extent_count(u32::MAX);
        ds.ddef = Some(ddef);

        ds
    }

    #[cfg(test)]
    /// Create a test downstairs which has one downstairs client transitioned to
    /// LiveRepair
    fn repair_test_one_repair() -> Self {
        let mut ds = Self::repair_test_all_active();

        // Set one of the clients to want a repair
        let to_repair = ClientId::new(1);
        test::move_to_live_repair(&mut ds, to_repair);

        // At this point you might think it makes sense to run
        //   `self.start_live_repair(&UpstairsState::Active, gw, 3, 0);`
        // But we don't actually want to start the repair here using
        //   `ds.start_live_repair`, because that submits some initial jobs too,
        // which get in the way of the depenedency resolution checking we want
        // to do. Our tests want dispatch specific sets of jobs themselves to
        // see what the dependency tree looks like.
        //
        // Right now, a lot of the sets don't actually need `self.repair` to be
        // Some, so we can leave it as None and they test the behavior we care
        // about. The ones that do need it set will set it themselves or call
        // the state transition functions as appropriate.
        //
        // TO CONSIDER: That said, perhaps the other tests *should* be run with
        // `self.repair` set to something, in case it matters in the future. In
        // which case, consider taking the state setting from a test like
        // `test_live_repair_flush_is_flush`, which sets up the LiveRepairData
        // as it might be right as any jobs are actually enqueued.

        ds
    }

    #[cfg(test)]
    pub(crate) fn disable_client_backpressure(&mut self) {
        self.backpressure_config.max_delay = Duration::ZERO;
    }

    pub(crate) fn set_client_backpressure(&self) {
        let mut jobs = vec![];
        let mut bytes = vec![];
        for c in self.clients.iter() {
            if matches!(c.state(), DsState::Active) {
                jobs.push(c.total_live_work());
                bytes.push(c.total_bytes_outstanding());
            }
        }
        // If none of the clients are active, then disable per-client
        // backpressure
        if jobs.is_empty() {
            for c in self.clients.iter() {
                c.set_delay_us(0);
            }
            return;
        }

        // The "slowest" Downstairs will have the highest values for jobs and
        // bytes, which we calculate here.  Then, we'll apply delays to clients
        // based on those values.
        let max_jobs = jobs.into_iter().max().unwrap();
        let max_bytes = bytes.into_iter().max().unwrap();

        let mut delays = ClientData::new(None);
        for i in ClientId::iter() {
            let c = &self.clients[i];
            if matches!(c.state(), DsState::Active) {
                // These values represent how much **faster** we are than the
                // slowest downstairs, and hence cause us to exert backpressure
                // on this client (to slow it down).
                let job_gap = (max_jobs - c.total_live_work()) as u64;
                let bytes_gap =
                    (max_bytes - c.total_bytes_outstanding()) as u64;

                let job_delay_us = (job_gap
                    .saturating_sub(self.backpressure_config.jobs_start)
                    as f64
                    * self.backpressure_config.jobs_scale)
                    .powf(2.0);

                let bytes_delay_us = (bytes_gap
                    .saturating_sub(self.backpressure_config.bytes_start)
                    as f64
                    * self.backpressure_config.bytes_scale)
                    .powf(2.0);

                let delay_us = (job_delay_us.max(bytes_delay_us) as u64)
                    .min(self.backpressure_config.max_delay.as_micros() as u64);

                delays[i] = Some(delay_us);
            }
        }
        // Cancel out any common delay, because it wouldn't be useful.
        //
        // (Seeing common delay would be unusual, because it would indicate that
        // one Downstairs is ahead by the bytes-based metric and another is
        // ahead by the jobs-based metric)
        let min_delay = *delays.iter().flatten().min().unwrap();
        delays.iter_mut().flatten().for_each(|c| *c -= min_delay);

        // Apply delay to clients
        for i in ClientId::iter() {
            if let Some(delay) = delays[i] {
                self.clients[i].set_delay_us(delay);
            } else {
                self.clients[i].set_delay_us(0);
            }
        }
    }

    fn notify_live_repair_start(&self, repair: &LiveRepairData) {
        if let Some(notify) = &self.notify {
            let log = self.log.new(o!("repair" => repair.id.to_string()));
            let mut repairs =
                Vec::with_capacity(repair.repair_downstairs.len());

            for cid in &repair.repair_downstairs {
                let Some(region_uuid) = self.clients[*cid].id() else {
                    // A downstairs doesn't have an id but is being repaired...?
                    warn!(log, "downstairs {cid} has a None id?");
                    continue;
                };

                let Some(target_addr) = self.clients[*cid].target_addr else {
                    // A downstairs doesn't have a target_addr but is being
                    // repaired...?
                    warn!(log, "downstairs {cid} has a None target_addr?");
                    continue;
                };

                repairs.push((region_uuid, target_addr));
            }

            notify.send(NotifyRequest::LiveRepairStart {
                upstairs_id: self.cfg.upstairs_id,
                session_id: self.cfg.session_id,
                repair_id: repair.id,
                repairs,
            })
        }
    }

    fn notify_live_repair_finish(&self, repair: &LiveRepairData) {
        if let Some(notify) = &self.notify {
            let log = self.log.new(o!("repair" => repair.id.to_string()));

            let mut repairs =
                Vec::with_capacity(repair.repair_downstairs.len());

            for cid in &repair.repair_downstairs {
                let Some(region_uuid) = self.clients[*cid].id() else {
                    // A downstairs doesn't have an id but is being repaired...?
                    warn!(log, "downstairs {cid} has a None id?");
                    continue;
                };

                let Some(target_addr) = self.clients[*cid].target_addr else {
                    // A downstairs doesn't have a target_addr but is being
                    // repaired...?
                    warn!(log, "downstairs {cid} has a None target_addr?");
                    continue;
                };

                repairs.push((region_uuid, target_addr));
            }

            notify.send(NotifyRequest::LiveRepairFinish {
                upstairs_id: self.cfg.upstairs_id,
                session_id: self.cfg.session_id,
                repair_id: repair.id,
                repairs,
                aborted: repair.aborting_repair,
            });
        }
    }

    fn notify_live_repair_progress(
        &self,
        repair_id: Uuid,
        current_extent: ExtentId,
    ) {
        if let Some(notify) = &self.notify {
            let extent_count = self.ddef.unwrap().extent_count();
            notify.send(NotifyRequest::LiveRepairProgress {
                upstairs_id: self.cfg.upstairs_id,
                repair_id,
                // surely we won't have u64::MAX extents
                current_item: current_extent.0 as i64,
                // i am serious, and don't call me shirley
                total_items: extent_count as i64,
            });
        }
    }

    fn notify_reconcile_start(&self, reconcile: &ReconcileData) {
        if let Some(notify) = &self.notify {
            let log = self.log.new(o!("reconcile" => reconcile.id.to_string()));

            // Reconcilation involves everyone
            let mut repairs = Vec::with_capacity(self.clients.len());

            for (cid, client) in self.clients.iter().enumerate() {
                let Some(region_uuid) = client.id() else {
                    // A downstairs doesn't have an id but is being reconciled...?
                    warn!(log, "downstairs {cid} has a None id?");
                    continue;
                };

                let Some(target_addr) = client.target_addr else {
                    // A downstairs doesn't have a target_addr but is being
                    // reconciled...?
                    warn!(log, "downstairs {cid} has a None target_addr?");
                    continue;
                };

                repairs.push((region_uuid, target_addr));
            }

            notify.send(NotifyRequest::ReconcileStart {
                upstairs_id: self.cfg.upstairs_id,
                repair_id: reconcile.id,
                session_id: self.cfg.session_id,
                repairs,
            });
        }
    }

    fn notify_reconcile_finished(
        &self,
        reconcile: &ReconcileData,
        aborted: bool,
    ) {
        if let Some(notify) = &self.notify {
            let log = self.log.new(o!("reconcile" => reconcile.id.to_string()));

            // Reconcilation involves everyone
            let mut repairs = Vec::with_capacity(self.clients.len());

            for (cid, client) in self.clients.iter().enumerate() {
                let Some(region_uuid) = client.id() else {
                    // A downstairs doesn't have an id but is being reconciled...?
                    warn!(log, "downstairs {cid} has a None id?");
                    continue;
                };

                let Some(target_addr) = client.target_addr else {
                    // A downstairs doesn't have a target_addr but is being
                    // reconciled...?
                    warn!(log, "downstairs {cid} has a None target_addr?");
                    continue;
                };

                repairs.push((region_uuid, target_addr));
            }

            notify.send(NotifyRequest::ReconcileFinish {
                upstairs_id: self.cfg.upstairs_id,
                session_id: self.cfg.session_id,
                repair_id: reconcile.id,
                aborted,
                repairs,
            });
        }
    }

    fn notify_reconcile_progress(
        &self,
        reconcile_id: Uuid,
        current_task: usize,
        task_count: usize,
    ) {
        if let Some(notify) = &self.notify {
            notify.send(NotifyRequest::ReconcileProgress {
                upstairs_id: self.cfg.upstairs_id,
                repair_id: reconcile_id,
                // surely we won't have usize::MAX extents
                current_item: current_task as i64,
                // i am serious, and don't call me shirley
                total_items: task_count as i64,
            });
        }
    }

    pub(crate) fn notify_client_task_stopped(
        &self,
        client_id: ClientId,
        reason: ClientRunResult,
    ) {
        if let Some(notify) = &self.notify {
            let Some(downstairs_id) = self.clients[client_id].id() else {
                return;
            };
            notify.send(NotifyRequest::ClientTaskStopped {
                upstairs_id: self.cfg.upstairs_id,
                downstairs_id,
                reason,
            });
        }
    }

    pub(crate) fn set_ddef(&mut self, ddef: RegionDefinition) {
        self.ddef = Some(ddef);
    }

    /// Checks whether there are any in-progress jobs present
    pub(crate) fn has_live_jobs(&self) -> bool {
        self.clients
            .iter()
            .any(|c| c.io_state_job_count().in_progress > 0)
    }

    /// Returns the per-client state for the given job
    ///
    /// This is a helper function to make unit tests shorter
    ///
    /// # Panics
    /// If the job isn't present in `ds_active`
    #[cfg(test)]
    fn job_state(&self, ds_id: JobId, cid: u8) -> IOState {
        self.ds_active.get(&ds_id).unwrap().state[ClientId::new(cid)].clone()
    }

    #[cfg(test)]
    fn job_states(&self, ds_id: JobId) -> [IOState; 3] {
        self.ds_active.get(&ds_id).unwrap().state.get().clone()
    }
}

/// Configuration for per-client backpressure
///
/// Per-client backpressure adds an artificial delay to the client queues, to
/// keep the three clients relatively in sync.  The delay is varied based on two
/// metrics:
///
/// - number of write bytes outstanding
/// - queue length
///
/// These metrics are _relative_ to the slowest downstairs; the goal is to slow
/// down the faster Downstairs to keep the gap bounded.
#[derive(Copy, Clone, Debug)]
struct DownstairsBackpressureConfig {
    /// When should backpressure start (in bytes)?
    bytes_start: u64,
    /// Scale for byte-based quadratic backpressure
    bytes_scale: f64,

    /// When should job-count-based backpressure start?
    jobs_start: u64,
    /// Scale for job-count-based quadratic backpressure
    jobs_scale: f64,

    /// Maximum delay
    max_delay: Duration,
}

#[cfg(test)]
pub(crate) mod test {
    use super::{
        ClientFaultReason, ClientNegotiationFailed, ClientStopReason,
        ConnectionMode, Downstairs, DsState, NegotiationStateData, PendingJob,
    };
    use crate::{
        downstairs::{LiveRepairData, LiveRepairState, ReconcileData},
        live_repair::ExtentInfo,
        upstairs::UpstairsState,
        BlockOpWaiter, BlockRes, ClientId, CrucibleError, DsStateData,
        ExtentFix, ExtentRepairIDs, IOState, IOop, ImpactedBlocks, JobId,
        RawReadResponse, ReconcileIO, ReconcileIOState, ReconciliationId,
        RegionMetadata, SnapshotDetails,
    };

    use bytes::BytesMut;
    use crucible_common::{BlockIndex, BlockOffset, ExtentId};
    use crucible_protocol::{Message, ReadBlockContext};
    use ringbuffer::RingBuffer;

    use std::{
        collections::{BTreeMap, HashMap},
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use uuid::Uuid;

    // Helper constants for verbose stopping states
    const STOP_FAULT_REQUESTED: DsState = DsState::Stopping(
        ClientStopReason::Fault(ClientFaultReason::RequestedFault),
    );
    const STOP_IO_ERROR: DsState =
        DsState::Stopping(ClientStopReason::Fault(ClientFaultReason::IOError));
    const STOP_FAILED_RECONCILE: DsState =
        DsState::Stopping(ClientStopReason::NegotiationFailed(
            ClientNegotiationFailed::FailedReconcile,
        ));

    /// Builds a single-block reply from the given request and response data
    pub fn build_read_response(data: &[u8]) -> RawReadResponse {
        RawReadResponse {
            data: BytesMut::from(data),
            blocks: vec![ReadBlockContext::Unencrypted {
                hash: crucible_common::integrity_hash(&[data]),
            }],
        }
    }

    /// Forces the given job to completion and acks it
    ///
    /// This calls `Downstairs`-internal APIs and is therefore in the same
    /// module, but should not be used outside of test code.
    #[cfg(test)]
    pub(crate) fn finish_job(ds: &mut Downstairs, ds_id: JobId) {
        for client_id in ClientId::iter() {
            ds.process_ds_completion(
                ds_id,
                client_id,
                Ok(RawReadResponse::default()),
                &UpstairsState::Active,
                None,
            );
        }
    }

    /// Helper function to legally move the given client to live-repair
    fn to_live_repair_ready(ds: &mut Downstairs, to_repair: ClientId) {
        ds.fault_client(
            to_repair,
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );
        let mode = ConnectionMode::Faulted;
        for state in [
            NegotiationStateData::Start,
            NegotiationStateData::WaitForPromote,
            NegotiationStateData::WaitForRegionInfo,
            NegotiationStateData::GetExtentVersions,
            NegotiationStateData::LiveRepairReady,
        ] {
            ds.clients[to_repair].checked_state_transition(
                &UpstairsState::Active,
                DsStateData::Connecting { state, mode },
            );
        }
    }

    /// Helper function to legally move the given client to live-repair
    pub(super) fn move_to_live_repair(
        ds: &mut Downstairs,
        to_repair: ClientId,
    ) {
        to_live_repair_ready(ds, to_repair);
        ds.clients[to_repair].checked_state_transition(
            &UpstairsState::Active,
            DsStateData::LiveRepair,
        );
    }

    fn set_all_reconcile(ds: &mut Downstairs) {
        let mode = ConnectionMode::New;
        let up_state = UpstairsState::GoActive(BlockRes::dummy());
        for cid in ClientId::iter() {
            for state in [
                NegotiationStateData::Start,
                NegotiationStateData::WaitForPromote,
                NegotiationStateData::WaitForRegionInfo,
                NegotiationStateData::GetExtentVersions,
                NegotiationStateData::WaitQuorum(RegionMetadata::default()),
                NegotiationStateData::Reconcile,
            ] {
                ds.clients[cid].checked_state_transition(
                    &up_state,
                    DsStateData::Connecting { state, mode },
                );
            }
        }
    }

    #[test]
    fn work_flush_three_ok() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.submit_flush(None, None, None);

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert_eq!(ds.retired_ids.len(), 1);
    }

    // Ensure that a snapshot requires all three downstairs to return Ok
    #[test]
    fn work_flush_snapshot_needs_three() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.submit_flush(
            Some(SnapshotDetails {
                snapshot_name: String::from("snap"),
            }),
            None,
            None,
        );

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));

        assert_eq!(ds.retired_ids.len(), 1);
    }

    #[test]
    fn work_flush_one_error_then_ok() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.submit_flush(None, None, None);

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));

        assert_eq!(ds.retired_ids.len(), 1);
        // No skipped jobs here.
        assert!(ds.clients[ClientId::new(0)].skipped_jobs.is_empty());
        assert!(ds.clients[ClientId::new(1)].skipped_jobs.is_empty());
        assert!(ds.clients[ClientId::new(2)].skipped_jobs.is_empty());
    }

    #[test]
    fn work_flush_two_errors_equals_fail() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.submit_flush(None, None, None);

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(RawReadResponse::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert_eq!(ds.retired_ids.len(), 1);
    }

    #[test]
    fn work_read_one_ok() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_read_eob();

        let response = Ok(build_read_response(&[]));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        let response = Ok(build_read_response(&[]));

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        let response = Ok(build_read_response(&[]));

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None,
        ));

        // A flush is required to move work to completed
        assert!(ds.retired_ids.is_empty());
    }

    #[test]
    fn work_read_one_bad_two_ok() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_read_eob();

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        let response = Ok(build_read_response(&[]));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        let response = Ok(build_read_response(&[]));

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None,
        ));

        // A flush is required to move work to completed
        // That this is still zero is part of the test
        assert!(ds.retired_ids.is_empty());
    }

    #[test]
    fn work_read_two_bad_one_ok() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_read_eob();

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        let response = Ok(build_read_response(&[]));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None,
        ));

        // A flush is required to move work to completed
        assert!(ds.retired_ids.is_empty());
    }

    #[test]
    fn work_read_three_bad() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_read_eob();

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.retired_ids.is_empty());

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.retired_ids.is_empty());
    }

    #[test]
    fn work_read_two_ok_one_bad() {
        // Test that missing data on the 2nd read response will panic
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_read_eob();

        let response = || Ok(build_read_response(&[]));

        // ack on first reply
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response(),
            &UpstairsState::Active,
            None,
        ));

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response(),
            &UpstairsState::Active,
            None,
        ));

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        // Work won't be completed until we get a flush.
        assert!(ds.retired_ids.is_empty());
    }

    #[test]
    fn work_write_unwritten_errors_are_counted() {
        // Verify that write IO errors are counted.
        work_errors_are_counted(false);
    }

    // Verify that write_unwritten IO errors are counted.
    #[test]
    fn work_write_errors_are_counted() {
        work_errors_are_counted(true);
    }

    // Instead of copying all the write tests, we put a wrapper around them
    // that takes write_unwritten as an arg.
    fn work_errors_are_counted(is_write_unwritten: bool) {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // send a write, and clients 0 and 1 will return errors
        // job is fast-acked, so it starts as acked

        let next_id =
            ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        let response = Ok(Default::default());
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.clients[ClientId::new(0)].stats.downstairs_errors > 0);
        assert!(ds.clients[ClientId::new(1)].stats.downstairs_errors > 0);
        assert_eq!(ds.clients[ClientId::new(2)].stats.downstairs_errors, 0);
    }

    #[test]
    fn work_delay_completion_flush_write() {
        work_delay_completion_flush(false);
    }

    #[test]
    fn work_delay_completion_flush_write_unwritten() {
        work_delay_completion_flush(true);
    }

    fn work_delay_completion_flush(is_write_unwritten: bool) {
        // Verify that a write/write_unwritten remains on the active
        // queue until a flush comes through and clears it.  In this case,
        // we only complete 2/3 for each IO.  We later come back and finish
        // the 3rd IO and the flush, which then allows the work to be
        // completed.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create two writes and send them to the downstairs
        let id1 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);
        let id2 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // Simulate completing both writes to downstairs 0 and 1
        //
        // all writes are acked immediately upon submission, so
        // process_ds_completion always returns `false`.
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));

        // Check that both writes have been fast-acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);
        assert!(ds.ds_active.get(&id2).unwrap().acked);

        // Work stays on active queue till the flush
        assert!(ds.retired_ids.is_empty());

        // Create the flush and send it to the downstairs
        let flush_id = ds.submit_flush(None, None, None);

        // Simulate completing the flush to downstairs 0 and 1
        assert!(!ds.process_ds_completion(
            flush_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(ds.process_ds_completion(
            flush_id,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));

        // Make sure downstairs 0 and 1 update their last flush id and
        // that downstairs 2 does not.
        assert_eq!(ds.clients[ClientId::new(0)].last_flush(), Some(flush_id));
        assert_eq!(ds.clients[ClientId::new(1)].last_flush(), Some(flush_id));
        assert_eq!(ds.clients[ClientId::new(2)].last_flush(), None);

        // Make sure all work is still on the active side
        assert!(ds.retired_ids.is_empty());

        // Now, finish the writes to downstairs 2
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));

        // The job should not move to completed until the flush goes as well.
        assert!(ds.retired_ids.is_empty());

        // Complete the flush on downstairs 2.
        assert!(!ds.process_ds_completion(
            flush_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));

        // All three jobs should now move to completed
        assert_eq!(ds.retired_ids.len(), 3);
        // Downstairs 2 should update the last flush it just did.
        assert_eq!(ds.clients[ClientId::new(2)].last_flush(), Some(flush_id));
    }

    #[test]
    fn work_assert_reads_do_not_cause_failure_state_transition() {
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // send a read, and clients 0 and 1 will return errors

        let next_id = ds.create_and_enqueue_generic_read_eob();

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        let response = Ok(build_read_response(&[3]));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None
        ));

        // Confirm that we've stored this block
        let responses = ds.ds_active.get(&next_id).unwrap().data.as_ref();
        assert!(responses.is_some());
        assert_eq!(
            responses.map(|responses| &responses.data[..]),
            Some([3].as_slice())
        );

        assert_eq!(ds.clients[ClientId::new(0)].stats.downstairs_errors, 1);
        assert_eq!(ds.clients[ClientId::new(1)].stats.downstairs_errors, 1);
        assert_eq!(ds.clients[ClientId::new(2)].stats.downstairs_errors, 0);

        // send another read, and expect all to return something
        // (reads shouldn't cause a Failed transition)

        let next_id = ds.create_and_enqueue_generic_read_eob();

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.ds_active.get(&next_id).unwrap().data.is_none());

        let response = Ok(build_read_response(&[6]));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None
        ));

        let responses = ds.ds_active.get(&next_id).unwrap().data.as_ref();
        assert!(responses.is_some());
        assert_eq!(
            responses.map(|responses| &responses.data[..]),
            Some([6].as_slice())
        );
    }

    #[test]
    fn work_completed_read_flush() {
        // Verify that a read remains on the active queue until a flush
        // comes through and clears it.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build our read, put it into the work queue
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Downstairs 0 now has completed this work.
        let response = Ok(build_read_response(&[]));
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));

        // One completion of a read means we can ACK
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Complete downstairs 1 and 2
        let response = Ok(build_read_response(&[]));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None
        ));

        let response = Ok(build_read_response(&[]));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None
        ));

        // Make sure the job is not yet retired
        assert!(ds.retired_ids.is_empty());

        // A flush is required to move work to completed
        // Create the flush then send it to all downstairs.
        let (mut rx, tx) = BlockOpWaiter::pair();
        let next_id = ds.submit_flush(None, Some(tx), None);

        // Complete the Flush at each downstairs.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        // Two completed means we return true (ack ready now)
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Verify the flush has been acked
        assert!(rx.try_wait().unwrap().is_ok());

        // The read and the flush should now be moved to completed.
        assert_eq!(ds.retired_ids.len(), 2);
    }

    #[test]
    fn retire_dont_retire_everything() {
        // Verify that a read not ACKED remains on the active queue even
        // if a flush comes through after it.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build our read, put it into the work queue
        let read_id = ds.create_and_enqueue_generic_read_eob();

        // Downstairs 0 now has completed this work.
        let response = Ok(build_read_response(&[]));
        assert!(ds.process_ds_completion(
            read_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));

        // One completion of a read means we can ACK
        assert!(ds.ds_active.get(&read_id).unwrap().acked);

        // A flush is required to move work to completed
        // Create the flush then send it to all downstairs.
        let (mut rx, tx) = BlockOpWaiter::pair();
        let flush_id = ds.submit_flush(None, Some(tx), None);

        // Send and complete the Flush at each downstairs.
        for cid in ClientId::iter() {
            ds.process_ds_completion(
                flush_id,
                cid,
                Ok(Default::default()),
                &UpstairsState::Active,
                None,
            );
        }
        assert!(rx.try_wait().unwrap().is_ok());

        // Verify the read is still acked
        assert!(ds.ds_active.get(&read_id).unwrap().acked);
        // The the flush should now be moved to completed.
        assert_eq!(ds.retired_ids.len(), 1);
        // The read should still be on the queue.
        assert_eq!(ds.ds_active.len(), 1);
    }

    #[test]
    fn work_completed_write_flush() {
        work_completed_writeio_flush(false);
    }

    #[test]
    fn work_completed_write_unwritten_flush() {
        work_completed_writeio_flush(true);
    }

    fn work_completed_writeio_flush(is_write_unwritten: bool) {
        // Verify that a write or write_unwritten remains on the active
        // queue until a flush comes through and clears it.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build our write IO.
        let next_id =
            ds.create_and_enqueue_generic_write_eob(is_write_unwritten);
        // Put the write on the queue.

        // Complete the write on all three downstairs.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Check that it should be fast-acked
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Work stays on active queue till the flush
        assert!(ds.retired_ids.is_empty());

        // Create the flush IO
        let (mut rx, tx) = BlockOpWaiter::pair();
        let next_id = ds.submit_flush(None, Some(tx), None);

        // Complete the flush on all three downstairs, acking on the 2nd
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        assert!(rx.try_wait().unwrap().is_ok());
        // The write and flush should now be completed.
        assert_eq!(ds.retired_ids.len(), 2);
    }

    #[test]
    fn work_delay_completion_flush_order_write() {
        work_delay_completion_flush_order(false);
    }

    #[test]
    fn work_delay_completion_flush_order_write_unwritten() {
        work_delay_completion_flush_order(true);
    }

    fn work_delay_completion_flush_order(is_write_unwritten: bool) {
        // Verify that a write/write_unwritten remains on the active queue
        // until a flush comes through and clears it.  In this case, we only
        // complete 2 of 3 for each IO.  We later come back and finish the
        // 3rd IO and the flush, which then allows the work to be completed.
        // Also, we mix up which client finishes which job first.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build two writes, put them on the work queue.
        let id1 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);
        let id2 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // Complete the writes on the 2 downstairs.
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Verify that this job was fast-acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);
        assert!(ds.ds_active.get(&id2).unwrap().acked);

        // Work stays on active queue till the flush.
        assert!(ds.retired_ids.is_empty());

        // Create and send the flush.
        let flush_id = ds.submit_flush(None, None, None);

        // Complete the flush on those downstairs.
        assert!(!ds.process_ds_completion(
            flush_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(ds.process_ds_completion(
            flush_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Not done yet, until all clients do the work.
        assert!(ds.retired_ids.is_empty());

        // Verify who has updated their last flush.
        assert_eq!(ds.clients[ClientId::new(0)].last_flush(), Some(flush_id));
        assert_eq!(ds.clients[ClientId::new(1)].last_flush(), None);
        assert_eq!(ds.clients[ClientId::new(2)].last_flush(), Some(flush_id));

        // Now, complete the writes
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id2,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Completed work won't happen till the last flush is done
        assert!(ds.retired_ids.is_empty());

        // Complete the flush
        assert!(!ds.process_ds_completion(
            flush_id,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Now, all three jobs (w,w,f) will move to completed.
        assert_eq!(ds.retired_ids.len(), 3);

        // downstairs 1 should now have that flush
        assert_eq!(ds.clients[ClientId::new(1)].last_flush(), Some(flush_id));
    }

    #[test]
    fn work_completed_read_replay() {
        // Verify that a single read will replay
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build our read IO and submit it to the work queue.
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Complete the read on one downstairs.
        let response = Ok(build_read_response(&[]));
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));

        // One completion should allow for an ACK
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Be sure the job is not yet in replay
        assert!(!ds.ds_active.get(&next_id).unwrap().replay);
        ds.replay_jobs(ClientId::new(0));
        // Now the IO should be replay
        assert!(ds.ds_active.get(&next_id).unwrap().replay);

        // The IO is still acked
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    #[test]
    fn work_completed_two_read_replay() {
        // Verify that a read will replay and acks are handled correctly if
        // there is more than one done read.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Build a read and put it on the work queue.
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Complete the read on one downstairs, verify it is ack ready.
        let response = Ok(build_read_response(&[1, 2, 3, 4]));
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Complete the read on a 2nd downstairs.
        let response = Ok(build_read_response(&[1, 2, 3, 4]));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None
        ));

        // Now, take the first downstairs offline.
        ds.replay_jobs(ClientId::new(0));

        // The ack should still be done
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Taking the second downstairs offline, the ack should still be done
        ds.replay_jobs(ClientId::new(1));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Redo the read on DS 0, IO should remain acked
        assert_eq!(ds.job_state(next_id, 0), IOState::InProgress);

        let response = Ok(build_read_response(&[]));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    #[test]
    fn work_completed_ack_read_replay() {
        // Verify that a read we Acked will still replay if that downstairs
        // goes away. Make sure everything still finishes ok.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the read and submit it to the three Downstairs queues
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Complete the read on one downstairs.
        let response = Ok(build_read_response(&[]));
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // The read has been acked
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Verify the IO has not completed yet.
        assert!(ds.retired_ids.is_empty());

        // Now, take that downstairs offline
        ds.replay_jobs(ClientId::new(0));

        // Acked IO should remain so, but should be newly InProgress
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
        assert_eq!(ds.job_state(next_id, 0), IOState::InProgress);

        // Redo on DS 0, IO should remain acked.
        let response = Ok(build_read_response(&[]));
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    // XXX another test that does read replay for the first IO
    // Also, the third IO?
    #[test]
    fn work_completed_ack_read_replay_hash_mismatch() {
        // Verify that a read replay won't cause a panic on hash mismatch.
        // During a replay, the same block may have been written after a read,
        // so the actual contents of the block are now different.  We can't
        // compare the read hash taken before a replay to one that happens
        // after.  A sample sequence is:
        //
        // Flush (all previous IO cleared, all blocks the same)
        // Read block 0
        // Write block 0
        // - Downstairs goes away here, causing a replay.
        //
        // In this case, the replay read will get the data again, but that
        // data came from the write, which is different than the original,
        // pre-replay read data (and hash).  In this case we can't compare
        // with the original hash that we stored for this read.
        //
        // For the test below, we don't actually need to do a write, we
        // can just change the "data" we fill the response with like we
        // received different data than the original read.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the read and submit to the three Downstairs
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Construct our fake response
        let response = Ok(build_read_response(
            &[122], // Original data.
        ));

        // Complete the read on one downstairs.
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Before we replay_jobs, the IO is not replay
        assert!(!ds.ds_active.get(&next_id).unwrap().replay);
        // Now, take that downstairs offline
        ds.replay_jobs(ClientId::new(0));
        // Now the IO should be replay
        assert!(ds.ds_active.get(&next_id).unwrap().replay);
        assert_eq!(ds.job_state(next_id, 0), IOState::InProgress);

        // Now, create a new response that has different data, and will
        // produce a different hash.
        let response = Ok(build_read_response(
            &[123], // Different data than before
        ));

        // Process the new read (with different data), make sure we don't
        // trigger the hash mismatch check.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));

        // Some final checks.  The replay should behave in every other way
        // like a regular read.
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    #[test]
    fn work_completed_ack_read_replay_two_hash_mismatch() {
        // Verify that a read replay won't cause a panic on hash mismatch.
        // During a replay, the same block may have been written after a read,
        // so the actual contents of the block are now different.  We can't
        // compare the read hash taken before a replay to one that happens
        // after.  A sample sequence is:
        //
        // Flush (all previous IO cleared, all blocks the same)
        // Read block 0
        // Write block 0
        //
        // In this case, the replay read will get the data again, but that
        // data came from the write, which is different than the original,
        // pre-replay read data (and hash).  In this case we can't compare
        // with the original hash that we stored for this read.
        //
        // For the test below, we don't actually need to do a write, we
        // can just change the "data" we fill the response with like we
        // received different data than the original read.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the read and submit it to the three downstairs
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // Construct our fake response
        let response = Ok(build_read_response(
            &[122], // Original data.
        ));

        // Complete the read on one downstairs.
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response,
            &UpstairsState::Active,
            None
        ));
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Construct our fake response for another downstairs.
        let response = Ok(build_read_response(
            &[122], // Original data.
        ));

        // Complete the read on the this downstairs as well
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None
        ));

        // Now, take the second downstairs offline
        ds.replay_jobs(ClientId::new(1));
        // Now the IO should be replay
        assert!(ds.ds_active.get(&next_id).unwrap().replay);

        // The job should be newly InProgress
        assert_eq!(ds.job_state(next_id, 1), IOState::InProgress);

        // Now, create a new response that has different data, and will
        // produce a different hash.
        let response = Ok(build_read_response(
            &[123], // Different data than before
        ));

        // Process the new read (with different data), make sure we don't
        // trigger the hash mismatch check.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response,
            &UpstairsState::Active,
            None
        ));

        // Some final checks.  The replay should behave in every other way
        // like a regular read.
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    #[test]
    fn work_completed_write_ack_ready_replay_write() {
        work_completed_write_ack_ready_replay(false);
    }

    #[test]
    fn work_completed_write_ack_ready_replay_write_unwritten() {
        work_completed_write_ack_ready_replay(true);
    }

    fn work_completed_write_ack_ready_replay(is_write_unwritten: bool) {
        // Verify that a replay when we have two completed writes or
        // write_unwritten will change state from AckReady back to NotAcked.
        // If we then redo the work, it should go back to AckReady.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write and put it on the work queue.
        let id1 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // Complete the write on two downstairs.
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Verify that this job was fast-acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);

        /* Now, take that downstairs offline */
        // Before replay_jobs, the IO is not replay
        assert!(!ds.ds_active.get(&id1).unwrap().replay);
        ds.replay_jobs(ClientId::new(1));
        // Now the IO should be replay
        assert!(ds.ds_active.get(&id1).unwrap().replay);

        // The job should remain acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);

        // Re-submit and complete the write, which should not ack again
        assert_eq!(ds.job_state(id1, 1), IOState::InProgress);
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
    }

    #[test]
    fn work_completed_write_acked_replay_write() {
        work_completed_write_acked_replay(false);
    }

    #[test]
    fn work_completed_write_acked_replay_write_unwritten() {
        work_completed_write_acked_replay(true);
    }

    fn work_completed_write_acked_replay(is_write_unwritten: bool) {
        // Verify that a replay when we have acked a write or write_unwritten
        // will not undo that ack.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write and put it on the work queue.
        let id1 = ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // Complete the write on two downstairs.
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Check that it should have been fast-acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);

        // Now, take that downstairs offline
        ds.replay_jobs(ClientId::new(0));

        // State should stay acked
        assert!(ds.ds_active.get(&id1).unwrap().acked);

        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
        assert!(!ds.process_ds_completion(
            id1,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));
    }

    #[test]
    fn reconcile_rc_to_message() {
        // Convert an extent fix to the crucible repair messages that
        // are sent to the downstairs.  Verify that the resulting
        // messages are what we expect
        let mut ds = Downstairs::test_default();
        let r0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 801);
        let r1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 802);
        let r2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 803);
        ds.clients[ClientId::new(0)].repair_addr = Some(r0);
        ds.clients[ClientId::new(1)].repair_addr = Some(r1);
        ds.clients[ClientId::new(2)].repair_addr = Some(r2);

        let repair_extent = ExtentId(9);
        let mut rec_list = HashMap::new();
        let ef = ExtentFix {
            source: ClientId::new(0),
            dest: vec![ClientId::new(1), ClientId::new(2)],
        };
        rec_list.insert(repair_extent, ef);
        let max_flush = 22;
        let max_gen = 33;
        let mut reconcile_task_list =
            ds.convert_rc_to_messages(rec_list, max_flush, max_gen);

        // Walk the list and check for messages we expect to find
        assert_eq!(reconcile_task_list.len(), 4);

        // First task, flush
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(0));
        match rio.op {
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id,
                flush_number,
                gen_number,
            } => {
                assert_eq!(repair_id, ReconciliationId(0));
                assert_eq!(extent_id, repair_extent);
                assert_eq!(client_id, ClientId::new(0));
                assert_eq!(flush_number, max_flush);
                assert_eq!(gen_number, max_gen);
            }
            m => {
                panic!("{:?} not ExtentFlush()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Second task, close extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(1));
        match rio.op {
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, ReconciliationId(1));
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Third task, repair extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(2));
        match rio.op {
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                assert_eq!(repair_id, rio.id);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(source_client_id, ClientId::new(0));
                assert_eq!(source_repair_address, r0);
                assert_eq!(
                    dest_clients,
                    vec![ClientId::new(1), ClientId::new(2)]
                );
            }
            m => {
                panic!("{:?} not ExtentRepair", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Third task, close extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(3));
        match rio.op {
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, ReconciliationId(3));
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);
    }

    #[test]
    fn reconcile_rc_to_message_two() {
        // Convert another extent fix to the crucible repair messages that
        // are sent to the downstairs.  Verify that the resulting
        // messages are what we expect
        let mut ds = Downstairs::test_default();
        let r0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 801);
        let r1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 802);
        let r2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 803);
        ds.clients[ClientId::new(0)].repair_addr = Some(r0);
        ds.clients[ClientId::new(1)].repair_addr = Some(r1);
        ds.clients[ClientId::new(2)].repair_addr = Some(r2);

        let repair_extent = ExtentId(5);
        let mut rec_list = HashMap::new();
        let ef = ExtentFix {
            source: ClientId::new(2),
            dest: vec![ClientId::new(0), ClientId::new(1)],
        };
        rec_list.insert(repair_extent, ef);
        let max_flush = 66;
        let max_gen = 77;
        let mut reconcile_task_list =
            ds.convert_rc_to_messages(rec_list, max_flush, max_gen);

        // Walk the list and check for messages we expect to find
        assert_eq!(reconcile_task_list.len(), 4);

        // First task, flush
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(0));
        match rio.op {
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id,
                flush_number,
                gen_number,
            } => {
                assert_eq!(repair_id, ReconciliationId(0));
                assert_eq!(extent_id, repair_extent);
                assert_eq!(client_id, ClientId::new(2));
                assert_eq!(flush_number, max_flush);
                assert_eq!(gen_number, max_gen);
            }
            m => {
                panic!("{:?} not ExtentFlush()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Second task, close extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(1));
        match rio.op {
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, ReconciliationId(1));
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Third task, repair extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(2));
        match rio.op {
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                assert_eq!(repair_id, rio.id);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(source_client_id, ClientId::new(2));
                assert_eq!(source_repair_address, r2);
                assert_eq!(
                    dest_clients,
                    vec![ClientId::new(0), ClientId::new(1)]
                );
            }
            m => {
                panic!("{:?} not ExtentRepair", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);

        // Third task, close extent
        let rio = reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, ReconciliationId(3));
        match rio.op {
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, ReconciliationId(3));
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(0)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(1)]);
        assert_eq!(ReconcileIOState::New, rio.state[ClientId::new(2)]);
    }

    // Tests for reconciliation
    #[test]
    fn send_next_reconciliation_req_none() {
        // No repairs on the queue, should return None
        let mut ds = Downstairs::test_default();

        ds.reconcile = Some(ReconcileData::new([]));

        let w = ds.send_next_reconciliation_req();
        assert!(w); // reconciliation is "done", because there's nothing there
    }

    #[test]
    fn reconcile_repair_workflow_not_repair() {
        // Verify that reconciliation will not give out work if a downstairs is
        // not in the correct state, and that it will clear the work queue and
        // mark other downstairs as failed.
        let mut ds = Downstairs::test_default();
        set_all_reconcile(&mut ds);

        let close_id = ReconciliationId(0);
        let rep_id = ReconciliationId(1);

        // Put two jobs on the todo list
        ds.reconcile = Some(ReconcileData::new([
            ReconcileIO::new(
                close_id,
                Message::ExtentClose {
                    repair_id: close_id,
                    extent_id: ExtentId(1),
                },
            ),
            ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ExtentId(1),
                },
            ),
        ]));

        // Send the first reconciliation req
        assert!(!ds.send_next_reconciliation_req());

        // Fault client 1, so that later event handling will kick us out of
        // repair
        ds.fault_client(
            ClientId::new(1),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Send an ack to trigger the reconciliation state check
        let nw = ds.on_reconciliation_ack(
            ClientId::new(0),
            close_id,
            &UpstairsState::Active,
        );
        assert!(!nw);

        // The two troublesome tasks will end up in DsState::New.
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_FAILED_RECONCILE,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_FAULT_REQUESTED);
        assert_eq!(ds.clients[ClientId::new(2)].state(), STOP_FAILED_RECONCILE);

        // Verify that reconciliation has been stopped
        assert!(ds.reconcile.is_none());
    }

    #[test]
    fn reconcile_repair_workflow_not_repair_later() {
        // Verify that rep_done still works even after we have a downstairs
        // in the FailedReconcile state. Verify that attempts to get new work
        // after a failed repair now return none.
        let mut ds = Downstairs::test_default();
        set_all_reconcile(&mut ds);

        let up_state = UpstairsState::Active;
        let rep_id = ReconciliationId(0);

        ds.reconcile = Some(ReconcileData::new([ReconcileIO::new(
            rep_id,
            Message::ExtentClose {
                repair_id: rep_id,
                extent_id: ExtentId(1),
            },
        )]));

        // Send that job
        ds.send_next_reconciliation_req();

        // Downstairs 0 and 2 are okay, but 1 failed (for some reason!)
        assert!(!ds.on_reconciliation_ack(ClientId::new(0), rep_id, &up_state));
        ds.on_reconciliation_failed(
            ClientId::new(1),
            rep_id,
            ExtentId(1),
            CrucibleError::GenericError("test extent error".to_owned()),
            &up_state,
        );
        assert!(!ds.on_reconciliation_ack(ClientId::new(2), rep_id, &up_state));

        // Getting the next work to do should verify the previous is done,
        // and handle a state change for a downstairs.
        for c in ds.clients.iter() {
            assert_eq!(c.state(), STOP_FAILED_RECONCILE);
        }

        // Verify that reconciliation has stopped
        assert!(ds.reconcile.is_none());
    }

    #[test]
    #[should_panic]
    fn reconcile_rep_in_progress_bad1() {
        // Verify the same downstairs can't mark a job in progress twice
        let mut ds = Downstairs::test_default();

        let rep_id = ReconciliationId(0);
        ds.reconcile = Some(ReconcileData::new([ReconcileIO::new(
            rep_id,
            Message::ExtentClose {
                repair_id: rep_id,
                extent_id: ExtentId(1),
            },
        )]));

        // Send that req
        assert!(!ds.send_next_reconciliation_req());
        ds.send_next_reconciliation_req(); // panics
    }

    #[test]
    fn reconcile_repair_workflow_1() {
        let mut ds = Downstairs::test_default();
        set_all_reconcile(&mut ds);

        let up_state = UpstairsState::Active;
        let close_id = ReconciliationId(0);
        let rep_id = ReconciliationId(1);

        ds.reconcile = Some(ReconcileData::new([
            ReconcileIO::new(
                close_id,
                Message::ExtentClose {
                    repair_id: close_id,
                    extent_id: ExtentId(1),
                },
            ),
            ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ExtentId(1),
                },
            ),
        ]));

        // Send the close job.  Reconciliation isn't done at this point!
        assert!(!ds.send_next_reconciliation_req());

        // Ack the close job.  Reconciliation isn't done at this point, because
        // there's another job in the task list.
        for i in ClientId::iter() {
            assert!(!ds.on_reconciliation_ack(i, close_id, &up_state));
        }

        // The third ack will have sent the next reconciliation job
        assert!(ds.reconcile.as_ref().unwrap().task_list.is_empty());

        // Now, make sure we consider this done only after all three are done
        assert!(!ds.on_reconciliation_ack(ClientId::new(0), rep_id, &up_state));
        assert!(!ds.on_reconciliation_ack(ClientId::new(1), rep_id, &up_state));
        // The third ack finishes reconciliation!
        assert!(ds.on_reconciliation_ack(ClientId::new(2), rep_id, &up_state));
        assert_eq!(ds.reconcile_repair_needed(), 0);
        assert_eq!(ds.reconcile_repaired, 2);
    }

    #[test]
    fn reconcile_repair_workflow_2() {
        // Verify Done or Skipped works when checking for a complete repair
        let mut ds = Downstairs::test_default();
        set_all_reconcile(&mut ds);

        let up_state = UpstairsState::Active;
        let rep_id = ReconciliationId(1);

        // Queue up a repair message, which will be skiped for client 0
        ds.reconcile = Some(ReconcileData::new([ReconcileIO::new(
            rep_id,
            Message::ExtentRepair {
                repair_id: rep_id,
                extent_id: ExtentId(1),
                source_client_id: ClientId::new(0),
                source_repair_address: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    803,
                ),
                dest_clients: vec![ClientId::new(1), ClientId::new(2)],
            },
        )]));

        // Send the job.  Reconciliation isn't done at this point!
        assert!(!ds.send_next_reconciliation_req());

        // Mark all three as in progress
        let Some(job) = &ds.reconcile.as_ref().unwrap().current_work else {
            panic!("failed to find current work");
        };
        assert_eq!(job.state[ClientId::new(0)], ReconcileIOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], ReconcileIOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], ReconcileIOState::InProgress);

        assert!(!ds.on_reconciliation_ack(ClientId::new(1), rep_id, &up_state));
        // The second ack finishes reconciliation, because it was skipped for
        // client 0 (which was the source of repairs).
        assert!(ds.on_reconciliation_ack(ClientId::new(2), rep_id, &up_state));
        assert_eq!(ds.reconcile_repair_needed(), 0);
        assert_eq!(ds.reconcile_repaired, 1);
    }

    #[test]
    #[should_panic]
    fn reconcile_repair_inprogress_not_done() {
        // Verify Done or Skipped works when checking for a complete repair
        let mut ds = Downstairs::test_default();

        let up_state = UpstairsState::Active;
        let rep_id = ReconciliationId(1);

        // Queue up a repair message, which will be skiped for client 0
        ds.reconcile = Some(ReconcileData::new([ReconcileIO::new(
            rep_id,
            Message::ExtentRepair {
                repair_id: rep_id,
                extent_id: ExtentId(1),
                source_client_id: ClientId::new(0),
                source_repair_address: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    803,
                ),
                dest_clients: vec![ClientId::new(1), ClientId::new(2)],
            },
        )]));

        // Send the job.  Reconciliation isn't done at this point!
        assert!(!ds.send_next_reconciliation_req());

        // If we get back an ack from client 0, something has gone terribly
        // wrong (because the jobs should have been skipped for it)
        ds.on_reconciliation_ack(ClientId::new(0), rep_id, &up_state);
        // this should panic!
    }

    #[test]
    #[should_panic]
    fn reconcile_leave_no_job_behind() {
        // Verify we can't start a new job before the old is finished.
        // Verify Done or Skipped works when checking for a complete repair
        let mut ds = Downstairs::test_default();

        let up_state = UpstairsState::Active;
        let close_id = ReconciliationId(0);
        let rep_id = ReconciliationId(1);

        // Queue up a repair message, which will be skiped for client 0
        ds.reconcile = Some(ReconcileData::new([
            ReconcileIO::new(
                close_id,
                Message::ExtentClose {
                    repair_id: close_id,
                    extent_id: ExtentId(1),
                },
            ),
            ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ExtentId(1),
                },
            ),
        ]));

        // Send the first req; reconciliation is not yet done
        assert!(!ds.send_next_reconciliation_req());

        // Now, make sure we consider this done only after all three are done
        assert!(!ds.on_reconciliation_ack(ClientId::new(1), rep_id, &up_state));
        assert!(!ds.on_reconciliation_ack(ClientId::new(2), rep_id, &up_state));
        // don't finish

        ds.send_next_reconciliation_req(); // panics!
    }

    #[test]
    fn write_unwritten_single_skip() {
        // Verfy that a write_unwritten with one skipped job will still
        // result in success sent back to the guest.
        w_io_single_skip(true);
    }
    #[test]
    fn write_single_skip() {
        // Verfy that a write with one skipped job will still result in
        // success being sent back to the guest.
        w_io_single_skip(false);
    }

    fn w_io_single_skip(is_write_unwritten: bool) {
        // A single downstairs skip won't prevent us from acking back OK to the
        // guest for write operations
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Mark client 1 as faulted
        ds.fault_client(
            ClientId::new(1),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Create a write, enqueue it on both the downstairs
        // and the guest work queues.
        let next_id =
            ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // Process resplies from the two running downstairs
        let response = || Ok(Default::default());
        ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response(),
            &UpstairsState::Active,
            None,
        );

        ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response(),
            &UpstairsState::Active,
            None,
        );

        let done = ds.ds_active.get(&next_id).unwrap();
        assert!(done.acked); // fast-acked
        assert!(done.result().is_ok());
    }

    #[test]
    fn write_unwritten_double_skip() {
        // Verify that write IO errors are counted.
        w_io_double_skip(true);
    }

    #[test]
    fn write_double_skip() {
        // Verify that write IO errors are counted.
        w_io_double_skip(false);
    }

    fn w_io_double_skip(is_write_unwritten: bool) {
        // up_ds_listen test, a double skip on a write or write_unwritten
        // will result in an error back to the guest.
        // A single downstairs skip won't prevent us from acking back OK to the
        // guest for write operations
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Mark clients 1 and 2 as faulted
        for cid in [ClientId::new(1), ClientId::new(2)] {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a write, enqueue it on both the downstairs
        // and the guest work queues.
        let next_id =
            ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        );

        let done = ds.ds_active.get(&next_id).unwrap();
        assert!(done.acked);
        assert!(done.result().is_err());
    }

    #[test]
    fn write_unwritten_fail_and_skip() {
        // Verify that an write_unwritten error + a skip results in error
        // back to the guest
        w_io_fail_and_skip(true);
    }
    #[test]
    fn write_fail_and_skip() {
        // Verify that a write error + a skip results in error back to the
        // guest
        w_io_fail_and_skip(false);
    }

    fn w_io_fail_and_skip(is_write_unwritten: bool) {
        // up_ds_listen test, a fail plus a skip on a write or write_unwritten
        // will result in an error back to the guest.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        ds.fault_client(
            ClientId::new(2),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Create a write, enqueue it on both the downstairs
        // and the guest work queues.
        let next_id =
            ds.create_and_enqueue_generic_write_eob(is_write_unwritten);

        // DS 0, the good IO.
        ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        );

        // DS 1, return error.
        // On a write_unwritten, This will return true because we have now
        // completed all three IOs (skipped, ok, and error here).
        let res = ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        );

        assert!(!res, "job should already be acked");

        let done = ds.ds_active.get(&next_id).unwrap();
        assert!(done.acked);
        assert!(done.result().is_err());
    }

    #[test]
    fn flush_io_single_skip() {
        // up_ds_listen test, a single downstairs skip won't prevent us
        // from acking back OK for a flush to the guest.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        ds.fault_client(
            ClientId::new(1),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Create a flush, enqueue it on both the downstairs
        // and the guest work queues.
        let (mut rx, tx) = BlockOpWaiter::pair();
        let next_id = ds.submit_flush(None, Some(tx), None);

        let response = || Ok(Default::default());
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response(),
            &UpstairsState::Active,
            None,
        ));

        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response(),
            &UpstairsState::Active,
            None,
        )); // acked!

        assert!(rx.try_wait().unwrap().is_ok());
    }

    #[test]
    fn flush_io_double_skip() {
        // up_ds_listen test, a double skip on a flush will result in an error
        // back to the guest.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in [ClientId::new(1), ClientId::new(2)] {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a flush, enqueue it on both the downstairs
        // and the guest work queues.
        let next_id = ds.submit_flush(None, None, None);

        // It should be acked when the first completion returns
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
    }

    #[test]
    fn flush_io_fail_and_skip() {
        // up_ds_listen test, a fail plus a skip on a flush will result in an
        // error back to the guest.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        ds.fault_client(
            ClientId::new(0),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Create a flush, enqueue it on both the downstairs
        // and the guest work queues.
        let (mut rx, tx) = BlockOpWaiter::pair();
        let next_id = ds.submit_flush(None, Some(tx), None);

        // DS 1 has a failure, and this won't return true as we don't
        // have enough success yet to ACK to the guest.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            Err(CrucibleError::GenericError("bad".to_string())),
            &UpstairsState::Active,
            None,
        ));

        // DS 2 as it's the final IO will indicate it is time to notify
        // the guest we have a result for them.
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        assert!(rx.try_wait().unwrap().is_err());
    }

    #[test]
    fn work_writes_bad() {
        // Verify that three bad writes will ACK the IO, and set the
        // downstairs clients to failed.
        // This test also makes sure proper mutex behavior is used in
        // process_ds_operation.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        let next_id = ds.create_and_enqueue_generic_write_eob(false);

        // Set the error that everyone will use.
        let response = || Err(CrucibleError::GenericError("bad".to_string()));

        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            response(),
            &UpstairsState::Active,
            None,
        ));
        // client 0 is failed, the others should be okay still
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Process the operation for client 1
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response(),
            &UpstairsState::Active,
            None,
        ));
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Three failures, But since this is a write we already have marked
        // the ACK as ready.
        // Process the operation for client 2
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response(),
            &UpstairsState::Active,
            None,
        ));
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), STOP_IO_ERROR,);

        // Verify that this work should have been fast-acked
        assert!(ds.ds_active.get(&next_id).unwrap().acked);
    }

    #[test]
    fn read_after_write_fail_is_alright() {
        // Verify that if a single write fails on a downstairs, reads can still
        // be acked.
        //
        // Verify after acking IOs, we can then send a flush and
        // clear the jobs (some now failed/skipped) from the work queue.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write that fails on one DS
        let next_id = ds.create_and_enqueue_generic_write_eob(false);

        // Set the error that everyone will use.
        let err_response = Err(CrucibleError::GenericError("bad".to_string()));

        // Process the error operation for client 0
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            err_response,
            &UpstairsState::Active,
            None
        ));
        // client 0 should be marked failed.
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);

        let ok_response = || Ok(Default::default());
        // Process the good operation for client 1
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // the operation was already fast-acked, because it's a write
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            ok_response(),
            &UpstairsState::Active,
            None
        ));
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Verify we should have fast-ack this work
        let write_job = ds.ds_active.get(&next_id).unwrap();
        assert!(write_job.acked);

        // Now, do a read.

        let next_id = ds.create_and_enqueue_generic_read_eob();

        // As this DS is failed, it should have been skipped
        assert_eq!(
            ds.job_states(next_id),
            [IOState::Skipped, IOState::InProgress, IOState::InProgress]
        );

        // We should have one job on the skipped job list for failed DS
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        let response = || Ok(build_read_response(&[]));

        // Process the operation for client 1; we send the ack here
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            response(),
            &UpstairsState::Active,
            None,
        ));

        // Process the operation for client 2, which does not ack
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response(),
            &UpstairsState::Active,
            None
        ));

        // Perform the flush.
        let next_id = {
            let next_id = ds.submit_flush(None, None, None);

            // As this DS is failed, it should have been skipped
            assert_eq!(
                ds.job_states(next_id),
                [IOState::Skipped, IOState::InProgress, IOState::InProgress]
            );

            next_id
        };

        let ok_response = || Ok(Default::default());
        // Process the operation for client 1
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            ok_response(),
            &UpstairsState::Active,
            None,
        ));

        // process_ds_operation should ack after we process this.
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // The write, the read, and now the flush should be completed.
        assert_eq!(ds.completed().len(), 3);

        // The last skipped flush should still be on the skipped list
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(0)]
            .skipped_jobs
            .contains(&JobId(1002)));
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);
    }

    #[test]
    fn read_after_two_write_fail_is_alright() {
        // Verify that if two writes fail, a read can still be acked.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write that fails on two DS
        let next_id = ds.create_and_enqueue_generic_write_eob(false);

        // Set the error that everyone will use.
        let err_response =
            || Err(CrucibleError::GenericError("bad".to_string()));

        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            err_response(),
            &UpstairsState::Active,
            None,
        ));
        // client 0 is failed, the others should be okay still
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Process the operation for client 1
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            err_response(),
            &UpstairsState::Active,
            None
        ));
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        let ok_response = Ok(Default::default());
        // Because we fast-ACK writes, this op will always return false
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            ok_response,
            &UpstairsState::Active,
            None
        ));
        assert_eq!(ds.clients[ClientId::new(0)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Verify we should have fast-ackd this work
        let write_job = ds.ds_active.get(&next_id).unwrap();
        assert!(write_job.acked);

        // Now, do a read.
        let next_id = ds.create_and_enqueue_generic_read_eob();

        // As this DS is failed, the jobs should be skipped
        assert_eq!(
            ds.job_states(next_id),
            [IOState::Skipped, IOState::Skipped, IOState::InProgress]
        );

        // Two downstairs should have a skipped job on their lists.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        let response = Ok(build_read_response(&[]));

        // Process the operation for client 2, which acks the job
        assert!(ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            response,
            &UpstairsState::Active,
            None
        ));
    }

    #[test]
    fn write_after_write_fail_is_alright() {
        // Verify that if a single write fails on a downstairs, a second
        // write can still be acked.
        // Then, send a flush and verify the work queue is cleared.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write that fails on one DS
        let next_id = ds.create_and_enqueue_generic_write_eob(false);

        // Make the error and ok responses
        let err_response =
            || Err(CrucibleError::GenericError("bad".to_string()));
        let ok_response = || Ok(Default::default());

        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // Process the error for client 1
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(1),
            err_response(),
            &UpstairsState::Active,
            None
        ));

        // process_ds_operation for client 2; the job was already acked (because
        // it's a write) so this returns false
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // Verify client states
        assert_eq!(ds.clients[ClientId::new(0)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // A faulted write won't change skipped job count.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        // Verify this work should have been fast-acked
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // Now, do another write.
        let next_id = ds.create_and_enqueue_generic_write_eob(false);
        assert_eq!(
            ds.job_states(next_id),
            [IOState::InProgress, IOState::Skipped, IOState::InProgress]
        );

        // Process the operation for client 0, re-use ok_response from above.
        // This will return false as we don't have enough work done yet.
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(0),
            ok_response(),
            &UpstairsState::Active,
            None
        ));
        // We don't process client 1, it had failed

        // again, the job was acked immediately because it's a write
        assert!(!ds.process_ds_completion(
            next_id,
            ClientId::new(2),
            ok_response(),
            &UpstairsState::Active,
            None
        ));
        // Verify we should have fast-acked this work too
        assert!(ds.ds_active.get(&next_id).unwrap().acked);

        // One downstairs should have a skipped job on its list.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(1)]
            .skipped_jobs
            .contains(&JobId(1001)));
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        // Enqueue the flush.
        let flush_id = ds.submit_flush(None, None, None);

        assert_eq!(
            ds.job_states(flush_id),
            [IOState::InProgress, IOState::Skipped, IOState::InProgress]
        );

        let ok_response = || Ok(Default::default());
        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            flush_id,
            ClientId::new(0),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // process_ds_operation should ack after we process client 2.
        assert!(ds.process_ds_completion(
            flush_id,
            ClientId::new(2),
            ok_response(),
            &UpstairsState::Active,
            None
        ));

        // The two writes and the flush should be completed.
        assert_eq!(ds.completed().len(), 3);

        // Only the skipped flush should remain.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(1)]
            .skipped_jobs
            .contains(&JobId(1002)));
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);
    }

    #[test]
    fn write_fail_skips_new_jobs() {
        // Verify that if a single write fails on a downstairs, any work that
        // was IOState::InProgress for that downstairs will change to
        // IOState::Skipped.  This also verifies that the list of skipped jobs
        // for each downstairs has the inflight job added to it.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write that fails on one DS
        let write_id = ds.create_and_enqueue_generic_write_eob(false);

        // Now, add a read.
        let read_id = ds.create_and_enqueue_generic_read_eob();

        // Verify the read is all InProgress
        let job = ds.ds_active.get(&read_id).unwrap();

        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        // Make the error and ok responses
        let err_response = Err(CrucibleError::GenericError("bad".to_string()));

        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Process the error for client 1
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(1),
            err_response,
            &UpstairsState::Active,
            None
        ));

        // Process the operation for client 2.  The job was already acked, so
        // this returns `false`
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None
        ));

        // Verify client states
        assert_eq!(ds.clients[ClientId::new(0)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // Verify the read switched from InProgress to Skipped
        let job = ds.ds_active.get(&read_id).unwrap();

        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);
    }

    #[test]
    fn write_fail_skips_inprogress_jobs() {
        // Verify that if a single write fails on a downstairs, any
        // work that was IOState::InProgress for that downstairs will change
        // to IOState::Skipped.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create the write that fails on one DS
        let write_id = ds.create_and_enqueue_generic_write_eob(false);
        assert!(ds.ds_active.get(&write_id).unwrap().acked);

        // Now, add a read.
        let read_id = ds.create_and_enqueue_generic_read_eob();

        // Make the error and ok responses
        let err_response =
            || Err(CrucibleError::GenericError("bad".to_string()));
        let ok_res = || Ok(Default::default());

        // Process the operation for client 0
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(0),
            ok_res(),
            &UpstairsState::Active,
            None,
        ));

        // Process the error for client 1
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(1),
            err_response(),
            &UpstairsState::Active,
            None,
        ));

        // process_ds_completion should return false, because the job was
        // already acked (since it's a write)
        assert!(!ds.process_ds_completion(
            write_id,
            ClientId::new(2),
            ok_res(),
            &UpstairsState::Active,
            None
        ));

        // Verify client states
        assert_eq!(ds.clients[ClientId::new(0)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state(), STOP_IO_ERROR,);
        assert_eq!(ds.clients[ClientId::new(2)].state(), DsState::Active);

        // The write was fast-acked, and the read is still going
        assert!(ds.ds_active.get(&write_id).unwrap().acked);

        // Verify the read switched from new to skipped

        let job = ds.ds_active.get(&read_id).unwrap();

        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);
    }

    #[test]
    #[should_panic]
    fn deactivate_ds_not_when_initializing() {
        // No deactivate of downstairs when upstairs not active.
        let mut ds = Downstairs::test_default();

        // This should panic, because `up` is in the wrong state
        ds.try_deactivate(ClientId::new(0), &UpstairsState::Initializing);
    }

    #[test]
    fn write_fail_skips_many_jobs() {
        // Create a bunch of jobs, do some, then encounter a write error.
        // Make sure that older jobs are still okay, and failed job was
        // skipped.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        let write_one = ds.create_and_enqueue_generic_write_eob(false);
        assert!(ds.ds_active.get(&write_one).unwrap().acked);

        // Now, add a read.
        let read_one = ds.create_and_enqueue_generic_read_eob();

        // Make the read ok response
        let rr = || Ok(build_read_response(&[]));

        for cid in ClientId::iter() {
            ds.process_ds_completion(
                write_one,
                cid,
                Ok(Default::default()),
                &UpstairsState::Active,
                None,
            );
            ds.process_ds_completion(
                read_one,
                cid,
                rr(),
                &UpstairsState::Active,
                None,
            );
        }

        // The write has been fast-acked; the read has been acked
        assert!(ds.ds_active.get(&write_one).unwrap().acked);
        assert!(ds.ds_active.get(&read_one).unwrap().acked);

        // Verify all IOs are done

        for cid in ClientId::iter() {
            let job = ds.ds_active.get(&read_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
            let job = ds.ds_active.get(&write_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
        }
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        // New write, this one will have a failure
        // Create a write
        let write_fail = ds.create_and_enqueue_generic_write_eob(false);

        let err_response = Err(CrucibleError::GenericError("bad".to_string()));

        // Process the operation for client 0, 1, 2.  It should already be acked
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(2),
            err_response,
            &UpstairsState::Active,
            None,
        ));

        // Verify client states
        assert_eq!(ds.clients[ClientId::new(0)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(2)].state(), STOP_IO_ERROR,);

        // Verify all IOs are done
        for cid in ClientId::iter() {
            let job = ds.ds_active.get(&read_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
            let job = ds.ds_active.get(&write_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
        }
        let job = ds.ds_active.get(&write_fail).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Done);
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);
        assert_eq!(
            job.state[ClientId::new(2)],
            IOState::Error(CrucibleError::GenericError("bad".to_string()))
        );

        // A failed job does not change the skipped count.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);
    }

    #[test]
    fn write_fail_past_present_future() {
        // Create a bunch of jobs, finish some, then encounter a write error.
        // Make sure that older jobs are still okay, failed job was error,
        // and jobs not yet started on the faulted downstairs have
        // transitioned to skipped.
        let mut ds = Downstairs::test_default();
        ds.force_active();

        // Create a write
        let write_one = ds.create_and_enqueue_generic_write_eob(false);
        assert!(ds.ds_active.get(&write_one).unwrap().acked);

        // Now, add a read.
        let read_one = ds.create_and_enqueue_generic_read_eob();

        // Make the read ok response
        let rr = || Ok(build_read_response(&[]));

        for cid in ClientId::iter() {
            ds.process_ds_completion(
                write_one,
                cid,
                Ok(Default::default()),
                &UpstairsState::Active,
                None,
            );
            ds.process_ds_completion(
                read_one,
                cid,
                rr(),
                &UpstairsState::Active,
                None,
            );
        }

        // Verify the read has been acked (the write was fast-acked)
        assert!(ds.ds_active.get(&read_one).unwrap().acked);
        assert!(ds.ds_active.get(&write_one).unwrap().acked);

        // Verify all IOs are done

        for cid in ClientId::iter() {
            let job = ds.ds_active.get(&read_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
            let job = ds.ds_active.get(&write_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
        }

        // Create a New write, this one will fail on one downstairs
        let write_fail = ds.create_and_enqueue_generic_write_eob(false);
        assert!(ds.ds_active.get(&write_fail).unwrap().acked);

        // Response for the write failure
        let err_response = Err(CrucibleError::GenericError("bad".to_string()));

        // Create some reads as well that will be InProgress
        let read_two = ds.create_and_enqueue_generic_read_eob();

        // Process the write operation for downstairs 0, 1
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(0),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        // Have downstairs 2 return error.
        assert!(!ds.process_ds_completion(
            write_fail,
            ClientId::new(2),
            err_response,
            &UpstairsState::Active,
            None,
        ));

        // Verify client states
        assert_eq!(ds.clients[ClientId::new(0)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state(), DsState::Active);
        assert_eq!(ds.clients[ClientId::new(2)].state(), STOP_IO_ERROR,);

        // Verify all IOs are done
        for cid in ClientId::iter() {
            // First read, still Done
            let job = ds.ds_active.get(&read_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
            // First write, still Done
            let job = ds.ds_active.get(&write_one).unwrap();
            assert_eq!(job.state[cid], IOState::Done);
        }
        // The failing write, done on 0,1
        let job = ds.ds_active.get(&write_fail).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Done);
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);
        // The failing write, error on 2
        assert_eq!(
            job.state[ClientId::new(2)],
            IOState::Error(CrucibleError::GenericError("bad".to_string()))
        );

        // The reads that were in progress
        let job = ds.ds_active.get(&read_two).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);

        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);
    }

    #[test]
    fn faulted_downstairs_skips_but_still_does_work() {
        // Verify work can progress through the work queue even when one
        // downstairs has failed. One write, one read, and one flush.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        ds.fault_client(
            ClientId::new(0),
            &UpstairsState::Active,
            ClientFaultReason::RequestedFault,
        );

        // Create a write
        let write_one = ds.create_and_enqueue_generic_write_eob(false);

        // Now, add a read.
        let read_one = ds.create_and_enqueue_generic_read_eob();

        // Finally, add a flush
        let flush_one = ds.submit_flush(None, None, None);

        let job = ds.ds_active.get(&write_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        let job = ds.ds_active.get(&read_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        let job = ds.ds_active.get(&flush_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        // Three skipped jobs on downstairs client 0
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 3);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 0);

        // Do the write
        assert!(!ds.process_ds_completion(
            write_one,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            write_one,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));

        // Make the read ok response, do the read
        let rr = || Ok(build_read_response(&[]));

        assert!(ds.process_ds_completion(
            read_one,
            ClientId::new(1),
            rr(),
            &UpstairsState::Active,
            None,
        ));
        assert!(!ds.process_ds_completion(
            read_one,
            ClientId::new(2),
            rr(),
            &UpstairsState::Active,
            None,
        ));

        let job = ds.ds_active.get(&read_one).unwrap();
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);
        assert_eq!(job.state[ClientId::new(2)], IOState::Done);
        let job = ds.ds_active.get(&write_one).unwrap();
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);
        assert_eq!(job.state[ClientId::new(2)], IOState::Done);

        // Do the flush
        assert!(!ds.process_ds_completion(
            flush_one,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        let job = ds.ds_active.get(&flush_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);
        assert!(ds.process_ds_completion(
            flush_one,
            ClientId::new(2),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        // The flush retires itself and preceding jobs

        // Skipped jobs just has the flush
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(0)]
            .skipped_jobs
            .contains(&JobId(1002)));

        // The writes, the read, and the flush should be completed.
        assert_eq!(ds.completed().len(), 3);
        // No more jobs on the queue
        assert_eq!(ds.active_count(), 0);
    }

    #[test]
    fn two_faulted_downstairs_can_still_read() {
        // Verify we can still read (and clear the work queue) with only
        // one downstairs.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in [ClientId::new(0), ClientId::new(2)] {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a write
        let write_one = ds.create_and_enqueue_generic_write_eob(false);

        // Now, add a read.
        let read_one = ds.create_and_enqueue_generic_read_eob();

        // Finally, add a flush
        let (mut rx, tx) = BlockOpWaiter::pair();
        let flush_one = ds.submit_flush(None, Some(tx), None);

        let job = ds.ds_active.get(&write_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);

        let job = ds.ds_active.get(&read_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);

        let job = ds.ds_active.get(&flush_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);

        // Skipped jobs added on downstairs client 0
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 3);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 0);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 3);

        // Do the write
        assert!(!ds.process_ds_completion(
            write_one,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        let job = ds.ds_active.get(&write_one).unwrap();
        assert!(job.acked); // fast-ack
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);

        // Make the read ok response, do the read
        let rr = Ok(build_read_response(&[]));

        assert!(ds.process_ds_completion(
            read_one,
            ClientId::new(1),
            rr,
            &UpstairsState::Active,
            None,
        ));

        // Verify that the read IO is done
        let job = ds.ds_active.get(&read_one).unwrap();
        assert!(job.acked);
        assert_eq!(job.state[ClientId::new(1)], IOState::Done);

        // Do the flush, which retires other jobs
        assert!(ds.process_ds_completion(
            flush_one,
            ClientId::new(1),
            Ok(Default::default()),
            &UpstairsState::Active,
            None,
        ));
        assert!(rx.try_wait().unwrap().is_err());

        // Skipped jobs now just have the flush.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(0)]
            .skipped_jobs
            .contains(&JobId(1002)));
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);
        assert!(ds.clients[ClientId::new(2)]
            .skipped_jobs
            .contains(&JobId(1002)));

        // The writes, the read, and the flush should be completed.
        assert_eq!(ds.completed().len(), 3);
        // No more jobs on the queue
        assert_eq!(ds.active_count(), 0);
    }

    #[test]
    fn three_faulted_enqueue_will_handle_read() {
        // When three downstairs are faulted, verify that enqueue will move
        // work through the queue for us.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in ClientId::iter() {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a read, but don't move it to InProgress
        let read_one = ds.create_and_enqueue_generic_read_eob();

        let job = ds.ds_active.get(&read_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);

        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);

        // Verify jobs can be acked.
        assert!(ds.ds_active.get(&read_one).unwrap().acked);

        // Our skipped jobs have not yet been cleared.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);
    }

    #[test]
    fn three_faulted_enqueue_will_handle_write() {
        // When three downstairs are faulted, verify that enqueue will move
        // work through the queue for us.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in ClientId::iter() {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a write and send it to the downstairs clients
        let write_one = ds.create_and_enqueue_generic_write_eob(false);

        let job = ds.ds_active.get(&write_one).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);
        assert!(job.acked, "job should be fast-acked");

        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);

        // No flush, no change in skipped jobs.
        assert_eq!(ds.clients[ClientId::new(0)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(1)].skipped_jobs.len(), 1);
        assert_eq!(ds.clients[ClientId::new(2)].skipped_jobs.len(), 1);
    }

    #[test]
    fn three_faulted_enqueue_will_handle_flush() {
        // When three downstairs are faulted, verify that enqueue will move
        // work through the queue for us.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in ClientId::iter() {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a flush.
        let (mut rx, tx) = BlockOpWaiter::pair();
        let _ = ds.submit_flush(None, Some(tx), None);
        // This job should be insta-skipped and then retired, so it's gone
        assert!(rx.try_wait().unwrap().is_err());
        for cid in ClientId::iter() {
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 1);
            assert!(ds.clients[cid].skipped_jobs.contains(&JobId(1000)));
        }

        // The flush should remove all work from the ds queue.
        assert_eq!(ds.completed().len(), 1);
        // No more jobs on the queue
        assert_eq!(ds.active_count(), 0);
    }

    #[test]
    fn three_faulted_enqueue_will_handle_many_ios() {
        // When three downstairs are faulted, verify that enqueue will move
        // work through the queue for us. Several jobs are submitted and
        // a final flush should clean them out.
        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in ClientId::iter() {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a read.
        let read_one = ds.create_and_enqueue_generic_read_eob();

        // The read should be acked because all downstairs are offline
        assert!(ds.ds_active.get(&read_one).unwrap().acked);

        // Create a write.
        let write_one = ds.create_and_enqueue_generic_write_eob(false);

        // The write should be fast-acked
        assert!(ds.ds_active.get(&write_one).unwrap().acked);

        // Skipped jobs are not yet cleared.
        for cid in ClientId::iter() {
            assert!(ds.clients[cid].skipped_jobs.contains(&read_one));
            assert!(ds.clients[cid].skipped_jobs.contains(&write_one));
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 2);
        }

        // Create a flush
        let (mut rx, tx) = BlockOpWaiter::pair();
        let _flush_one = ds.submit_flush(None, Some(tx), None);
        // The flush instantly returns an error and retires old jobs
        assert!(rx.try_wait().unwrap().is_err());

        // The flush should remove all work from the ds queue.
        assert_eq!(ds.completed().len(), 3);
        // No more jobs on the queue
        assert_eq!(ds.active_count(), 0);

        // Skipped jobs now just has the flush
        for cid in ClientId::iter() {
            assert!(ds.clients[cid].skipped_jobs.contains(&JobId(1002)));
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 1);
        }
    }

    #[test]
    fn three_faulted_retire_skipped_some_leave_some() {
        // When three downstairs are faulted, verify that enqueue will move
        // work through the queue for us. Several jobs are submitted and
        // a flush, then several more jobs. Verify the jobs after the flush
        // stay on the ds_skipped_jobs list.

        let mut ds = Downstairs::test_default();
        ds.force_active();
        for cid in ClientId::iter() {
            ds.fault_client(
                cid,
                &UpstairsState::Active,
                ClientFaultReason::RequestedFault,
            );
        }

        // Create a read, write, flush
        let read_one = ds.create_and_enqueue_generic_read_eob();
        let write_one = ds.create_and_enqueue_generic_write_eob(false);

        // The read and write are skipped + acked
        for cid in ClientId::iter() {
            assert!(ds.clients[cid].skipped_jobs.contains(&read_one));
            assert!(ds.clients[cid].skipped_jobs.contains(&write_one));
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 2);
        }
        assert!(ds.ds_active.get(&read_one).unwrap().acked);
        assert!(ds.ds_active.get(&write_one).unwrap().acked);

        let (mut rx, tx) = BlockOpWaiter::pair();
        let flush_one = ds.submit_flush(None, Some(tx), None);
        assert!(rx.try_wait().unwrap().is_err());

        let read_two = ds.create_and_enqueue_generic_read_eob();
        let write_two = ds.create_and_enqueue_generic_write_eob(false);

        for cid in ClientId::iter() {
            assert!(ds.clients[cid].skipped_jobs.contains(&flush_one));
            assert!(ds.clients[cid].skipped_jobs.contains(&read_two));
            assert!(ds.clients[cid].skipped_jobs.contains(&write_two));
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 3);
        }
        assert!(ds.ds_active.get(&read_two).unwrap().acked);
        assert!(ds.ds_active.get(&write_two).unwrap().acked);

        let (mut rx, tx) = BlockOpWaiter::pair();
        let flush_two = ds.submit_flush(None, Some(tx), None);
        assert!(rx.try_wait().unwrap().is_err());

        for cid in ClientId::iter() {
            assert!(ds.clients[cid].skipped_jobs.contains(&flush_two));
            assert_eq!(ds.clients[cid].skipped_jobs.len(), 1);
        }
    }

    // verify new work can progress with one failed downstairs.
    // two failed downstairs, what to do?  All writes will fail, all flushes
    // will fail, so, what?

    // What follows is a number of tests for the repair solver code,
    // specifically the repair_or_noop() function.  This set of tests
    // makes use of a few layers of boilerplate to avoid a bunch of
    // code duplication and simplifies what the tests need to look like.
    //
    // The basic format of each test is.
    // Generate a desired good and bad ExtentInfo struct.
    // Decide what downstairs client should be the source for repair.
    // Decide which downstairs clients should be the destination for repair.
    //
    // To test, we send the good EI, the bad EI and the source/dest client IDs
    // and verify that the IOop::ExtentLiveRepair or IOop::ExtentNoOp is
    // generated (and populated) correctly.
    //
    #[test]
    fn test_solver_no_work() {
        // Make sure that the repair solver will return NoOp when a repair
        // is not required.
        let mut ds = Downstairs::test_default();

        // We walk through all the possible choices for extent under
        // repair to be sure they can all handle it.
        for source in ClientId::iter() {
            let ei = ExtentInfo {
                generation: 5,
                flush_number: 3,
                dirty: false,
            };
            assert!(ds.clients[ClientId::new(0)]
                .repair_info
                .replace(ei)
                .is_none());
            assert!(ds.clients[ClientId::new(1)]
                .repair_info
                .replace(ei)
                .is_none());
            assert!(ds.clients[ClientId::new(2)]
                .repair_info
                .replace(ei)
                .is_none());

            let repair_extent = if source == ClientId::new(0) {
                vec![ClientId::new(1), ClientId::new(2)]
            } else if source == ClientId::new(1) {
                vec![ClientId::new(0), ClientId::new(2)]
            } else {
                vec![ClientId::new(0), ClientId::new(1)]
            };
            let eid = ExtentId(0);
            let (_repair_ids, deps) = ds.get_repair_ids(eid);

            let repair_op = ds.repair_or_noop(
                eid,            // Extent
                deps,           // Vec<u64>
                source,         // Source extent
                &repair_extent, // Repair extent
            );

            println!("repair op: {:?}", repair_op);
            match repair_op {
                IOop::ExtentLiveNoOp { .. } => {}
                x => {
                    panic!("Incorrect work type returned: {:?}", x);
                }
            }
            println!("Passed for source {}", source);
        }
    }

    // Sub-test that a ExtentLiveRepair IOop is returned from repair_or_noop,
    // This sub-function allows us to try different source and repair clients
    // and verifies the expected results.
    //
    // This function requires you have already populated the downstairs
    // repair_info field with the desired extent info you wish to compare, and
    // You are sending in the source and repair(Vec) the client IDs you
    // expect to see in the resulting IOop::ExtentLiveRepair.
    fn what_needs_repair(
        ds: &mut Downstairs,
        source: ClientId,
        repair: Vec<ClientId>,
    ) {
        let (_repair_ids, deps) = ds.get_repair_ids(ExtentId(0));
        let repair_op = ds.repair_or_noop(
            ExtentId(0), // Extent
            deps,        // Vec<u64>
            source,
            &repair,
        );

        println!("repair op: {:?}", repair_op);

        match repair_op {
            IOop::ExtentLiveRepair {
                extent,
                source_downstairs,
                repair_downstairs,
                ..
            } => {
                assert_eq!(extent, ExtentId(0));
                assert_eq!(source_downstairs, source);
                assert_eq!(repair_downstairs, repair);
            }
            x => {
                panic!("Incorrect work type returned: {:?}", x);
            }
        }
    }

    // Given a good ExtentInfo, and a bad ExtentInfo, generate all the
    // possible combinations of downstairs clients with one source
    // downstairs client, and one repair downstairs client.
    //
    // We manually submit the good ExtentInfo and the bad ExtentInfo
    // into the downstairs hashmap at the proper place, then verify
    // that, after repair_or_noop() has run, we have generated the
    // expected IOop.
    fn submit_one_source_one_repair(
        ds: &mut Downstairs,
        good_ei: ExtentInfo,
        bad_ei: ExtentInfo,
    ) {
        for source in ClientId::iter() {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // First try one source, one repair
            let repair = if source == ClientId::new(0) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(1)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(0)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);

            // Next try the other downstairs to repair.
            let repair = if source == ClientId::new(2) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(1)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(2)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);
        }
    }

    // Given a good ExtentInfo, and a bad ExtentInfo, generate all the
    // possible combinations of downstairs clients with one source
    // extent, and two repair extents.
    //
    // We manually submit the good ExtentInfo and the bad ExtentInfo
    // into the downstairs hashmap at the proper place, then verify
    // that, after repair_or_noop() has run, we have generated the
    // expected IOop.
    fn submit_one_source_two_repair(
        ds: &mut Downstairs,
        good_ei: ExtentInfo,
        bad_ei: ExtentInfo,
    ) {
        for source in ClientId::iter() {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // One source, two repair
            let repair = if source == ClientId::new(0) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(1), ClientId::new(2)]
            } else if source == ClientId::new(1) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(0), ClientId::new(2)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(0), ClientId::new(1)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);
        }
    }

    #[test]
    fn test_solver_dirty_needs_repair_two() {
        // Make sure that the repair solver will see a dirty extent_info
        // field is true and mark that downstairs for repair.
        // We test with two downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: true,
        };
        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_dirty_needs_repair_one() {
        // Make sure that the repair solver will see a dirty extent_info
        // field is true and repair that downstairs.
        // We test with just one downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: true,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_gen_lower_needs_repair_one() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_gen_lower_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_gen_higher_needs_repair_one() {
        // Make sure that the repair solver will see a generation extent_info
        // field is higher on a downstairs client, and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 2,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_gen_higher_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 3,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 6,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_flush_lower_needs_repair_one() {
        // Make sure that the repair solver will see a flush extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 2,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_flush_lower_needs_repair_two() {
        // Make sure that the repair solver will see a flush extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 1,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_flush_higher_needs_repair_one() {
        // Make sure that the repair solver will see a flush extent_info
        // field is higher on a downstairs client, and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 9,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 9,
            flush_number: 4,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[test]
    fn test_solver_flush_higher_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = Downstairs::test_default();
        let g_ei = ExtentInfo {
            generation: 6,
            flush_number: 8,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 6,
            flush_number: 9,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    // Test function that will return a LiveRepairData struct populated with
    // data you provide for it.
    fn create_live_repair_data(
        extent_count: u32,
        repair_ids: ExtentRepairIDs,
        eid: ExtentId,
    ) -> Option<LiveRepairData> {
        let state = LiveRepairState::Closing {
            close_job: PendingJob::new(repair_ids.close_id),
            repair_job: PendingJob::new(repair_ids.repair_id),
            reopen_job: PendingJob::new(repair_ids.reopen_id),
            noop_job: PendingJob::new(repair_ids.noop_id),
        };
        Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count,
            repair_downstairs: vec![ClientId::new(1)],
            source_downstairs: ClientId::new(0),
            aborting_repair: false,
            active_extent: eid,
            min_id: repair_ids.close_id,
            repair_job_ids: BTreeMap::new(),
            state,
        })
    }

    #[test]
    fn test_live_repair_enqueue_reopen() {
        // Make sure the create_and_enqueue_reopen_io() function does
        // what we expect it to do

        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(0);

        // Upstairs "guest" work IDs.
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());

        ds.repair = create_live_repair_data(
            ds.ddef.unwrap().extent_count(),
            repair_ids,
            eid,
        );

        // deps for our reopen job
        deps.push(repair_ids.close_id);
        deps.push(repair_ids.repair_id);

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        ds.create_and_enqueue_reopen_io(eid, deps, repair_ids.reopen_id);

        let job = ds.ds_active.get(&repair_ids.reopen_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveReopen {
                dependencies: d,
                extent: e,
            } => {
                assert_eq!(*d, &[repair_ids.close_id, repair_ids.repair_id]);
                assert_eq!(*e, eid);
            }
            x => {
                panic!("Bad DownstairsIO type returned: {:?}", x);
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::InProgress);
        }
        assert!(!job.acked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[test]
    fn test_live_repair_enqueue_close() {
        // Make sure the create_and_enqueue_close_io() function does
        // what we expect it to do, which also tests create_close_io()
        // function as well.
        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(0);

        // Upstairs "guest" work IDs.
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());

        ds.repair = create_live_repair_data(
            ds.ddef.unwrap().extent_count(),
            repair_ids,
            eid,
        );

        deps.push(repair_ids.close_id);
        deps.push(repair_ids.repair_id);

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)j
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        // Set flush to non-zero to make the test more likely to detect an
        // incorrect flush number
        ds.next_flush = 0x1DE;
        let next_flush = ds.next_flush;
        let repair = vec![ClientId::new(0), ClientId::new(2)];
        ds.create_and_enqueue_close_io(eid, deps, repair_ids.close_id, &repair);

        let job = ds.ds_active.get(&repair_ids.close_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
                repair_downstairs,
            } => {
                assert_eq!(
                    *dependencies,
                    &[repair_ids.close_id, repair_ids.repair_id]
                );
                assert_eq!(*extent, eid);
                assert_eq!(*flush_number, next_flush);
                assert_eq!(*gen_number, ds.cfg.generation());
                assert_eq!(*repair_downstairs, repair);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentFlushClose, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::InProgress);
        }
        assert!(!job.acked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[test]
    fn test_live_repair_enqueue_repair_noop() {
        // Make sure the create_and_enqueue_repair_io() function does what we
        // expect it to do.  In this case we expect the job created to be a
        // no-op job.

        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(0);

        // Reserved job IDs
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());

        ds.repair = create_live_repair_data(
            ds.ddef.unwrap().extent_count(),
            repair_ids,
            eid,
        );

        deps.push(repair_ids.repair_id);
        deps.push(repair_ids.noop_id);

        let source = ClientId::new(0);
        let repair = vec![ClientId::new(1), ClientId::new(2)];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            ds.clients[cid].repair_info = Some(ei);
        }

        ds.create_and_enqueue_repair_io(
            eid,
            deps,
            repair_ids.repair_id,
            source,
            &repair,
        );

        let job = ds.ds_active.get(&repair_ids.repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveNoOp { dependencies } => {
                assert_eq!(
                    *dependencies,
                    [repair_ids.repair_id, repair_ids.noop_id]
                );
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveNoOp, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::InProgress);
        }
        assert!(!job.acked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[test]
    fn test_live_repair_enqueue_repair_repair() {
        // Make sure the create_and_enqueue_repair_io() function does
        // what we expect it to do.
        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(0);

        // Reserved work IDs.
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());

        ds.repair = create_live_repair_data(
            ds.ddef.unwrap().extent_count(),
            repair_ids,
            eid,
        );

        deps.push(repair_ids.repair_id);
        deps.push(repair_ids.noop_id);

        let source = ClientId::new(0);
        let repair = vec![ClientId::new(1), ClientId::new(2)];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        ds.clients[ClientId::new(0)].repair_info = Some(ei);
        ds.clients[ClientId::new(1)].repair_info = Some(ei);
        let bad_ei = ExtentInfo {
            generation: 5,
            flush_number: 2,
            dirty: false,
        };
        ds.clients[ClientId::new(2)].repair_info = Some(bad_ei);
        // We also need a fake repair address
        for cid in ClientId::iter() {
            ds.clients[cid].repair_addr =
                Some("127.0.0.1:1234".parse().unwrap());
        }

        ds.create_and_enqueue_repair_io(
            eid,
            deps,
            repair_ids.repair_id,
            source,
            &repair,
        );

        let job = ds.ds_active.get(&repair_ids.repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs,
                source_repair_address,
                repair_downstairs,
            } => {
                assert_eq!(
                    *dependencies,
                    [repair_ids.repair_id, repair_ids.noop_id]
                );
                assert_eq!(*extent, eid);
                assert_eq!(*source_downstairs, source);
                assert_eq!(
                    *source_repair_address,
                    "127.0.0.1:1234".parse().unwrap()
                );
                // We are only repairing 2 because it had the different
                // ExtentInfo.  0 and 1 were the same.
                assert_eq!(*repair_downstairs, vec![ClientId::new(2)]);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveRepair, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::InProgress);
        }
        assert!(!job.acked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    /// Create and enqueue a full set of repair functions
    ///
    /// This gets the required locks, determines the next job IDs, checks
    /// dependencies, and enqueues all four jobs (close / repair / noop /
    /// reopen).
    ///
    /// The repair job is **actually** enqueued as an `ExtentLiveNoop` as well
    /// (instead of an `ExtentLiveRepair`, because the latter requires more
    /// infrastructure to be set up in the `Downstairs`; specifically,
    /// `create_and_enqueue_repair_io` expects `Downstairs::repair_info` to be
    /// valid.
    ///
    /// This function creates the same dependency chain as `repair_extent`,
    /// which is actually used in a real system, but it doesn't wait for repairs
    /// to complete before enqueuing stuff.
    fn create_and_enqueue_repair_ops(
        ds: &mut Downstairs,
        eid: ExtentId,
    ) -> ExtentRepairIDs {
        let (extent_repair_ids, deps) = ds.get_repair_ids(eid);

        if ds.repair.is_none() {
            ds.repair = Some(LiveRepairData {
                id: Uuid::new_v4(),
                extent_count: 3,
                active_extent: eid,
                min_id: JobId(1000),
                repair_job_ids: BTreeMap::new(),
                source_downstairs: ClientId::new(0),
                repair_downstairs: vec![ClientId::new(1)],
                aborting_repair: false,
                state: LiveRepairState::Closing {
                    close_job: PendingJob::new(extent_repair_ids.close_id),
                    repair_job: PendingJob::new(extent_repair_ids.repair_id),
                    noop_job: PendingJob::new(extent_repair_ids.noop_id),
                    reopen_job: PendingJob::new(extent_repair_ids.reopen_id),
                },
            });
        }

        ds.create_and_enqueue_close_io(
            eid,
            deps,
            extent_repair_ids.close_id,
            &[ClientId::new(1)], // repair downstairs
        );
        ds.create_and_enqueue_noop_io(
            vec![extent_repair_ids.close_id],
            extent_repair_ids.repair_id,
        );
        ds.create_and_enqueue_noop_io(
            vec![extent_repair_ids.repair_id],
            extent_repair_ids.noop_id,
        );
        ds.create_and_enqueue_reopen_io(
            eid,
            vec![extent_repair_ids.noop_id],
            extent_repair_ids.reopen_id,
        );

        extent_repair_ids
    }

    // The next section of tests verify repair dependencies are honored.

    // W is Write
    // R is read
    // F is flush
    // Rp is a Repair

    #[test]
    fn test_live_repair_deps_writes() {
        // Test that writes on different blocks in the extent are all
        // captured by the repair at the end.
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 |   W   |
        //   2 |     W |
        //   3 | RpRpRp| 0,1,2
        //   4 | RpRpRp| 3
        //   5 | RpRpRp| 4
        //   6 | RpRpRp| 5

        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(1);

        // Write operations 0 to 2
        let mut ids = vec![];
        for i in 0..3 {
            let ds_id = ds.submit_test_write_block(eid, BlockOffset(i), false);
            ids.push(ds_id);
        }

        // Repair IO functions assume you have the locks
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);

        assert_eq!(ds.ds_active.len(), 7);

        // The three writes don't depend on anything
        assert!(ds.get_deps(ids[0]).is_empty());
        assert!(ds.get_deps(ids[1]).is_empty());
        assert!(ds.get_deps(ids[2]).is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(ds.get_deps(repair_ids.close_id), &ids);
    }

    #[test]
    fn test_live_repair_deps_reads() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   R   |
        //   2 |     R |
        //   3 | RpRpRp| 0,1,2
        //   4 | RpRpRp| 3
        //   5 | RpRpRp| 4
        //   6 | RpRpRp| 5

        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(1);

        // Create read operations 0 to 2
        let mut ids = vec![];
        for i in 0..3 {
            ids.push(ds.submit_read_block(eid, BlockOffset(i)));
        }

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);

        assert_eq!(ds.ds_active.len(), 7);

        // The three reads don't depend on anything
        assert!(ds.get_deps(ids[0]).is_empty());
        assert!(ds.get_deps(ids[1]).is_empty());
        assert!(ds.get_deps(ids[2]).is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(ds.get_deps(repair_ids.close_id), ids);
    }

    #[test]
    fn test_live_repair_deps_mix() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   W   |
        //   2 | F F F | 0,1
        //   3 |RpRpRp | 2
        //   4 |RpRpRp |
        //   5 |RpRpRp |
        //   6 |RpRpRp |

        let mut ds = Downstairs::repair_test_one_repair();
        let eid = ExtentId(1);

        let read_id = ds.submit_read_block(eid, BlockOffset(0));
        let write_id = ds.submit_test_write_block(eid, BlockOffset(1), false);
        let flush_id = ds.submit_flush(None, None, None);

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);

        assert_eq!(ds.ds_active.len(), 7);

        // The read and the write don't depend on anything
        assert!(ds.get_deps(read_id).is_empty());
        assert!(ds.get_deps(write_id).is_empty());
        // The flush requires the read and the write
        assert_eq!(ds.get_deps(flush_id), &[read_id, write_id]);
        // The repair will have just the flush
        assert_eq!(ds.get_deps(repair_ids.close_id), &[flush_id]);
    }

    #[test]
    fn test_live_repair_deps_repair() {
        // Basic test for inter-repair dependencies
        //
        // This only actually tests our test function
        // (`create_and_enqueue_repair_ops`), not the actual `repair_extent`,
        // but is still worthwhile
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp| 0
        //   2 | RpRpRp| 1
        //   3 | RpRpRp| 2
        let mut ds = Downstairs::repair_test_one_repair();

        let eid = ExtentId(1);
        let ids = create_and_enqueue_repair_ops(&mut ds, eid);

        assert_eq!(ds.ds_active.len(), 4);

        assert_eq!(ids.close_id, JobId(1000));
        assert!(ds.get_deps(ids.close_id).is_empty());
        assert_eq!(ids.repair_id, JobId(1001));
        assert_eq!(ds.get_deps(ids.repair_id), &[JobId(1000)]);
        assert_eq!(ids.noop_id, JobId(1002));
        assert_eq!(ds.get_deps(ids.noop_id), &[JobId(1001)]);
        assert_eq!(ids.reopen_id, JobId(1003));
        assert_eq!(ds.get_deps(ids.reopen_id), &[JobId(1002)]);
    }

    #[test]
    fn test_live_repair_deps_repair_write() {
        // Write after repair depends on the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp| 0
        //   2 | RpRpRp| 1
        //   3 | RpRpRp| 2
        //   4 |     W | 3

        let mut ds = Downstairs::repair_test_one_repair();
        let eid = ExtentId(1);

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);

        let write_id = ds.submit_test_write_block(eid, BlockOffset(2), false);

        assert_eq!(ds.ds_active.len(), 5);

        assert_eq!(repair_ids.close_id, JobId(1000));
        assert!(ds.get_deps(repair_ids.close_id).is_empty());

        assert_eq!(write_id, JobId(1004));
        assert_eq!(ds.get_deps(write_id), &[repair_ids.reopen_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_read() {
        // Read after repair requires the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp|
        //   2 | RpRpRp|
        //   3 | RpRpRp|
        //   4 | R     | 3
        let mut ds = Downstairs::repair_test_one_repair();
        let eid = ExtentId(1);

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);

        let read_id = ds.submit_read_block(eid, BlockOffset(0));

        assert_eq!(ds.ds_active.len(), 5);

        assert_eq!(repair_ids.close_id, JobId(1000));
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        // The read depends on the last item of the repair
        assert_eq!(read_id, JobId(1004));
        assert_eq!(ds.get_deps(read_id), &[repair_ids.reopen_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_flush() {
        // Flush after repair requires the flush
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp|
        //   2 | RpRpRp|
        //   3 | RpRpRp|
        //   4 | F F F | 3
        let mut ds = Downstairs::repair_test_one_repair();
        let eid = ExtentId(1);

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, eid);
        let flush_id = ds.submit_flush(None, None, None);

        assert_eq!(ds.ds_active.len(), 5);

        assert_eq!(repair_ids.close_id, JobId(1000));
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        // The flush depends on the repair close operation
        assert_eq!(flush_id, JobId(1004));
        assert_eq!(ds.get_deps(flush_id), &[repair_ids.reopen_id]);
    }

    #[test]
    fn test_live_repair_deps_no_overlap() {
        // No overlap, no deps, IO before repair
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |     W |       |       |
        //   1 |       |       | R     |
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |       | RpRpRp|       |
        let mut ds = Downstairs::repair_test_one_repair();

        let write_id =
            ds.submit_test_write_block(ExtentId(0), BlockOffset(2), false);
        let read_id = ds.submit_read_block(ExtentId(2), BlockOffset(0));
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 6);

        // The read and the write don't depend on anything
        assert!(ds.get_deps(write_id).is_empty());
        assert!(ds.get_deps(read_id).is_empty());
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
    }

    #[test]
    fn test_live_repair_deps_after_no_overlap() {
        // No overlap no deps IO after repair.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       | RpRpRp|       |
        //   1 |       | RpRpRp|       |
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       |       |   R   |
        //   5 |     W |       |       |
        let mut ds = Downstairs::repair_test_one_repair();

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));
        let write_id =
            ds.submit_test_write_block(ExtentId(0), BlockOffset(2), false);
        let read_id = ds.submit_read_block(ExtentId(2), BlockOffset(1));

        assert_eq!(ds.ds_active.len(), 6);

        // The read and the write don't depend on anything
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        assert!(ds.get_deps(write_id).is_empty());
        assert!(ds.get_deps(read_id).is_empty());
    }

    #[test]
    fn test_live_repair_deps_flush_repair_flush() {
        // Flush Repair Flush
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | F F F | F F F |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |       | RpRpRp|
        //   5 | F F F | F F F | 0,4

        let mut ds = Downstairs::repair_test_one_repair();

        let flush_id1 = ds.submit_flush(None, None, None);
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));
        let flush_id2 = ds.submit_flush(None, None, None);

        assert_eq!(ds.ds_active.len(), 6);

        assert_eq!(flush_id1, JobId(1000));
        assert!(ds.get_deps(flush_id1).is_empty());

        assert_eq!(repair_ids.close_id, JobId(1001));
        assert_eq!(ds.get_deps(repair_ids.close_id), &[JobId(1000)]);

        assert_eq!(flush_id2, JobId(1005));
        assert_eq!(ds.get_deps(flush_id2), &[JobId(1000), JobId(1004)]);
    }

    #[test]
    fn test_live_repair_deps_repair_flush_repair() {
        // Repair Flush Repair
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | RpRpRp|       |
        //   1 | RpRpRp|       |
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | F F F | F F F | 3
        //   5 |       | RpRpRp| 4
        //   6 |       | RpRpRp|
        //   7 |       | RpRpRp|
        //   8 |       | RpRpRp|
        let mut ds = Downstairs::repair_test_one_repair();

        let repair_ids1 = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        let flush_id = ds.submit_flush(None, None, None);

        let repair_ids2 = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 9);

        assert_eq!(repair_ids1.close_id, JobId(1000));
        assert!(ds.get_deps(repair_ids1.close_id).is_empty());
        assert_eq!(flush_id, JobId(1004));
        assert_eq!(ds.get_deps(flush_id), &[JobId(1003)]);
        assert_eq!(repair_ids2.close_id, JobId(1005));
        assert_eq!(ds.get_deps(repair_ids2.close_id), &[JobId(1004)]);
    }

    #[test]
    fn test_live_repair_deps_repair_wspan_left() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   W W | W     |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 0
        //   3 |       | RpRpRp| 0
        //   4 |       | RpRpRp| 0
        let mut ds = Downstairs::repair_test_one_repair();

        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(1), BlockIndex(3)),
            false,
        );

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 5);

        assert!(ds.get_deps(write_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[write_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_wspan_right() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | RpRpRp|       |
        //
        let mut ds = Downstairs::repair_test_one_repair();

        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        assert_eq!(ds.ds_active.len(), 5);

        assert!(ds.get_deps(write_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[write_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_rspan_left() {
        // A repair will depend on a read spanning the extent

        // Read spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   R R | R     |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |       | RpRpRp|
        let mut ds = Downstairs::repair_test_one_repair();

        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(1),
            BlockIndex(3),
        ));

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 5);

        assert!(ds.get_deps(read_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[read_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_rspan_right() {
        // A repair will depend on a read spanning the extent
        // Read spans other extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     R | R R   |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | RpRpRp|       |
        let mut ds = Downstairs::repair_test_one_repair();

        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(2),
            BlockIndex(4),
        ));

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        assert_eq!(ds.ds_active.len(), 5);

        assert!(ds.get_deps(read_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[read_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_other() {
        // A write can be depended on by two different repairs, who won't
        // depend on each other.
        // This situation does not really exist, as a repair won't start
        // until the previous repair finishes.
        //       block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W     |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       | 1
        //   3 | RpRpRp|       | 2
        //   4 | RpRpRp|       | 3
        //   5 |       | RpRpRp| 0
        //   6 |       | RpRpRp| 5
        //   7 |       | RpRpRp| 6
        //   8 |       | RpRpRp| 6
        let mut ds = Downstairs::repair_test_one_repair();

        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(3)),
            false,
        );

        let repair_ids1 = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));
        let repair_ids2 = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 9);

        assert!(ds.get_deps(write_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids1.close_id), &[write_id]);
        assert_eq!(ds.get_deps(repair_ids2.close_id), &[write_id]);
    }

    #[test]
    fn test_live_repair_deps_super_spanner() {
        // Super spanner
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R R R | R R R |
        //   1 |       | RpRpRp|       | 0
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        let mut ds = Downstairs::repair_test_one_repair();

        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(0),
            BlockIndex(8),
        ));

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 5);

        assert!(ds.get_deps(read_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[read_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_wafter() {
        // Write after repair spans extent.
        // This write needs to include a future repair that
        // does not exist yet.
        // The write below will update both extent 0 (blocks 0,1,2)
        // and extent 1 (blocks 3,4,5).  Since the write depends on
        // the repair for extent 0, but also will change extent 1, we
        // have to hold this write until both extents are repaired.
        //
        // This means that the write will reserve a new set of job IDs, which
        // will be allocated but not enqueued.
        //
        // The IO starts like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | Rclose|       |
        //   1 | Rrep  |       | 0
        //   2 | Rnoop |       | 1
        //   3 | Ropen |       | 2
        //     |     W | W W   |
        //
        // However, once the write notices the repair in progress, it will
        // create space for the three future repair jobs that will make
        // the dep list look like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //  *0 | Rclose|       |
        //  *1 | Rrep  |       | 0
        //  *2 | Rnoop |       | 1
        //  *3 | Ropen |       | 2
        //   4 |       | Rclose|
        //   5 |       | Rrep  | 4
        //   6 |       | Rnoop | 5
        //   7 |       | Ropen | 6
        //  *8 |     W | W W   | 3,7
        //
        //  (jobs marked with * are enqueued)
        //
        // We also verify that the job IDs make sense for our repair id
        // reservation that happens when we need to insert a job like this.
        let mut ds = Downstairs::repair_test_one_repair();

        // Automatic reservation of job IDs for future extents only happens if
        // downstairs has `repair` set to Some sensible value, so we populate it
        // with what things would look like at the start of the repair. But, we
        // enqueue jobs ourselves.
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        // A write of blocks 2,3,4 which spans extent 0 and extent 1.
        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );

        assert_eq!(ds.ds_active.len(), 5);
        assert!(ds.get_deps(repair_ids.close_id).is_empty());

        // We know our first repair job will reserve 4 ids: 1000, 1001,
        // 1002, and 1003. The write to a spanning extent will go
        // and create 4 more job IDs, but will not enqueue them yet.
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        assert_eq!(write_id, JobId(1008));
        assert_eq!(ds.get_deps(write_id), &[repair_ids.reopen_id, JobId(1007)]);
    }

    #[test]
    fn test_live_repair_deps_repair_rafter() {
        // Read after spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 1
        //   3 |       | RpRpRp| 2
        //   4 |   R R | R     | 3
        let mut ds = Downstairs::repair_test_one_repair();

        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(1),
            BlockIndex(3),
        ));

        assert_eq!(ds.ds_active.len(), 5);

        assert_eq!(repair_ids.close_id, JobId(1000));
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        assert_eq!(read_id, JobId(1004));
        assert_eq!(ds.get_deps(read_id), &[repair_ids.reopen_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_overlappers() {
        // IOs that span both sides.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R     |       |
        //   1 |       |     W | W W W |
        //   2 |       | RpRpRp|       | 0,1
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |       | RpRpRp|       |
        //
        let mut ds = Downstairs::repair_test_one_repair();

        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(0),
            BlockIndex(3),
        ));

        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(5), BlockIndex(8)),
            false,
        );

        // The final repair command
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        assert_eq!(ds.ds_active.len(), 6);

        assert!(ds.get_deps(read_id).is_empty());
        assert!(ds.get_deps(write_id).is_empty());
        assert_eq!(ds.get_deps(repair_ids.close_id), &[read_id, write_id]);
    }

    #[test]
    fn test_live_repair_deps_repair_kitchen_sink() {
        // Repair simulator
        // In truth, you would never have more than one repair out at
        // the same time (the way it is now) but from a pure dependency point
        // of view, there is no reason you could not.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       |   W   |
        //   1 | RpRpRp|       |       |
        //   2 | RpRpRp|       |       | 1
        //   3 | RpRpRp|       |       | 2
        //   4 | RpRpRp|       |       | 3
        //   5 |       |   W   |       |
        //   6 |     R |       |       | 4
        //   7 |       | RpRpRp|       | 5
        //   8 |       | RpRpRp|       | 7
        //   9 |       | RpRpRp|       | 8
        //  10 |       | RpRpRp|       | 9
        //  11 |       |     W |       | 10
        //  12 |       |       | RpRpRp| 0
        //  13 |       |       | RpRpRp| 12
        //  14 |       |       | RpRpRp| 13
        //  15 |       |       | RpRpRp| 14
        let mut ds = Downstairs::repair_test_one_repair();

        let write_id1 =
            ds.submit_test_write_block(ExtentId(2), BlockOffset(1), false);

        // The first repair command
        let repair_ids1 = create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        let write_id2 =
            ds.submit_test_write_block(ExtentId(1), BlockOffset(1), false);

        let read_id = ds.submit_read_block(ExtentId(0), BlockOffset(2));

        // The second repair command
        let repair_ids2 = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));

        let write_id3 =
            ds.submit_test_write_block(ExtentId(1), BlockOffset(2), false);

        // The third repair command
        let repair_ids3 = create_and_enqueue_repair_ops(&mut ds, ExtentId(2));

        assert_eq!(ds.ds_active.len(), 16);

        assert_eq!(write_id1, JobId(1000));
        assert!(ds.get_deps(write_id1).is_empty());

        assert_eq!(repair_ids1.close_id, JobId(1001));
        assert!(ds.get_deps(repair_ids1.close_id).is_empty());

        assert_eq!(write_id2, JobId(1005));
        assert!(ds.get_deps(write_id2).is_empty());

        assert_eq!(read_id, JobId(1006));
        assert_eq!(ds.get_deps(read_id), &[repair_ids1.reopen_id]);

        assert_eq!(repair_ids2.close_id, JobId(1007));
        assert_eq!(ds.get_deps(repair_ids2.close_id), &[write_id2]);

        assert_eq!(write_id3, JobId(1011));
        assert_eq!(ds.get_deps(write_id3), &[repair_ids2.reopen_id]);

        assert_eq!(repair_ids3.close_id, JobId(1012));
        assert_eq!(ds.get_deps(repair_ids3.close_id), &[write_id1]);
    }

    #[test]
    fn test_live_repair_no_repair_yet() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, but we have not yet started the actual repair
        // work. IOs that arrive at this point in time should go ahead
        // on the good downstairs client, and still be skipped on the
        // LiveRepair client
        //
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   0 |   R   |       |
        //   0 | F F F | F F F | 0,1
        let mut ds = Downstairs::repair_test_one_repair();

        // A write of blocks 2,3,4 which spans the extent.
        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );

        let read_id = ds.submit_read_block(ExtentId(0), BlockOffset(1));

        let flush_id = ds.submit_flush(None, None, None);

        assert_eq!(ds.ds_active.len(), 3);

        assert!(ds.get_deps(write_id).is_empty());
        assert!(ds.get_deps(read_id).is_empty());
        assert_eq!(ds.get_deps(flush_id), &[write_id, read_id]);
        // Check that the IOs were skipped on downstairs 1.
        assert_eq!(ds.job_state(write_id, 1), IOState::Skipped);
        assert_eq!(ds.job_state(read_id, 1), IOState::Skipped);
        assert_eq!(ds.job_state(flush_id, 1), IOState::Skipped);
    }

    #[test]
    fn test_live_repair_repair_write_push() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  The write (that spans extents should have
        // created IDs for future repair work and then made itself
        // dependent on those repairs finishing.
        //
        // We start like this:
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //
        // But, end up with spots reserved for a future repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |     W | W W   | 3
        let mut ds = Downstairs::repair_test_one_repair();

        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(1000),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1000)),
                repair_job: PendingJob::new(JobId(1001)),
                noop_job: PendingJob::new(JobId(1002)),
                reopen_job: PendingJob::new(JobId(1003)),
            },
        });

        // A write of blocks 2,3,4 which spans extents.
        let write_id = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );
        assert_eq!(ds.ds_active.len(), 1);

        assert_eq!(write_id, JobId(1004));
        assert_eq!(ds.get_deps(write_id), &[JobId(1003)]);
    }

    #[test]
    fn test_live_repair_repair_read_push() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  The read (that spans extents should have
        // created IDs for future repair work and then made itself
        // dependent on those repairs finishing.
        //
        // We start like this:
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     R | R R   |
        //
        // But, end up with spots reserved for a future repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 1
        //   3 |       | RpRpRp| 2
        //  *4 |     R | R R   | 3
        let mut ds = Downstairs::repair_test_one_repair();

        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(1000),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1000)),
                repair_job: PendingJob::new(JobId(1001)),
                noop_job: PendingJob::new(JobId(1002)),
                reopen_job: PendingJob::new(JobId(1003)),
            },
        });

        // A read of blocks 2,3,4 which spans extents.
        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(2),
            BlockIndex(4),
        ));

        // Now enqueue the repair on extent 1, it should populate one of the
        // empty job slots.
        let repair_ids = create_and_enqueue_repair_ops(&mut ds, ExtentId(1));
        assert_eq!(ds.ds_active.len(), 5);

        // The close job is the first one in the job list
        assert_eq!(repair_ids.close_id, JobId(1000));
        // The repair job should have no dependencies.
        assert!(ds.get_deps(repair_ids.close_id).is_empty());
        // Our read should be ID 1004, and depends on the last repair job
        assert_eq!(read_id, JobId(1004));
        assert_eq!(ds.get_deps(read_id), &[repair_ids.reopen_id]);
    }

    #[test]
    fn test_live_repair_flush_is_flush() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  A flush should depend on any outstanding
        // repair operations, but won't generate future repair dependencies
        // like reads or writes do, as the flush will make use of the
        // extent_limit to allow it to slip in between repairs as well as
        // keep consistent what needs to be, and not flush what should not
        // be flushed.
        //
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | F F F | F F F |
        let mut ds = Downstairs::repair_test_one_repair();

        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(1000),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1000)),
                repair_job: PendingJob::new(JobId(1001)),
                noop_job: PendingJob::new(JobId(1002)),
                reopen_job: PendingJob::new(JobId(1003)),
            },
        });

        let flush_id = ds.submit_flush(None, None, None);

        assert_eq!(ds.ds_active.len(), 1);
        assert!(ds.get_deps(flush_id).is_empty());
    }

    #[test]
    fn test_live_repair_send_io_write_below() {
        // Verify that we will send a write during LiveRepair when
        // the IO is an extent that is already repaired.
        let mut ds = Downstairs::repair_test_one_repair();

        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(1),
            min_id: JobId(1004),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1004)),
                repair_job: PendingJob::new(JobId(1005)),
                noop_job: PendingJob::new(JobId(1006)),
                reopen_job: PendingJob::new(JobId(1007)),
            },
        });

        // A write of block 1 extents 0 (already repaired).
        let job_id =
            ds.submit_test_write_block(ExtentId(0), BlockOffset(1), false);

        for cid in ClientId::iter() {
            assert_eq!(ds.job_state(job_id, cid.get()), IOState::InProgress);
        }
    }

    fn submit_three_ios(ds: &mut Downstairs) {
        ds.submit_test_write_block(ExtentId(0), BlockOffset(0), false);
        ds.submit_read_block(ExtentId(0), BlockOffset(0));

        // WriteUnwritten
        ds.submit_test_write_block(ExtentId(0), BlockOffset(0), true);
    }

    #[test]
    fn test_repair_dep_cleanup_some() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered to reflect both the removal of skipped
        // jobs as well as removal of Done jobs that happened before the
        // downstairs went to LiveRepair, and also, won't remove jobs that
        // happened after the repair has started and should be allowed
        // through.  This test builds on the previous test, so some things
        // are not checked here.
        let mut ds = Downstairs::repair_test_all_active();

        // Channels we want to appear to be working
        // Now, send some IOs.
        submit_three_ios(&mut ds);

        // Fault the downstairs
        let to_repair = ClientId::new(1);
        move_to_live_repair(&mut ds, to_repair);

        let next_id = ds.peek_next_id().0;
        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(next_id),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(next_id)),
                repair_job: PendingJob::new(JobId(next_id + 1)),
                noop_job: PendingJob::new(JobId(next_id + 2)),
                reopen_job: PendingJob::new(JobId(next_id + 3)),
            },
        });

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&mut ds);

        // Same as the last repair assignment but active_extent is 1 now
        let next_id = ds.peek_next_id().0;
        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(1),
            min_id: JobId(next_id),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(next_id)),
                repair_job: PendingJob::new(JobId(next_id + 1)),
                noop_job: PendingJob::new(JobId(next_id + 2)),
                reopen_job: PendingJob::new(JobId(next_id + 3)),
            },
        });

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&mut ds);

        // Submit a flush as well.
        let flush_id = ds.submit_flush(None, None, None);

        // For the four latest jobs, they should be New as they are IOs that
        // are on an extent we "already repaired".
        for job_id in (1006..1010).map(JobId) {
            let job = ds.ds_active.get(&job_id).unwrap();
            assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
            assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
            assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);
        }

        // Walk the four final jobs, verify that the dependencies will be
        // updated for our downstairs under repair, but will still include
        // the jobs that came after the repair.
        let job = ds.ds_active.get(&JobId(1006)).unwrap();
        let current_deps = job.work.deps();

        assert_eq!(current_deps, &[JobId(1005)]);
        assert_eq!(ds.get_pruned_deps(JobId(1006), ClientId::new(1)), []);

        let job = ds.ds_active.get(&JobId(1007)).unwrap();
        let current_deps = job.work.deps();

        assert_eq!(current_deps, &[JobId(1006)]);
        assert_eq!(
            ds.get_pruned_deps(JobId(1007), ClientId::new(1)),
            [JobId(1006)]
        );

        let job = ds.ds_active.get(&JobId(1008)).unwrap();
        let current_deps = job.work.deps();

        assert_eq!(current_deps, &[JobId(1007)]);
        assert_eq!(
            ds.get_pruned_deps(JobId(1008), ClientId::new(1)),
            [JobId(1007)]
        );

        // For the flush, verify that the extent limit is cleared on clients
        // that are not under repair, and that it is present for clients
        // that are being repaired.
        let job = ds.ds_active.get(&flush_id).unwrap();
        let ioop = ds.clients[ClientId::new(0)].prune_deps(
            flush_id,
            job.work.clone(),
            ds.repair.as_ref().map(|r| r.min_id),
        );
        match ioop {
            IOop::Flush {
                dependencies,
                extent_limit,
                ..
            } => {
                assert_eq!(dependencies, &[JobId(1008)]);
                assert!(extent_limit.is_none());
            }
            _ => {
                panic!("Wrong job type");
            }
        }

        let ioop = ds.clients[ClientId::new(1)].prune_deps(
            flush_id,
            job.work.clone(),
            ds.repair.as_ref().map(|r| r.min_id),
        );
        match ioop {
            IOop::Flush {
                dependencies,
                extent_limit,
                ..
            } => {
                assert_eq!(dependencies, &[JobId(1008)]);
                assert!(extent_limit.is_some());
            }
            _ => {
                panic!("Wrong job type");
            }
        }
    }

    #[test]
    fn test_repair_dep_cleanup_repair() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered.
        // We want to be sure that a repair job that may have ended up
        // with work IDs below the lowest skipped job does not get skipped
        // as we require that job to be processed. To assist in
        // understanding what is going on, we have a table here to show
        // the IOs and where they are in each extent.

        //       block   block
        // op# | 0 1 2 | 3 4 5 |
        // ----|-------|-------|
        //   0 |   W W | W     |
        //   1 |     W | W W   |
        //   2 |       | W W W |
        //                       Space for future repair
        //   4 | RpRpRp|       |
        //                       Space for future repair
        //   7 |       | W W W |
        //   8 |   W   |       |
        //                       Space for future repair
        //  13 |   W W | W     |
        let mut ds = Downstairs::repair_test_all_active();

        // Now, put three IOs on the queue
        ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(1), BlockIndex(3)),
            false,
        );
        ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );
        ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(3), BlockIndex(5)),
            false,
        );

        // Fault the downstairs
        let to_repair = ClientId::new(1);
        move_to_live_repair(&mut ds, to_repair);

        let next_id = ds.peek_next_id().0;

        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(next_id),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(next_id)),
                repair_job: PendingJob::new(JobId(next_id + 1)),
                noop_job: PendingJob::new(JobId(next_id + 2)),
                reopen_job: PendingJob::new(JobId(next_id + 3)),
            },
        });

        // Put a repair job on the queue.
        create_and_enqueue_repair_ops(&mut ds, ExtentId(0));

        // Create a write on extent 1 (not yet repaired)
        ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(3), BlockIndex(5)),
            false,
        );

        // Now, submit another write, this one will be on the extent
        // that is under repair.
        ds.submit_test_write_block(ExtentId(0), BlockOffset(1), false);

        // Submit a final write.  This has a shadow that covers every
        // IO submitted so far, and will also require creation of
        // space for future repair work.
        ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(1), BlockIndex(3)),
            false,
        );

        // Previous tests have verified what happens before job 1007
        // Starting with job 7, this is special because it will have
        // different dependencies on the Active downstairs vs what the
        // dependencies will be on the LiveRepair downstairs.  On active,
        // it should require jobs 1 and 2. With the LiveRepair downstairs,
        // it will not depend on anything. This is okay, because the job
        // itself is Skipped there, so we won't actually send it.

        let job = ds.ds_active.get(&JobId(1007)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        let current_deps = job.work.deps();
        assert_eq!(current_deps, &[JobId(1002)]);

        // Verify that the Skipped job is Skipped
        assert_eq!(ds.job_state(JobId(1007), 1), IOState::Skipped);

        // This second write after starting a repair should require job 6 (i.e.
        // the final job of the repair) on both the Active and LiveRepair
        // downstairs, since it masks job 0
        let job = ds.ds_active.get(&JobId(1008)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        let current_deps = job.work.deps();
        assert_eq!(current_deps, &[JobId(1006)]);

        // LiveRepair downstairs won't see past the repair.
        assert_eq!(
            ds.get_pruned_deps(JobId(1008), ClientId::new(1)),
            [JobId(1006)]
        );

        // This final job depends on everything on Active downstairs, but
        // a smaller subset for the LiveRepair downstairs
        let job = ds.ds_active.get(&JobId(1013)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(1)], IOState::InProgress);
        assert_eq!(job.state[ClientId::new(2)], IOState::InProgress);

        // The last write depends on
        // 1) the final operation on the repair of extent 0
        // 2) the write operation (8) on extent 0
        // 3) a new repair operation on extent 1
        let current_deps = job.work.deps();
        assert_eq!(current_deps, &[JobId(1006), JobId(1008), JobId(1012)]);

        assert_eq!(
            ds.get_pruned_deps(JobId(1013), ClientId::new(1)),
            [JobId(1006), JobId(1008), JobId(1012)]
        );
        assert_eq!(current_deps, &[JobId(1006), JobId(1008), JobId(1012)]);
    }

    #[test]
    fn test_repair_dep_cleanup_sk_repair() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered.
        // Simulating what happens when we start repair with just the close
        // and reopen, but not the repair and noop jobs.
        // Be sure that the dependency removal for 4 won't remove 2,3, but
        // will remove 0.
        // A write after the repair should have the issued repairs, but not
        // the repairs that are not yet present.
        //       block   block
        // op# | 0 1 2 | 3 4 5 |
        // ----|-------|-------|
        //   0 |   W   |       |
        //                       Live Repair starts here
        //   1 | Rclose|       |
        //   2 |       |       | Reserved for future repair
        //   3 |       |       | Reserved for future repair
        //   4 | Reopen|       |
        //   5 |   W   |       |
        let mut ds = Downstairs::repair_test_all_active();

        // Put the first write on the queue
        ds.submit_test_write_block(ExtentId(0), BlockOffset(1), false);

        // Fault the downstairs
        let to_repair = ClientId::new(1);
        to_live_repair_ready(&mut ds, to_repair);

        // Start the repair normally. This enqueues the close & reopen jobs, and
        // reserves Job IDs for the repair/noop
        ds.check_live_repair_start(&UpstairsState::Active);
        assert!(ds.live_repair_in_progress());

        // Submit a write.
        ds.submit_test_write_block(ExtentId(0), BlockOffset(1), false);

        let flushclose_jobid = 1001;
        let flushclose_job =
            ds.ds_active.get(&JobId(flushclose_jobid)).unwrap();
        assert!(matches!(
            &flushclose_job.work,
            IOop::ExtentFlushClose { .. }
        ));

        // ExtentFlushClose depens on the write at the top level
        assert_eq!(flushclose_job.work.deps(), &vec![JobId(1000)]);

        // After in_progressing the ExtentFlushClose, it will still depend on
        // the write for the active downstairs, but will not depend on anything
        // for the downstairs in LiveRepair.
        assert_eq!(
            ds.get_pruned_deps(JobId(flushclose_jobid), ClientId::new(0)),
            [JobId(1000)]
        );
        assert_eq!(
            ds.get_pruned_deps(JobId(flushclose_jobid), ClientId::new(1)),
            []
        );
        assert_eq!(
            ds.get_pruned_deps(JobId(flushclose_jobid), ClientId::new(2)),
            [JobId(1000)]
        );

        // The second write should depend on the reopen on all downstairs
        let write_jobid = JobId(1005);
        assert_eq!(
            ds.job_states(write_jobid),
            [
                IOState::InProgress,
                IOState::InProgress,
                IOState::InProgress,
            ]
        );

        assert_eq!(
            ds.get_pruned_deps(write_jobid, ClientId::new(0)),
            [JobId(1004)]
        );
        assert_eq!(
            ds.get_pruned_deps(write_jobid, ClientId::new(1)),
            [JobId(1004)]
        );
        assert_eq!(
            ds.get_pruned_deps(write_jobid, ClientId::new(2)),
            [JobId(1004)]
        );
    }

    #[test]
    fn test_live_repair_span_write_write() {
        // We have a downstairs that is in LiveRepair, and we have indicated
        // that this extent is under repair.  The write (that spans extents
        // should have created IDs for future repair work and then made itself
        // dependent on those repairs finishing. The write that comes next
        // lands on the extent that has future repairs reserved, and that
        // 2nd write needs to also depend on those repair jobs.
        //
        // We start with extent 0 under repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //
        // First, a write spanning extents 0 and 1
        //     | block | block |
        //     | 0 1 2 | 3 4 5 |
        // ----|-------|-------|-----
        //     |     W | W W   |
        //
        // Then, a write on the "spanned to" extent 1, but not overlapping
        // with the first write.
        //     | block | block |
        //     | 0 1 2 | 3 4 5 |
        // ----|-------|-------|-----
        //     |       |     W |
        //
        // The first write will create and depend on jobs reserved for a
        // future repair.  The second job should discover the repair jobs and
        // also have them as dependencies.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |     W | W W   | 3
        //   6 |       |     W | 3
        //

        // Downstairs 1 is in LiveRepair already from repair_test_default()
        let mut ds = Downstairs::repair_test_one_repair();

        // Make sure we're in the repair on extent 0
        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(1000),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1000)),
                repair_job: PendingJob::new(JobId(1001)),
                noop_job: PendingJob::new(JobId(1002)),
                reopen_job: PendingJob::new(JobId(1003)),
            },
        });

        // A write of blocks 2,3,4 which spans extents 0-1.
        let write_id1 = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(4)),
            false,
        );

        // A write of block 5 which is on extent 1, but does not
        // overlap with the previous write
        let write_id2 =
            ds.submit_test_write_block(ExtentId(1), BlockOffset(2), false);

        assert_eq!(ds.ds_active.len(), 2);

        // The first job, should have the dependences for the new repair work
        assert_eq!(write_id1, JobId(1004));
        assert_eq!(ds.get_deps(write_id1), &[JobId(1003)]);
        assert_eq!(ds.job_state(write_id1, 0), IOState::InProgress);
        assert_eq!(ds.job_state(write_id1, 1), IOState::InProgress);
        assert_eq!(ds.job_state(write_id1, 2), IOState::InProgress);

        // The 2nd job should also have the dependences for the new repair work
        assert_eq!(ds.get_deps(write_id2), &[JobId(1003)]);
        assert_eq!(ds.job_state(write_id2, 0), IOState::InProgress);
        assert_eq!(ds.job_state(write_id2, 1), IOState::InProgress);
        assert_eq!(ds.job_state(write_id2, 2), IOState::InProgress);
    }

    #[test]
    fn test_spicy_live_repair() {
        // We have a downstairs that is in LiveRepair, and send a write which is
        // above the extent above repair.  This is fine; it's skipped on the
        // being-repaired downstairs.
        //
        // Then, send a write which reserves repair job ids for extent 1 (blocks
        // 3-5).  This is the same as the test above.
        //
        // Finally, send a read which spans extents 1-2 (blocks 5-6).  This must
        // depend on the previously-inserted repair jobs.  However, that's not
        // enough: because the initial write was skipped in the under-repair
        // downstairs, the read won't depend on that write on that downstairs!
        // Instead, we have to add *new* repair dependencies for extent 2;
        // without these dependencies, it would be possible to read old data
        // from block 6 on the under-repair downstairs.
        //
        //     | Under |       |       |
        //     | Repair|       |       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-------
        //   0 |       | W W W | W     | none; skipped in under-repair ds
        //   1 |       | RpRpRp|       | 0
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |     W | W     |       | 4
        //   6 |       |       | RpRpRp|
        //   7 |       |       | RpRpRp|
        //   8 |       |       | RpRpRp|
        //   9 |       |       | RpRpRp|
        //   10|       |     R | R     | 4,9
        //
        // More broadly: if a job is _not_ going to be skipped (e.g. the final
        // read job in this example), then it needs reserved repair job IDs for
        // every extent that it touches.

        // Downstairs 1 is in LiveRepair already from repair_test_default()
        let mut ds = Downstairs::repair_test_one_repair();

        // Make sure we're in the repair on extent 0
        ds.repair = Some(LiveRepairData {
            id: Uuid::new_v4(),
            extent_count: 3,
            active_extent: ExtentId(0),
            min_id: JobId(1000),
            repair_job_ids: BTreeMap::new(),
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
            aborting_repair: false,
            state: LiveRepairState::Closing {
                close_job: PendingJob::new(JobId(1000)),
                repair_job: PendingJob::new(JobId(1001)),
                noop_job: PendingJob::new(JobId(1002)),
                reopen_job: PendingJob::new(JobId(1003)),
            },
        });

        // A write of blocks 3,4,5,6 which spans extents 1-2.
        let write_id1 = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(3), BlockIndex(6)),
            false,
        );

        // A write of block 2-3, which overlaps the previous write and should
        // also trigger a repair.
        let write_id2 = ds.submit_test_write(
            ImpactedBlocks::new(BlockIndex(2), BlockIndex(3)),
            false,
        );

        // A read of block 5-7, which overlaps the previous repair and should
        // also force waiting on a new repair.
        let read_id = ds.submit_dummy_read(ImpactedBlocks::new(
            BlockIndex(5),
            BlockIndex(7),
        ));

        assert_eq!(ds.ds_active.len(), 3);

        // The first job should have no dependencies
        assert_eq!(write_id1, JobId(1000));
        assert!(ds.get_deps(write_id1).is_empty());
        assert_eq!(ds.job_state(write_id1, 0), IOState::InProgress);
        assert_eq!(ds.job_state(write_id1, 1), IOState::Skipped);
        assert_eq!(ds.job_state(write_id1, 2), IOState::InProgress);

        assert_eq!(write_id2, JobId(1005));
        assert_eq!(ds.get_deps(write_id2), &[JobId(1004)]);
        assert_eq!(ds.job_state(write_id2, 0), IOState::InProgress);
        assert_eq!(ds.job_state(write_id2, 1), IOState::InProgress);
        assert_eq!(ds.job_state(write_id2, 2), IOState::InProgress);

        assert_eq!(read_id, JobId(1010));
        assert_eq!(ds.get_deps(read_id), &[JobId(1004), JobId(1009)]);
        assert_eq!(ds.job_state(read_id, 0), IOState::InProgress);
        assert_eq!(ds.job_state(read_id, 1), IOState::InProgress);
        assert_eq!(ds.job_state(read_id, 2), IOState::InProgress);
    }
}
