// Copyright 2023 Oxide Computer Company
//! Data structures specific to Crucible's `struct Upstairs`
use crate::{
    cdt,
    client::{ClientAction, ClientRunResult, ClientStopReason},
    control::ControlRequest,
    deadline_secs,
    deferred::{
        DeferredBlockReq, DeferredQueue, DeferredWrite, EncryptedWrite,
    },
    downstairs::{Downstairs, DownstairsAction},
    extent_from_offset,
    stats::UpStatOuter,
    Block, BlockOp, BlockReq, BlockRes, Buffer, Bytes, ClientId, ClientMap,
    CrucibleOpts, DsState, EncryptionContext, GtoS, Guest, Message,
    RegionDefinition, RegionDefinitionStatus, SnapshotDetails, WQCounts,
};
use crucible_common::CrucibleError;

use std::{
    ops::DerefMut,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::future::{pending, ready, Either};
use ringbuffer::RingBuffer;
use slog::{debug, error, info, o, warn, Logger};
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep_until, Instant},
};
use uuid::Uuid;

/// How often to log stats for DTrace
const STAT_INTERVAL_SECS: f32 = 1.0;

/// High-level upstairs state, from the perspective of the guest
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
    GoActive(BlockRes),

    /// The upstairs is fully online and accepting guest IO
    Active,

    /// The upstairs is deactivating
    ///
    /// In-flight IO continues, but no new IO is allowed.  When all IO has been
    /// completed (including the final flush), the downstairs task should stop;
    /// when all three Downstairs have stopped, the upstairs should enter
    /// `UpstairsState::Initializing` and reply on this channel.
    Deactivating(BlockRes),
}

/// Crucible upstairs state
///
/// This `struct` has exclusive ownership over (almost) everything that's needed
/// to run the Crucible upstairs, and a shared handle to the [`Guest`] data
/// structure (which is our main source of operations).
///
/// In normal operation, the `Upstairs` expects to run a simple loop forever in
/// an async task:
/// ```ignore
/// loop {
///     let action = self.select().await;
///     self.apply(action).await
/// }
/// ```
/// (this is implemented as [`Upstairs::run`])
///
/// Under the hood, [`Upstairs::select`] selects from **many** possible async
/// events:
/// - Messages from downstairs clients
/// - [`BlockReq`] from the guest
/// - Various timeouts
///   - Client timeout
///   - Client ping intervals
///   - Live-repair checks
///   - IOPS leaking
///   - Automatic flushes
///   - DTrace logging of stats
/// - Control requests from the controller server
///
/// It returns a strongly-typed [`UpstairsAction`], which is then handled by
/// [`Upstairs::apply`].
///
/// This means that only one thing is happening at a time, and it's easy to
/// drive the `Upstairs` in tests by sending it a synthetic stream of events.
/// In addition, the `Upstairs` always has exclusive ownership of its own data,
/// so there are no locks to worry about.
///
/// The downside to this architecture is multi-step state machine have to be
/// written out manually, rather than writing an `async` function and having the
/// compiler to the `async`-to-state-machine transform. A notable example is
/// live-repair, which keeps track of its state in `downstairs::LiveRepairState`
/// and manually steps through that state machine in response to incoming
/// events.
///
/// (This downside is necessary because we often have multiple state machines
/// running simultaneously, so can't put them into independent async tasks while
/// still maintaining a single point of ownership for all `Upstairs` data.  The
/// previous architecture used multiple async tasks and a mutex around upstairs
/// data, but that makes it tricky to reason about edge cases and invariants)
///
/// `Upstairs::apply` does two things on every event: specific handling of that
/// event, and what we vaguely describe as "invariant maintenance".  The latter
/// is a bunch of actions which are (1) cheap to check and (2) put the
/// `Upstairs` into a known state afterwards.
///
/// For example, we _always_ do things like
/// - Send all pending IO (limited by the downstairs' `MAX_ACTIVE_COUNT`)
/// - Ack all ackable jobs to the guest
/// - Step through the live-repair state machine (if it's running)
/// - Check for client-side deactivation (if it's pending)
/// - Set backpressure time in the guest
///
/// Keeping the `Upstairs` "clean" through this invariant maintenance makes it
/// easier to think about its state, because it's guaranteed to be clean when we
/// next call `Upstairs::apply`.
pub(crate) struct Upstairs {
    /// Current state
    pub(crate) state: UpstairsState,

    /// Downstairs jobs and per-client state
    pub(crate) downstairs: Downstairs,

    /// The guest struct keeps track of jobs accepted from the Guest
    ///
    /// A single job submitted can produce multiple downstairs requests.
    pub(crate) guest: Arc<Guest>,

    /// Region definition
    ///
    /// This is (optionally) provided on startup, and checked for consistency
    /// between all three Downstairs.
    ///
    /// The region definition allows us to translate an LBA to an extent and
    /// block offset.
    ddef: RegionDefinitionStatus,

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
    ///
    /// Shared with the metrics producer, so this `struct` wraps a
    /// `std::sync::Mutex`
    pub(crate) stats: UpStatOuter,

    /// Fixed configuration
    pub(crate) cfg: Arc<UpstairsConfig>,

    /// Logger used by the upstairs
    pub(crate) log: Logger,

    /// Next time to check for repairs
    repair_check_interval: Option<Instant>,

    /// Next time to leak IOP / bandwidth tokens from the Guest
    leak_deadline: Instant,

    /// Next time to trigger an automatic flush
    flush_deadline: Instant,

    /// Next time to trigger a stats update
    stat_deadline: Instant,

    /// Interval between automatic flushes
    flush_timeout_secs: f32,

    /// Receiver queue for control requests
    control_rx: mpsc::Receiver<ControlRequest>,

    /// Sender handle for control requests
    ///
    /// This is public so that others can clone it to get a controller handle
    pub(crate) control_tx: mpsc::Sender<ControlRequest>,

    /// Stream of post-processed `BlockOp` futures
    deferred_reqs: DeferredQueue<Option<DeferredBlockReq>>,
}

/// Action to be taken which modifies the [`Upstairs`] state
#[derive(Debug)]
pub(crate) enum UpstairsAction {
    Downstairs(DownstairsAction),
    Guest(BlockReq),

    /// A deferred block request has completed
    DeferredBlockReq(DeferredBlockReq),

    LeakCheck,
    FlushCheck,
    StatUpdate,
    RepairCheck,
    Control(ControlRequest),

    /// We received an event of some kind, but it requires no follow-up work
    NoOp,
}

#[derive(Debug)]
pub(crate) struct UpstairsConfig {
    /// Upstairs UUID
    pub upstairs_id: Uuid,

    /// Unique session ID
    pub session_id: Uuid,

    /// Generation number
    ///
    /// This is _mostly_ invariant, but we're allowing for interior mutability
    /// so that `BlockOp::GoActiveWithGen` can work.  We may remove that
    /// operation in the future, since it's only used in unit tests.
    pub generation: AtomicU64,

    pub read_only: bool,

    /// Encryption context, if present
    ///
    /// This is `Some(..)` if a key is provided in the `CrucibleOpts`
    pub encryption_context: Option<EncryptionContext>,

    /// Does this Upstairs throw random errors?
    pub lossy: bool,
}

impl UpstairsConfig {
    pub(crate) fn encrypted(&self) -> bool {
        self.encryption_context.is_some()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

impl Upstairs {
    pub(crate) fn new(
        opt: &CrucibleOpts,
        gen: u64,
        expected_region_def: Option<RegionDefinition>,
        guest: Arc<Guest>,
        tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    ) -> Self {
        /*
         * XXX Make sure we have three and only three downstairs
         */
        #[cfg(not(test))]
        assert_eq!(opt.target.len(), 3, "bad targets {:?}", opt.target);

        // Build the target map, which is either empty (during some tests) or
        // fully populated with all three targets.
        let mut ds_target = ClientMap::new();
        for (i, v) in opt.target.iter().enumerate() {
            ds_target.insert(ClientId::new(i as u8), *v);
        }

        // Create an encryption context if a key is supplied.
        let encryption_context = opt.key_bytes().map(|key| {
            EncryptionContext::new(
                key,
                // XXX: Figure out what to do if no expected region definition
                // was supplied. It would be good to do BlockOp::QueryBlockSize
                // here, but this creates a deadlock. Upstairs::new runs before
                // up_ds_listen in up_main, and up_ds_listen needs to run to
                // answer BlockOp::QueryBlockSize. (Note that the downstairs
                // have not reported in yet, so if no expected definition was
                // supplied no downstairs information is available.)
                expected_region_def
                    .map(|rd| rd.block_size() as usize)
                    .unwrap_or(512),
            )
        });

        let uuid = opt.id;
        let stats = UpStatOuter::new(uuid);

        let rd_status = match expected_region_def {
            None => RegionDefinitionStatus::WaitingForDownstairs,
            Some(d) => RegionDefinitionStatus::ExpectingFromDownstairs(d),
        };

        let session_id = Uuid::new_v4();
        let log = guest.log.new(o!("session_id" => session_id.to_string()));
        info!(log, "Crucible {} has session id: {}", uuid, session_id);
        info!(log, "Upstairs opts: {}", opt);

        let cfg = Arc::new(UpstairsConfig {
            encryption_context,
            upstairs_id: uuid,
            session_id,
            generation: AtomicU64::new(gen),
            read_only: opt.read_only,
            lossy: opt.lossy,
        });

        info!(log, "Crucible stats registered with UUID: {}", uuid);
        let downstairs = Downstairs::new(
            cfg.clone(),
            ds_target,
            tls_context,
            log.new(o!("" => "downstairs")),
        );
        let flush_timeout_secs = opt.flush_timeout.unwrap_or(0.5);
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(500);

        Upstairs {
            state: UpstairsState::Initializing,
            cfg,
            repair_check_interval: None,
            leak_deadline: deadline_secs(1.0),
            flush_deadline: deadline_secs(flush_timeout_secs),
            stat_deadline: deadline_secs(STAT_INTERVAL_SECS),
            flush_timeout_secs,
            guest,
            ddef: rd_status,
            need_flush: false,
            stats,
            log,
            downstairs,
            control_rx,
            control_tx,
            deferred_reqs: DeferredQueue::new(),
        }
    }

    /// Build an Upstairs for simple tests
    #[cfg(test)]
    pub fn test_default(ddef: Option<RegionDefinition>) -> Self {
        let opts = CrucibleOpts {
            id: Uuid::new_v4(),
            target: vec![],
            lossy: false,
            flush_timeout: None,
            key: None,
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only: false,
        };

        let log = crucible_common::build_logger();

        Self::new(
            &opts,
            0,
            ddef,
            Arc::new(Guest::new(Some(log.clone()))),
            None,
        )
    }

    /// Runs the upstairs (forever)
    pub(crate) async fn run(&mut self) {
        loop {
            let action = self.select().await;
            self.apply(action).await
        }
    }

    /// Select an event from possible actions
    async fn select(&mut self) -> UpstairsAction {
        tokio::select! {
            d = self.downstairs.select() => {
                UpstairsAction::Downstairs(d)
            }
            d = self.guest.recv() => {
                UpstairsAction::Guest(d)
            }
            _ = self.repair_check_interval
                .map(|r| Either::Left(sleep_until(r)))
                .unwrap_or(Either::Right(pending()))
            => {
                UpstairsAction::RepairCheck
            }
            d = self.deferred_reqs.next(), if !self.deferred_reqs.is_empty()
            => {
                match d {
                    // Normal operation: the deferred task gave us back a
                    // DeferredBlockReq, which we need to handle.
                    Some(Some(d)) => UpstairsAction::DeferredBlockReq(d),

                    // The innermost Option is None if the deferred task handled
                    // the request on its own (and replied to the `BlockReq`
                    // already). This happens if encryption fails, which would
                    // be odd, but possible?
                    Some(None) => UpstairsAction::NoOp,

                    // The outer Option is None if the FuturesOrdered is empty
                    None => {
                        // Calling `deferred_reqs.next()` on an empty queue must
                        // set the flag marking the deferred futures list as
                        // empty; assert that here as a sanity check.
                        assert!(self.deferred_reqs.is_empty());
                        UpstairsAction::NoOp
                    }
                }
            }
            _ = sleep_until(self.leak_deadline) => {
                UpstairsAction::LeakCheck
            }
            _ = sleep_until(self.flush_deadline) => {
                UpstairsAction::FlushCheck
            }
            _ = sleep_until(self.stat_deadline) => {
                UpstairsAction::StatUpdate
            }
            c = self.control_rx.recv() => {
                // We can always unwrap this, because we hold a handle to the tx
                // side as well (so the channel will never close)
                UpstairsAction::Control(c.unwrap())
            }
        }
    }

    /// Apply an action returned from [`Upstairs::select`]
    pub(crate) async fn apply(&mut self, action: UpstairsAction) {
        match action {
            UpstairsAction::Downstairs(d) => {
                self.apply_downstairs_action(d).await
            }
            UpstairsAction::Guest(b) => {
                self.defer_guest_request(b).await;
            }
            UpstairsAction::DeferredBlockReq(req) => {
                self.apply_guest_request(req).await;
            }
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
                self.flush_deadline = deadline_secs(self.flush_timeout_secs);
            }
            UpstairsAction::StatUpdate => {
                self.on_stat_update().await;
                self.stat_deadline = deadline_secs(STAT_INTERVAL_SECS);
            }
            UpstairsAction::RepairCheck => {
                self.on_repair_check().await;
            }
            UpstairsAction::Control(c) => {
                self.on_control_req(c).await;
            }
            UpstairsAction::NoOp => (),
        }

        // Check whether we need to mark a Downstairs as faulted because too
        // many jobs have piled up.
        self.gone_too_long();

        // Check to see whether live-repair can continue
        //
        // This must be called before acking jobs, because it looks in
        // `Downstairs::ackable_jobs` to see which jobs are done.
        if let Some(job_id) = self.downstairs.check_live_repair() {
            let mut gw = self.guest.guest_work.lock().await;
            self.downstairs
                .continue_live_repair(job_id, &mut gw, &self.state);
        }

        // Send jobs downstairs as they become available.  This must be called
        // after `continue_live_repair`, which may enqueue jobs.
        for i in ClientId::iter() {
            if self.downstairs.clients[i].should_do_more_work() {
                self.downstairs.io_send(i).await;
            }
        }

        // Handle any jobs that have become ready for acks
        if self.downstairs.has_ackable_jobs() {
            let mut gw = self.guest.guest_work.lock().await;
            self.downstairs.ack_jobs(&mut gw, &self.stats).await;
        }

        // Check for client-side deactivation
        if matches!(&self.state, UpstairsState::Deactivating(..)) {
            info!(self.log, "checking for deactivation");
            for i in ClientId::iter() {
                // Clients become Deactivated, then New (when the IO task
                // completes and the client is restarted).  We don't try to
                // deactivate them _again_ in such cases.
                if matches!(
                    self.downstairs.clients[i].state(),
                    DsState::Deactivated | DsState::New
                ) {
                    debug!(self.log, "already deactivated {i}");
                } else if self.downstairs.try_deactivate(i, &self.state) {
                    info!(self.log, "deactivated client {i}");
                } else {
                    info!(self.log, "not ready to deactivate client {i}");
                }
            }
            if self
                .downstairs
                .clients
                .iter()
                .all(|c| c.ready_to_deactivate())
            {
                info!(self.log, "All DS in the proper state! -> INIT");
                let prev = std::mem::replace(
                    &mut self.state,
                    UpstairsState::Initializing,
                );
                let UpstairsState::Deactivating(res) = prev else {
                    panic!("invalid upstairs state {prev:?}"); // checked above
                };
                res.send_ok();
            }
        }

        // For now, check backpressure after every event.  We may want to make
        // this more nuanced in the future.
        self.set_backpressure();
    }

    /// Helper function to await all deferred block requests
    ///
    /// This is only useful in tests because it **only** processes deferred
    /// block requests (doing no other Upstairs work).  In production, there
    /// could be other events that need handling simultaneously, so we do not
    /// want to stall the Upstairs.
    #[cfg(test)]
    async fn await_deferred_reqs(&mut self) {
        while let Some(req) = self.deferred_reqs.next().await {
            let req = req.unwrap(); // the deferred request should not fail
            self.apply(UpstairsAction::DeferredBlockReq(req)).await;
        }
        assert!(self.deferred_reqs.is_empty());
    }

    /// Check outstanding IOops for each downstairs.
    ///
    /// If the number is too high, then mark that downstairs as failed, scrub
    /// any outstanding jobs, and restart the client IO task.
    fn gone_too_long(&mut self) {
        // If we are not active, then just exit.
        if !matches!(self.state, UpstairsState::Active) {
            return;
        }

        for cid in ClientId::iter() {
            // Only downstairs in these states are checked.
            match self.downstairs.clients[cid].state() {
                DsState::Active
                | DsState::LiveRepair
                | DsState::Offline
                | DsState::Replay => {
                    self.downstairs.check_gone_too_long(cid, &self.state);
                }
                _ => {}
            }
        }
    }

    /// Fires the `up-status` DTrace probe
    async fn on_stat_update(&self) {
        let up_count = self.guest.guest_work.lock().await.active.len() as u32;
        let ds_count = self.downstairs.active_count() as u32;
        let ds_state = self.downstairs.collect_stats(|c| c.state());

        let ds_io_count = self.downstairs.io_state_count();
        let ds_reconciled = self.downstairs.reconcile_repaired();
        let ds_reconcile_needed = self.downstairs.reconcile_repair_needed();
        let ds_live_repair_completed = self
            .downstairs
            .collect_stats(|c| c.stats.live_repair_completed);
        let ds_live_repair_aborted = self
            .downstairs
            .collect_stats(|c| c.stats.live_repair_aborted);
        let ds_connected = self.downstairs.collect_stats(|c| c.stats.connected);
        let ds_replaced = self.downstairs.collect_stats(|c| c.stats.replaced);
        let ds_flow_control =
            self.downstairs.collect_stats(|c| c.stats.flow_control);
        let ds_extents_repaired =
            self.downstairs.collect_stats(|c| c.stats.extents_repaired);
        let ds_extents_confirmed =
            self.downstairs.collect_stats(|c| c.stats.extents_confirmed);
        let ds_ro_lr_skipped =
            self.downstairs.collect_stats(|c| c.stats.ro_lr_skipped);

        let up_backpressure = self
            .guest
            .backpressure_us
            .load(std::sync::atomic::Ordering::Acquire);
        let write_bytes_out = self.downstairs.write_bytes_outstanding();

        cdt::up__status!(|| {
            let arg = Arg {
                up_count,
                up_backpressure,
                write_bytes_out,
                ds_count,
                ds_state,
                ds_io_count,
                ds_reconciled,
                ds_reconcile_needed,
                ds_live_repair_completed,
                ds_live_repair_aborted,
                ds_connected,
                ds_replaced,
                ds_flow_control,
                ds_extents_repaired,
                ds_extents_confirmed,
                ds_ro_lr_skipped,
            };
            ("stats", arg)
        });
    }

    /// Handles a request from the (optional) control server
    async fn on_control_req(&self, c: ControlRequest) {
        match c {
            ControlRequest::UpstairsStats(tx) => {
                let ds_state = self.downstairs.collect_stats(|c| c.state());
                let up_jobs = self.guest.guest_work.lock().await.active.len();
                let ds_jobs = self.downstairs.active_count();
                let repair_done = self.downstairs.reconcile_repaired();
                let repair_needed = self.downstairs.reconcile_repair_needed();
                let extents_repaired =
                    self.downstairs.collect_stats(|c| c.stats.extents_repaired);
                let extents_confirmed = self
                    .downstairs
                    .collect_stats(|c| c.stats.extents_confirmed);
                let extent_limit = self
                    .downstairs
                    .collect_stats(|c| matches!(c.state(), DsState::LiveRepair))
                    .map(|b| {
                        if b {
                            self.downstairs
                                .active_repair_extent()
                                .map(|v| v as usize)
                        } else {
                            None
                        }
                    });
                let live_repair_completed = self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_completed);
                let live_repair_aborted = self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_aborted);

                // Translate from rich UpstairsState to simplified UpState
                // TODO: remove this distinction?
                let state = match &self.state {
                    UpstairsState::Initializing
                    | UpstairsState::GoActive(..) => {
                        crate::UpState::Initializing
                    }
                    UpstairsState::Active => crate::UpState::Active,
                    UpstairsState::Deactivating(..) => {
                        crate::UpState::Deactivating
                    }
                };

                let r = tx.send(crate::control::UpstairsStats {
                    state,
                    ds_state: ds_state.to_vec(),
                    up_jobs,
                    ds_jobs,
                    repair_done,
                    repair_needed,
                    extents_repaired: extents_repaired.to_vec(),
                    extents_confirmed: extents_confirmed.to_vec(),
                    extent_limit: extent_limit.to_vec(),
                    live_repair_completed: live_repair_completed.to_vec(),
                    live_repair_aborted: live_repair_aborted.to_vec(),
                });
                if r.is_err() {
                    warn!(self.log, "control message reply failed");
                }
            }
            ControlRequest::DownstairsWorkQueue(tx) => {
                let out = self.downstairs.get_work_summary();
                let r = tx.send(out);
                if r.is_err() {
                    warn!(self.log, "control message reply failed");
                }
            }
        }
    }

    /// Checks if a repair is possible. If so, checks if any Downstairs is in
    /// the [DsState::LiveRepairReady] state, indicating it needs to be
    /// repaired. If a Downstairs needs to be repaired, try to start repairing
    /// it. When starting the repair fails, this function will schedule a task
    /// to retry the repair by setting [Self::repair_check_interval].
    ///
    /// If this Upstairs is [UpstairsConfig::read_only], this function will move
    /// any Downstairs from [DsState::LiveRepairReady] back to [DsState::Active]
    /// without actually performing any repair.
    pub(crate) async fn on_repair_check(&mut self) {
        info!(self.log, "Checking if live repair is needed");
        if !matches!(self.state, UpstairsState::Active) {
            info!(self.log, "inactive, no live repair needed");
            self.repair_check_interval = None;
            return;
        }

        if self.cfg.read_only {
            info!(self.log, "read-only, no live repair needed");
            // Repair can't happen on a read-only downstairs, so short circuit
            // here. There's no state drift to repair anyway, this read-only
            // Upstairs wouldn't have caused any modifications.
            for c in self.downstairs.clients.iter_mut() {
                c.skip_live_repair(&self.state);
            }
            self.repair_check_interval = None;
            return;
        }

        // Verify that all downstairs and the upstairs are in the proper state
        // before we begin a live repair.
        let repair_in_progress = self.downstairs.live_repair_in_progress();

        let any_in_repair_ready = self
            .downstairs
            .clients
            .iter()
            .any(|c| c.state() == DsState::LiveRepairReady);

        if repair_in_progress {
            info!(self.log, "Live Repair already running");
            // Queue up a later check if we need it
            if any_in_repair_ready {
                self.repair_check_interval = Some(deadline_secs(10.0));
            } else {
                self.repair_check_interval = None;
            }
        } else if !any_in_repair_ready {
            self.repair_check_interval = None;
            info!(self.log, "No Live Repair required at this time");
        } else if !self.downstairs.start_live_repair(
            &self.state,
            self.guest.guest_work.lock().await.deref_mut(),
            self.ddef.get_def().unwrap().extent_count().into(),
        ) {
            // It's hard to hit this condition; we need a Downstairs to be in
            // LiveRepairReady, but for no other downstairs to be in Active.
            warn!(self.log, "Could not start live repair, trying again later");
            self.repair_check_interval = Some(deadline_secs(10.0));
        } else {
            // We started the repair in the call to start_live_repair above
        }
    }

    /// Returns `true` if we're ready to accept guest IO
    fn guest_io_ready(&self) -> bool {
        matches!(self.state, UpstairsState::Active)
    }

    /// When a `BlockReq` arrives, defer it as a future
    async fn defer_guest_request(&mut self, req: BlockReq) {
        match req.op {
            // All Write operations are deferred, because they will offload
            // encryption to a separate thread pool.
            BlockOp::Write { offset, data } => {
                self.submit_deferred_write(offset, data, req.res, false);
            }
            BlockOp::WriteUnwritten { offset, data } => {
                self.submit_deferred_write(offset, data, req.res, true);
            }
            // If we have any deferred requests in the FuturesOrdered, then we
            // have to keep using it for subsequent requests (even ones that are
            // not writes) to preserve FIFO ordering
            _ if !self.deferred_reqs.is_empty() => {
                self.deferred_reqs.push_back(Either::Left(ready(Ok(Some(
                    DeferredBlockReq::Other(req),
                )))));
            }
            // Otherwise, we can apply a non-write operation immediately, saving
            // a trip through the FuturesUnordered
            _ => {
                self.apply_guest_request_inner(req).await;
            }
        }
    }

    /// Apply a deferred guest request
    ///
    /// For IO operations, we build the downstairs work and if required split
    /// the single IO into multiple IOs to the downstairs. The IO operations are
    /// pushed into downstairs work queues, and will later be sent over the
    /// network.
    ///
    /// This function can be called before the upstairs is active, so any
    /// operation that requires the upstairs to be active should check that
    /// and report an error.
    async fn apply_guest_request(&mut self, req: DeferredBlockReq) {
        match req {
            DeferredBlockReq::Write(req) => self.submit_write(req).await,
            DeferredBlockReq::Other(req) => {
                self.apply_guest_request_inner(req).await
            }
        }
    }

    /// Does the actual work for a (non-write) guest request
    ///
    /// # Panics
    /// This function assumes that `BlockReq::Write` and
    /// `BlockReq::WriteUnwritten` are always deferred and handled separately;
    /// it will panic if `req` matches either of them.
    async fn apply_guest_request_inner(&mut self, req: BlockReq) {
        // If any of the submit_* functions fail to send to the downstairs, they
        // return an error.  These are reported to the Guest.
        let BlockReq { op, res } = req;
        match op {
            // These three options can be handled by this task directly,
            // and don't require the upstairs to be fully online.
            BlockOp::GoActive => {
                self.set_active_request(res).await;
            }
            BlockOp::GoActiveWithGen { gen } => {
                self.cfg.generation.store(gen, Ordering::Release);
                self.set_active_request(res).await;
            }
            BlockOp::QueryGuestIOReady { data } => {
                *data.lock().await = self.guest_io_ready();
                res.send_ok();
            }
            BlockOp::QueryUpstairsUuid { data } => {
                *data.lock().await = self.cfg.upstairs_id;
                res.send_ok();
            }
            BlockOp::Deactivate => {
                self.set_deactivate(res).await;
            }

            // Query ops
            BlockOp::QueryBlockSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.block_size();
                        res.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Block size not available (active: {})",
                            self.guest_io_ready()
                        );
                        res.send_err(CrucibleError::PropertyNotAvailable(
                            "block size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryTotalSize { data } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        *data.lock().await = rd.total_size();
                        res.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Total size not available (active: {})",
                            self.guest_io_ready()
                        );
                        res.send_err(CrucibleError::PropertyNotAvailable(
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
                        res.send_ok();
                    }
                    None => {
                        warn!(
                            self.log,
                            "Extent size not available (active: {})",
                            self.guest_io_ready()
                        );
                        res.send_err(CrucibleError::PropertyNotAvailable(
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
                res.send_ok();
            }

            BlockOp::ShowWork { data } => {
                // TODO should this first check if the Upstairs is active?
                *data.lock().await = self.show_all_work().await;
                res.send_ok();
            }

            BlockOp::Read { offset, data } => {
                self.submit_read(offset, data, res).await
            }
            BlockOp::Write { .. } | BlockOp::WriteUnwritten { .. } => {
                panic!("writes must always be deferred")
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
                    res.send_err(CrucibleError::UpstairsInactive);
                    return;
                }
                self.submit_flush(Some(res), snapshot_details).await;
            }
            BlockOp::ReplaceDownstairs {
                id,
                old,
                new,
                result,
            } => match self.downstairs.replace(id, old, new, &self.state) {
                Ok(v) => {
                    *result.lock().await = v;
                    res.send_ok();
                }
                Err(e) => res.send_err(e),
            },
        }
    }

    pub(crate) async fn show_all_work(&self) -> WQCounts {
        let gior = self.guest_io_ready();
        let up_count = self.guest.guest_work.lock().await.active.len();

        let ds_count = self.downstairs.active_count();

        println!(
            "----------------------------------------------------------------"
        );
        println!(
            " Crucible gen:{} GIO:{} work queues:  Upstairs:{}  downstairs:{}",
            self.cfg.generation(),
            gior,
            up_count,
            ds_count,
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
    async fn set_active_request(&mut self, res: BlockRes) {
        match &self.state {
            UpstairsState::Initializing => {
                self.state = UpstairsState::GoActive(res);
                info!(self.log, "{} active request set", self.cfg.upstairs_id);
            }
            UpstairsState::GoActive(..) => {
                info!(
                    self.log,
                    "{} request to activate upstairs already going active",
                    self.cfg.upstairs_id
                );
                res.send_err(CrucibleError::UpstairsAlreadyActive);
                return;
            }
            UpstairsState::Deactivating(..) => {
                warn!(
                    self.log,
                    "{} active denied while Deactivating", self.cfg.upstairs_id
                );
                res.send_err(CrucibleError::UpstairsDeactivating);
                return;
            }
            UpstairsState::Active => {
                info!(
                    self.log,
                    "{} Request to activate upstairs already active",
                    self.cfg.upstairs_id
                );
                res.send_err(CrucibleError::UpstairsAlreadyActive);
                return;
            }
        }
        // Notify all clients that they should go active when they hit an
        // appropriate state in their negotiation.
        for c in self.downstairs.clients.iter_mut() {
            c.set_active_request().await;
        }
    }

    /// Request that the Upstairs deactivate
    ///
    /// This will return immediately if all of the Downstairs clients are done;
    /// otherwise, it will schedule a final flush that triggers deactivation
    /// when complete.
    ///
    /// In either case, `self.state` is set to `UpstairsState::Deactivating`
    async fn set_deactivate(&mut self, res: BlockRes) {
        info!(self.log, "Request to deactivate this guest");
        match &self.state {
            UpstairsState::Initializing | UpstairsState::GoActive(..) => {
                res.send_err(CrucibleError::UpstairsInactive);
                return;
            }
            UpstairsState::Deactivating(..) => {
                res.send_err(CrucibleError::UpstairsDeactivating);
                return;
            }
            UpstairsState::Active => (),
        }
        if !self.downstairs.can_deactivate_immediately() {
            debug!(self.log, "not ready to deactivate; submitting final flush");
            self.submit_flush(None, None).await;
        } else {
            debug!(self.log, "ready to deactivate right away");
            // Deactivation is handled in the invariant-checking portion of
            // Upstairs::apply.
        }

        self.state = UpstairsState::Deactivating(res);
    }

    pub(crate) async fn submit_flush(
        &mut self,
        res: Option<BlockRes>,
        snapshot_details: Option<SnapshotDetails>,
    ) {
        // Notice that unlike submit_read and submit_write, we do not check for
        // guest_io_ready here. The upstairs itself can call submit_flush
        // (without the guest being involved), so the check is handled at the
        // BlockOp::Flush level above.

        self.need_flush = false;

        /*
         * Get the next ID for our new guest work job. Note that the flush
         * ID and the next_id are connected here, in that all future writes
         * should be flushed at the next flush ID.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let gw_id = gw.next_gw_id();
        cdt::gw__flush__start!(|| (gw_id.0));

        if snapshot_details.is_some() {
            info!(self.log, "flush with snap requested");
        }

        let next_id = self.downstairs.submit_flush(gw_id, snapshot_details);

        let new_gtos = GtoS::new(next_id, None, res);
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__flush__start!(|| (gw_id.0));
    }

    /// Submits a read job to the downstairs
    async fn submit_read(
        &mut self,
        offset: Block,
        data: Buffer,
        res: BlockRes,
    ) {
        self.submit_read_inner(offset, data, Some(res)).await
    }

    /// Submits a dummy read (without associated `BlockReq`)
    #[cfg(test)]
    pub(crate) async fn submit_dummy_read(
        &mut self,
        offset: Block,
        data: Buffer,
    ) {
        self.submit_read_inner(offset, data, None).await
    }

    /// Submit a read job to the downstairs, optionally without a `BlockReq`
    ///
    /// # Panics
    /// If `req` is `None` and this isn't the test suite
    async fn submit_read_inner(
        &mut self,
        offset: Block,
        data: Buffer,
        res: Option<BlockRes>,
    ) {
        #[cfg(not(test))]
        assert!(res.is_some());

        if !self.guest_io_ready() {
            if let Some(res) = res {
                res.send_err(CrucibleError::UpstairsInactive);
            }
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
            if let Some(res) = res {
                res.send_err(e);
            }
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
        let gw_id = gw.next_gw_id();
        cdt::gw__read__start!(|| (gw_id.0));

        let next_id = self.downstairs.submit_read(gw_id, impacted_blocks, ddef);

        // New work created, add to the guest_work HM.  It's fine to do this
        // after submitting the job to the downstairs, because no one else is
        // modifying the Upstairs right now; even if the job finishes
        // instantaneously, it can't interrupt this function.
        let new_gtos = GtoS::new(next_id, Some(data), res);
        gw.active.insert(gw_id, new_gtos);

        cdt::up__to__ds__read__start!(|| (gw_id.0));
    }

    /// Submits a new write job to the upstairs
    ///
    /// This function **defers** the write job submission, because writes
    /// require encrypting data (which is expensive) and we'd like to return as
    /// quickly as possible.
    fn submit_deferred_write(
        &mut self,
        offset: Block,
        data: Bytes,
        res: BlockRes,
        is_write_unwritten: bool,
    ) {
        self.submit_deferred_write_inner(
            offset,
            data,
            Some(res),
            is_write_unwritten,
        )
    }

    /// Submits a dummy write (without an associated `BlockReq`)
    ///
    /// This **does not** go through the deferred-write pipeline
    #[cfg(test)]
    pub(crate) async fn submit_dummy_write(
        &mut self,
        offset: Block,
        data: Bytes,
        is_write_unwritten: bool,
    ) {
        if let Some(w) = self
            .compute_deferred_write(offset, data, None, is_write_unwritten)
            .and_then(DeferredWrite::run)
        {
            self.apply_guest_request(DeferredBlockReq::Write(w)).await
        }
    }

    /// Submits a new write job to the upstairs, optionally without a `BlockReq`
    ///
    /// # Panics
    /// If `req` is `None` and this isn't running in the test suite
    fn submit_deferred_write_inner(
        &mut self,
        offset: Block,
        data: Bytes,
        res: Option<BlockRes>,
        is_write_unwritten: bool,
    ) {
        // It's possible for the write to be invalid out of the gate, in which
        // case `compute_deferred_write` replies to the `req` itself and returns
        // `None`.  Otherwise, we have to store a future to process the write
        // result.
        if let Some(w) =
            self.compute_deferred_write(offset, data, res, is_write_unwritten)
        {
            let (tx, rx) = oneshot::channel();
            rayon::spawn(move || {
                let out = w.run().map(DeferredBlockReq::Write);
                let _ = tx.send(out);
            });
            self.deferred_reqs.push_back(Either::Right(rx));
        }
    }

    fn compute_deferred_write(
        &mut self,
        offset: Block,
        data: Bytes,
        res: Option<BlockRes>,
        is_write_unwritten: bool,
    ) -> Option<DeferredWrite> {
        #[cfg(not(test))]
        assert!(res.is_some());

        if !self.guest_io_ready() {
            if let Some(res) = res {
                res.send_err(CrucibleError::UpstairsInactive);
            }
            return None;
        }
        if self.cfg.read_only {
            if let Some(res) = res {
                res.send_err(CrucibleError::ModifyingReadOnlyRegion);
            }
            return None;
        }

        /*
         * Verify IO is in range for our region
         */
        let ddef = self.ddef.get_def().unwrap();
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            if let Some(res) = res {
                res.send_err(e);
            }
            return None;
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

        Some(DeferredWrite {
            ddef,
            impacted_blocks,
            data,
            res,
            is_write_unwritten,
            cfg: self.cfg.clone(),
        })
    }

    async fn submit_write(&mut self, write: EncryptedWrite) {
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
        let gw_id = gw.next_gw_id();
        if write.is_write_unwritten {
            cdt::gw__write__unwritten__start!(|| (gw_id.0));
        } else {
            cdt::gw__write__start!(|| (gw_id.0));
        }

        let next_id = self.downstairs.submit_write(
            gw_id,
            write.impacted_blocks,
            write.writes,
            write.is_write_unwritten,
        );

        // New work created, add to the guest_work HM
        let new_gtos = GtoS::new(next_id, None, write.res);
        gw.active.insert(gw_id, new_gtos);

        if write.is_write_unwritten {
            cdt::up__to__ds__write__unwritten__start!(|| (gw_id.0));
        } else {
            cdt::up__to__ds__write__start!(|| (gw_id.0));
        }
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_downstairs_action(&mut self, d: DownstairsAction) {
        match d {
            DownstairsAction::Client { client_id, action } => {
                self.apply_client_action(client_id, action).await;
            }
        }
    }

    /// React to an event sent by one of the downstairs clients
    async fn apply_client_action(
        &mut self,
        client_id: ClientId,
        action: ClientAction,
    ) {
        match action {
            ClientAction::Connected => {
                self.downstairs.clients[client_id].stats.connected += 1;
                self.downstairs.clients[client_id].send_here_i_am().await;
            }
            ClientAction::Ping => {
                self.downstairs.clients[client_id].send_ping().await;
            }
            ClientAction::Timeout => {
                // Ask the downstairs client task to stop, because the client
                // has hit a Crucible timeout.
                //
                // This will come back to `TaskStopped`, at which point we'll
                // clear out the task and restart it.
                //
                // We need to reset the timeout, because otherwise it will keep
                // firing and will monopolize the future.
                let c = &mut self.downstairs.clients[client_id];
                c.reset_timeout();
                c.halt_io_task(ClientStopReason::Timeout);
            }
            ClientAction::Response(m) => {
                self.on_client_message(client_id, m).await;
            }
            ClientAction::TaskStopped(r) => {
                self.on_client_task_stopped(client_id, r);
            }
            ClientAction::ChannelClosed => {
                // See docstring for `ClientAction::ChannelClosed`
                warn!(
                    self.log,
                    "IO channel closed for {client_id}; \
                     we are hopefully exiting"
                );
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
                    .continue_negotiation(m, &self.state, &mut self.ddef)
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
                                if self.connect_region_set().await {
                                    // We connected normally, so there's no need
                                    // to check for live-repair.
                                    self.repair_check_interval = None;
                                }
                            }

                            DsState::LiveRepairReady => {
                                // Immediately check for live-repair
                                self.repair_check_interval =
                                    Some(deadline_secs(0.0));
                            }

                            s => panic!("bad state after negotiation: {s:?}"),
                        }
                    }
                }
            }

            Message::ExtentError { .. } => {
                self.downstairs.on_reconciliation_failed(
                    client_id,
                    m,
                    &self.state,
                );
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
                self.on_no_longer_active(client_id, m);
            }

            Message::UuidMismatch { .. } => {
                self.on_uuid_mismatch(client_id, m);
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
    async fn connect_region_set(&mut self) -> bool {
        /*
         * If reconciliation (also called Repair in DsState) is required, it
         * happens in three phases.  Typically an interruption of repair will
         * result in things starting over, but if actual repair work to an
         * extent is completed, that extent won't need to be repaired again.
         *
         * The three phases are:
         *
         * Collect:
         * When a Downstairs connects, the Upstairs collects the gen/flush/dirty
         * (GFD) info from all extents.  This GFD information is stored and the
         * Upstairs waits for all three Downstairs to attach.
         *
         * Compare:
         * In the compare phase, the upstairs will walk the list of all extents
         * and compare the G/F/D from each of the downstairs.  When there is a
         * mismatch between downstairs (The dirty bit counts as a mismatch and
         * will force a repair even if generation and flush numbers agree). For
         * each mismatch, the upstairs determines which downstairs has the
         * extent that should be the source, and which of the other downstairs
         * extents needs repair. This list of mismatches (source,
         * destination(s)) is collected. Once an upstairs has compiled its
         * repair list, it will then generates a sequence of Upstairs ->
         * Downstairs repair commands to repair each extent that needs to be
         * fixed.  For a given piece of repair work, the commands are:
         * - Send a flush to source extent.
         * - Close extent on all downstairs.
         * - Send repair command to destination extents (with source extent
         *   IP/Port).
         * (See DS-DS Repair)
         * - Reopen all extents.
         *
         * Repair:
         * During repair Each command issued from the upstairs must be completed
         * before the next will be sent. The Upstairs is responsible for walking
         * the repair commands and sending them to the required downstairs, and
         * waiting for them to finish.  The actual repair work for an extent
         * takes place on the downstairs being repaired.
         *
         * Repair (ds to ds)
         * Each downstairs runs a repair server (Dropshot) that listens for
         * repair requests from other downstairs.  A downstairs with an extent
         * that needs repair will contact the source downstairs and request the
         * list of files for an extent, then request each file.  Once all files
         * are local to the downstairs needing repair, it will replace the
         * existing extent files with the new ones.
         */
        let collate_status = {
            /*
             * Reconciliation only happens during initialization.
             * Look at all three downstairs region information collected.
             * Determine the highest flush number and make sure our generation
             * is high enough.
             */
            if !matches!(&self.state, UpstairsState::GoActive(..)) {
                info!(
                    self.log,
                    "could not connect region set due to bad state: {:?}",
                    self.state
                );
                return false;
            }
            /*
             * Make sure all downstairs are in the correct state before we
             * proceed.
             */
            let not_ready = self
                .downstairs
                .clients
                .iter()
                .any(|c| c.state() != DsState::WaitQuorum);
            if not_ready {
                info!(
                    self.log,
                    "Waiting for {} more clients to be ready", not_ready
                );
                return false;
            }

            /*
             * We figure out if there is any reconciliation to do, and if so, we
             * build the list of operations that will repair the extents that
             * are not in sync.
             *
             * If we fail to collate, then we need to kick out all the
             * downstairs out, forget any activation requests, and the
             * upstairs goes back to waiting for another activation request.
             */
            self.downstairs.collate()
        };

        match collate_status {
            Err(e) => {
                error!(self.log, "Failed downstairs collate with: {}", e);
                // We failed to collate the three downstairs, so we need
                // to reset that activation request.  Call
                // `abort_reconciliation` to abort reconciliation for all
                // clients.
                self.set_inactive(e);
                self.downstairs.abort_reconciliation(&self.state);
                false
            }
            Ok(true) => {
                // We have populated all of the reconciliation requests in
                // `Downstairs::reconcile_task_list`.  Start reconciliation by
                // sending the first request.
                self.downstairs.send_next_reconciliation_req().await;
                true
            }
            Ok(false) => {
                info!(self.log, "No downstairs repair required");
                self.on_reconciliation_done(DsState::WaitQuorum);
                info!(self.log, "Set Active after no repair");
                true
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

        if !matches!(self.state, UpstairsState::GoActive(..)) {
            error!(
                self.log,
                "reconciliation done, but upstairs is no longer GoActive: {:?}",
                self.state
            );
            return;
        }

        // Swap out the state for UpstairsState::Active
        let UpstairsState::GoActive(res) =
            std::mem::replace(&mut self.state, UpstairsState::Active)
        else {
            unreachable!(); // checked above
        };
        res.send_ok();
        info!(
            self.log,
            "{} is now active with session: {}",
            self.cfg.upstairs_id,
            self.cfg.session_id
        );
        self.stats.add_activation();
    }

    fn on_no_longer_active(&mut self, client_id: ClientId, m: Message) {
        let Message::YouAreNoLongerActive {
            new_upstairs_id,
            new_session_id,
            new_gen,
        } = m
        else {
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
        let uuid_desc = if self.cfg.upstairs_id == new_upstairs_id {
            "same upstairs UUID".to_owned()
        } else {
            format!("different upstairs UUID {new_upstairs_id:?}")
        };

        let our_gen = self.cfg.generation();
        if new_gen <= our_gen {
            // Here, our generation number is greater than or equal to the newly
            // active Upstairs, which shares our UUID. We shouldn't have
            // received this message. The downstairs is confused.
            error!(
                client_log,
                "bad YouAreNoLongerActive with our gen {our_gen} >= {new_gen} \
                 and {uuid_desc}",
            );
        } else {
            // The next generation of this Upstairs connected, which is fine.
            warn!(
                client_log,
                "saw YouAreNoLongerActive with our gen {our_gen} < {new_gen} \
                 and {uuid_desc}",
            );
        };

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id].disable(&self.state);
        self.set_inactive(CrucibleError::NoLongerActive);
    }

    fn on_uuid_mismatch(&mut self, client_id: ClientId, m: Message) {
        let Message::UuidMismatch { expected_id } = m else {
            panic!("called on_uuid_mismatch on invalid message {m:?}");
        };

        let client_log = &self.downstairs.clients[client_id].log;
        error!(
            client_log,
            "received UuidMismatch, expecting {expected_id:?}!"
        );

        // Restart the state machine for this downstairs client
        self.downstairs.clients[client_id].disable(&self.state);
        self.set_inactive(CrucibleError::UuidMismatch);
    }

    /// Forces the upstairs state to `UpstairsState::Active`
    ///
    /// This means that we haven't gone through negotiation, so behavior may be
    /// wonky or unexpected; this is only allowed during unit tests.
    #[cfg(test)]
    pub(crate) fn force_active(&mut self) -> Result<(), CrucibleError> {
        match &self.state {
            UpstairsState::Initializing => {
                self.state = UpstairsState::Active;
                Ok(())
            }
            UpstairsState::Active | UpstairsState::GoActive(..) => {
                Err(CrucibleError::UpstairsAlreadyActive)
            }
            UpstairsState::Deactivating(..) => {
                /*
                 * We don't support deactivate interruption, so we have to
                 * let the currently running deactivation finish before we
                 * can accept an activation.
                 */
                Err(CrucibleError::UpstairsDeactivating)
            }
        }
    }

    fn set_inactive(&mut self, err: CrucibleError) {
        let prev =
            std::mem::replace(&mut self.state, UpstairsState::Initializing);
        if let UpstairsState::GoActive(res) = prev {
            res.send_err(err);
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
        // If the upstairs is already active (or trying to go active), then the
        // downstairs should automatically call PromoteToActive when it reaches
        // the relevant state.
        let auto_promote = match self.state {
            UpstairsState::Active | UpstairsState::GoActive(..) => true,
            UpstairsState::Initializing
            | UpstairsState::Deactivating { .. } => false,
        };

        self.downstairs.reinitialize(
            client_id,
            auto_promote,
            reason,
            &self.state,
        );
    }

    fn set_backpressure(&self) {
        let dsw_max = self
            .downstairs
            .clients
            .iter()
            .map(|c| c.total_live_work())
            .max()
            .unwrap_or(0);
        let ratio = dsw_max as f64 / crate::IO_OUTSTANDING_MAX as f64;
        self.guest
            .set_backpressure(self.downstairs.write_bytes_outstanding(), ratio);
    }

    /// Returns the `RegionDefinition`
    ///
    /// # Panics
    /// If the region definition is not yet known (i.e. it wasn't provided on
    /// startup, and no Downstairs have started talking to us yet).
    #[cfg(test)]
    pub(crate) fn get_region_definition(&self) -> RegionDefinition {
        self.ddef.get_def().unwrap()
    }

    /// Helper function to do a checked state transition on the given client
    #[cfg(test)]
    pub(crate) fn ds_transition(
        &mut self,
        client_id: ClientId,
        new_state: DsState,
    ) {
        self.downstairs.clients[client_id]
            .checked_state_transition(&self.state, new_state);
    }

    /// Helper function to get a downstairs client state
    #[cfg(test)]
    pub(crate) fn ds_state(&self, client_id: ClientId) -> DsState {
        self.downstairs.clients[client_id].state()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{
        downstairs::test::set_all_active,
        test::{make_encrypted_upstairs, make_upstairs},
        BlockContext, BlockReq, BlockReqWaiter, DsState, JobId,
    };
    use bytes::BytesMut;
    use crucible_common::integrity_hash;
    use crucible_protocol::ReadResponse;
    use futures::FutureExt;

    // Test function to create just enough of an Upstairs for our needs.
    pub(crate) fn create_test_upstairs() -> Upstairs {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        set_all_active(&mut up.downstairs);
        for c in up.downstairs.clients.iter_mut() {
            // Give all downstairs a repair address
            c.repair_addr = Some("0.0.0.0:1".parse().unwrap());
        }

        up.force_active().unwrap();
        up
    }

    // A function that does some setup that other tests can use to avoid
    // having the same boilerplate code all over the place, and allows the
    // test to make clear what it's actually testing.
    //
    // The caller will indicate which downstairs client it wished to be
    // moved to LiveRepair.
    pub(crate) async fn start_up_and_repair(or_ds: ClientId) -> Upstairs {
        let mut up = create_test_upstairs();

        // Move our downstairs client fail_id to LiveRepair.
        let client = &mut up.downstairs.clients[or_ds];
        client.checked_state_transition(&up.state, DsState::Faulted);
        client.checked_state_transition(&up.state, DsState::LiveRepairReady);

        // Start repairing the downstairs; this also enqueues the jobs
        up.apply(UpstairsAction::RepairCheck).await;

        // Assert that the repair started
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());

        // The first thing that should happen after we start repair_exetnt
        // is two jobs show up on the work queue, one for close and one for
        // the eventual re-open.  Wait here for those jobs to show up on the
        // work queue before returning.
        let jobs = up.downstairs.active_count();
        assert_eq!(jobs, 2);
        up
    }

    #[tokio::test]
    async fn reconcile_not_ready() {
        // Verify reconcile returns false when a downstairs is not ready
        let mut up = Upstairs::test_default(None);
        up.ds_transition(ClientId::new(0), DsState::WaitActive);
        up.ds_transition(ClientId::new(0), DsState::WaitQuorum);

        up.ds_transition(ClientId::new(1), DsState::WaitActive);
        up.ds_transition(ClientId::new(1), DsState::WaitQuorum);

        let res = up.connect_region_set().await;
        assert!(!res);
        assert!(!matches!(&up.state, &UpstairsState::Active))
    }

    #[tokio::test]
    async fn deactivate_not_while_deactivating() {
        // Verify that we can't set deactivate on the upstairs when
        // the upstairs is still in init.
        // Verify that we can't set deactivate on the upstairs when
        // we are deactivating.
        // TODO: This test should change when we support this behavior.

        let mut up = Upstairs::test_default(None);

        let (ds_done_brw, ds_done_res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Deactivate,
            res: ds_done_res,
        }))
        .await;
        assert!(ds_done_brw.wait(&up.log).await.is_err());

        up.force_active().unwrap();

        let (ds_done_brw, ds_done_res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Deactivate,
            res: ds_done_res,
        }))
        .await;
        assert!(ds_done_brw.wait(&up.log).await.is_ok());

        let (ds_done_brw, ds_done_res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Deactivate,
            res: ds_done_res,
        }))
        .await;
        assert!(ds_done_brw.wait(&up.log).await.is_err())
    }

    #[tokio::test]
    async fn deactivate_when_empty() {
        // Verify we can deactivate if no work is present, without
        // creating a flush (as their should already have been one).
        // Verify after all three downstairs are deactivated, we can
        // transition the upstairs back to init.

        let mut up = Upstairs::test_default(None);
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        // The deactivate message should happen immediately
        let (ds_done_brw, ds_done_res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Deactivate,
            res: ds_done_res,
        }))
        .await;

        // Make sure the correct DS have changed state.
        for client_id in ClientId::iter() {
            // The downstairs is already deactivated
            assert_eq!(up.ds_state(client_id), DsState::Deactivated);

            // Push the event loop forward with the info that the IO task has
            // now stopped.
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::TaskStopped(
                    ClientRunResult::RequestedStop(
                        ClientStopReason::Deactivated,
                    ),
                ),
            }))
            .await;

            // This causes the downstairs state to be reinitialized
            assert_eq!(up.ds_state(client_id), DsState::New);

            if client_id.get() < 2 {
                assert!(matches!(up.state, UpstairsState::Deactivating { .. }));
            } else {
                // Once the third task stops, we're back in initializing
                assert!(matches!(up.state, UpstairsState::Initializing));
            }
        }
        assert!(ds_done_brw.wait(&up.log).await.is_ok());
    }

    // Job dependency tests
    //
    // Each job dependency test will include a chart of the operations and
    // dependencies that are expected to be created through the submission of
    // those operations. An example:
    //
    //             block
    //    op# | 0 1 2 3 4 5 | deps
    //    ----|-------------|-----
    //      0 | W           |
    //      1 |   W         |
    //      2 |     W       |
    //      3 | FFFFFFFFFFF | 0,1,2
    //      4 |       W     | 3
    //      5 |         W   | 3
    //      6 |           W | 3
    //
    // The order of enqueued operations matches the op# column. In the above
    // example, three writes were submitted, followed by a flush, followed by
    // three more writes. There is only one operation per row.
    //
    // An operation marks what block it acts on in an extent (in the center
    // column) with the type of operation it is: R is a read, W is a write, and
    // Wu is a write unwritten. Flushes impact the whole extent and are marked
    // with F across every block. If an operation covers more than one extent,
    // it will have multiple columns titled 'block'.
    //
    // The deps column shows which operations this operation depends on -
    // dependencies must run before the operation can run. If the column is
    // empty, then the operation does not depend on any other operation. In the
    // above example, operation 3 depends on operations 0, 1, and 2.
    //

    #[tokio::test]
    async fn test_deps_writes_depend_on_overlapping_writes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | W     | 0

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0x00; 512]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_deps_writes_depend_on_overlapping_writes_chain() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | W     | 0
        //   2 | W     | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0x00; 512]),
                false,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0x55; 512]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id],);
    }

    #[tokio::test]
    async fn test_deps_writes_depend_on_overlapping_writes_and_flushes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | FFFFF | 0
        //   2 | W     | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs.submit_flush(None, None).await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0x55; 512]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write (op 0)
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // flush (op 1)
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // write (op 2)
    }

    #[tokio::test]
    async fn test_deps_all_writes_depend_on_flushes() {
        // Test that the following job dependency graph is made:
        //
        //          block
        // op# | 0 1 2 3 4 5 | deps
        // ----|-------------|-----
        //   0 | W           |
        //   1 |   W         |
        //   2 |     W       |
        //   3 | FFFFFFFFFFF | 0,1,2
        //   4 |       W     | 3
        //   5 |         W   | 3
        //   6 |           W | 3

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // ops 0 to 2
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // op 3
        upstairs.submit_flush(None, None).await;

        // ops 4 to 6
        for i in 3..6 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 7);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert!(jobs[1].work.deps().is_empty()); // write @ 1
        assert!(jobs[2].work.deps().is_empty()); // write @ 2

        assert_eq!(
            jobs[3].work.deps(), // flush
            &[jobs[0].ds_id, jobs[1].ds_id, jobs[2].ds_id],
        );

        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // write @ 3
        assert_eq!(jobs[5].work.deps(), &[jobs[3].ds_id]); // write @ 4
        assert_eq!(jobs[6].work.deps(), &[jobs[3].ds_id]); // write @ 5
    }

    #[tokio::test]
    async fn test_deps_little_writes_depend_on_big_write() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W W W |
        //   1 | W     | 0
        //   2 |   W   | 0
        //   3 |     W | 0

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512 * 3]),
                false,
            )
            .await;

        // ops 1 to 3
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 4);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0,1,2

        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // write @ 0
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // write @ 1
        assert_eq!(jobs[3].work.deps(), &[jobs[0].ds_id]); // write @ 2
    }

    #[tokio::test]
    async fn test_deps_little_writes_depend_on_big_write_chain() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W W W |
        //   1 | W     | 0
        //   2 |   W   | 0
        //   3 |     W | 0
        //   4 | W     | 1
        //   5 |   W   | 2
        //   6 |     W | 3

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512 * 3]),
                false,
            )
            .await;

        // ops 1 to 3
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // ops 4 to 6
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 7);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0,1,2

        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // write @ 0
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // write @ 1
        assert_eq!(jobs[3].work.deps(), &[jobs[0].ds_id]); // write @ 2

        assert_eq!(
            jobs[4].work.deps(), // second write @ 0
            &[jobs[1].ds_id],
        );
        assert_eq!(
            jobs[5].work.deps(), // second write @ 1
            &[jobs[2].ds_id],
        );
        assert_eq!(
            jobs[6].work.deps(), // second write @ 2
            &[jobs[3].ds_id],
        );
    }

    #[tokio::test]
    async fn test_deps_big_write_depends_on_little_writes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 |   W   |
        //   2 |     W |
        //   3 | W W W | 0,1,2

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // ops 0 to 2
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // op 3
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512 * 3]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 4);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert!(jobs[1].work.deps().is_empty()); // write @ 1
        assert!(jobs[2].work.deps().is_empty()); // write @ 2

        assert_eq!(
            jobs[3].work.deps(), // write @ 0,1,2
            &[jobs[0].ds_id, jobs[1].ds_id, jobs[2].ds_id],
        );
    }

    #[tokio::test]
    async fn test_deps_read_depends_on_write() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | R     | 0

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // read @ 0
    }

    #[tokio::test]
    async fn test_deps_big_read_depends_on_little_writes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 |   W   |
        //   2 |     W |
        //   3 | R R   | 0,1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // ops 0 to 2
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // op 3
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512 * 2))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 4);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert!(jobs[1].work.deps().is_empty()); // write @ 1
        assert!(jobs[2].work.deps().is_empty()); // write @ 2

        assert_eq!(
            jobs[3].work.deps(), // read @ 0,1
            &[jobs[0].ds_id, jobs[1].ds_id],
        );
    }

    #[tokio::test]
    async fn test_deps_read_no_depend_on_read() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 | R     |
        //
        // (aka two reads don't depend on each other)

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        // op 1
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // read @ 0
        assert!(jobs[1].work.deps().is_empty()); // read @ 0
    }

    #[tokio::test]
    async fn test_deps_multiple_reads_depend_on_write() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | R     | 0
        //   2 | R     | 0

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        // op 2
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // read @ 0
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // read @ 0
    }

    #[tokio::test]
    async fn test_deps_read_depends_on_flush() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 | FFFFF | 0
        //   2 | R     | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 1
        upstairs.submit_flush(None, None).await;

        // op 2
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512 * 2))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // flush
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // read @ 0
    }

    #[tokio::test]
    async fn test_deps_flushes_depend_on_flushes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | FFFFF |
        //   1 | FFFFF | 0
        //   2 | FFFFF | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        upstairs.submit_flush(None, None).await;

        upstairs.submit_flush(None, None).await;

        upstairs.submit_flush(None, None).await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]);
    }

    #[tokio::test]
    async fn test_deps_flushes_depend_on_flushes_and_all_writes() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | FFFFF |
        //   1 | W     | 0
        //   2 |   W   | 0
        //   3 | FFFFF | 0,1,2
        //   4 | W     | 3
        //   5 |   W   | 3
        //   6 |     W | 3
        //   7 | FFFFF | 3,4,5,6

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs.submit_flush(None, None).await;

        // ops 1 to 2
        for i in 0..2 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // op 3
        upstairs.submit_flush(None, None).await;

        // ops 4 to 6
        for i in 0..3 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512]),
                    false,
                )
                .await;
        }

        // op 7
        upstairs.submit_flush(None, None).await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 8);

        assert!(jobs[0].work.deps().is_empty()); // flush (op 0)

        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // write (op 1)
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // write (op 2)

        assert_eq!(
            jobs[3].work.deps(),
            &[jobs[0].ds_id, jobs[1].ds_id, jobs[2].ds_id],
        ); // flush (op 3)

        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // write (op 4)
        assert_eq!(jobs[5].work.deps(), &[jobs[3].ds_id]); // write (op 5)
        assert_eq!(jobs[6].work.deps(), &[jobs[3].ds_id]); // write (op 6)

        assert_eq!(
            jobs[7].work.deps(), // flush (op 7)
            &[jobs[3].ds_id, jobs[4].ds_id, jobs[5].ds_id, jobs[6].ds_id],
        );
    }

    #[tokio::test]
    async fn test_deps_writes_depend_on_read() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 | W     | 0

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
    }

    #[tokio::test]
    async fn test_deps_write_unwrittens_depend_on_read() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 | Wu    | 0
        //   2 | R     | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                true,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
    }

    #[tokio::test]
    async fn test_deps_read_write_ladder_1() {
        // Test that the following job dependency graph is made:
        //
        //          block
        // op# | 0 1 2 3 4 5 | deps
        // ----|-------------|-----
        //   0 | R           |
        //   1 | Wu          | 0
        //   2 |   R R       |
        //   3 |   W W       | 2
        //   4 |       R R   |
        //   5 |       WuWu  | 4

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_read(Block::new_512(0), Buffer::new(512))
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                true,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_read(Block::new_512(1), Buffer::new(512 * 2))
            .await;

        // op 3
        upstairs
            .submit_dummy_write(
                Block::new_512(1),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 4
        upstairs
            .submit_dummy_read(Block::new_512(3), Buffer::new(512 * 2))
            .await;

        // op 5
        upstairs
            .submit_dummy_write(
                Block::new_512(3),
                Bytes::from(vec![0xff; 512 * 2]),
                true,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 6);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1

        assert!(jobs[2].work.deps().is_empty()); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3

        assert!(jobs[4].work.deps().is_empty()); // op 4
        assert_eq!(jobs[5].work.deps(), &[jobs[4].ds_id]); // op 5
    }

    #[tokio::test]
    async fn test_deps_read_write_ladder_2() {
        // Test that the following job dependency graph is made:
        //
        //          block
        // op# | 0 1 2 3 4 5 | deps
        // ----|-------------|-----
        //   0 | WuWu        |
        //   1 |   WuWu      | 0
        //   2 |     WuWu    | 1
        //   3 |       WuWu  | 2
        //   4 |         WuWu| 3

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // ops 0 to 4
        for i in 0..5 {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512 * 2]),
                    true,
                )
                .await;
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3
        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // op 4
    }

    #[tokio::test]
    async fn test_deps_read_write_ladder_3() {
        // Test that the following job dependency graph is made:
        //
        //          block
        // op# | 0 1 2 3 4 5 | deps
        // ----|-------------|-----
        //   0 |         W W |
        //   1 |       W W   | 0
        //   2 |     W W     | 1
        //   3 |   W W       | 2
        //   4 | W W         | 3

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // ops 0 to 4
        for i in (0..5).rev() {
            upstairs
                .submit_dummy_write(
                    Block::new_512(i),
                    Bytes::from(vec![0xff; 512 * 2]),
                    false,
                )
                .await;
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3
        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // op 4
    }

    #[tokio::test]
    async fn test_deps_read_write_batman() {
        // Test that the following job dependency graph is made:
        //
        //          block
        // op# | 0 1 2 3 4 5 | deps
        // ----|-------------|-----
        //   0 | W W         |
        //   1 |         W W |
        //   2 |   W W W W   | 0,1
        //   3 |             |
        //   4 |             |

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(4),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(1),
                Bytes::from(vec![0xff; 512 * 4]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert!(jobs[1].work.deps().is_empty()); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id],); // op 2
    }

    #[tokio::test]
    async fn test_deps_multi_extent_write() {
        // Test that the following job dependency graph is made:
        //
        //     |      block     |      block      |
        // op# | 95 96 97 98 99 |  0  1  2  3  4  | deps
        // ----|----------------|-----------------|-----
        //   0 |  W  W          |                 |
        //   1 |     W  W  W  W |  W  W  W        | 0
        //   2 |                |        W  W     | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(95),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(96),
                Bytes::from(vec![0xff; 512 * 7]),
                false,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(102),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        let ds = &upstairs.downstairs;
        let jobs = ds.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        // confirm which extents are impacted (in case make_upstairs changes)
        assert_eq!(ds.get_extents_for(jobs[0]).extents().unwrap().count(), 1);
        assert_eq!(ds.get_extents_for(jobs[1]).extents().unwrap().count(), 2);
        assert_eq!(ds.get_extents_for(jobs[2]).extents().unwrap().count(), 1);
        assert_ne!(ds.get_extents_for(jobs[0]), ds.get_extents_for(jobs[2]));

        // confirm deps
        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
    }

    #[tokio::test]
    async fn test_deps_multi_extent_there_and_back_again() {
        // Test that the following job dependency graph is made:
        //
        //     |      block     |      block      |
        // op# | 95 96 97 98 99 |  0  1  2  3  4  | deps
        // ----|----------------|-----------------|-----
        //   0 |  W  W          |                 |
        //   1 |     W  W  W  W |  W  W  W        | 0
        //   2 |                |     W           | 1
        //   3 |              Wu|  Wu Wu          | 1,2
        //   4 |              R |                 | 3

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(95),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(96),
                Bytes::from(vec![0xff; 512 * 7]),
                false,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(101),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 3
        upstairs
            .submit_dummy_write(
                Block::new_512(99),
                Bytes::from(vec![0xff; 512 * 3]),
                true,
            )
            .await;

        // op 4
        upstairs
            .submit_dummy_read(Block::new_512(99), Buffer::new(512))
            .await;

        let ds = &upstairs.downstairs;
        let jobs = ds.get_all_jobs();
        assert_eq!(jobs.len(), 5);

        // confirm which extents are impacted (in case make_upstairs changes)
        assert_eq!(ds.get_extents_for(jobs[0]).extents().unwrap().count(), 1);
        assert_eq!(ds.get_extents_for(jobs[1]).extents().unwrap().count(), 2);
        assert_eq!(ds.get_extents_for(jobs[2]).extents().unwrap().count(), 1);
        assert_eq!(ds.get_extents_for(jobs[3]).extents().unwrap().count(), 2);
        assert_eq!(ds.get_extents_for(jobs[4]).extents().unwrap().count(), 1);

        assert_ne!(ds.get_extents_for(jobs[0]), ds.get_extents_for(jobs[2]));
        assert_ne!(ds.get_extents_for(jobs[4]), ds.get_extents_for(jobs[2]));

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[1].ds_id, jobs[2].ds_id]); // op 3
        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // op 4
    }

    #[tokio::test]
    async fn test_deps_multi_extent_batman() {
        // Test that the following job dependency graph is made:
        //
        //     |      block     |      block      |
        // op# | 95 96 97 98 99 |  0  1  2  3  4  | deps
        // ----|----------------|-----------------|-----
        //   0 |  W  W          |                 |
        //   1 |                |        W        |
        //   2 |     Wu Wu Wu Wu|  Wu Wu Wu       | 0,1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_write(
                Block::new_512(95),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        // op 1
        upstairs
            .submit_dummy_write(
                Block::new_512(102),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(96),
                Bytes::from(vec![0xff; 512 * 7]),
                true,
            )
            .await;

        let ds = &upstairs.downstairs;
        let jobs = ds.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        // confirm which extents are impacted (in case make_upstairs changes)
        assert_eq!(ds.get_extents_for(jobs[0]).extents().unwrap().count(), 1);
        assert_eq!(ds.get_extents_for(jobs[1]).extents().unwrap().count(), 1);
        assert_eq!(ds.get_extents_for(jobs[2]).extents().unwrap().count(), 2);

        assert_ne!(ds.get_extents_for(jobs[0]), ds.get_extents_for(jobs[1]));

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert!(jobs[1].work.deps().is_empty()); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id]); // op 2
    }

    #[tokio::test]
    async fn test_read_flush_write_hash_mismatch() {
        // Test that the following job dependency graph is made:
        //
        //     |      block     |
        // op# | 95 96 97 98 99 | deps
        // ----|----------------|-----
        //   0 |  R  R          |
        //   1 | FFFFFFFFFFFFFFF| 0
        //   2 |     W  W       | 1

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // op 0
        upstairs
            .submit_dummy_read(Block::new_512(95), Buffer::new(512 * 2))
            .await;

        // op 1
        upstairs.submit_flush(None, None).await;

        // op 2
        upstairs
            .submit_dummy_write(
                Block::new_512(96),
                Bytes::from(vec![0xff; 512 * 2]),
                false,
            )
            .await;

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        // assert read has no deps
        assert!(jobs[0].work.deps().is_empty()); // op 0

        // assert flush depends on the read
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1

        // assert write depends on just the flush
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
    }

    #[tokio::test]
    async fn test_deps_depend_on_acked_work() {
        // Test that jobs will depend on acked work (important for the case of
        // replay - the upstairs will replay all work since the last flush if a
        // downstairs leaves and comes back)

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // submit a write, complete, then ack it

        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        {
            let ds = &mut upstairs.downstairs;
            let jobs = ds.get_all_jobs();
            assert_eq!(jobs.len(), 1);

            let ds_id = jobs[0].ds_id;

            crate::downstairs::test::finish_job(ds, ds_id);
        }

        // submit an overlapping write

        upstairs
            .submit_dummy_write(
                Block::new_512(0),
                Bytes::from(vec![0xff; 512]),
                false,
            )
            .await;

        {
            let ds = &upstairs.downstairs;
            let jobs = ds.get_all_jobs();

            // retire_check not run yet, so there's two active jobs
            assert_eq!(jobs.len(), 2);

            // the second write should still depend on the first write!
            assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        }
    }

    #[tokio::test]
    async fn test_check_for_repair_normal() {
        // No repair needed here.
        // Verify we can't repair when the upstairs is not active.
        // Verify we wont try to repair if it's not needed.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));

        // Before we are active, we have no need to repair or check for future
        // repairs.
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(!up.downstairs.live_repair_in_progress());

        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        // No need to repair or check for future repairs here either
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(!up.downstairs.live_repair_in_progress());

        // No downstairs should change state.
        for c in up.downstairs.clients.iter() {
            assert_eq!(c.state(), DsState::Active);
        }
        assert!(up.downstairs.repair().is_none());
    }

    #[tokio::test]
    async fn test_check_for_repair_do_repair() {
        // No repair needed here.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        // Force client 1 into LiveRepairReady
        up.ds_transition(ClientId::new(1), DsState::Faulted);
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady);
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());
        assert_eq!(up.ds_state(ClientId::new(1)), DsState::LiveRepair);
        assert!(up.downstairs.repair().is_some());
    }

    #[tokio::test]
    async fn test_check_for_repair_do_two_repair() {
        // No repair needed here.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        for i in [1, 2].into_iter().map(ClientId::new) {
            // Force client 1 into LiveRepairReady
            up.ds_transition(i, DsState::Faulted);
            up.ds_transition(i, DsState::LiveRepairReady);
        }
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());

        assert_eq!(up.ds_state(ClientId::new(0)), DsState::Active);
        assert_eq!(up.ds_state(ClientId::new(1)), DsState::LiveRepair);
        assert_eq!(up.ds_state(ClientId::new(2)), DsState::LiveRepair);
        assert!(up.downstairs.repair().is_some())
    }

    #[tokio::test]
    async fn test_check_for_repair_already_repair() {
        // No repair needed here.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);
        up.ds_transition(ClientId::new(1), DsState::Faulted);
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady);
        up.ds_transition(ClientId::new(1), DsState::LiveRepair);

        // Start the live-repair
        up.on_repair_check().await;
        assert!(up.downstairs.live_repair_in_progress());
        assert!(up.repair_check_interval.is_none());

        // Pretend that DS 0 faulted then came back through to LiveRepairReady;
        // we won't halt the existing repair, but will configure
        // repair_check_interval to check again in the future.
        up.ds_transition(ClientId::new(0), DsState::Faulted);
        up.ds_transition(ClientId::new(0), DsState::LiveRepairReady);

        up.on_repair_check().await;
        assert!(up.downstairs.live_repair_in_progress());
        assert!(up.repair_check_interval.is_some());
    }

    #[tokio::test]
    async fn test_check_for_repair_task_running() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);
        up.ds_transition(ClientId::new(1), DsState::Faulted);
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady);

        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());

        // Checking again is idempotent
        up.on_repair_check().await;
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());
    }

    // Deactivate tests
    #[tokio::test]
    async fn deactivate_after_work_completed_write() {
        deactivate_after_work_completed(false).await;
    }

    #[tokio::test]
    async fn deactivate_after_work_completed_write_unwritten() {
        deactivate_after_work_completed(true).await;
    }

    async fn deactivate_after_work_completed(is_write_unwritten: bool) {
        // Verify that submitted IO will continue after a deactivate.
        // Verify that the flush takes three completions.
        // Verify that deactivate done returns the upstairs to init.

        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        // Build a write, put it on the work queue.
        let offset = Block::new_512(7);
        let data = Bytes::from(vec![1; 512]);
        let op = if is_write_unwritten {
            BlockOp::WriteUnwritten { offset, data }
        } else {
            BlockOp::Write { offset, data }
        };
        let (_write_brw, write_res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq { op, res: write_res }))
            .await;
        up.await_deferred_reqs().await;
        let id1 = JobId(1000); // We know that job IDs start at 1000

        // Create and enqueue the flush by setting deactivate
        let (mut deactivate_done_brw, deactivate_done_res) =
            BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Deactivate,
            res: deactivate_done_res,
        }))
        .await;

        // The deactivate didn't return right away
        assert_eq!(deactivate_done_brw.try_wait(), None);

        // We know that the deactivate created a flush operation, which was
        // assigned the next available ID.
        let flush_id = JobId(id1.0 + 1);

        // Complete the writes
        for client_id in ClientId::iter() {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::Response(Message::WriteAck {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: id1,
                    result: Ok(()),
                }),
            }))
            .await;
        }

        // Verify the deactivate is not done yet.
        assert_eq!(deactivate_done_brw.try_wait(), None);

        // Make sure no DS have changed state.
        for c in up.downstairs.clients.iter() {
            assert_eq!(c.state(), DsState::Active);
        }

        // Complete the flush on two downstairs, at which point the deactivate
        // is still pending.
        for client_id in [0, 2].into_iter().map(ClientId::new) {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::Response(Message::FlushAck {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: flush_id,
                    result: Ok(()),
                }),
            }))
            .await;
            assert_eq!(deactivate_done_brw.try_wait(), None);
        }

        // These downstairs should now be deactivated now
        assert_eq!(up.ds_state(ClientId::new(0)), DsState::Deactivated);
        assert_eq!(up.ds_state(ClientId::new(2)), DsState::Deactivated);

        // Verify the remaining DS is still running
        assert_eq!(up.ds_state(ClientId::new(1)), DsState::Active);

        // Verify the deactivate is not done yet.
        assert_eq!(deactivate_done_brw.try_wait(), None);
        assert!(matches!(up.state, UpstairsState::Deactivating { .. }));

        // Complete the flush on the remaining downstairs
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::FlushAck {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: flush_id,
                result: Ok(()),
            }),
        }))
        .await;

        assert_eq!(up.ds_state(ClientId::new(1)), DsState::Deactivated);

        // Report all three DS as missing, which moves them to New and finishes
        // deactivation
        for client_id in ClientId::iter() {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::TaskStopped(
                    ClientRunResult::RequestedStop(
                        ClientStopReason::Deactivated,
                    ),
                ),
            }))
            .await;
        }
        assert_eq!(deactivate_done_brw.try_wait(), Some(Ok(())));

        // Verify we have disconnected and can go back to init.
        assert!(matches!(up.state, UpstairsState::Initializing));

        // Verify after the ds_missing, all downstairs are New
        for c in up.downstairs.clients.iter() {
            assert_eq!(c.state(), DsState::New);
        }
    }

    #[tokio::test]
    async fn good_decryption() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // fake read response from downstairs that will successfully decrypt
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, hash) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data)
            .unwrap();

        let responses = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: BytesMut::from(&data[..]),
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    },
                ),
                hash,
            }],
        }]);

        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(0),
            action: ClientAction::Response(Message::ReadResponse {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: JobId(1000),
                responses,
            }),
        }))
        .await;
        // no panic, great work everyone
    }

    #[tokio::test]
    async fn bad_decryption_means_panic() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // fake read response from downstairs that will fail decryption
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data)
            .unwrap();

        let nonce: [u8; 12] = nonce.into();
        let mut tag: [u8; 16] = tag.into();

        // alter tag
        if tag[3] == 0xFF {
            tag[3] = 0x00;
        } else {
            tag[3] = 0xFF;
        }

        // compute integrity hash after alteration above! It should still
        // validate
        let hash = integrity_hash(&[&nonce, &tag, &data]);

        let responses = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: BytesMut::from(&data[..]),
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext { nonce, tag },
                ),
                hash,
            }],
        }]);

        // Prepare to receive the message with an invalid tag
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(0),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses,
                }),
            }));

        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(
            r.contains("DecryptionError"),
            "panic for the wrong reason: {r}"
        );
    }

    /// Confirms that the encrypted read hash checksum works
    #[tokio::test]
    async fn bad_hash_on_encrypted_read_panic() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // fake read response from downstairs that will fail integrity hash
        // check
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data)
            .unwrap();

        let nonce: [u8; 12] = nonce.into();
        let tag: [u8; 16] = tag.into();

        let responses = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: BytesMut::from(&data[..]),
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext { nonce, tag },
                ),
                hash: 10000, // junk hash
            }],
        }]);

        // Prepare to receive the message with a junk hash
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(0),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses,
                }),
            }));

        // Don't use `should_panic`, as the `unwrap` above could cause this test
        // to pass for the wrong reason.
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(r.contains("HashMismatch"));
    }

    #[tokio::test]
    async fn bad_read_hash_makes_panic() {
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // fake read response from downstairs that will fail integrity hash
        // check
        let responses = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: BytesMut::from([1u8; 512].as_slice()),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash: 10000, // junk hash
            }],
        }]);

        // Prepare to handle the response with a junk hash
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(0),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses,
                }),
            }));

        // Don't use `should_panic`, as the `unwrap` above could cause this test
        // to pass for the wrong reason.
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(r.contains("HashMismatch"));
    }

    #[tokio::test]
    async fn work_read_hash_mismatch() {
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        let data = BytesMut::from([1u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: JobId(1000),
                responses: r1,
            }),
        }))
        .await;

        // Send back a second response with different data and a hash that (1)
        // is correct for that data, but (2) does not match the original hash.
        //
        // This distinguishes between a regular hash failure and a hash mismatch
        // between multiple ReadResponse
        let data = BytesMut::from([2u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r2 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r2,
                }),
            }));
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch")); // not the usual mismatch error
        assert!(r.contains("read hash mismatch"));
    }

    #[tokio::test]
    async fn work_read_hash_mismatch_third() {
        // Test that a hash mismatch on the third response will trigger a panic.
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        for client_id in [ClientId::new(0), ClientId::new(1)] {
            let data = BytesMut::from([1u8; 512].as_slice());
            let hash = integrity_hash(&[&data]);
            let r = Ok(vec![ReadResponse {
                eid: 0,
                offset,

                data: data.clone(),
                block_contexts: vec![BlockContext {
                    encryption_context: None,
                    hash,
                }],
            }]);
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r,
                }),
            }))
            .await;
        }

        // Send back a second response with different data and a hash that (1)
        // is correct for that data, but (2) does not match the original hash.
        //
        // This distinguishes between a regular hash failure and a hash mismatch
        // between multiple ReadResponse
        let data = BytesMut::from([2u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r,
                }),
            }));
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch")); // not the usual mismatch error
        assert!(r.contains("read hash mismatch"));
    }

    #[tokio::test]
    async fn work_read_hash_inside() {
        // Test that a hash length mismatch will panic
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        let data = BytesMut::from([1u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: JobId(1000),
                responses: r1,
            }),
        }))
        .await;

        // Send back a second response with more data (2 blocks instead of 1);
        // the first block matches.
        let data = BytesMut::from([1u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let response = ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };
        let r2 = Ok(vec![response.clone(), response.clone()]);
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r2,
                }),
            }));
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }

    #[tokio::test]
    async fn work_read_hash_mismatch_no_data() {
        // Test that empty data first, then data later will trigger
        // hash mismatch panic.
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // The first read has no block contexts, because it was unwritten
        let data = BytesMut::from([0u8; 512].as_slice());
        let r1 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![],
        }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: JobId(1000),
                responses: r1,
            }),
        }))
        .await;

        // Send back a second response with actual block contexts (oh no!)
        let hash = integrity_hash(&[&data]);
        let r2 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r2,
                }),
            }));
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }

    #[tokio::test]
    async fn work_read_hash_mismatch_no_data_next() {
        // Test that missing data on the 2nd read response will panic
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(512);
        let offset = Block::new_512(7);
        let (_tx, res) = BlockReqWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockReq {
            op: BlockOp::Read { offset, data },
            res,
        }))
        .await;

        // The first read has no block contexts, because it was unwritten
        let data = BytesMut::from([0u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                upstairs_id: up.cfg.upstairs_id,
                session_id: up.cfg.session_id,
                job_id: JobId(1000),
                responses: r1,
            }),
        }))
        .await;

        // Send back a second response with no actual data (oh no!)
        let r2 = Ok(vec![ReadResponse {
            eid: 0,
            offset,

            data: data.clone(),
            block_contexts: vec![
                // No block contexts!
            ],
        }]);
        let fut =
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    responses: r2,
                }),
            }));
        let result = std::panic::AssertUnwindSafe(fut).catch_unwind().await;

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }
}
