// Copyright 2023 Oxide Computer Company
//! Data structures specific to Crucible's `struct Upstairs`
use crate::{
    cdt,
    client::{ClientAction, ClientRunResult},
    control::ControlRequest,
    deadline_secs,
    deferred::{
        DeferredBlockOp, DeferredMessage, DeferredQueue, DeferredRead,
        DeferredWrite, EncryptedWrite, WriteRes,
    },
    downstairs::{Downstairs, DownstairsAction},
    extent_from_offset,
    guest::GuestBlockRes,
    stats::UpStatOuter,
    BlockOp, BlockRes, Buffer, ClientId, ClientMap, CrucibleOpts, DsState,
    EncryptionContext, GuestIoHandle, Message, RegionDefinition,
    RegionDefinitionStatus, SnapshotDetails, WQCounts,
};
use crucible_common::{BlockIndex, CrucibleError};
use serde::{Deserialize, Serialize};

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytes::BytesMut;
use futures::future::{pending, Either};
use slog::{debug, error, info, o, warn, Logger};
use tokio::{
    sync::mpsc,
    time::{sleep_until, Instant},
};
use uuid::Uuid;

/// How often to log stats for DTrace
const STAT_INTERVAL_SECS: f32 = 1.0;

/// Minimum IO size (in bytes) before encryption / decryption is done off-thread
const MIN_DEFER_SIZE_BYTES: u64 = 8192;

/// Number of threads to dedicate to encryption / decryption
///
/// This number is picked somewhat arbitrarily by eyeballing flamegraphs until
/// `WorkerThread::wait_until_cold` looks like a reasonable fraction of CPU
/// time.  Rayon's default is "number of CPU threads", which is dramatic
/// overkill on a 128-thread system; in such a system, we see a ton of CPU time
/// being spent spinning in `wait_until_cold`.
const WORKER_POOL_SIZE: usize = 8;

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

/// Crucible upstairs counters
///
/// Counters indicating the upstairs selects path.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct UpCounters {
    apply: u64,
    action_downstairs: u64,
    action_guest: u64,
    action_deferred_block: u64,
    action_deferred_message: u64,
    action_leak_check: u64,
    action_flush_check: u64,
    action_stat_check: u64,
    action_repair_check: u64,
    action_control_check: u64,
    action_noop: u64,
}

impl UpCounters {
    fn new() -> UpCounters {
        UpCounters {
            apply: 0,
            action_downstairs: 0,
            action_guest: 0,
            action_deferred_block: 0,
            action_deferred_message: 0,
            action_leak_check: 0,
            action_flush_check: 0,
            action_stat_check: 0,
            action_repair_check: 0,
            action_control_check: 0,
            action_noop: 0,
        }
    }
}
/// Crucible upstairs state
///
/// This `struct` has exclusive ownership over (almost) everything that's needed
/// to run the Crucible upstairs, and a handle to the incoming `Guest` queues
/// (which is our main source of operations).
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
/// - [`BlockOp`] from the guest
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
/// - Send all pending IO to the client work tasks
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
    pub(crate) guest: GuestIoHandle,

    /// Set to `true` when we first notice the `Guest` has been dropped
    ///
    /// The `Guest` being dropped is indicated when the [`GuestIoHandle`]
    /// receives `None` from its `req_rx` receiver.
    guest_dropped: bool,

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
    /// Some internal counters
    pub(crate) counters: UpCounters,

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
    deferred_ops: DeferredQueue<DeferredBlockOp>,

    /// Stream of decrypted `Message` futures
    deferred_msgs: DeferredQueue<DeferredMessage>,

    /// Thread pool for doing heavy CPU work outside the Tokio runtime
    pool: rayon::ThreadPool,
}

/// Action to be taken which modifies the [`Upstairs`] state
#[derive(Debug)]
pub(crate) enum UpstairsAction {
    Downstairs(DownstairsAction),
    Guest(BlockOp),

    /// A deferred block request has completed
    DeferredBlockOp(DeferredBlockOp),

    /// A deferred message has arrived
    DeferredMessage(DeferredMessage),

    LeakCheck,
    FlushCheck,
    StatUpdate,
    RepairCheck,
    Control(ControlRequest),

    /// The guest connection has been dropped
    GuestDropped,

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
        guest: GuestIoHandle,
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
        let counters = UpCounters::new();

        let rd_status = match expected_region_def {
            None => RegionDefinitionStatus::WaitingForDownstairs,
            Some(d) => RegionDefinitionStatus::ExpectingFromDownstairs(d),
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(WORKER_POOL_SIZE)
            .build()
            .expect("failed to build rayon thread pool");

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
        let mut downstairs = Downstairs::new(
            cfg.clone(),
            ds_target,
            tls_context,
            log.new(o!("" => "downstairs")),
        );
        let flush_timeout_secs = opt.flush_timeout.unwrap_or(0.5);
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(500);

        if let Some(ddef) = expected_region_def {
            downstairs.set_ddef(ddef);
        }

        Upstairs {
            state: UpstairsState::Initializing,
            cfg,
            repair_check_interval: None,
            leak_deadline: deadline_secs(1.0),
            flush_deadline: deadline_secs(flush_timeout_secs),
            stat_deadline: deadline_secs(STAT_INTERVAL_SECS),
            flush_timeout_secs,
            guest,
            guest_dropped: false,
            ddef: rd_status,
            need_flush: false,
            stats,
            counters,
            log,
            downstairs,
            control_rx,
            control_tx,
            deferred_ops: DeferredQueue::new(),
            deferred_msgs: DeferredQueue::new(),
            pool,
        }
    }

    #[cfg(test)]
    pub(crate) fn disable_client_backpressure(&mut self) {
        self.downstairs.disable_client_backpressure();
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
        let (_guest, io) = crate::guest::Guest::new(Some(log.clone()));

        Self::new(&opts, 0, ddef, io, None)
    }

    /// Runs the upstairs (forever)
    pub(crate) async fn run(&mut self) {
        while !self.done() {
            let action = self.select().await;
            self.counters.apply += 1;
            cdt::up__apply!(|| (self.counters.apply));
            self.apply(action)
        }
    }

    /// Returns `true` if the worker thread can stop
    ///
    /// This is only true if the `Guest` handle has been dropped and all
    /// remaining messages have been processed.
    fn done(&self) -> bool {
        self.guest_dropped
            && self.guest.guest_work.is_empty()
            && self.downstairs.ds_active.is_empty()
            && self.deferred_ops.is_empty()
            && self.deferred_msgs.is_empty()
    }

    /// Select an event from possible actions
    async fn select(&mut self) -> UpstairsAction {
        tokio::select! {
            d = self.downstairs.select() => {
                UpstairsAction::Downstairs(d)
            }
            d = self.guest.recv(), if !self.guest_dropped => {
                d
            }
            _ = self.repair_check_interval
                .map(|r| Either::Left(sleep_until(r)))
                .unwrap_or(Either::Right(pending()))
            => {
                UpstairsAction::RepairCheck
            }
            d = self.deferred_ops.next(), if !self.deferred_ops.is_empty()
            => {
                match d {
                    // Normal operation: the deferred task gave us back a
                    // DeferredBlockOp, which we need to handle.
                    Some(d) => UpstairsAction::DeferredBlockOp(d),

                    // The outer Option is None if the FuturesOrdered is empty
                    None => {
                        // Calling `deferred_ops.next()` on an empty queue must
                        // set the flag marking the deferred futures list as
                        // empty; assert that here as a sanity check.
                        assert!(self.deferred_ops.is_empty());
                        UpstairsAction::NoOp
                    }
                }
            }
            m = self.deferred_msgs.next(), if !self.deferred_msgs.is_empty()
            => {
                // The outer Option is None if the queue is empty.  If this is
                // the case, then we check that the empty flag was set.
                let Some(m) = m else {
                    assert!(self.deferred_msgs.is_empty());
                    return UpstairsAction::NoOp;
                };
                UpstairsAction::DeferredMessage(m)
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
    pub(crate) fn apply(&mut self, action: UpstairsAction) {
        match action {
            UpstairsAction::Downstairs(d) => {
                self.counters.action_downstairs += 1;
                cdt::up__action_downstairs!(|| (self
                    .counters
                    .action_downstairs));
                self.apply_downstairs_action(d)
            }
            UpstairsAction::Guest(b) => {
                self.counters.action_guest += 1;
                cdt::up__action_guest!(|| (self.counters.action_guest));
                self.defer_guest_request(b);
            }
            UpstairsAction::GuestDropped => {
                self.guest_dropped = true;
            }
            UpstairsAction::DeferredBlockOp(req) => {
                self.counters.action_deferred_block += 1;
                cdt::up__action_deferred_block!(|| (self
                    .counters
                    .action_deferred_block));
                self.apply_guest_request(req);
            }
            UpstairsAction::DeferredMessage(m) => {
                self.counters.action_deferred_message += 1;
                cdt::up__action_deferred_message!(|| (self
                    .counters
                    .action_deferred_message));
                self.on_client_message(m);
            }
            UpstairsAction::LeakCheck => {
                self.counters.action_leak_check += 1;
                cdt::up__action_leak_check!(|| (self
                    .counters
                    .action_leak_check));
                const LEAK_MS: usize = 1000;
                self.guest.leak_check(LEAK_MS);
                let leak_tick =
                    tokio::time::Duration::from_millis(LEAK_MS as u64);
                self.leak_deadline =
                    Instant::now().checked_add(leak_tick).unwrap();
            }
            UpstairsAction::FlushCheck => {
                self.counters.action_flush_check += 1;
                cdt::up__action_flush_check!(|| (self
                    .counters
                    .action_flush_check));
                if self.need_flush {
                    self.submit_flush(None, None);
                }
                self.flush_deadline = deadline_secs(self.flush_timeout_secs);
            }
            UpstairsAction::StatUpdate => {
                self.counters.action_stat_check += 1;
                cdt::up__action_stat_check!(|| (self
                    .counters
                    .action_stat_check));
                self.on_stat_update();
                self.stat_deadline = deadline_secs(STAT_INTERVAL_SECS);
            }
            UpstairsAction::RepairCheck => {
                self.counters.action_repair_check += 1;
                cdt::up__action_repair_check!(|| (self
                    .counters
                    .action_repair_check));
                self.on_repair_check();
            }
            UpstairsAction::Control(c) => {
                self.counters.action_control_check += 1;
                cdt::up__action_control_check!(|| (self
                    .counters
                    .action_control_check));
                self.on_control_req(c);
            }
            UpstairsAction::NoOp => {
                self.counters.action_noop += 1;
                cdt::up__action_noop!(|| (self.counters.action_noop));
            }
        }

        // Check whether we need to mark an offline Downstairs as faulted
        // because too many jobs have piled up.
        self.gone_too_long();

        // Check to see whether live-repair can continue
        //
        // This must be called before acking jobs, because it looks in
        // `Downstairs::ackable_jobs` to see which jobs are done.
        if let Some(job_id) = self.downstairs.check_live_repair() {
            self.downstairs.continue_live_repair(
                job_id,
                &mut self.guest.guest_work,
                &self.state,
            );
        }

        // Send jobs downstairs as they become available.  This must be called
        // after `continue_live_repair`, which may enqueue jobs.
        for i in ClientId::iter() {
            if self.downstairs.clients[i].should_do_more_work() {
                let ddef = self.ddef.get_def().unwrap();
                self.downstairs.io_send(i, &ddef);
            }
        }

        // Handle any jobs that have become ready for acks
        if self.downstairs.has_ackable_jobs() {
            self.downstairs
                .ack_jobs(&mut self.guest.guest_work, &self.stats)
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
                res.send_ok(());
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
    async fn await_deferred_ops(&mut self) {
        while let Some(req) = self.deferred_ops.next().await {
            self.apply(UpstairsAction::DeferredBlockOp(req));
        }
        assert!(self.deferred_ops.is_empty());
    }

    /// Helper function to await all deferred messages
    ///
    /// This is only useful in tests because it **only** processes deferred
    /// messages (doing no other Upstairs work).  In production, there
    /// could be other events that need handling simultaneously, so we do not
    /// want to stall the Upstairs.
    #[cfg(test)]
    async fn await_deferred_msgs(&mut self) {
        while let Some(msg) = self.deferred_msgs.next().await {
            self.apply(UpstairsAction::DeferredMessage(msg));
        }
        assert!(self.deferred_msgs.is_empty());
    }

    /// Check outstanding IOops for each downstairs.
    ///
    /// We never kick out a Downstairs that is replying to us, but will
    /// eventually transition a Downstairs from Offline to Faulted (which then
    /// leads to scrubbing any outstanding jobs, and restarting the client IO
    /// task).
    fn gone_too_long(&mut self) {
        // If we are not active, then just exit.
        if !matches!(self.state, UpstairsState::Active) {
            return;
        }

        for cid in ClientId::iter() {
            if self.downstairs.clients[cid].state() == DsState::Offline {
                self.downstairs.check_gone_too_long(cid, &self.state);
            }
        }
    }

    /// Fires the `up-status` DTrace probe
    fn on_stat_update(&self) {
        cdt::up__status!(|| {
            let arg = Arg {
                session_id: self.cfg.session_id.to_string(),
                up_count: self.guest.guest_work.len() as u32,
                up_counters: self.counters,
                next_job_id: self.downstairs.peek_next_id(),
                up_backpressure: self.guest.get_backpressure().as_micros(),
                write_bytes_out: self.downstairs.write_bytes_outstanding(),
                ds_count: self.downstairs.active_count() as u32,
                ds_state: self.downstairs.collect_stats(|c| c.state()),
                ds_io_count: self.downstairs.io_state_count(),
                ds_reconciled: self.downstairs.reconcile_repaired(),
                ds_reconcile_needed: self.downstairs.reconcile_repair_needed(),
                ds_reconcile_aborted: self
                    .downstairs
                    .reconcile_repair_aborted(),
                ds_live_repair_completed: self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_completed),
                ds_live_repair_aborted: self
                    .downstairs
                    .collect_stats(|c| c.stats.live_repair_aborted),
                ds_connected: self
                    .downstairs
                    .collect_stats(|c| c.stats.connected),
                ds_replaced: self
                    .downstairs
                    .collect_stats(|c| c.stats.replaced),
                ds_extents_repaired: self
                    .downstairs
                    .collect_stats(|c| c.stats.extents_repaired),
                ds_extents_confirmed: self
                    .downstairs
                    .collect_stats(|c| c.stats.extents_confirmed),
                ds_extent_limit: self
                    .downstairs
                    .active_repair_extent()
                    .map(|v| v.0 as usize)
                    .unwrap_or(0),
                ds_delay_us: self
                    .downstairs
                    .collect_stats(|c| c.get_delay_us() as usize),
                ds_ro_lr_skipped: self
                    .downstairs
                    .collect_stats(|c| c.stats.ro_lr_skipped),
            };
            ("stats", arg)
        });
    }

    /// Handles a request from the (optional) control server
    fn on_control_req(&self, c: ControlRequest) {
        match c {
            ControlRequest::UpstairsStats(tx) => {
                let ds_state = self.downstairs.collect_stats(|c| c.state());
                let up_jobs = self.guest.guest_work.len();
                let ds_jobs = self.downstairs.active_count();
                let reconcile_done = self.downstairs.reconcile_repaired();
                let reconcile_needed =
                    self.downstairs.reconcile_repair_needed();
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
                                .map(|v| v.0 as usize)
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
                    reconcile_done,
                    reconcile_needed,
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
    pub(crate) fn on_repair_check(&mut self) {
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
            &mut self.guest.guest_work,
            self.ddef.get_def().unwrap().extent_count(),
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

    /// When a `BlockOp` arrives, defer it as a future
    fn defer_guest_request(&mut self, op: BlockOp) {
        match op {
            // All Write operations are deferred, because they will offload
            // encryption to a separate thread pool.
            BlockOp::Write { offset, data, done } => {
                self.submit_deferred_write(offset, data, done, false);
            }
            BlockOp::WriteUnwritten { offset, data, done } => {
                self.submit_deferred_write(offset, data, done, true);
            }
            // If we have any deferred requests in the FuturesOrdered, then we
            // have to keep using it for subsequent requests (even ones that are
            // not writes) to preserve FIFO ordering
            _ if !self.deferred_ops.is_empty() => {
                self.deferred_ops.push_immediate(DeferredBlockOp::Other(op));
            }
            // Otherwise, we can apply a non-write operation immediately, saving
            // a trip through the DeferredQueue
            _ => {
                self.apply_guest_request_inner(op);
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
    fn apply_guest_request(&mut self, op: DeferredBlockOp) {
        match op {
            DeferredBlockOp::Write(op) => self.submit_write(op),
            DeferredBlockOp::Other(op) => self.apply_guest_request_inner(op),
        }
    }

    /// Does the actual work for a (non-write) guest request
    ///
    /// # Panics
    /// This function assumes that `BlockOp::Write` and
    /// `BlockOp::WriteUnwritten` are always deferred and handled separately;
    /// it will panic if `req` matches either of them.
    fn apply_guest_request_inner(&mut self, op: BlockOp) {
        // If any of the submit_* functions fail to send to the downstairs, they
        // return an error.  These are reported to the Guest.
        match op {
            // These three options can be handled by this task directly,
            // and don't require the upstairs to be fully online.
            BlockOp::GoActive { done } => {
                self.set_active_request(done);
            }
            BlockOp::GoActiveWithGen { gen, done } => {
                // We allow this if we are not active yet, or we are active
                // with the requested generation number.
                match &self.state {
                    UpstairsState::Active | UpstairsState::GoActive(..) => {
                        if self.cfg.generation() == gen {
                            // Okay, we want to activate with what we already
                            // have, that's valid., let the set_active_request
                            // handle things.
                            self.set_active_request(done);
                        } else {
                            // Gen's don't match, but we are already active,
                            // or in progress to activate, so fail this request.
                            done.send_err(
                                CrucibleError::GenerationNumberInvalid,
                            );
                        }
                    }
                    UpstairsState::Deactivating(..) => {
                        // Don't update gen, return error
                        done.send_err(CrucibleError::UpstairsDeactivating);
                    }
                    UpstairsState::Initializing => {
                        // This case, we update our generation and then
                        // let set_active_request handle the rest.
                        self.cfg.generation.store(gen, Ordering::Release);
                        self.set_active_request(done);
                    }
                }
            }
            BlockOp::QueryGuestIOReady { done } => {
                done.send_ok(self.guest_io_ready());
            }
            BlockOp::QueryUpstairsUuid { done } => {
                done.send_ok(self.cfg.upstairs_id);
            }
            BlockOp::Deactivate { done } => {
                self.set_deactivate(done);
            }

            // Query ops
            BlockOp::QueryBlockSize { done } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        done.send_ok(rd.block_size());
                    }
                    None => {
                        warn!(
                            self.log,
                            "Block size not available (active: {})",
                            self.guest_io_ready()
                        );
                        done.send_err(CrucibleError::PropertyNotAvailable(
                            "block size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryTotalSize { done } => {
                match self.ddef.get_def() {
                    Some(rd) => {
                        done.send_ok(rd.total_size());
                    }
                    None => {
                        warn!(
                            self.log,
                            "Total size not available (active: {})",
                            self.guest_io_ready()
                        );
                        done.send_err(CrucibleError::PropertyNotAvailable(
                            "total size".to_string(),
                        ));
                    }
                };
            }
            // Testing options
            BlockOp::QueryExtentSize { done } => {
                // Yes, test only
                match self.ddef.get_def() {
                    Some(rd) => {
                        done.send_ok(rd.extent_size());
                    }
                    None => {
                        warn!(
                            self.log,
                            "Extent size not available (active: {})",
                            self.guest_io_ready()
                        );
                        done.send_err(CrucibleError::PropertyNotAvailable(
                            "extent size".to_string(),
                        ));
                    }
                };
            }
            BlockOp::QueryWorkQueue { done } => {
                // TODO should this first check if the Upstairs is active?
                let active_count = self
                    .downstairs
                    .clients
                    .iter()
                    .filter(|c| c.state() == DsState::Active)
                    .count();
                done.send_ok(WQCounts {
                    up_count: self.guest.guest_work.len(),
                    ds_count: self.downstairs.active_count(),
                    active_count,
                });
            }

            BlockOp::ShowWork { done } => {
                // TODO should this first check if the Upstairs is active?
                done.send_ok(self.show_all_work());
            }

            BlockOp::Read { offset, data, done } => {
                self.submit_read(offset, data, done)
            }
            BlockOp::Write { .. } | BlockOp::WriteUnwritten { .. } => {
                panic!("writes must always be deferred")
            }
            BlockOp::Flush {
                snapshot_details,
                done,
            } => {
                /*
                 * Submit for read and write both check if the upstairs is
                 * ready for guest IO or not.  Because the Upstairs itself can
                 * call submit_flush, we have to check here that it is okay
                 * to accept IO from the guest before calling a guest requested
                 * flush command.
                 */
                if !self.guest_io_ready() {
                    done.send_err(CrucibleError::UpstairsInactive);
                    return;
                }
                self.submit_flush(Some(done), snapshot_details);
            }
            BlockOp::ReplaceDownstairs { id, old, new, done } => {
                let r = self.downstairs.replace(id, old, new, &self.state);
                done.send_result(r);
            }

            #[cfg(test)]
            BlockOp::GetDownstairsState { done } => {
                let mut out = crate::ClientData::new(DsState::New);
                for i in ClientId::iter() {
                    out[i] = self.downstairs.clients[i].state();
                }
                done.send_ok(out);
            }

            #[cfg(test)]
            BlockOp::FaultDownstairs { client_id, done } => {
                self.downstairs.skip_all_jobs(client_id);
                self.downstairs.clients[client_id].fault(
                    &self.state,
                    crate::client::ClientStopReason::RequestedFault,
                );
                done.send_ok(());
            }
        }
    }

    pub(crate) fn show_all_work(&self) -> WQCounts {
        let gior = self.guest_io_ready();
        let up_count = self.guest.active_count();

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
                self.guest.show_work();
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

        self.guest.guest_work.print_last_completed(5);
        println!();

        WQCounts {
            up_count,
            ds_count,
            active_count,
        }
    }

    /// Request that the Upstairs go active
    fn set_active_request(&mut self, res: BlockRes) {
        match &self.state {
            UpstairsState::Initializing => {
                self.state = UpstairsState::GoActive(res);
                info!(self.log, "{} active request set", self.cfg.upstairs_id);
            }
            UpstairsState::GoActive(..) => {
                // We have already been sent a request to go active, but we
                // are not active yet and will respond (on the original
                // BlockRes) when we do become active.
                info!(
                    self.log,
                    "{} request to activate upstairs already going active",
                    self.cfg.upstairs_id
                );
                res.send_err(CrucibleError::UpstairsActivateInProgress);
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
                // We are already active, so go ahead and respond again.
                info!(
                    self.log,
                    "{} Request to activate upstairs already active",
                    self.cfg.upstairs_id
                );
                res.send_ok(());
                return;
            }
        }
        // Notify all clients that they should go active when they hit an
        // appropriate state in their negotiation.
        for c in self.downstairs.clients.iter_mut() {
            c.set_active_request();
        }
    }

    /// Request that the Upstairs deactivate
    ///
    /// This will return immediately if all of the Downstairs clients are done;
    /// otherwise, it will schedule a final flush that triggers deactivation
    /// when complete.
    ///
    /// In either case, `self.state` is set to `UpstairsState::Deactivating`
    fn set_deactivate(&mut self, res: BlockRes) {
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
            self.submit_flush(None, None);
        } else {
            debug!(self.log, "ready to deactivate right away");
            // Deactivation is handled in the invariant-checking portion of
            // Upstairs::apply.
        }

        self.state = UpstairsState::Deactivating(res);
    }

    pub(crate) fn submit_flush(
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
        let (gw_id, _) = self.guest.guest_work.submit_job(
            |gw_id| {
                cdt::gw__flush__start!(|| (gw_id.0));
                if snapshot_details.is_some() {
                    info!(self.log, "flush with snap requested");
                }
                self.downstairs.submit_flush(gw_id, snapshot_details)
            },
            res.map(GuestBlockRes::Other),
        );

        cdt::up__to__ds__flush__start!(|| (gw_id.0));
    }

    /// Submits a read job to the downstairs
    fn submit_read(
        &mut self,
        offset: BlockIndex,
        data: Buffer,
        res: BlockRes<Buffer, (Buffer, CrucibleError)>,
    ) {
        self.submit_read_inner(offset, data, Some(res))
    }

    /// Submits a dummy read (without associated `BlockOp`)
    #[cfg(test)]
    pub(crate) fn submit_dummy_read(
        &mut self,
        offset: BlockIndex,
        data: Buffer,
    ) {
        self.submit_read_inner(offset, data, None)
    }

    /// Submit a read job to the downstairs, optionally without a `BlockOp`
    ///
    /// # Panics
    /// If `res` is `None` and this isn't the test suite
    fn submit_read_inner(
        &mut self,
        offset: BlockIndex,
        data: Buffer,
        res: Option<BlockRes<Buffer, (Buffer, CrucibleError)>>,
    ) {
        #[cfg(not(test))]
        assert!(res.is_some());

        if !self.guest_io_ready() {
            if let Some(res) = res {
                res.send_err((data, CrucibleError::UpstairsInactive));
            }
            return;
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let ddef = self.ddef.get_def().unwrap();

        /*
         * Verify IO is in range for our region
         */
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            if let Some(res) = res {
                res.send_err((data, e));
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
            ddef.bytes_to_blocks(data.len()),
        );

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let (gw_id, _) = self.guest.guest_work.submit_job(
            |gw_id| {
                cdt::gw__read__start!(|| (gw_id.0));
                self.downstairs.submit_read(gw_id, impacted_blocks)
            },
            res.map(|res| GuestBlockRes::Read(data, res)),
        );

        cdt::up__to__ds__read__start!(|| (gw_id.0));
    }

    /// Submits a dummy write (without an associated `BlockOp`)
    ///
    /// This **does not** go through the deferred-write pipeline
    #[cfg(test)]
    pub(crate) fn submit_dummy_write(
        &mut self,
        offset: BlockIndex,
        data: BytesMut,
        is_write_unwritten: bool,
    ) {
        if let Some(w) = self.compute_deferred_write(
            offset,
            data,
            BlockRes::dummy(),
            is_write_unwritten,
        ) {
            self.submit_write(DeferredWrite::run(w))
        }
    }

    /// Submits a new write job to the upstairs
    ///
    /// This function **defers** the write job submission, because writes
    /// require encrypting data (which is expensive) and we'd like to return as
    /// quickly as possible.
    fn submit_deferred_write(
        &mut self,
        offset: BlockIndex,
        data: BytesMut,
        res: BlockRes,
        is_write_unwritten: bool,
    ) {
        // It's possible for the write to be invalid out of the gate, in which
        // case `compute_deferred_write` replies to the `res` itself and returns
        // `None`.  Otherwise, we have to store a future to process the write
        // result.
        if let Some(w) =
            self.compute_deferred_write(offset, data, res, is_write_unwritten)
        {
            let should_defer = !self.deferred_ops.is_empty()
                || w.data.len() > MIN_DEFER_SIZE_BYTES as usize;
            if should_defer {
                let tx = self.deferred_ops.push_oneshot();
                self.pool.spawn(move || {
                    let out = DeferredBlockOp::Write(w.run());
                    let _ = tx.send(out);
                });
            } else {
                let out = DeferredBlockOp::Write(w.run());
                self.apply_guest_request(out);
            }
        }
    }

    fn compute_deferred_write(
        &mut self,
        offset: BlockIndex,
        data: BytesMut,
        res: BlockRes,
        is_write_unwritten: bool,
    ) -> Option<DeferredWrite> {
        if !self.guest_io_ready() {
            res.send_err(CrucibleError::UpstairsInactive);
            return None;
        }
        if self.cfg.read_only {
            res.send_err(CrucibleError::ModifyingReadOnlyRegion);
            return None;
        }

        /*
         * Verify IO is in range for our region
         */
        let ddef = self.ddef.get_def().unwrap();
        if let Err(e) = ddef.validate_io(offset, data.len()) {
            res.send_err(e);
            return None;
        }

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span two extents.
         */
        let impacted_blocks =
            extent_from_offset(&ddef, offset, ddef.bytes_to_blocks(data.len()));

        let guard = self.downstairs.early_write_backpressure(data.len() as u64);

        // Fast-ack, pretending to be done immediately for Write operations
        let res = if is_write_unwritten {
            WriteRes::WriteUnwritten(res)
        } else {
            res.send_ok(());
            WriteRes::Write
        };

        Some(DeferredWrite {
            ddef,
            impacted_blocks,
            data,
            res,
            cfg: self.cfg.clone(),
            guard,
        })
    }

    fn submit_write(&mut self, write: EncryptedWrite) {
        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        self.need_flush = true;

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let is_write_unwritten = write.is_write_unwritten();
        let (gw_id, _) = self.guest.guest_work.submit_job(
            |gw_id| {
                if is_write_unwritten {
                    cdt::gw__write__unwritten__start!(|| (gw_id.0));
                } else {
                    cdt::gw__write__start!(|| (gw_id.0));
                }
                self.downstairs.submit_write(
                    gw_id,
                    write.impacted_blocks,
                    write.data,
                    is_write_unwritten,
                    write.guard,
                )
            },
            Some(match write.res {
                WriteRes::Write => GuestBlockRes::Acked,
                WriteRes::WriteUnwritten(res) => GuestBlockRes::Other(res),
            }),
        );

        if is_write_unwritten {
            cdt::up__to__ds__write__unwritten__start!(|| (gw_id.0));
        } else {
            cdt::up__to__ds__write__start!(|| (gw_id.0));
        }
    }

    /// React to an event sent by one of the downstairs clients
    fn apply_downstairs_action(&mut self, d: DownstairsAction) {
        match d {
            DownstairsAction::Client { client_id, action } => {
                self.apply_client_action(client_id, action);
            }
        }
    }

    /// React to an event sent by one of the downstairs clients
    fn apply_client_action(
        &mut self,
        client_id: ClientId,
        action: ClientAction,
    ) {
        match action {
            ClientAction::Connected => {
                self.downstairs.clients[client_id].stats.connected += 1;
                self.downstairs.clients[client_id].send_here_i_am();
            }
            ClientAction::Response(m) => {
                // We would not have received ClientAction::Response if the IO
                // task was not still running, so it's safe to unwrap this.
                let id = self.downstairs.clients[client_id]
                    .get_connection_id()
                    .expect("io task must be running");

                // Defer the message if it's a (large) read that needs
                // decryption, or there are other deferred messages in the queue
                // (to preserve order).  Otherwise, handle it immediately.
                if let Message::ReadResponse { header, .. } = &m {
                    // Any read larger than `MIN_DEFER_SIZE_BYTES` constant
                    // should be deferred to the worker pool; smaller reads can
                    // be processed in-thread (since the overhead isn't worth
                    // it)
                    let should_defer = !self.deferred_msgs.is_empty()
                        || match &header.blocks {
                            Ok(rs) => {
                                // Find the number of bytes being decrypted
                                let response_size = rs.len() as u64
                                    * self
                                        .ddef
                                        .get_def()
                                        .map(|b| b.block_size())
                                        .unwrap_or(0);

                                response_size > MIN_DEFER_SIZE_BYTES
                            }
                            Err(_) => false,
                        };

                    let dr = DeferredRead {
                        message: m,
                        client_id,
                        connection_id: id,
                        cfg: self.cfg.clone(),
                        log: self.log.new(o!("job" => "decrypt")),
                    };
                    if should_defer {
                        let tx = self.deferred_msgs.push_oneshot();
                        self.pool.spawn(move || {
                            let out = dr.run();
                            let _ = tx.send(out);
                        });
                    } else {
                        // Do decryption right here!
                        self.on_client_message(dr.run());
                    }
                } else {
                    let dm = DeferredMessage {
                        message: m,
                        hashes: vec![],
                        client_id,
                        connection_id: id,
                    };
                    if self.deferred_msgs.is_empty() {
                        self.on_client_message(dm);
                    } else {
                        self.deferred_msgs.push_immediate(dm);
                    }
                }
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

    fn on_client_message(&mut self, dm: DeferredMessage) {
        let (client_id, m, hashes) = (dm.client_id, dm.message, dm.hashes);

        // It's possible for a deferred message to arrive **after** we have
        // disconnected from this particular Downstairs.  In that case, we want
        // to ignore the message, because we've already marked it as Skipped or
        // re-sent it (marking it as New).
        if self.downstairs.clients[client_id].get_connection_id()
            != Some(dm.connection_id)
        {
            warn!(
                self.log,
                "ignoring deferred message with out-dated connection id"
            );
            return;
        }

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
                    hashes,
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
                    .continue_negotiation(m, &self.state, &mut self.ddef);

                match r {
                    // continue_negotiation returns an error if the upstairs
                    // should go inactive!
                    Err(e) => self.set_inactive(e),
                    Ok(false) => (),
                    Ok(true) => {
                        // Copy the region definition into the Downstairs
                        self.downstairs.set_ddef(self.ddef.get_def().unwrap());

                        // Negotiation succeeded for this Downstairs, let's see
                        // what we can do from here
                        match self.downstairs.clients[client_id].state() {
                            DsState::Active => (),

                            DsState::WaitQuorum => {
                                // See if we have a quorum
                                if self.connect_region_set() {
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
                if self.downstairs.on_reconciliation_ack(
                    client_id,
                    m,
                    &self.state,
                ) {
                    // reconciliation is done, great work everyone
                    self.on_reconciliation_done(DsState::Reconcile);
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
    fn connect_region_set(&mut self) -> bool {
        /*
         * If reconciliation is required, it happens in three phases.
         * Typically an interruption of reconciliation will result in things
         * starting over, but if actual repair work to an extent is
         * completed, that extent won't need to be repaired again.
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
                info!(self.log, "Waiting for more clients to be ready");
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
                self.downstairs.send_next_reconciliation_req();
                true
            }
            Ok(false) => {
                info!(self.log, "No downstairs reconciliation required");
                self.on_reconciliation_done(DsState::WaitQuorum);
                info!(self.log, "Set Active after no reconciliation");
                true
            }
        }
    }

    /// Called when reconciliation is complete
    fn on_reconciliation_done(&mut self, from_state: DsState) {
        // This should only ever be called if reconciliation completed
        // successfully; make some assertions to that effect.
        self.downstairs.on_reconciliation_done(from_state);

        info!(self.log, "All required reconciliation work is completed");
        info!(
            self.log,
            "Set Downstairs and Upstairs active after reconciliation"
        );

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
        res.send_ok(());
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
            UpstairsState::Active => Ok(()),
            UpstairsState::GoActive(..) => {
                Err(CrucibleError::UpstairsActivateInProgress)
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

        #[cfg(feature = "notify-nexus")]
        self.downstairs
            .notify_nexus_of_client_task_stopped(client_id, reason);

        // If the upstairs is already active (or trying to go active), then the
        // downstairs should automatically call PromoteToActive when it reaches
        // the relevant state.
        let auto_promote = match self.state {
            UpstairsState::Active | UpstairsState::GoActive(..) => true,
            UpstairsState::Initializing
            | UpstairsState::Deactivating { .. } => false,
        };

        self.downstairs
            .reinitialize(client_id, auto_promote, &self.state);
    }

    /// Sets both guest and per-client backpressure
    fn set_backpressure(&self) {
        self.guest.set_backpressure(
            self.downstairs.write_bytes_outstanding(),
            self.downstairs.jobs_outstanding(),
        );

        self.downstairs.set_client_backpressure();
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
        client::ClientStopReason,
        downstairs::test::set_all_active,
        test::{make_encrypted_upstairs, make_upstairs},
        Block, BlockOp, BlockOpWaiter, DsState, JobId,
    };
    use bytes::BytesMut;
    use crucible_common::integrity_hash;
    use crucible_protocol::{ReadBlockContext, ReadResponseHeader};
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
    pub(crate) fn start_up_and_repair(or_ds: ClientId) -> Upstairs {
        let mut up = create_test_upstairs();

        // Move our downstairs client fail_id to LiveRepair.
        let client = &mut up.downstairs.clients[or_ds];
        client.checked_state_transition(&up.state, DsState::Faulted);
        client.checked_state_transition(&up.state, DsState::LiveRepairReady);

        // Start repairing the downstairs; this also enqueues the jobs
        up.apply(UpstairsAction::RepairCheck);

        // Assert that the repair started
        up.on_repair_check();
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

    #[test]
    fn reconcile_not_ready() {
        // Verify reconcile returns false when a downstairs is not ready
        let mut up = Upstairs::test_default(None);
        up.ds_transition(ClientId::new(0), DsState::WaitActive);
        up.ds_transition(ClientId::new(0), DsState::WaitQuorum);

        up.ds_transition(ClientId::new(1), DsState::WaitActive);
        up.ds_transition(ClientId::new(1), DsState::WaitQuorum);

        let res = up.connect_region_set();
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

        let (ds_done_brw, ds_done_res) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Deactivate {
            done: ds_done_res,
        }));

        let reply = ds_done_brw.wait().await;
        assert!(reply.is_err());

        up.force_active().unwrap();

        let (ds_done_brw, ds_done_res) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Deactivate {
            done: ds_done_res,
        }));

        let reply = ds_done_brw.wait().await;
        assert!(reply.is_ok());

        let (ds_done_brw, ds_done_res) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Deactivate {
            done: ds_done_res,
        }));

        let reply = ds_done_brw.wait().await;
        assert!(reply.is_err());
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
        let (ds_done_brw, ds_done_res) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Deactivate {
            done: ds_done_res,
        }));

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
            }));

            // This causes the downstairs state to be reinitialized
            assert_eq!(up.ds_state(client_id), DsState::New);

            if client_id.get() < 2 {
                assert!(matches!(up.state, UpstairsState::Deactivating { .. }));
            } else {
                // Once the third task stops, we're back in initializing
                assert!(matches!(up.state, UpstairsState::Initializing));
            }
        }

        let reply = ds_done_brw.wait().await;
        assert!(reply.is_ok());
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

    #[test]
    fn test_deps_writes_depend_on_overlapping_writes() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0x00; 512].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[test]
    fn test_deps_writes_depend_on_overlapping_writes_chain() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0x00; 512].as_slice()),
            false,
        );

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0x55; 512].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id],);
    }

    #[test]
    fn test_deps_writes_depend_on_overlapping_writes_and_flushes() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_flush(None, None);

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0x55; 512].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write (op 0)
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // flush (op 1)
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // write (op 2)
    }

    #[test]
    fn test_deps_all_writes_depend_on_flushes() {
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
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // op 3
        upstairs.submit_flush(None, None);

        // ops 4 to 6
        for i in 3..6 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
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

    #[test]
    fn test_deps_little_writes_depend_on_big_write() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512 * 3].as_slice()),
            false,
        );

        // ops 1 to 3
        for i in 0..3 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 4);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0,1,2

        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // write @ 0
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // write @ 1
        assert_eq!(jobs[3].work.deps(), &[jobs[0].ds_id]); // write @ 2
    }

    #[test]
    fn test_deps_little_writes_depend_on_big_write_chain() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512 * 3].as_slice()),
            false,
        );

        // ops 1 to 3
        for i in 0..3 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // ops 4 to 6
        for i in 0..3 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
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

    #[test]
    fn test_deps_big_write_depends_on_little_writes() {
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
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // op 3
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512 * 3].as_slice()),
            false,
        );

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

    #[test]
    fn test_deps_read_depends_on_write() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // read @ 0
    }

    #[test]
    fn test_deps_big_read_depends_on_little_writes() {
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
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // op 3
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(2, 512));

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

    #[test]
    fn test_deps_read_no_depend_on_read() {
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
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        // op 1
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // read @ 0
        assert!(jobs[1].work.deps().is_empty()); // read @ 0
    }

    #[test]
    fn test_deps_multiple_reads_depend_on_write() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        // op 2
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // read @ 0
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]); // read @ 0
    }

    #[test]
    fn test_deps_read_depends_on_flush() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_flush(None, None);

        // op 2
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(2, 512));

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // write @ 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // flush
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // read @ 0
    }

    #[test]
    fn test_deps_flushes_depend_on_flushes() {
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

        upstairs.submit_flush(None, None);

        upstairs.submit_flush(None, None);

        upstairs.submit_flush(None, None);

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]);
    }

    #[test]
    fn test_deps_flushes_depend_on_flushes_and_all_writes() {
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
        upstairs.submit_flush(None, None);

        // ops 1 to 2
        for i in 0..2 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // op 3
        upstairs.submit_flush(None, None);

        // ops 4 to 6
        for i in 0..3 {
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512].as_slice()),
                false,
            );
        }

        // op 7
        upstairs.submit_flush(None, None);

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

    #[test]
    fn test_deps_writes_depend_on_read() {
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
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
    }

    #[test]
    fn test_deps_write_unwrittens_depend_on_read() {
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
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            true,
        );

        // op 2
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
    }

    #[test]
    fn test_deps_read_write_ladder_1() {
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
        upstairs.submit_dummy_read(BlockIndex(0), Buffer::new(1, 512));

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            true,
        );

        // op 2
        upstairs.submit_dummy_read(BlockIndex(1), Buffer::new(2, 512));

        // op 3
        upstairs.submit_dummy_write(
            BlockIndex(1),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 4
        upstairs.submit_dummy_read(BlockIndex(3), Buffer::new(2, 512));

        // op 5
        upstairs.submit_dummy_write(
            BlockIndex(3),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            true,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 6);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1

        assert!(jobs[2].work.deps().is_empty()); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3

        assert!(jobs[4].work.deps().is_empty()); // op 4
        assert_eq!(jobs[5].work.deps(), &[jobs[4].ds_id]); // op 5
    }

    #[test]
    fn test_deps_read_write_ladder_2() {
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
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512 * 2].as_slice()),
                true,
            );
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3
        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // op 4
    }

    #[test]
    fn test_deps_read_write_ladder_3() {
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
            upstairs.submit_dummy_write(
                BlockIndex(i),
                BytesMut::from([0xff; 512 * 2].as_slice()),
                false,
            );
        }

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]); // op 3
        assert_eq!(jobs[4].work.deps(), &[jobs[3].ds_id]); // op 4
    }

    #[test]
    fn test_deps_read_write_batman() {
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
        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(4),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(1),
            BytesMut::from([0xff; 512 * 4].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty()); // op 0
        assert!(jobs[1].work.deps().is_empty()); // op 1
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id],); // op 2
    }

    #[test]
    fn test_deps_multi_extent_write() {
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
        upstairs.submit_dummy_write(
            BlockIndex(95),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(96),
            BytesMut::from([0xff; 512 * 7].as_slice()),
            false,
        );

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(102),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

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

    #[test]
    fn test_deps_multi_extent_there_and_back_again() {
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
        upstairs.submit_dummy_write(
            BlockIndex(95),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(96),
            BytesMut::from([0xff; 512 * 7].as_slice()),
            false,
        );

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(101),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 3
        upstairs.submit_dummy_write(
            BlockIndex(99),
            BytesMut::from([0xff; 512 * 3].as_slice()),
            true,
        );

        // op 4
        upstairs.submit_dummy_read(BlockIndex(99), Buffer::new(1, 512));

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

    #[test]
    fn test_deps_multi_extent_batman() {
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
        upstairs.submit_dummy_write(
            BlockIndex(95),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        // op 1
        upstairs.submit_dummy_write(
            BlockIndex(102),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(96),
            BytesMut::from([0xff; 512 * 7].as_slice()),
            true,
        );

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

    #[test]
    fn test_read_flush_write_hash_mismatch() {
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
        upstairs.submit_dummy_read(BlockIndex(95), Buffer::new(2, 512));

        // op 1
        upstairs.submit_flush(None, None);

        // op 2
        upstairs.submit_dummy_write(
            BlockIndex(96),
            BytesMut::from([0xff; 512 * 2].as_slice()),
            false,
        );

        let jobs = upstairs.downstairs.get_all_jobs();
        assert_eq!(jobs.len(), 3);

        // assert read has no deps
        assert!(jobs[0].work.deps().is_empty()); // op 0

        // assert flush depends on the read
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]); // op 1

        // assert write depends on just the flush
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]); // op 2
    }

    #[test]
    fn test_deps_depend_on_acked_work() {
        // Test that jobs will depend on acked work (important for the case of
        // replay - the upstairs will replay all work since the last flush if a
        // downstairs leaves and comes back)

        let mut upstairs = make_upstairs();
        upstairs.force_active().unwrap();

        // submit a write, complete, then ack it

        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        {
            let ds = &mut upstairs.downstairs;
            let jobs = ds.get_all_jobs();
            assert_eq!(jobs.len(), 1);

            let ds_id = jobs[0].ds_id;

            crate::downstairs::test::finish_job(ds, ds_id);
        }

        // submit an overlapping write

        upstairs.submit_dummy_write(
            BlockIndex(0),
            BytesMut::from([0xff; 512].as_slice()),
            false,
        );

        {
            let ds = &upstairs.downstairs;
            let jobs = ds.get_all_jobs();

            // retire_check not run yet, so there's two active jobs
            assert_eq!(jobs.len(), 2);

            // the second write should still depend on the first write!
            assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        }
    }

    #[test]
    fn test_check_for_repair_normal() {
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
        up.on_repair_check();
        assert!(up.repair_check_interval.is_none());
        assert!(!up.downstairs.live_repair_in_progress());

        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        // No need to repair or check for future repairs here either
        up.on_repair_check();
        assert!(up.repair_check_interval.is_none());
        assert!(!up.downstairs.live_repair_in_progress());

        // No downstairs should change state.
        for c in up.downstairs.clients.iter() {
            assert_eq!(c.state(), DsState::Active);
        }
        assert!(up.downstairs.repair().is_none());
    }

    #[test]
    fn test_check_for_repair_do_repair() {
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
        up.on_repair_check();
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());
        assert_eq!(up.ds_state(ClientId::new(1)), DsState::LiveRepair);
        assert!(up.downstairs.repair().is_some());
    }

    #[test]
    fn test_check_for_repair_do_two_repair() {
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
        up.on_repair_check();
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());

        assert_eq!(up.ds_state(ClientId::new(0)), DsState::Active);
        assert_eq!(up.ds_state(ClientId::new(1)), DsState::LiveRepair);
        assert_eq!(up.ds_state(ClientId::new(2)), DsState::LiveRepair);
        assert!(up.downstairs.repair().is_some())
    }

    #[test]
    fn test_check_for_repair_already_repair() {
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
        up.on_repair_check();
        assert!(up.downstairs.live_repair_in_progress());
        assert!(up.repair_check_interval.is_none());

        // Pretend that DS 0 faulted then came back through to LiveRepairReady;
        // we won't halt the existing repair, but will configure
        // repair_check_interval to check again in the future.
        up.ds_transition(ClientId::new(0), DsState::Faulted);
        up.ds_transition(ClientId::new(0), DsState::LiveRepairReady);

        up.on_repair_check();
        assert!(up.downstairs.live_repair_in_progress());
        assert!(up.repair_check_interval.is_some());
    }

    #[test]
    fn test_check_for_repair_task_running() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let mut up = Upstairs::test_default(Some(ddef));
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);
        up.ds_transition(ClientId::new(1), DsState::Faulted);
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady);

        up.on_repair_check();
        assert!(up.repair_check_interval.is_none());
        assert!(up.downstairs.live_repair_in_progress());

        // Checking again is idempotent
        up.on_repair_check();
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
        let offset = BlockIndex(7);
        let data = BytesMut::from([1; 512].as_slice());
        let (_write_res, done) = BlockOpWaiter::pair();
        let op = if is_write_unwritten {
            BlockOp::WriteUnwritten { offset, data, done }
        } else {
            BlockOp::Write { offset, data, done }
        };
        up.apply(UpstairsAction::Guest(op));
        up.await_deferred_ops().await;
        let id1 = JobId(1000); // We know that job IDs start at 1000

        // Create and enqueue the flush by setting deactivate
        let (mut deactivate_done_brw, deactivate_done_res) =
            BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Deactivate {
            done: deactivate_done_res,
        }));

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
            }));
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
            }));
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
        }));

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
            }));
        }

        let reply = deactivate_done_brw.try_wait().unwrap();
        assert!(reply.is_ok());

        // Verify we have disconnected and can go back to init.
        assert!(matches!(up.state, UpstairsState::Initializing));

        // Verify after the ds_missing, all downstairs are New
        for c in up.downstairs.clients.iter() {
            assert_eq!(c.state(), DsState::New);
        }
    }

    #[test]
    fn good_decryption() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // fake read response from downstairs that will successfully decrypt
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _hash) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data);

        let blocks = Ok(vec![ReadBlockContext::Encrypted {
            ctx: crucible_protocol::EncryptionContext {
                nonce: nonce.into(),
                tag: tag.into(),
            },
        }]);
        let data = BytesMut::from(&data[..]);

        // Because this read is small, it happens right away
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(0),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks,
                },
                data,
            }),
        }));

        // This was a small read and handled in-line
        assert!(up.deferred_msgs.is_empty());
        // No panic, great job everyone
    }

    #[tokio::test]
    async fn good_deferred_decryption() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let blocks = 16384 / 512;
        let data = Buffer::new(blocks, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _hash) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data);

        let nonce: [u8; 12] = nonce.into();
        let tag: [u8; 16] = tag.into();

        // Build up the long read response, which should be long enough to
        // trigger the deferred read path.
        let mut responses = vec![];
        let mut buf = BytesMut::new();
        for _ in 0..blocks {
            responses.push(ReadBlockContext::Encrypted {
                ctx: crucible_protocol::EncryptionContext { nonce, tag },
            });

            buf.extend(&data);
        }
        let responses = Ok(responses);

        // This defers decryption to a separate thread, because the read is
        // large.  We'll check that the job is deferred below.
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(0),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: responses,
                },
                data: buf,
            }),
        }));

        // This was a large read and was deferred
        assert!(!up.deferred_msgs.is_empty());

        up.await_deferred_msgs().await;
        // No panic, great job everyone
    }

    #[tokio::test]
    async fn bad_deferred_decryption_means_panic() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let blocks = 16384 / 512;
        let data = Buffer::new(blocks, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // fake read response from downstairs that will fail decryption
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data);

        let nonce: [u8; 12] = nonce.into();
        let mut tag: [u8; 16] = tag.into();

        // alter tag
        if tag[3] == 0xFF {
            tag[3] = 0x00;
        } else {
            tag[3] = 0xFF;
        }

        // Build up the long read response, which should be long enough to
        // trigger the deferred read path.
        let mut responses = vec![];
        let mut buf = BytesMut::new();
        for _ in 0..blocks {
            responses.push(ReadBlockContext::Encrypted {
                ctx: crucible_protocol::EncryptionContext { nonce, tag },
            });

            buf.extend(&data[..]);
        }
        let responses = Ok(responses);

        // This defers decryption to a separate thread, because the read is
        // large.  This won't panic, because decryption failing just populates
        // the message with an error.
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(0),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: responses,
                },
                data: buf,
            }),
        }));

        // Prepare to receive the message with an invalid tag
        let fut = up.await_deferred_msgs();

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

    /// Confirm that an offloaded decryption also panics (eventually)
    #[test]
    fn bad_decryption_means_panic() {
        let mut up = make_encrypted_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // fake read response from downstairs that will fail decryption
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = up
            .cfg
            .encryption_context
            .as_ref()
            .unwrap()
            .encrypt_in_place(&mut data);

        let nonce: [u8; 12] = nonce.into();
        let mut tag: [u8; 16] = tag.into();

        // alter tag
        if tag[3] == 0xFF {
            tag[3] = 0x00;
        } else {
            tag[3] = 0xFF;
        }

        let responses = Ok(vec![ReadBlockContext::Encrypted {
            ctx: crucible_protocol::EncryptionContext { nonce, tag },
        }]);

        // Prepare to receive the message with an invalid tag
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(0),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: responses,
                    },
                    data: data.as_slice().into(),
                }),
            }))
        });

        let result = thread.join();
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

    #[test]
    fn bad_read_hash_makes_panic() {
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // fake read response from downstairs that will fail integrity hash
        // check
        let responses = Ok(vec![ReadBlockContext::Unencrypted {
            hash: 10000, // junk hash
        }]);

        // Prepare to handle the response with a junk hash
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(0),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: responses,
                    },
                    data: BytesMut::from([1u8; 512].as_slice()),
                }),
            }))
        });

        // Don't use `should_panic`, as the `unwrap` above could cause this test
        // to pass for the wrong reason.
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(r.contains("HashMismatch"));
    }

    #[test]
    fn work_read_hash_mismatch() {
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        let data = BytesMut::from([1u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: r1,
                },
                data,
            }),
        }));

        // Send back a second response with different data and a hash that (1)
        // is correct for that data, but (2) does not match the original hash.
        //
        // This distinguishes between a regular hash failure and a hash mismatch
        // between multiple ReadResponse
        let data = BytesMut::from([2u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r2 = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r2,
                    },
                    data,
                }),
            }))
        });
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch")); // not the usual mismatch error
        assert!(r.contains("read hash mismatch"));
    }

    #[test]
    fn work_read_hash_mismatch_third() {
        // Test that a hash mismatch on the third response will trigger a panic.
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        for client_id in [ClientId::new(0), ClientId::new(1)] {
            let data = BytesMut::from([1u8; 512].as_slice());
            let hash = integrity_hash(&[&data]);
            let r = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id,
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r,
                    },
                    data: data.clone(),
                }),
            }));
        }

        // Send back a second response with different data and a hash that (1)
        // is correct for that data, but (2) does not match the original hash.
        //
        // This distinguishes between a regular hash failure and a hash mismatch
        // between multiple ReadResponse
        let data = BytesMut::from([2u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r,
                    },
                    data,
                }),
            }))
        });
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch")); // not the usual mismatch error
        assert!(r.contains("read hash mismatch"));
    }

    #[test]
    fn work_read_hash_inside() {
        // Test that a hash length mismatch will panic
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        let data = BytesMut::from([1u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: r1,
                },
                data,
            }),
        }));

        // Send back a second response with more data (2 blocks instead of 1);
        // the first block matches.
        let data = BytesMut::from([1u8; 512 * 2].as_slice());
        let hash = integrity_hash(&[&data[0..512]]);
        let response = ReadBlockContext::Unencrypted { hash };
        let r2 = Ok(vec![response, response]);
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r2,
                    },
                    data,
                }),
            }))
        });
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }

    #[test]
    fn work_read_hash_mismatch_no_data() {
        // Test that empty data first, then data later will trigger
        // hash mismatch panic.
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // The first read has no block contexts, because it was unwritten
        let data = BytesMut::from([0u8; 512].as_slice());
        let r1 = Ok(vec![ReadBlockContext::Empty]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: r1,
                },
                data: data.clone(),
            }),
        }));

        // Send back a second response with actual block contexts (oh no!)
        let hash = integrity_hash(&[&data]);
        let r2 = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r2,
                    },
                    data,
                }),
            }))
        });
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }

    #[test]
    fn work_read_hash_mismatch_no_data_next() {
        // Test that missing data on the 2nd read response will panic
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        let data = Buffer::new(1, 512);
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Read { offset, data, done }));

        // The first read has no block contexts, because it was unwritten
        let data = BytesMut::from([0u8; 512].as_slice());
        let hash = integrity_hash(&[&data]);
        let r1 = Ok(vec![ReadBlockContext::Unencrypted { hash }]);
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id: ClientId::new(1),
            action: ClientAction::Response(Message::ReadResponse {
                header: ReadResponseHeader {
                    upstairs_id: up.cfg.upstairs_id,
                    session_id: up.cfg.session_id,
                    job_id: JobId(1000),
                    blocks: r1,
                },
                data: data.clone(),
            }),
        }));

        // Send back a second response with no actual data (oh no!)
        let r2 = Ok(vec![ReadBlockContext::Empty]);
        let thread = std::thread::spawn(move || {
            up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
                client_id: ClientId::new(2),
                action: ClientAction::Response(Message::ReadResponse {
                    header: ReadResponseHeader {
                        upstairs_id: up.cfg.upstairs_id,
                        session_id: up.cfg.session_id,
                        job_id: JobId(1000),
                        blocks: r2,
                    },
                    data,
                }),
            }))
        });
        let result = thread.join();

        assert!(result.is_err());
        let r = result
            .as_ref()
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap();
        assert!(!r.contains("HashMismatch"));
        assert!(r.contains("read hash mismatch"));
    }

    #[test]
    fn write_defer() {
        let mut up = make_upstairs();
        up.force_active().unwrap();
        set_all_active(&mut up.downstairs);

        const NODEFER_SIZE: usize = MIN_DEFER_SIZE_BYTES as usize - 512;
        const DEFER_SIZE: usize = MIN_DEFER_SIZE_BYTES as usize * 2;

        // Submit a short write, which should not be deferred
        let mut data = BytesMut::new();
        data.extend_from_slice(vec![1; NODEFER_SIZE].as_slice());
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Write { offset, data, done }));
        assert_eq!(up.deferred_ops.len(), 0);

        // Submit a long write, which should be deferred
        let mut data = BytesMut::new();
        data.extend_from_slice(vec![2; DEFER_SIZE].as_slice());
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Write { offset, data, done }));
        assert_eq!(up.deferred_ops.len(), 1);
        assert_eq!(up.deferred_msgs.len(), 0);

        // Submit a short write, which would normally not be deferred, but
        // there's already a deferred job in the queue
        let mut data = BytesMut::new();
        data.extend_from_slice(vec![3; NODEFER_SIZE].as_slice());
        let offset = BlockIndex(7);
        let (_res, done) = BlockOpWaiter::pair();
        up.apply(UpstairsAction::Guest(BlockOp::Write { offset, data, done }));
        assert_eq!(up.deferred_ops.len(), 2);
        assert_eq!(up.deferred_msgs.len(), 0);
    }
}
