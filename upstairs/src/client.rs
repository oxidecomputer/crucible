use crate::{
    cdt, deadline_secs, integrity_hash, live_repair::ExtentInfo,
    upstairs::UpstairsConfig, upstairs::UpstairsState, ClientIOStateCount,
    ClientId, CrucibleDecoder, CrucibleEncoder, CrucibleError, DownstairsIO,
    DsState, EncryptionContext, IOState, IOop, JobId, Message, ReadResponse,
    ReconcileIO, RegionDefinitionStatus, RegionMetadata, WrappedStream,
};
use crucible_protocol::{ReconciliationId, CRUCIBLE_MESSAGE_VERSION};

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use futures::{SinkExt, StreamExt};
use slog::{debug, error, info, o, warn, Logger};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    time::{sleep_until, Instant},
};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

const TIMEOUT_SECS: f32 = 50.0;
const PING_INTERVAL_SECS: f32 = 5.0;

/// Handle to a running I/O task
///
/// The I/O task is "thin"; it simply forwards messages around.  The task
/// continues running until `client_request_tx` is dropped, so it is always
/// valid to send it new messages.
#[derive(Debug)]
struct ClientTaskHandle {
    /// Handle to send data to the I/O task
    client_request_tx: mpsc::Sender<Message>,

    /// One-shot sender to stop the client
    ///
    /// This is a oneshot so that functions which stop the client don't
    /// necessarily need to be `async`.
    ///
    /// It is `None` if we have already requested that the client stop, but have
    /// not yet seen the task finished.
    client_stop_tx: Option<oneshot::Sender<ClientStopReason>>,

    /// Handle to receive data from the I/O task
    client_response_rx: mpsc::Receiver<ClientResponse>,
}

#[derive(Debug)]
pub(crate) struct DownstairsClient {
    /// Shared (static) configuration
    cfg: Arc<UpstairsConfig>,

    /// One's own client ID
    client_id: ClientId,

    /// Per-client log
    pub(crate) log: Logger,

    /// Client task, if one is running
    client_task: Option<ClientTaskHandle>,

    /// IO state counters
    pub(crate) io_state_count: ClientIOStateCount,

    /// UUID for this downstairs region
    ///
    /// Unpopulated until provided by `Message::RegionInfo`
    region_uuid: Option<Uuid>,

    /// The IP:Port of each of the downstairs
    ///
    /// This is left unpopulated in some unit tests
    pub(crate) target_addr: Option<SocketAddr>,

    /// The IP:Port for repair when contacting the downstairs
    ///
    /// This is set to `None` during initialization
    pub(crate) repair_addr: Option<SocketAddr>,

    /// TLS context (if present)
    ///
    /// This is passed as a pointer to minimize copies
    tls_context: Option<Arc<crucible_common::x509::TLSContext>>,

    /// State of the downstairs connection
    state: DsState,

    /// The last flush job ID that each downstairs has acked.
    ///
    /// Note that this is a job ID; not a flush ID!
    pub(crate) last_flush: JobId,

    /// Cache of new jobs
    new_jobs: BTreeSet<JobId>,

    /// Jobs that have been skipped
    pub(crate) skipped_jobs: BTreeSet<JobId>,

    /// Region metadata for this particular Downstairs
    ///
    /// On Startup, we collect info from each downstairs region. We use that
    /// info to make sure that all three regions in a region set are the
    /// same, and if not the same, to decide which data we will consider
    /// valid and make the other downstairs contain that same data.
    pub(crate) region_metadata: Option<RegionMetadata>,

    /**
     * Live Repair info
     * This will contain the extent info for each downstairs as reported
     * by those downstairs and is used to decide if an extent requires
     * repair or not.
     */
    pub(crate) repair_info: Option<ExtentInfo>,

    /// Deadline for the next ping
    ping_interval: Instant,

    /// Deadline until we mark the client as dead
    timeout_deadline: Instant,

    /// Ping every 10 seconds if things are idle
    ping_count: u64,

    /// Accumulated statistics
    pub(crate) stats: DownstairsStats,

    /// State for the "promote to active" action
    promote_state: Option<PromoteState>,

    /// State for startup negotiation
    negotiation_state: NegotiationState,
}

impl DownstairsClient {
    pub(crate) fn new(
        client_id: ClientId,
        cfg: Arc<UpstairsConfig>,
        target_addr: Option<SocketAddr>,
        log: Logger,
        tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    ) -> Self {
        let mut out = Self {
            cfg,
            client_task: None, // started up below
            client_id,
            region_uuid: None,
            negotiation_state: NegotiationState::Start,
            ping_count: 0,
            ping_interval: deadline_secs(PING_INTERVAL_SECS),
            tls_context,
            promote_state: None,
            timeout_deadline: deadline_secs(TIMEOUT_SECS),
            log,
            target_addr,
            repair_addr: None,
            state: DsState::New,
            last_flush: JobId(0),
            stats: DownstairsStats::default(),
            new_jobs: BTreeSet::new(),
            skipped_jobs: BTreeSet::new(),
            region_metadata: None,
            repair_info: None,
            io_state_count: ClientIOStateCount::new(),
        };

        out.start_task(false);
        out
    }

    /// Builds a minimal `DownstairsClient` for testing
    ///
    /// The resulting client has no target address; any packets sent by the
    /// client will disappear into the void.
    #[cfg(test)]
    fn test_default() -> Self {
        let cfg = Arc::new(UpstairsConfig {
            encryption_context: None,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
            read_only: false,
            lossy: false,
        });
        let mut out = Self {
            cfg,
            client_task: None, // started up below
            client_id: ClientId::new(0),
            region_uuid: None,
            negotiation_state: NegotiationState::Start,
            ping_count: 0,
            ping_interval: deadline_secs(PING_INTERVAL_SECS),
            tls_context: None,
            promote_state: None,
            timeout_deadline: deadline_secs(TIMEOUT_SECS),
            log: crucible_common::build_logger(),
            target_addr: None,
            repair_addr: None,
            state: DsState::New,
            last_flush: JobId(0),
            stats: DownstairsStats::default(),
            new_jobs: BTreeSet::new(),
            skipped_jobs: BTreeSet::new(),
            region_metadata: None,
            repair_info: None,
            io_state_count: ClientIOStateCount::new(),
        };
        out.start_dummy_task();
        out
    }

    /// Return true if more work can be sent.
    ///
    /// If `io_send` can send more work, return true, otherwise return false.
    /// Because this is used on a `select!` branch that returns
    /// `futures::future::ready(())`, a short-circuit can be created if no more
    /// work can actually be done.
    pub(crate) fn should_do_more_work(&self) -> bool {
        self.client_task.is_some()
            && !self.new_jobs.is_empty()
            && matches!(self.state, DsState::Active | DsState::LiveRepair)
            && (crate::MAX_ACTIVE_COUNT
                - self.io_state_count.in_progress as usize)
                > 0
    }

    /// Choose which `ClientAction` to apply
    ///
    /// This function is called from within a top-level `select!`, so not only
    /// must the select expressions be cancel safe, but the **bodies** must also
    /// be cancel-safe.  This is why we simply return a single value in the body
    /// of each statement.
    pub(crate) async fn select(&mut self) -> ClientAction {
        tokio::select! {
            d = async {
                if let Some(c) = &mut self.client_task {
                    tokio::select! {
                        m = c.client_response_rx.recv() => {
                            match m {
                                Some(ClientResponse::Connected) =>
                                    ClientAction::Connected,
                                Some(ClientResponse::Message(m)) =>
                                    ClientAction::Response(m),
                                Some(ClientResponse::Done(r)) =>
                                    ClientAction::TaskStopped(r),
                                None =>
                                    ClientAction::ChannelClosed,
                            }
                        }
                    }
                } else {
                    futures::future::pending().await
                }
            } => {
                d
            }
            _ = sleep_until(self.ping_interval), if self.client_task.is_some()
            => {
                ClientAction::Ping
            }
            _ = sleep_until(self.timeout_deadline) => {
                ClientAction::Timeout
            }
        }
    }

    /// If the client task is running, send a `Message::HereIAm`
    ///
    /// If the client task is **not** running, log a warning to that effect.
    pub(crate) async fn send_here_i_am(&mut self) {
        if let Some(task) = &self.client_task {
            if let Err(e) = task
                .client_request_tx
                .send(Message::HereIAm {
                    version: CRUCIBLE_MESSAGE_VERSION,
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    gen: self.cfg.gen,
                    read_only: self.cfg.read_only,
                    encrypted: self.cfg.encrypted(),
                    alternate_versions: vec![],
                })
                .await
            {
                warn!(self.log, "failed to send HereIAm: {e}");
                // TODO we should probably fail more loudly here?
            }
        } else {
            panic!(
                "send_here_i_am should not be called when client task is stopped"
            );
        }
    }

    /// If the client task is running, send a `Message::Ruok`
    ///
    /// If the client task is **not** running, log a warning to that effect.
    pub(crate) async fn send_ping(&mut self) {
        self.ping_interval = deadline_secs(PING_INTERVAL_SECS);
        // It's possible for the client task to have stopped after we requested
        // the ping.  If that's the case, then we'll catch it on the next
        // go-around, and should just log an error here.
        if let Some(task) = &self.client_task {
            if let Err(e) = task.client_request_tx.send(Message::Ruok).await {
                warn!(self.log, "failed to send ping: {e}");
            }
            self.ping_count += 1;
            cdt::ds__ping__sent!(|| (self.ping_count, self.client_id.get()));
        } else {
            // This, on the other hand, should not be possible, because we only
            // give the Ping action if the client task is Some, and we do not
            // clear it autonomously.
            panic!(
                "send_ping should not be called when client task is stopped"
            );
        }
    }

    pub(crate) fn halt_io_task(&mut self, r: ClientStopReason) {
        if let Some(task) = &mut self.client_task {
            if let Some(t) = std::mem::take(&mut task.client_stop_tx) {
                if let Err(_e) = t.send(r) {
                    warn!(self.log, "failed to send stop request")
                }
            } else {
                warn!(self.log, "client task is already stopping")
            }
        } else {
            panic!(
                "halt_io_task should not be called when client task is stopped"
            );
        }
    }

    /// Return a list of downstairs request IDs that represent unissued
    /// requests for this client.
    ///
    /// Returns a tuple of `(jobs, flow control)` where flow control is true if
    /// the jobs list has been clamped to `max_count`.
    pub(crate) fn new_work(
        &mut self,
        max_count: usize,
    ) -> (BTreeSet<JobId>, bool) {
        if max_count >= self.new_jobs.len() {
            // Happy path: we can grab everything
            (std::mem::take(&mut self.new_jobs), false)
        } else {
            // Otherwise, pop elements from the queue
            let mut out = BTreeSet::new();
            for _ in 0..max_count {
                out.insert(self.new_jobs.pop_first().unwrap());
            }
            (out, true)
        }
    }

    /// Requeues a single job
    ///
    /// This is used when running in lossy mode, where jobs may be skipped in
    /// `io_send`.
    pub(crate) fn requeue_one(&mut self, work: JobId) {
        self.new_jobs.insert(work);
    }

    pub(crate) async fn send(&mut self, m: Message) {
        if let Some(task) = &self.client_task {
            if let Err(e) = task.client_request_tx.send(m).await {
                warn!(self.log, "failed to send message: {e}")
            }
        } else {
            panic!("send should not be called when client task is stopped");
        }
    }

    /// Sets a job state, handling `io_state_count` counters
    fn set_job_state(
        &mut self,
        job: &mut DownstairsIO,
        new_state: IOState,
    ) -> IOState {
        self.io_state_count.incr(&new_state);
        let old_state = job.state.insert(self.client_id, new_state);
        self.io_state_count.decr(&old_state);
        old_state
    }

    /// Mark the given job as in-progress for this client
    ///
    /// Returns an `IOop` with modified dependencies
    pub(crate) fn in_progress(
        &mut self,
        job: &mut DownstairsIO,
        repair_min_id: Option<JobId>,
    ) -> IOop {
        let old_state = self.set_job_state(job, IOState::InProgress);
        assert_eq!(old_state, IOState::New);

        let mut out = job.work.clone();
        if self.dependencies_need_cleanup() {
            match &mut out {
                IOop::Write { dependencies, .. }
                | IOop::WriteUnwritten { dependencies, .. }
                | IOop::Flush { dependencies, .. }
                | IOop::Read { dependencies, .. }
                | IOop::ExtentClose { dependencies, .. }
                | IOop::ExtentFlushClose { dependencies, .. }
                | IOop::ExtentLiveRepair { dependencies, .. }
                | IOop::ExtentLiveReopen { dependencies, .. }
                | IOop::ExtentLiveNoOp { dependencies } => {
                    self.remove_dep_if_live_repair(
                        dependencies,
                        job.ds_id,
                        repair_min_id.expect("must have repair_min_id"),
                    );
                }
            }
        }
        // If our downstairs is under repair, then include any extent limit sent
        // in the IOop; otherwise, clear it out
        if let IOop::Flush { extent_limit, .. } = &mut out {
            if !matches!(self.state, DsState::LiveRepair) {
                *extent_limit = None;
            }
        }
        out
    }

    pub(crate) fn replay_job(&mut self, job: &mut DownstairsIO) {
        /*
         * If the job is InProgress or New, then we can just go back
         * to New and no extra work is required.
         * If it's Done, then by definition it has been acked; assert that here
         * to double-check.
         */
        if IOState::Done == job.state[self.client_id] {
            assert!(job.acked);
        }
        let old_state = self.set_job_state(job, IOState::New);
        job.replay = true;
        if old_state != IOState::New {
            self.requeue_one(job.ds_id);
        }
    }

    /// Sets this job as skipped and moves it to `skipped_jobs`
    pub(crate) fn skip_job(&mut self, job: &mut DownstairsIO) {
        self.set_job_state(job, IOState::Skipped);
        self.skipped_jobs.insert(job.ds_id);
    }

    /// Returns true if it's possible that we need to clean job dependencies
    pub(crate) fn dependencies_need_cleanup(&self) -> bool {
        matches!(self.state, DsState::LiveRepair)
            && !self.skipped_jobs.is_empty()
    }

    /// Sets our state to `DsState::Repair`
    ///
    /// # Panics
    /// If the previous state is not `DsState::WaitQuorum`
    pub(crate) fn begin_reconcile(&mut self) {
        /*
         * We transition all the downstairs to needing repair here
         * while we have the downstairs lock.  This will insure that
         * all downstairs enter the repair path.
         */
        info!(self.log, "Transition from {} to Repair", self.state);
        /*
         * This is a panic and not an error because we should
         * not call this method without already verifying the
         * downstairs are in the proper state.
         */
        assert_eq!(self.state, DsState::WaitQuorum);
        self.state = DsState::Repair;
    }

    /// Go through the list of dependencies and remove any jobs that this
    /// downstairs has already skipped, as the downstairs on the other side will
    /// not have received these IOs.
    ///
    /// First off, any job that was "skipped" should not be a dependency for
    /// this specific downstairs.  In addition, any job that happened before
    /// the skipped jobs that was marked as "Done" should also be removed, as
    /// there will be no replay here and we are basically rebuilding this
    /// downstairs from other downstairs.
    fn remove_dep_if_live_repair(
        &self,
        deps: &mut Vec<JobId>,
        ds_id: JobId,
        repair_min_id: JobId,
    ) {
        debug!(
            self.log,
            "{} Remove check skipped:{:?} from deps:{:?}",
            ds_id,
            self.skipped_jobs,
            deps
        );
        assert!(matches!(self.state, DsState::LiveRepair));

        deps.retain(|x| !self.skipped_jobs.contains(x));

        // If we are repairing, then there must be a repair_min_id set so we
        // know where to stop with dependency inclusion.
        debug!(
            self.log,
            "{} Remove check < min repaired:{} from deps:{:?}",
            ds_id,
            repair_min_id,
            deps
        );
        deps.retain(|x| x >= &repair_min_id);
        info!(self.log, " {} final dependency list {:?}", ds_id, deps);
    }

    /// When the downstairs is marked as missing, handle its state transition
    pub(crate) fn on_missing(&mut self) {
        let current = &self.state;
        let new_state = match current {
            DsState::Active | DsState::Replay | DsState::Offline => {
                DsState::Offline
            }

            DsState::Faulted
            | DsState::LiveRepair
            | DsState::LiveRepairReady => DsState::Faulted,

            DsState::New
            | DsState::Deactivated
            | DsState::Repair
            | DsState::FailedRepair => DsState::New,

            DsState::Replacing => DsState::Replaced,

            _ => {
                /*
                 * Any other state means we had not yet enabled this
                 * downstairs to receive IO, so we go to the back of the
                 * line and have to re-verify it again.
                 */
                DsState::Disconnected
            }
        };

        if *current != new_state {
            info!(
                self.log,
                "Gone missing, transition from {current:?} to {new_state:?}"
            );
        } else {
            info!(self.log, "Still missing, state is {current:?}");
        }

        // Should we move jobs now?  When do we move work that has
        // been submitted over to "skipped"
        self.state = new_state;

        // Mark the client task as absent
        assert!(self.client_task.is_some());
        self.client_task = None;
    }

    /// Checks whether this Downstairs is ready for the upstairs to deactivate
    ///
    /// # Panics
    /// If the downstairs is offline
    pub(crate) fn ready_to_deactivate(&self) -> bool {
        match &self.state {
            DsState::New | DsState::WaitActive => {
                info!(
                    self.log,
                    "ready to deactivate from state {:?}", self.state
                );
                true
            }
            DsState::Offline => {
                panic!("can't deactivate while a downstairs is offline")
            }
            s => {
                info!(self.log, "not ready to deactivate due to state {s:?}");
                false
            }
        }
    }

    /// Switches the client state to Deactivated and stops the IO task
    pub(crate) fn deactivate(&mut self, up_state: &UpstairsState) {
        self.checked_state_transition(up_state, DsState::Deactivated);
        self.halt_io_task(ClientStopReason::Deactivated)
    }

    /// Resets this Downstairs and start a fresh connection
    ///
    /// # Panics
    /// If `self.client_task` is not `None`, or `self.target_addr` is `None`
    pub(crate) fn reinitialize(&mut self, auto_promote: Option<u64>) {
        // Clear this Downstair's repair address, and let the YesItsMe set it.
        // This works if this Downstairs is new, reconnecting, or was replaced
        // entirely; the repair address could have changed in any of these
        // cases.
        self.repair_addr = None;

        if let Some(g) = auto_promote {
            self.promote_state = Some(PromoteState::Waiting(g));
        } else {
            self.promote_state = None;
        }
        self.negotiation_state = NegotiationState::Start;

        // Restart with a short delay
        self.start_task(true);
    }

    /// Returns the last flush ID handled by this client
    pub(crate) fn last_flush(&self) -> JobId {
        self.last_flush
    }

    /// Starts a dummy IO task, saving the handle in `self.client_task`
    ///
    /// # Panics
    /// If `self.client_task` is not `None`
    #[cfg(test)]
    fn start_dummy_task(&mut self) {
        assert!(
            self.client_task.is_none(),
            "cannot start task when it is already running"
        );
        let (client_request_tx, client_request_rx) = mpsc::channel(500);
        let (_client_response_tx, client_response_rx) = mpsc::channel(500);
        let (client_stop_tx, client_stop_rx) = oneshot::channel();

        // Forget these without dropping them, so that we can send values into
        // the void!
        std::mem::forget(client_request_rx);
        std::mem::forget(client_stop_rx);

        self.client_task = Some(ClientTaskHandle {
            client_request_tx,
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        });
    }
    /// Starts a client IO task, saving the handle in `self.client_task`
    ///
    /// If we are running unit tests and `self.target_addr` is not populated, we
    /// start a dummy task instead.
    ///
    /// # Panics
    /// If `self.client_task` is not `None`, or `self.target_addr` is `None` and
    /// this isn't running in test mode
    fn start_task(&mut self, delay: bool) {
        #[cfg(test)]
        if self.target_addr.is_none() {
            self.start_dummy_task();
        } else {
            self.start_network_task(delay);
        }

        #[cfg(not(test))]
        self.start_network_task(delay);
    }

    fn start_network_task(&mut self, delay: bool) {
        assert!(
            self.client_task.is_none(),
            "cannot start task when it is already running"
        );
        let target = self.target_addr.expect("socket address is not populated");
        let (client_request_tx, client_request_rx) = mpsc::channel(500);
        let (client_response_tx, client_response_rx) = mpsc::channel(500);
        let (client_stop_tx, client_stop_rx) = oneshot::channel();

        let tls_context = self.tls_context.clone();
        let log = self.log.new(o!("" => "io task"));
        let client_id = self.client_id;
        tokio::spawn(async move {
            client_run(
                client_id,
                tls_context,
                target,
                client_request_rx,
                client_stop_rx,
                client_response_tx,
                delay,
                log,
            )
            .await
        });
        self.reset_timeout();
        self.client_task = Some(ClientTaskHandle {
            client_request_tx,
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        });
    }

    /// Indicate that the upstairs has requested that we go active
    ///
    /// This either sent a `PromoteToActive` request directly, or schedules it
    /// to be sent once the client state reaches `WaitActive`.
    ///
    /// # Panics
    /// If we already called this function (without `reinitialize` in between),
    /// or `self.state` is invalid for promotion.
    pub(crate) async fn set_active_request(&mut self, gen: u64) {
        match self.promote_state {
            Some(PromoteState::Waiting(..)) => {
                panic!("called set_active_request while already waiting")
            }
            Some(PromoteState::Sent(..)) => {
                panic!("called set_active_request after it was sent")
            }
            None => (),
        }
        // If we're already in the point of negotiation where we're waiting to
        // go active, then immediately go active!
        match self.state {
            DsState::New => {
                info!(
                    self.log,
                    "client set_active_request while in {:?}; waiting...",
                    self.state,
                );
                self.promote_state = Some(PromoteState::Waiting(gen));
            }
            DsState::WaitActive => {
                info!(
                    self.log,
                    "client set_active_request while in WaitActive \
                 -> WaitForPromote"
                );
                let r = self
                    .client_task
                    .as_ref()
                    .unwrap()
                    .client_request_tx
                    .send(Message::PromoteToActive {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        gen,
                    })
                    .await;

                // If the client task has stopped, then print a warning but
                // otherwise continue (because we'll be cleaned up by the
                // JoinHandle watcher).
                if let Err(e) = r {
                    warn!(
                        self.log,
                        "failed to sent PromoteToActive client request: {e}"
                    );
                }
                self.promote_state = Some(PromoteState::Sent(gen));
                // TODO: negotiation / promotion state is spread across
                // DsState, PromoteState, and NegotiationState.  We should
                // consolidate into a single place
                assert!(
                    self.negotiation_state == NegotiationState::Start
                        || self.negotiation_state
                            == NegotiationState::WaitForPromote
                );
                self.negotiation_state = NegotiationState::WaitForPromote;
            }
            s => panic!("invalid state for set_active_request: {s:?}"),
        }
    }

    /// Accessor method for client connection state
    pub(crate) fn state(&self) -> DsState {
        self.state
    }

    /// Sets the current state to `DsState::FailedRepair`
    pub(crate) fn set_failed_repair(&mut self, up_state: &UpstairsState) {
        info!(self.log, "Transition from {} to FailedRepair", self.state);
        self.checked_state_transition(up_state, DsState::FailedRepair);
        self.restart_connection(up_state, ClientStopReason::FailedRepair)
    }

    pub(crate) fn restart_connection(
        &mut self,
        up_state: &UpstairsState,
        reason: ClientStopReason,
    ) {
        let new_state = match self.state {
            DsState::Active => DsState::Offline,
            DsState::Replay => DsState::Offline,
            DsState::Offline => DsState::Offline,
            DsState::Migrating => DsState::Faulted,
            DsState::Faulted => DsState::Faulted,
            DsState::Deactivated => DsState::New,
            DsState::Repair => DsState::New,
            DsState::FailedRepair => DsState::New,
            DsState::LiveRepair => DsState::Faulted,
            DsState::LiveRepairReady => DsState::Faulted,
            DsState::Replacing => DsState::Replaced,
            _ => {
                /*
                 * Any other state means we had not yet enabled this
                 * downstairs to receive IO, so we go to the back of the
                 * line and have to re-verify it again.
                 */
                DsState::Disconnected
            }
        };

        info!(
            self.log,
            "restarting connection, transition from {} to {}",
            self.state,
            new_state,
        );

        self.checked_state_transition(up_state, new_state);
        self.halt_io_task(reason);
    }

    /// Sets the current state to `DsState::Active`
    pub(crate) fn set_active(&mut self) {
        info!(self.log, "Transition from {} to Active", self.state);
        self.state = DsState::Active;
    }

    pub(crate) fn enqueue(
        &mut self,
        io: &mut DownstairsIO,
        last_repair_extent: Option<u64>,
    ) -> IOState {
        assert_eq!(io.state[self.client_id], IOState::New);

        // If a downstairs is faulted or ready for repair, we can move
        // that job directly to IOState::Skipped
        // If a downstairs is in repair, then we need to see if this
        // IO is on a repaired extent or not.  If an IO spans extents
        // where some are repaired and some are not, then this IO had
        // better have the dependencies already set to reflect the
        // requirement that a repair IO will need to finish first.
        let r = match self.state {
            DsState::Faulted
            | DsState::Replaced
            | DsState::Replacing
            | DsState::LiveRepairReady => {
                io.state.insert(self.client_id, IOState::Skipped);
                self.skipped_jobs.insert(io.ds_id);
                IOState::Skipped
            }
            DsState::LiveRepair => {
                // Pick the latest repair limit that's relevant for this
                // downstairs.  This is either the extent under repair (if
                // there are no reserved repair jobs), or the last extent
                // for which we have reserved a repair job ID; either way, the
                // caller has provided it to us.
                if io.work.send_io_live_repair(last_repair_extent) {
                    // Leave this IO as New, the downstairs will receive it.
                    self.new_jobs.insert(io.ds_id);
                    IOState::New
                } else {
                    // Move this IO to skipped, we are not ready for
                    // the downstairs to receive it.
                    io.state.insert(self.client_id, IOState::Skipped);
                    self.skipped_jobs.insert(io.ds_id);
                    IOState::Skipped
                }
            }
            _ => {
                self.new_jobs.insert(io.ds_id);
                IOState::New
            }
        };
        self.io_state_count.incr(&r);
        r
    }

    /// Prepares for a new connection, then restarts the IO task
    pub(crate) fn replace(
        &mut self,
        up_state: &UpstairsState,
        new: SocketAddr,
    ) {
        self.target_addr = Some(new);

        self.region_metadata = None;
        self.checked_state_transition(up_state, DsState::Replacing);
        self.stats.replaced += 1;

        self.halt_io_task(ClientStopReason::Replacing);
    }

    /// Sets `self.state` to `new_state`, with logging and validity checking
    ///
    /// Conceptually, this function is a checked assignment to `self.state`.
    /// Thinking in terms of the graph of all possible states, this function
    /// will panic if there is not a valid state transition edge between the
    /// current `self.state` and the requested `new_state`.
    ///
    /// For example, transitioning to a `new_state` of [DsState::Replacing] is
    /// *always* possible, so this will never panic for that state transition.
    /// On the other hand, [DsState::Replaced] can *only* follow
    /// [DsState::Replacing], so if the current state is *anything else*, that
    /// indicates a logic error happened in some other part of the code.
    ///
    /// If the state transition is valid, this function simply sets `self.state`
    /// to the newly requested state. There's no magic here beyond that; this
    /// function does not change anything about the state or any other internal
    /// variables.
    ///
    /// # Panics
    /// If the transition is not valid
    pub(crate) fn checked_state_transition(
        &mut self,
        up_state: &UpstairsState,
        new_state: DsState,
    ) {
        // TODO this should probably be private!
        info!(self.log, "ds_transition from {} to {new_state}", self.state);

        let old_state = self.state;

        /*
         * Check that this is a valid transition
         */
        let panic_invalid = || {
            panic!(
                "[{}] {} Invalid transition: {:?} -> {:?}",
                self.client_id, self.cfg.upstairs_id, old_state, new_state
            )
        };
        match new_state {
            DsState::Replacing => {
                // A downstairs can be replaced at any time.
            }
            DsState::Replaced => {
                assert_eq!(old_state, DsState::Replacing);
            }
            DsState::WaitActive => {
                if old_state == DsState::Offline {
                    if matches!(up_state, UpstairsState::Active) {
                        panic!(
                            "[{}] {} Bad up active state change {} -> {}",
                            self.client_id,
                            self.cfg.upstairs_id,
                            old_state,
                            new_state,
                        );
                    }
                } else if old_state != DsState::New
                    && old_state != DsState::Faulted
                    && old_state != DsState::Disconnected
                {
                    panic!(
                        "[{}] {} Negotiation failed, {:?} -> {:?}",
                        self.client_id,
                        self.cfg.upstairs_id,
                        old_state,
                        new_state,
                    );
                }
            }
            DsState::WaitQuorum => {
                assert_eq!(old_state, DsState::WaitActive);
            }
            DsState::FailedRepair => {
                assert_eq!(old_state, DsState::Repair);
            }
            DsState::Faulted => {
                match old_state {
                    DsState::Active
                    | DsState::Faulted
                    | DsState::Repair
                    | DsState::LiveRepair
                    | DsState::LiveRepairReady
                    | DsState::Offline
                    | DsState::Replay => {} /* Okay */
                    _ => {
                        panic_invalid();
                    }
                }
            }
            DsState::Repair => {
                assert!(!matches!(up_state, UpstairsState::Active));
                assert_eq!(old_state, DsState::WaitQuorum);
            }
            DsState::Replay => {
                assert!(matches!(up_state, UpstairsState::Active));
                assert_eq!(old_state, DsState::Offline);
            }
            DsState::Active => {
                match old_state {
                    DsState::WaitQuorum
                    | DsState::Replay
                    | DsState::Repair
                    | DsState::LiveRepair => {} // Okay

                    DsState::LiveRepairReady if self.cfg.read_only => {} // Okay

                    _ => {
                        panic_invalid();
                    }
                }
                /*
                 * Make sure repair happened when the upstairs is inactive.
                 */
                if old_state == DsState::Repair {
                    assert!(!matches!(up_state, UpstairsState::Active));
                }
            }
            DsState::Deactivated => {
                // We only go deactivated if we were actually active, or
                // somewhere past active.
                // if deactivate is requested before active, the downstairs
                // state should just go back to NEW and re-require an
                // activation.
                match old_state {
                    DsState::Active
                    | DsState::Replay
                    | DsState::LiveRepair
                    | DsState::LiveRepairReady
                    | DsState::Repair => {} // Okay
                    _ => {
                        panic_invalid();
                    }
                }
            }
            DsState::LiveRepair => {
                assert_eq!(old_state, DsState::LiveRepairReady);
            }
            DsState::LiveRepairReady => {
                match old_state {
                    DsState::Faulted | DsState::Replaced => {} // Okay
                    _ => {
                        panic_invalid();
                    }
                }
            }
            DsState::New => {
                // Before new, we must have been in
                // on of these states.
                match old_state {
                    DsState::Active
                    | DsState::Deactivated
                    | DsState::Faulted
                    | DsState::FailedRepair => {} // Okay
                    _ => {
                        panic_invalid();
                    }
                }
            }
            DsState::Offline => {
                match old_state {
                    DsState::Active | DsState::Replay => {} // Okay
                    _ => {
                        panic_invalid();
                    }
                }
            }
            DsState::Disabled => {
                // A move to Disabled can happen at any time we are talking
                // to a downstairs.
            }
            DsState::BadVersion => match old_state {
                DsState::New | DsState::Disconnected => {}
                _ => {
                    panic_invalid();
                }
            },
            _ => {
                panic!(
                    "[{}] Missing check for transition {} to {}",
                    self.client_id, old_state, new_state
                );
            }
        }

        if old_state != new_state {
            info!(
                self.log,
                "[{}] Transition from {} to {}",
                self.client_id,
                old_state,
                new_state,
            );
            self.state = new_state;
        } else {
            warn!(
                self.log,
                "[{}] transition to same state: {}", self.client_id, new_state
            );
        }
    }

    /// Remove all jobs from `self.new_jobs`
    ///
    /// This is only useful when marking the downstair as faulted or similar
    pub(crate) fn clear_new_jobs(&mut self) {
        self.new_jobs.clear()
    }

    /// Aborts an in-progress live repair, restarting the task
    ///
    /// # Panics
    /// If this client is not in `DsState::LiveRepair`
    pub(crate) fn abort_repair(&mut self, up_state: &UpstairsState) {
        assert_eq!(self.state, DsState::LiveRepair);
        self.checked_state_transition(up_state, DsState::Faulted);
        self.halt_io_task(ClientStopReason::FailedLiveRepair);
        self.repair_info = None;
        self.stats.live_repair_aborted += 1;
    }

    /// Sets the state to `Fault` and restarts the IO task
    pub(crate) fn fault(
        &mut self,
        up_state: &UpstairsState,
        reason: ClientStopReason,
    ) {
        self.checked_state_transition(up_state, DsState::Faulted);
        self.halt_io_task(reason);
    }

    /// Finishes an in-progress live repair, setting our state to `Active`
    ///
    /// # Panics
    /// If this client is not in `DsState::LiveRepair`
    pub(crate) fn finish_repair(&mut self, up_state: &UpstairsState) {
        assert_eq!(self.state, DsState::LiveRepair);
        self.checked_state_transition(up_state, DsState::Active);
        self.repair_info = None;
        self.stats.live_repair_completed += 1;
    }

    /// Resets our timeout deadline
    pub(crate) fn reset_timeout(&mut self) {
        self.timeout_deadline = deadline_secs(TIMEOUT_SECS);
    }

    /// Handles a single IO operation
    ///
    /// Returns `true` if the job is now ackable, `false` otherwise
    pub(crate) fn process_io_completion(
        &mut self,
        job: &mut DownstairsIO,
        mut responses: Result<Vec<ReadResponse>, CrucibleError>,
        deactivate: bool,
        extent_info: Option<ExtentInfo>,
    ) -> bool {
        let ds_id = job.ds_id;
        if job.state[self.client_id] == IOState::Skipped {
            // This job was already marked as skipped, and at that time
            // all required action was taken on it.  We can drop any more
            // processing of it here and return.
            warn!(self.log, "Dropping already skipped job {}", ds_id);
            return false;
        }

        let mut jobs_completed_ok = job.state_count().completed_ok();
        let mut ackable = false;

        // Validate integrity hashes and optionally authenticated decryption.
        //
        // With AE, responses can come back that are invalid given an encryption
        // context. Test this here. If it fails, then something has gone
        // irrecoverably wrong and we should panic.
        let mut read_response_hashes = Vec::new();
        let new_state = match &mut responses {
            Ok(responses) => {
                responses.iter_mut().for_each(|x| {
                    let mh =
                        if let Some(context) = &self.cfg.encryption_context {
                            validate_encrypted_read_response(
                                x, context, &self.log,
                            )
                        } else {
                            validate_unencrypted_read_response(x, &self.log)
                        }
                        .expect("decryption failed");
                    read_response_hashes.push(mh);
                });
                jobs_completed_ok += 1;
                IOState::Done
            }
            Err(e) => {
                // The downstairs sent us this error
                error!(
                    self.log,
                    "DS Reports error {e:?} on job {}, {:?} EC", ds_id, job,
                );
                IOState::Error(e.clone())
            }
        };

        let old_state = job.state.insert(self.client_id, new_state.clone());
        self.io_state_count.decr(&old_state);
        self.io_state_count.incr(&new_state);

        /*
         * Verify the job was InProgress
         */
        if old_state != IOState::InProgress {
            // This job is in an unexpected state.
            panic!(
                "[{}] Job {} completed while not InProgress: {:?}",
                self.client_id, ds_id, old_state
            );
        }

        if let IOState::Error(e) = new_state {
            // Some errors can be returned without considering the Downstairs
            // bad. For example, it's still an error if a snapshot exists
            // already but we should not increment downstairs_errors and
            // transition that Downstairs to Failed - that downstairs is still
            // able to serve IO.
            match e {
                CrucibleError::SnapshotExistsAlready(_) => {
                    // pass
                }
                _ => {
                    match job.work {
                        // Mark this downstairs as bad if this was a write or flush
                        // XXX: reconcilation, retries?
                        // XXX: Errors should be reported to nexus
                        IOop::Write { .. }
                        | IOop::WriteUnwritten { .. }
                        | IOop::Flush { .. } => {
                            self.stats.downstairs_errors += 1;
                        }

                        // If a repair job errors, mark that downstairs as bad
                        IOop::ExtentClose { .. }
                        | IOop::ExtentFlushClose { .. }
                        | IOop::ExtentLiveRepair { .. }
                        | IOop::ExtentLiveReopen { .. }
                        | IOop::ExtentLiveNoOp { .. } => {
                            // TODO: Figure out a plan on how to handle
                            // errors during repair.  We must invalidate
                            // any jobs dependent on the repair success as
                            // well as throw out the whole repair and start
                            // over as we can no longer trust results from
                            // the downstairs under repair.
                            self.stats.downstairs_errors += 1;
                        }

                        // If a read job fails, we sometimes need to panic.
                        IOop::Read {
                            dependencies: _,
                            requests: _,
                        } => {
                            // It's possible we get a read error if the
                            // downstairs disconnects. However XXX, someone
                            // should be told about this error.
                            //
                            // Some errors, we need to panic on.
                            match e {
                                CrucibleError::HashMismatch => {
                                    panic!(
                                        "[{}] {} read hash mismatch {:?} {:?}",
                                        self.client_id, ds_id, e, job
                                    );
                                }
                                CrucibleError::DecryptionError => {
                                    panic!(
                                        "[{}] {} read decrypt error {:?} {:?}",
                                        self.client_id, ds_id, e, job
                                    );
                                }
                                _ => {
                                    error!(
                                        self.log,
                                        "{} read error {:?} {:?}",
                                        ds_id,
                                        e,
                                        job
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else if job.acked {
            assert_eq!(new_state, IOState::Done);
            /*
             * If this job is already acked, then we don't have much
             * more to do here.  If it's a flush, then we want to be
             * sure to update the last flush for this client.
             */
            match &job.work {
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                    gen_number: _gen_number,
                    snapshot_details: _,
                    extent_limit: _,
                } => {
                    self.last_flush = ds_id;
                }
                IOop::Read {
                    dependencies: _dependencies,
                    requests,
                } => {
                    /*
                     * For a read, make sure the data from a previous read
                     * has the same hash
                     */
                    let read_data: Vec<ReadResponse> = responses.unwrap();
                    assert!(!read_data.is_empty());
                    if job.read_response_hashes != read_response_hashes {
                        // XXX This error needs to go to Nexus
                        // XXX This will become the "force all downstairs
                        // to stop and refuse to restart" mode.
                        let msg = format!(
                            "[{}] read hash mismatch on id {}\n\
                            Expected {:x?}\n\
                            Computed {:x?}\n\
                            guest_id:{} request:{:?}\n\
                            job state:{:?}",
                            self.client_id,
                            ds_id,
                            job.read_response_hashes,
                            read_response_hashes,
                            job.guest_id,
                            requests,
                            job.state,
                        );
                        if job.replay {
                            info!(self.log, "REPLAY {}", msg);
                        } else {
                            panic!("{}", msg);
                        }
                    }
                }
                /*
                 * Write and WriteUnwritten IOs have no action here
                 * If this job was LiveRepair, we should never get here,
                 * as those jobs should never be acked before all three
                 * are done.
                 */
                IOop::Write { .. } | IOop::WriteUnwritten { .. } => {}
                IOop::ExtentClose { .. }
                | IOop::ExtentFlushClose { .. }
                | IOop::ExtentLiveRepair { .. }
                | IOop::ExtentLiveReopen { .. }
                | IOop::ExtentLiveNoOp { .. } => {
                    panic!(
                        "[{}] Bad job received in process_ds_completion: {:?}",
                        self.client_id, job
                    );
                }
            }
        } else {
            assert_eq!(new_state, IOState::Done);
            assert!(!job.acked);

            let read_data: Vec<ReadResponse> = responses.unwrap();

            /*
             * Transition this job from Done to AckReady if enough have
             * returned ok.
             */
            match &job.work {
                IOop::Read { .. } => {
                    assert!(!read_data.is_empty());
                    assert!(extent_info.is_none());
                    if jobs_completed_ok == 1 {
                        assert!(job.data.is_none());
                        assert!(job.read_response_hashes.is_empty());
                        job.data = Some(read_data);
                        job.read_response_hashes = read_response_hashes;
                        assert!(!job.acked);
                        ackable = true;
                        debug!(self.log, "Read AckReady {}", job.ds_id);
                        cdt::up__to__ds__read__done!(|| job.guest_id);
                    } else {
                        /*
                         * If another job has finished already, we can
                         * compare our read hash to
                         * that and verify they are the same.
                         */
                        debug!(self.log, "Read already AckReady {ds_id}");
                        if job.read_response_hashes != read_response_hashes {
                            // XXX This error needs to go to Nexus
                            // XXX This will become the "force all downstairs
                            // to stop and refuse to restart" mode.
                            panic!(
                                "[{}] read hash mismatch on {} \n\
                                Expected {:x?}\n\
                                Computed {:x?}\n\
                                job: {:?}",
                                self.client_id,
                                ds_id,
                                job.read_response_hashes,
                                read_response_hashes,
                                job,
                            );
                        }
                    }
                }
                IOop::Write { .. } => {
                    assert!(read_data.is_empty());
                    assert!(extent_info.is_none());
                    if jobs_completed_ok == 2 {
                        ackable = true;
                        cdt::up__to__ds__write__done!(|| job.guest_id);
                    }
                }
                IOop::WriteUnwritten { .. } => {
                    assert!(read_data.is_empty());
                    assert!(extent_info.is_none());
                    if jobs_completed_ok == 2 {
                        ackable = true;
                        cdt::up__to__ds__write__unwritten__done!(
                            || job.guest_id
                        );
                    }
                }
                IOop::Flush {
                    snapshot_details, ..
                } => {
                    assert!(read_data.is_empty());
                    assert!(extent_info.is_none());
                    /*
                     * If we are deactivating or have requested a
                     * snapshot, then we want an ACK from all three
                     * downstairs, not the usual two.
                     *
                     * TODO here for handling the case where one (or two,
                     * or three! gasp!) downstairs are Offline.
                     */
                    let ack_at_num_jobs =
                        if deactivate || snapshot_details.is_some() {
                            3
                        } else {
                            2
                        };

                    if jobs_completed_ok == ack_at_num_jobs {
                        ackable = true;
                        cdt::up__to__ds__flush__done!(|| job.guest_id);
                        if deactivate {
                            debug!(self.log, "deactivate flush {ds_id} done");
                        }
                    }
                    self.last_flush = ds_id;
                }
                IOop::ExtentClose { extent, .. } => {
                    panic!(
                        "[{}] job: {:?} Received illegal IOop::ExtentClose {}",
                        self.client_id, job, extent,
                    );
                }
                IOop::ExtentFlushClose { .. } => {
                    assert!(read_data.is_empty());

                    let ci = self.repair_info.replace(extent_info.unwrap());
                    if ci.is_some() {
                        panic!(
                            "[{}] Unexpected repair found on insertion: {:?}",
                            self.client_id, ci
                        );
                    }

                    if jobs_completed_ok == 3 {
                        debug!(self.log, "ExtentFlushClose {ds_id} AckReady");
                        ackable = true;
                    }
                }
                IOop::ExtentLiveRepair { .. } => {
                    assert!(read_data.is_empty());
                    if jobs_completed_ok == 3 {
                        debug!(self.log, "ExtentLiveRepair AckReady {ds_id}");
                        ackable = true;
                    }
                }
                IOop::ExtentLiveReopen { .. } => {
                    assert!(read_data.is_empty());
                    if jobs_completed_ok == 3 {
                        debug!(self.log, "ExtentLiveReopen AckReady {ds_id}");
                        ackable = true;
                    }
                }
                IOop::ExtentLiveNoOp { .. } => {
                    assert!(read_data.is_empty());
                    if jobs_completed_ok == 3 {
                        debug!(self.log, "ExtentLiveNoOp AckReady {ds_id}");
                        ackable = true;
                    }
                }
            }
        }
        ackable
    }

    /// Mark this client as disabled and halt its IO task
    ///
    /// The IO task will automatically restart in the main event handler
    pub(crate) fn disable(&mut self, up_state: &UpstairsState) {
        self.checked_state_transition(up_state, DsState::Disabled);
        self.halt_io_task(ClientStopReason::Disabled);
    }

    /// Helper function to send a message to the client
    ///
    /// # Panics
    /// If the client task isn't running or the handle has been closed
    async fn send_client_message(&mut self, m: Message) {
        // send promotion message
        self.client_task
            .as_ref()
            .expect("can't send message without client task")
            .client_request_tx
            .send(m)
            .await
            .unwrap() // TODO is this okay?
    }

    /// Skips from `LiveRepairRead` to `Active`; a no-op otherwise
    ///
    /// # Panics
    /// If this downstairs is not read-only
    pub(crate) fn skip_live_repair(&mut self, up_state: &UpstairsState) {
        if self.state == DsState::LiveRepairReady {
            assert!(self.cfg.read_only);
            // TODO: could we do this transition early, by automatically
            // skipping LiveRepairReady if read-only?
            self.checked_state_transition(up_state, DsState::Active);
            self.stats.ro_lr_skipped += 1;
        }
    }

    /// Moves from `LiveRepairReady` to `LiveRepair`; a no-op otherwise
    pub(crate) fn start_live_repair(&mut self, up_state: &UpstairsState) {
        if self.state == DsState::LiveRepairReady {
            self.checked_state_transition(up_state, DsState::LiveRepair);
        }
    }

    /// Continues the negotiation and initial reconciliation process
    ///
    /// Returns an error if the upstairs should go inactive, which occurs if the
    /// error is at or after `Message::YouAreNowActive`.
    ///
    /// Returns `true` if negotiation for this downstairs is complete
    pub(crate) async fn continue_negotiation(
        &mut self,
        m: Message,
        up_state: &UpstairsState,
        upstairs_gen: u64,
        ddef: &mut RegionDefinitionStatus,
    ) -> Result<bool, CrucibleError> {
        /*
         * Either we get all the way through the negotiation, or we hit the
         * timeout and exit to retry.
         *
         * XXX There are many ways we can handle this, but as we figure out
         * how the upstairs is notified that a DS is new or moving, or other
         * things, this way will work. We will revisit when we have more info.
         *
         * The negotiation flow starts as follows, with the value of the
         * negotiated variable on the left:
         *
         * NegotiationState::Start
         * -----------------------
         *          Upstairs             Downstairs
         *           HereIAm(...)  --->
         *                         <---  YesItsMe(...)
         *
         * At this point, a downstairs will wait for a PromoteToActive message
         * to be sent to it.  If this is a new upstairs that has not yet
         * connected to a downstairs, then we will wait for the guest to send
         * us this message and pass it down to the downstairs.  If a downstairs
         * is reconnecting after having already been active, then we look at our
         * upstairs guest_io_ready() and, if the upstairs is ready, we send the
         * downstairs the message ourselves that they should promote to active.
         * For downstairs currently in Disconnected or New states, we move to
         * WaitActive, for Faulted or Offline states, we stay in that state..
         *
         * NegotiationState::WaitForPromote
         * --------------------------------
         *    PromoteToActive(uuid)--->
         *                         <---  YouAreNowActive(uuid)
         *
         * YouAreNowActive includes information about the upstairs and session
         * ID and we do some sanity checking here to make sure it all still
         * matches with what we expect.  We next request RegionInfo from the
         * downstairs.
         *
         * NegotiationState::WaitForRegionInfo
         * -----------------------------------
         *       RegionInfoPlease  --->
         *                         <---  RegionInfo(r)
         *
         * At this point the upstairs looks to see what state the downstairs is
         * currently in.  It will be WaitActive, Faulted, or Offline.
         *
         * Depending on which state, we will either choose
         * NegotiationState::GetLastFlush or NegotiationState::GetExtentVersions
         * next.
         *
         * For the Offline state, the downstairs was connected and verified
         * and some point after that, the connection was lost.  To handle this
         * condition we want to know the last flush this downstairs had ACKd
         * so we can give it whatever work it missed.
         *
         * For WaitActive, it means this downstairs never was "Active" and we
         * have to go through the full compare of this downstairs with other
         * downstairs and make sure they are consistent.  To do that, we will
         * request extent versions and skip over NegotiationState::GetLastFlush.
         *
         * For Faulted, we don't know the condition of the data on the
         * Downstairs, so we transition this downstairs to LiveRepairReady.  We
         * also request extent versions and will have to repair this
         * downstairs, skipping over NegotiationState::GetLastFlush as well.
         *
         * NegotiationState::GetLastFlush (offline only)
         * ------------------------------
         *          Upstairs             Downstairs
         *          LastFlush(lf)) --->
         *                         <---  LastFlushAck(lf)
         *
         * After receiving our last flush, we now move this downstairs state to
         * Replay and skip ahead to NegotiationState::Done
         *
         * NegotiationState::GetExtentVersions
         * (WaitActive and LiveRepairReady come here from WaitForRegionInfo)
         * -----------------------------------
         *          Upstairs             Downstairs
         *    ExtentVersionsPlease --->
         *                         <---  ExtentVersions(g, v, d)
         *
         * Now with the extent info, Upstairs calls process_downstairs() and
         * if no problems, sends connected=true to the up_listen() task,
         * we set the downstairs to DsState::WaitQuorum and we exit the
         * while loop.
         *
         * NegotiationState::Done
         * ----------------------
         *    Now the downstairs is ready to receive replay IOs from the
         *    upstairs. We set the downstairs to DsState::Replay and the while
         *    loop is exited.
         */
        match m {
            Message::YesItsMe {
                version,
                repair_addr,
            } => {
                if self.negotiation_state != NegotiationState::Start {
                    error!(self.log, "got version already");
                    self.restart_connection(
                        up_state,
                        ClientStopReason::BadNegotiationOrder,
                    );
                    return Ok(false);
                }
                if version != CRUCIBLE_MESSAGE_VERSION {
                    error!(
                        self.log,
                        "expected version {}, got {}",
                        CRUCIBLE_MESSAGE_VERSION,
                        version
                    );
                    self.checked_state_transition(
                        up_state,
                        DsState::BadVersion,
                    );
                    self.restart_connection(
                        up_state,
                        ClientStopReason::Incompatible,
                    );
                    return Ok(false);
                }
                self.negotiation_state = NegotiationState::WaitForPromote;
                self.repair_addr = Some(repair_addr);
                match self.promote_state {
                    Some(PromoteState::Waiting(gen)) => {
                        self.send_client_message(Message::PromoteToActive {
                            upstairs_id: self.cfg.upstairs_id,
                            session_id: self.cfg.session_id,
                            gen,
                        })
                        .await;
                        self.promote_state = Some(PromoteState::Sent(gen));
                        self.negotiation_state =
                            NegotiationState::WaitForPromote;
                        // TODO This is an unfortunate corner of the state
                        // machine, where we have to be in WaitActive despite
                        // _already_ having gone active.
                        if self.state == DsState::New {
                            self.checked_state_transition(
                                up_state,
                                DsState::WaitActive,
                            );
                        }
                    }
                    Some(PromoteState::Sent(_gen)) => {
                        // We shouldn't be able to get here.
                        panic!("got YesItsMe with promote_state == Sent");
                    }
                    None => {
                        // Nothing to do here, wait for set_active_request
                        self.checked_state_transition(
                            up_state,
                            DsState::WaitActive,
                        );
                    }
                }
            }
            Message::VersionMismatch { version } => {
                error!(
                    self.log,
                    "downstairs version is {version}, \
                     ours is {CRUCIBLE_MESSAGE_VERSION}"
                );
                self.checked_state_transition(up_state, DsState::BadVersion);
                self.restart_connection(
                    up_state,
                    ClientStopReason::Incompatible,
                );
            }
            Message::EncryptedMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs encrypted is {expected}, ours is {}",
                    self.cfg.encrypted()
                );
                self.restart_connection(
                    up_state,
                    ClientStopReason::Incompatible,
                );
            }
            Message::ReadOnlyMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs read_only is {expected}, ours is {}",
                    self.cfg.read_only,
                );
                self.restart_connection(
                    up_state,
                    ClientStopReason::Incompatible,
                );
            }
            Message::YouAreNowActive {
                upstairs_id,
                session_id,
                gen,
            } => {
                if self.negotiation_state != NegotiationState::WaitForPromote {
                    error!(
                        self.log,
                        "Received YouAreNowActive out of order! {:?}",
                        self.negotiation_state
                    );
                    self.restart_connection(
                        up_state,
                        ClientStopReason::BadNegotiationOrder,
                    );
                    return Ok(false);
                }

                let match_uuid = self.cfg.upstairs_id == upstairs_id;
                let match_session = self.cfg.session_id == session_id;
                let match_gen = upstairs_gen == gen;
                let matches_self = match_uuid && match_session && match_gen;

                if !matches_self {
                    error!(
                        self.log,
                        "YouAreNowActive didn't match self! {} {} {}",
                        if !match_uuid {
                            format!(
                                "UUID {:?} != {:?}",
                                self.cfg.upstairs_id, upstairs_id
                            )
                        } else {
                            String::new()
                        },
                        if !match_session {
                            format!(
                                "session {:?} != {:?}",
                                self.cfg.session_id, session_id
                            )
                        } else {
                            String::new()
                        },
                        if !match_gen {
                            format!("gen {:?} != {:?}", upstairs_gen, gen)
                        } else {
                            String::new()
                        },
                    );
                    self.checked_state_transition(up_state, DsState::New);
                    if !match_gen {
                        let gen_error = format!(
                            "Generation requested:{} found:{}",
                            gen, upstairs_gen,
                        );
                        self.restart_connection(
                            up_state,
                            ClientStopReason::Incompatible,
                        );
                        return Err(CrucibleError::GenerationNumberTooLow(
                            gen_error,
                        ));
                    } else {
                        self.restart_connection(
                            up_state,
                            ClientStopReason::Incompatible,
                        );
                        return Err(CrucibleError::UuidMismatch);
                    }
                }

                self.negotiation_state = NegotiationState::WaitForRegionInfo;
                self.send_client_message(Message::RegionInfoPlease).await;
            }
            Message::RegionInfo { region_def } => {
                if self.negotiation_state != NegotiationState::WaitForRegionInfo
                {
                    error!(self.log, "Received RegionInfo out of order!");
                    self.restart_connection(
                        up_state,
                        ClientStopReason::BadNegotiationOrder,
                    );
                    return Ok(false);
                }
                info!(
                    self.log,
                    "downstairs client at {:?} has region UUID {}",
                    self.target_addr,
                    region_def.uuid(),
                );

                // Add (and/or verify) this region info to our
                // collection for each downstairs.
                if region_def.get_encrypted() != self.cfg.encrypted() {
                    error!(self.log, "encryption expectation mismatch!");
                    self.restart_connection(
                        up_state,
                        ClientStopReason::Incompatible,
                    );
                    return Ok(false);
                }

                /*
                 * TODO: Verify that a new downstairs does not share the same
                 * UUID with an existing downstiars.
                 *
                 * TODO(#551) Verify that `region_def` makes sense (valid,
                 * nonzero block size, etc.)
                 */

                /*
                 * If this downstairs was previously registered, make sure this
                 * connection reports the same UUID the old connection did,
                 * unless we are replacing a downstairs.
                 *
                 * XXX The expected per-client UUIDs should eventually be
                 * provided when the upstairs stairs. When that happens, they
                 * can be verified here.
                 */
                if let Some(uuid) = self.region_uuid {
                    if uuid != region_def.uuid() {
                        // If we are replacing the downstairs, then a new UUID
                        // is okay.
                        if self.state == DsState::Replaced {
                            warn!(
                                self.log,
                                "[{}] replace downstairs uuid:{} with {}",
                                self.client_id,
                                uuid,
                                region_def.uuid(),
                            );
                        } else {
                            panic!(
                                "New client:{} uuid:{} does not match \
                                 existing {}",
                                self.client_id,
                                region_def.uuid(),
                                uuid,
                            );
                        }
                    } else {
                        info!(self.log, "Returning UUID:{} matches", uuid);
                    }
                }

                /*
                 * If this is a new downstairs connection, insert the UUID.
                 * If this is a replacement downstairs, insert the UUID.
                 * If it is an existing UUID, we already compared and it is good,
                 * so the insert is unnecessary, but will result in the same UUID.
                 */
                self.region_uuid = Some(region_def.uuid());

                /*
                 * If there is an expected region definition of any kind
                 * (either from a previous connection or an expectation that
                 * was supplied when this upstairs was created), make sure
                 * the new definition matches it.
                 *
                 * If this upstairs' creator didn't specify any expected
                 * values, the first downstairs to connect sets the expected
                 * values for the other two.
                 */
                if let Some(prev_def) = ddef.get_def() {
                    if prev_def.block_size() != region_def.block_size()
                        || prev_def.extent_size().value
                            != region_def.extent_size().value
                        || prev_def.extent_size().block_size_in_bytes()
                            != region_def.extent_size().block_size_in_bytes()
                        || prev_def.extent_count() != region_def.extent_count()
                    {
                        // TODO(#558) Figure out if we can handle this error.
                        // Possibly not.
                        panic!(
                            "[{}] New downstairs region info mismatch: \
                                 {:?} vs. {:?}",
                            self.client_id, ddef, region_def
                        );
                    }
                }

                *ddef = RegionDefinitionStatus::Received(region_def);

                // Match on the current state of this downstairs
                match self.state {
                    DsState::Offline => {
                        /*
                         * If we are coming from state Offline, then it means
                         * the downstairs has departed then came back in short
                         * enough time that it does not have to go into full
                         * recovery/repair mode. If we have verified that the
                         * UUID and region info is the same, we can reconnect
                         * and let any outstanding work be replayed to catch us
                         * up.  We do need to tell the downstairs the last flush
                         * ID it had ACKd to us.
                         */
                        let lf = self.last_flush;
                        info!(
                            self.log,
                            "send last flush ID to this DS: {}", lf
                        );
                        self.negotiation_state = NegotiationState::GetLastFlush;

                        self.send_client_message(Message::LastFlush {
                            last_flush_number: lf,
                        })
                        .await;
                    }
                    DsState::WaitActive
                    | DsState::Faulted
                    | DsState::Replaced => {
                        /*
                         * Ask for the current version of all extents.
                         */
                        self.negotiation_state =
                            NegotiationState::GetExtentVersions;
                        self.send_client_message(Message::ExtentVersionsPlease)
                            .await;
                    }
                    DsState::Replacing => {
                        warn!(
                            self.log,
                            "exiting negotiation because we're replacing"
                        );
                        self.restart_connection(
                            up_state,
                            ClientStopReason::Replacing,
                        );
                    }
                    bad_state => {
                        panic!(
                            "[{}] join from invalid state {} {} {:?}",
                            self.client_id,
                            bad_state,
                            self.cfg.upstairs_id,
                            self.negotiation_state,
                        );
                    }
                }
            }
            Message::LastFlushAck { last_flush_number } => {
                if self.negotiation_state != NegotiationState::GetLastFlush {
                    error!(self.log, "Received LastFlushAck out of order!");
                    self.restart_connection(
                        up_state,
                        ClientStopReason::BadNegotiationOrder,
                    );
                    return Ok(false); // TODO should we trigger set_inactive?
                }
                match self.state {
                    DsState::Replacing => {
                        error!(
                            self.log,
                            "exiting negotiation due to LastFlushAck \
                             while replacing"
                        );
                        self.restart_connection(
                            up_state,
                            ClientStopReason::Replacing,
                        );
                        return Ok(false); // TODO should we trigger set_inactive?
                    }
                    DsState::Offline => (),
                    s => panic!("got LastFlushAck in bad state {s:?}"),
                }
                info!(
                    self.log,
                    "Replied this last flush ID: {last_flush_number}"
                );
                assert_eq!(self.last_flush, last_flush_number);

                // Setting the state to "Replay" here is a formality; we
                // actually copied over the jobs in `Downstairs::reinitialize`
                // if the client was coming back from Offline.
                //
                // XXX should we remove this state?
                self.checked_state_transition(up_state, DsState::Replay);

                // Immediately set the state to Active, since we've already
                // copied over the jobs.
                self.checked_state_transition(up_state, DsState::Active);

                self.negotiation_state = NegotiationState::Done;
            }
            Message::ExtentVersions {
                gen_numbers,
                flush_numbers,
                dirty_bits,
            } => {
                if self.negotiation_state != NegotiationState::GetExtentVersions
                {
                    error!(self.log, "Received ExtentVersions out of order!");
                    self.restart_connection(
                        up_state,
                        ClientStopReason::BadNegotiationOrder,
                    );
                    return Ok(false); // TODO should we trigger set_inactive?
                }
                match self.state {
                    DsState::WaitActive => {
                        self.checked_state_transition(
                            up_state,
                            DsState::WaitQuorum,
                        );
                    }
                    DsState::Replacing => {
                        warn!(
                            self.log,
                            "exiting negotiation due to ExtentVersions while \
                             replacing"
                        );
                        self.restart_connection(
                            up_state,
                            ClientStopReason::Replacing,
                        );
                        return Ok(false); // TODO should we trigger set_inactive?
                    }
                    DsState::Faulted | DsState::Replaced => {
                        self.checked_state_transition(
                            up_state,
                            DsState::LiveRepairReady,
                        );
                    }
                    s => panic!("downstairs in invalid state {s}"),
                }

                /*
                 * Record this downstairs region info for later
                 * comparison with the other downstairs in this
                 * region set.
                 */
                let dsr = RegionMetadata {
                    generation: gen_numbers,
                    flush_numbers,
                    dirty: dirty_bits,
                };

                if let Some(old_rm) = self.region_metadata.replace(dsr) {
                    warn!(self.log, "new RM replaced this: {:?}", old_rm);
                }
                self.negotiation_state = NegotiationState::Done;
            }
            m => panic!("invalid message in continue_negotiation: {m:?}"),
        }
        Ok(self.negotiation_state == NegotiationState::Done)
    }

    /// Sends the next reconciliation job to all clients
    ///
    /// The `job` argument should be a reference to
    /// `Downstairs::reconcile_current_work`, and its state is updated.
    pub(crate) async fn send_next_reconciliation_req(
        &mut self,
        job: &mut ReconcileIO,
    ) {
        // If someone has moved us out of repair, this is a logic error
        if self.state != DsState::Repair {
            panic!("should still be in repair");
        }
        let prev_state = job.state.insert(self.client_id, IOState::InProgress);
        assert_eq!(prev_state, IOState::New);

        // Some reconciliation messages need to be adjusted on a per-client
        // basis, e.g. not sending ExtentRepair to clients that aren't being
        // repaired.
        match &job.op {
            Message::ExtentRepair {
                repair_id,
                dest_clients,
                ..
            } => {
                assert!(!dest_clients.is_empty());
                if dest_clients.iter().any(|d| *d == self.client_id) {
                    info!(self.log, "sending repair request {repair_id:?}");
                    self.send_client_message(job.op.clone()).await;
                } else {
                    // Skip this job for this Downstairs, since only the target
                    // clients need to do the repair.
                    let prev_state =
                        job.state.insert(self.client_id, IOState::Skipped);
                    assert_eq!(prev_state, IOState::InProgress);
                    info!(self.log, "no action needed request {repair_id:?}");
                }
            }
            Message::ExtentFlush {
                repair_id,
                client_id,
                ..
            } => {
                if *client_id == self.client_id {
                    info!(self.log, "sending flush request {repair_id:?}");
                    self.send_client_message(job.op.clone()).await;
                } else {
                    info!(self.log, "skipping flush request {repair_id:?}");
                    // Skip this job for this Downstairs, since it's narrowly
                    // aimed at a different client.
                    let prev_state =
                        job.state.insert(self.client_id, IOState::Skipped);
                    assert_eq!(prev_state, IOState::InProgress);
                }
            }
            Message::ExtentReopen { .. } | Message::ExtentClose { .. } => {
                // All other repair ops are sent as-is
                self.send_client_message(job.op.clone()).await;
            }
            m => panic!("invalid reconciliation request {m:?}"),
        }
    }

    /// When a reconciliation job is done, mark it as complete for this client
    ///
    /// Returns `true` if the job is done for all clients
    pub(crate) fn on_reconciliation_job_done(
        &mut self,
        repair_id: ReconciliationId,
        job: &mut ReconcileIO,
    ) -> bool {
        let old_state = job.state.insert(self.client_id, IOState::Done);
        assert_eq!(old_state, IOState::InProgress);
        assert_eq!(job.id, repair_id);
        job.state
            .iter()
            .all(|s| matches!(s, IOState::Done | IOState::Skipped))
    }

    pub(crate) fn total_live_work(&self) -> usize {
        (self.io_state_count.new + self.io_state_count.in_progress) as usize
    }
}

/// How to handle "promote to active" requests
#[derive(Debug)]
enum PromoteState {
    /// Send `PromoteToActive` when the state machine reaches `WaitForPromote`
    Waiting(u64),
    /// We have already sent `PromoteToActive`
    Sent(u64),
}

/// Tracks client negotiation progress
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum NegotiationState {
    Start,
    WaitForPromote,
    WaitForRegionInfo,
    GetLastFlush,
    GetExtentVersions,
    Done,
}

/// Action requested by `DownstairsClient::select`
///
/// This is split into a separate data structure because we need to distinguish
/// between choosing an action (which must be cancel safe) and applying that
/// action (which need not be).
#[derive(Debug)]
pub(crate) enum ClientAction {
    /// We have connected to the socket
    Connected,

    /// We have received a message on the client task channel
    Response(Message),

    /// The client task has stopped
    TaskStopped(ClientRunResult),

    /// It's time to ping the client
    Ping,

    /// The client has hit a (Crucible) timeout
    Timeout,

    /// The client IO channel has returned `None`
    ///
    /// This should never happen during normal operation, because
    /// 1) the IO task continues to run until the main upstairs task tells it to
    ///    stop (by dropping the `client_request_tx` handle), and
    /// 2) we are running with `panic=abort`, so if the IO task panics, it
    ///    should bring down the whole program
    ///
    /// However, this _may_ happen during shutdown, because the Tokio runtime
    /// shuts down tasks in arbitrary order and it's possible for the IO channel
    /// sender to be dropped before the main task stops.
    ChannelClosed,
}

#[derive(Debug, Default)]
pub(crate) struct DownstairsStats {
    /// Number of errors recorded
    pub downstairs_errors: usize,

    /// Count of extents repaired live.
    pub extents_repaired: usize,

    /// Count of extents checked but not needing live repair.
    pub extents_confirmed: usize,

    /// Count of time a downstairs LiveRepair completed.
    pub live_repair_completed: usize,

    /// Count of time a downstairs LiveRepair was aborted.
    pub live_repair_aborted: usize,

    /// Times we skipped repairing a downstairs due to being read-only
    pub ro_lr_skipped: usize,

    /// Count of downstairs connections
    pub connected: usize,

    /// Count of downstairs replacements
    pub replaced: usize,

    /// Count of times a downstairs has had flow control turned on
    pub flow_control: usize,
}

/// When the upstairs halts the IO client task, it must provide a reason
#[derive(Debug)]
pub(crate) enum ClientStopReason {
    /// Crucible-level timeout (i.e. no packets received in too long)
    Timeout,

    /// We are about to replace the client task
    Replacing,

    /// We have disabled the downstairs client for some reason
    ///
    /// (for example, we have received `Message::YouAreNoLongerActive`)
    Disabled,

    /// Repair failed and we're restarting
    FailedRepair,

    /// Negotiation message received out of order
    BadNegotiationOrder,

    /// Negotiation says that we are incompatible
    Incompatible,

    /// Live-repair failed
    FailedLiveRepair,

    /// Too many jobs in the queue
    TooManyOutstandingJobs,

    /// The upstairs has requested that we deactivate
    Deactivated,
}

/// Response received from the I/O task
#[derive(Debug)]
pub(crate) enum ClientResponse {
    /// We have connected to the socket and are starting the main loop
    Connected,
    /// We have received a message over the network
    Message(Message),
    /// The client task has stopped
    Done(ClientRunResult),
}

/// Value returned by the `client_run` task
#[derive(Debug)]
pub(crate) enum ClientRunResult {
    /// The initial connection timed out
    ConnectionTimeout,
    /// We failed to make the initial connection
    ConnectionFailed(std::io::Error),
    /// A socket write failed
    WriteFailed(anyhow::Error),
    /// We received an error while reading from the connection
    ReadFailed(anyhow::Error),
    /// The `DownstairsClient` requested that the task stop, so it did
    RequestedStop(ClientStopReason),
    /// The socket closed cleanly and the task exited
    Finished,
}

#[allow(clippy::too_many_arguments)]
async fn client_run(
    client_id: ClientId,
    tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    target: SocketAddr,
    mut rx: mpsc::Receiver<Message>,
    mut stop: oneshot::Receiver<ClientStopReason>,
    tx: mpsc::Sender<ClientResponse>,
    delay: bool,
    log: Logger,
) {
    let r = client_run_inner(
        client_id,
        tls_context,
        target,
        &mut rx,
        &mut stop,
        &tx,
        delay,
        &log,
    )
    .await;

    warn!(log, "client task is sending Done({r:?})");
    tx.send(ClientResponse::Done(r)).await.unwrap();
    while let Some(v) = rx.recv().await {
        warn!(log, "exiting client task is ignoring message {v:?}");
    }
    info!(log, "client task is exiting");
}

#[allow(clippy::too_many_arguments)]
async fn client_run_inner(
    client_id: ClientId,
    tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    target: SocketAddr,
    rx: &mut mpsc::Receiver<Message>,
    stop: &mut oneshot::Receiver<ClientStopReason>,
    tx: &mpsc::Sender<ClientResponse>,
    delay: bool,
    log: &Logger,
) -> ClientRunResult {
    // If we're reconnecting, then add a short delay to avoid constantly
    // spinning (e.g. if something is fundamentally wrong with the Downstairs)
    if delay {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    // Make connection to this downstairs.
    let sock = if target.is_ipv4() {
        TcpSocket::new_v4().unwrap()
    } else {
        TcpSocket::new_v6().unwrap()
    };

    // Set a connect timeout, and connect to the target:
    info!(log, "connecting to {target}");
    let tcp: TcpStream = tokio::select! {
        _ = sleep_until(deadline_secs(10.0))=> {
            warn!(log, "connect timeout");
            return ClientRunResult::ConnectionTimeout;
        }
        tcp = sock.connect(target) => {
            match tcp {
                Ok(tcp) => {
                    info!(log, "ds_connection connected");
                    tcp
                }
                Err(e) => {
                    warn!(
                        log, "ds_connection connect to {target} failure: {e:?}",
                    );
                    return ClientRunResult::ConnectionFailed(e);
                }
            }
        }
    };

    // We're connected; before we wrap it, set TCP_NODELAY to assure
    // that we don't get Nagle'd.
    tcp.set_nodelay(true).expect("could not set TCP_NODELAY");

    let tcp = {
        if let Some(tls_context) = &tls_context {
            // XXX these unwraps are bad!
            let config = tls_context.get_client_config().unwrap();

            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

            let server_name = tokio_rustls::rustls::ServerName::try_from(
                format!("downstairs{}", client_id).as_str(),
            )
            .unwrap();

            WrappedStream::Https(
                connector.connect(server_name, tcp).await.unwrap(),
            )
        } else {
            WrappedStream::Http(tcp)
        }
    };
    proc_stream(tcp, rx, stop, tx, log).await
}

async fn proc_stream(
    stream: WrappedStream,
    rx: &mut mpsc::Receiver<Message>,
    stop: &mut oneshot::Receiver<ClientStopReason>,
    tx: &mpsc::Sender<ClientResponse>,
    log: &Logger,
) -> ClientRunResult {
    match stream {
        WrappedStream::Http(sock) => {
            let (read, write) = sock.into_split();

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            cmd_loop(rx, stop, tx, fr, fw, log).await
        }
        WrappedStream::Https(stream) => {
            let (read, write) = tokio::io::split(stream);

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            cmd_loop(rx, stop, tx, fr, fw, log).await
        }
    }
}

async fn cmd_loop<R, W>(
    rx: &mut mpsc::Receiver<Message>,
    stop: &mut oneshot::Receiver<ClientStopReason>,
    tx: &mpsc::Sender<ClientResponse>,
    mut fr: FramedRead<R, crucible_protocol::CrucibleDecoder>,
    mut fw: FramedWrite<W, crucible_protocol::CrucibleEncoder>,
    log: &Logger,
) -> ClientRunResult
where
    R: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send + 'static,
{
    tx.send(ClientResponse::Connected)
        .await
        .expect("client_response_tx closed unexpectedly");
    loop {
        tokio::select! {
            f = fr.next() => {
                match f.transpose() {
                    Err(e) => {
                        warn!(log, "downstairs client error {e}");
                        break ClientRunResult::ReadFailed(e);
                    }
                    Ok(None) => {
                        // Downstairs disconnected
                        warn!(log, "downstairs disconnected");
                        break ClientRunResult::Finished;
                    }
                    Ok(Some(m)) => {
                        tx.send(ClientResponse::Message(m))
                            .await
                            .expect("client_response_tx closed unexpectedly");
                    }
                }
            }
            m = rx.recv() => {
                let Some(m) = m else {
                    panic!("client_request_tx closed unexpectedly");
                };
                if let Err(e) = fw.send(m).await {
                    break ClientRunResult::WriteFailed(e);
                }
            }
            s = &mut *stop => {
                match s {
                    Ok(s) => {
                        break ClientRunResult::RequestedStop(s);
                    }
                    Err(e) => {
                        panic!("client_stop_rx closed unexpectedly: {e:?}");
                    }
                }
            }
        }
    }
}

/// Returns:
/// - Ok(Some(valid_hash)) for successfully decrypted data
/// - Ok(None) if there were no block contexts and block was all 0
/// - Err otherwise
///
/// The return value of this will be stored with the job, and compared
/// between each read.
pub(crate) fn validate_encrypted_read_response(
    response: &mut ReadResponse,
    encryption_context: &EncryptionContext,
    log: &Logger,
) -> Result<Option<u64>, CrucibleError> {
    // XXX because we don't have block generation numbers, an attacker
    // downstairs could:
    //
    // 1) remove encryption context and cause a denial of service, or
    // 2) roll back a block by writing an old data and encryption context
    //
    // check that this read response contains block contexts that contain
    // (at least one) encryption context.

    if response.block_contexts.is_empty() {
        // No block context(s) in the response!
        //
        // Either this is a read of an unwritten block, or an attacker
        // removed the encryption contexts from the db. Because the Upstairs
        // will perform reconciliation before activating, and because the
        // final step of reconciliation is a flush (which will remove block
        // contexts that do not match with the extent data), we should never
        // expect to see this case unless this is a blank block.
        //
        // XXX if it's not a blank block, we may be under attack?
        assert!(response.data[..].iter().all(|&x| x == 0));
        return Ok(None);
    }

    let mut valid_hash = None;
    let mut successful_decryption = false;

    // Attempt decryption with each encryption context, and fail if all
    // do not work. The most recent encryption context will most likely
    // be the correct one so start there.
    for ctx in response.block_contexts.iter().rev() {
        let block_encryption_ctx =
            if let Some(block_encryption_ctx) = &ctx.encryption_context {
                block_encryption_ctx
            } else {
                // this block context is missing an encryption context!
                // continue to see if another block context has a valid one.
                //
                // XXX should this be an error instead?
                continue;
            };

        // Validate integrity hash before decryption
        let computed_hash = integrity_hash(&[
            &block_encryption_ctx.nonce[..],
            &block_encryption_ctx.tag[..],
            &response.data[..],
        ]);

        if computed_hash == ctx.hash {
            valid_hash = Some(ctx.hash);

            // Now that the integrity hash was verified, attempt
            // decryption.
            //
            // Note: decrypt_in_place does not overwrite the buffer if
            // it fails, otherwise we would need to copy here. There's a
            // unit test to validate this behaviour.
            use aes_gcm_siv::{Nonce, Tag};
            let decryption_result = encryption_context.decrypt_in_place(
                &mut response.data[..],
                Nonce::from_slice(&block_encryption_ctx.nonce[..]),
                Tag::from_slice(&block_encryption_ctx.tag[..]),
            );

            if decryption_result.is_ok() {
                successful_decryption = true;
                break;
            } else {
                // Only one hash + nonce + tag combination will match the
                // data that is returned. Due to the fact that nonces are
                // random for each write, even if the Guest wrote the
                // same data block 100 times, only one index will be
                // valid. The sqlite backend will return any number of block
                // contexts, where the raw file backend will only return
                // one (because it knows the active slot).
                //
                // If the computed integrity hash matched but decryption
                // failed, continue to the next contexts. the current
                // hashing algorithm (xxHash) is not a cryptographic hash
                // and is only u64, so collisions are not impossible.
                warn!(
                    log,
                    "Decryption failed even though integrity hash matched!"
                );
            }
        }
    }

    if let Some(valid_hash) = valid_hash {
        if !successful_decryption {
            // No encryption context combination decrypted this block, but
            // one valid hash was found. This can occur if the decryption
            // key doesn't match the key that the data was encrypted with.
            error!(log, "Decryption failed with correct hash");
            Err(CrucibleError::DecryptionError)
        } else {
            // Filter out contexts that don't match, and return the successful
            // hash.
            response
                .block_contexts
                .retain(|context| context.hash == valid_hash);

            Ok(Some(valid_hash))
        }
    } else {
        error!(log, "No match for integrity hash");
        for ctx in response.block_contexts.iter() {
            let block_encryption_ctx =
                if let Some(block_encryption_ctx) = &ctx.encryption_context {
                    block_encryption_ctx
                } else {
                    error!(log, "missing encryption context!");
                    continue;
                };

            let computed_hash = integrity_hash(&[
                &block_encryption_ctx.nonce[..],
                &block_encryption_ctx.tag[..],
                &response.data[..],
            ]);
            error!(
                log,
                "Expected: 0x{:x} != Computed: 0x{:x}", ctx.hash, computed_hash
            );
        }

        // no hash was correct
        Err(CrucibleError::HashMismatch)
    }
}

/// Returns:
/// - Ok(Some(valid_hash)) where the integrity hash matches
/// - Ok(None) where there is no integrity hash in the response and the
///   block is all 0
/// - Err otherwise
pub(crate) fn validate_unencrypted_read_response(
    response: &mut ReadResponse,
    log: &Logger,
) -> Result<Option<u64>, CrucibleError> {
    if !response.block_contexts.is_empty() {
        // check integrity hashes - make sure at least one is correct.
        let mut successful_hash = false;
        let computed_hash = integrity_hash(&[&response.data[..]]);

        // The most recent hash is probably going to be the right one.
        for context in response.block_contexts.iter().rev() {
            if computed_hash == context.hash {
                successful_hash = true;
                break;
            }
        }

        if successful_hash {
            // Filter out contexts that don't match, and return the
            // successful hash.
            response
                .block_contexts
                .retain(|context| context.hash == computed_hash);

            Ok(Some(computed_hash))
        } else {
            // No integrity hash was correct for this response
            error!(log, "No match computed hash:0x{:x}", computed_hash,);
            for context in response.block_contexts.iter().rev() {
                error!(log, "No match          hash:0x{:x}", context.hash);
            }
            error!(log, "Data from hash:");
            for i in 0..6 {
                error!(log, "[{}]:{}", i, response.data[i]);
            }

            Err(CrucibleError::HashMismatch)
        }
    } else {
        // No block context(s) in the response!
        //
        // Either this is a read of an unwritten block, or an attacker
        // removed the hashes from the db. Because the Upstairs will perform
        // reconciliation before activating, and because the final step of
        // reconciliation is a flush (which will remove block contexts that
        // do not match with the extent data), we should never expect to see
        // this case unless this is a blank block.
        //
        // XXX if it's not a blank block, we may be under attack?
        assert!(response.data[..].iter().all(|&x| x == 0));

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn downstairs_transition_normal() {
        // Verify the correct downstairs progression
        // New -> WA -> WQ -> Active
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
    }

    #[test]
    fn downstairs_transition_replay() {
        // Verify offline goes to replay
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        // Upstairs goes active!
        client
            .checked_state_transition(&UpstairsState::Active, DsState::Active);
        client
            .checked_state_transition(&UpstairsState::Active, DsState::Offline);
        client
            .checked_state_transition(&UpstairsState::Active, DsState::Replay);
    }

    #[test]
    fn downstairs_transition_deactivate_new() {
        // Verify deactivate goes to new
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        // Upstairs goes active!
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Active,
            DsState::Deactivated,
        );
        client.checked_state_transition(&UpstairsState::Active, DsState::New);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_new() {
        // Verify deactivate goes to new
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Deactivated,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_wa() {
        // Verify no deactivate from wa
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Deactivated,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_wq() {
        // Verify no deactivate from wq
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Deactivated,
        );
    }

    #[test]
    fn downstairs_transition_active_to_faulted() {
        // Verify active upstairs can go to faulted
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Faulted,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_disconnect_no_active() {
        // Verify no activation from disconnected
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Deactivated,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_offline_no_active() {
        // Verify no activation from offline
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_wa() {
        // Verify we can't go to the same state we are in
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_wq() {
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_active() {
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_no_new_to_offline() {
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_offline() {
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_backwards() {
        // Verify state can't go backwards
        // New -> WA -> WQ -> WA
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_bad_transition_wq() {
        // Verify error when going straight to WQ
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_replay() {
        // Verify new goes to replay will fail
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Replay,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_offline() {
        // Verify offline cannot go to WQ
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Offline,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_active() {
        // Verify active can't go back to WQ
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
    }

    #[test]
    fn downstairs_transition_active_faulted() {
        // Verify
        let mut client = DownstairsClient::test_default();
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitActive,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::WaitQuorum,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Active,
        );
        client.checked_state_transition(
            &UpstairsState::Initializing,
            DsState::Faulted,
        );
    }
}
