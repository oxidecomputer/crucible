// Copyright 2023 Oxide Computer Company
use crate::{
    cdt, integrity_hash, io_limits::ClientIOLimits, live_repair::ExtentInfo,
    upstairs::UpstairsConfig, upstairs::UpstairsState, ClientIOStateCount,
    ClientId, ConnectionMode, CrucibleDecoder, CrucibleError, DownstairsIO,
    DsState, EncryptionContext, IOState, IOop, JobId, Message, RawReadResponse,
    ReconcileIO, ReconcileIOState, RegionDefinitionStatus, RegionMetadata,
};
use crucible_common::{x509::TLSContext, ExtentId, VerboseTimeout};
use crucible_protocol::{
    MessageWriter, ReconciliationId, CRUCIBLE_MESSAGE_VERSION,
};

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{debug, error, info, o, warn, Logger};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    time::{sleep, sleep_until, Duration, Instant},
};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

// Disconnect from downstairs upstairs after 45 sec, logging a warning every 15s
pub(crate) const CLIENT_TIMEOUT: VerboseTimeout = VerboseTimeout {
    tick: Duration::from_secs(15),
    count: 3,
};

const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Delay before a client reconnects (to prevent spamming connections)
pub const CLIENT_RECONNECT_DELAY: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Handle to a running I/O task
///
/// The I/O task is "thin"; it simply forwards messages around.  The task
/// continues running until `client_request_tx` is dropped, so it is always
/// valid to send it new messages.
#[derive(Debug)]
struct ClientTaskHandle {
    /// Handle to send data to the I/O task
    ///
    /// The only thing that we send to the client is [`Message`], which is then
    /// sent out over the network.
    client_request_tx: mpsc::UnboundedSender<Message>,

    /// Handle to receive data from the I/O task
    ///
    /// The client has a variety of responses, which include [`Message`]
    /// replies, but also things like "the I/O task has stopped"
    client_response_rx: mpsc::UnboundedReceiver<ClientResponse>,

    /// One-shot sender to ask the client to open its connection
    ///
    /// This is used to hold the client (without connecting) in cases where we
    /// have deliberately deactivated this client.
    client_connect_tx: Option<oneshot::Sender<()>>,

    /// One-shot sender to stop the client
    ///
    /// This is a oneshot so that functions which stop the client don't
    /// necessarily need to be `async`.
    ///
    /// It is `None` if we have already requested that the client stop, but have
    /// not yet seen the task finished.
    client_stop_tx: Option<oneshot::Sender<ClientStopReason>>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
}

impl ConnectionId {
    fn update(&mut self) {
        self.0 += 1;
    }
}

/// Per-client data
///
/// This data structure contains client-specific state and manages communication
/// with a per-client IO task (through the `ClientTaskHandle`).
#[derive(Debug)]
pub(crate) struct DownstairsClient {
    /// Shared (static) configuration
    cfg: Arc<UpstairsConfig>,

    /// One's own client ID
    client_id: ClientId,

    /// Per-client log
    pub(crate) log: Logger,

    /// Client task IO
    ///
    /// The client task always sends `ClientResponse::Done` before stopping;
    /// this handle should never be dropped before that point.
    client_task: ClientTaskHandle,

    /// Number of jobs in each IO state
    io_state_job_count: ClientIOStateCount,

    /// Number of bytes associated with each IO state
    io_state_byte_count: ClientIOStateCount<u64>,

    /// Absolute IO limits for this client
    io_limits: ClientIOLimits,

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

    /// The `JobId` of the last flush that this downstairs has acked
    ///
    /// Note that this is a job ID, not a downstairs flush index (contrast with
    /// [`Downstairs::next_flush`], which is a flush index).
    pub(crate) last_flush: JobId,

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

    /// Accumulated statistics
    pub(crate) stats: DownstairsStats,

    /// Session ID for a clients connection to a downstairs.
    connection_id: ConnectionId,

    /// Per-client delay, written here and read by the [`ClientIoTask`]
    client_delay_us: Arc<AtomicU64>,
}

impl DownstairsClient {
    pub(crate) fn new(
        client_id: ClientId,
        cfg: Arc<UpstairsConfig>,
        target_addr: Option<SocketAddr>,
        io_limits: ClientIOLimits,
        log: Logger,
        tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    ) -> Self {
        let client_delay_us = Arc::new(AtomicU64::new(0));
        Self {
            cfg,
            client_task: Self::new_io_task(
                target_addr,
                false, // do not delay in starting the task
                false, // do not start the task until GoActive
                client_id,
                tls_context.clone(),
                client_delay_us.clone(),
                &log,
            ),
            client_id,
            io_limits,
            region_uuid: None,
            tls_context,
            log,
            target_addr,
            repair_addr: None,
            state: DsState::Connecting {
                mode: ConnectionMode::New,
                state: NegotiationState::Start {
                    auto_promote: false,
                },
            },
            last_flush: JobId(0),
            stats: DownstairsStats::default(),
            skipped_jobs: BTreeSet::new(),
            region_metadata: None,
            repair_info: None,
            io_state_job_count: ClientIOStateCount::default(),
            io_state_byte_count: ClientIOStateCount::default(),
            connection_id: ConnectionId(0),
            client_delay_us,
        }
    }

    /// Choose which `ClientAction` to apply
    ///
    /// This function is called from within a top-level `select!`, so not only
    /// must the select expressions be cancel safe, but the **bodies** must also
    /// be cancel-safe.  This is why we simply return a single value in the body
    /// of each statement.
    ///
    /// This function will wait forever if we have asked for the client task to
    /// stop, so it should only be called in a higher-level `select!`.
    pub(crate) async fn select(&mut self) -> ClientAction {
        loop {
            let out = match self.client_task.client_response_rx.recv().await {
                Some(c) => c.into(),
                None => break ClientAction::ChannelClosed,
            };
            // Ignore client responses if we have told the client to exit (we
            // still propagate other ClientAction variants, e.g. TaskStopped).
            if self.client_task.client_stop_tx.is_some()
                || !matches!(out, ClientAction::Response(..))
            {
                break out;
            }
        }
    }

    /// Send a `Message::HereIAm` via the client IO task
    pub(crate) fn send_here_i_am(&mut self) {
        self.send(Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION,
            upstairs_id: self.cfg.upstairs_id,
            session_id: self.cfg.session_id,
            gen: self.cfg.generation(),
            read_only: self.cfg.read_only,
            encrypted: self.cfg.encrypted(),
            alternate_versions: vec![],
        });
    }

    fn halt_io_task(&mut self, up_state: &UpstairsState, r: ClientStopReason) {
        info!(self.log, "halting IO task due to {r:?}");
        if let Some(t) = self.client_task.client_stop_tx.take() {
            if let Err(_e) = t.send(r) {
                warn!(self.log, "failed to send stop request")
            }
            self.checked_state_transition(up_state, DsState::Stopping(r));
        } else {
            warn!(self.log, "client task is already stopping")
        }
    }

    pub(crate) fn send(&mut self, m: Message) {
        // Normally, the client task continues running until
        // `self.client_task.client_request_tx` is dropped; as such, we should
        // always be able to send it a message.
        //
        // However, during Tokio shutdown, tasks may stop in arbitrary order.
        // We log an error but don't panic, because panicking is uncouth.
        if let Err(e) = self.client_task.client_request_tx.send(m) {
            error!(
                self.log,
                "failed to send message: {e};
                 this should only happen during shutdown"
            )
        }
    }

    /// Sets a job state, handling `io_state/byte_count` counters
    fn set_job_state(
        &mut self,
        job: &mut DownstairsIO,
        new_state: IOState,
    ) -> IOState {
        let is_running = matches!(new_state, IOState::InProgress);
        self.io_state_job_count[&new_state] += 1;
        self.io_state_byte_count[&new_state] += job.work.job_bytes();
        let old_state = job.state.insert(self.client_id, new_state);
        let was_running = matches!(old_state, IOState::InProgress);
        self.io_state_job_count[&old_state] -= 1;
        self.io_state_byte_count[&old_state] -= job.work.job_bytes();

        // Update our bytes-in-flight counter
        if was_running && !is_running {
            // Because the job is no longer running, it shouldn't count for
            // backpressure or IO limits.  Remove the backpressure guard for
            // this client, which decrements backpressure counters on drop.
            job.io_limits.take(&self.client_id);
        } else if is_running && !was_running {
            match self.io_limits.try_claim(job.work.job_bytes() as u32) {
                Ok(g) => {
                    job.io_limits.insert(self.client_id, g);
                }
                Err(e) => {
                    // We can't handle the case of "running out of permits
                    // during replay", because waiting for a permit would
                    // deadlock the worker task.  Log the error and continue.
                    warn!(
                        self.log,
                        "could not claim IO permits when replaying job: {e:?}"
                    )
                }
            }
        }

        old_state
    }

    /// Retire a job state, handling `io_state/byte_count` counters
    pub(crate) fn retire_job(&mut self, job: &DownstairsIO) {
        let state = &job.state[self.client_id];
        self.io_state_job_count[state] -= 1;
        self.io_state_byte_count[state] -= job.work.job_bytes();
    }

    /// Returns the number of jobs in each IO state
    pub(crate) fn io_state_job_count(&self) -> ClientIOStateCount {
        self.io_state_job_count
    }

    /// Returns the number of bytes associated with each IO state
    #[allow(unused)] // XXX this will be used in the future!
    pub(crate) fn io_state_byte_count(&self) -> ClientIOStateCount<u64> {
        self.io_state_byte_count
    }

    /// Returns a client-specialized copy of the job's `IOop`
    ///
    /// Dependencies are pruned if we're in live-repair, and the `extent_limit`
    /// is cleared if we're _not_ in live-repair.
    ///
    /// This is public so that we can call it in unit tests, but should
    /// generally not be called from general-purpose code.
    pub(crate) fn prune_deps(
        &self,
        ds_id: JobId,
        mut job: IOop,
        repair_min_id: Option<JobId>,
    ) -> IOop {
        if self.dependencies_need_cleanup() {
            match &mut job {
                IOop::Write { dependencies, .. }
                | IOop::WriteUnwritten { dependencies, .. }
                | IOop::Flush { dependencies, .. }
                | IOop::Barrier { dependencies, .. }
                | IOop::Read { dependencies, .. }
                | IOop::ExtentFlushClose { dependencies, .. }
                | IOop::ExtentLiveRepair { dependencies, .. }
                | IOop::ExtentLiveReopen { dependencies, .. }
                | IOop::ExtentLiveNoOp { dependencies } => {
                    self.remove_dep_if_live_repair(
                        dependencies,
                        ds_id,
                        repair_min_id.expect("must have repair_min_id"),
                    );
                }
            }
        }
        // If our downstairs is under repair, then include any extent limit sent
        // in the IOop; otherwise, clear it out
        if let IOop::Flush { extent_limit, .. } = &mut job {
            if !matches!(self.state, DsState::LiveRepair) {
                *extent_limit = None;
            }
        }
        job
    }

    /// Sets the given job's state to [`IOState::InProgress`]
    ///
    /// # Panics
    /// If the job's state is [`IOState::Done`] but the job has not been acked
    pub(crate) fn replay_job(&mut self, job: &mut DownstairsIO) {
        // If it's Done, then by definition it has been acked; test that here
        // to double-check.
        if IOState::Done == job.state[self.client_id] && !job.acked {
            panic!("[{}] This job was not acked: {:?}", self.client_id, job);
        }

        self.set_job_state(job, IOState::InProgress);
        job.replay = true;
    }

    /// Sets this job as skipped and moves it to `skipped_jobs`
    ///
    /// # Panics
    /// If the job's state is not [`IOState::InProgress`]
    pub(crate) fn skip_job(&mut self, ds_id: JobId, job: &mut DownstairsIO) {
        let prev_state = self.set_job_state(job, IOState::Skipped);
        assert!(matches!(prev_state, IOState::InProgress));
        self.skipped_jobs.insert(ds_id);
    }

    /// Returns true if it's possible that we need to clean job dependencies
    pub(crate) fn dependencies_need_cleanup(&self) -> bool {
        matches!(self.state, DsState::LiveRepair)
            && !self.skipped_jobs.is_empty()
    }

    /// Sets our state to `DsState::Reconcile`
    ///
    /// # Panics
    /// If the current state is invalid
    pub(crate) fn begin_reconcile(&mut self) {
        info!(self.log, "Transition from {:?} to Reconcile", self.state);
        let DsState::Connecting { state, mode } = &mut self.state else {
            panic!("invalid state {:?}", self.state);
        };
        assert_eq!(*state, NegotiationState::WaitQuorum);
        assert_eq!(*mode, ConnectionMode::New); // XXX Replaced?
        *state = NegotiationState::Reconcile;
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

    /// Checks whether this Downstairs is ready for the upstairs to deactivate
    ///
    /// # Panics
    /// If the downstairs is offline
    pub(crate) fn ready_to_deactivate(&self) -> bool {
        match &self.state {
            DsState::Connecting {
                mode: ConnectionMode::New,
                state: NegotiationState::Start { .. },
            } => {
                info!(
                    self.log,
                    "ready to deactivate from state {:?}", self.state
                );
                true
            }
            s => {
                info!(self.log, "not ready to deactivate due to state {s:?}");
                false
            }
        }
    }

    /// Switches the client state to Deactivated and stops the IO task
    pub(crate) fn deactivate(&mut self, up_state: &UpstairsState) {
        self.halt_io_task(up_state, ClientStopReason::Deactivated)
    }

    /// Resets this Downstairs and start a fresh connection
    ///
    /// # Panics
    /// If `self.client_task` is not `None`, or `self.target_addr` is `None`
    pub(crate) fn reinitialize(
        &mut self,
        up_state: &UpstairsState,
        can_replay: bool,
    ) {
        // Clear this Downstair's repair address, and let the YesItsMe set it.
        // This works if this Downstairs is new, reconnecting, or was replaced
        // entirely; the repair address could have changed in any of these
        // cases.
        self.repair_addr = None;

        // If the upstairs is already active (or trying to go active), then the
        // downstairs should automatically call PromoteToActive when it reaches
        // the relevant state.
        let auto_promote = match up_state {
            UpstairsState::Active | UpstairsState::GoActive(..) => true,
            UpstairsState::Initializing
            | UpstairsState::Deactivating { .. } => false,
        };

        let current = &self.state;
        let new_mode = match current {
            DsState::Active
            | DsState::Connecting {
                mode: ConnectionMode::Offline,
                ..
            } if !can_replay => Some(ConnectionMode::Faulted),
            DsState::LiveRepair
            | DsState::Stopping(ClientStopReason::Fault(..)) => {
                Some(ConnectionMode::Faulted)
            }

            DsState::Active => Some(ConnectionMode::Offline),

            DsState::Stopping(ClientStopReason::NegotiationFailed(..))
            | DsState::Stopping(ClientStopReason::Disabled)
            | DsState::Stopping(ClientStopReason::Deactivated) => {
                Some(ConnectionMode::New)
            }

            // If we have replaced a downstairs, don't forget that.
            DsState::Stopping(ClientStopReason::Replacing) => {
                Some(ConnectionMode::Replaced)
            }

            // We stay in these states through the task restart
            DsState::Connecting { .. } => None,
        };
        let new_state = new_mode.map(|mode| DsState::Connecting {
            mode,
            state: NegotiationState::Start { auto_promote },
        });

        // Jobs are skipped and replayed in `Downstairs::reinitialize`, which is
        // (probably) the caller of this function.
        if let Some(new_state) = new_state {
            self.checked_state_transition(up_state, new_state);
        }

        self.connection_id.update();

        // Restart with a short delay, connecting if we're auto-promoting
        self.start_task(true, auto_promote);
    }

    /// Returns the last flush ID handled by this client
    pub(crate) fn last_flush(&self) -> JobId {
        self.last_flush
    }

    /// Starts a client IO task, saving the handle in `self.client_task`
    ///
    /// If we are running unit tests and `self.target_addr` is not populated, we
    /// start a dummy task instead.
    ///
    /// # Panics
    /// If `self.client_task` is not `None`, or `self.target_addr` is `None` and
    /// this isn't running in test mode
    fn start_task(&mut self, delay: bool, connect: bool) {
        self.client_task = Self::new_io_task(
            self.target_addr,
            delay,
            connect,
            self.client_id,
            self.tls_context.clone(),
            self.client_delay_us.clone(),
            &self.log,
        );
    }

    fn new_io_task(
        target: Option<SocketAddr>,
        delay: bool,
        connect: bool,
        client_id: ClientId,
        tls_context: Option<Arc<TLSContext>>,
        client_delay_us: Arc<AtomicU64>,
        log: &Logger,
    ) -> ClientTaskHandle {
        #[cfg(test)]
        if let Some(target) = target {
            Self::new_network_task(
                target,
                delay,
                connect,
                client_id,
                tls_context,
                client_delay_us,
                log,
            )
        } else {
            Self::new_dummy_task(connect)
        }

        #[cfg(not(test))]
        Self::new_network_task(
            target.expect("must provide socketaddr"),
            delay,
            connect,
            client_id,
            tls_context,
            client_delay_us,
            log,
        )
    }

    fn new_network_task(
        target: SocketAddr,
        delay: bool,
        connect: bool,
        client_id: ClientId,
        tls_context: Option<Arc<TLSContext>>,
        client_delay_us: Arc<AtomicU64>,
        log: &Logger,
    ) -> ClientTaskHandle {
        // Messages in flight are limited by backpressure, so we can use
        // unbounded channels here without fear of runaway.
        let (client_request_tx, client_request_rx) = mpsc::unbounded_channel();
        let (client_response_tx, client_response_rx) =
            mpsc::unbounded_channel();
        let (client_stop_tx, client_stop_rx) = oneshot::channel();
        let (client_connect_tx, client_connect_rx) = oneshot::channel();

        let client_connect_tx = if connect {
            client_connect_tx.send(()).unwrap();
            None
        } else {
            Some(client_connect_tx)
        };

        let log = log.new(o!("" => "io task"));
        tokio::spawn(async move {
            let mut c = ClientIoTask {
                client_id,
                tls_context,
                target,
                request_rx: client_request_rx,
                response_tx: client_response_tx,
                start: client_connect_rx,
                stop: client_stop_rx,
                recv_task: ClientRxTask {
                    handle: None,
                    log: log.clone(),
                },
                delay,
                client_delay_us,
                log,
            };
            c.run().await
        });
        ClientTaskHandle {
            client_request_tx,
            client_connect_tx,
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        }
    }

    /// Starts a dummy IO task, returning its IO handle
    #[cfg(test)]
    fn new_dummy_task(connect: bool) -> ClientTaskHandle {
        let (client_request_tx, client_request_rx) = mpsc::unbounded_channel();
        let (_client_response_tx, client_response_rx) =
            mpsc::unbounded_channel();
        let (client_stop_tx, client_stop_rx) = oneshot::channel();
        let (client_connect_tx, client_connect_rx) = oneshot::channel();

        // Forget these without dropping them, so that we can send values into
        // the void!
        std::mem::forget(client_request_rx);
        std::mem::forget(client_stop_rx);
        std::mem::forget(client_connect_rx);

        ClientTaskHandle {
            client_request_tx,
            client_connect_tx: if connect {
                None
            } else {
                Some(client_connect_tx)
            },
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        }
    }

    /// Indicate that the upstairs has requested that we go active
    ///
    /// This either sent a `PromoteToActive` request directly, or schedules it
    /// to be sent once the client state reaches `WaitActive`.
    ///
    /// # Panics
    /// If we already called this function (without `reinitialize` in between),
    /// or `self.state` is invalid for promotion.
    pub(crate) fn set_active_request(&mut self) {
        if let Some(t) = self.client_task.client_connect_tx.take() {
            info!(self.log, "sending connect oneshot to client");
            if let Err(e) = t.send(()) {
                error!(
                    self.log,
                    "failed to set client as active {e:?};
                     are we shutting down?"
                );
            }
        }
        // If we're already in the point of negotiation where we're waiting to
        // go active, then immediately go active!
        match &mut self.state {
            DsState::Connecting {
                state: NegotiationState::Start { auto_promote },
                mode: ConnectionMode::New | ConnectionMode::Replaced,
            } => {
                if *auto_promote {
                    panic!("called set_active_request while already waiting")
                }
                *auto_promote = true;
                info!(
                    self.log,
                    "client set_active_request while in {:?}; waiting...",
                    self.state,
                );
            }
            DsState::Connecting {
                state: NegotiationState::WaitActive,
                mode: ConnectionMode::New | ConnectionMode::Replaced,
            } => {
                info!(
                    self.log,
                    "client set_active_request while in {:?} -> WaitForPromote",
                    self.state,
                );
                self.send(Message::PromoteToActive {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    gen: self.cfg.generation(),
                });

                let DsState::Connecting { mode, .. } = self.state else {
                    unreachable!()
                };
                self.state = DsState::Connecting {
                    state: NegotiationState::WaitForPromote,
                    mode,
                };
            }
            s => panic!("invalid state for set_active_request: {s:?}"),
        }
    }

    /// Accessor method for client connection state
    pub(crate) fn state(&self) -> DsState {
        self.state
    }

    pub(crate) fn abort_negotiation(
        &mut self,
        up_state: &UpstairsState,
        reason: ClientNegotiationFailed,
    ) {
        self.halt_io_task(up_state, reason.into());
    }

    /// Sets the current state to `DsState::Active`
    pub(crate) fn set_active(&mut self) {
        info!(self.log, "Transition from {} to Active", self.state);
        self.state = DsState::Active;
    }

    /// Checks whether the given job should be sent
    ///
    /// Returns an [`EnqueueResult`] indicating how the caller should handle the
    /// packet.
    ///
    /// If the job should be skipped, then it is added to `self.skipped_jobs`.
    /// `self.io_state_job_count` is updated with the incoming job state.
    #[must_use]
    pub(crate) fn enqueue(
        &mut self,
        ds_id: JobId,
        io: &IOop,
        last_repair_extent: Option<ExtentId>,
    ) -> EnqueueResult {
        // If a downstairs is faulted or ready for repair, we can move
        // that job directly to IOState::Skipped
        // If a downstairs is in repair, then we need to see if this
        // IO is on a repaired extent or not.  If an IO spans extents
        // where some are repaired and some are not, then this IO had
        // better have the dependencies already set to reflect the
        // requirement that a repair IO will need to finish first.
        let should_send = self.should_send(io, last_repair_extent);

        // Update our set of skipped jobs if we're not sending this one
        if matches!(should_send, EnqueueResult::Skip) {
            self.skipped_jobs.insert(ds_id);
        }

        // Update our state counters based on the job state
        let state = should_send.state();
        self.io_state_job_count[&state] += 1;
        self.io_state_byte_count[&state] += io.job_bytes();

        should_send
    }

    /// Checks whether the given job should be sent or skipped
    ///
    /// Returns an [`EnqueueResult`] indicating how the caller should handle the
    /// packet.
    #[must_use]
    fn should_send(
        &self,
        io: &IOop,
        last_repair_extent: Option<ExtentId>,
    ) -> EnqueueResult {
        match self.state {
            // We never send jobs if we're in certain inactive states
            DsState::Connecting {
                mode: ConnectionMode::Faulted | ConnectionMode::Replaced,
                ..
            }
            | DsState::Stopping(
                ClientStopReason::Fault(..)
                | ClientStopReason::Disabled
                | ClientStopReason::Replacing
                | ClientStopReason::NegotiationFailed(..),
            ) => EnqueueResult::Skip,

            // We conditionally send jobs if we're in live-repair, depending on
            // the current extent.
            DsState::LiveRepair => {
                // Pick the latest repair limit that's relevant for this
                // downstairs.  This is either the extent under repair (if
                // there are no reserved repair jobs), or the last extent
                // for which we have reserved a repair job ID; either way, the
                // caller has provided it to us.
                if io.send_io_live_repair(last_repair_extent) {
                    EnqueueResult::Send
                } else {
                    EnqueueResult::Skip
                }
            }

            // Send jobs if the client is active or offline
            //
            // Sending jobs to an offline client seems counter-intuitive, but it
            // means that those jobs are marked as InProgress, so they aren't
            // cleared out by a subsequent flush (so we'll be able to bring that
            // client back into compliance by replaying jobs).
            DsState::Active => EnqueueResult::Send,
            DsState::Connecting {
                mode: ConnectionMode::Offline,
                ..
            } => EnqueueResult::Hold,

            DsState::Stopping(ClientStopReason::Deactivated)
            | DsState::Connecting {
                mode: ConnectionMode::New,
                ..
            } => panic!(
                "enqueue should not be called from state {:?}",
                self.state
            ),
        }
    }

    /// Prepares for a new connection, then restarts the IO task
    pub(crate) fn replace(
        &mut self,
        up_state: &UpstairsState,
        new: SocketAddr,
    ) {
        self.target_addr = Some(new);

        self.region_metadata = None;
        self.stats.replaced += 1;

        self.halt_io_task(up_state, ClientStopReason::Replacing);
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
        _up_state: &UpstairsState,
        new_state: DsState,
    ) {
        // TODO reimplement all of the checks
        self.state = new_state;
    }

    /// Sets `repair_info` to `None` and increments `live_repair_aborted`
    pub(crate) fn clear_repair_state(&mut self) {
        self.repair_info = None;
        self.stats.live_repair_aborted += 1;
    }

    /// Sets the state to `Fault` and restarts the IO task
    pub(crate) fn fault(
        &mut self,
        up_state: &UpstairsState,
        reason: ClientFaultReason,
    ) {
        self.halt_io_task(up_state, reason.into());
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

    /// Handles a single IO operation
    ///
    /// If this is a read response, then the values in `responses` must
    /// _already_ be decrypted, with validated contexts stored in
    /// `responses.blocks`.
    pub(crate) fn process_io_completion(
        &mut self,
        ds_id: JobId,
        job: &mut DownstairsIO,
        responses: Result<RawReadResponse, CrucibleError>,
        extent_info: Option<ExtentInfo>,
    ) {
        if job.state[self.client_id] == IOState::Skipped {
            // This job was already marked as skipped, and at that time
            // all required action was taken on it.  We can drop any more
            // processing of it here and return.
            warn!(self.log, "Dropping already skipped job {}", ds_id);
            return;
        }

        let new_state = match responses {
            Ok(read_data) => {
                // Messages have already been decrypted out-of-band
                match job.work {
                    IOop::Read { .. } => {
                        assert!(!read_data.blocks.is_empty());
                        assert!(extent_info.is_none());
                        if job.data.is_none() {
                            job.data = Some(read_data);
                        } else {
                            // If another job has finished already, we compare
                            // our read hash to that and verify they are the
                            // same.
                            debug!(self.log, "Read already AckReady {ds_id}");
                            let job_blocks = &job.data.as_ref().unwrap().blocks;
                            if job_blocks != &read_data.blocks {
                                // XXX This error needs to go to Nexus
                                // XXX This will become the "force all
                                // downstairs to stop and refuse to restart"
                                // mode.
                                let msg = format!(
                                    "[{}] read hash mismatch on {} \n\
                                        Expected {:x?}\n\
                                        Computed {:x?}\n\
                                        job: {:?}",
                                    self.client_id,
                                    ds_id,
                                    job_blocks,
                                    read_data.blocks,
                                    job,
                                );
                                if job.replay {
                                    info!(self.log, "REPLAY {msg}");
                                } else {
                                    panic!("{msg}");
                                }
                            }
                        }
                    }
                    IOop::Write { .. }
                    | IOop::WriteUnwritten { .. }
                    | IOop::Barrier { .. } => {
                        assert!(read_data.blocks.is_empty());
                        assert!(read_data.data.is_empty());
                        assert!(extent_info.is_none());
                    }
                    IOop::Flush { .. } => {
                        assert!(read_data.blocks.is_empty());
                        assert!(read_data.data.is_empty());
                        assert!(extent_info.is_none());

                        self.last_flush = ds_id;
                    }
                    IOop::ExtentFlushClose { .. } => {
                        assert!(read_data.blocks.is_empty());
                        assert!(read_data.data.is_empty());

                        let ci = self.repair_info.replace(extent_info.unwrap());
                        if ci.is_some() {
                            panic!(
                            "[{}] Unexpected repair found on insertion: {:?}",
                            self.client_id, ci
                        );
                        }
                    }
                    IOop::ExtentLiveRepair { .. }
                    | IOop::ExtentLiveReopen { .. }
                    | IOop::ExtentLiveNoOp { .. } => {
                        assert!(read_data.blocks.is_empty());
                        assert!(read_data.data.is_empty());
                        assert!(extent_info.is_none());
                    }
                }
                IOState::Done
            }
            Err(e) => {
                // The downstairs sent us this error
                error!(
                    self.log,
                    "DS Reports error {e:?} on job {}, {:?} EC", ds_id, job,
                );
                match (&job.work, &e) {
                    // Some errors can be returned without considering the
                    // Downstairs bad. For example, it's still an error if a
                    // snapshot exists already but we should not increment
                    // `downstairs_errors` and transition that Downstairs to
                    // Failed - that downstairs is still able to serve IO.
                    (
                        IOop::Flush {
                            snapshot_details: Some(..),
                            ..
                        },
                        CrucibleError::SnapshotExistsAlready(..),
                    ) => (),

                    // If a read job fails, we sometimes need to panic
                    (IOop::Read { .. }, CrucibleError::HashMismatch) => {
                        panic!(
                            "{} [{}] {} read hash mismatch {:?} {:?}",
                            self.cfg.session_id, self.client_id, ds_id, e, job
                        );
                    }
                    (IOop::Read { .. }, CrucibleError::DecryptionError) => {
                        panic!(
                            "[{}] {} read decrypt error {:?} {:?}",
                            self.client_id, ds_id, e, job
                        );
                    }

                    // Other IO errors increment a counter and (higher up the
                    // call stack) cause us to mark this Downstairs as faulted.
                    //
                    // XXX: Errors should be reported to nexus
                    _ => {
                        self.stats.downstairs_errors += 1;
                    }
                }
                IOState::Error(e)
            }
        };

        // Update the state, maintaining various counters
        let old_state = self.set_job_state(job, new_state);

        // The job must have been InProgress
        assert_eq!(
            old_state,
            IOState::InProgress,
            "[{}] Job {ds_id} completed while not InProgress: {job:?}",
            self.client_id,
        );
    }

    /// Mark this client as disabled and halt its IO task
    ///
    /// The IO task will automatically restart in the main event handler
    pub(crate) fn disable(&mut self, up_state: &UpstairsState) {
        self.halt_io_task(up_state, ClientStopReason::Disabled);
    }

    /// Skips from `LiveRepairReady` to `Active`; a no-op otherwise
    ///
    /// # Panics
    /// If this downstairs is not read-only
    pub(crate) fn skip_live_repair(&mut self, up_state: &UpstairsState) {
        let DsState::Connecting { state, .. } = self.state else {
            return;
        };
        if state == NegotiationState::LiveRepairReady {
            assert!(self.cfg.read_only);

            // TODO: could we do this transition early, by automatically
            // skipping LiveRepairReady if read-only?
            self.checked_state_transition(up_state, DsState::Active);
            self.stats.ro_lr_skipped += 1;
        }
    }

    /// Moves from `LiveRepairReady` to `LiveRepair`, a no-op otherwise
    pub(crate) fn start_live_repair(&mut self, up_state: &UpstairsState) {
        let DsState::Connecting { state, .. } = self.state else {
            return;
        };
        if state == NegotiationState::LiveRepairReady {
            self.checked_state_transition(up_state, DsState::LiveRepair);
        }
    }

    /// Continues the negotiation and initial reconciliation process
    ///
    /// Returns an error if the upstairs should go inactive, which occurs if the
    /// error is at or after `Message::YouAreNowActive`.
    ///
    /// Returns a flag indicating how to proceed
    pub(crate) fn continue_negotiation(
        &mut self,
        m: Message,
        up_state: &UpstairsState,
        ddef: &mut RegionDefinitionStatus,
    ) -> Result<NegotiationResult, CrucibleError> {
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
         * After receiving our last flush, we now replay all saved jobs for this
         * Downstairs and skip ahead to NegotiationState::Done
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
         *    upstairs. We set the downstairs to DsState::Active and the while
         *    loop is exited.
         */
        let DsState::Connecting { state, mode } = &mut self.state else {
            error!(
                self.log,
                "tried to continue negotiation while not connecting"
            );
            self.abort_negotiation(
                up_state,
                ClientNegotiationFailed::BadNegotiationOrder,
            );
            return Ok(NegotiationResult::NotDone);
        };
        let mut out = NegotiationResult::NotDone;
        let mode = *mode; // mode is immutable here
        match m {
            Message::YesItsMe {
                version,
                repair_addr,
            } => {
                let NegotiationState::Start { auto_promote } = *state else {
                    error!(self.log, "got version already");
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::BadNegotiationOrder,
                    );
                    return Ok(NegotiationResult::NotDone);
                };
                if version != CRUCIBLE_MESSAGE_VERSION {
                    error!(
                        self.log,
                        "expected version {}, got {}",
                        CRUCIBLE_MESSAGE_VERSION,
                        version
                    );
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::Incompatible,
                    );
                    return Ok(NegotiationResult::NotDone);
                }
                self.repair_addr = Some(repair_addr);
                if auto_promote {
                    *state = NegotiationState::WaitForPromote;
                    self.send(Message::PromoteToActive {
                        upstairs_id: self.cfg.upstairs_id,
                        session_id: self.cfg.session_id,
                        gen: self.cfg.generation(),
                    });
                    info!(
                        self.log,
                        "version negotiation from state {:?}", self.state
                    );
                } else {
                    // Nothing to do here, wait for set_active_request
                    *state = NegotiationState::WaitActive;
                }
            }
            Message::VersionMismatch { version } => {
                error!(
                    self.log,
                    "downstairs version is {version}, \
                     ours is {CRUCIBLE_MESSAGE_VERSION}"
                );
                self.abort_negotiation(
                    up_state,
                    ClientNegotiationFailed::Incompatible,
                );
            }
            Message::EncryptedMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs encrypted is {expected}, ours is {}",
                    self.cfg.encrypted()
                );
                self.abort_negotiation(
                    up_state,
                    ClientNegotiationFailed::Incompatible,
                );
            }
            Message::ReadOnlyMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs read_only is {expected}, ours is {}",
                    self.cfg.read_only,
                );
                self.abort_negotiation(
                    up_state,
                    ClientNegotiationFailed::Incompatible,
                );
            }
            Message::YouAreNowActive {
                upstairs_id,
                session_id,
                gen,
            } => {
                if *state != NegotiationState::WaitForPromote {
                    error!(
                        self.log,
                        "Received YouAreNowActive out of order! {state:?}",
                    );
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::BadNegotiationOrder,
                    );
                    // XXX why isn't this an error?
                    return Ok(NegotiationResult::NotDone);
                }

                let match_uuid = self.cfg.upstairs_id == upstairs_id;
                let match_session = self.cfg.session_id == session_id;
                let upstairs_gen = self.cfg.generation();
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
                    if !match_gen {
                        let gen_error = format!(
                            "Generation requested:{} found:{}",
                            gen, upstairs_gen,
                        );
                        self.abort_negotiation(
                            up_state,
                            ClientNegotiationFailed::Incompatible,
                        );
                        return Err(CrucibleError::GenerationNumberTooLow(
                            gen_error,
                        ));
                    } else {
                        self.abort_negotiation(
                            up_state,
                            ClientNegotiationFailed::Incompatible,
                        );
                        return Err(CrucibleError::UuidMismatch);
                    }
                }

                *state = NegotiationState::WaitForRegionInfo;
                self.send(Message::RegionInfoPlease);
            }
            Message::RegionInfo { region_def } => {
                if *state != NegotiationState::WaitForRegionInfo {
                    error!(self.log, "Received RegionInfo out of order!");
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::BadNegotiationOrder,
                    );
                    return Ok(NegotiationResult::NotDone);
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
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::Incompatible,
                    );
                    return Ok(NegotiationResult::NotDone);
                }

                /*
                 * TODO: Verify that a new downstairs does not share the same
                 * UUID with an existing downstairs.
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
                        if mode == ConnectionMode::Replaced {
                            warn!(
                                self.log,
                                "Replace downstairs uuid:{} with {}",
                                uuid,
                                region_def.uuid(),
                            );
                        } else {
                            // If we are not yet active (and, as such, we have
                            // not finished reconciliation), we can replace a
                            // downstairs here.
                            match up_state {
                                UpstairsState::Initializing
                                | UpstairsState::GoActive(_) => {
                                    warn!(
                                        self.log,
                                        "Replace {} with {} before active",
                                        uuid,
                                        region_def.uuid(),
                                    );
                                }
                                _ => {
                                    panic!(
                                        "New client:{} uuid:{} does not match \
                                         existing {} ds_state:{:?} \
                                         up_state:{:?}",
                                        self.client_id,
                                        region_def.uuid(),
                                        uuid,
                                        self.state,
                                        up_state,
                                    );
                                }
                            }
                        }
                    } else {
                        info!(self.log, "Returning UUID:{} matches", uuid);
                    }
                }

                /*
                 * If this is a new downstairs connection, insert the UUID.
                 * If this is a replacement downstairs, insert the UUID.
                 * If it is an existing UUID, we already compared and it is
                 * good, so the insert is unnecessary, but will result in the
                 * same UUID.
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
                match mode {
                    ConnectionMode::Offline => {
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
                        *state = NegotiationState::GetLastFlush;

                        self.send(Message::LastFlush {
                            last_flush_number: lf,
                        });
                    }
                    ConnectionMode::New
                    | ConnectionMode::Faulted
                    | ConnectionMode::Replaced => {
                        /*
                         * Ask for the current version of all extents.
                         */
                        *state = NegotiationState::GetExtentVersions;
                        self.send(Message::ExtentVersionsPlease);
                    }
                }
            }
            Message::LastFlushAck { last_flush_number } => {
                if *state != NegotiationState::GetLastFlush {
                    error!(self.log, "Received LastFlushAck out of order!");
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::BadNegotiationOrder,
                    );
                    // TODO should we trigger set_inactive?
                    return Ok(NegotiationResult::NotDone);
                }
                assert_eq!(
                    mode,
                    ConnectionMode::Offline,
                    "got LastFlushAck in bad state {:?}",
                    self.state
                );
                info!(
                    self.log,
                    "Replied this last flush ID: {last_flush_number}"
                );
                assert_eq!(self.last_flush, last_flush_number);

                // Immediately set the state to Active, and return a flag
                // indicating that jobs should be replayed.
                self.checked_state_transition(up_state, DsState::Active);
                out = NegotiationResult::Replay;
            }
            Message::ExtentVersions {
                gen_numbers,
                flush_numbers,
                dirty_bits,
            } => {
                if *state != NegotiationState::GetExtentVersions {
                    error!(self.log, "Received ExtentVersions out of order!");
                    self.abort_negotiation(
                        up_state,
                        ClientNegotiationFailed::BadNegotiationOrder,
                    );
                    // TODO should we trigger set_inactive?
                    return Ok(NegotiationResult::NotDone);
                }
                match mode {
                    ConnectionMode::New => {
                        *state = NegotiationState::WaitQuorum;
                        out = NegotiationResult::WaitQuorum;
                    }
                    // Special case: if a downstairs is replaced while we're
                    // still trying to go active, then we use the WaitQuorum
                    // path instead of LiveRepair.
                    ConnectionMode::Replaced
                        if matches!(
                            up_state,
                            UpstairsState::Initializing
                                | UpstairsState::GoActive(..)
                        ) =>
                    {
                        *state = NegotiationState::WaitQuorum;
                        out = NegotiationResult::WaitQuorum;
                    }

                    ConnectionMode::Faulted | ConnectionMode::Replaced => {
                        *state = NegotiationState::LiveRepairReady;
                        out = NegotiationResult::LiveRepair;
                    }
                    ConnectionMode::Offline => {
                        panic!(
                            "got ExtentVersions from invalid state {:?}",
                            self.state
                        );
                    }
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
            }
            m => panic!("invalid message in continue_negotiation: {m:?}"),
        }
        Ok(out)
    }

    /// Sends the next reconciliation job to all clients
    ///
    /// The `job` argument should be a reference to
    /// `Downstairs::reconcile_current_work`, and its state is updated.
    pub(crate) fn send_next_reconciliation_req(
        &mut self,
        job: &mut ReconcileIO,
    ) {
        // If someone has moved us out of reconcile, this is a logic error
        if !matches!(
            self.state,
            DsState::Connecting {
                state: NegotiationState::Reconcile,
                mode: ConnectionMode::New
            }
        ) {
            panic!(
                "[{}] should still be in reconcile, not {:?}",
                self.client_id, self.state
            );
        }
        let prev_state = job
            .state
            .insert(self.client_id, ReconcileIOState::InProgress);
        assert_eq!(prev_state, ReconcileIOState::New);

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
                    info!(self.log, "sending reconcile request {repair_id:?}");
                    self.send(job.op.clone());
                } else {
                    // Skip this job for this Downstairs, since only the target
                    // clients need to do the reconcile.
                    let prev_state = job
                        .state
                        .insert(self.client_id, ReconcileIOState::Skipped);
                    assert_eq!(prev_state, ReconcileIOState::InProgress);
                    debug!(self.log, "no action needed request {repair_id:?}");
                }
            }
            Message::ExtentFlush {
                repair_id,
                client_id,
                ..
            } => {
                if *client_id == self.client_id {
                    debug!(self.log, "sending flush request {repair_id:?}");
                    self.send(job.op.clone());
                } else {
                    debug!(self.log, "skipping flush request {repair_id:?}");
                    // Skip this job for this Downstairs, since it's narrowly
                    // aimed at a different client.
                    let prev_state = job
                        .state
                        .insert(self.client_id, ReconcileIOState::Skipped);
                    assert_eq!(prev_state, ReconcileIOState::InProgress);
                }
            }
            Message::ExtentReopen { .. } | Message::ExtentClose { .. } => {
                // All other reconcile ops are sent as-is
                self.send(job.op.clone());
            }
            m => panic!("invalid reconciliation request {m:?}"),
        }
    }

    /// When a reconciliation job is done, mark it as complete for this client
    ///
    /// Returns `true` if the job is done for all clients
    pub(crate) fn on_reconciliation_job_done(
        &mut self,
        reconcile_id: ReconciliationId,
        job: &mut ReconcileIO,
    ) -> bool {
        let old_state =
            job.state.insert(self.client_id, ReconcileIOState::Done);
        assert_eq!(old_state, ReconcileIOState::InProgress);
        assert_eq!(job.id, reconcile_id);
        job.state.iter().all(|s| {
            matches!(s, ReconcileIOState::Done | ReconcileIOState::Skipped)
        })
    }

    pub(crate) fn total_live_work(&self) -> usize {
        self.io_state_job_count.in_progress as usize
    }

    pub(crate) fn total_bytes_outstanding(&self) -> usize {
        self.io_state_byte_count.in_progress as usize
    }

    /// Returns a unique ID for the current connection, or `None`
    ///
    /// This can be used to disambiguate between messages returned from
    /// different connections to the same Downstairs.
    pub(crate) fn get_connection_id(&self) -> Option<ConnectionId> {
        if self.client_task.client_stop_tx.is_some() {
            Some(self.connection_id)
        } else {
            None
        }
    }

    /// Sets the per-client delay
    pub(crate) fn set_delay_us(&self, delay: u64) {
        self.client_delay_us.store(delay, Ordering::Relaxed);
    }

    /// Looks up the per-client delay
    pub(crate) fn get_delay_us(&self) -> u64 {
        self.client_delay_us.load(Ordering::Relaxed)
    }

    #[cfg(feature = "notify-nexus")]
    pub(crate) fn id(&self) -> Option<Uuid> {
        self.region_uuid
    }
}

/// Tracks client negotiation progress
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum NegotiationState {
    /// Initial state, waiting to hear `YesItsMe` from the client
    ///
    /// Once this message is heard, transitions to either `WaitActive` (if
    /// `auto_promote` is `false`) or `WaitQuorum` (if `auto_promote` is `true`)
    Start {
        auto_promote: bool,
    },

    /// Waiting for activation by the guest
    WaitActive,

    /// Waiting for the minimum number of downstairs to be present.
    WaitQuorum,

    WaitForPromote,
    WaitForRegionInfo,
    GetLastFlush,
    GetExtentVersions,

    /// Initial startup, downstairs are repairing from each other.
    Reconcile,

    /// Waiting for live-repair to begin
    LiveRepairReady,
}

/// Result value returned when negotiation is complete
pub(crate) enum NegotiationResult {
    NotDone,
    WaitQuorum,
    Replay,
    LiveRepair,
}

/// Result value from [`DownstairsClient::enqueue`]
pub(crate) enum EnqueueResult {
    /// The given job should be marked as in progress and sent
    Send,

    /// The given job should be marked as in progress, but not sent
    ///
    /// This is used when the Downstairs is Offline; we want to mark the job as
    /// in-progress so that it's eligible for replay, but the job should not
    /// actually go out on the wire.
    Hold,

    /// The job should be marked as skipped and not sent
    Skip,
}

impl EnqueueResult {
    pub(crate) fn state(&self) -> IOState {
        match self {
            EnqueueResult::Send | EnqueueResult::Hold => IOState::InProgress,
            EnqueueResult::Skip => IOState::Skipped,
        }
    }
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
}

/// When the upstairs halts the IO client task, it must provide a reason
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ClientStopReason {
    /// We are about to replace the client task
    Replacing,

    /// We have disabled the downstairs client for some reason
    ///
    /// (for example, we have received `Message::YouAreNoLongerActive`)
    Disabled,

    /// The upstairs has requested that we deactivate
    Deactivated,

    /// Something went wrong during negotiation
    #[allow(unused)] // logged in debug messages
    NegotiationFailed(ClientNegotiationFailed),

    /// We have explicitly faulted the client
    #[allow(unused)] // logged in debug messages
    Fault(ClientFaultReason),
}

/// Subset of [`ClientStopReason`] for faulting a client
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ClientNegotiationFailed {
    /// Reconcile failed and we're restarting
    FailedReconcile,

    /// Negotiation message received out of order
    BadNegotiationOrder,

    /// Negotiation says that we are incompatible
    Incompatible,
}

impl From<ClientNegotiationFailed> for ClientStopReason {
    fn from(f: ClientNegotiationFailed) -> ClientStopReason {
        ClientStopReason::NegotiationFailed(f)
    }
}

/// Subset of [`ClientStopReason`] for faulting a client
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ClientFaultReason {
    /// Received an error from some non-recoverable IO (write or flush)
    IOError,

    /// Live-repair failed
    FailedLiveRepair,

    /// Too many jobs in the queue
    TooManyOutstandingJobs,

    /// Too many bytes in the queue
    TooManyOutstandingBytes,

    /// The upstairs has requested that we deactivate when we were offline
    OfflineDeactivated,

    /// The Upstairs has dropped jobs that would be needed for replay
    IneligibleForReplay,

    #[cfg(test)]
    RequestedFault,
}

impl From<ClientFaultReason> for ClientStopReason {
    fn from(f: ClientFaultReason) -> ClientStopReason {
        ClientStopReason::Fault(f)
    }
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

impl From<ClientResponse> for ClientAction {
    fn from(c: ClientResponse) -> Self {
        match c {
            ClientResponse::Connected => ClientAction::Connected,
            ClientResponse::Message(m) => ClientAction::Response(m),
            ClientResponse::Done(r) => ClientAction::TaskStopped(r),
        }
    }
}

/// Value returned by the `client_run` task
#[derive(Debug)]
pub(crate) enum ClientRunResult {
    /// The initial connection timed out
    ConnectionTimeout,
    /// We failed to make the initial connection
    #[allow(dead_code)]
    ConnectionFailed(std::io::Error),
    /// We experienced a timeout after connecting
    Timeout,
    /// A socket write failed
    #[allow(dead_code)]
    WriteFailed(anyhow::Error),
    /// We received an error while reading from the connection
    #[allow(dead_code)]
    ReadFailed(anyhow::Error),
    /// The `DownstairsClient` requested that the task stop, so it did
    #[allow(dead_code)]
    RequestedStop(ClientStopReason),
    /// The socket closed cleanly and the task exited
    Finished,
    /// One of the queues used to communicate with the main task closed
    ///
    /// This should only occur during program exit, when tasks are destroyed in
    /// arbitrary order.
    QueueClosed,
    /// The receive task has been cancelled
    ///
    /// This should only occur during program exit, when tasks are cancelled in
    /// arbitrary order (so the main client task may be awaiting the rx task
    /// when the latter is cancelled)
    ReceiveTaskCancelled,
}

/// Data structure to hold context for the client IO task
///
/// Client IO is managed by two tasks:
/// - The tx task, which calls `ClientIoTask::run`, sends messages from the main
///   task to the downstairs via a socket
/// - The rx task, which is spawned within `ClientIoTask::run` (and is not
///   publicly visible) receives messages from the socket and sends them
///   directly to the main task.
///
/// Splitting tx and rx is important, because it means that one or the other
/// should always be able to make progress.
struct ClientIoTask {
    client_id: ClientId,
    tls_context: Option<Arc<crucible_common::x509::TLSContext>>,
    target: SocketAddr,

    /// Request channel from the main task
    request_rx: mpsc::UnboundedReceiver<Message>,

    /// Reply channel to the main task
    response_tx: mpsc::UnboundedSender<ClientResponse>,

    /// Oneshot used to start the task
    start: oneshot::Receiver<()>,

    /// Oneshot used to stop the task
    stop: oneshot::Receiver<ClientStopReason>,

    /// Delay on startup, to avoid a busy-loop if connections always fail
    delay: bool,

    /// Handle for the rx task
    recv_task: ClientRxTask,

    /// Shared handle to receive per-client backpressure delay
    ///
    /// Written by the [`DownstairsClient`] and read by the IO task
    client_delay_us: Arc<AtomicU64>,

    log: Logger,
}

/// Handle for the rx side of client IO
///
/// This is a convenient wrapper so that we can join the task exactly once,
/// aborting if the wrapper is dropped without being joined.
struct ClientRxTask {
    handle: Option<tokio::task::JoinHandle<ClientRunResult>>,
    log: Logger,
}

impl ClientRxTask {
    /// Waits for the client IO task to end
    ///
    /// # Panics
    /// If the `JoinHandle` returns a `JoinError`, or this is called without an
    /// IO handle (i.e. before the task is started or after it has been joined).
    async fn join(&mut self) -> ClientRunResult {
        let Some(t) = self.handle.as_mut() else {
            panic!("cannot join client rx task twice")
        };
        let out = match t.await {
            Ok(r) => r,
            Err(e) if e.is_cancelled() => {
                warn!(
                    self.log,
                    "client task was cancelled without us; \
                     hopefully the program is exiting"
                );
                ClientRunResult::ReceiveTaskCancelled
            }
            Err(e) => {
                panic!("join error on recv_task: {e:?}");
            }
        };
        // The IO task has finished, one way or another
        self.handle.take();
        out
    }
}

impl Drop for ClientRxTask {
    fn drop(&mut self) {
        if let Some(t) = self.handle.take() {
            t.abort();
        }
    }
}

impl ClientIoTask {
    async fn run(&mut self) {
        let r = self.run_inner().await;

        warn!(self.log, "client task is sending Done({r:?})");
        if self.response_tx.send(ClientResponse::Done(r)).is_err() {
            warn!(
                self.log,
                "client task could not reply to main task; shutting down?"
            );
        }
        while let Some(v) = self.request_rx.recv().await {
            warn!(self.log, "exiting client task is ignoring message {v}");
        }
        info!(self.log, "client task is exiting");
    }

    async fn run_inner(&mut self) -> ClientRunResult {
        // If we're reconnecting, then add a short delay to avoid constantly
        // spinning (e.g. if something is fundamentally wrong with the
        // Downstairs)
        //
        // The upstairs can still stop us here, e.g. if we need to transition
        // from Offline -> Faulted because we hit a job limit, that bounces the
        // IO task (whether it *should* is debatable).
        if self.delay {
            tokio::select! {
                s = &mut self.stop => {
                    warn!(self.log, "client IO task stopped during sleep");
                    return match s {
                        Ok(s) =>
                            ClientRunResult::RequestedStop(s),
                        Err(e) => {
                            warn!(
                                self.log,
                               "client_stop_rx closed unexpectedly: {e:?}"
                            );
                            ClientRunResult::QueueClosed
                        }
                    }
                }
                _ = tokio::time::sleep(CLIENT_RECONNECT_DELAY) => {
                    // this is fine
                },
            }
        }

        // Wait for the start oneshot to fire.  This may happen immediately, but
        // not necessarily (for example, if the client was deactivated).  We
        // also wait for the stop oneshot here, in case someone decides to stop
        // the IO task before it tries to connect.
        tokio::select! {
            s = &mut self.start => {
                if let Err(e) = s {
                    warn!(self.log, "failed to await start oneshot: {e}");
                    return ClientRunResult::QueueClosed;
                }
                // Otherwise, continue as usual
            }
            s = &mut self.stop => {
                warn!(self.log, "client IO task stopped before connecting");
                return match s {
                    Ok(s) =>
                        ClientRunResult::RequestedStop(s),
                    Err(e) => {
                        warn!(
                            self.log,
                           "client_stop_rx closed unexpectedly: {e:?}"
                        );
                        ClientRunResult::QueueClosed
                    }
                }
            }
        }

        // Make connection to this downstairs.
        let sock = if self.target.is_ipv4() {
            TcpSocket::new_v4().unwrap()
        } else {
            TcpSocket::new_v6().unwrap()
        };

        // Set a connect timeout, and connect to the target:
        info!(self.log, "connecting to {}", self.target);
        let tcp: TcpStream = tokio::select! {
            _ = sleep(Duration::from_secs(10))=> {
                warn!(self.log, "connect timeout");
                return ClientRunResult::ConnectionTimeout;
            }
            tcp = sock.connect(self.target) => {
                match tcp {
                    Ok(tcp) => {
                        info!(
                            self.log,
                            "ds_connection connected from {:?}",
                            tcp.local_addr()
                        );
                        tcp
                    }
                    Err(e) => {
                        warn!(
                            self.log,
                            "ds_connection connect to {} failure: {e:?}",
                            self.target,
                        );
                        return ClientRunResult::ConnectionFailed(e);
                    }
                }
            }
            s = &mut self.stop => {
                warn!(self.log, "client IO task stopped during connection");
                return match s {
                    Ok(s) =>
                        ClientRunResult::RequestedStop(s),
                    Err(e) => {
                        warn!(
                            self.log,
                           "client_stop_rx closed unexpectedly: {e:?}"
                        );
                        ClientRunResult::QueueClosed
                    }
                }
            }
        };

        // We're connected; before we wrap it, set TCP_NODELAY to assure
        // that we don't get Nagle'd.
        tcp.set_nodelay(true).expect("could not set TCP_NODELAY");

        if let Some(tls_context) = &self.tls_context {
            // XXX these unwraps are bad!
            let config = tls_context.get_client_config().unwrap();

            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

            let server_name = tokio_rustls::rustls::ServerName::try_from(
                format!("downstairs{}", self.client_id).as_str(),
            )
            .unwrap();

            let sock = connector.connect(server_name, tcp).await.unwrap();
            let (read, write) = tokio::io::split(sock);
            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = MessageWriter::new(write);
            self.cmd_loop(fr, fw).await
        } else {
            let (read, write) = tcp.into_split();
            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = MessageWriter::new(write);
            self.cmd_loop(fr, fw).await
        }
    }

    async fn cmd_loop<R, W>(
        &mut self,
        fr: FramedRead<R, crucible_protocol::CrucibleDecoder>,
        mut fw: MessageWriter<W>,
    ) -> ClientRunResult
    where
        R: tokio::io::AsyncRead
            + std::marker::Unpin
            + std::marker::Send
            + 'static,
        W: tokio::io::AsyncWrite
            + std::marker::Unpin
            + std::marker::Send
            + 'static,
    {
        self.response_tx
            .send(ClientResponse::Connected)
            .expect("client_response_tx closed unexpectedly");

        // Spawn a separate task to receive data over the network, so that we
        // can always make progress and keep the socket buffer from filling up.
        self.recv_task.handle = Some(tokio::spawn(rx_loop(
            self.response_tx.clone(),
            fr,
            self.log.clone(),
            self.client_id,
        )));

        let mut ping_deadline = Instant::now() + PING_INTERVAL;
        let mut ping_count = 0u64;
        loop {
            tokio::select! {
                join_result = self.recv_task.join() => {
                    break join_result
                }

                m = self.request_rx.recv() => {
                    let Some(m) = m else {
                        warn!(
                            self.log,
                            "client request queue closed unexpectedly; \
                             is the program exiting?"
                         );
                        break ClientRunResult::QueueClosed;
                    };

                    if let Err(e) = self.write(&mut fw, m).await {
                        break e;
                    }
                }

                _ = sleep_until(ping_deadline) => {
                    ping_deadline = Instant::now() + PING_INTERVAL;
                    ping_count += 1;
                    cdt::ds__ping__sent!(|| (ping_count, self.client_id.get()));

                    let m = Message::Ruok;
                    if let Err(e) = self.write(&mut fw, m).await {
                        break e;
                    }
                }

                s = &mut self.stop => {
                    match s {
                        Ok(s) => {
                            break ClientRunResult::RequestedStop(s);
                        }

                        Err(e) => {
                            warn!(
                                self.log,
                                "client_stop_rx closed unexpectedly: {e:?}"
                            );
                            break ClientRunResult::QueueClosed;
                        }
                    }
                }
            }
        }
    }

    /// Writes a message to the given `AsyncWrite` stream, with cancel detection
    ///
    /// We wait for three possible outcomes:
    ///
    /// - The write completes (this is the normal outcome), with or without an
    ///   error.  Any error is returned.
    /// - The client rx task times out or exits for some other reason.  This is
    ///   definitionally a termination condition, so it is returned as an
    ///   `Err(..)` variant.
    /// - The main task requests that the IO task stop through the oneshot
    ///   channel.  This is returned as `ClientRunResult::RequestedStop`
    ///
    /// Any error returned here is an indication that the client task should
    /// stop immediately.
    async fn write<W>(
        &mut self,
        fw: &mut MessageWriter<W>,
        m: Message,
    ) -> Result<(), ClientRunResult>
    where
        W: tokio::io::AsyncWrite
            + std::marker::Unpin
            + std::marker::Send
            + 'static,
    {
        // Delay communication with this client based on backpressure, to keep
        // the three clients relatively in sync with each other.
        //
        // We don't need to delay writes, because they're already constrained by
        // the global backpressure system and cannot build up an unbounded
        // queue.  This is admittedly quite subtle; see crucible#1167 for
        // discussions and graphs.
        if !matches!(m, Message::Write { .. } | Message::WriteUnwritten { .. })
        {
            let d = self.client_delay_us.load(Ordering::Relaxed);
            if d > 0 {
                tokio::time::sleep(Duration::from_micros(d)).await;
            }
        }

        update_net_start_probes(&m, self.client_id);
        // There's some duplication between this function and `cmd_loop` above,
        // but it's not obvious whether there's a cleaner way to organize stuff.
        tokio::select! {
            r = fw.send(m) => {
                if let Err(e) = r {
                    Err(ClientRunResult::WriteFailed(e.into()))
                } else {
                    Ok(())
                }
            }
            s = &mut self.stop => {
                match s {
                    Ok(s) => {
                        Err(ClientRunResult::RequestedStop(s))
                    }

                    Err(e) => {
                        warn!(
                            self.log,
                            "client_stop_rx closed unexpectedly: {e:?}"
                        );
                        Err(ClientRunResult::QueueClosed)
                    }
                }
            }
            join_result = self.recv_task.join() => {
                Err(join_result)
            }
        }
    }
}

async fn rx_loop<R>(
    response_tx: mpsc::UnboundedSender<ClientResponse>,
    mut fr: FramedRead<R, crucible_protocol::CrucibleDecoder>,
    log: Logger,
    cid: ClientId,
) -> ClientRunResult
where
    R: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send + 'static,
{
    loop {
        tokio::select! {
            f = fr.next() => {
                match f {
                    Some(Ok(m)) => {
                        update_net_done_probes(&m, cid);
                        if let Err(e) =
                            response_tx.send(ClientResponse::Message(m))
                        {
                            warn!(
                                log,
                                "client response queue closed unexpectedly: \
                                 {e}; is the program exiting?"
                            );
                            break ClientRunResult::QueueClosed;
                        }
                    }
                    Some(Err(e)) => {
                        warn!(log, "downstairs client error {e}");
                        break ClientRunResult::ReadFailed(e);
                    }
                    None => {
                        warn!(log, "downstairs disconnected");
                        break ClientRunResult::Finished;
                    }
                }
            }
            _ = CLIENT_TIMEOUT.wait(&log) => {
                warn!(log, "inactivity timeout");
                break ClientRunResult::Timeout;
            }
        }
    }
}

fn update_net_start_probes(m: &Message, cid: ClientId) {
    match m {
        Message::ReadRequest { job_id, .. } => {
            cdt::ds__read__net__start!(|| (job_id.0, cid.get()));
        }
        Message::Write { ref header, .. } => {
            cdt::ds__write__net__start!(|| (header.job_id.0, cid.get()));
        }
        Message::WriteUnwritten { ref header, .. } => {
            cdt::ds__write__unwritten__net__start!(|| (
                header.job_id.0,
                cid.get()
            ));
        }
        Message::Flush { job_id, .. } => {
            cdt::ds__flush__net__start!(|| (job_id.0, cid.get()));
        }
        _ => {}
    }
}
fn update_net_done_probes(m: &Message, cid: ClientId) {
    match m {
        Message::ReadResponse { ref header, .. } => {
            cdt::ds__read__net__done!(|| (header.job_id.0, cid.get()));
        }
        Message::WriteAck { job_id, .. } => {
            cdt::ds__write__net__done!(|| (job_id.0, cid.get()));
        }
        Message::WriteUnwrittenAck { job_id, .. } => {
            cdt::ds__write__unwritten__net__done!(|| (job_id.0, cid.get()));
        }
        Message::FlushAck { job_id, .. } => {
            cdt::ds__flush__net__done!(|| (job_id.0, cid.get()));
        }
        _ => {}
    }
}

/// Returns:
/// - `Ok(())` for successfully decrypted data, or if there is no block context
///   and the block is all 0s (i.e. a valid empty block)
/// - `Err(..)` otherwise
pub(crate) fn validate_encrypted_read_response(
    block_context: Option<crucible_protocol::EncryptionContext>,
    data: &mut [u8],
    encryption_context: &EncryptionContext,
    log: &Logger,
) -> Result<(), CrucibleError> {
    // XXX because we don't have block generation numbers, an attacker
    // downstairs could:
    //
    // 1) remove encryption context and cause a denial of service, or
    // 2) roll back a block by writing an old data and encryption context
    //
    // check that this read response contains block contexts that contain
    // a matching encryption context.

    let Some(ctx) = block_context else {
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
        if data.iter().all(|&x| x == 0) {
            return Ok(());
        } else {
            error!(log, "got empty block context with non-blank block");
            return Err(CrucibleError::MissingBlockContext);
        }
    };

    // We're using authenticated encryption, so if it decrypts correctly, we can
    // be confident that it wasn't corrupted.  Corruption either on-disk
    // (unlikely due to ZFS) or in-transit (unlikely-ish due to TCP checksums)
    // will both result in decryption failure; we can't tell these cases apart.
    //
    // Note: decrypt_in_place does not overwrite the buffer if it fails,
    // otherwise we would need to copy here. There's a unit test to validate
    // this behaviour.
    use aes_gcm_siv::{Nonce, Tag};
    let decryption_result = encryption_context.decrypt_in_place(
        data,
        Nonce::from_slice(&ctx.nonce[..]),
        Tag::from_slice(&ctx.tag[..]),
    );
    if decryption_result.is_ok() {
        Ok(())
    } else {
        error!(log, "Decryption failed!");
        Err(CrucibleError::DecryptionError)
    }
}

/// Returns:
/// - Ok(()) where the integrity hash matches (or the integrity hash is missing
///   and the block is all 0s, indicating an empty block)
/// - Err otherwise
pub(crate) fn validate_unencrypted_read_response(
    block_hash: Option<u64>,
    data: &mut [u8],
    log: &Logger,
) -> Result<(), CrucibleError> {
    if let Some(hash) = block_hash {
        // check integrity hashes - make sure it is correct
        let computed_hash = integrity_hash(&[data]);

        if computed_hash == hash {
            Ok(())
        } else {
            // No integrity hash was correct for this response
            error!(log, "No match computed hash:0x{:x}", computed_hash,);
            error!(log, "No match          hash:0x{:x}", hash);
            error!(log, "Data from hash:");
            for (i, d) in data[..6].iter().enumerate() {
                error!(log, "[{i}]:{d}");
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
        if data[..].iter().all(|&x| x == 0) {
            Ok(())
        } else {
            error!(log, "got empty block context with non-blank block");
            Err(CrucibleError::MissingBlockContext)
        }
    }
}
