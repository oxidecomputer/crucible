// Copyright 2023 Oxide Computer Company
use crate::{
    cdt, integrity_hash, io_limits::ClientIOLimits, live_repair::ExtentInfo,
    upstairs::UpstairsConfig, upstairs::UpstairsState, ClientIOStateCount,
    ClientId, ConnectionMode, CrucibleDecoder, CrucibleError, DownstairsIO,
    DsState, DsStateData, EncryptionContext, IOState, IOop, JobId, Message,
    RawReadResponse, ReconcileIO, ReconcileIOState, RegionDefinitionStatus,
    RegionMetadata,
};
use crucible_common::{x509::TLSContext, NegotiationError, VerboseTimeout};
use crucible_protocol::{
    MessageWriter, ReconciliationId, CRUCIBLE_MESSAGE_VERSION,
};
use strum::IntoDiscriminant;

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
    sync::{
        mpsc,
        oneshot::{self, error::RecvError},
    },
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
    state: DsStateData,

    /// The `JobId` of the last flush that this downstairs has acked
    ///
    /// This will be `None` between initial startup and the first flush that
    /// this Downstairs acks.
    ///
    /// Note that this is a job ID, not a downstairs flush index (contrast with
    /// [`Downstairs::next_flush`], which is a flush index).
    last_flush: Option<JobId>,

    /// Jobs that have been skipped
    pub(crate) skipped_jobs: BTreeSet<JobId>,

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
        let (client_connect_tx, client_connect_rx) = oneshot::channel();
        let client_task = Self::new_io_task(
            target_addr,
            ClientConnectDelay::Wait(client_connect_rx),
            client_id,
            tls_context.clone(),
            client_delay_us.clone(),
            &log,
        );
        Self {
            cfg,
            client_task,
            client_id,
            io_limits,
            region_uuid: None,
            tls_context,
            log,
            target_addr,
            repair_addr: None,
            state: DsStateData::Connecting {
                mode: ConnectionMode::New,
                state: NegotiationStateData::WaitConnect(client_connect_tx),
            },
            last_flush: None,
            stats: DownstairsStats::default(),
            skipped_jobs: BTreeSet::new(),
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
            self.checked_state_transition(up_state, DsStateData::Stopping(r));
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
        mut repair_min_id: Option<JobId>,
    ) -> IOop {
        if matches!(self.state, DsStateData::LiveRepair) {
            assert!(repair_min_id.is_some());
        } else {
            // If this specific downstairs is not under repair, then clear
            // out any extent limit.
            if let IOop::Flush { extent_limit, .. } = &mut job {
                *extent_limit = None;
            }
            // Since we are not under repair, then we can also clear the
            // repair_min_id.  This prevents jobs at/below min_id from
            // being pruned below.
            repair_min_id = None;
        }

        if !self.skipped_jobs.is_empty() || repair_min_id.is_some() {
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
                    debug!(
                        self.log,
                        "{} Check/remove skipped:{:?}, repair min id {:?} \
                         from deps:{:?}",
                        ds_id,
                        self.skipped_jobs,
                        repair_min_id,
                        dependencies,
                    );

                    dependencies.retain(|x| {
                        !self.skipped_jobs.contains(x)
                            && repair_min_id.map(|r| *x >= r).unwrap_or(true)
                    });
                    info!(
                        self.log,
                        " {} final dependency list {:?}", ds_id, dependencies
                    );
                }
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

    /// Sets our state to `DsStateData::Reconcile`
    ///
    /// # Panics
    /// If the current state is invalid
    pub(crate) fn begin_reconcile(&mut self) {
        info!(self.log, "Transition from {:?} to Reconcile", self.state());
        let DsStateData::Connecting { state, mode } = &mut self.state else {
            panic!(
                "invalid state {:?} for client {}",
                self.state(),
                self.client_id
            );
        };
        assert_eq!(state.discriminant(), NegotiationState::WaitQuorum);
        assert_eq!(mode, &ConnectionMode::New);
        *state = NegotiationStateData::Reconcile;
    }

    /// Checks whether this Downstairs is ready for the upstairs to deactivate
    pub(crate) fn ready_to_deactivate(&self) -> bool {
        match &self.state {
            DsStateData::Connecting {
                mode: ConnectionMode::New,
                state: NegotiationStateData::WaitConnect(..),
            } => {
                info!(
                    self.log,
                    "ready to deactivate from state {:?}",
                    self.state(),
                );
                true
            }
            s => {
                info!(
                    self.log,
                    "not ready to deactivate due to state {:?}",
                    DsState::from(s)
                );
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
        // Clear this Downstairs' repair address, and let the YesItsMe set it.
        // This works if this Downstairs is new, reconnecting, or was replaced
        // entirely; the repair address could have changed in any of these
        // cases.
        self.repair_addr = None;

        // If the upstairs is already active (or trying to go active), then we
        // should automatically connect to the Downstairs.
        let auto_connect = match up_state {
            UpstairsState::Active | UpstairsState::GoActive(..) => true,
            UpstairsState::Disabled(..)
            | UpstairsState::Initializing
            | UpstairsState::Deactivating { .. } => false,
        };

        let current = &self.state;
        let new_mode = match current {
            // If we can't replay jobs, then reconnection must happen through
            // live-repair (rather than replay).
            DsStateData::Active
            | DsStateData::Connecting {
                mode: ConnectionMode::Offline,
                ..
            } if !can_replay => ConnectionMode::Faulted,

            // If the Downstairs has spontaneously stopped, we will attempt to
            // replay jobs when reconnecting
            DsStateData::Active => ConnectionMode::Offline,

            // Other failures during connection preserve the previous mode
            DsStateData::Connecting { mode, .. } => *mode,

            // Faults or failures during live-repair must go through live-repair
            DsStateData::LiveRepair
            | DsStateData::Stopping(ClientStopReason::Fault(..)) => {
                ConnectionMode::Faulted
            }

            // Failures during deactivation restart from the very beginning
            DsStateData::Stopping(ClientStopReason::Disabled)
            | DsStateData::Stopping(ClientStopReason::Deactivated) => {
                ConnectionMode::New
            }

            // Failures during negotiation either restart from the beginning, or
            // go through the live-repair path.
            DsStateData::Stopping(ClientStopReason::NegotiationFailed(..)) => {
                match up_state {
                    // If we haven't activated yet (or we're deactivating) then
                    // start from New
                    UpstairsState::GoActive(..)
                    | UpstairsState::Initializing
                    | UpstairsState::Disabled(..)
                    | UpstairsState::Deactivating { .. } => ConnectionMode::New,

                    // Otherwise, use live-repair
                    UpstairsState::Active => ConnectionMode::Faulted,
                }
            }

            DsStateData::Stopping(ClientStopReason::Replacing) => {
                match up_state {
                    // If we haven't activated yet (or we're deactivating), then
                    // start from New
                    UpstairsState::GoActive(..)
                    | UpstairsState::Initializing
                    | UpstairsState::Disabled(..)
                    | UpstairsState::Deactivating { .. } => ConnectionMode::New,

                    // Otherwise, use live-repair; `ConnectionMode::Replaced`
                    // indicates that the address is allowed to change.
                    UpstairsState::Active => ConnectionMode::Replaced,
                }
            }
        };
        // Note that jobs are skipped / replayed in `Downstairs::reinitialize`,
        // which is (probably) the caller of this function!

        self.connection_id.update();

        let (client_connect, state) = if auto_connect {
            (
                ClientConnectDelay::Delay(CLIENT_RECONNECT_DELAY),
                NegotiationStateData::Start,
            )
        } else {
            let (client_connect_tx, client_connect_rx) = oneshot::channel();
            (
                ClientConnectDelay::Wait(client_connect_rx),
                NegotiationStateData::WaitConnect(client_connect_tx),
            )
        };
        self.client_task = Self::new_io_task(
            self.target_addr,
            client_connect,
            self.client_id,
            self.tls_context.clone(),
            self.client_delay_us.clone(),
            &self.log,
        );

        let new_state = DsStateData::Connecting {
            mode: new_mode,
            state,
        };

        self.checked_state_transition(up_state, new_state);
    }

    /// Returns the last flush ID handled by this client
    pub(crate) fn last_flush(&self) -> Option<JobId> {
        self.last_flush
    }

    fn new_io_task(
        target: Option<SocketAddr>,
        start: ClientConnectDelay,
        client_id: ClientId,
        tls_context: Option<Arc<TLSContext>>,
        client_delay_us: Arc<AtomicU64>,
        log: &Logger,
    ) -> ClientTaskHandle {
        #[cfg(test)]
        if let Some(target) = target {
            Self::new_network_task(
                target,
                start,
                client_id,
                tls_context,
                client_delay_us,
                log,
            )
        } else {
            Self::new_dummy_task(start)
        }

        #[cfg(not(test))]
        Self::new_network_task(
            target.expect("must provide socketaddr"),
            start,
            client_id,
            tls_context,
            client_delay_us,
            log,
        )
    }

    fn new_network_task(
        target: SocketAddr,
        start: ClientConnectDelay,
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

        let log = log.new(o!("" => "io task"));
        tokio::spawn(async move {
            let mut c = ClientIoTask {
                client_id,
                tls_context,
                target,
                request_rx: client_request_rx,
                response_tx: client_response_tx,
                stop: client_stop_rx,
                recv_task: ClientRxTask {
                    handle: None,
                    log: log.clone(),
                },
                client_delay_us,
                log,
            };
            c.run(start).await
        });
        ClientTaskHandle {
            client_request_tx,
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        }
    }

    /// Starts a dummy IO task, returning its IO handle
    #[cfg(test)]
    fn new_dummy_task(_start: ClientConnectDelay) -> ClientTaskHandle {
        let (client_request_tx, client_request_rx) = mpsc::unbounded_channel();
        let (_client_response_tx, client_response_rx) =
            mpsc::unbounded_channel();
        let (client_stop_tx, client_stop_rx) = oneshot::channel();

        // Forget these without dropping them, so that we can send values into
        // the void!
        std::mem::forget(client_request_rx);
        std::mem::forget(client_stop_rx);

        ClientTaskHandle {
            client_request_tx,
            client_stop_tx: Some(client_stop_tx),
            client_response_rx,
        }
    }

    /// Indicate that the upstairs has requested that we go active
    pub(crate) fn set_active_request(&mut self) {
        if let DsStateData::Connecting { state, .. } = &mut self.state {
            if matches!(state, NegotiationStateData::WaitConnect(..)) {
                info!(self.log, "sending connect oneshot to client");
                let mut s = NegotiationStateData::Start;
                std::mem::swap(state, &mut s);
                let NegotiationStateData::WaitConnect(t) = s else {
                    unreachable!();
                };
                if let Err(e) = t.send(()) {
                    error!(
                        self.log,
                        "failed to set client as active {e:?};
                         are we shutting down?"
                    );
                }
            }
        }
    }

    /// Accessor method for client connection state
    pub(crate) fn state(&self) -> DsState {
        (&self.state).into()
    }

    /// Accessor method for data-bearing client connection state
    pub(crate) fn state_data(&self) -> &DsStateData {
        &self.state
    }

    pub(crate) fn abort_negotiation(
        &mut self,
        up_state: &UpstairsState,
        reason: ClientNegotiationFailed,
    ) {
        self.halt_io_task(up_state, reason.into());
    }

    /// Sets the current state to `DsStateData::Active`
    pub(crate) fn set_active(&mut self) {
        info!(
            self.log,
            "Transition from {} to Active",
            DsState::from(&self.state)
        );
        self.state = DsStateData::Active;
    }

    /// Sets the `DsStateData::Connecting` mode to `Faulted`
    ///
    /// This changes the subsequent path through negotiation, without restarting
    /// the client IO task.  Doing so is safe because the faulted path is
    /// a superset of the offline path.
    ///
    /// # Panics
    /// If we are not in `DsStateData::Connecting { mode: ConnectionMode::Offline,
    /// .. }`
    pub(crate) fn set_connection_mode_faulted(&mut self) {
        let DsStateData::Connecting { mode, .. } = &mut self.state else {
            panic!("not connecting");
        };
        assert_eq!(*mode, ConnectionMode::Offline);
        *mode = ConnectionMode::Faulted
    }

    /// Applies an [`EnqueueResult`] for the given job
    ///
    /// If the job should be skipped, then it is added to `self.skipped_jobs`.
    /// `self.io_state_job_count` is updated with the incoming job state.
    pub(crate) fn apply_enqueue_result(
        &mut self,
        ds_id: JobId,
        io: &IOop,
        should_send: EnqueueResult,
    ) {
        // Update our set of skipped jobs if we're not sending this one
        if matches!(should_send, EnqueueResult::Skip) {
            self.skipped_jobs.insert(ds_id);
        }

        // Update our state counters based on the job state
        let state = should_send.state();
        self.io_state_job_count[&state] += 1;
        self.io_state_byte_count[&state] += io.job_bytes();
    }

    /// Checks whether the client is accepting IO
    pub fn should_send(&self) -> Result<EnqueueResult, ShouldSendError> {
        match self.state {
            // We never send jobs if we're in certain inactive states
            DsStateData::Connecting {
                mode: ConnectionMode::New,
                ..
            } if self.cfg.read_only => {
                // Read only upstairs can connect with just a single downstairs
                // ready, we skip jobs on the other downstairs till they connect.
                Ok(EnqueueResult::Skip)
            }
            DsStateData::Connecting {
                mode: ConnectionMode::Faulted | ConnectionMode::Replaced,
                ..
            }
            | DsStateData::Stopping(
                ClientStopReason::Fault(..)
                | ClientStopReason::Disabled
                | ClientStopReason::Replacing
                | ClientStopReason::NegotiationFailed(..),
            ) => Ok(EnqueueResult::Skip),

            // Send jobs if the client is active or in live-repair.  The caller
            // is responsible for checking whether live-repair jobs should be
            // skipped, and this happens outside of this function
            DsStateData::Active => Ok(EnqueueResult::Send),
            DsStateData::LiveRepair => Err(ShouldSendError::InLiveRepair),

            // Holding jobs for an offline client means that those jobs are
            // marked as InProgress, so they aren't cleared out by a subsequent
            // flush (so we'll be able to bring that client back into compliance
            // by replaying jobs).
            DsStateData::Connecting {
                mode: ConnectionMode::Offline,
                ..
            } => Ok(EnqueueResult::Hold),

            DsStateData::Stopping(ClientStopReason::Deactivated)
            | DsStateData::Connecting {
                mode: ConnectionMode::New, // RO client checked above
                ..
            } => panic!(
                "enqueue should not be called from state {:?}",
                self.state()
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
        new_state: DsStateData,
    ) {
        if !Self::is_state_transition_valid(up_state, &self.state, &new_state) {
            panic!(
                "invalid state transition for client {} from {:?} -> {:?} \
                 (with up_state: {:?})",
                self.client_id,
                DsState::from(&self.state),
                DsState::from(&new_state),
                up_state
            );
        }
        self.state = new_state;
    }

    /// Check if a state transition is valid, returning `true` or `false`
    fn is_state_transition_valid(
        up_state: &UpstairsState,
        prev_state: &DsStateData,
        next_state: &DsStateData,
    ) -> bool {
        match (prev_state, next_state) {
            // Restarting negotiation is always allowed
            (
                DsStateData::Connecting { .. },
                DsStateData::Connecting {
                    state:
                        NegotiationStateData::Start
                        | NegotiationStateData::WaitConnect(..),
                    ..
                },
            ) => true,

            // Check normal negotiation path
            (
                DsStateData::Connecting {
                    state: prev_state,
                    mode: prev_mode,
                },
                DsStateData::Connecting {
                    state: next_state,
                    mode: next_mode,
                },
            ) => {
                if *next_mode == ConnectionMode::New
                    && matches!(up_state, UpstairsState::Active)
                {
                    return false;
                }
                next_mode == prev_mode
                    && NegotiationStateData::is_transition_valid(
                        *prev_mode, prev_state, next_state,
                    )
            }

            // We can go to Active either through reconciliation or replay;
            // in other cases, we must use live-repair
            (DsStateData::Connecting { state, mode }, DsStateData::Active) => {
                matches!(
                    (state, mode),
                    (
                        NegotiationStateData::WaitForRegionInfo,
                        ConnectionMode::Offline
                    ) | (NegotiationStateData::Reconcile, ConnectionMode::New)
                        | (
                            NegotiationStateData::LiveRepairReady,
                            ConnectionMode::Faulted | ConnectionMode::Replaced
                        )
                )
            }
            (
                DsStateData::Connecting { state, mode },
                DsStateData::LiveRepair,
            ) => {
                matches!(
                    (state, mode),
                    (
                        NegotiationStateData::LiveRepairReady,
                        ConnectionMode::Faulted | ConnectionMode::Replaced
                    )
                )
            }

            // When can we stop the IO task ourselves?
            (DsStateData::LiveRepair, DsStateData::Active) => true,
            (
                DsStateData::Connecting { .. },
                DsStateData::Stopping(
                    ClientStopReason::NegotiationFailed(..)
                    | ClientStopReason::Replacing
                    | ClientStopReason::Disabled
                    | ClientStopReason::Fault(..),
                ),
            ) => true,
            (
                DsStateData::Active | DsStateData::LiveRepair,
                DsStateData::Stopping(
                    ClientStopReason::Fault(..)
                    | ClientStopReason::Replacing
                    | ClientStopReason::Disabled,
                ),
            ) => true,
            (_, DsStateData::Stopping(ClientStopReason::Deactivated)) => {
                matches!(up_state, UpstairsState::Deactivating(..))
            }

            (
                DsStateData::Stopping(r),
                DsStateData::Connecting { mode, state },
            ) => {
                use ClientStopReason as R;
                matches!(
                    (r, mode, state),
                    (
                        R::Fault(ClientFaultReason::OfflineDeactivated),
                        ConnectionMode::Faulted,
                        NegotiationStateData::WaitConnect(..)
                    ) | (
                        R::Fault(..),
                        ConnectionMode::Faulted,
                        NegotiationStateData::Start
                    ) | (
                        R::Deactivated | R::Disabled,
                        ConnectionMode::New,
                        NegotiationStateData::WaitConnect(..)
                    ) | (
                        R::Replacing,
                        ConnectionMode::Replaced,
                        NegotiationStateData::Start
                    ) | (
                        R::Replacing,
                        ConnectionMode::New,
                        NegotiationStateData::Start
                            | NegotiationStateData::WaitConnect(..)
                    ) | (
                        R::NegotiationFailed(..),
                        ConnectionMode::New,
                        NegotiationStateData::Start
                            | NegotiationStateData::WaitConnect(..)
                    )
                )
            }

            // When the upstairs is active, we can always spontaneously
            // disconnect, which brings us to either Offline or Faulted
            // depending on whether replay is valid
            (
                _,
                DsStateData::Connecting {
                    mode: ConnectionMode::Offline | ConnectionMode::Faulted,
                    state: NegotiationStateData::Start,
                },
            ) => matches!(up_state, UpstairsState::Active),

            // Anything not allowed is prohibited
            _ => false,
        }
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
    /// If this client is not in `DsStateData::LiveRepair`
    pub(crate) fn finish_repair(&mut self, up_state: &UpstairsState) {
        assert!(matches!(self.state, DsStateData::LiveRepair));
        self.checked_state_transition(up_state, DsStateData::Active);
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

                        self.last_flush = Some(ds_id);
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

    /// Halts the client IO task, if not already disabled
    ///
    /// The IO task will automatically restart in the main event handler, coming
    /// back in `ConnectionMode::New`
    pub(crate) fn disable(&mut self, up_state: &UpstairsState) {
        // If the task is already disabled, then restarting the IO task just
        // puts us back in an identical place, so it's not necessary.
        if !matches!(
            self.state,
            DsStateData::Connecting {
                mode: ConnectionMode::New,
                state: NegotiationStateData::WaitConnect(..)
            }
        ) {
            self.halt_io_task(up_state, ClientStopReason::Disabled);
        }
    }

    /// Skips from `LiveRepairReady` to `Active`; a no-op otherwise
    ///
    /// # Panics
    /// If this downstairs is not read-only
    pub(crate) fn skip_live_repair(&mut self, up_state: &UpstairsState) {
        let DsStateData::Connecting { state, .. } = &self.state else {
            return;
        };
        if matches!(state, NegotiationStateData::LiveRepairReady) {
            assert!(self.cfg.read_only);

            // TODO: could we do this transition early, by automatically
            // skipping LiveRepairReady if read-only?
            self.checked_state_transition(up_state, DsStateData::Active);
            self.stats.ro_lr_skipped += 1;
        }
    }

    /// Moves from `LiveRepairReady` to `LiveRepair`
    ///
    /// # Panics
    /// If the state is not `Connecting { state: LiveRepairReady }`
    pub(crate) fn start_live_repair(&mut self, up_state: &UpstairsState) {
        let DsStateData::Connecting { state, .. } = &self.state else {
            panic!("invalid state");
        };
        assert!(matches!(state, NegotiationStateData::LiveRepairReady));
        self.checked_state_transition(up_state, DsStateData::LiveRepair);
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
    ) -> Result<NegotiationResult, NegotiationError> {
        let DsStateData::Connecting { state, mode } = &mut self.state else {
            error!(
                self.log,
                "tried to continue negotiation while not connecting"
            );
            return Err(NegotiationError::OutOfOrder);
        };
        let mode = *mode; // mode is immutable here
        match m {
            Message::YesItsMe {
                version,
                repair_addr,
            } => {
                let NegotiationStateData::Start = *state else {
                    error!(self.log, "got version already");
                    return Err(NegotiationError::OutOfOrder);
                };
                if version != CRUCIBLE_MESSAGE_VERSION {
                    error!(
                        self.log,
                        "expected version {}, got {}",
                        CRUCIBLE_MESSAGE_VERSION,
                        version
                    );
                    return Err(NegotiationError::IncompatibleVersion {
                        expected: CRUCIBLE_MESSAGE_VERSION,
                        actual: version,
                    });
                }
                self.repair_addr = Some(repair_addr);
                *state = NegotiationStateData::WaitForPromote;
                self.send(Message::PromoteToActive {
                    upstairs_id: self.cfg.upstairs_id,
                    session_id: self.cfg.session_id,
                    gen: self.cfg.generation(),
                });
                Ok(NegotiationResult::NotDone)
            }
            Message::VersionMismatch { version } => {
                error!(
                    self.log,
                    "downstairs version is {version}, \
                     ours is {CRUCIBLE_MESSAGE_VERSION}"
                );
                Err(NegotiationError::IncompatibleVersion {
                    expected: CRUCIBLE_MESSAGE_VERSION,
                    actual: version,
                })
            }
            Message::EncryptedMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs encrypted is {expected}, ours is {}",
                    self.cfg.encrypted()
                );
                Err(NegotiationError::EncryptionMismatch {
                    expected: self.cfg.encrypted(),
                    actual: expected,
                })
            }
            Message::ReadOnlyMismatch { expected } => {
                error!(
                    self.log,
                    "downstairs read_only is {expected}, ours is {}",
                    self.cfg.read_only,
                );
                Err(NegotiationError::ReadOnlyMismatch {
                    expected: self.cfg.read_only,
                    actual: expected,
                })
            }
            Message::YouAreNowActive {
                upstairs_id,
                session_id,
                gen,
            } => {
                if !matches!(state, NegotiationStateData::WaitForPromote) {
                    error!(
                        self.log,
                        "Received YouAreNowActive out of order! {:?}",
                        state.discriminant()
                    );
                    return Err(NegotiationError::OutOfOrder);
                }

                let mut err = None;
                if self.cfg.upstairs_id != upstairs_id {
                    error!(
                        self.log,
                        "UUID mismatch in YouAreNowActive: {:?} != {:?}",
                        self.cfg.upstairs_id,
                        upstairs_id
                    );
                    err = Some(NegotiationError::UpstairsIdMismatch {
                        expected: self.cfg.upstairs_id,
                        actual: upstairs_id,
                    });
                }
                if self.cfg.session_id != session_id {
                    error!(
                        self.log,
                        "Session mismatch in YouAreNowActive: {:?} != {:?}",
                        self.cfg.session_id,
                        session_id
                    );
                    err = Some(NegotiationError::SessionIdMismatch {
                        expected: self.cfg.session_id,
                        actual: session_id,
                    });
                }
                let upstairs_gen = self.cfg.generation();
                if upstairs_gen != gen {
                    error!(
                        self.log,
                        "generation mismatch in YouAreNowActive: {} != {}",
                        upstairs_gen,
                        gen
                    );
                    err = Some(NegotiationError::GenerationNumberTooLow {
                        requested: upstairs_gen,
                        actual: gen,
                    });
                }
                if let Some(e) = err {
                    Err(e)
                } else {
                    *state = NegotiationStateData::WaitForRegionInfo;
                    self.send(Message::RegionInfoPlease);
                    Ok(NegotiationResult::NotDone)
                }
            }
            Message::RegionInfo { region_def } => {
                if !matches!(state, NegotiationStateData::WaitForRegionInfo) {
                    error!(self.log, "Received RegionInfo out of order!");
                    return Err(NegotiationError::OutOfOrder);
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
                    return Err(NegotiationError::EncryptionMismatch {
                        expected: self.cfg.encrypted(),
                        actual: region_def.get_encrypted(),
                    });
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
                                        self.state(),
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
                            "send last flush ID to this DS: {:?}", lf
                        );
                        self.send(Message::LastFlush {
                            last_flush_number: lf,
                        });
                        self.set_active();
                        Ok(NegotiationResult::Replay)
                    }
                    ConnectionMode::New
                    | ConnectionMode::Faulted
                    | ConnectionMode::Replaced => {
                        /*
                         * Ask for the current version of all extents.
                         */
                        *state = NegotiationStateData::GetExtentVersions;
                        self.send(Message::ExtentVersionsPlease);
                        Ok(NegotiationResult::NotDone)
                    }
                }
            }
            Message::ExtentVersions {
                gen_numbers,
                flush_numbers,
                dirty_bits,
            } => {
                if !matches!(state, NegotiationStateData::GetExtentVersions) {
                    error!(self.log, "Received ExtentVersions out of order!");
                    return Err(NegotiationError::OutOfOrder);
                }
                /*
                 * Record this downstairs region info for later
                 * comparison with the other downstairs in this
                 * region set.
                 */
                let dsr = RegionMetadata::new(
                    &gen_numbers,
                    &flush_numbers,
                    &dirty_bits,
                );
                let out = match mode {
                    ConnectionMode::New => {
                        *state = NegotiationStateData::WaitQuorum(dsr);
                        NegotiationResult::WaitQuorum
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
                        *state = NegotiationStateData::WaitQuorum(dsr);
                        NegotiationResult::WaitQuorum
                    }

                    ConnectionMode::Faulted | ConnectionMode::Replaced => {
                        *state = NegotiationStateData::LiveRepairReady;
                        NegotiationResult::LiveRepair
                    }
                    ConnectionMode::Offline => {
                        panic!(
                            "got ExtentVersions from invalid state {:?}",
                            self.state()
                        );
                    }
                };
                Ok(out)
            }
            m => panic!("invalid message in continue_negotiation: {m:?}"),
        }
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
            DsStateData::Connecting {
                state: NegotiationStateData::Reconcile,
                mode: ConnectionMode::New | ConnectionMode::Replaced
            }
        ) {
            panic!(
                "[{}] should still be in reconcile, not {:?}",
                self.client_id,
                self.state()
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

    /// Checks whether the client is in a state where it can accept IO
    pub(crate) fn is_accepting_io(&self) -> bool {
        matches!(
            self.state,
            DsStateData::Active
                | DsStateData::LiveRepair
                | DsStateData::Connecting {
                    mode: ConnectionMode::Offline,
                    ..
                }
        )
    }

    pub(crate) fn id(&self) -> Option<Uuid> {
        self.region_uuid
    }
}

/// Tracks client negotiation progress
///
/// The exact path through negotiation depends on the [`ConnectionMode`].
///
/// There are three main paths, shown below:
///
/// ```text
///              ┌───────┐
///              │ Start │
///              └───┬───┘
///                  │
///          ┌───────▼────────┐
///          │ WaitForPromote │
///          └───────┬────────┘
///                  │
///         ┌────────▼──────────┐
///         │ WaitForRegionInfo │
///         └──┬──────────────┬─┘
///    Offline │              │ New / Faulted / Replaced
///   (replay) │         ┌────▼────────────┐
///            │         │GetExtentVersions│
///            │         └─┬─────────────┬─┘
///            │           │ New         │ Faulted / Replaced
///            │    ┌──────▼───┐    ┌────▼──────────┐
///            │    │WaitQuorum│    │LiveRepairReady│
///            │    └────┬─────┘    └────┬──────────┘
///            │         │               │
///            │    ┌────▼────┐          │
///            │    │Reconcile│          │
///            │    └────┬────┘          │
///            │         │               │
///            │     ┌───▼──┐            │
///            └─────► Done ◄────────────┘
///                  └──────┘
/// ```
///
/// `Done` isn't actually present in the state machine; it's indicated by
/// returning a [`NegotiationResult`] other than [`NegotiationResult::NotDone`].
#[derive(strum::EnumDiscriminants)]
#[strum_discriminants(name(NegotiationState))]
#[strum_discriminants(derive(Serialize, Deserialize, JsonSchema))]
#[strum_discriminants(serde(rename_all = "snake_case"))]
#[strum_discriminants(serde(tag = "type", content = "value"))]
pub enum NegotiationStateData {
    /// One-shot sender to ask the client to open its connection
    ///
    /// This is used to hold the client (without connecting) in cases where we
    /// have deliberately deactivated this client.
    WaitConnect(oneshot::Sender<()>),

    /// After connecting, waiting to hear `YesItsMe` from the client
    ///
    /// Once this message is heard, sends `PromoteToActive` and transitions to
    /// `WaitForPromote`
    Start,

    /// Waiting to hear `YouAreNowActive` from the client
    WaitForPromote,

    /// Waiting to hear `RegionInfo` from the client
    WaitForRegionInfo,

    /// Waiting to hear `ExtentVersions` from the client
    GetExtentVersions,

    /// Waiting for the minimum number of downstairs to be present.
    WaitQuorum(RegionMetadata),

    /// Initial startup, downstairs are repairing from each other.
    Reconcile,

    /// Waiting for live-repair to begin
    LiveRepairReady,
}

impl NegotiationStateData {
    /// Checks whether a particular transition is valid
    ///
    /// See the docstring of [`NegotiationStateData`] for a drawing of the full
    /// state transition diagram
    fn is_transition_valid(
        mode: ConnectionMode,
        prev_state: &Self,
        next_state: &Self,
    ) -> bool {
        matches!(
            (prev_state, next_state, mode),
            (
                NegotiationStateData::Start,
                NegotiationStateData::WaitForPromote,
                _
            ) | (
                NegotiationStateData::WaitForPromote,
                NegotiationStateData::WaitForRegionInfo,
                _
            ) | (
                NegotiationStateData::WaitForRegionInfo,
                NegotiationStateData::GetExtentVersions,
                ConnectionMode::New
                    | ConnectionMode::Faulted
                    | ConnectionMode::Replaced,
            ) | (
                NegotiationStateData::GetExtentVersions,
                NegotiationStateData::WaitQuorum(..),
                ConnectionMode::New
            ) | (
                NegotiationStateData::WaitQuorum(..),
                NegotiationStateData::Reconcile,
                ConnectionMode::New
            ) | (
                NegotiationStateData::GetExtentVersions,
                NegotiationStateData::LiveRepairReady,
                ConnectionMode::Faulted | ConnectionMode::Replaced,
            )
        )
    }
}
/// Result value returned when negotiation is complete
pub(crate) enum NegotiationResult {
    NotDone,
    WaitQuorum,
    Replay,
    LiveRepair,
}

/// Success value from [`DownstairsClient::should_send`]
#[derive(Copy, Clone)]
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

/// Error result from [`DownstairsClient::should_send`]
#[derive(Copy, Clone)]
pub(crate) enum ShouldSendError {
    /// The caller should check against our active live-repair extent
    InLiveRepair,
}

impl EnqueueResult {
    pub(crate) fn state(&self) -> IOState {
        match self {
            EnqueueResult::Send | EnqueueResult::Hold => IOState::InProgress,
            EnqueueResult::Skip => IOState::Skipped,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use slog::Discard;

    #[test]
    fn test_set_active_request_while_replacing() {
        let mut client = DownstairsClient::new(
            ClientId::new(0),
            Arc::new(UpstairsConfig {
                upstairs_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                generation: AtomicU64::new(1),
                read_only: false,
                encryption_context: None,
            }),
            None,
            ClientIOLimits::new(100, 1024 * 1024),
            Logger::root(Discard, o!()),
            None,
        );

        let upstairs_state = UpstairsState::Initializing;
        client.checked_state_transition(
            &upstairs_state,
            DsStateData::Stopping(ClientStopReason::Replacing),
        );

        // This should not panic when a client is in Stopping(Replacing) state
        client.set_active_request();
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

    /// Negotiation says that our message versions are incompatible
    IncompatibleVersion,

    /// Negotiation says that our session IDs are incompatible
    IncompatibleSession,

    /// Negotiation says that region settings are incompatible
    IncompatibleSettings,
}

impl From<ClientNegotiationFailed> for ClientStopReason {
    fn from(f: ClientNegotiationFailed) -> ClientStopReason {
        ClientStopReason::NegotiationFailed(f)
    }
}

impl From<NegotiationError> for ClientNegotiationFailed {
    fn from(value: NegotiationError) -> Self {
        match value {
            NegotiationError::OutOfOrder => Self::BadNegotiationOrder,
            NegotiationError::IncompatibleVersion { .. } => {
                Self::IncompatibleVersion
            }
            NegotiationError::ReadOnlyMismatch { .. }
            | NegotiationError::EncryptionMismatch { .. } => {
                Self::IncompatibleSettings
            }
            NegotiationError::GenerationZeroIsIllegal
            | NegotiationError::GenerationNumberTooLow { .. }
            | NegotiationError::UpstairsIdMismatch { .. }
            | NegotiationError::SessionIdMismatch { .. } => {
                Self::IncompatibleSession
            }
        }
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

    /// The upstairs has requested that we deactivate when we were offline
    OfflineDeactivated,

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
    ///
    /// The reason for the stop will be in the `DsStateData::Stopping(..)` member
    RequestedStop,
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

/// Convert a oneshot result into a `ClientStopReason`
impl From<Result<ClientStopReason, RecvError>> for ClientRunResult {
    fn from(value: Result<ClientStopReason, RecvError>) -> Self {
        match value {
            Ok(..) => ClientRunResult::RequestedStop,
            Err(..) => ClientRunResult::QueueClosed,
        }
    }
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

    /// Oneshot used to stop the task
    stop: oneshot::Receiver<ClientStopReason>,

    /// Handle for the rx task
    recv_task: ClientRxTask,

    /// Shared handle to receive per-client backpressure delay
    ///
    /// Written by the [`DownstairsClient`] and read by the IO task
    client_delay_us: Arc<AtomicU64>,

    log: Logger,
}

enum ClientConnectDelay {
    /// Connect after a fixed delay
    Delay(std::time::Duration),
    /// Wait for a oneshot to fire before connecting
    Wait(oneshot::Receiver<()>),
}

impl ClientConnectDelay {
    async fn wait(self, log: &Logger) -> Result<(), ClientRunResult> {
        match self {
            ClientConnectDelay::Delay(dur) => {
                info!(log, "sleeping for {dur:?} before connecting");
                tokio::time::sleep(dur).await;
                Ok(())
            }
            ClientConnectDelay::Wait(rx) => {
                info!(log, "client is waiting for oneshot");
                rx.await.map_err(|e| {
                    warn!(log, "failed to await start oneshot: {e}");
                    ClientRunResult::QueueClosed
                })
            }
        }
    }
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
    async fn run(&mut self, start: ClientConnectDelay) {
        let r = self.run_inner(start).await;

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

    async fn run_inner(
        &mut self,
        start: ClientConnectDelay,
    ) -> ClientRunResult {
        // Wait for either the connection delay to expire (either time-based or
        // a oneshot), or for the stop oneshot to receive a message.
        tokio::select! {
            r = start.wait(&self.log) => {
                if let Err(e) = r {
                    return e;
                }
            },
            s = &mut self.stop => {
                warn!(
                    self.log,
                    "client IO task stopped before connecting: {s:?}"
                );
                return s.into();
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
                warn!(
                    self.log,
                    "client IO task stopped during connection: {s:?}"
                );
                return s.into();
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
                    info!(self.log, "client stopping due to {s:?}");
                    break s.into();
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
                info!(self.log, "client stopped in write due to {s:?}");
                Err(s.into())
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
