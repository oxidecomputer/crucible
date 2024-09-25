// Copyright 2023 Oxide Computer Company

#[cfg(not(test))]
compile_error!("dummy_downstairs should only be used in unit tests");

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::client::CLIENT_TIMEOUT_SECS;
use crate::guest::Guest;
use crate::up_main;
use crate::BlockIO;
use crate::Buffer;
use crate::CrucibleError;
use crate::DsState;
use crate::{IO_OUTSTANDING_MAX_BYTES, IO_OUTSTANDING_MAX_JOBS};
use crucible_client_types::CrucibleOpts;
use crucible_common::Block;
use crucible_common::BlockIndex;
use crucible_common::ExtentId;
use crucible_common::RegionDefinition;
use crucible_common::RegionOptions;
use crucible_protocol::ClientId;
use crucible_protocol::CrucibleDecoder;
use crucible_protocol::CrucibleEncoder;
use crucible_protocol::JobId;
use crucible_protocol::Message;
use crucible_protocol::ReadBlockContext;
use crucible_protocol::ReadResponseHeader;
use crucible_protocol::WriteHeader;

use bytes::BytesMut;
use futures::SinkExt;
use futures::StreamExt;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Drain;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

// Create a simple logger
fn csl() -> Logger {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
}

/// Handle to a running Downstairs loopback task
///
/// The loopback task receives messages over the network (on `local_addr`)
/// and sends them to `tx`; and receives messages on `rx` and sends them
/// over the network.
///
/// In other words, this lets us pretend to be a Downstairs.
pub struct DownstairsHandle {
    log: Logger,

    loopback_worker: tokio::task::JoinHandle<Result<(), CrucibleError>>,
    rx: mpsc::UnboundedReceiver<Message>,
    tx: mpsc::UnboundedSender<Message>,
    stop: oneshot::Sender<()>,

    uuid: Uuid,
    local_addr: SocketAddr,
    repair_addr: SocketAddr,
    cfg: DownstairsConfig,
    upstairs_session_id: Option<Uuid>,
}

impl DownstairsHandle {
    /// Returns the next non-ping packet
    ///
    /// (pings are answered inline)
    async fn recv(&mut self) -> Option<Message> {
        loop {
            let packet = self.rx.recv().await?;
            if packet == Message::Ruok {
                self.handle_ping()
            } else {
                break Some(packet);
            }
        }
    }

    /// Tries to return the next non-ping packet
    ///
    /// (pings are answered automatically)
    fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        loop {
            let packet = self.rx.try_recv();
            if packet == Ok(Message::Ruok) {
                self.handle_ping();
            } else {
                break packet;
            }
        }
    }

    fn handle_ping(&mut self) {
        if self.cfg.reply_to_ping {
            // Respond to pings right away
            if let Err(e) = self.send(Message::Imok) {
                error!(self.log, "could not send ping: {e:?}");
            }
            info!(self.log, "responded to ping");
        } else {
            info!(self.log, "ignored ping");
        }
    }

    /// Send a message, pretending to be the Downstairs
    fn send(
        &mut self,
        m: Message,
    ) -> Result<(), mpsc::error::SendError<Message>> {
        self.tx.send(m)
    }

    pub async fn negotiate_start(&mut self) {
        let packet = self.recv().await.unwrap();
        if let Message::HereIAm {
            version,
            upstairs_id: _,
            session_id: _,
            gen: _,
            read_only,
            encrypted: _,
            alternate_versions: _,
        } = &packet
        {
            info!(
                self.log,
                "negotiate packet {:?} (upstairs read-only {})",
                packet,
                read_only
            );

            if *read_only != self.cfg.read_only {
                panic!("read only mismatch!");
            }

            self.send(Message::YesItsMe {
                version: *version,
                repair_addr: self.repair_addr,
            })
            .unwrap();
        } else {
            panic!("wrong packet {packet:?}, expected HereIAm")
        }

        let packet = self.recv().await.unwrap();
        if let Message::PromoteToActive {
            upstairs_id,
            session_id,
            gen,
        } = &packet
        {
            assert!(*gen == 1);

            info!(self.log, "negotiate packet {:?}", packet);

            // Record the session id the upstairs sent us
            self.upstairs_session_id = Some(*session_id);

            self.send(Message::YouAreNowActive {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                gen: *gen,
            })
            .unwrap();
        } else {
            panic!("wrong packet {packet:?}, expected PromoteToActive")
        }

        let packet = self.recv().await.unwrap();
        if let Message::RegionInfoPlease = &packet {
            info!(self.log, "negotiate packet {:?}", packet);

            self.send(Message::RegionInfo {
                region_def: self.get_region_definition(),
            })
            .unwrap();
        } else {
            panic!("wrong packet: {packet:?}, expected RegionInfoPlease");
        }
    }

    pub async fn negotiate_step_extent_versions_please(&mut self) {
        let packet = self.recv().await.unwrap();
        if let Message::ExtentVersionsPlease = &packet {
            info!(self.log, "negotiate packet {:?}", packet);

            self.send(Message::ExtentVersions {
                gen_numbers: self.cfg.gen_numbers.clone(),
                flush_numbers: self.cfg.flush_numbers.clone(),
                dirty_bits: self.cfg.dirty_bits.clone(),
            })
            .unwrap();
        } else {
            panic!("wrong packet: {packet:?}, expected ExtentVersionsPlease")
        }
    }

    pub async fn negotiate_step_last_flush(
        &mut self,
        last_flush_number: JobId,
    ) {
        let packet = self.recv().await.unwrap();
        if let Message::LastFlush { .. } = &packet {
            info!(self.log, "negotiate packet {:?}", packet);

            self.send(Message::LastFlushAck { last_flush_number })
                .unwrap();
        } else {
            panic!("wrong packet: {packet:?}, expected LastFlush");
        }
    }

    pub fn get_region_options(&self) -> RegionOptions {
        let mut region_options = RegionOptions::default();
        region_options.set_block_size(512);
        region_options.set_extent_size(self.cfg.extent_size);
        region_options.set_uuid(self.uuid);
        region_options.set_encrypted(false);

        region_options.validate().unwrap();

        region_options
    }

    pub fn get_region_definition(&self) -> RegionDefinition {
        let mut def =
            RegionDefinition::from_options(&self.get_region_options()).unwrap();

        def.set_extent_count(self.cfg.extent_count);

        def
    }

    /// Stops the loopback worker, returning the `SocketAddr` for reuse
    pub async fn halt(self) -> SocketAddr {
        if let Err(()) = self.stop.send(()) {
            // This may be fine, if the worker was kicked out and stopped on
            // its own (e.g. because one of its queues was closed)
            warn!(self.log, "could not stop loopback worker");
        }
        let r = self.loopback_worker.await;
        r.expect("failed to join loopback worker")
            .expect("loopback worker returned an error");
        self.local_addr
    }

    /// Awaits a `Message::Flush { .. }` and sends a `FlushAck`
    ///
    /// Returns the flush number for further checks.
    ///
    /// # Panics
    /// If a non-flush message arrives
    pub async fn ack_flush(&mut self) -> u64 {
        let Message::Flush {
            job_id,
            flush_number,
            upstairs_id,
            ..
        } = self.recv().await.unwrap()
        else {
            panic!("saw non flush!");
        };
        self.send(Message::FlushAck {
            upstairs_id,
            session_id: self.upstairs_session_id.unwrap(),
            job_id,
            result: Ok(()),
        })
        .unwrap();
        flush_number
    }

    /// Awaits a `Message::Write { .. }` and sends a `WriteAck`
    ///
    /// Returns the job ID for further checks.
    ///
    /// # Panics
    /// If a non-write message arrives
    pub async fn ack_write(&mut self) -> JobId {
        let Message::Write { header, .. } = self.recv().await.unwrap() else {
            panic!("saw non write!");
        };
        self.send(Message::WriteAck {
            upstairs_id: header.upstairs_id,
            session_id: self.upstairs_session_id.unwrap(),
            job_id: header.job_id,
            result: Ok(()),
        })
        .unwrap();
        header.job_id
    }

    /// Awaits a `Message::Read` and sends a blank `ReadResponse`
    ///
    /// Returns the job ID for further checks
    ///
    /// # Panics
    /// If a non-read message arrives
    pub async fn ack_read(&mut self) -> JobId {
        let Message::ReadRequest {
            job_id,
            upstairs_id,
            ..
        } = self.recv().await.unwrap()
        else {
            panic!("saw non write!");
        };
        let (block, data) = make_blank_read_response();
        self.send(Message::ReadResponse {
            header: ReadResponseHeader {
                upstairs_id,
                session_id: self.upstairs_session_id.unwrap(),
                job_id,
                blocks: Ok(vec![block]),
            },
            data: data.clone(),
        })
        .unwrap();
        job_id
    }
}

#[derive(Clone)]
pub struct DownstairsConfig {
    read_only: bool,
    reply_to_ping: bool,

    extent_count: u32,
    extent_size: Block,

    gen_numbers: Vec<u64>,
    flush_numbers: Vec<u64>,
    dirty_bits: Vec<bool>,
}

impl DownstairsConfig {
    async fn start(self, log: Logger) -> DownstairsHandle {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let uuid = Uuid::new_v4();
        self.restart(uuid, bind_addr, log).await
    }

    async fn restart(
        self,
        uuid: Uuid,
        bind_addr: SocketAddr,
        log: Logger,
    ) -> DownstairsHandle {
        let listener = TcpListener::bind(&bind_addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        // Dummy repair task, to get a SocketAddr for the `YesItsMe` reply
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let repair_listener = TcpListener::bind(&bind_addr).await.unwrap();
        let repair_addr = repair_listener.local_addr().unwrap();

        let (tx, mut loopback_rx) = mpsc::unbounded_channel();
        let (loopback_tx, rx) = mpsc::unbounded_channel();

        let (stop, mut stop_rx) = oneshot::channel();

        let log_ = log.clone();
        let loopback_worker = tokio::task::spawn(async move {
            info!(log, "loopback worker is listening on {local_addr:?}");
            let (sock, raddr) = listener.accept().await.unwrap();
            info!(log, "loopback worker connected to {raddr:?}");

            let (read, write) = sock.into_split();

            let mut fr = FramedRead::new(read, CrucibleDecoder::new());
            let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        info!(
                            log,
                            "stop sent, time to exit"
                        );
                        break;
                    }
                    r = fr.next() => {
                        if let Some(t) = r {
                            if let Err(e) = t {
                                info!(
                                    log,
                                    "framed read error: {e:?}; exiting"
                                );
                                break;
                            } else if let Err(e) = loopback_tx.send(
                                t.unwrap())
                            {
                                info!(
                                    log,
                                    "loopback_tx error: {e:?}; exiting"
                                );
                                break;
                            }
                        } else {
                            info!(
                                log,
                                "framed read disconnected; exiting"
                            );
                            break;
                        }
                    }
                    t = loopback_rx.recv() => {
                        if let Some(t) = t {
                            if let Err(e) = fw.send(t).await {
                                info!(
                                    log,
                                    "framed write error {e:?}; exiting"
                                );
                                break;
                            }
                        } else {
                            info!(
                                log,
                                "loopback mpsc rx channel closed, exiting"
                            );
                            break;
                        }
                    }
                }
            }
            Ok(())
        });

        DownstairsHandle {
            log: log_,
            loopback_worker,
            rx,
            tx,

            stop,
            uuid,
            cfg: self,
            upstairs_session_id: None,
            local_addr,
            repair_addr,
        }
    }
}

pub struct TestHarness {
    log: Logger,

    ds1: Option<DownstairsHandle>,
    ds2: DownstairsHandle,
    ds3: DownstairsHandle,

    /// JoinHandle for the Crucible `up_main` task
    _join_handle: JoinHandle<()>,

    /// Handle to the guest, to submit IOs
    guest: Arc<Guest>,
}

/// Number of extents in `TestHarness::default_config`
const DEFAULT_EXTENT_COUNT: u32 = 25;

impl TestHarness {
    pub async fn new() -> TestHarness {
        Self::new_(false).await
    }

    pub async fn new_ro() -> TestHarness {
        Self::new_(true).await
    }

    pub fn ds1(&mut self) -> &mut DownstairsHandle {
        self.ds1.as_mut().unwrap()
    }

    fn default_config(read_only: bool) -> DownstairsConfig {
        DownstairsConfig {
            read_only,
            reply_to_ping: true,

            // Extent count is picked so that we can hit
            // IO_OUTSTANDING_MAX_BYTES in less than IO_OUTSTANDING_MAX_JOBS,
            // i.e. letting us test both byte and job fault conditions.
            extent_count: DEFAULT_EXTENT_COUNT,
            extent_size: Block::new_512(10),

            gen_numbers: vec![0u64; DEFAULT_EXTENT_COUNT as usize],
            flush_numbers: vec![0u64; DEFAULT_EXTENT_COUNT as usize],
            dirty_bits: vec![false; DEFAULT_EXTENT_COUNT as usize],
        }
    }

    async fn new_(read_only: bool) -> TestHarness {
        let log = csl();

        let cfg = Self::default_config(read_only);

        let ds1 = cfg.clone().start(log.new(o!("downstairs" => 1))).await;
        let ds2 = cfg.clone().start(log.new(o!("downstairs" => 2))).await;
        let ds3 = cfg.clone().start(log.new(o!("downstairs" => 3))).await;

        // Configure our guest without backpressure, to speed up tests which
        // require triggering a timeout
        let (g, mut io) = Guest::new(Some(log.clone()));
        io.disable_queue_backpressure();
        io.disable_byte_backpressure();
        let guest = Arc::new(g);

        let crucible_opts = CrucibleOpts {
            id: Uuid::new_v4(),
            target: vec![ds1.local_addr, ds2.local_addr, ds3.local_addr],
            flush_timeout: Some(86400.0),
            read_only,

            ..Default::default()
        };

        let join_handle = up_main(crucible_opts, 1, None, io, None).unwrap();

        let mut handles: Vec<JoinHandle<()>> = vec![];

        {
            let guest = guest.clone();
            handles.push(tokio::spawn(async move {
                guest.activate().await.unwrap();
            }));
        }

        // Connect all 3x Downstairs
        //
        // We're using a VecDeque instead of FuturesOrdered so that if a future
        // panics, we get a meaningful backtrace with a correct line number.
        let mut fut = VecDeque::new();
        for mut ds in [ds1, ds2, ds3] {
            fut.push_back(tokio::spawn(async move {
                ds.negotiate_start().await;
                ds.negotiate_step_extent_versions_please().await;
                ds
            }));
        }

        let ds1 = fut.pop_front().unwrap().await.unwrap();
        let ds2 = fut.pop_front().unwrap().await.unwrap();
        let ds3 = fut.pop_front().unwrap().await.unwrap();

        for _ in 0..10 {
            if guest.query_is_active().await.unwrap() {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        assert!(guest.query_is_active().await.unwrap());

        TestHarness {
            log,
            ds1: Some(ds1),
            ds2,
            ds3,
            _join_handle: join_handle,
            guest,
        }
    }

    fn take_ds1(&mut self) -> DownstairsHandle {
        self.ds1.take().unwrap()
    }

    /// Restarts ds1, without doing any negotiation after it comes up
    pub async fn restart_ds1(&mut self) {
        let ds1 = self.take_ds1();
        let cfg = ds1.cfg.clone();
        let log = ds1.log.clone();
        let uuid = ds1.uuid;
        let addr = ds1.halt().await;
        self.ds1 = Some(cfg.restart(uuid, addr, log).await);
    }

    /// Spawns a function on the `Guest`
    ///
    /// This is used to begin a read/write/flush, which would otherwise not
    /// return until one or more Downstairs replied.
    pub fn spawn<R, F, G>(&self, f: F) -> tokio::task::JoinHandle<G>
    where
        F: FnOnce(Arc<Guest>) -> R + Send + 'static,
        R: std::future::Future<Output = G> + Send + 'static,
        G: Send + 'static,
    {
        let g = self.guest.clone();
        tokio::spawn(async move { f(g).await })
    }
}

fn make_blank_read_response() -> (ReadBlockContext, BytesMut) {
    let data = vec![0u8; 512];
    let hash = crucible_common::integrity_hash(&[&data]);

    (
        ReadBlockContext::Unencrypted { hash },
        BytesMut::from(&data[..]),
    )
}

/// Filter the first element that matches some predicate out of a list
pub fn filter_out<T, P>(l: &mut Vec<T>, pred: P) -> Option<T>
where
    P: FnMut(&T) -> bool,
{
    let idx = l.iter().position(pred);
    idx.map(|i| l.remove(i))
}

/// Test that replay occurs after a downstairs disconnects and reconnects
#[tokio::test]
async fn test_replay_occurs() {
    let mut harness = TestHarness::new().await;

    // Send a read
    harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await.unwrap();
    });

    // Confirm all downstairs receive said read
    let ds1_message = harness.ds1().recv().await.unwrap();

    assert!(matches!(ds1_message, Message::ReadRequest { .. }));

    assert!(matches!(
        harness.ds2.recv().await.unwrap(),
        Message::ReadRequest { .. },
    ));

    assert!(matches!(
        harness.ds3.recv().await.unwrap(),
        Message::ReadRequest { .. },
    ));

    // If downstairs 1 disconnects and reconnects, it should get the exact
    // same message replayed to it.

    harness.restart_ds1().await;

    harness.ds1().negotiate_start().await;
    harness.ds1().negotiate_step_last_flush(JobId(0)).await;

    let mut ds1_message_second_time = None;

    for _ in 0..10 {
        if let Ok(m) = harness.ds1().try_recv() {
            ds1_message_second_time = Some(m);
            break;
        }

        eprintln!("waiting for ds1 message in test_replay_occurs");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    assert_eq!(ds1_message, ds1_message_second_time.unwrap());
}

/// Test that after giving up on a downstairs, setting it to faulted, and
/// letting it reconnect, live repair occurs. Check that each extent is
/// repaired with the correct source, and that extent limits are honoured if
/// additional IO comes through.
#[tokio::test]
async fn test_successful_live_repair() {
    let mut harness = TestHarness::new().await;

    // Send some jobs, so that we test job skipping when DS1 is faulted.
    const NUM_JOBS: usize = 200;

    for _ in 0..NUM_JOBS {
        // We must use `spawn` here because `read` will wait for the
        // response to come back before returning
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // Assert we're seeing the read requests (without replying on DS1)
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        harness.ds2.ack_read().await;
        harness.ds3.ack_read().await;

        h.await.unwrap(); // we have > 1x reply, so the read will return
    }

    // Now, fault DS0 to begin live-repair
    harness
        .guest
        .fault_downstairs(ClientId::new(0))
        .await
        .unwrap();

    // Give the IO worker time to notice that the pipe is broken
    tokio::time::sleep(Duration::from_millis(100)).await;

    run_live_repair(harness).await;
}

/// Runs live repair on a harness with DS1 marked as faulted
async fn run_live_repair(mut harness: TestHarness) {
    // Assert the Upstairs isn't sending ds1 more work, because it is Faulted
    let v = harness.ds1().try_recv();
    assert_eq!(
        v,
        Err(TryRecvError::Disconnected),
        "ds1 message queue must be disconnected"
    );

    // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
    // flush_timeout set to 24 hours, we shouldn't see anything else
    assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
    assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

    // Flush to clean out skipped jobs
    {
        let jh = harness.spawn(|guest| async move {
            guest.flush(None).await.unwrap();
        });

        harness.ds2.ack_flush().await;
        harness.ds3.ack_flush().await;

        // Wait for the flush to come back
        jh.await.unwrap();
    }

    // Confirm that DS1 has been disconnected (and cannot reply to jobs)
    {
        let job_id = JobId(1000);
        let (block, data) = make_blank_read_response();
        let session_id = harness.ds1().upstairs_session_id.unwrap();
        let upstairs_id = harness.guest.get_uuid().await.unwrap();
        match harness.ds1().send(Message::ReadResponse {
            header: ReadResponseHeader {
                upstairs_id,
                session_id,
                job_id,
                blocks: Ok(vec![block]),
            },
            data: data.clone(),
        }) {
            Ok(()) => panic!("DS1 should be disconnected"),
            Err(e) => {
                info!(
                    harness.log,
                    "ds1 can't reply to job {}, because it's disconnected: {}",
                    job_id,
                    e
                );
            }
        }
    }

    // Assert the Upstairs isn't sending ds1 more work, because it is
    // Faulted
    let v = harness.ds1().try_recv();
    match v {
        // We're either disconnected, or the queue is empty.
        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
            // This is expected, continue on
        }
        _ => {
            // Any other error (or success!) is unexpected
            panic!("try_recv returned {v:?}");
        }
    }

    // Reconnect ds1
    harness.restart_ds1().await;

    harness.ds1().negotiate_start().await;
    harness.ds1().negotiate_step_extent_versions_please().await;

    // The Upstairs will start sending LiveRepair related work, which may be
    // out of order. Buffer some here.

    let mut ds1_buffered_messages = vec![];
    let mut ds2_buffered_messages = vec![];
    let mut ds3_buffered_messages = vec![];

    for eid in (0..DEFAULT_EXTENT_COUNT).map(ExtentId) {
        // The Upstairs first sends the close and reopen jobs
        for _ in 0..2 {
            ds1_buffered_messages.push(harness.ds1().recv().await.unwrap());
            ds2_buffered_messages.push(harness.ds2.recv().await.unwrap());
            ds3_buffered_messages.push(harness.ds3.recv().await.unwrap());
        }

        assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
        assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
        assert!(ds3_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

        assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        assert!(ds3_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));

        // This is the reopen job id for extent eid

        let reopen_job_id = {
            let m = ds1_buffered_messages
                .iter()
                .position(|m| matches!(m, Message::ExtentLiveReopen { .. }))
                .unwrap();
            match ds1_buffered_messages[m] {
                Message::ExtentLiveReopen { job_id, .. } => job_id,

                _ => panic!(
                    "ds1_buffered_messages[m] not Message::ExtentLiveReopen"
                ),
            }
        };

        // Extent limit is Some(eid), where eid is the current loop
        // iteration. It marks the extent at and below are clear to receive
        // IO. Issue some single extent reads and writes to make sure that
        // extent limit is honoured. Do this only after receiving the two
        // above messages as that guarantees we are in the repair task and
        // that extent_limit is set. Make sure that the first read to the
        // extent under repair has the the ExtentLiveReopen job as a
        // dependency, and that later writes have that read as their
        // dependency (which works because the read already depended on the
        // ExtentLiveReopen job). Batch up responses to send after the live
        // repair is done.

        let mut responses = vec![Vec::new(); 3];

        for io_eid in (0..DEFAULT_EXTENT_COUNT).map(ExtentId) {
            let mut dep_job_id = [reopen_job_id; 3];
            // read
            harness.spawn(move |guest| async move {
                let mut buffer = Buffer::new(1, 512);
                guest
                    .read(BlockIndex(io_eid.0 as u64 * 10), &mut buffer)
                    .await
                    .unwrap();
            });

            if io_eid <= eid {
                // IO at or below the extent under repair is sent to the
                // downstairs under repair.
                let m1 = harness.ds1().recv().await.unwrap();

                match &m1 {
                    Message::ReadRequest {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[0]));
                        }

                        let (block, data) = make_blank_read_response();
                        responses[0].push(Message::ReadResponse {
                            header: ReadResponseHeader {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                job_id: *job_id,
                                blocks: Ok(vec![block]),
                            },
                            data,
                        });

                        // At this point, the next operation is going to be
                        // a write.  This write will depend on the three
                        // reads that are already enqueued (but not the
                        // repair close, because the reads already
                        // implicitly depend on the repair close id).  We'll
                        // update our target dep_job_id to match this read.
                        dep_job_id[0] = *job_id;
                    }
                    _ => panic!("saw {m1:?}"),
                }
            } else {
                // All IO above this is skipped for the downstairs under
                // repair.
                let r = harness.ds1().try_recv();
                assert!(
                    matches!(r, Err(TryRecvError::Empty)),
                    "unexpected response {r:?}"
                );
            }

            let m2 = harness.ds2.recv().await.unwrap();
            let m3 = harness.ds3.recv().await.unwrap();

            match &m2 {
                Message::ReadRequest {
                    upstairs_id,
                    session_id,
                    job_id,
                    dependencies,
                    ..
                } => {
                    if io_eid == eid {
                        assert!(dependencies.contains(&dep_job_id[1]));
                    }

                    let (block, data) = make_blank_read_response();
                    responses[1].push(Message::ReadResponse {
                        header: ReadResponseHeader {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            blocks: Ok(vec![block]),
                        },
                        data,
                    });
                    dep_job_id[1] = *job_id;
                }
                _ => panic!("saw {m2:?}"),
            }

            match &m3 {
                Message::ReadRequest {
                    upstairs_id,
                    session_id,
                    job_id,
                    dependencies,
                    ..
                } => {
                    if io_eid == eid {
                        assert!(dependencies.contains(&dep_job_id[2]));
                    }

                    let (block, data) = make_blank_read_response();
                    responses[2].push(Message::ReadResponse {
                        header: ReadResponseHeader {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            blocks: Ok(vec![block]),
                        },
                        data,
                    });
                    dep_job_id[2] = *job_id;
                }
                _ => panic!("saw {m3:?}"),
            }

            // write
            harness.spawn(move |guest| async move {
                let bytes = BytesMut::from(vec![1u8; 512].as_slice());
                guest
                    .write(BlockIndex(io_eid.0 as u64 * 10), bytes)
                    .await
                    .unwrap();
            });

            if io_eid <= eid {
                // IO at or below the extent under repair is sent to the
                // downstairs under repair.
                let m1 = harness.ds1().recv().await.unwrap();

                match &m1 {
                    Message::Write {
                        header:
                            WriteHeader {
                                upstairs_id,
                                session_id,
                                job_id,
                                dependencies,
                                ..
                            },
                        ..
                    } => {
                        if io_eid == eid {
                            assert!(dependencies.contains(&dep_job_id[0]));
                        }

                        responses[0].push(Message::WriteAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        });
                        // Writes are blocking, so we need to update
                        // dep_job_id right away:
                        dep_job_id[0] = *job_id;
                    }
                    _ => panic!("saw {m1:?}"),
                }
            } else {
                // All IO above this is skipped for the downstairs under
                // repair.
                let r = harness.ds1().try_recv();
                assert!(
                    matches!(r, Err(TryRecvError::Empty)),
                    "unexpected IO: {r:?}"
                );
            }

            let m2 = harness.ds2.recv().await.unwrap();
            let m3 = harness.ds3.recv().await.unwrap();

            match &m2 {
                Message::Write {
                    header:
                        WriteHeader {
                            upstairs_id,
                            session_id,
                            job_id,
                            dependencies,
                            ..
                        },
                    ..
                } => {
                    if io_eid == eid {
                        assert!(dependencies.contains(&dep_job_id[1]));
                    }

                    responses[1].push(Message::WriteAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    });
                    dep_job_id[1] = *job_id;
                }
                _ => panic!("saw {m2:?}"),
            }

            match &m3 {
                Message::Write {
                    header:
                        WriteHeader {
                            upstairs_id,
                            session_id,
                            job_id,
                            dependencies,
                            ..
                        },
                    ..
                } => {
                    if io_eid == eid {
                        assert!(dependencies.contains(&dep_job_id[2]));
                    }

                    responses[2].push(Message::WriteAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    });
                    dep_job_id[2] = *job_id;
                }
                _ => panic!("saw {m3:?}"),
            }
        }

        // The repair task then waits for the close responses.

        let m1 = filter_out(&mut ds1_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveClose { .. })
        })
        .unwrap();
        let m2 = filter_out(&mut ds2_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveFlushClose { .. })
        })
        .unwrap();
        let m3 = filter_out(&mut ds3_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveFlushClose { .. })
        })
        .unwrap();

        match &m1 {
            Message::ExtentLiveClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                // ds1 didn't get the flush, it was set to faulted
                let gen = 1;
                let flush = 0;
                let dirty = false;

                harness
                    .ds1()
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap();
            }
            _ => panic!("saw {m1:?}"),
        }

        match &m2 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds2
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap()
            }
            _ => panic!("saw {m2:?}"),
        }

        match &m3 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds3
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .unwrap()
            }
            _ => panic!("saw {m3:?}"),
        }

        // Based on those gen, flush, and dirty values, ds1 should get the
        // ExtentLiveRepair message, while ds2 and ds3 should get
        // ExtentLiveNoOp.

        let m1 = harness.ds1().recv().await.unwrap();
        let m2 = harness.ds2.recv().await.unwrap();
        let m3 = harness.ds3.recv().await.unwrap();

        match &m1 {
            Message::ExtentLiveRepair {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                source_client_id,
                ..
            } => {
                assert!(*source_client_id != ClientId::new(0));
                assert!(*extent_id == eid);

                harness
                    .ds1()
                    .send(Message::ExtentLiveRepairAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap();
            }
            _ => panic!("saw {m3:?}"),
        }

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),
            _ => panic!("saw {m2:?}"),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),
            _ => panic!("saw {m2:?}"),
        }

        // Now, all downstairs will see ExtentLiveNoop

        let m1 = harness.ds1().recv().await.unwrap();
        let m2 = harness.ds2.recv().await.unwrap();
        let m3 = harness.ds3.recv().await.unwrap();

        match &m1 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds1()
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),
            _ => panic!("saw {m2:?}"),
        }

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),
            _ => panic!("saw {m2:?}"),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .unwrap(),
            _ => panic!("saw {m2:?}"),
        }

        // Finally, processing the ExtentLiveNoOp above means that the
        // dependencies for the final Reopen are all completed.

        let m1 = filter_out(&mut ds1_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveReopen { .. })
        })
        .unwrap();
        let m2 = filter_out(&mut ds2_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveReopen { .. })
        })
        .unwrap();
        let m3 = filter_out(&mut ds3_buffered_messages, |x| {
            matches!(x, Message::ExtentLiveReopen { .. })
        })
        .unwrap();

        match &m1 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                harness
                    .ds1()
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap()
            }
            _ => panic!("saw {m2:?}"),
        }

        match &m2 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                harness
                    .ds2
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap()
            }
            _ => panic!("saw {m2:?}"),
        }

        match &m3 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                assert!(*extent_id == eid);

                harness
                    .ds3
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .unwrap()
            }
            _ => panic!("saw {m2:?}"),
        }

        // After those are done, send out the read and write job responses
        for m in &responses[0] {
            harness.ds1().send(m.clone()).unwrap();
        }
        for m in &responses[1] {
            harness.ds2.send(m.clone()).unwrap();
        }
        for m in &responses[2] {
            harness.ds3.send(m.clone()).unwrap();
        }
    }

    // Expect the live repair to send a final flush
    {
        let flush_number = harness.ds1().ack_flush().await;
        assert_eq!(flush_number, 27);
        let flush_number = harness.ds2.ack_flush().await;
        assert_eq!(flush_number, 27);
        let flush_number = harness.ds3.ack_flush().await;
        assert_eq!(flush_number, 27);
    }

    // Try another read
    harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await.unwrap();
    });

    // All downstairs should see it
    harness.ds1().ack_read().await;
    harness.ds2.ack_read().await;
    harness.ds3.ack_read().await;
}

/// Test that we will mark a Downstairs as failed if we hit the byte limit
#[tokio::test]
async fn test_byte_fault_condition() {
    // Send enough bytes such that when we hit the client timeout, we are above
    // our bytes-in-flight limit (so the downstairs gets marked as faulted
    // instead of offline).
    //
    // Notice that we keep DS1 replying to pings through this process, so it
    // doesn't get set to offline early.
    let mut harness = TestHarness::new().await;

    // `num_jobs` sends enough bytes to hit the IO_OUTSTANDING_MAX_BYTES
    // condition on downstairs 1, which should mark it as faulted and kick it
    // out.
    const WRITE_SIZE: usize = 105 * 1024; // 105 KiB
    let write_buf = BytesMut::from(vec![1; WRITE_SIZE].as_slice()); // 50 KiB
    let num_jobs = IO_OUTSTANDING_MAX_BYTES as usize / write_buf.len() + 10;
    assert!(num_jobs < IO_OUTSTANDING_MAX_JOBS);

    // First, we'll send jobs until the timeout
    for _ in 0..num_jobs {
        // We must `spawn` here because `write` will wait for the response
        // to come back before returning
        let write_buf = write_buf.clone();
        let h = harness.spawn(move |guest| async move {
            guest.write(BlockIndex(0), write_buf).await.unwrap();
        });

        // Before we're kicked out, assert we're seeing the read requests
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::Write { .. },
        ));
        harness.ds2.ack_write().await;
        harness.ds3.ack_write().await;

        // With 2x responses, we can now await the write job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        assert_eq!(ds[ClientId::new(0)], DsState::Active);
        assert_eq!(ds[ClientId::new(1)], DsState::Active);
        assert_eq!(ds[ClientId::new(2)], DsState::Active);
    }

    // Sleep until we're confident that the Downstairs is kicked out
    let sleep_time = CLIENT_TIMEOUT_SECS + 5.0;
    info!(
        harness.log,
        "waiting {sleep_time} secs for Upstairs to kick out DS1"
    );
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs_f32(sleep_time)) => {
            // we're done!
        }
        // we don't listen to ds1 here, so we won't acknowledge any pings!
        v = harness.ds2.recv() => {
            panic!("received unexpected message on ds2: {v:?}")
        }
        v = harness.ds3.recv() => {
            panic!("received unexpected message on ds3: {v:?}")
        }
    }

    // Check to make sure that happened
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Faulted);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // Confirm that the system comes up after live-repair
    run_live_repair(harness).await;
}

/// Test that we will transition a downstairs from offline -> faulted if we hit
/// the byte limit after it's already offline
#[tokio::test]
async fn test_byte_fault_condition_offline() {
    let mut harness = TestHarness::new().await;
    harness.ds1().cfg.reply_to_ping = false;

    // Two different transitions occur during this test:
    // - We're not replying to pings, so DS1 will eventually transition from
    //   Active -> Offline (after 45 seconds).
    // - Then, after its job count hits IO_OUTSTANDING_MAX_BYTES, it will
    //   transition from Offline -> Faulted
    const MARGIN_SECS: f32 = 2.0;
    const SEND_JOBS_TIME: f32 = CLIENT_TIMEOUT_SECS - MARGIN_SECS;
    let start_time = tokio::time::Instant::now();

    // `num_jobs` sends enough bytes to hit the IO_OUTSTANDING_MAX_BYTES
    // condition on downstairs 1, which should mark it as faulted and kick it
    // out.
    const WRITE_SIZE: usize = 105 * 1024; // 105 KiB
    let write_buf = BytesMut::from(vec![1; WRITE_SIZE].as_slice()); // 50 KiB
    let num_jobs = IO_OUTSTANDING_MAX_BYTES as usize / write_buf.len() + 10;
    assert!(num_jobs < IO_OUTSTANDING_MAX_JOBS);

    // First, we'll send jobs until the timeout
    for i in 0..num_jobs / 2 {
        // Delay so that we hit SEND_JOBS_TIME at the end of this loop
        tokio::time::sleep_until(
            start_time
                + Duration::from_secs_f32(
                    SEND_JOBS_TIME * i as f32 / (num_jobs / 2) as f32,
                ),
        )
        .await;

        // We must `spawn` here because `write` will wait for the response
        // to come back before returning
        let write_buf = write_buf.clone();
        let h = harness.spawn(move |guest| async move {
            guest.write(BlockIndex(0), write_buf).await.unwrap();
        });

        // Before we're kicked out, assert we're seeing the read requests
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::Write { .. },
        ));
        harness.ds2.ack_write().await;
        harness.ds3.ack_write().await;

        // With 2x responses, we can now await the read job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        assert_eq!(ds[ClientId::new(0)], DsState::Active);
        assert_eq!(ds[ClientId::new(1)], DsState::Active);
        assert_eq!(ds[ClientId::new(2)], DsState::Active);
    }

    // Sleep until we're confident that the Downstairs is kicked out
    info!(harness.log, "waiting for Upstairs to kick out DS1");
    tokio::time::sleep(Duration::from_secs_f32(2.0 * MARGIN_SECS)).await;

    // Check to make sure that happened
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Offline);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // At this point, DS1 has `num_jobs` pending and is offline.  We can
    // transition it to `Faulted` by sending it enough to hit
    // `IO_OUTSTANDING_MAX_BYTES`
    for i in (num_jobs / 2)..num_jobs {
        let write_buf = write_buf.clone();
        let h = harness.spawn(move |guest| async move {
            guest.write(BlockIndex(0), write_buf).await.unwrap();
        });

        // After ds1 is kicked out, we shouldn't see any more messages
        match harness.ds1().try_recv() {
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
            x => {
                panic!("Write {i} should return EMPTY, but we got:{x:?}");
            }
        }
        harness.ds2.ack_write().await;
        harness.ds3.ack_write().await;

        // With 2x responses, we can now await the write job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        if (i + 1) * WRITE_SIZE < IO_OUTSTANDING_MAX_BYTES as usize {
            assert_eq!(ds[ClientId::new(0)], DsState::Offline);
            assert_eq!(ds[ClientId::new(1)], DsState::Active);
            assert_eq!(ds[ClientId::new(2)], DsState::Active);
        } else {
            assert_eq!(ds[ClientId::new(0)], DsState::Faulted);
            assert_eq!(ds[ClientId::new(1)], DsState::Active);
            assert_eq!(ds[ClientId::new(2)], DsState::Active);
        }
    }

    // Confirm that the system comes up after live-repair
    run_live_repair(harness).await;
}

#[tokio::test]
async fn test_offline_can_deactivate() {
    // Verify that we can deactivate with a downstairs Offline when no IOs are
    // outstanding.
    let mut harness = TestHarness::new().await;
    harness.ds1().cfg.reply_to_ping = false;

    // We're not replying to pings, so DS1 will eventually transition from
    // Active -> Offline (after CLIENT_TIMEOUT_SECS seconds).
    const MARGIN_SECS: f32 = 5.0;

    // Sleep until we're confident that the Downstairs is kicked out.  We do
    // a loop here so the other downstairs will have a chance to respond to
    // pings.
    let sleep_time = 5.0;
    let loop_stop_time = CLIENT_TIMEOUT_SECS + MARGIN_SECS;
    let mut loop_time = 0.0;

    // Sleep until we're confident that the Downstairs is kicked out
    while loop_time < loop_stop_time {
        tokio::time::sleep(Duration::from_secs_f32(sleep_time)).await;
        // Respond to pings, but drop anything else (we should not get
        // anything else)
        let _ = harness.ds2.try_recv();
        let _ = harness.ds3.try_recv();

        loop_time += sleep_time;
    }

    // Check to make sure downstairs 1 is now offline.
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Offline);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // Send a deactivate request.  Since we have no IO, this should happen
    // right away.
    harness.guest.deactivate().await.unwrap();
}

#[tokio::test]
async fn test_offline_with_io_can_deactivate() {
    // Verify that we can deactivate with a downstairs Offline when there are
    // IOs are outstanding.
    let mut harness = TestHarness::new().await;
    harness.ds1().cfg.reply_to_ping = false;

    // We're not replying to pings, so DS1 will eventually transition from
    // Active -> Offline (after CLIENT_TIMEOUT_SECS seconds).
    const MARGIN_SECS: f32 = 5.0;

    // Sleep until we're confident that the Downstairs is kicked out.  We do
    // a loop here so the other downstairs will have a chance to respond to
    // pings.
    let sleep_time = 5.0;
    let loop_stop_time = CLIENT_TIMEOUT_SECS + MARGIN_SECS;
    let mut loop_time = 0.0;

    // Sleep until we're confident that the Downstairs is kicked out
    while loop_time < loop_stop_time {
        tokio::time::sleep(Duration::from_secs_f32(sleep_time)).await;
        // Respond to pings, but drop anything else (we should not get
        // anything else)
        let _ = harness.ds2.try_recv();
        let _ = harness.ds3.try_recv();

        loop_time += sleep_time;
    }

    // Check to make sure downstairs 1 is now offline.
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Offline);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // We must `spawn` here because `read` will wait for the response to
    // come back before returning
    let h = harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await.unwrap();
    });

    // DS1 should not be receiving messages
    // Respond with read responses for downstairs 2 and 3
    harness.ds2.ack_read().await;
    harness.ds3.ack_read().await;

    // With 1x responses, we can now await the read job (which ensures that
    // the Upstairs has finished updating its state).
    h.await.unwrap();

    // Send a deactivate request.  This sends a final flush, and should push
    // the Offline downstairs to Faulted, which will clean up outstanding jobs.
    let deactivate_handle =
        harness.spawn(|guest| async move { guest.deactivate().await });

    // Ack the final flush on the two remaining downstairs.
    harness.ds2.ack_flush().await;
    harness.ds3.ack_flush().await;

    // At this point the remaining downstairs should have ack'd the final flush
    // and the deactivation should move forward so we can wait on the
    // deactivation completion.
    deactivate_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_all_offline_with_io_can_deactivate() {
    // Verify that we can deactivate with all downstairs Offline when there
    // are IOs are outstanding.
    let harness = TestHarness::new().await;

    // We're not replying to pings, so DS1 will eventually transition from
    // Active -> Offline (after CLIENT_TIMEOUT_SECS seconds).
    const MARGIN_SECS: f32 = 5.0;

    // Sleep until we're confident that all Downstairs are kicked out.
    let wait_time = CLIENT_TIMEOUT_SECS + MARGIN_SECS;
    tokio::time::sleep(Duration::from_secs_f32(wait_time)).await;

    // Check to make sure all downstairs are offline.
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Offline);
    assert_eq!(ds[ClientId::new(1)], DsState::Offline);
    assert_eq!(ds[ClientId::new(2)], DsState::Offline);

    // We must `spawn` here because `read` will wait for the response to
    // come back before returning
    let h = harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await
    });

    // Send a deactivate request.  This sends a final flush, and should push
    // all the downstairs to Faulted, which will clean up outstanding jobs.
    let deactivate_handle =
        harness.spawn(|guest| async move { guest.deactivate().await });

    // Now that we sent the deactivate, the read should complete with error.
    assert!(h.await.unwrap().is_err());

    // The deactivate request should also finish, as all downstairs are
    // offline and can do no work.
    deactivate_handle.await.unwrap().unwrap();
}

/// Test that we will mark a Downstairs as failed if we hit the job limit
#[tokio::test]
async fn test_job_fault_condition() {
    let mut harness = TestHarness::new().await;

    // We're going to queue up > IO_OUTSTANDING_MAX_JOBS, then wait for a
    // timeout, so that when timeout hits, the downstairs will become Faulted
    // instead of Offline.
    let num_jobs = IO_OUTSTANDING_MAX_JOBS + 200;
    for _ in 0..num_jobs {
        // We must `spawn` here because `write` will wait for the response to
        // come back before returning
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // DS1 should be receiving messages
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        // Respond with read responses for downstairs 2 and 3
        harness.ds2.ack_read().await;
        harness.ds3.ack_read().await;

        // With 1x responses, we can now await the read job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        assert_eq!(ds[ClientId::new(0)], DsState::Active);
        assert_eq!(ds[ClientId::new(1)], DsState::Active);
        assert_eq!(ds[ClientId::new(2)], DsState::Active);
    }

    // Sleep until we're confident that the Downstairs is kicked out
    //
    // Because it has so many pending jobs, it will become Faulted instead of
    // Offline (or rather, will transition Active -> Offline -> Faulted
    // immediately).
    let sleep_time = CLIENT_TIMEOUT_SECS + 5.0;
    info!(harness.log, "waiting for Upstairs to kick out DS1");
    info!(
        harness.log,
        "waiting {sleep_time} secs for Upstairs to kick out DS1"
    );
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs_f32(sleep_time)) => {
            // we're done!
        }
        // we don't listen to ds1 here, so we won't acknowledge any pings!
        v = harness.ds2.recv() => {
            panic!("received unexpected message on ds2: {v:?}")
        }
        v = harness.ds3.recv() => {
            panic!("received unexpected message on ds3: {v:?}")
        }
    }

    // Check to make sure that happened
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Faulted);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // Confirm that the system comes up after live-repair
    run_live_repair(harness).await;
}

/// Test that we will transition a downstairs from offline -> faulted if we hit
/// the job limit after it's already offline
#[tokio::test]
async fn test_job_fault_condition_offline() {
    let mut harness = TestHarness::new().await;
    harness.ds1().cfg.reply_to_ping = false;

    // Two different transitions occur during this test:
    // - We're not replying to pings, so DS1 will eventually transition from
    //   Active -> Offline (after 45 seconds).
    // - Then, after its job count hits IO_OUTSTANDING_MAX_JOBS, it will
    //   transition from Offline -> Faulted

    const MARGIN_SECS: f32 = 2.0;
    const SEND_JOBS_TIME: f32 = CLIENT_TIMEOUT_SECS - MARGIN_SECS;
    let num_jobs = IO_OUTSTANDING_MAX_JOBS / 2;
    let start_time = tokio::time::Instant::now();

    for i in 0..num_jobs {
        // Delay so that we hit SEND_JOBS_TIME at the end of this loop
        tokio::time::sleep_until(
            start_time
                + Duration::from_secs_f32(
                    SEND_JOBS_TIME * i as f32 / num_jobs as f32,
                ),
        )
        .await;

        // We must `spawn` here because `write` will wait for the response to
        // come back before returning
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // DS1 should be receiving messages
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        // Respond with read responses for downstairs 2 and 3
        harness.ds2.ack_read().await;
        harness.ds3.ack_read().await;

        // With 1x responses, we can now await the read job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        assert_eq!(ds[ClientId::new(0)], DsState::Active);
        assert_eq!(ds[ClientId::new(1)], DsState::Active);
        assert_eq!(ds[ClientId::new(2)], DsState::Active);
    }

    // Sleep until we're confident that the Downstairs is kicked out
    info!(harness.log, "waiting for Upstairs to kick out DS1");
    tokio::time::sleep(Duration::from_secs_f32(2.0 * MARGIN_SECS)).await;

    // Check to make sure that happened
    let ds = harness.guest.downstairs_state().await.unwrap();
    assert_eq!(ds[ClientId::new(0)], DsState::Offline);
    assert_eq!(ds[ClientId::new(1)], DsState::Active);
    assert_eq!(ds[ClientId::new(2)], DsState::Active);

    // At this point, DS1 has `num_jobs` pending and is offline.  We can
    // transition it to `Faulted` by sending it enough to hit
    // `IO_OUTSTANDING_MAX_JOBS`
    info!(harness.log, "sending more jobs to fault DS1");
    for i in num_jobs..IO_OUTSTANDING_MAX_JOBS + 200 {
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // DS1 should never receive messages, because it's offline
        match harness.ds1().try_recv() {
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
            x => {
                panic!("Read {i} should return EMPTY, but we got:{x:?}");
            }
        }

        // Respond with read responses for downstairs 2 and 3
        harness.ds2.ack_read().await;
        harness.ds3.ack_read().await;

        // With 1x responses, we can now await the read job (which ensures that
        // the Upstairs has finished updating its state).
        h.await.unwrap();

        let ds = harness.guest.downstairs_state().await.unwrap();
        if i < IO_OUTSTANDING_MAX_JOBS {
            // At this point, we should still be offline
            assert_eq!(ds[ClientId::new(0)], DsState::Offline);
            assert_eq!(ds[ClientId::new(1)], DsState::Active);
            assert_eq!(ds[ClientId::new(2)], DsState::Active);
        } else {
            // After ds1 is kicked out, we shouldn't see any more messages
            assert_eq!(ds[ClientId::new(0)], DsState::Faulted);
            assert_eq!(ds[ClientId::new(1)], DsState::Active);
            assert_eq!(ds[ClientId::new(2)], DsState::Active);
        }
    }

    // Confirm that the system comes up after live-repair
    run_live_repair(harness).await;
}

/// Test that an error during the live repair doesn't halt indefinitely
#[tokio::test]
async fn test_error_during_live_repair_no_halt() {
    let mut harness = TestHarness::new().await;

    // Send some jobs, so that we test job skipping when DS1 is faulted.
    const NUM_JOBS: usize = 200;
    let mut job_ids = Vec::with_capacity(NUM_JOBS);

    for _ in 0..NUM_JOBS {
        // We must `spawn` here because `read` will wait for the response to
        // come back before returning
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // Assert we're seeing the read requests (without replying on DS1)
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        let job_id = harness.ds2.ack_read().await;
        job_ids.push(job_id);
        harness.ds3.ack_read().await;

        h.await.unwrap(); // we've received > 1x reply, so the read finishes
    }

    // Now, fault DS0 to begin live-repair
    harness
        .guest
        .fault_downstairs(ClientId::new(0))
        .await
        .unwrap();

    // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
    // flush_timeout set to 24 hours, we shouldn't see anything else
    assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
    assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

    // Flush to clean out skipped jobs
    {
        // We must `spawn` here because `flush` will wait for the response to
        // come back before returning
        let jh = harness.spawn(|guest| async move {
            guest.flush(None).await.unwrap();
        });

        harness.ds2.ack_flush().await;
        harness.ds3.ack_flush().await;

        // Wait for the flush to come back
        jh.await.unwrap();
    }

    // Confirm that DS1 has been disconnected (and cannot reply to jobs)
    {
        let (block, data) = make_blank_read_response();
        let session_id = harness.ds1().upstairs_session_id.unwrap();
        let upstairs_id = harness.guest.get_uuid().await.unwrap();
        match harness.ds1().send(Message::ReadResponse {
            header: ReadResponseHeader {
                upstairs_id,
                session_id,
                job_id: job_ids[0],
                blocks: Ok(vec![block]),
            },
            data: data.clone(),
        }) {
            Ok(()) => panic!("DS1 should be disconnected"),
            Err(e) => {
                info!(
                    harness.log,
                    "ds1 can't reply to job {}, because it's disconnected: {}",
                    job_ids[0],
                    e
                );
            }
        }
    }

    // Assert the Upstairs isn't sending ds1 more work, because it is
    // Faulted
    let v = harness.ds1().try_recv();
    match v {
        // We're either disconnected, or the queue is empty.
        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
            // This is expected, continue on
        }
        _ => {
            // Any other error (or success!) is unexpected
            panic!("try_recv returned {v:?}");
        }
    }

    // Reconnect ds1
    harness.restart_ds1().await;

    harness.ds1().negotiate_start().await;
    harness.ds1().negotiate_step_extent_versions_please().await;

    // The Upstairs will start sending LiveRepair related work, which may be
    // out of order. Buffer some here.

    let mut ds1_buffered_messages = vec![];
    let mut ds2_buffered_messages = vec![];
    let mut ds3_buffered_messages = vec![];

    // EID 0

    // The Upstairs first sends the close and reopen jobs
    for _ in 0..2 {
        ds1_buffered_messages.push(harness.ds1().recv().await.unwrap());
        ds2_buffered_messages.push(harness.ds2.recv().await.unwrap());
        ds3_buffered_messages.push(harness.ds3.recv().await.unwrap());
    }

    assert!(ds1_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
    assert!(ds2_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
    assert!(ds3_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

    assert!(ds1_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
    assert!(ds2_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
    assert!(ds3_buffered_messages
        .iter()
        .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));

    // The repair task then waits for the close responses.

    let m1 = filter_out(&mut ds1_buffered_messages, |x| {
        matches!(x, Message::ExtentLiveClose { .. })
    })
    .unwrap();
    let m2 = filter_out(&mut ds2_buffered_messages, |x| {
        matches!(x, Message::ExtentLiveFlushClose { .. })
    })
    .unwrap();
    let m3 = filter_out(&mut ds3_buffered_messages, |x| {
        matches!(x, Message::ExtentLiveFlushClose { .. })
    })
    .unwrap();

    match &m1 {
        Message::ExtentLiveClose {
            upstairs_id,
            session_id,
            job_id,
            extent_id,
            ..
        } => {
            assert!(*extent_id == ExtentId(0));

            // ds1 didn't get the flush, it was set to faulted
            let gen = 1;
            let flush = 0;
            let dirty = false;

            harness
                .ds1()
                .send(Message::ExtentLiveCloseAck {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok((gen, flush, dirty)),
                })
                .unwrap();
        }
        _ => panic!("saw {m1:?}"),
    }

    match &m2 {
        Message::ExtentLiveFlushClose {
            upstairs_id,
            session_id,
            job_id,
            extent_id,
            ..
        } => {
            assert!(*extent_id == ExtentId(0));

            // ds2 and ds3 did get a flush
            let gen = 0;
            let flush = 2;
            let dirty = false;

            harness
                .ds2
                .send(Message::ExtentLiveCloseAck {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok((gen, flush, dirty)),
                })
                .unwrap()
        }
        _ => panic!("saw {m2:?}"),
    }

    match &m3 {
        Message::ExtentLiveFlushClose {
            upstairs_id,
            session_id,
            job_id,
            extent_id,
            ..
        } => {
            assert!(*extent_id == ExtentId(0));

            // ds2 and ds3 did get a flush
            let gen = 0;
            let flush = 2;
            let dirty = false;

            harness
                .ds3
                .send(Message::ExtentLiveCloseAck {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok((gen, flush, dirty)),
                })
                .unwrap()
        }
        _ => panic!("saw {m3:?}"),
    }

    // Based on those gen, flush, and dirty values, ds1 should get the
    // ExtentLiveRepair message, while ds2 and ds3 should get
    // ExtentLiveNoOp.

    let m1 = harness.ds1().recv().await.unwrap();
    let m2 = harness.ds2.recv().await.unwrap();
    let m3 = harness.ds3.recv().await.unwrap();

    match &m1 {
        Message::ExtentLiveRepair {
            upstairs_id,
            session_id,
            job_id,
            extent_id,
            source_client_id,
            ..
        } => {
            assert!(*source_client_id != ClientId::new(0));
            assert!(*extent_id == ExtentId(0));

            // send back error report here!
            harness
                .ds1()
                .send(Message::ErrorReport {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    error: CrucibleError::GenericError(String::from(
                        "bad news, networks are tricky",
                    )),
                })
                .unwrap();
        }
        _ => panic!("saw {m3:?}"),
    }

    match &m2 {
        Message::ExtentLiveNoOp {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds2
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    match &m3 {
        Message::ExtentLiveNoOp {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds3
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    error!(harness.log, "dropping ds1 now!");

    // Now, all downstairs will see ExtentLiveNoop, except ds1, which will
    // abort itself due to an ErrorReport during an extent repair.

    // If the Upstairs doesn't disconnect and try to reconnect to the
    // downstairs, this test will get stuck here, and will not progress to
    // the negotate_start function below.
    error!(harness.log, "reconnecting ds1 now!");
    harness.restart_ds1().await;

    error!(harness.log, "ds1 negotiate start now!");
    harness.ds1().negotiate_start().await;
    error!(harness.log, "ds1 negotiate extent versions please now!");
    harness.ds1().negotiate_step_extent_versions_please().await;

    // Continue faking for downstairs 2 and 3 - the work that was occuring
    // for extent 0 should finish before the Upstairs aborts the repair
    // task.

    let m2 = harness.ds2.recv().await.unwrap();
    let m3 = harness.ds3.recv().await.unwrap();

    match &m2 {
        Message::ExtentLiveNoOp {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds2
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    match &m3 {
        Message::ExtentLiveNoOp {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds3
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    let m2 = filter_out(&mut ds2_buffered_messages, |x| {
        matches!(x, Message::ExtentLiveReopen { .. })
    })
    .unwrap();
    let m3 = filter_out(&mut ds3_buffered_messages, |x| {
        matches!(x, Message::ExtentLiveReopen { .. })
    })
    .unwrap();

    match &m2 {
        Message::ExtentLiveReopen {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds2
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    match &m3 {
        Message::ExtentLiveReopen {
            upstairs_id,
            session_id,
            job_id,
            ..
        } => harness
            .ds3
            .send(Message::ExtentLiveAckId {
                upstairs_id: *upstairs_id,
                session_id: *session_id,
                job_id: *job_id,
                result: Ok(()),
            })
            .unwrap(),
        _ => panic!("saw {m2:?}"),
    }

    // The Upstairs will abort the live repair task, and will send a final
    // flush to ds2 and ds3. The flush number will not be incremented as it
    // would have been for each extent.
    let flush_number = harness.ds2.ack_flush().await;
    assert_eq!(flush_number, 3);
    let flush_number = harness.ds3.ack_flush().await;
    assert_eq!(flush_number, 3);

    // After this, another repair task will start from the beginning, and
    // send a bunch of work to ds1 again.

    // ExtentLiveReopen will be sent first, because it acts as a gate for future
    // operations (which include it as a dependency)
    let msg = harness.ds1().recv().await.unwrap();
    assert!(
        matches!(
            msg,
            Message::ExtentLiveReopen {
                extent_id: ExtentId(0),
                ..
            },
        ),
        "expected ExtentLiveReopen, got {msg:?}"
    );

    let msg = harness.ds1().recv().await.unwrap();
    assert!(
        matches!(
            msg,
            Message::ExtentLiveClose {
                extent_id: ExtentId(0),
                ..
            },
        ),
        "expected ExtentLiveClose, got {msg:?}"
    );
}

/// Test that after giving up on a downstairs, setting it to faulted, and
/// letting it reconnect, live repair does *not* occur if the upstairs is
/// configured read-only.
#[tokio::test]
async fn test_no_read_only_live_repair() {
    let mut harness = TestHarness::new_ro().await;

    // Send some jobs, so that we test job skipping when DS1 is faulted.
    const NUM_JOBS: usize = 200;
    let mut job_ids = Vec::with_capacity(NUM_JOBS);

    for _ in 0..NUM_JOBS {
        // We must `spawn` here because `read` will wait for the response to
        // come back before returning
        let h = harness.spawn(|guest| async move {
            let mut buffer = Buffer::new(1, 512);
            guest.read(BlockIndex(0), &mut buffer).await.unwrap();
        });

        // Assert we're seeing the read requests (without replying on DS1)
        assert!(matches!(
            harness.ds1().recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        let job_id = harness.ds2.ack_read().await;
        job_ids.push(job_id);
        harness.ds3.ack_read().await;

        h.await.unwrap(); // after > 1x response, the read finishes
    }

    // Now, fault DS0 to begin live-repair
    harness
        .guest
        .fault_downstairs(ClientId::new(0))
        .await
        .unwrap();

    // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
    // flush_timeout set to 24 hours, we shouldn't see anything else
    assert!(matches!(harness.ds2.try_recv(), Err(TryRecvError::Empty)));
    assert!(matches!(harness.ds3.try_recv(), Err(TryRecvError::Empty)));

    // Flush to clean out skipped jobs
    {
        // We must `spawn` here because `flush` will wait for the
        // response to come back before returning
        let jh = harness.spawn(|guest| async move {
            guest.flush(None).await.unwrap();
        });

        harness.ds2.ack_flush().await;
        harness.ds3.ack_flush().await;

        // Wait for the flush to come back
        jh.await.unwrap();
    }

    // Confirm that DS1 has been disconnected (and cannot reply to jobs)
    {
        let (block, data) = make_blank_read_response();
        let session_id = harness.ds1().upstairs_session_id.unwrap();
        let upstairs_id = harness.guest.get_uuid().await.unwrap();
        match harness.ds1().send(Message::ReadResponse {
            header: ReadResponseHeader {
                upstairs_id,
                session_id,
                job_id: job_ids[0],
                blocks: Ok(vec![block]),
            },
            data: data.clone(),
        }) {
            Ok(()) => panic!("DS1 should be disconnected"),
            Err(e) => {
                info!(
                    harness.log,
                    "ds1 can't reply to job {}, because it's disconnected: {}",
                    job_ids[0],
                    e
                );
            }
        }
    }

    // Assert the Upstairs isn't sending ds1 more work, because it is
    // Faulted
    let v = harness.ds1().try_recv();
    match v {
        // We're either disconnected, or the queue is empty.
        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
            // This is expected, continue on
        }

        _ => {
            // Any other error (or success!) is unexpected
            panic!("try_recv returned {v:?}");
        }
    }

    // Reconnect ds1
    harness.restart_ds1().await;

    harness.ds1().negotiate_start().await;
    harness.ds1().negotiate_step_extent_versions_please().await;

    // Wait for all three downstairs to be online before we send
    // our final read.
    loop {
        let qwq = harness.guest.query_work_queue().await.unwrap();
        if qwq.active_count == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!(harness.log, "submitting final read!");

    // The read should be served as normal
    harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await.unwrap();
    });

    // All downstairs should see it
    harness.ds1().ack_read().await;
    harness.ds2.ack_read().await;
    harness.ds3.ack_read().await;
}

/// Test that deactivation doesn't fail if one client is slower than others
#[tokio::test]
async fn test_deactivate_slow() {
    let mut harness = TestHarness::new().await;

    // Queue up a read, so that the deactivate requires a flush
    // We must `spawn` here because `read` will wait for the
    // response to come back before returning
    harness.spawn(|guest| async move {
        let mut buffer = Buffer::new(1, 512);
        guest.read(BlockIndex(0), &mut buffer).await.unwrap();
    });

    // Ensure that all three clients got the read request
    harness.ds1().ack_read().await;
    harness.ds2.ack_read().await;
    harness.ds3.ack_read().await;

    // Send a deactivate request.  This sends a final flush
    let deactivate_handle =
        harness.spawn(|guest| async move { guest.deactivate().await });

    // Reply on ds1, which concludes the deactivation of this downstairs
    harness.ds1().ack_flush().await;

    // At this point, the upstairs sends YouAreNoLongerActive, but then
    // drops the sender end, so we can't actually see it.
    assert!(harness.ds1().recv().await.is_none());

    // Reconnect ds1, since the Downstairs always tries to reconnect
    let ds1 = harness.take_ds1();

    // Restart ds1, in a task because this won't actually connect right away
    const RECONNECT_NONE: u8 = 0;
    const RECONNECT_TRYING: u8 = 1;
    const RECONNECT_DONE: u8 = 2;
    let reconnected = Arc::new(AtomicU8::new(RECONNECT_NONE));
    let ds1_restart_handle = {
        let reconnected = reconnected.clone();
        tokio::spawn(async move {
            let cfg = ds1.cfg.clone();
            let log = ds1.log.clone();
            let uuid = ds1.uuid;
            let addr = ds1.halt().await;
            reconnected.store(RECONNECT_TRYING, Ordering::Release);
            let mut ds1 = cfg.restart(uuid, addr, log).await;

            ds1.negotiate_start().await;
            reconnected.store(RECONNECT_DONE, Ordering::Release);
            ds1.negotiate_step_extent_versions_please().await;
        })
    };

    // Give that task some time to try reconnecting.  It won't get anywhere,
    // because we don't automatically reconnect in this case.  Activating
    // will also fail, because we're still in deactivating.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);
    assert!(harness.guest.activate().await.is_err());

    // Finish deactivation on ds2 / ds3
    harness.ds2.ack_flush().await;
    assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);
    assert!(harness.guest.activate().await.is_err());
    harness.ds3.ack_flush().await;

    // At this point, the upstairs should be Initializing, but we still
    // won't connect because we don't automatically connect after being
    // Deactivated.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_TRYING);

    // At this point, deactivation is done and we can join that handle
    deactivate_handle.await.unwrap().unwrap();

    // Now, we can try to reactivate the guest.
    //
    // This won't work, because we haven't restarted ds2 and ds3, but it
    // will allow ds1 to reconnect.
    harness.spawn(|guest| async move { guest.activate().await.unwrap() });

    // At this point, ds1 should reconnect
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    ds1_restart_handle.await.unwrap();
    assert_eq!(reconnected.load(Ordering::Acquire), RECONNECT_DONE);
}

/// Test that replaying writes works
#[tokio::test]
async fn test_write_replay() {
    let mut harness = TestHarness::new().await;

    // Send a write, which will succeed
    let write_handle = harness.spawn(|guest| async move {
        let mut data = BytesMut::new();
        data.resize(512, 1u8);
        guest.write(BlockIndex(0), data).await.unwrap();
    });

    // Ensure that all three clients got the write request
    let job_id = harness.ds1().ack_write().await;
    harness.ds2.ack_write().await;
    harness.ds3.ack_write().await;

    // Check that the write worked
    write_handle.await.unwrap();

    // If downstairs 1 disconnects and reconnects, it should get the exact
    // same message replayed to it.
    harness.restart_ds1().await;

    harness.ds1().negotiate_start().await;
    harness.ds1().negotiate_step_last_flush(JobId(0)).await;

    // Ensure that we get the same Write
    // Send a reply, which is the second time this Write operation completes
    let new_job_id = harness.ds1().ack_write().await;
    assert_eq!(new_job_id, job_id);

    // Give it a second to think about it
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check that the guest hasn't panicked by sending it a message that
    // requires going to the worker thread.
    harness.guest.get_uuid().await.unwrap();
}
