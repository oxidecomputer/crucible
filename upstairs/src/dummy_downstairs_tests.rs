// Copyright 2023 Oxide Computer Company

#[cfg(test)]
pub(crate) mod protocol_test {
    use core::fmt::Error;
    use core::fmt::Formatter;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::up_main;
    use crate::BlockContext;
    use crate::BlockIO;
    use crate::Buffer;
    use crate::CrucibleError;
    use crate::Guest;
    use crate::IO_OUTSTANDING_MAX;
    use crate::MAX_ACTIVE_COUNT;
    use crucible_client_types::CrucibleOpts;
    use crucible_common::Block;
    use crucible_common::RegionDefinition;
    use crucible_common::RegionOptions;
    use crucible_protocol::ClientId;
    use crucible_protocol::CrucibleDecoder;
    use crucible_protocol::CrucibleEncoder;
    use crucible_protocol::JobId;
    use crucible_protocol::Message;

    use anyhow::bail;
    use anyhow::Result;
    use bytes::Bytes;
    use bytes::BytesMut;
    use futures::SinkExt;
    use futures::StreamExt;
    use slog::error;
    use slog::info;
    use slog::o;
    use slog::Drain;
    use slog::Logger;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use tokio_util::codec::FramedRead;
    use tokio_util::codec::FramedWrite;
    use uuid::Uuid;

    // Create a simple logger
    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    macro_rules! bail_assert {
        ($cond:expr) => {
            if !($cond) {
                bail!(concat!("failed at ", file!(), ":", line!()));
            }
        };
    }

    macro_rules! bail_assert_eq {
        ($left:expr, $right:expr) => {
            let lv = $left;
            let rv = $right;
            if lv != rv {
                bail!(format!(
                    "{:?} != {:?} at {}:{}",
                    lv,
                    rv,
                    file!(),
                    line!(),
                ));
            }
        };
    }

    pub struct Downstairs {
        log: Logger,
        listener: TcpListener,
        local_addr: SocketAddr,
        _repair_listener: TcpListener,
        repair_addr: SocketAddr,
        uuid: Uuid,
        read_only: bool,

        extent_count: u32,
        extent_size: Block,

        gen_numbers: Vec<u64>,
        flush_numbers: Vec<u64>,
        dirty_bits: Vec<bool>,
    }

    pub struct ConnectedDownstairs {
        inner: Downstairs,

        upstairs_session_id: Mutex<Option<Uuid>>,

        fr: Arc<Mutex<FramedCrucibleRead>>,
        fw: Arc<Mutex<FramedCrucibleWrite>>,
    }

    impl std::fmt::Debug for ConnectedDownstairs {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
            f.write_str("ConnectedDownstairs")?;
            Ok(())
        }
    }

    type FramedCrucibleRead =
        FramedRead<tokio::net::tcp::OwnedReadHalf, CrucibleDecoder>;
    type FramedCrucibleWrite =
        FramedWrite<tokio::net::tcp::OwnedWriteHalf, CrucibleEncoder>;

    impl Downstairs {
        pub async fn new(log: Logger) -> Downstairs {
            let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

            let listener = TcpListener::bind(&bind_addr).await.unwrap();
            let local_addr = listener.local_addr().unwrap();

            let repair_listener = TcpListener::bind(&bind_addr).await.unwrap();
            let repair_addr = repair_listener.local_addr().unwrap();

            Downstairs {
                log,
                listener,
                local_addr,
                _repair_listener: repair_listener,
                repair_addr,
                uuid: Uuid::new_v4(),
                read_only: false,

                extent_count: 10,
                extent_size: Block::new_512(10),

                gen_numbers: vec![0u64; 10],
                flush_numbers: vec![0u64; 10],
                dirty_bits: vec![false; 10],
            }
        }

        pub fn set_read_only(&mut self) {
            self.read_only = true;
        }

        pub async fn into_connected_downstairs(self) -> ConnectedDownstairs {
            let (sock, _raddr) = self.listener.accept().await.unwrap();

            let (read, write) = sock.into_split();

            let fr = Arc::new(Mutex::new(FramedRead::new(
                read,
                CrucibleDecoder::new(),
            )));
            let fw = Arc::new(Mutex::new(FramedWrite::new(
                write,
                CrucibleEncoder::new(),
            )));

            ConnectedDownstairs {
                inner: self,
                upstairs_session_id: Mutex::new(None),
                fr,
                fw,
            }
        }

        pub fn get_region_options(&self) -> RegionOptions {
            let mut region_options = RegionOptions::default();
            region_options.set_block_size(512);
            region_options.set_extent_size(self.extent_size);
            region_options.set_uuid(self.uuid);
            region_options.set_encrypted(false);

            region_options.validate().unwrap();

            region_options
        }

        pub fn get_region_definition(&self) -> RegionDefinition {
            let mut def =
                RegionDefinition::from_options(&self.get_region_options())
                    .unwrap();

            def.set_extent_count(self.extent_count);

            def
        }
    }

    impl ConnectedDownstairs {
        pub fn close(self) -> Downstairs {
            self.inner
        }

        pub async fn negotiate_start(&self) -> Result<()> {
            // We loop here as a way of ignoring ping (Ruok) packets while
            // we wait for negotiation messages.

            loop {
                let packet = self
                    .fr
                    .lock()
                    .await
                    .next()
                    .await
                    .transpose()
                    .unwrap()
                    .unwrap();

                match &packet {
                    Message::HereIAm {
                        version,
                        upstairs_id: _,
                        session_id: _,
                        gen: _,
                        read_only,
                        encrypted: _,
                        alternate_versions: _,
                    } => {
                        info!(
                            self.inner.log,
                            "negotiate packet {:?} (upstairs read-only {})",
                            packet,
                            read_only
                        );

                        if *read_only != self.inner.read_only {
                            bail!("read only mismatch!");
                        }

                        self.fw
                            .lock()
                            .await
                            .send(Message::YesItsMe {
                                version: *version,
                                repair_addr: self.inner.repair_addr,
                            })
                            .await
                            .unwrap();

                        break;
                    }

                    Message::Ruok => {
                        // Respond to pings right away
                        if let Err(e) =
                            self.fw.lock().await.send(Message::Imok).await
                        {
                            error!(self.inner.log, "negotiate_start could not send on fw due to {}", e);
                        }
                        info!(self.inner.log, "responded to ping");

                        continue;
                    }

                    x => {
                        bail!("wrong packet {:?}, expected HereIAm or Ruok", x)
                    }
                }
            }

            loop {
                let packet = self
                    .fr
                    .lock()
                    .await
                    .next()
                    .await
                    .transpose()
                    .unwrap()
                    .unwrap();

                match &packet {
                    Message::PromoteToActive {
                        upstairs_id,
                        session_id,
                        gen,
                    } => {
                        bail_assert!(*gen == 1);

                        info!(self.inner.log, "negotiate packet {:?}", packet);

                        // Record the session id the upstairs sent us
                        *self.upstairs_session_id.lock().await =
                            Some(*session_id);

                        self.fw
                            .lock()
                            .await
                            .send(Message::YouAreNowActive {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                gen: *gen,
                            })
                            .await
                            .unwrap();

                        break;
                    }

                    Message::Ruok => {
                        // Respond to pings right away
                        if let Err(e) =
                            self.fw.lock().await.send(Message::Imok).await
                        {
                            error!(self.inner.log, "negotiate_start could not send on fw due to {}", e);
                        }
                        info!(self.inner.log, "responded to ping");

                        continue;
                    }

                    x => bail!(
                        "wrong packet {:?}, expected PromoteToActive or Ruok",
                        x
                    ),
                }
            }

            loop {
                let packet = self
                    .fr
                    .lock()
                    .await
                    .next()
                    .await
                    .transpose()
                    .unwrap()
                    .unwrap();

                match &packet {
                    Message::RegionInfoPlease => {
                        info!(self.inner.log, "negotiate packet {:?}", packet);

                        self.fw
                            .lock()
                            .await
                            .send(Message::RegionInfo {
                                region_def: self.inner.get_region_definition(),
                            })
                            .await
                            .unwrap();
                        break Ok(());
                    }

                    Message::Ruok => {
                        // Respond to pings right away
                        if let Err(e) =
                            self.fw.lock().await.send(Message::Imok).await
                        {
                            error!(self.inner.log, "negotiate_start could not send on fw due to {}", e);
                        }
                        info!(self.inner.log, "responded to ping");

                        continue;
                    }

                    x => bail!(
                        "wrong packet: {:?}, expected RegionInfoPlease or Ruok",
                        x
                    ),
                }
            }
        }

        pub async fn negotiate_step_extent_versions_please(
            &self,
        ) -> Result<()> {
            let packet = self
                .fr
                .lock()
                .await
                .next()
                .await
                .transpose()
                .unwrap()
                .unwrap();

            match &packet {
                Message::ExtentVersionsPlease => {
                    info!(self.inner.log, "negotiate packet {:?}", packet);

                    self.fw
                        .lock()
                        .await
                        .send(Message::ExtentVersions {
                            gen_numbers: self.inner.gen_numbers.clone(),
                            flush_numbers: self.inner.flush_numbers.clone(),
                            dirty_bits: self.inner.dirty_bits.clone(),
                        })
                        .await
                        .unwrap();
                }

                x => bail!(
                    "wrong packet: {:?}, expected ExtentVersionsPlease",
                    x
                ),
            }

            Ok(())
        }

        pub async fn negotiate_step_last_flush(
            &self,
            last_flush_number: JobId,
        ) -> Result<()> {
            let packet = self
                .fr
                .lock()
                .await
                .next()
                .await
                .transpose()
                .unwrap()
                .unwrap();

            match &packet {
                Message::LastFlush { .. } => {
                    info!(self.inner.log, "negotiate packet {:?}", packet);

                    self.fw
                        .lock()
                        .await
                        .send(Message::LastFlushAck { last_flush_number })
                        .await
                        .unwrap();
                }

                x => bail!("wrong packet: {:?}, expected LastFlush", x),
            }

            Ok(())
        }

        // Spawn a task to pull messages off the framed reader and put into a
        // channel
        pub async fn spawn_message_receiver(
            &self,
        ) -> (JoinHandle<()>, mpsc::Receiver<Message>) {
            let (tx, rx) = mpsc::channel(1000);
            let fw = self.fw.clone();
            let fr = self.fr.clone();
            let log = self.inner.log.clone();

            let jh = tokio::spawn(async move {
                loop {
                    match fr.lock().await.next().await.transpose() {
                        Ok(v) => match v {
                            None => {
                                // disconnection, bail
                                error!(log, "spawn_message_receiver saw disconnect, bailing");
                                return;
                            }

                            Some(Message::Ruok) => {
                                // Respond to pings right away
                                if let Err(e) =
                                    fw.lock().await.send(Message::Imok).await
                                {
                                    error!(log, "spawn_message_receiver could not send on fw due to {}", e);
                                }
                                info!(log, "responded to ping");
                            }

                            Some(m) => {
                                tx.send(m).await.unwrap();
                            }
                        },

                        Err(e) => {
                            error!(
                                log,
                                "spawn_message_receiver died due to {}", e
                            );
                            break;
                        }
                    }
                }
            });

            (jh, rx)
        }
    }

    pub struct TestHarness {
        log: Logger,
        ds1: Mutex<Option<Arc<ConnectedDownstairs>>>,
        ds2: Arc<ConnectedDownstairs>,
        ds3: Arc<ConnectedDownstairs>,
        _join_handle: JoinHandle<()>,
        guest: Arc<Guest>,
    }

    impl TestHarness {
        pub async fn new() -> Result<TestHarness> {
            Self::new_(false).await
        }

        pub async fn new_ro() -> Result<TestHarness> {
            Self::new_(true).await
        }

        async fn new_(read_only: bool) -> Result<TestHarness> {
            let log = csl();

            let mut ds1 = Downstairs::new(log.new(o!("downstairs" => 1))).await;
            let mut ds2 = Downstairs::new(log.new(o!("downstairs" => 2))).await;
            let mut ds3 = Downstairs::new(log.new(o!("downstairs" => 3))).await;

            if read_only {
                ds1.set_read_only();
                ds2.set_read_only();
                ds3.set_read_only();
            }

            // Configure our guest without queue backpressure, to speed up tests
            // which require triggering a timeout
            let mut g = Guest::new();
            g.backpressure_config.queue_max_delay = Duration::ZERO;
            let guest = Arc::new(g);

            let crucible_opts = CrucibleOpts {
                id: Uuid::new_v4(),
                target: vec![ds1.local_addr, ds2.local_addr, ds3.local_addr],
                flush_timeout: Some(86400.0),
                read_only,

                ..Default::default()
            };

            let join_handle = up_main(
                crucible_opts,
                1,
                None,
                guest.clone(),
                None,
                Some(log.new(o!("upstairs" => 1))),
            )
            .await
            .unwrap();

            let ds1 = Arc::new(ds1.into_connected_downstairs().await);
            let ds2 = Arc::new(ds2.into_connected_downstairs().await);
            let ds3 = Arc::new(ds3.into_connected_downstairs().await);

            let mut handles: Vec<JoinHandle<Result<()>>> = vec![];

            {
                let guest = guest.clone();
                handles.push(tokio::spawn(async move {
                    guest.activate().await?;
                    Ok(())
                }));
            }
            {
                let ds1 = ds1.clone();
                handles.push(tokio::spawn(async move {
                    ds1.negotiate_start().await?;
                    ds1.negotiate_step_extent_versions_please().await?;
                    Ok(())
                }));
            }
            {
                let ds2 = ds2.clone();
                handles.push(tokio::spawn(async move {
                    ds2.negotiate_start().await?;
                    ds2.negotiate_step_extent_versions_please().await?;
                    Ok(())
                }));
            }
            {
                let ds3 = ds3.clone();
                handles.push(tokio::spawn(async move {
                    ds3.negotiate_start().await?;
                    ds3.negotiate_step_extent_versions_please().await?;
                    Ok(())
                }));
            }

            for handle in handles {
                handle.await.unwrap()?;
            }

            for _ in 0..10 {
                if guest.query_is_active().await.unwrap() {
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            bail_assert!(guest.query_is_active().await.unwrap());

            Ok(TestHarness {
                log,
                ds1: Mutex::new(Some(ds1)),
                ds2,
                ds3,
                _join_handle: join_handle,
                guest,
            })
        }

        pub async fn ds1(&self) -> Arc<ConnectedDownstairs> {
            let ds1 = &*self.ds1.lock().await;
            ds1.as_ref().unwrap().clone()
        }

        pub async fn take_ds1(&self) -> ConnectedDownstairs {
            let ds1_arc = self.ds1.lock().await.take().unwrap();
            Arc::try_unwrap(ds1_arc).unwrap()
        }
    }

    fn make_blank_read_response() -> crucible_protocol::ReadResponse {
        let data = vec![0u8; 512];
        let hash = crucible_common::integrity_hash(&[&data]);

        crucible_protocol::ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data: BytesMut::from(&data[..]),
            block_contexts: vec![BlockContext {
                hash,
                encryption_context: None,
            }],
        }
    }

    /// Filter the first element that matches some predicate out of a list
    pub fn filter_out<T, P>(l: &mut Vec<T>, pred: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        let idx = l.iter().position(pred);
        idx.map(|i| l.remove(i))
    }

    /// Test that flow control kicks in at MAX_ACTIVE_COUNT jobs, and until the
    /// downstairs responds Ok for a job, no more work is sent. Once three
    /// downstairs responds with a read response for a certain job, then more
    /// work is sent.
    #[tokio::test]
    async fn test_flow_control() -> Result<()> {
        let harness = Arc::new(TestHarness::new().await?);

        let (_jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        for _ in 0..MAX_ACTIVE_COUNT {
            let harness = harness.clone();

            // We must tokio::spawn here because `read` will wait for the
            // response to come back before returning
            tokio::spawn(async move {
                let buffer = Buffer::new(512);
                harness.guest.read(Block::new_512(0), buffer).await.unwrap();
            });
        }

        let mut job_ids = Vec::with_capacity(MAX_ACTIVE_COUNT);

        for _ in 0..MAX_ACTIVE_COUNT {
            match ds1_messages.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            bail_assert!(matches!(
                ds2_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));
        }

        // Confirm that's all the Upstairs sent us - with the flush_timeout set
        // to 24 hours, we shouldn't see anything else

        bail_assert!(matches!(
            ds1_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Performing any guest reads will not send them to the downstairs

        {
            let harness = harness.clone();

            tokio::spawn(async move {
                let buffer = Buffer::new(512);
                harness.guest.read(Block::new_512(0), buffer).await.unwrap();
            });
        }

        bail_assert!(matches!(
            ds1_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Once the downstairs respond with a ReadRequest for a job, then more
        // work will be sent downstairs

        harness
            .ds1()
            .await
            .fw
            .lock()
            .await
            .send(Message::ReadResponse {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness
                    .ds1()
                    .await
                    .upstairs_session_id
                    .lock()
                    .await
                    .unwrap(),
                job_id: job_ids[0],
                responses: Ok(vec![make_blank_read_response()]),
            })
            .await
            .unwrap();

        harness
            .ds2
            .fw
            .lock()
            .await
            .send(Message::ReadResponse {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness
                    .ds2
                    .upstairs_session_id
                    .lock()
                    .await
                    .unwrap(),
                job_id: job_ids[0],
                responses: Ok(vec![make_blank_read_response()]),
            })
            .await
            .unwrap();

        harness
            .ds3
            .fw
            .lock()
            .await
            .send(Message::ReadResponse {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness
                    .ds3
                    .upstairs_session_id
                    .lock()
                    .await
                    .unwrap(),
                job_id: job_ids[0],
                responses: Ok(vec![make_blank_read_response()]),
            })
            .await
            .unwrap();

        // Assert that we now see one more read request
        let mut ds1_final_read_request = None;
        let mut ds2_final_read_request = None;
        let mut ds3_final_read_request = None;

        for _ in 0..3 {
            if ds1_final_read_request.is_some()
                && ds2_final_read_request.is_some()
                && ds3_final_read_request.is_some()
            {
                break;
            }

            if let Ok(m) = ds1_messages.try_recv() {
                ds1_final_read_request = Some(m);
            }
            if let Ok(m) = ds2_messages.try_recv() {
                ds2_final_read_request = Some(m);
            }
            if let Ok(m) = ds3_messages.try_recv() {
                ds3_final_read_request = Some(m);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        bail_assert!(matches!(
            ds1_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        bail_assert!(matches!(
            ds1_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));

        bail_assert!(matches!(
            ds2_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));

        bail_assert!(matches!(
            ds3_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));

        Ok(())
    }

    /// Test that replay occurs after a downstairs disconnects and reconnects
    #[tokio::test]
    async fn test_replay_occurs() -> Result<()> {
        let harness = Arc::new(TestHarness::new().await?);

        let (jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        // Send a read
        {
            let harness = harness.clone();

            // We must tokio::spawn here because `read` will wait for the
            // response to come back before returning
            tokio::spawn(async move {
                let buffer = Buffer::new(512);
                harness.guest.read(Block::new_512(0), buffer).await.unwrap();
            });
        }

        // Confirm all downstairs receive said read
        let ds1_message = ds1_messages.recv().await.unwrap();

        bail_assert!(matches!(ds1_message, Message::ReadRequest { .. }));

        bail_assert!(matches!(
            ds2_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        bail_assert!(matches!(
            ds3_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        // If downstairs 1 disconnects and reconnects, it should get the exact
        // same message replayed to it.

        drop(ds1_messages);
        jh1.abort();

        let ds1 = harness.take_ds1().await;
        let ds1 = ds1.close();
        let ds1 = ds1.into_connected_downstairs().await;

        ds1.negotiate_start().await?;
        ds1.negotiate_step_last_flush(JobId(0)).await?;

        let (_jh1, mut ds1_messages) = ds1.spawn_message_receiver().await;

        let mut ds1_message_second_time = None;

        for _ in 0..10 {
            if let Ok(m) = ds1_messages.try_recv() {
                ds1_message_second_time = Some(m);
                break;
            }

            eprintln!("waiting for ds1 message in test_replay_occurs");

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        bail_assert_eq!(ds1_message, ds1_message_second_time.unwrap());

        Ok(())
    }

    /// Test that after giving up on a downstairs, setting it to faulted, and
    /// letting it reconnect, live repair occurs. Check that each extent is
    /// repaired with the correct source, and that extent limits are honoured if
    /// additional IO comes through.
    #[tokio::test]
    async fn test_successful_live_repair() -> Result<()> {
        let harness = Arc::new(TestHarness::new().await?);

        let (jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        // Send 200 more than IO_OUTSTANDING_MAX jobs. Flow control will kick in
        // at MAX_ACTIVE_COUNT messages, so we need to be sending read responses
        // while reads are being sent. After IO_OUTSTANDING_MAX jobs, the
        // Upstairs will set ds1 to faulted, and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let harness = harness.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let buffer = Buffer::new(512);
                    harness
                        .guest
                        .read(Block::new_512(0), buffer)
                        .await
                        .unwrap();
                });
            }

            if i < MAX_ACTIVE_COUNT {
                // Before flow control kicks in, assert we're seeing the read
                // requests
                bail_assert!(matches!(
                    ds1_messages.recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After flow control kicks in, we shouldn't see any more
                // messages
                match ds1_messages.try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match ds2_messages.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let harness = harness.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    harness.guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match ds2_messages.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate().take(MAX_ACTIVE_COUNT) {
            match harness
                .ds1()
                .await
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds1()
                        .await
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: *job_id,
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = ds1_messages.try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        drop(ds1_messages);
        jh1.abort();

        let ds1 = harness.take_ds1().await;
        let ds1 = ds1.close();
        let ds1 = ds1.into_connected_downstairs().await;

        ds1.negotiate_start().await?;
        ds1.negotiate_step_extent_versions_please().await?;

        let (_jh1, mut ds1_messages) = ds1.spawn_message_receiver().await;

        // The Upstairs will start sending LiveRepair related work, which may be
        // out of order. Buffer some here.

        let mut ds1_buffered_messages = vec![];
        let mut ds2_buffered_messages = vec![];
        let mut ds3_buffered_messages = vec![];

        for eid in 0..10 {
            // The Upstairs first sends the close and reopen jobs
            for _ in 0..2 {
                ds1_buffered_messages.push(ds1_messages.recv().await.unwrap());
                ds2_buffered_messages.push(ds2_messages.recv().await.unwrap());
                ds3_buffered_messages.push(ds3_messages.recv().await.unwrap());
            }

            bail_assert!(ds1_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
            bail_assert!(ds2_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
            bail_assert!(ds3_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

            bail_assert!(ds1_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
            bail_assert!(ds2_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
            bail_assert!(ds3_buffered_messages
                .iter()
                .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));

            // This is the reopen job id for extent eid

            let reopen_job_id = {
                let m = ds1_buffered_messages
                    .iter()
                    .position(|m| matches!(m, Message::ExtentLiveReopen { .. }))
                    .unwrap();
                match ds1_buffered_messages[m] {
                    Message::ExtentLiveReopen { job_id, .. } => {
                        job_id
                    }

                    _ => bail!("ds1_buffered_messages[m] not Message::ExtentLiveReopen"),
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
            // repair is done, otherwise flow control will kick in.

            let mut responses = vec![Vec::new(); 3];

            for io_eid in 0usize..10 {
                let mut dep_job_id = [reopen_job_id; 3];
                // read

                {
                    let harness = harness.clone();
                    tokio::spawn(async move {
                        let buffer = Buffer::new(512);
                        harness
                            .guest
                            .read(Block::new_512(io_eid as u64 * 10), buffer)
                            .await
                            .unwrap();
                    })
                };

                if io_eid <= eid {
                    // IO at or below the extent under repair is sent to the
                    // downstairs under repair.
                    let m1 = ds1_messages.recv().await.unwrap();

                    match &m1 {
                        Message::ReadRequest {
                            upstairs_id,
                            session_id,
                            job_id,
                            dependencies,
                            ..
                        } => {
                            if io_eid == eid {
                                bail_assert!(
                                    dependencies.contains(&dep_job_id[0])
                                );
                            }

                            responses[0].push(Message::ReadResponse {
                                upstairs_id: *upstairs_id,
                                session_id: *session_id,
                                job_id: *job_id,
                                responses: Ok(vec![make_blank_read_response()]),
                            });

                            // At this point, the next operation is going to be
                            // a write.  This write will depend on the three
                            // reads that are already enqueued (but not the
                            // repair close, because the reads already
                            // implicitly depend on the repair close id).  We'll
                            // update our target dep_job_id to match this read.
                            dep_job_id[0] = *job_id;
                        }

                        _ => bail!("saw {:?}", m1),
                    }
                } else {
                    // All IO above this is skipped for the downstairs under
                    // repair.
                    bail_assert!(matches!(
                        ds1_messages.try_recv(),
                        Err(TryRecvError::Empty)
                    ));
                }

                let m2 = ds2_messages.recv().await.unwrap();
                let m3 = ds3_messages.recv().await.unwrap();

                match &m2 {
                    Message::ReadRequest {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            bail_assert!(dependencies.contains(&dep_job_id[1]));
                        }

                        responses[1].push(Message::ReadResponse {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            responses: Ok(vec![make_blank_read_response()]),
                        });
                        dep_job_id[1] = *job_id;
                    }

                    _ => bail!("saw {:?}", m2),
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
                            bail_assert!(dependencies.contains(&dep_job_id[2]));
                        }

                        responses[2].push(Message::ReadResponse {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            responses: Ok(vec![make_blank_read_response()]),
                        });
                        dep_job_id[2] = *job_id;
                    }

                    _ => bail!("saw {:?}", m3),
                }

                // write

                {
                    let harness = harness.clone();
                    tokio::spawn(async move {
                        let bytes = Bytes::from(vec![1u8; 512]);
                        harness
                            .guest
                            .write(Block::new_512(io_eid as u64 * 10), bytes)
                            .await
                            .unwrap();
                    })
                };

                if io_eid <= eid {
                    // IO at or below the extent under repair is sent to the
                    // downstairs under repair.
                    let m1 = ds1_messages.recv().await.unwrap();

                    match &m1 {
                        Message::Write {
                            upstairs_id,
                            session_id,
                            job_id,
                            dependencies,
                            ..
                        } => {
                            if io_eid == eid {
                                bail_assert!(
                                    dependencies.contains(&dep_job_id[0])
                                );
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

                        _ => bail!("saw {:?}", m1),
                    }
                } else {
                    // All IO above this is skipped for the downstairs under
                    // repair.
                    bail_assert!(matches!(
                        ds1_messages.try_recv(),
                        Err(TryRecvError::Empty)
                    ));
                }

                let m2 = ds2_messages.recv().await.unwrap();
                let m3 = ds3_messages.recv().await.unwrap();

                match &m2 {
                    Message::Write {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            bail_assert!(dependencies.contains(&dep_job_id[1]));
                        }

                        responses[1].push(Message::WriteAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        });
                        dep_job_id[1] = *job_id;
                    }

                    _ => bail!("saw {:?}", m2),
                }

                match &m3 {
                    Message::Write {
                        upstairs_id,
                        session_id,
                        job_id,
                        dependencies,
                        ..
                    } => {
                        if io_eid == eid {
                            bail_assert!(dependencies.contains(&dep_job_id[2]));
                        }

                        responses[2].push(Message::WriteAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        });
                        dep_job_id[2] = *job_id;
                    }

                    _ => bail!("saw {:?}", m3),
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
                    bail_assert!(*extent_id == eid);

                    // ds1 didn't get the flush, it was set to faulted
                    let gen = 1;
                    let flush = 0;
                    let dirty = false;

                    ds1.fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .await
                        .unwrap();
                }

                _ => bail!("saw {:?}", m1),
            }

            match &m2 {
                Message::ExtentLiveFlushClose {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    bail_assert!(*extent_id == eid);

                    // ds2 and ds3 did get a flush
                    let gen = 0;
                    let flush = 2;
                    let dirty = false;

                    harness
                        .ds2
                        .fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .await
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveFlushClose {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    bail_assert!(*extent_id == eid);

                    // ds2 and ds3 did get a flush
                    let gen = 0;
                    let flush = 2;
                    let dirty = false;

                    harness
                        .ds3
                        .fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveCloseAck {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok((gen, flush, dirty)),
                        })
                        .await
                        .unwrap()
                }

                _ => bail!("saw {:?}", m3),
            }

            // Based on those gen, flush, and dirty values, ds1 should get the
            // ExtentLiveRepair message, while ds2 and ds3 should get
            // ExtentLiveNoOp.

            let m1 = ds1_messages.recv().await.unwrap();
            let m2 = ds2_messages.recv().await.unwrap();
            let m3 = ds3_messages.recv().await.unwrap();

            match &m1 {
                Message::ExtentLiveRepair {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    source_client_id,
                    ..
                } => {
                    bail_assert!(*source_client_id != ClientId::new(0));
                    bail_assert!(*extent_id == eid);

                    ds1.fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveRepairAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .await
                        .unwrap();
                }

                _ => bail!("saw {:?}", m3),
            }

            match &m2 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds2
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds3
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            // Now, all downstairs will see ExtentLiveNoop

            let m1 = ds1_messages.recv().await.unwrap();
            let m2 = ds2_messages.recv().await.unwrap();
            let m3 = ds3_messages.recv().await.unwrap();

            match &m1 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => ds1
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m2 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds2
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveNoOp {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => harness
                    .ds3
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveAckId {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap(),

                _ => bail!("saw {:?}", m2),
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
                    bail_assert!(*extent_id == eid);

                    ds1.fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .await
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m2 {
                Message::ExtentLiveReopen {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    bail_assert!(*extent_id == eid);

                    harness
                        .ds2
                        .fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .await
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            match &m3 {
                Message::ExtentLiveReopen {
                    upstairs_id,
                    session_id,
                    job_id,
                    extent_id,
                    ..
                } => {
                    bail_assert!(*extent_id == eid);

                    harness
                        .ds3
                        .fw
                        .lock()
                        .await
                        .send(Message::ExtentLiveAckId {
                            upstairs_id: *upstairs_id,
                            session_id: *session_id,
                            job_id: *job_id,
                            result: Ok(()),
                        })
                        .await
                        .unwrap()
                }

                _ => bail!("saw {:?}", m2),
            }

            // After those are done, send out the read and write job responses
            for m in &responses[0] {
                ds1.fw.lock().await.send(m).await.unwrap();
            }
            for m in &responses[1] {
                harness.ds2.fw.lock().await.send(m).await.unwrap();
            }
            for m in &responses[2] {
                harness.ds3.fw.lock().await.send(m).await.unwrap();
            }
        }

        // Expect the live repair to send a final flush
        {
            let flush_job_id = match ds1_messages.recv().await.unwrap() {
                Message::Flush {
                    job_id,
                    flush_number: 12,
                    ..
                } => job_id,

                _ => bail!("saw non flush!"),
            };

            bail_assert!(matches!(
                ds2_messages.recv().await.unwrap(),
                Message::Flush {
                    flush_number: 12,
                    ..
                },
            ));

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::Flush {
                    flush_number: 12,
                    ..
                },
            ));

            ds1.fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: ds1.upstairs_session_id.lock().await.unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();
        }

        // Try another read
        {
            {
                let harness = harness.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let buffer = Buffer::new(512);
                    harness
                        .guest
                        .read(Block::new_512(0), buffer)
                        .await
                        .unwrap();
                });
            }

            // All downstairs should see it

            bail_assert!(matches!(
                ds1_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            bail_assert!(matches!(
                ds2_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));
        }

        Ok(())
    }

    /// Test that an error during the live repair doesn't halt indefinitely
    #[tokio::test]
    async fn test_error_during_live_repair_no_halt() -> Result<()> {
        let harness = Arc::new(TestHarness::new().await?);

        let (jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        // Send 200 more than IO_OUTSTANDING_MAX jobs. Flow control will kick in
        // at MAX_ACTIVE_COUNT messages, so we need to be sending read responses
        // while reads are being sent. After IO_OUTSTANDING_MAX jobs, the
        // Upstairs will set ds1 to faulted, and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let harness = harness.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let buffer = Buffer::new(512);
                    harness
                        .guest
                        .read(Block::new_512(0), buffer)
                        .await
                        .unwrap();
                });
            }

            if i < MAX_ACTIVE_COUNT {
                // Before flow control kicks in, assert we're seeing the read
                // requests
                bail_assert!(matches!(
                    ds1_messages.recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After flow control kicks in, we shouldn't see any more
                // messages
                match ds1_messages.try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match ds2_messages.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let harness = harness.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    harness.guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match ds2_messages.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate().take(MAX_ACTIVE_COUNT) {
            match harness
                .ds1()
                .await
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds1()
                        .await
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: *job_id,
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
            {
                Ok(()) => {
                    info!(
                        harness.log,
                        "sent read response for job {} = {}", i, job_id,
                    );
                }

                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = ds1_messages.try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        drop(ds1_messages);
        jh1.abort();

        let ds1 = harness.take_ds1().await;
        let ds1 = ds1.close();
        let ds1 = ds1.into_connected_downstairs().await;

        ds1.negotiate_start().await?;
        ds1.negotiate_step_extent_versions_please().await?;

        let (_jh1, mut ds1_messages) = ds1.spawn_message_receiver().await;

        // The Upstairs will start sending LiveRepair related work, which may be
        // out of order. Buffer some here.

        let mut ds1_buffered_messages = vec![];
        let mut ds2_buffered_messages = vec![];
        let mut ds3_buffered_messages = vec![];

        // EID 0

        // The Upstairs first sends the close and reopen jobs
        for _ in 0..2 {
            ds1_buffered_messages.push(ds1_messages.recv().await.unwrap());
            ds2_buffered_messages.push(ds2_messages.recv().await.unwrap());
            ds3_buffered_messages.push(ds3_messages.recv().await.unwrap());
        }

        bail_assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveClose { .. })));
        bail_assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));
        bail_assert!(ds3_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveFlushClose { .. })));

        bail_assert!(ds1_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        bail_assert!(ds2_buffered_messages
            .iter()
            .any(|m| matches!(m, Message::ExtentLiveReopen { .. })));
        bail_assert!(ds3_buffered_messages
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
                bail_assert!(*extent_id == 0);

                // ds1 didn't get the flush, it was set to faulted
                let gen = 1;
                let flush = 0;
                let dirty = false;

                ds1.fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .await
                    .unwrap();
            }

            _ => bail!("saw {:?}", m1),
        }

        match &m2 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                bail_assert!(*extent_id == 0);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds2
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .await
                    .unwrap()
            }

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                ..
            } => {
                bail_assert!(*extent_id == 0);

                // ds2 and ds3 did get a flush
                let gen = 0;
                let flush = 2;
                let dirty = false;

                harness
                    .ds3
                    .fw
                    .lock()
                    .await
                    .send(Message::ExtentLiveCloseAck {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        result: Ok((gen, flush, dirty)),
                    })
                    .await
                    .unwrap()
            }

            _ => bail!("saw {:?}", m3),
        }

        // Based on those gen, flush, and dirty values, ds1 should get the
        // ExtentLiveRepair message, while ds2 and ds3 should get
        // ExtentLiveNoOp.

        let m1 = ds1_messages.recv().await.unwrap();
        let m2 = ds2_messages.recv().await.unwrap();
        let m3 = ds3_messages.recv().await.unwrap();

        match &m1 {
            Message::ExtentLiveRepair {
                upstairs_id,
                session_id,
                job_id,
                extent_id,
                source_client_id,
                ..
            } => {
                bail_assert!(*source_client_id != ClientId::new(0));
                bail_assert!(*extent_id == 0);

                // send back error report here!
                ds1.fw
                    .lock()
                    .await
                    .send(Message::ErrorReport {
                        upstairs_id: *upstairs_id,
                        session_id: *session_id,
                        job_id: *job_id,
                        error: CrucibleError::GenericError(String::from(
                            "bad news, networks are tricky",
                        )),
                    })
                    .await
                    .unwrap();
            }

            _ => bail!("saw {:?}", m3),
        }

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        error!(harness.log, "dropping ds1 now!");

        // Now, all downstairs will see ExtentLiveNoop, except ds1, which will
        // abort itself due to an ErrorReport during an extent repair.
        drop(ds1_messages);
        jh1.abort();

        let ds1 = ds1.close();

        // If the Upstairs doesn't disconnect and try to reconnect to the
        // downstairs, this test will get stuck here, and will not progress to
        // the negotate_start function below.
        error!(harness.log, "reconnecting ds1 now!");
        let ds1 = ds1.into_connected_downstairs().await;

        error!(harness.log, "ds1 negotiate start now!");
        ds1.negotiate_start().await?;
        error!(harness.log, "ds1 negotiate extent versions please now!");
        ds1.negotiate_step_extent_versions_please().await?;

        error!(harness.log, "ds1 spawn message receiver now!");
        let (_jh1, mut ds1_messages) = ds1.spawn_message_receiver().await;

        // Continue faking for downstairs 2 and 3 - the work that was occuring
        // for extent 0 should finish before the Upstairs aborts the repair
        // task.

        let m2 = ds2_messages.recv().await.unwrap();
        let m3 = ds3_messages.recv().await.unwrap();

        match &m2 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
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
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        match &m3 {
            Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                job_id,
                ..
            } => harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ExtentLiveAckId {
                    upstairs_id: *upstairs_id,
                    session_id: *session_id,
                    job_id: *job_id,
                    result: Ok(()),
                })
                .await
                .unwrap(),

            _ => bail!("saw {:?}", m2),
        }

        // The Upstairs will abort the live repair task, and will send a final
        // flush to ds2 and ds3. The flush number will not be incremented as it
        // would have been for each extent.

        let flush_job_id = match ds2_messages.recv().await.unwrap() {
            Message::Flush {
                job_id,
                flush_number: 3,
                ..
            } => job_id,

            _ => bail!("saw non flush!"),
        };

        bail_assert!(matches!(
            ds3_messages.recv().await.unwrap(),
            Message::Flush {
                flush_number: 3,
                ..
            },
        ));

        harness
            .ds2
            .fw
            .lock()
            .await
            .send(Message::FlushAck {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness
                    .ds2
                    .upstairs_session_id
                    .lock()
                    .await
                    .unwrap(),
                job_id: flush_job_id,
                result: Ok(()),
            })
            .await
            .unwrap();

        harness
            .ds3
            .fw
            .lock()
            .await
            .send(Message::FlushAck {
                upstairs_id: harness.guest.get_uuid().await.unwrap(),
                session_id: harness
                    .ds3
                    .upstairs_session_id
                    .lock()
                    .await
                    .unwrap(),
                job_id: flush_job_id,
                result: Ok(()),
            })
            .await
            .unwrap();

        // After this, another repair task will start from the beginning, and
        // send a bunch of work to ds1 again.

        bail_assert!(matches!(
            ds1_messages.recv().await.unwrap(),
            Message::ExtentLiveClose { extent_id: 0, .. },
        ));

        bail_assert!(matches!(
            ds1_messages.recv().await.unwrap(),
            Message::ExtentLiveReopen { extent_id: 0, .. },
        ));

        Ok(())
    }

    /// Test that after giving up on a downstairs, setting it to faulted, and
    /// letting it reconnect, live repair does *not* occur if the upstairs is
    /// configured read-only.
    #[tokio::test]
    async fn test_no_read_only_live_repair() -> Result<()> {
        let harness = Arc::new(TestHarness::new_ro().await?);

        let (jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        // Send 200 more than IO_OUTSTANDING_MAX jobs. Flow control will kick in
        // at MAX_ACTIVE_COUNT messages, so we need to be sending read responses
        // while reads are being sent. After IO_OUTSTANDING_MAX jobs, the
        // Upstairs will set ds1 to faulted, and send it no more work.
        const NUM_JOBS: usize = IO_OUTSTANDING_MAX + 200;
        let mut job_ids = Vec::with_capacity(NUM_JOBS);

        for i in 0..NUM_JOBS {
            {
                let harness = harness.clone();

                // We must tokio::spawn here because `read` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    let buffer = Buffer::new(512);
                    harness
                        .guest
                        .read(Block::new_512(0), buffer)
                        .await
                        .unwrap();
                });
            }

            if i < MAX_ACTIVE_COUNT {
                // Before flow control kicks in, assert we're seeing the read
                // requests
                bail_assert!(matches!(
                    ds1_messages.recv().await.unwrap(),
                    Message::ReadRequest { .. },
                ));
            } else {
                // After flow control kicks in, we shouldn't see any more
                // messages
                match ds1_messages.try_recv() {
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                    x => {
                        info!(
                            harness.log,
                            "Read {i} should return EMPTY, but we got:{:?}", x
                        );

                        bail!(
                            "Read {i} should return EMPTY, but we got:{:?}",
                            x
                        );
                    }
                }
            }

            match ds2_messages.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => bail!("saw non read request!"),
            }

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            // Respond with read responses for downstairs 2 and 3
            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: job_ids[i],
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
                .unwrap();
        }

        // Confirm that's all the Upstairs sent us (only ds2 and ds3) - with the
        // flush_timeout set to 24 hours, we shouldn't see anything else
        bail_assert!(matches!(
            ds2_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));
        bail_assert!(matches!(
            ds3_messages.try_recv(),
            Err(TryRecvError::Empty)
        ));

        // Flush to clean out skipped jobs
        {
            let jh = {
                let harness = harness.clone();

                // We must tokio::spawn here because `flush` will wait for the
                // response to come back before returning
                tokio::spawn(async move {
                    harness.guest.flush(None).await.unwrap();
                })
            };

            let flush_job_id = match ds2_messages.recv().await.unwrap() {
                Message::Flush { job_id, .. } => job_id,

                _ => bail!("saw non flush ack!"),
            };

            bail_assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::Flush { .. },
            ));

            harness
                .ds2
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds2
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            harness
                .ds3
                .fw
                .lock()
                .await
                .send(Message::FlushAck {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds3
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: flush_job_id,
                    result: Ok(()),
                })
                .await
                .unwrap();

            // Wait for the flush to come back
            jh.await.unwrap();
        }

        // Send ds1 responses for the jobs it saw
        for (i, job_id) in job_ids.iter().enumerate().take(MAX_ACTIVE_COUNT) {
            match harness
                .ds1()
                .await
                .fw
                .lock()
                .await
                .send(Message::ReadResponse {
                    upstairs_id: harness.guest.get_uuid().await.unwrap(),
                    session_id: harness
                        .ds1()
                        .await
                        .upstairs_session_id
                        .lock()
                        .await
                        .unwrap(),
                    job_id: *job_id,
                    responses: Ok(vec![make_blank_read_response()]),
                })
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    // We should be able to send a few, but at some point
                    // the Upstairs will disconnect us.
                    error!(
                        harness.log,
                        "could not send read response for job {} = {}: {}",
                        i,
                        job_id,
                        e
                    );
                    break;
                }
            }
        }

        // Assert the Upstairs isn't sending ds1 more work, because it is
        // Faulted
        let v = ds1_messages.try_recv();
        match v {
            // We're either disconnected, or the queue is empty.
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                // This is expected, continue on
            }

            _ => {
                // Any other error (or success!) is unexpected
                bail!("try_recv returned {:?}", v);
            }
        }

        // Reconnect ds1
        drop(ds1_messages);
        jh1.abort();

        let ds1 = harness.take_ds1().await;
        let ds1 = ds1.close();
        let ds1 = ds1.into_connected_downstairs().await;

        ds1.negotiate_start().await?;
        ds1.negotiate_step_extent_versions_please().await?;

        let (_jh1, mut ds1_messages) = ds1.spawn_message_receiver().await;

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
        let harness = harness.clone();

        // We must tokio::spawn here because `read` will wait for the
        // response to come back before returning
        tokio::spawn(async move {
            let buffer = Buffer::new(512);
            harness.guest.read(Block::new_512(0), buffer).await.unwrap();
        });

        // All downstairs should see it
        bail_assert!(matches!(
            ds1_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));
        bail_assert!(matches!(
            ds2_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        bail_assert!(matches!(
            ds3_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        Ok(())
    }
}
