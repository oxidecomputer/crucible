// Copyright 2023 Oxide Computer Company

#[cfg(test)]
pub(crate) mod protocol_test {
    use core::fmt::Error;
    use core::fmt::Formatter;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::test::up_test::csl;
    use crate::up_main;
    use crate::BlockContext;
    use crate::BlockIO;
    use crate::Buffer;
    use crate::Guest;
    use crucible_client_types::CrucibleOpts;
    use crucible_common::Block;
    use crucible_common::RegionDefinition;
    use crucible_common::RegionOptions;
    use crucible_protocol::CrucibleDecoder;
    use crucible_protocol::CrucibleEncoder;

    use crucible_protocol::Message;
    use futures::SinkExt;
    use futures::StreamExt;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use tokio_util::codec::FramedRead;
    use tokio_util::codec::FramedWrite;

    use slog::info;
    use slog::o;
    use slog::Logger;

    use bytes::BytesMut;
    use uuid::Uuid;

    pub struct Downstairs {
        log: Logger,
        listener: TcpListener,
        local_addr: SocketAddr,
        _repair_listener: TcpListener,
        repair_addr: SocketAddr,
        uuid: Uuid,

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

                extent_count: 10,
                extent_size: Block::new_512(10),

                gen_numbers: vec![0u64; 10],
                flush_numbers: vec![0u64; 10],
                dirty_bits: vec![false; 10],
            }
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

        pub async fn negotiate_start(&self) {
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
                    read_only: _,
                    encrypted: _,
                    alternate_versions: _,
                } => {
                    info!(self.inner.log, "negotiate packet {:?}", packet);

                    self.fw
                        .lock()
                        .await
                        .send(Message::YesItsMe {
                            version: *version,
                            repair_addr: self.inner.repair_addr,
                        })
                        .await
                        .unwrap();
                }

                _ => panic!("wrong packet"),
            }

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
                    info!(self.inner.log, "negotiate packet {:?}", packet);

                    // Record the session id the upstairs sent us
                    *self.upstairs_session_id.lock().await = Some(*session_id);

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
                }

                _ => panic!("wrong packet"),
            }

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
                }

                _ => panic!("wrong packet"),
            }
        }

        pub async fn negotiate_step_extent_versions_please(&self) {
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

                _ => panic!("wrong packet"),
            }
        }

        pub async fn negotiate_step_last_flush(&self, last_flush_number: u64) {
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

                _ => panic!("wrong packet"),
            }
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
                    match fr.lock().await.next().await.transpose().unwrap() {
                        None => {
                            // disconnection, bail
                            return;
                        }

                        Some(Message::Ruok) => {
                            // Respond to pings right away
                            fw.lock().await.send(Message::Imok).await.unwrap();
                        }

                        Some(m) => {
                            info!(log, "received {:?}", m);
                            tx.send(m).await.unwrap();
                        }
                    }
                }
            });

            (jh, rx)
        }
    }

    pub struct TestHarness {
        _log: Logger,
        ds1: Mutex<Option<Arc<ConnectedDownstairs>>>,
        ds2: Arc<ConnectedDownstairs>,
        ds3: Arc<ConnectedDownstairs>,
        _join_handle: JoinHandle<()>,
        guest: Arc<Guest>,
    }

    impl TestHarness {
        pub async fn new() -> TestHarness {
            /* JAMES!
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default_panic(info);
                std::process::exit(1);
            }));
            */
            let log = csl();

            let ds1 = Downstairs::new(log.new(o!("downstairs" => 1))).await;
            let ds2 = Downstairs::new(log.new(o!("downstairs" => 2))).await;
            let ds3 = Downstairs::new(log.new(o!("downstairs" => 3))).await;

            let guest = Arc::new(Guest::new());

            let crucible_opts = CrucibleOpts {
                id: Uuid::new_v4(),
                target: vec![ds1.local_addr, ds2.local_addr, ds3.local_addr],
                flush_timeout: Some(600),

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

            let mut handles = vec![];

            {
                let guest = guest.clone();
                handles.push(tokio::spawn(async move {
                    guest.activate().await.unwrap();
                }));
            }
            {
                let ds1 = ds1.clone();
                handles.push(tokio::spawn(async move {
                    ds1.negotiate_start().await;
                    ds1.negotiate_step_extent_versions_please().await;
                }));
            }
            {
                let ds2 = ds2.clone();
                handles.push(tokio::spawn(async move {
                    ds2.negotiate_start().await;
                    ds2.negotiate_step_extent_versions_please().await;
                }));
            }
            {
                let ds3 = ds3.clone();
                handles.push(tokio::spawn(async move {
                    ds3.negotiate_start().await;
                    ds3.negotiate_step_extent_versions_please().await;
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            for _ in 0..10 {
                if guest.query_is_active().await.unwrap() {
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            assert!(guest.query_is_active().await.unwrap());

            TestHarness {
                _log: log,
                ds1: Mutex::new(Some(ds1)),
                ds2,
                ds3,
                _join_handle: join_handle,
                guest,
            }
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

    /// Test that flow control kicks in at 100 jobs, and until the downstairs
    /// responds Ok for a job, no more work is sent. Once three downstairs
    /// responds with a read response for a certain job, then more work is sent.
    #[tokio::test]
    async fn test_flow_control() {
        let harness = Arc::new(TestHarness::new().await);

        let (_jh1, mut ds1_messages) =
            harness.ds1().await.spawn_message_receiver().await;
        let (_jh2, mut ds2_messages) =
            harness.ds2.spawn_message_receiver().await;
        let (_jh3, mut ds3_messages) =
            harness.ds3.spawn_message_receiver().await;

        for _ in 0..100 {
            let harness = harness.clone();

            // We must tokio::spawn here because `read` will wait for the
            // response to come back before returning
            tokio::spawn(async move {
                let buffer = Buffer::new(512);
                harness.guest.read(Block::new_512(0), buffer).await.unwrap();
            });
        }

        let mut job_ids = Vec::with_capacity(100);

        for _ in 0..100 {
            match ds1_messages.recv().await.unwrap() {
                Message::ReadRequest { job_id, .. } => {
                    // Record the job ids of the read requests
                    job_ids.push(job_id);
                }

                _ => panic!("saw non read request!"),
            }

            assert!(matches!(
                ds2_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));

            assert!(matches!(
                ds3_messages.recv().await.unwrap(),
                Message::ReadRequest { .. },
            ));
        }

        // Confirm that's all the Upstairs sent us - with the flush_timeout set
        // to five minutes, we shouldn't see anything else

        assert!(matches!(ds1_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds2_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds3_messages.try_recv(), Err(TryRecvError::Empty)));

        // Performing any guest reads will not send them to the downstairs

        {
            let harness = harness.clone();

            tokio::spawn(async move {
                let buffer = Buffer::new(512);
                harness.guest.read(Block::new_512(0), buffer).await.unwrap();
            });
        }

        assert!(matches!(ds1_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds2_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds3_messages.try_recv(), Err(TryRecvError::Empty)));

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

        assert!(matches!(ds1_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds2_messages.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ds3_messages.try_recv(), Err(TryRecvError::Empty)));

        assert!(matches!(
            ds1_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));

        assert!(matches!(
            ds2_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));

        assert!(matches!(
            ds3_final_read_request.unwrap(),
            Message::ReadRequest { .. },
        ));
    }

    /// Test that replay occurs after a downstairs disconnects and reconnects
    #[tokio::test]
    async fn test_replay_occurs() {
        let harness = Arc::new(TestHarness::new().await);

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

        assert!(matches!(ds1_message, Message::ReadRequest { .. }));

        assert!(matches!(
            ds2_messages.recv().await.unwrap(),
            Message::ReadRequest { .. },
        ));

        assert!(matches!(
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

        ds1.negotiate_start().await;
        ds1.negotiate_step_last_flush(0).await;

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

        assert_eq!(ds1_message, ds1_message_second_time.unwrap());
    }
}
