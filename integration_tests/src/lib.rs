// Copyright 2022 Oxide Computer Company

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::sync::Arc;

    use anyhow::*;
    use base64::encode;
    use crucible::{Bytes, *};
    use crucible_downstairs::*;
    use crucible_client_types::VolumeConstructionRequest;
    use futures::lock::Mutex;
    use httptest::{matchers::*, responders::*, Expectation, Server};
    use rand::Rng;
    use tempfile::*;
    use uuid::*;

    #[allow(dead_code)]
    struct TestDownstairs {
        tempdir: TempDir,
        downstairs: Arc<Mutex<Downstairs>>,
    }

    impl TestDownstairs {
        pub fn new(
            address: IpAddr,
            port: u16,
            encrypted: bool,
            read_only: bool,
        ) -> Result<Self> {
            let tempdir = tempdir()?;

            let _region = create_region(
                512, /* block_size */
                tempdir.path().to_path_buf(),
                10, /* extent_size */
                1,  /* extent_count */
                Uuid::new_v4(),
                encrypted,
            )?;

            let downstairs = build_downstairs_for_region(
                &tempdir.path(),
                false, /* lossy */
                false, /* return_errors */
                read_only,
            )?;

            let adownstairs = downstairs.clone();
            tokio::spawn(async move {
                start_downstairs(
                    adownstairs,
                    address,
                    None, /* oximeter */
                    port,
                    None, /* cert_pem */
                    None, /* key_pem */
                    None, /* root_cert_pem */
                )
                .await
            });

            Ok(TestDownstairs {
                tempdir,
                downstairs,
            })
        }
    }

    // Spin off three downstairs at the given ports.
    // Return a Crucible Opts struct pre-populated with the same
    // three given ports for targets.
    fn three_downstairs(
        port1: u16,
        port2: u16,
        port3: u16,
        read_only: bool,
    ) -> Result<CrucibleOpts> {
        let _downstairs1 =
            TestDownstairs::new("127.0.0.1".parse()?, port1, true, read_only)?;
        let _downstairs2 =
            TestDownstairs::new("127.0.0.1".parse()?, port2, true, read_only)?;
        let _downstairs3 =
            TestDownstairs::new("127.0.0.1".parse()?, port3, true, read_only)?;

        // Generate random data for our key
        let key_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let key_string = encode(&key_bytes);

        let co = CrucibleOpts {
            id: Uuid::new_v4(),
            target: vec![
                format!("127.0.0.1:{}", port1).parse()?,
                format!("127.0.0.1:{}", port2).parse()?,
                format!("127.0.0.1:{}", port3).parse()?,
            ],
            lossy: false,
            flush_timeout: None,
            key: Some(key_string),
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only,
        };
        Ok(co)
    }

    // Note the port number for downstairs in each test must be unique
    // from both the other downstairs in the same test, AND with other
    // downstairs in other tests.  The helpful three_downstairs()
    // function should help to make this easier.

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_downstairs() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54001, 54002, 54003, false).unwrap();

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    opts,
                    gen: 0,
                }],
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume = Arc::new(tokio::task::block_in_place(|| {
            Volume::construct(vcr, None)
        })?);

        volume.activate(0)?;

        // Verify contents are zero on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_two_layers() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54004, 54005, 54006, false).unwrap();

        // Create in memory block io full of 11
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // Verify contents are 11 on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Verify parent wasn't written to
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_three_layers() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54007, 54008, 54009, false).unwrap();

        // Create in memory block io full of 11
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Create volume with read only parent
        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    opts,
                    gen: 0,
                }],
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let mut volume =
            tokio::task::block_in_place(|| Volume::construct(vcr, None))?;

        volume.add_read_only_parent({
            let mut volume = Volume::new(BLOCK_SIZE as u64);
            volume.add_subvolume(in_memory_data.clone())?;
            Arc::new(volume)
        })?;

        volume.activate(0)?;

        // Verify contents are 11 on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Verify parent wasn't written to
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_url() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54010, 54011, 54012, false).unwrap();

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/ff.raw"))
                .times(1..)
                .respond_with(status_code(200).body(vec![0xff; BLOCK_SIZE])),
        );
        server.expect(
            Expectation::matching(request::method_path("HEAD", "/ff.raw"))
                .times(1..)
                .respond_with(status_code(200).append_header(
                    "Content-Length",
                    format!("{}", BLOCK_SIZE),
                )),
        );

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    opts,
                    gen: 0,
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Volume {
                        id: Uuid::new_v4(),
                        block_size: BLOCK_SIZE as u64,
                        sub_volumes: vec![VolumeConstructionRequest::Url {
                            id: Uuid::new_v4(),
                            block_size: BLOCK_SIZE as u64,
                            url: server.url("/ff.raw").to_string(),
                        }],
                        read_only_parent: None,
                    },
                )),
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume =
            tokio::task::block_in_place(|| Volume::construct(vcr, None))?;
        volume.activate(0)?;

        // Read one block: should be all 0xff
        let buffer = Buffer::new(BLOCK_SIZE);
        tokio::task::block_in_place(|| {
            volume.read(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                buffer.clone(),
            )
        })?
        .block_wait()?;

        assert_eq!(vec![0xff; BLOCK_SIZE], *buffer.as_vec());

        // Write one block full of 0x01
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x01; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Read one block: should be all 0x01
        let buffer = Buffer::new(BLOCK_SIZE);
        tokio::task::block_in_place(|| {
            volume.read(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                buffer.clone(),
            )
        })?
        .block_wait()?;

        assert_eq!(vec![0x01; BLOCK_SIZE], *buffer.as_vec());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_read_only() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54013, 54014, 54015, true).unwrap();

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: BLOCK_SIZE as u64,
                        opts,
                        gen: 0,
                    },
                )),
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume =
            tokio::task::block_in_place(|| Volume::construct(vcr, None))?;
        volume.activate(0)?;

        // Read one block: should be all 0x00
        let buffer = Buffer::new(BLOCK_SIZE);
        tokio::task::block_in_place(|| {
            volume.read(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                buffer.clone(),
            )
        })?
        .block_wait()?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs() -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write and read work as expected
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(54016, 54017, 54018, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(0)?;
        guest.query_work_queue()?;

        // Verify contents are zero on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write data in
        guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_upstairs_read_only_rejects_write() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin up three read-only downstairs
        let opts = three_downstairs(54019, 54020, 54021, true).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        // Read-only Upstairs should return errors if writes are attempted.
        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(0)?;

        // Expect an error attempting to write.
        let write_result = guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )?
            .block_wait();
        assert!(write_result.is_err());
        assert!(matches!(
            write_result.err().unwrap(),
            CrucibleError::ModifyingReadOnlyRegion
        ));

        Ok(())
    }
}
