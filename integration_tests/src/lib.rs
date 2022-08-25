// Copyright 2022 Oxide Computer Company

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::sync::Arc;

    use anyhow::*;
    use base64::encode;
    use crucible::{Bytes, *};
    use crucible_client_types::VolumeConstructionRequest;
    use crucible_downstairs::*;
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
                5, /* extent_size */
                2, /* extent_count */
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
    async fn integration_test_region() -> Result<()> {
        // Test a simple single layer volume with a read, write, read
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
        let opts = three_downstairs(54004, 54005, 54006, false).unwrap();
        integration_test_two_layers_common(opts, false).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_two_layers_write_unwritten() -> Result<()> {
        let opts = three_downstairs(54007, 54008, 54009, false).unwrap();
        integration_test_two_layers_common(opts, true).await
    }

    async fn integration_test_two_layers_common(
        opts: CrucibleOpts,
        is_write_unwritten: bool,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 512;
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
        if is_write_unwritten {
            volume
                .write_unwritten(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![55; BLOCK_SIZE * 10]),
                )?
                .block_wait()?;
        } else {
            volume
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![55; BLOCK_SIZE * 10]),
                )?
                .block_wait()?;
        }

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

        let opts = three_downstairs(54010, 54011, 54012, false).unwrap();

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

        let opts = three_downstairs(54013, 54014, 54015, false).unwrap();

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
    async fn integration_test_just_read() -> Result<()> {
        // Just do a read of a new volume.
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54016, 54017, 54018, true).unwrap();

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
    async fn integration_test_volume_write_unwritten_1() -> Result<()> {
        // Test a simple single layer volume, verify write_unwritten
        // works as expected.
        // Volume with a subvolume:
        // |----------|
        //
        // Write A:
        // |AAAAAAAAAA|
        // Write unwritten B:
        // |BBBBBBBBBB|
        //
        // Should result in:
        // |AAAAAAAAAA|
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54019, 54020, 54021, false).unwrap();

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

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read volume, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write_unwritten data in, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read volume, verify original contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_write_unwritten_2() -> Result<()> {
        // Test a simple single layer volume, verify a first write_unwritten
        // won't be altered by a 2nd write_unwritten.
        // Volume with a subvolume:
        // |----------|
        //
        // Write unwritten A:
        // |AAAAAAAAAA|
        // Write unwritten B:
        // |BBBBBBBBBB|
        //
        // Should result in:
        // |AAAAAAAAAA|
        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54022, 54023, 54024, false).unwrap();

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

        // Write data in
        volume
            .write_unwritten(
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

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read volume, verify original contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_write_unwritten_sparse() -> Result<()> {
        // Test a simple single layer volume
        // Perform a smaller write, then a larger write_unwritten and
        // verify the smaller write is not over-written.
        // Volume with a subvolume:
        // |----------|
        //
        // Write A:
        // |A---------|
        // Write unwritten B:
        // |BBBBBBBBBB|
        //
        // Should result in:
        // |ABBBBBBBBBB|
        const BLOCK_SIZE: usize = 512;

        let opts = three_downstairs(54025, 54026, 54027, false).unwrap();

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

        // Write data at block 0
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x33; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // A second Write_unwritten that overlaps the original write.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read and verify
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first block is from the first write
        assert_eq!(vec![0x33_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify the remaining blocks have the write_unwritten data
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE * 9],
            dl[BLOCK_SIZE..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_write_unwritten_subvols() -> Result<()> {
        // Test a single layer volume with two subvolumes,
        // verify a first write_unwritten that crosses the subvols
        // works as expected.
        //
        // Volume with two subvolumes:
        // |----------||----------|
        //
        // Write unwritten:
        // |AAAAAAAAAA||AAAAAAAAAA|
        //
        // Should result in:
        // |AAAAAAAAAA||AAAAAAAAAA|
        const BLOCK_SIZE: usize = 512;

        let mut sv = Vec::new();
        let opts = three_downstairs(54028, 54029, 54030, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let opts = three_downstairs(54031, 54032, 54033, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: sv,
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume = Arc::new(tokio::task::block_in_place(|| {
            Volume::construct(vcr, None)
        })?);

        volume.activate(0)?;
        let full_volume_size = BLOCK_SIZE * 20;
        // Write data in
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; full_volume_size]),
            )?
            .block_wait()?;

        // Read parent, verify contents
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; full_volume_size], *buffer.as_vec());

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; full_volume_size]),
            )?
            .block_wait()?;

        // Read volume, verify original contents
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; full_volume_size], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_write_unwritten_subvols_sparse(
    ) -> Result<()> {
        // Test a single layer volume with two subvolumes,
        // verify a first write_unwritten that crosses the subvols
        // works as expected.
        //
        // Two subvolumes:
        // |----------||----------|
        // Write unwritten A:
        // |---------A||A---------|
        // Write unwritten B:
        // |BBBBBBBBBB||BBBBBBBBBB|
        //
        // Should result in:
        // |BBBBBBBBBA||ABBBBBBBBB|
        const BLOCK_SIZE: usize = 512;

        let mut sv = Vec::new();
        let opts = three_downstairs(54034, 54035, 54036, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let opts = three_downstairs(54037, 54038, 54039, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: sv,
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume = Arc::new(tokio::task::block_in_place(|| {
            Volume::construct(vcr, None)
        })?);

        volume.activate(0)?;
        let full_volume_size = BLOCK_SIZE * 20;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 2], *buffer.as_vec());

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; full_volume_size]),
            )?
            .block_wait()?;

        // Read full volume, verify first write_unwritten still valid, but the
        // other blocks of the 2nd write_unwritten are updated.
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in blocks 0-9 is the second write_unwritten
        assert_eq!(vec![0x22_u8; BLOCK_SIZE * 9], dl[0..(BLOCK_SIZE * 9)]);

        // Verify data in blocks 10-11 is the first write_unwritten
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE * 2],
            dl[(BLOCK_SIZE * 9)..(BLOCK_SIZE * 11)]
        );

        // Verify the remaining blocks have the second write_unwritten data
        assert_eq!(
            vec![0x22_u8; BLOCK_SIZE * 9],
            dl[(BLOCK_SIZE * 11)..full_volume_size]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_write_unwritten_subvols_3() -> Result<()> {
        // Test a single layer volume with two subvolumes,
        // A first write_unwritten that crosses the subvols
        // A 2nd write unwritten.
        // A 3rd regular write
        //
        // Two subvolumes:
        // |----------||----------|
        // Write unwritten A:
        // |---------A||A---------|
        // Write unwritten B:
        // |BBBBBBBBBB||----------|
        // Write C:
        // |-------CCC||CCCCCCCCCC|
        //
        // Should result in:
        // |BBBBBBBCCC||CCCCCCCCCC|
        const BLOCK_SIZE: usize = 512;

        let mut sv = Vec::new();
        let opts = three_downstairs(54040, 54041, 54042, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let opts = three_downstairs(54043, 54044, 54045, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: sv,
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume = Arc::new(tokio::task::block_in_place(|| {
            Volume::construct(vcr, None)
        })?);

        volume.activate(0)?;
        let full_volume_size = BLOCK_SIZE * 20;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // A write
        volume
            .write(
                Block::new(7, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x11; BLOCK_SIZE * 13]),
            )?
            .block_wait()?;

        // Read full volume
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in blocks 0-7 is the second write_unwritten
        assert_eq!(vec![0x22_u8; BLOCK_SIZE * 7], dl[0..(BLOCK_SIZE * 7)]);

        // Verify data in blocks 8-19 is the third write
        assert_eq!(
            vec![0x11_u8; BLOCK_SIZE * 13],
            dl[(BLOCK_SIZE * 7)..full_volume_size]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_two_layers_parent_smaller() -> Result<()> {
        let opts = three_downstairs(54046, 54047, 54048, false).unwrap();
        integration_test_two_layers_small_common(opts, false).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_two_layers_parent_smaller_unwritten() -> Result<()>
    {
        let opts = three_downstairs(54049, 54050, 54051, false).unwrap();
        integration_test_two_layers_small_common(opts, true).await
    }

    async fn integration_test_two_layers_small_common(
        opts: CrucibleOpts,
        is_write_unwritten: bool,
    ) -> Result<()> {
        // Test a RO parent that is smaller than the SubVolume.

        const BLOCK_SIZE: usize = 512;
        // Create in_memory block_io
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 5,
        ));

        // Fill the in_memory block_io with 1s
        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 5]),
            )?
            .block_wait()?;

        // Read back in_memory, verify 1s
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], *buffer.as_vec());

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // Verify parent contents in one read
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![11; BLOCK_SIZE * 5];
        expected.extend(vec![0x00; BLOCK_SIZE * 5]);
        assert_eq!(expected, *buffer.as_vec());

        // One big write!
        let write_offset = Block::new(0, BLOCK_SIZE.trailing_zeros());
        let write_data = Bytes::from(vec![55; BLOCK_SIZE * 10]);
        if is_write_unwritten {
            volume.write(write_offset, write_data)?.block_wait()?;
        } else {
            volume
                .write_unwritten(write_offset, write_data)?
                .block_wait()?;
        }

        // Verify volume contents in one read
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_scrub() -> Result<()> {
        // Volume with a subvolume and a RO parent:
        // SV: |----------|
        // RO: |1111111111|
        //
        // Read volume, expect 1's
        // Run scrubber on volume.
        // Write unwritten 5's:
        //     |5555555555|
        //
        // Because the scrubber, we should expect the original 1's:
        //     |1111111111|

        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54052, 54053, 54054, false).unwrap();

        // Create in_memory block_io
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        // Fill in_memory (which will become RO parent)
        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // Verify contents are 11 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub().unwrap();

        // Now, try a write_unwritten, this should not change our
        // data as the scrubber has finished.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_scrub_short() -> Result<()> {
        // Volume with a subvolume and a smaller RO parent:
        // SV: |----------|
        // RO: |11111|
        //
        // Read volume, expect 1's
        // Run scrubber on volume.
        // Write unwritten 5's:
        //     |5555555555|
        //
        // Because the scrubber, we should expect the original 1's
        // for the start, then 5's at the end
        //     |1111155555|

        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54055, 54056, 54057, false).unwrap();

        // Create in_memory block_io
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 5,
        ));

        // Fill in_memory (which will become RO parent)
        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 5]),
            )?
            .block_wait()?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // Verify contents of RO parent are 1s at startup
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], *buffer.as_vec());

        // Verify contents of blocks 5-10 are zero.
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        volume
            .read(Block::new(5, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![00; BLOCK_SIZE * 5], *buffer.as_vec());

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub().unwrap();

        // Now, try a write_unwritten, this should not change our
        // unwritten data as the scrubber has finished.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first half is from the RO parent
        assert_eq!(vec![11; BLOCK_SIZE * 5], dl[0..BLOCK_SIZE * 5]);
        // Verify data in the second half is from the write unwritten
        assert_eq!(
            vec![55; BLOCK_SIZE * 5],
            dl[BLOCK_SIZE * 5..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_scrub_short_sparse() -> Result<()> {
        // Volume with a subvolume and a smaller RO parent:
        // SV: |----------|
        // RO: |11111|
        //
        // Read first half of volume, expect 1's
        // Write to lower half of volume:
        // SV: |--2-------|
        //
        // Write to upper half of volume:
        // SV: |-------3--|
        //
        // Run scrubber on volume.
        //
        // Because the scrubber, we should expect the original 1's
        // where unwritten, and new data where writes came before scrubber.
        //     |1121100300|

        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54058, 54059, 54060, false).unwrap();

        // Create in_memory block_io
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 5,
        ));

        // Fill in_memory (which will become RO parent)
        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 5]),
            )?
            .block_wait()?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // SV: |--2-------|
        volume
            .write(
                Block::new(2, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // SV: |-------3--|
        volume
            .write(
                Block::new(7, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume except where new writes have
        // landed
        volume.scrub().unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // First blocks are 1s   |11--------|
        let mut expected = vec![11; BLOCK_SIZE * 2];
        // Our write of 2        |112-------|
        expected.extend(vec![22; BLOCK_SIZE]);
        // Two more blocks of 2  |11211-----|
        expected.extend(vec![11; BLOCK_SIZE * 2]);
        // Two blocks of 0s      |1121100---|
        expected.extend(vec![0; BLOCK_SIZE * 2]);
        // Our write of 3        |11211003--|
        expected.extend(vec![33; BLOCK_SIZE]);
        // Two final blocks of 0 |1121100300|
        expected.extend(vec![0; BLOCK_SIZE * 2]);
        assert_eq!(expected, *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_scrub_useless() -> Result<()> {
        // Volume with a subvolume and a RO parent:
        // SV: |----------|
        // RO: |1111111111|
        //
        // Read volume, expect 1's
        // Write to entire volume.
        // SV  |5555555555|
        //
        // Run scrubber on volume.
        //
        // Scrubber should do nothing:
        // SV  |5555555555|

        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54061, 54062, 54063, false).unwrap();

        // Create in_memory block_io
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        // Fill in_memory (which will become RO parent)
        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;
        volume.add_read_only_parent(in_memory_data.clone())?;

        volume.activate(0)?;

        // Verify contents are 11 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write to the whole volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Call the scrubber.  This should do nothing
        volume.scrub().unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_subvols_parent_scrub_sparse() -> Result<()>
    {
        // Test a volume with two sub volumes, and RO parent
        // verify two writes, one that crosses the subvolume boundary
        // are persisted and data from the RO parent fills in
        // any holes.
        //
        // Two subvolumes:
        //     |----------||----------|
        // RO: |1111111111|
        //
        // Write A:
        //     |---------2||2---------|
        // Write B:
        //     |33--------||----------|
        //
        //  Scrub
        //  Read should result in
        //     |3311111112||2000000000|
        const BLOCK_SIZE: usize = 512;

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

        let mut sv = Vec::new();
        let opts = three_downstairs(54064, 54065, 54066, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let opts = three_downstairs(54067, 54068, 54069, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: sv,
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

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], *buffer.as_vec());

        // A second write
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Call the scrubber
        volume.scrub().unwrap();

        // Read full volume
        let buffer = Buffer::new(BLOCK_SIZE * 20);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Build the expected vec to compare our read with.
        // First two blocks are 3s     |33--------||----------|
        let mut expected = vec![33; BLOCK_SIZE * 2];
        // Original 1s from RO parent  |331111111-||----------|
        expected.extend(vec![11; BLOCK_SIZE * 7]);
        // Two blocks of 2             |3311111112||2---------|
        expected.extend(vec![22; BLOCK_SIZE * 2]);
        // remaining final blocks of 0 |3311111112||2000000000|
        expected.extend(vec![0; BLOCK_SIZE * 9]);
        assert_eq!(expected, *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_volume_subvols_parent_scrub_sparse_2(
    ) -> Result<()> {
        // Test a volume with two sub volumes, and 3/4th RO parent
        // Write a few spots, one spanning the sub vols.
        // Verify scrubber and everything works as expected.
        //
        // Two subvolumes:
        //     |----------||----------|
        // RO: |1111111111  11111|
        //
        // Write A:
        //     |---------2||2---------|
        // Write B:
        //     |33--------||----------|
        // Write C:
        //     |----------||----44----|
        //
        //  Scrub
        //  Read should result in
        //     |3311111112||2111440000|
        const BLOCK_SIZE: usize = 512;

        // Create in memory block io full of 11
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 15,
        ));

        in_memory_data
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![11; BLOCK_SIZE * 15]),
            )?
            .block_wait()?;

        let mut sv = Vec::new();
        let opts = three_downstairs(54070, 54071, 54072, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let opts = three_downstairs(54073, 54074, 54075, false).unwrap();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: sv,
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

        // Write data to last block of first vol, and first block of
        // second vol, AKA write A.
        //     |---------2||2---------|
        volume
            .write(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], *buffer.as_vec());

        // Write B
        //     |33--------||----------|
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Write C
        //     |----------||----44----|
        volume
            .write(
                Block::new(14, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![44; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Call the scrubber
        volume.scrub().unwrap();

        // Read full volume
        let buffer = Buffer::new(BLOCK_SIZE * 20);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Build the expected vec to compare our read with.
        // First two blocks are 3s     |33--------||----------|
        let mut expected = vec![33; BLOCK_SIZE * 2];
        // Original 1s from RO parent  |331111111-||----------|
        expected.extend(vec![11; BLOCK_SIZE * 7]);
        // Two blocks of 2             |3311111112||2---------|
        expected.extend(vec![22; BLOCK_SIZE * 2]);
        // More 1s from RO parent      |3311111112||2111000000|
        expected.extend(vec![11; BLOCK_SIZE * 3]);
        // Two blocks of 4             |3311111112||211144----|
        expected.extend(vec![44; BLOCK_SIZE * 2]);
        // remaining final blocks of 0 |3311111112||2111440000|
        expected.extend(vec![0; BLOCK_SIZE * 4]);
        assert_eq!(expected, *buffer.as_vec());

        Ok(())
    }

    // Test that multiple upstairs can connect to a single read-only downstairs
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_multi_read_only() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let mut opts = three_downstairs(54076, 54077, 54078, true).unwrap();

        let vcr_1: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: BLOCK_SIZE as u64,
                        opts: opts.clone(),
                        gen: 0,
                    },
                )),
            };

        // Second volume should have a unique UUID
        opts.id = Uuid::new_v4();

        let vcr_2: VolumeConstructionRequest =
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

        let volume1 =
            tokio::task::block_in_place(|| Volume::construct(vcr_1, None))?;
        volume1.activate(0)?;

        let volume2 =
            tokio::task::block_in_place(|| Volume::construct(vcr_2, None))?;
        volume2.activate(0)?;

        // Read one block: should be all 0x00
        let buffer = Buffer::new(BLOCK_SIZE);
        tokio::task::block_in_place(|| {
            volume1.read(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                buffer.clone(),
            )
        })?
        .block_wait()?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec());

        let buffer = Buffer::new(BLOCK_SIZE);
        tokio::task::block_in_place(|| {
            volume2.read(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                buffer.clone(),
            )
        })?
        .block_wait()?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_scrub_no_rop() -> Result<()> {
        // Volume with a subvolume and no RO parent:, verify the scrub
        // does no work
        // SV: |----------|
        //
        // Read volume, expect 0's
        // Write to half volume.
        // SV  |55555-----|
        //
        // Run scrubber on volume. (Should do nothing)
        //
        // Read volume again, no change
        // SV  |55555-----|

        const BLOCK_SIZE: usize = 512;
        let opts = three_downstairs(54079, 54080, 54081, false).unwrap();

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume_create_guest(opts, 0, None)?;

        volume.activate(0)?;

        // Verify contents are 00 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write to half volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 5]),
            )?
            .block_wait()?;

        // Call the scrubber.  This should do nothing
        volume.scrub().unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Build the expected vec to compare our read with.
        // First 5 blocks are 5s              |55555-----|
        let mut expected = vec![55; BLOCK_SIZE * 5];
        // Original 0s from unwritten blocks  |5555500000|
        expected.extend(vec![0; BLOCK_SIZE * 5]);
        assert_eq!(expected, *buffer.as_vec());

        Ok(())
    }

    // The following tests work at the "guest" layer.  The volume
    // layers above (in general) will eventually call a BlockIO trait
    // on a guest layer.  Port numbers from this point below should
    // start at 55001 and go up from there.

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs() -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write and read work as expected
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55016, 55017, 55018, false).unwrap();

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
        let opts = three_downstairs(55019, 55020, 55021, true).unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten() -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected
        // The size here spans two extents.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55022, 55023, 55024, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in
        guest
            .write_unwritten(
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

        // Write_unwritten again with different data
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read back the same blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        // Now, just write.  This should update our data.
        guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x89; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read back the same blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Verify data is now from the new write.
        assert_eq!(vec![0x89_u8; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten_sparse_1() -> Result<()>
    {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55025, 55026, 55027, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in the first block
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Write_unwritten again with different data and same start
        // range, but write to blocks 2 and 3 this time as well.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )?
            .block_wait()?;

        // Read back the first block.
        let buffer = Buffer::new(BLOCK_SIZE);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], *buffer.as_vec());

        // Read back the next two blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(1, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x99_u8; BLOCK_SIZE * 2], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten_sparse_mid(
    ) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes where the middle block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55028, 55029, 55030, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in the second block
        guest
            .write_unwritten(
                Block::new(1, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )?
            .block_wait()?;

        // Read back the all three blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 3);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first block is from the second write_unwritten
        assert_eq!(vec![0x99_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify data in the second block is from the first write_unwritten
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], dl[BLOCK_SIZE..(BLOCK_SIZE * 2)]);

        // Verify data in the third block is from the second write_unwritten
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE * 2)..(BLOCK_SIZE * 2 + BLOCK_SIZE)]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten_sparse_end(
    ) -> Result<()> {
        // Test write_unwritten and read work as expected,
        // this time with sparse writes where the last block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55031, 55032, 55033, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in the third block
        guest
            .write_unwritten(
                Block::new(2, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 1]),
            )?
            .block_wait()?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )?
            .block_wait()?;

        // Read back the all three blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 3);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first two blocks is the data from the
        // second write_unwritten
        assert_eq!(vec![0x99_u8; BLOCK_SIZE * 2], dl[0..BLOCK_SIZE * 2]);

        // Verify data in the third block is from the first write_unwritten
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE * 2)..(BLOCK_SIZE * 2 + BLOCK_SIZE)]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten_span() -> Result<()> {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55034, 55035, 55036, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Read back both blocks
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(4, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_guest_downstairs_unwritten_span_2() -> Result<()>
    {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let opts = three_downstairs(55037, 55038, 55039, false).unwrap();

        let guest = Arc::new(Guest::new());
        let gc = guest.clone();

        tokio::spawn(async move {
            up_main(opts, 0, gc, None).await.unwrap();
        });

        guest.activate(1)?;
        guest.query_work_queue()?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )?
            .block_wait()?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 2]),
            )?
            .block_wait()?;

        // Read back both blocks
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(4, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().to_vec();

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }
}
