// Copyright 2022 Oxide Computer Company

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::net::SocketAddr;
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
        address: IpAddr,
        tempdir: TempDir,
        downstairs: Arc<Mutex<Downstairs>>,
    }

    impl TestDownstairs {
        pub async fn new(
            address: IpAddr,
            encrypted: bool,
            read_only: bool,
        ) -> Result<Self> {
            let tempdir = tempfile::Builder::new()
                .prefix(&"downstairs-")
                .rand_bytes(8)
                .tempdir()?;

            let _region = create_region(
                512, /* block_size */
                tempdir.path().to_path_buf(),
                5, /* extent_size */
                2, /* extent_count */
                Uuid::new_v4(),
                encrypted,
            )?;

            let downstairs = build_downstairs_for_region(
                tempdir.path(),
                false, /* lossy */
                false, /* return_errors */
                read_only,
            )?;

            let _join_handle = start_downstairs(
                downstairs.clone(),
                address,
                None, /* oximeter */
                0,    /* any port */
                0,    /* any rport */
                None, /* cert_pem */
                None, /* key_pem */
                None, /* root_cert_pem */
            )
            .await?;

            Ok(TestDownstairs {
                address,
                tempdir,
                downstairs,
            })
        }

        pub async fn reboot_read_only(&mut self) -> Result<()> {
            self.downstairs = build_downstairs_for_region(
                self.tempdir.path(),
                false, /* lossy */
                false, /* return_errors */
                true,
            )?;

            let _join_handle = start_downstairs(
                self.downstairs.clone(),
                self.address,
                None, /* oximeter */
                0,    /* any port */
                0,    /* any rport */
                None, /* cert_pem */
                None, /* key_pem */
                None, /* root_cert_pem */
            )
            .await?;

            Ok(())
        }

        pub async fn address(&self) -> SocketAddr {
            // If start_downstairs returned Ok, then address will be populated
            self.downstairs.lock().await.address.unwrap()
        }
    }

    struct TestDownstairsSet {
        downstairs1: TestDownstairs,
        downstairs2: TestDownstairs,
        downstairs3: TestDownstairs,
        crucible_opts: CrucibleOpts,
    }

    impl TestDownstairsSet {
        /// Spin off three downstairs
        pub async fn new(read_only: bool) -> Result<TestDownstairsSet> {
            let downstairs1 =
                TestDownstairs::new("127.0.0.1".parse()?, true, read_only)
                    .await?;
            let downstairs2 =
                TestDownstairs::new("127.0.0.1".parse()?, true, read_only)
                    .await?;
            let downstairs3 =
                TestDownstairs::new("127.0.0.1".parse()?, true, read_only)
                    .await?;

            // Generate random data for our key
            let key_bytes = rand::thread_rng().gen::<[u8; 32]>();
            let key_string = encode(&key_bytes);

            let crucible_opts = CrucibleOpts {
                id: Uuid::new_v4(),
                target: vec![
                    downstairs1.address().await,
                    downstairs2.address().await,
                    downstairs3.address().await,
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

            Ok(TestDownstairsSet {
                downstairs1,
                downstairs2,
                downstairs3,
                crucible_opts,
            })
        }

        // Return a Crucible Opts struct pre-populated with the same
        // three given ports for targets.
        pub fn opts(&self) -> CrucibleOpts {
            self.crucible_opts.clone()
        }

        pub async fn reboot_read_only(&mut self) -> Result<()> {
            self.downstairs1.reboot_read_only().await?;
            self.downstairs2.reboot_read_only().await?;
            self.downstairs3.reboot_read_only().await?;

            self.crucible_opts.read_only = true;
            self.crucible_opts.target = vec![
                self.downstairs1.address().await,
                self.downstairs2.address().await,
                self.downstairs3.address().await,
            ];

            Ok(())
        }
    }

    #[tokio::test]
    async fn integration_test_region() -> Result<()> {
        // Test a simple single layer volume with a read, write, read
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;

        // Verify contents are zero on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_two_layers() -> Result<()> {
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();
        integration_test_two_layers_common(opts, false).await
    }

    #[tokio::test]
    async fn integration_test_two_layers_write_unwritten() -> Result<()> {
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();
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
            )
            .await?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data.clone()).await?;

        volume.activate(0).await?;

        // Verify contents are 11 on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write data in
        if is_write_unwritten {
            volume
                .write_unwritten(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![55; BLOCK_SIZE * 10]),
                )
                .await?;
        } else {
            volume
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![55; BLOCK_SIZE * 10]),
                )
                .await?;
        }

        // Verify parent wasn't written to
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_three_layers() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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
            )
            .await?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

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

        let mut volume = Volume::construct(vcr, None).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64);
                volume.add_subvolume(in_memory_data.clone()).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate(0).await?;

        // Verify contents are 11 on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Verify parent wasn't written to
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_url() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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

        let volume = Volume::construct(vcr, None).await?;
        volume.activate(0).await?;

        // Read one block: should be all 0xff
        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0xff; BLOCK_SIZE], *buffer.as_vec().await);

        // Write one block full of 0x01
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x01; BLOCK_SIZE]),
            )
            .await?;

        // Read one block: should be all 0x01
        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x01; BLOCK_SIZE], *buffer.as_vec().await);
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_just_read() -> Result<()> {
        // Just do a read of a new volume.
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(true).await?;
        let opts = tds.opts();

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

        let volume = Volume::construct(vcr, None).await?;
        volume.activate(0).await?;

        // Read one block: should be all 0x00
        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;

        // Write data in
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read volume, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write_unwritten data in, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read volume, verify original contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;

        // Write data in
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read volume, verify original contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;

        // Write data at block 0
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x33; BLOCK_SIZE]),
            )
            .await?;

        // A second Write_unwritten that overlaps the original write.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read and verify
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

        // Verify data in the first block is from the first write
        assert_eq!(vec![0x33_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify the remaining blocks have the write_unwritten data
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE * 9],
            dl[BLOCK_SIZE..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    #[tokio::test]
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
        let tds1 = TestDownstairsSet::new(false).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let tds2 = TestDownstairsSet::new(false).await?;
        let opts = tds2.opts();
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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;

        let full_volume_size = BLOCK_SIZE * 20;
        // Write data in
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; full_volume_size]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; full_volume_size], *buffer.as_vec().await);

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; full_volume_size]),
            )
            .await?;

        // Read volume, verify original contents
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; full_volume_size], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds1 = TestDownstairsSet::new(false).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let tds2 = TestDownstairsSet::new(false).await?;
        let opts = tds2.opts();
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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;
        let full_volume_size = BLOCK_SIZE * 20;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 2]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 2], *buffer.as_vec().await);

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; full_volume_size]),
            )
            .await?;

        // Read full volume, verify first write_unwritten still valid, but the
        // other blocks of the 2nd write_unwritten are updated.
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

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

    #[tokio::test]
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
        let tds1 = TestDownstairsSet::new(false).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let tds2 = TestDownstairsSet::new(false).await?;
        let opts = tds2.opts();
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

        let volume = Arc::new(Volume::construct(vcr, None).await?);

        volume.activate(0).await?;
        let full_volume_size = BLOCK_SIZE * 20;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 2]),
            )
            .await?;

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x22; BLOCK_SIZE * 10]),
            )
            .await?;

        // A write
        volume
            .write(
                Block::new(7, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x11; BLOCK_SIZE * 13]),
            )
            .await?;

        // Read full volume
        let buffer = Buffer::new(full_volume_size);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

        // Verify data in blocks 0-7 is the second write_unwritten
        assert_eq!(vec![0x22_u8; BLOCK_SIZE * 7], dl[0..(BLOCK_SIZE * 7)]);

        // Verify data in blocks 8-19 is the third write
        assert_eq!(
            vec![0x11_u8; BLOCK_SIZE * 13],
            dl[(BLOCK_SIZE * 7)..full_volume_size]
        );

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_two_layers_parent_smaller() -> Result<()> {
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();
        integration_test_two_layers_small_common(opts, false).await
    }

    #[tokio::test]
    async fn integration_test_two_layers_parent_smaller_unwritten() -> Result<()>
    {
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();
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
            )
            .await?;

        // Read back in_memory, verify 1s
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        in_memory_data
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], *buffer.as_vec().await);

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate(0).await?;

        // Verify parent contents in one read
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![11; BLOCK_SIZE * 5];
        expected.extend(vec![0x00; BLOCK_SIZE * 5]);
        assert_eq!(expected, *buffer.as_vec().await);

        // One big write!
        let write_offset = Block::new(0, BLOCK_SIZE.trailing_zeros());
        let write_data = Bytes::from(vec![55; BLOCK_SIZE * 10]);
        if is_write_unwritten {
            volume.write(write_offset, write_data).await?;
        } else {
            volume.write_unwritten(write_offset, write_data).await?;
        }

        // Verify volume contents in one read
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate(0).await?;

        // Verify contents are 11 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub().await.unwrap();

        // Now, try a write_unwritten, this should not change our
        // data as the scrubber has finished.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate(0).await?;

        // Verify contents of RO parent are 1s at startup
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], *buffer.as_vec().await);

        // Verify contents of blocks 5-10 are zero.
        let buffer = Buffer::new(BLOCK_SIZE * 5);
        volume
            .read(Block::new(5, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![00; BLOCK_SIZE * 5], *buffer.as_vec().await);

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub().await.unwrap();

        // Now, try a write_unwritten, this should not change our
        // unwritten data as the scrubber has finished.
        volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

        // Verify data in the first half is from the RO parent
        assert_eq!(vec![11; BLOCK_SIZE * 5], dl[0..BLOCK_SIZE * 5]);
        // Verify data in the second half is from the write unwritten
        assert_eq!(
            vec![55; BLOCK_SIZE * 5],
            dl[BLOCK_SIZE * 5..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate(0).await?;

        // SV: |--2-------|
        volume
            .write(
                Block::new(2, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE]),
            )
            .await?;

        // SV: |-------3--|
        volume
            .write(
                Block::new(7, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE]),
            )
            .await?;

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume except where new writes have
        // landed
        volume.scrub().await.unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

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
        assert_eq!(expected, *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

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
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate(0).await?;

        // Verify contents are 11 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write to the whole volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Call the scrubber.  This should do nothing
        volume.scrub().await.unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
            )
            .await?;

        let mut sv = Vec::new();
        let tds1 = TestDownstairsSet::new(false).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let tds2 = TestDownstairsSet::new(false).await?;
        let opts = tds2.opts();
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

        let mut volume = Volume::construct(vcr, None).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64);
                volume.add_subvolume(in_memory_data).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate(0).await?;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE * 2]),
            )
            .await?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], *buffer.as_vec().await);

        // A second write
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE * 2]),
            )
            .await?;

        // Call the scrubber
        volume.scrub().await.unwrap();

        // Read full volume
        let buffer = Buffer::new(BLOCK_SIZE * 20);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Build the expected vec to compare our read with.
        // First two blocks are 3s     |33--------||----------|
        let mut expected = vec![33; BLOCK_SIZE * 2];
        // Original 1s from RO parent  |331111111-||----------|
        expected.extend(vec![11; BLOCK_SIZE * 7]);
        // Two blocks of 2             |3311111112||2---------|
        expected.extend(vec![22; BLOCK_SIZE * 2]);
        // remaining final blocks of 0 |3311111112||2000000000|
        expected.extend(vec![0; BLOCK_SIZE * 9]);
        assert_eq!(expected, *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
            )
            .await?;

        let mut sv = Vec::new();
        let tds1 = TestDownstairsSet::new(false).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            opts,
            gen: 0,
        });
        let tds2 = TestDownstairsSet::new(false).await?;
        let opts = tds2.opts();
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

        let mut volume = Volume::construct(vcr, None).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64);
                volume.add_subvolume(in_memory_data).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate(0).await?;

        // Write data to last block of first vol, and first block of
        // second vol, AKA write A.
        //     |---------2||2---------|
        volume
            .write(
                Block::new(9, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![22; BLOCK_SIZE * 2]),
            )
            .await?;

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        volume
            .read(Block::new(9, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], *buffer.as_vec().await);

        // Write B
        //     |33--------||----------|
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![33; BLOCK_SIZE * 2]),
            )
            .await?;

        // Write C
        //     |----------||----44----|
        volume
            .write(
                Block::new(14, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![44; BLOCK_SIZE * 2]),
            )
            .await?;

        // Call the scrubber
        volume.scrub().await.unwrap();

        // Read full volume
        let buffer = Buffer::new(BLOCK_SIZE * 20);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

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
        assert_eq!(expected, *buffer.as_vec().await);

        Ok(())
    }

    // Test that multiple upstairs can connect to a single read-only downstairs
    #[tokio::test]
    async fn integration_test_multi_read_only() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(true).await?;
        let mut opts = tds.opts();

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

        let volume1 = Volume::construct(vcr_1, None).await?;
        volume1.activate(0).await?;

        let volume2 = Volume::construct(vcr_2, None).await?;
        volume2.activate(0).await?;

        // Read one block: should be all 0x00
        let buffer = Buffer::new(BLOCK_SIZE);
        volume1
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec().await);

        let buffer = Buffer::new(BLOCK_SIZE);
        volume2
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
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
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(opts, 0, None, BLOCK_SIZE)
            .await?;

        volume.activate(0).await?;

        // Verify contents are 00 at startup
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write to half volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![55; BLOCK_SIZE * 5]),
            )
            .await?;

        // Call the scrubber.  This should do nothing
        volume.scrub().await.unwrap();

        // Read and verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Build the expected vec to compare our read with.
        // First 5 blocks are 5s              |55555-----|
        let mut expected = vec![55; BLOCK_SIZE * 5];
        // Original 0s from unwritten blocks  |5555500000|
        expected.extend(vec![0; BLOCK_SIZE * 5]);
        assert_eq!(expected, *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_snapshot_backed_vol() -> Result<()> {
        // Test using a "snapshot" (for this test, downstairs booted read-only)
        // as a read-only parent.

        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set = TestDownstairsSet::new(false).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                0,
                None,
                BLOCK_SIZE,
            )
            .await?;

        volume.activate(0).await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(random_buffer.clone()),
            )
            .await?;

        volume.deactivate().await?;

        drop(volume);

        test_downstairs_set.reboot_read_only().await?;

        // Validate that this now accepts reads and flushes, but rejects writes
        {
            let mut volume = Volume::new(BLOCK_SIZE as u64);
            volume
                .add_subvolume_create_guest(
                    test_downstairs_set.opts(),
                    0,
                    None,
                    BLOCK_SIZE,
                )
                .await?;

            volume.activate(0).await?;

            let buffer = Buffer::new(volume.total_size().await? as usize);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            assert_eq!(*buffer.as_vec().await, random_buffer);

            assert!(volume
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![0u8; BLOCK_SIZE]),
                )
                .await
                .is_err());

            volume.flush(None).await?;
        }

        // create a new volume, layering a new set of downstairs on top of the
        // read-only one we just (re)booted
        let top_layer_tds = TestDownstairsSet::new(false).await?;
        let top_layer_opts = top_layer_tds.opts();
        let bottom_layer_opts = test_downstairs_set.opts();

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    opts: top_layer_opts,
                    gen: 0,
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Volume {
                        id: Uuid::new_v4(),
                        block_size: BLOCK_SIZE as u64,
                        sub_volumes: vec![VolumeConstructionRequest::Region {
                            block_size: BLOCK_SIZE as u64,
                            opts: bottom_layer_opts,
                            gen: 0,
                        }],
                        read_only_parent: None,
                    },
                )),
            };

        let volume = Volume::construct(vcr, None).await?;
        volume.activate(0).await?;

        // Validate that source blocks originally come from the read-only parent
        {
            let buffer = Buffer::new(volume.total_size().await? as usize);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            assert_eq!(*buffer.as_vec().await, random_buffer);
        }

        // Validate a flush works
        volume.flush(None).await?;

        // Write one block of 0x00 in, validate with a read
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0u8; BLOCK_SIZE]),
            )
            .await?;

        {
            let buffer = Buffer::new(volume.total_size().await? as usize);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            let buffer_vec = buffer.as_vec().await;

            assert_eq!(buffer_vec[..BLOCK_SIZE], vec![0u8; BLOCK_SIZE]);
            assert_eq!(buffer_vec[BLOCK_SIZE..], random_buffer[BLOCK_SIZE..]);
        }

        // Validate a flush still works
        volume.flush(None).await?;

        Ok(())
    }

    // The following tests work at the "guest" layer. The volume
    // layers above (in general) will eventually call a BlockIO trait
    // on a guest layer.

    #[tokio::test]
    async fn integration_test_guest_downstairs() -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write and read work as expected
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(0).await?;
        guest.query_work_queue().await?;

        // Verify contents are zero on init
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write data in
        guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_upstairs_read_only_rejects_write() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin up three read-only downstairs
        let tds = TestDownstairsSet::new(true).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        // Read-only Upstairs should return errors if writes are attempted.
        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(0).await?;

        // Expect an error attempting to write.
        let write_result = guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await;
        assert!(write_result.is_err());
        assert!(matches!(
            write_result.err().unwrap(),
            CrucibleError::ModifyingReadOnlyRegion
        ));

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten() -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected
        // The size here spans two extents.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write_unwritten again with different data
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read back the same blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Now, just write.  This should update our data.
        guest
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x89; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read back the same blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Verify data is now from the new write.
        assert_eq!(vec![0x89_u8; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten_sparse_1() -> Result<()>
    {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the first block
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )
            .await?;

        // Write_unwritten again with different data and same start
        // range, but write to blocks 2 and 3 this time as well.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )
            .await?;

        // Read back the first block.
        let buffer = Buffer::new(BLOCK_SIZE);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], *buffer.as_vec().await);

        // Read back the next two blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(1, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x99_u8; BLOCK_SIZE * 2], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten_sparse_mid(
    ) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes where the middle block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the second block
        guest
            .write_unwritten(
                Block::new(1, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )
            .await?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )
            .await?;

        // Read back the all three blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 3);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

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

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten_sparse_end(
    ) -> Result<()> {
        // Test write_unwritten and read work as expected,
        // this time with sparse writes where the last block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the third block
        guest
            .write_unwritten(
                Block::new(2, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )
            .await?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 3]),
            )
            .await?;

        // Read back the all three blocks.
        let buffer = Buffer::new(BLOCK_SIZE * 3);
        guest
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

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

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten_span() -> Result<()> {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )
            .await?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 2]),
            )
            .await?;

        // Read back both blocks
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(4, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_guest_downstairs_unwritten_span_2() -> Result<()>
    {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE]),
            )
            .await?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                Block::new(4, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x99; BLOCK_SIZE * 2]),
            )
            .await?;

        // Read back both blocks
        let buffer = Buffer::new(BLOCK_SIZE * 2);
        guest
            .read(Block::new(4, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        // Get the data into a vec we can take slices of.
        let dl = buffer.as_vec().await.to_vec();

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            dl[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_insertion_order() -> Result<()> {
        // Test that job insertion order is preserved
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;

        let future_1 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x00; BLOCK_SIZE]),
        );

        let future_2 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x11; BLOCK_SIZE]),
        );

        let future_3 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x22; BLOCK_SIZE]),
        );

        future_1.await?;
        future_2.await?;
        future_3.await?;

        // Verify last write landed
        let buffer = Buffer::new(BLOCK_SIZE);
        guest.read(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            buffer.clone(),
        )
        .await?;

        let dl = buffer.as_vec().await.to_vec();

        assert_eq!(vec![0x22; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_insertion_order_out_of_order_await() -> Result<()> {
        // Test that job insertion order is preserved even if futures are polled
        // out of order
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::new(false).await?;
        let opts = tds.opts();

        let guest = Arc::new(Guest::new(BLOCK_SIZE));
        let gc = guest.clone();

        let _join_handle = up_main(opts, 0, gc, None).await?;

        guest.activate(1).await?;

        let future_1 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x00; BLOCK_SIZE]),
        );

        let future_2 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x11; BLOCK_SIZE]),
        );

        let future_3 = guest.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![0x22; BLOCK_SIZE]),
        );

        future_3.await?;
        future_2.await?;
        future_1.await?;

        // Verify last write landed
        let buffer = Buffer::new(BLOCK_SIZE);
        guest.read(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            buffer.clone(),
        )
        .await?;

        let dl = buffer.as_vec().await.to_vec();

        assert_eq!(vec![0x22; BLOCK_SIZE], dl[0..BLOCK_SIZE]);

        Ok(())
    }
}
