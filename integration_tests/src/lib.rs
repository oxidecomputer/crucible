// Copyright 2023 Oxide Computer Company

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use anyhow::*;
    use base64::{engine, Engine};
    use crucible::*;
    use crucible_client_types::VolumeConstructionRequest;
    use crucible_downstairs::*;
    use crucible_pantry::pantry::Pantry;
    use crucible_pantry_client::Client as CruciblePantryClient;
    use httptest::{matchers::*, responders::*, Expectation, Server};
    use rand::Rng;
    use repair_client::Client;
    use sha2::Digest;
    use slog::{info, o, warn, Drain, Logger};
    use tempfile::*;
    use tokio::sync::mpsc;
    use uuid::*;

    // Create a simple logger
    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct TestDownstairs {
        address: IpAddr,
        tempdir: TempDir,
        downstairs: Option<RunningDownstairs>,
    }

    impl TestDownstairs {
        #[allow(clippy::too_many_arguments)]
        pub async fn new(
            address: IpAddr,
            encrypted: bool,
            read_only: bool,
            blocks_per_extent: u64,
            extent_count: u32,
            problematic: bool,
            backend: Backend,
            clone_source: Option<SocketAddr>,
        ) -> Result<Self> {
            let tempdir = tempfile::Builder::new()
                .prefix(&"downstairs-")
                .rand_bytes(8)
                .tempdir()?;

            let region = create_region_with_backend(
                tempdir.path().to_path_buf(),
                Block {
                    value: blocks_per_extent,
                    shift: 9,
                },
                extent_count,
                Uuid::new_v4(),
                encrypted,
                backend,
                csl(),
            )?;

            // If this is a read-write extent, then reuse the existing region;
            // this skips automatic migration of SQLite -> raw extents and lets
            // us keep writing to a SQLite region.
            let builder = if read_only {
                Downstairs::new_builder(tempdir.path(), read_only)
            } else {
                assert!(!region.read_only());
                DownstairsBuilder::from_region(region)
            };

            let mut downstairs = builder
                .set_lossy(problematic)
                .set_logger(csl())
                .set_test_errors(problematic, problematic, problematic)
                .build()?;

            if let Some(ref clone_source) = clone_source {
                downstairs.clone_region(*clone_source).await?
            }

            let downstairs = start_downstairs(
                downstairs, address, None, /* oximeter */
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
                downstairs: Some(downstairs),
            })
        }

        pub async fn reboot_read_only(&mut self) -> Result<()> {
            let downstairs = Downstairs::new_builder(self.tempdir.path(), true)
                .set_logger(csl())
                .build()?;

            self.downstairs = Some(
                start_downstairs(
                    downstairs,
                    self.address,
                    None, /* oximeter */
                    0,    /* any port */
                    0,    /* any rport */
                    None, /* cert_pem */
                    None, /* key_pem */
                    None, /* root_cert_pem */
                )
                .await?,
            );

            Ok(())
        }

        pub async fn reboot_read_write(&mut self) -> Result<()> {
            let downstairs =
                Downstairs::new_builder(self.tempdir.path(), false)
                    .set_logger(csl())
                    .build()?;

            self.downstairs = Some(
                start_downstairs(
                    downstairs,
                    self.address,
                    None, /* oximeter */
                    0,    /* any port */
                    0,    /* any rport */
                    None, /* cert_pem */
                    None, /* key_pem */
                    None, /* root_cert_pem */
                )
                .await?,
            );

            Ok(())
        }

        // Take a downstairs and start it with clone option.  This should
        // attempt to clone extents from the provided source IP:Port downstairs.
        //
        // The Result is returned to the caller.
        pub async fn reboot_clone(&mut self, source: SocketAddr) -> Result<()> {
            // Note that we don't start the new Downstairs here, so we'll clear
            // out `self.downstairs`.
            self.downstairs = None;
            let mut downstairs =
                Downstairs::new_builder(self.tempdir.path(), true)
                    .set_logger(csl())
                    .build()?;
            downstairs.clone_region(source).await
        }

        pub fn address(&self) -> SocketAddr {
            // If start_downstairs returned Ok, then address will be populated
            self.downstairs.as_ref().unwrap().address
        }

        // Return the repair address for a running downstairs
        pub fn repair_address(&self) -> SocketAddr {
            // If start_downstairs returned Ok, then address will be populated
            self.downstairs.as_ref().unwrap().repair_address
        }
    }

    #[derive(Debug)]
    struct TestDownstairsSet {
        downstairs1: TestDownstairs,
        downstairs2: TestDownstairs,
        downstairs3: TestDownstairs,
        crucible_opts: CrucibleOpts,
        blocks_per_extent: u64,
        extent_count: u32,
    }

    impl TestDownstairsSet {
        /// Spin off three downstairs, with a 5120b region
        pub async fn small(
            read_only: bool,
            backend: Backend,
        ) -> Result<TestDownstairsSet> {
            // 5 * 2 * 512 = 5120b
            let blocks_per_extent = 5;
            let extent_count = 2;
            TestDownstairsSet::new_with_flag(
                read_only,
                blocks_per_extent,
                extent_count,
                false,
                backend,
            )
            .await
        }

        /// Spin off three SQLite downstairs, with a 5120b region
        pub async fn small_sqlite(
            read_only: bool,
        ) -> Result<TestDownstairsSet> {
            // 5 * 2 * 512 = 5120b
            let blocks_per_extent = 5;
            let extent_count = 2;
            TestDownstairsSet::new_with_flag(
                read_only,
                blocks_per_extent,
                extent_count,
                false,
                Backend::SQLite,
            )
            .await
        }

        /// Spin off three downstairs, with a 50 MB region
        pub async fn big(
            read_only: bool,
            backend: Backend,
        ) -> Result<TestDownstairsSet> {
            // 512 * 188 * 512 = 49283072b ~= 50MB
            let blocks_per_extent = 512;
            let extent_count = 188;
            TestDownstairsSet::new_with_flag(
                read_only,
                blocks_per_extent,
                extent_count,
                false,
                backend,
            )
            .await
        }

        /// Spin off three problematic downstairs, with a 10 MB region
        pub async fn problem(backend: Backend) -> Result<TestDownstairsSet> {
            // 512 * 40 * 512 = 10485760b = 10MB
            let blocks_per_extent = 512;
            let extent_count = 188;
            TestDownstairsSet::new_with_flag(
                false, // read only
                blocks_per_extent,
                extent_count,
                true, // problematic
                backend,
            )
            .await
        }

        /// Spin off three downstairs
        pub async fn new_with_flag(
            read_only: bool,
            blocks_per_extent: u64,
            extent_count: u32,
            problematic: bool,
            backend: Backend,
        ) -> Result<TestDownstairsSet> {
            let downstairs1 = TestDownstairs::new(
                "127.0.0.1".parse()?,
                true,
                read_only,
                blocks_per_extent,
                extent_count,
                problematic,
                backend,
                None,
            )
            .await?;
            let downstairs2 = TestDownstairs::new(
                "127.0.0.1".parse()?,
                true,
                read_only,
                blocks_per_extent,
                extent_count,
                problematic,
                backend,
                None,
            )
            .await?;
            let downstairs3 = TestDownstairs::new(
                "127.0.0.1".parse()?,
                true,
                read_only,
                blocks_per_extent,
                extent_count,
                problematic,
                backend,
                None,
            )
            .await?;

            // Generate random data for our key
            let key_bytes = rand::thread_rng().gen::<[u8; 32]>();
            let key_string =
                engine::general_purpose::STANDARD.encode(key_bytes);

            let crucible_opts = CrucibleOpts {
                id: Uuid::new_v4(),
                target: vec![
                    downstairs1.address(),
                    downstairs2.address(),
                    downstairs3.address(),
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
                blocks_per_extent,
                extent_count,
            })
        }

        // Return a Crucible Opts struct pre-populated with the same
        // three given ports for targets.
        pub fn opts(&self) -> CrucibleOpts {
            self.crucible_opts.clone()
        }

        pub fn blocks_per_extent(&self) -> u64 {
            self.blocks_per_extent
        }

        pub fn extent_count(&self) -> u32 {
            self.extent_count
        }

        pub async fn reboot_read_only(&mut self) -> Result<()> {
            self.downstairs1.reboot_read_only().await?;
            self.downstairs2.reboot_read_only().await?;
            self.downstairs3.reboot_read_only().await?;

            self.crucible_opts.read_only = true;
            self.crucible_opts.target = vec![
                self.downstairs1.address(),
                self.downstairs2.address(),
                self.downstairs3.address(),
            ];

            Ok(())
        }

        pub async fn reboot_read_write(&mut self) -> Result<()> {
            assert!(!self.crucible_opts.read_only);
            self.downstairs1.reboot_read_write().await?;
            self.downstairs2.reboot_read_write().await?;
            self.downstairs3.reboot_read_write().await?;

            self.crucible_opts.target = vec![
                self.downstairs1.address(),
                self.downstairs2.address(),
                self.downstairs3.address(),
            ];

            Ok(())
        }

        pub async fn new_downstairs(&self) -> Result<TestDownstairs> {
            TestDownstairs::new(
                "127.0.0.1".parse()?,
                true,
                false,
                self.blocks_per_extent,
                self.extent_count,
                false,
                Backend::RawFile,
                None,
            )
            .await
        }

        pub fn downstairs1_address(&self) -> SocketAddr {
            self.downstairs1.address()
        }

        pub fn downstairs2_address(&self) -> SocketAddr {
            self.downstairs2.address()
        }

        pub fn downstairs3_address(&self) -> SocketAddr {
            self.downstairs3.address()
        }
    }

    async fn integration_test_region(backend: Backend) -> Result<()> {
        // Test a simple single layer volume with a read, write, read
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // Verify contents are zero on init
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Write data in
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_region_huge_io(backend: Backend) -> Result<()> {
        // Test a simple single layer volume with a read, write, read, with IOs
        // that exceed our MDTS (and should be split automatically)
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::big(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // We'll do a 4 MiB read, which is > 1 MiB (our MDTS)
        const NUM_BLOCKS: usize = (4 * 1024 * 1024) / BLOCK_SIZE + 1;

        // Verify contents are zero on init
        let mut buffer = Buffer::new(NUM_BLOCKS, BLOCK_SIZE);
        for i in 0..10 {
            volume.read(BlockIndex(i), &mut buffer).await?;
            assert_eq!(vec![0x00_u8; BLOCK_SIZE * NUM_BLOCKS], &buffer[..]);
        }

        for i in 0..10 {
            let mut data = vec![0; BLOCK_SIZE * NUM_BLOCKS];
            rand::thread_rng().fill(&mut data[..]);
            volume
                .write(BlockIndex(i), BytesMut::from(data.as_slice()))
                .await?;

            // Read parent, verify contents
            let mut buffer = Buffer::new(NUM_BLOCKS, BLOCK_SIZE);
            volume.read(BlockIndex(i), &mut buffer).await?;
            assert_eq!(&data, &buffer[..]);

            // Do a shifted read, to detect any block shuffling that's
            // consistent between reads and writes (which would be weird!)
            let mut buffer = Buffer::new(NUM_BLOCKS - 1, BLOCK_SIZE);
            volume.read(BlockIndex(i + 1), &mut buffer).await?;
            assert_eq!(&data[BLOCK_SIZE..], &buffer[..]);
        }

        Ok(())
    }

    async fn volume_zero_length_io(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // A read of zero length does not error.
        let mut buffer = Buffer::new(0, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // A write of zero length does not return error.
        volume
            .write(BlockIndex(0), BytesMut::from(vec![0x55; 0].as_slice()))
            .await?;

        Ok(())
    }

    async fn integration_test_two_layers(backend: Backend) -> Result<()> {
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();
        integration_test_two_layers_common(tds, opts, false).await
    }

    async fn integration_test_two_layers_write_unwritten(
        backend: Backend,
    ) -> Result<()> {
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();
        integration_test_two_layers_common(tds, opts, true).await
    }

    async fn integration_test_two_layers_common(
        tds: TestDownstairsSet,
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        in_memory_data.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data.clone()).await?;

        volume.activate().await?;

        // Verify contents are 11 on init
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Write data in
        if is_write_unwritten {
            volume
                .write_unwritten(
                    BlockIndex(0),
                    BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
                )
                .await?;
        } else {
            volume
                .write(
                    BlockIndex(0),
                    BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
                )
                .await?;
        }

        // Verify parent wasn't written to
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        in_memory_data.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_three_layers(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        // Create in memory block io full of 11
        let in_memory_data = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        in_memory_data
            .write(
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        in_memory_data.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Create volume with read only parent
        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let log = csl();
        let mut volume = Volume::construct(vcr, None, log.clone()).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64, log.clone());
                volume.add_subvolume(in_memory_data.clone()).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate().await?;

        // Verify contents are 11 on init
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Write data in
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Verify parent wasn't written to
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        in_memory_data.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_url(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(false, backend).await?;
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

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
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

        let volume = Volume::construct(vcr, None, csl()).await?;
        volume.activate().await?;

        // Read one block: should be all 0xff
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0xff; BLOCK_SIZE], &buffer[..]);

        // Write one block full of 0x01
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x01; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Read one block: should be all 0x01
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x01; BLOCK_SIZE], &buffer[..]);
        Ok(())
    }

    async fn integration_test_just_read(backend: Backend) -> Result<()> {
        // Just do a read of a new volume.
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(true, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                    opts,
                    gen: 1,
                },
            )),
        };

        let volume = Volume::construct(vcr, None, csl()).await?;
        volume.activate().await?;

        // Read one block: should be all 0x00
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_1(
        backend: Backend,
    ) -> Result<()> {
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

        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // Write data in
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read volume, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Write_unwritten data in, should not change anything
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x22; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read volume, verify original contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_2(
        backend: Backend,
    ) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // Write data in
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x22; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read volume, verify original contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_sparse(
        backend: Backend,
    ) -> Result<()> {
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

        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        // Write data at block 0
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x33; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // A second Write_unwritten that overlaps the original write.
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read and verify
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in the first block is from the first write
        assert_eq!(vec![0x33_u8; BLOCK_SIZE], &buffer[0..BLOCK_SIZE]);

        // Verify the remaining blocks have the write_unwritten data
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE * 9],
            &buffer[BLOCK_SIZE..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_subvols(
        backend: Backend,
    ) -> Result<()> {
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
        let tds1 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds1.blocks_per_extent(),
            extent_count: tds1.extent_count(),
            opts,
            gen: 1,
        });
        let tds2 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds2.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds2.blocks_per_extent(),
            extent_count: tds2.extent_count(),
            opts,
            gen: 1,
        });

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: sv,
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;

        let full_volume_blocks = 20;
        let full_volume_size = BLOCK_SIZE * full_volume_blocks;
        // Write data in
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x55; full_volume_size].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(full_volume_blocks, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; full_volume_size], &buffer[..]);

        // A second Write_unwritten data, should not change anything
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x22; full_volume_size].as_slice()),
            )
            .await?;

        // Read volume, verify original contents
        let mut buffer = Buffer::new(full_volume_blocks, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; full_volume_size], &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_subvols_sparse(
        backend: Backend,
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
        let tds1 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds1.blocks_per_extent(),
            extent_count: tds1.extent_count(),
            opts,
            gen: 1,
        });
        let tds2 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds2.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds2.blocks_per_extent(),
            extent_count: tds2.extent_count(),
            opts,
            gen: 1,
        });

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: sv,
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;
        let full_volume_blocks = 20;
        let full_volume_size = BLOCK_SIZE * full_volume_blocks;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                BlockIndex(9),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        volume.read(BlockIndex(9), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 2], &buffer[..]);

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x22; full_volume_size].as_slice()),
            )
            .await?;

        // Read full volume, verify first write_unwritten still valid, but the
        // other blocks of the 2nd write_unwritten are updated.
        let mut buffer = Buffer::new(full_volume_blocks, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in blocks 0-9 is the second write_unwritten
        assert_eq!(vec![0x22_u8; BLOCK_SIZE * 9], &buffer[0..(BLOCK_SIZE * 9)]);

        // Verify data in blocks 10-11 is the first write_unwritten
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE * 2],
            &buffer[(BLOCK_SIZE * 9)..(BLOCK_SIZE * 11)]
        );

        // Verify the remaining blocks have the second write_unwritten data
        assert_eq!(
            vec![0x22_u8; BLOCK_SIZE * 9],
            &buffer[(BLOCK_SIZE * 11)..full_volume_size]
        );

        Ok(())
    }

    async fn integration_test_volume_write_unwritten_subvols_3(
        backend: Backend,
    ) -> Result<()> {
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
        let tds1 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds1.blocks_per_extent(),
            extent_count: tds1.extent_count(),
            opts,
            gen: 1,
        });
        let tds2 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds2.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds2.blocks_per_extent(),
            extent_count: tds2.extent_count(),
            opts,
            gen: 1,
        });

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: sv,
            read_only_parent: None,
        };

        let volume = Arc::new(Volume::construct(vcr, None, csl()).await?);

        volume.activate().await?;
        let full_volume_blocks = 20;
        let full_volume_size = BLOCK_SIZE * full_volume_blocks;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write_unwritten(
                BlockIndex(9),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // A second Write_unwritten data, should not change the previous
        // write_unwritten, but should change the remaining blocks that
        // were not written yet.
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x22; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // A write
        volume
            .write(
                BlockIndex(7),
                BytesMut::from(vec![0x11; BLOCK_SIZE * 13].as_slice()),
            )
            .await?;

        // Read full volume
        let mut buffer = Buffer::new(full_volume_blocks, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in blocks 0-7 is the second write_unwritten
        assert_eq!(vec![0x22_u8; BLOCK_SIZE * 7], &buffer[0..(BLOCK_SIZE * 7)]);

        // Verify data in blocks 8-19 is the third write
        assert_eq!(
            vec![0x11_u8; BLOCK_SIZE * 13],
            &buffer[(BLOCK_SIZE * 7)..full_volume_size]
        );

        Ok(())
    }

    async fn integration_test_two_layers_parent_smaller(
        backend: Backend,
    ) -> Result<()> {
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();
        integration_test_two_layers_small_common(tds, opts, false).await
    }

    async fn integration_test_two_layers_parent_smaller_unwritten(
        backend: Backend,
    ) -> Result<()> {
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();
        integration_test_two_layers_small_common(tds, opts, true).await
    }

    async fn integration_test_two_layers_small_common(
        tds: TestDownstairsSet,
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 5].as_slice()),
            )
            .await?;

        // Read back in_memory, verify 1s
        let mut buffer = Buffer::new(5, BLOCK_SIZE);
        in_memory_data.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], &buffer[..]);

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate().await?;

        // Verify parent contents in one read
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![11; BLOCK_SIZE * 5];
        expected.extend(vec![0x00; BLOCK_SIZE * 5]);
        assert_eq!(expected, &buffer[..]);

        // One big write!
        let write_offset = BlockIndex(0);
        let write_data = BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice());
        if is_write_unwritten {
            volume.write(write_offset, write_data).await?;
        } else {
            volume.write_unwritten(write_offset, write_data).await?;
        }

        // Verify volume contents in one read
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_scrub(backend: Backend) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate().await?;

        // Verify contents are 11 at startup
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub(None, None).await.unwrap();

        // Now, try a write_unwritten, this should not change our
        // data as the scrubber has finished.
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_scrub_short(backend: Backend) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 5].as_slice()),
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate().await?;

        // Verify contents of RO parent are 1s at startup
        let mut buffer = Buffer::new(5, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 5], &buffer[..]);

        // Verify contents of blocks 5-10 are zero.
        let mut buffer = Buffer::new(5, BLOCK_SIZE);
        volume.read(BlockIndex(5), &mut buffer).await?;

        assert_eq!(vec![00; BLOCK_SIZE * 5], &buffer[..]);

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume.
        volume.scrub(None, None).await.unwrap();

        // Now, try a write_unwritten, this should not change our
        // unwritten data as the scrubber has finished.
        volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in the first half is from the RO parent
        assert_eq!(vec![11; BLOCK_SIZE * 5], &buffer[0..BLOCK_SIZE * 5]);
        // Verify data in the second half is from the write unwritten
        assert_eq!(
            vec![55; BLOCK_SIZE * 5],
            &buffer[BLOCK_SIZE * 5..BLOCK_SIZE * 10]
        );

        Ok(())
    }

    async fn integration_test_scrub_short_sparse(
        backend: Backend,
    ) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 5].as_slice()),
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate().await?;

        // SV: |--2-------|
        volume
            .write(
                BlockIndex(2),
                BytesMut::from(vec![22; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // SV: |-------3--|
        volume
            .write(
                BlockIndex(7),
                BytesMut::from(vec![33; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Call the scrubber.  This should replace all data from the
        // RO parent into the main volume except where new writes have
        // landed
        volume.scrub(None, None).await.unwrap();

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

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
        assert_eq!(expected, &buffer[..]);

        Ok(())
    }

    async fn integration_test_scrub_useless(backend: Backend) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;
        volume.add_read_only_parent(in_memory_data).await?;

        volume.activate().await?;

        // Verify contents are 11 at startup
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; BLOCK_SIZE * 10], &buffer[..]);

        // Write to the whole volume
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Call the scrubber.  This should do nothing
        volume.scrub(None, None).await.unwrap();

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![55; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_subvols_parent_scrub_sparse(
        backend: Backend,
    ) -> Result<()> {
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        let mut sv = Vec::new();
        let tds1 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds1.blocks_per_extent(),
            extent_count: tds1.extent_count(),
            opts,
            gen: 1,
        });
        let tds2 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds2.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds2.blocks_per_extent(),
            extent_count: tds2.extent_count(),
            opts,
            gen: 1,
        });

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: sv,
            read_only_parent: None,
        };

        let log = csl();
        let mut volume = Volume::construct(vcr, None, log.clone()).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64, log.clone());
                volume.add_subvolume(in_memory_data).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate().await?;

        // Write data to last block of first vol, and first block of
        // second vol.
        volume
            .write(
                BlockIndex(9),
                BytesMut::from(vec![22; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Read and verify contents
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        volume.read(BlockIndex(9), &mut buffer).await?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], &buffer[..]);

        // A second write
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![33; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Call the scrubber
        volume.scrub(None, None).await.unwrap();

        // Read full volume
        let mut buffer = Buffer::new(20, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Build the expected vec to compare our read with.
        // First two blocks are 3s     |33--------||----------|
        let mut expected = vec![33; BLOCK_SIZE * 2];
        // Original 1s from RO parent  |331111111-||----------|
        expected.extend(vec![11; BLOCK_SIZE * 7]);
        // Two blocks of 2             |3311111112||2---------|
        expected.extend(vec![22; BLOCK_SIZE * 2]);
        // remaining final blocks of 0 |3311111112||2000000000|
        expected.extend(vec![0; BLOCK_SIZE * 9]);
        assert_eq!(expected, &buffer[..]);

        Ok(())
    }

    async fn integration_test_volume_subvols_parent_scrub_sparse_2(
        backend: Backend,
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
                BlockIndex(0),
                BytesMut::from(vec![11; BLOCK_SIZE * 15].as_slice()),
            )
            .await?;

        let mut sv = Vec::new();
        let tds1 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds1.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds1.blocks_per_extent(),
            extent_count: tds1.extent_count(),
            opts,
            gen: 1,
        });
        let tds2 = TestDownstairsSet::small(false, backend).await?;
        let opts = tds2.opts();
        sv.push(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds2.blocks_per_extent(),
            extent_count: tds2.extent_count(),
            opts,
            gen: 1,
        });

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: sv,
            read_only_parent: None,
        };

        let log = csl();
        let mut volume = Volume::construct(vcr, None, log.clone()).await?;

        volume
            .add_read_only_parent({
                let mut volume = Volume::new(BLOCK_SIZE as u64, log.clone());
                volume.add_subvolume(in_memory_data).await?;
                Arc::new(volume)
            })
            .await?;

        volume.activate().await?;

        // Write data to last block of first vol, and first block of
        // second vol, AKA write A.
        //     |---------2||2---------|
        volume
            .write(
                BlockIndex(9),
                BytesMut::from(vec![22; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Read and verify contents
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        volume.read(BlockIndex(9), &mut buffer).await?;

        assert_eq!(vec![22; BLOCK_SIZE * 2], &buffer[..]);

        // Write B
        //     |33--------||----------|
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![33; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Write C
        //     |----------||----44----|
        volume
            .write(
                BlockIndex(14),
                BytesMut::from(vec![44; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Call the scrubber
        volume.scrub(None, None).await.unwrap();

        // Read full volume
        let mut buffer = Buffer::new(20, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

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
        assert_eq!(expected, &buffer[..]);

        Ok(())
    }

    // Test that multiple upstairs can connect to a single read-only downstairs
    async fn integration_test_multi_read_only(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let tds = TestDownstairsSet::small(true, backend).await?;
        let mut opts = tds.opts();

        let vcr_1 = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                    opts: opts.clone(),
                    gen: 1,
                },
            )),
        };

        // Second volume should have a unique UUID
        opts.id = Uuid::new_v4();

        let vcr_2 = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                    opts,
                    gen: 1,
                },
            )),
        };

        let log = csl();
        let volume1 = Volume::construct(vcr_1, None, log.clone()).await?;
        volume1.activate().await?;

        let volume2 = Volume::construct(vcr_2, None, log.clone()).await?;
        volume2.activate().await?;

        // Read one block: should be all 0x00
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume1.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], &buffer[..]);

        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume2.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00; BLOCK_SIZE], &buffer[..]);

        Ok(())
    }

    async fn integration_test_scrub_no_rop(backend: Backend) -> Result<()> {
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
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: tds.blocks_per_extent(),
                    extent_count: tds.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        // Verify contents are 00 at startup
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0; BLOCK_SIZE * 10], &buffer[..]);

        // Write to half volume
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![55; BLOCK_SIZE * 5].as_slice()),
            )
            .await?;

        // Call the scrubber.  This should do nothing
        volume.scrub(None, None).await.unwrap();

        // Read and verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        // Build the expected vec to compare our read with.
        // First 5 blocks are 5s              |55555-----|
        let mut expected = vec![55; BLOCK_SIZE * 5];
        // Original 0s from unwritten blocks  |5555500000|
        expected.extend(vec![0; BLOCK_SIZE * 5]);
        assert_eq!(expected, &buffer[..]);

        Ok(())
    }

    async fn integration_test_snapshot_backed_vol(
        backend: Backend,
    ) -> Result<()> {
        // Test using a "snapshot" (for this test, downstairs booted read-only)
        // as a read-only parent.

        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await?;

        drop(volume);

        test_downstairs_set.reboot_read_only().await?;

        // Validate that this now accepts reads and flushes, but rejects writes
        {
            let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
            volume
                .add_subvolume_create_guest(
                    test_downstairs_set.opts(),
                    volume::RegionExtentInfo {
                        block_size: BLOCK_SIZE as u64,
                        blocks_per_extent: test_downstairs_set
                            .blocks_per_extent(),
                        extent_count: test_downstairs_set.extent_count(),
                    },
                    2,
                    None,
                )
                .await?;

            volume.activate().await?;

            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..], random_buffer);

            assert!(volume
                .write(
                    BlockIndex(0),
                    BytesMut::from(vec![0u8; BLOCK_SIZE].as_slice()),
                )
                .await
                .is_err());

            volume.flush(None).await?;
        }

        // create a new volume, layering a new set of downstairs on top of the
        // read-only one we just (re)booted
        let top_layer_tds = TestDownstairsSet::small(false, backend).await?;
        let top_layer_opts = top_layer_tds.opts();
        let bottom_layer_opts = test_downstairs_set.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: top_layer_tds.blocks_per_extent(),
                extent_count: top_layer_tds.extent_count(),
                opts: top_layer_opts,
                gen: 3,
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: BLOCK_SIZE as u64,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: BLOCK_SIZE as u64,
                        blocks_per_extent: test_downstairs_set
                            .blocks_per_extent(),
                        extent_count: test_downstairs_set.extent_count(),
                        opts: bottom_layer_opts,
                        gen: 3,
                    }],
                    read_only_parent: None,
                },
            )),
        };

        let volume = Volume::construct(vcr, None, csl()).await?;
        volume.activate().await?;

        // Validate that source blocks originally come from the read-only parent
        {
            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..], random_buffer);
        }

        // Validate a flush works
        volume.flush(None).await?;

        // Write one block of 0x00 in, validate with a read
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0u8; BLOCK_SIZE].as_slice()),
            )
            .await?;

        {
            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..BLOCK_SIZE], vec![0u8; BLOCK_SIZE]);
            assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);
        }

        // Validate a flush still works
        volume.flush(None).await?;

        Ok(())
    }

    async fn integration_test_sqlite_backed_vol(
        backend: Backend,
    ) -> Result<()> {
        // Test using an old SQLite backend as a read-only parent.

        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set =
            TestDownstairsSet::small_sqlite(false).await?;

        // This must be a SQLite extent!
        assert!(test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await?;

        drop(volume);

        test_downstairs_set.reboot_read_only().await?;

        // This must still be a SQLite backend!
        assert!(test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        // Validate that this now accepts reads and flushes, but rejects writes
        {
            let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
            volume
                .add_subvolume_create_guest(
                    test_downstairs_set.opts(),
                    volume::RegionExtentInfo {
                        block_size: BLOCK_SIZE as u64,
                        blocks_per_extent: test_downstairs_set
                            .blocks_per_extent(),
                        extent_count: test_downstairs_set.extent_count(),
                    },
                    2,
                    None,
                )
                .await?;

            volume.activate().await?;

            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..], random_buffer);

            assert!(volume
                .write(
                    BlockIndex(0),
                    BytesMut::from(vec![0u8; BLOCK_SIZE].as_slice()),
                )
                .await
                .is_err());

            volume.flush(None).await?;
        }

        // create a new volume, layering a new set of downstairs on top of the
        // read-only one we just (re)booted
        let top_layer_tds = TestDownstairsSet::small(false, backend).await?;
        let top_layer_opts = top_layer_tds.opts();
        let bottom_layer_opts = test_downstairs_set.opts();

        // The new volume is **not** using the SQLite backend!
        if !matches!(backend, Backend::SQLite) {
            assert!(!top_layer_tds
                .downstairs1
                .tempdir
                .path()
                .join("00/000/000.db")
                .exists());
        }

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: top_layer_tds.blocks_per_extent(),
                extent_count: top_layer_tds.extent_count(),
                opts: top_layer_opts,
                gen: 3,
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: BLOCK_SIZE as u64,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: BLOCK_SIZE as u64,
                        blocks_per_extent: test_downstairs_set
                            .blocks_per_extent(),
                        extent_count: test_downstairs_set.extent_count(),
                        opts: bottom_layer_opts,
                        gen: 3,
                    }],
                    read_only_parent: None,
                },
            )),
        };

        let volume = Volume::construct(vcr, None, csl()).await?;
        volume.activate().await?;

        // Validate that source blocks originally come from the read-only parent
        {
            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..], random_buffer);
        }

        // Validate a flush works
        volume.flush(None).await?;

        // Write one block of 0x00 in, validate with a read
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0u8; BLOCK_SIZE].as_slice()),
            )
            .await?;

        {
            let volume_size = volume.total_size().await? as usize;
            assert_eq!(volume_size % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(&buffer[..BLOCK_SIZE], vec![0u8; BLOCK_SIZE]);
            assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);
        }

        // Validate a flush still works
        volume.flush(None).await?;

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_sqlite_migration() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then reopen as
        // read-write (which will automatically migrate the extent)
        let mut test_downstairs_set =
            TestDownstairsSet::small_sqlite(false).await?;
        // This must be a SQLite extent!
        assert!(test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await?;

        drop(volume);

        // This must still be a SQLite extent!
        assert!(test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        test_downstairs_set.reboot_read_write().await?;
        // This should now be migrated, and the DB file should be deleted
        assert!(!test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                2,
                None,
            )
            .await?;

        volume.activate().await?;
        // Validate that source blocks are the same
        let volume_size = volume.total_size().await? as usize;
        assert_eq!(volume_size % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[..], random_buffer);

        Ok(())
    }

    async fn integration_test_clone_raw(backend: Backend) -> Result<()> {
        // Test downstairs region clone.
        // Create three downstairs with some backend, write some data to them.
        // Restart them all read only.
        // Create a new downstairs.
        // Clone a read only downstairs to the new downstairs
        // Make a new volume with two old and the one new downstairs and verify
        // the original data we wrote.
        // Create a new downstairs directly from the clone.
        // Make a new volume with two old and the one new downstairs and verify
        // the original data we wrote.
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await.unwrap();
        drop(volume);

        // Restart all the downstairs read only
        test_downstairs_set.reboot_read_only().await?;

        // Make the new downstairs
        let mut new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        // Clone an original downstairs to our new downstairs
        let clone_source = test_downstairs_set.downstairs1.repair_address();
        new_ds.reboot_clone(clone_source).await.unwrap();

        // Restart the new downstairs read only.
        new_ds.reboot_read_only().await.unwrap();
        let new_target = new_ds.address();

        // Take our original opts, and replace a target with the downstairs
        // we just cloned.
        let mut new_opts = test_downstairs_set.opts();
        new_opts.target[0] = new_target;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                new_opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                3,
                None,
            )
            .await?;

        volume.activate().await?;

        // Verify our volume still has the random data we wrote.
        let volume_size = volume.total_size().await? as usize;
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        // Clone an original downstairs directly to our new downstairs
        let clone_source = test_downstairs_set.downstairs1.repair_address();
        let mut new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            Some(clone_source),
        )
        .await
        .unwrap();

        // Restart the new downstairs read only.
        new_ds.reboot_read_only().await.unwrap();
        let new_target = new_ds.address();

        // Take our original opts, and replace a target with the downstairs
        // we just cloned.
        let mut new_opts = test_downstairs_set.opts();
        new_opts.target[0] = new_target;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                new_opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                3,
                None,
            )
            .await?;

        volume.activate().await?;

        // Verify our volume still has the random data we wrote.
        let volume_size = volume.total_size().await? as usize;
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_clone_sql() -> Result<()> {
        // Test downstairs region clone.
        // Create three downstairs with sql backend, write some data to them.
        // Restart them all read only.
        // Create a new downstairs.
        // Clone a read only downstairs to the new downstairs
        // Make a new volume with two old and the one new downstairs and verify
        // the original data we wrote.
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set =
            TestDownstairsSet::small_sqlite(false).await?;

        // This must be a SQLite extent!
        assert!(test_downstairs_set
            .downstairs1
            .tempdir
            .path()
            .join("00/000/000.db")
            .exists());

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await.unwrap();
        drop(volume);

        // Restart the original downstairs read only.
        test_downstairs_set.reboot_read_only().await?;

        // Make the new downstairs
        let mut new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        // Clone a read only downstairs to our new downstairs.
        let clone_source = test_downstairs_set.downstairs1.repair_address();
        new_ds.reboot_clone(clone_source).await.unwrap();

        new_ds.reboot_read_only().await.unwrap();
        let new_target = new_ds.address();

        // The cloned region should have the .db file
        assert!(new_ds.tempdir.path().join("00/000/000.db").exists());

        // Replace one of the targets in our original with the new downstairs.
        let mut new_opts = test_downstairs_set.opts();
        new_opts.target[0] = new_target;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                new_opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                3,
                None,
            )
            .await?;

        volume.activate().await?;

        // Verify our volume still has the random data we wrote.
        let volume_size = volume.total_size().await? as usize;
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_repair_ready_not_closed() -> Result<()> {
        // Create a new downstairs.
        // Verify repair ready returns false when an extent is open

        // Create a downstairs
        let new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,  // encrypted
            false, // read only
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        let repair_addr = new_ds.repair_address();

        let url = format!("http://{:?}", repair_addr);
        let repair_server = Client::new(&url);

        // Extent are open, so the repair ready request should return false.
        let rc = repair_server.extent_repair_ready(0).await;
        assert!(!rc.unwrap().into_inner());

        let rc = repair_server.extent_repair_ready(1).await;
        assert!(!rc.unwrap().into_inner());

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_repair_ready_read_only() -> Result<()> {
        // Create a new downstairs.
        // Verify repair ready returns true for read only downstairs,
        // even when extents are open.

        // Create a downstairs
        let new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true, // encrypted
            true, // read only
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        let repair_addr = new_ds.repair_address();

        let url = format!("http://{:?}", repair_addr);
        let repair_server = Client::new(&url);

        // Extent are not open, but the region is read only, so the requests
        // should all return true.
        let rc = repair_server.extent_repair_ready(0).await;
        assert!(rc.unwrap().into_inner());

        let rc = repair_server.extent_repair_ready(1).await;
        assert!(rc.unwrap().into_inner());

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_clone_diff_ec() -> Result<()> {
        // Test downstairs region clone.
        // Verify different extent count will fail.

        let mut ds_one = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            3,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        ds_one.reboot_read_only().await.unwrap();
        let clone_source = ds_one.repair_address();

        let mut ds_two = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2, // <- Different than above
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        assert!(ds_two.reboot_clone(clone_source).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_clone_diff_es() -> Result<()> {
        // Test downstairs region clone.
        // Verify different extent size will fail.

        let mut ds_one = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            9,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        ds_one.reboot_read_only().await.unwrap();
        let clone_source = ds_one.repair_address();

        let mut ds_two = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5, // <- Different than above
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        assert!(ds_two.reboot_clone(clone_source).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_clone_not_ro() -> Result<()> {
        // Test downstairs region clone.
        // Verify you can't clone from a RW downstairs

        let ds_one = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            false, // <- RO is false.
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        let clone_source = ds_one.repair_address();

        let mut ds_two = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        assert!(ds_two.reboot_clone(clone_source).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_clone_diff_encrypted() -> Result<()> {
        // Test downstairs region clone.
        // Verify downstairs encryption state must match.

        let ds_one = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            false,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        let clone_source = ds_one.repair_address();

        let mut ds_two = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true, // <- Encrypted is different
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        assert!(ds_two.reboot_clone(clone_source).await.is_err());
        Ok(())
    }

    async fn integration_test_volume_replace_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Replace a downstairs with a new one
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them
        let test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        // Create a new downstairs, then replace one of our current
        // downstairs with that new one.
        let new_downstairs = test_downstairs_set.new_downstairs().await?;

        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        // We can use the result from calling replace_downstairs to
        // intuit status on progress of the replacement.
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            match volume
                .replace_downstairs(
                    test_downstairs_set.opts().id,
                    test_downstairs_set.downstairs1_address(),
                    new_downstairs.address(),
                )
                .await
                .unwrap()
            {
                ReplaceResult::StartedAlready => {
                    println!("Waiting for replacement to finish");
                }
                ReplaceResult::CompletedAlready => {
                    println!("Downstairs replacement completed");
                    break;
                }
                x => {
                    panic!("Bad result from replace_downstairs: {:?}", x);
                }
            }
        }

        // Read back what we wrote.
        let volume_size = volume.total_size().await? as usize;
        assert_eq!(volume_size % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        Ok(())
    }

    async fn integration_test_volume_clone_replace_ro_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Replace a read only downstairs with a new one, that we cloned
        // from the original ro downstairs.
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them, then change to
        // read-only.
        let mut test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.deactivate().await.unwrap();

        // Restart the three downstairs in read only mode.
        test_downstairs_set.reboot_read_only().await?;

        // Restart our volume with the restarted read-only downstairs.
        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                2,
                None,
            )
            .await?;

        volume.activate().await?;

        // Make the new downstairs, but don't start it
        let mut new_ds = TestDownstairs::new(
            "127.0.0.1".parse().unwrap(),
            true,
            true,
            5,
            2,
            false,
            Backend::RawFile,
            None,
        )
        .await
        .unwrap();

        let clone_source = test_downstairs_set.downstairs1.repair_address();

        // Clone a read only downstairs into the new downstairs region.
        new_ds.reboot_clone(clone_source).await.unwrap();
        // Start the new downstairs read only.
        new_ds.reboot_read_only().await.unwrap();

        // Replace a downstairs in our RO volume with the new downstairs we
        // just cloned.
        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1.address(),
                new_ds.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        // We can use the result from calling replace_downstairs to
        // intuit status on progress of the replacement.
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            match volume
                .replace_downstairs(
                    test_downstairs_set.opts().id,
                    test_downstairs_set.downstairs1.address(),
                    new_ds.address(),
                )
                .await
                .unwrap()
            {
                ReplaceResult::StartedAlready => {
                    println!("Waiting for replacement to finish");
                }
                ReplaceResult::CompletedAlready => {
                    println!("Downstairs replacement completed");
                    break;
                }
                x => {
                    panic!("Bad result from replace_downstairs: {:?}", x);
                }
            }
        }

        // Read back what we wrote.
        let volume_size = volume.total_size().await? as usize;
        assert_eq!(volume_size % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        Ok(())
    }

    async fn integration_test_volume_replace_bad_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Attempt to replace a downstairs that we don't have.
        const BLOCK_SIZE: usize = 512;

        // Create three downstairs.
        let test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        // Create two new downstairs
        let new_downstairs = test_downstairs_set.new_downstairs().await?;
        let other_new_downstairs = test_downstairs_set.new_downstairs().await?;

        // Request to replace a downstairs but with a "source" downstairs
        // that we don't have.
        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                new_downstairs.address(),
                other_new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Missing);
        Ok(())
    }

    async fn integration_test_volume_inactive_replace_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Replace a downstairs before the volume is active.
        const BLOCK_SIZE: usize = 512;

        // Create three downstairs.
        let test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        // Create new downstairs
        let new_downstairs = test_downstairs_set.new_downstairs().await?;

        // Replace a downstairs.
        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);
        Ok(())
    }

    async fn integration_test_volume_twice_replace_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Replace a downstairs, then replace it again.
        const BLOCK_SIZE: usize = 512;

        // Create three downstairs.
        let test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;
        // Create new downstairs
        let new_downstairs = test_downstairs_set.new_downstairs().await?;

        // Replace a downstairs.
        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        // Replace the same downstairs again.
        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::StartedAlready);
        Ok(())
    }

    async fn integration_test_volume_replace_active(
        backend: Backend,
    ) -> Result<()> {
        // Attempt to replace a downstairs with one of our other
        // active downstairs.  This should return error as its is not
        // a legal replacement.
        const BLOCK_SIZE: usize = 512;

        // Create three downstairs.
        let test_downstairs_set =
            TestDownstairsSet::small(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        // Replace a downstairs with another active downstairs.
        // Expect an error back from this
        volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                test_downstairs_set.downstairs2_address(),
            )
            .await
            .unwrap_err();

        // Replace a downstairs with another active downstairs.
        // Same test as before, but with different order.
        volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs2_address(),
                test_downstairs_set.downstairs1_address(),
            )
            .await
            .unwrap_err();

        Ok(())
    }

    async fn integration_test_volume_replace_downstairs_then_takeover(
        backend: Backend,
    ) -> Result<()> {
        // This test is only valid for backends that will undergo replacement
        if backend == Backend::SQLite {
            return Ok(());
        }

        let log = csl();
        // Replace a downstairs with a new one which will kick off
        // LiveRepair. Then spin up a new Upstairs with a newer
        // generation number that will take over from the current
        // Upstairs that is in the middle of the LiveRepair.
        const BLOCK_SIZE: usize = 512;

        // boot three downstairs, write some data to them
        let test_downstairs_set =
            TestDownstairsSet::big(false, backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        let random_buffer = {
            let mut random_buffer =
                vec![0u8; volume.total_size().await? as usize];
            rand::thread_rng().fill(&mut random_buffer[..]);
            random_buffer
        };

        volume
            .write(BlockIndex(0), BytesMut::from(random_buffer.as_slice()))
            .await?;

        volume.flush(None).await?;

        // Create a new downstairs, then replace one of our current
        // downstairs with that new one.
        let new_downstairs = test_downstairs_set.new_downstairs().await?;

        let res = volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);
        match volume
            .replace_downstairs(
                test_downstairs_set.opts().id,
                test_downstairs_set.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap()
        {
            ReplaceResult::StartedAlready => {
                warn!(log, "Waited for some repair work, proceeding with test");
            }
            ReplaceResult::CompletedAlready => {
                // This test is invalid if the repair completed already
                panic!("Downstairs replacement completed");
            }
            x => {
                panic!("Bad result from replace_downstairs: {:?}", x);
            }
        }

        // A new Upstairs arrives, with a newer gen number, and the updated
        // target list
        let mut opts = test_downstairs_set.opts();
        opts.target = vec![
            new_downstairs.address(),
            test_downstairs_set.downstairs2_address(),
            test_downstairs_set.downstairs3_address(),
        ];

        let mut new_volume = Volume::new(BLOCK_SIZE as u64, csl());
        new_volume
            .add_subvolume_create_guest(
                opts,
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                2,
                None,
            )
            .await?;

        new_volume.activate().await?;

        while !new_volume.query_is_active().await? {
            // new_volume will repair before activating, so this waits for that
            warn!(log, "Waiting for new_volume activate");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        // Read back what we wrote.
        let volume_size = volume.total_size().await? as usize;
        assert_eq!(volume_size % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(volume_size / BLOCK_SIZE, BLOCK_SIZE);
        new_volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&buffer[BLOCK_SIZE..], &random_buffer[BLOCK_SIZE..]);

        Ok(())
    }

    async fn integration_test_problematic_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Make sure problematic downstairs don't cause problems Upstairs.
        const BLOCK_SIZE: usize = 512;

        // Create three problematic downstairs.
        let test_downstairs_set = TestDownstairsSet::problem(backend).await?;

        let mut volume = Volume::new(BLOCK_SIZE as u64, csl());
        volume
            .add_subvolume_create_guest(
                test_downstairs_set.opts(),
                volume::RegionExtentInfo {
                    block_size: BLOCK_SIZE as u64,
                    blocks_per_extent: test_downstairs_set.blocks_per_extent(),
                    extent_count: test_downstairs_set.extent_count(),
                },
                1,
                None,
            )
            .await?;

        volume.activate().await?;

        // Write three times, read three times, for a total of 150M
        let total_size = volume.total_size().await? as usize;
        const CHUNK_SIZE: usize = 1048576; // 1M

        for _ in 0..3 {
            let chunks: Vec<(usize, Vec<u8>)> = (0..total_size)
                .step_by(CHUNK_SIZE)
                .map(|i| {
                    (
                        i / 512, // block offset!
                        {
                            let mut random_buffer = vec![0u8; CHUNK_SIZE];
                            rand::thread_rng().fill(&mut random_buffer[..]);
                            random_buffer
                        },
                    )
                })
                .collect();

            for (i, random_buffer) in chunks.iter() {
                volume
                    .write(
                        BlockIndex(*i as u64),
                        BytesMut::from(random_buffer.as_slice()),
                    )
                    .await?;
            }

            for (i, random_buffer) in chunks {
                assert_eq!(CHUNK_SIZE % BLOCK_SIZE, 0);
                let mut buffer =
                    Buffer::new(CHUNK_SIZE / BLOCK_SIZE, BLOCK_SIZE);
                volume.read(BlockIndex(i as u64), &mut buffer).await?;

                assert_eq!(random_buffer, &buffer[..]);
            }
        }

        Ok(())
    }

    // The following tests work at the "guest" layer. The volume
    // layers above (in general) will eventually call a BlockIO trait
    // on a guest layer.

    // ZZZ Make a test of guest.activate_with_gen both fail and pass.
    // Maybe in a different place?  We need downstairs to do this.
    async fn integration_test_guest_downstairs(backend: Backend) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write and read work as expected
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Verify contents are zero on init
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Write data in
        guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    /// Drop the guest right away and confirm that the worker thread stops
    async fn integration_test_guest_drop_early(backend: Backend) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let join_handle = up_main(opts, 1, None, io, None)?;

        drop(guest);

        let r = join_handle.await;
        assert!(r.is_ok());
        Ok(())
    }

    /// Same as `integration_test_guest_downstairs`, but dropping the Guest at
    /// the end and confirming that the worker thread stops
    async fn integration_test_guest_drop(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Verify contents are zero on init
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x00_u8; BLOCK_SIZE * 10], buffer.to_vec());

        // Write data in
        guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], buffer.to_vec());
        drop(guest);

        let r = join_handle.await;
        assert!(r.is_ok());
        Ok(())
    }

    async fn integration_test_guest_zero_length_io(
        backend: Backend,
    ) -> Result<()> {
        // Test the guest layer with a write and read of zero length
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Read of length 0
        let mut buffer = Buffer::new(0, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Write of length 0
        guest.write(BlockIndex(0), BytesMut::new()).await?;

        Ok(())
    }

    async fn integration_test_guest_replace_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify we can replace
        // a downstairs
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let log = csl();
        let (guest, io) = Guest::new(Some(log.clone()));
        let _jh = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;

        // Write data in
        guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Create new downstairs
        let new_downstairs = tds.new_downstairs().await?;

        // Replace a downstairs.
        let res = guest
            .replace_downstairs(
                tds.opts().id,
                tds.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        // We can use the result from calling replace_downstairs to
        // intuit status on progress of the replacement.
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            match guest
                .replace_downstairs(
                    tds.opts().id,
                    tds.downstairs1_address(),
                    new_downstairs.address(),
                )
                .await
                .unwrap()
            {
                ReplaceResult::StartedAlready => {
                    info!(log, "Waiting for replacement to finish");
                }
                ReplaceResult::CompletedAlready => {
                    info!(log, "Downstairs replacement completed");
                    break;
                }
                x => {
                    panic!("Bad result from replace_downstairs: {:?}", x);
                }
            }
        }

        // Read back our block post replacement, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_guest_replace_ds_before_active(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify we can replace a downstairs
        // before the upstairs is active.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let log = csl();
        let (guest, io) = Guest::new(Some(log.clone()));
        let _jh = up_main(opts, 1, None, io, None)?;

        // Create new downstairs
        let new_downstairs = tds.new_downstairs().await?;

        // Replace a downstairs.
        let res = guest
            .replace_downstairs(
                tds.opts().id,
                tds.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        guest.activate().await?;

        // Write data in
        guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read back our block post replacement, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_upstairs_read_only_rejects_write(
        backend: Backend,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin up three read-only downstairs
        let tds = TestDownstairsSet::small(true, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        // Read-only Upstairs should return errors if writes are attempted.
        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;

        // Expect an error attempting to write.
        let write_result = guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await;
        assert!(write_result.is_err());
        assert!(matches!(
            write_result.err().unwrap(),
            CrucibleError::ModifyingReadOnlyRegion
        ));

        Ok(())
    }

    async fn integration_test_guest_replace_many_downstairs(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify we can replace one
        // downstairs, but not another while the replace is active.

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let log = csl();
        let (guest, io) = Guest::new(Some(log.clone()));
        let _jh = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;

        // Create new downstairs
        let new_downstairs = tds.new_downstairs().await?;
        let another_new_downstairs = tds.new_downstairs().await?;

        // Replace a downstairs.
        let res = guest
            .replace_downstairs(
                tds.opts().id,
                tds.downstairs1_address(),
                new_downstairs.address(),
            )
            .await
            .unwrap();

        assert_eq!(res, ReplaceResult::Started);

        // Replace a second downstairs before our first has finished.
        // This should return error.
        guest
            .replace_downstairs(
                tds.opts().id,
                tds.downstairs2_address(),
                another_new_downstairs.address(),
            )
            .await
            .unwrap_err();

        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected
        // The size here spans two extents.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Write_unwritten again with different data
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read back the same blocks.
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Now, just write.  This should update our data.
        guest
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x89; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read back the same blocks.
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Verify data is now from the new write.
        assert_eq!(vec![0x89_u8; BLOCK_SIZE * 10], &buffer[..]);

        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten_sparse_1(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the first block
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Write_unwritten again with different data and same start
        // range, but write to blocks 2 and 3 this time as well.
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 3].as_slice()),
            )
            .await?;

        // Read back the first block.
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], &buffer[..]);

        // Read back the next two blocks.
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        guest.read(BlockIndex(1), &mut buffer).await?;

        // Verify data is still the original contents.
        assert_eq!(vec![0x99_u8; BLOCK_SIZE * 2], &buffer[..]);

        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten_sparse_mid(
        backend: Backend,
    ) -> Result<()> {
        // Test using the guest layer to verify a new region is
        // what we expect, and a write_unwritten and read work as expected,
        // this time with sparse writes where the middle block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the second block
        guest
            .write_unwritten(
                BlockIndex(1),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 3].as_slice()),
            )
            .await?;

        // Read back the all three blocks.
        let mut buffer = Buffer::new(3, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in the first block is from the second write_unwritten
        assert_eq!(vec![0x99_u8; BLOCK_SIZE], &buffer[0..BLOCK_SIZE]);

        // Verify data in the second block is from the first write_unwritten
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE],
            &buffer[BLOCK_SIZE..(BLOCK_SIZE * 2)]
        );

        // Verify data in the third block is from the second write_unwritten
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            &buffer[(BLOCK_SIZE * 2)..(BLOCK_SIZE * 2 + BLOCK_SIZE)]
        );
        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten_sparse_end(
        backend: Backend,
    ) -> Result<()> {
        // Test write_unwritten and read work as expected,
        // this time with sparse writes where the last block is written
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in the third block
        guest
            .write_unwritten(
                BlockIndex(2),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Write_unwritten again with different data and writing
        // to blocks 0, 1, and 2.
        guest
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 3].as_slice()),
            )
            .await?;

        // Read back the all three blocks.
        let mut buffer = Buffer::new(3, BLOCK_SIZE);
        guest.read(BlockIndex(0), &mut buffer).await?;

        // Verify data in the first two blocks is the data from the
        // second write_unwritten
        assert_eq!(vec![0x99_u8; BLOCK_SIZE * 2], &buffer[0..BLOCK_SIZE * 2]);

        // Verify data in the third block is from the first write_unwritten
        assert_eq!(
            vec![0x55_u8; BLOCK_SIZE],
            &buffer[(BLOCK_SIZE * 2)..(BLOCK_SIZE * 2 + BLOCK_SIZE)]
        );

        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten_span(
        backend: Backend,
    ) -> Result<()> {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                BlockIndex(4),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                BlockIndex(4),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Read back both blocks
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        guest.read(BlockIndex(4), &mut buffer).await?;

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], &buffer[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            &buffer[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }

    async fn integration_test_guest_downstairs_unwritten_span_2(
        backend: Backend,
    ) -> Result<()> {
        // Test write_unwritten and read work as expected,
        // Have the IO span an extent boundary.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await?;
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None)?;

        guest.activate().await?;
        guest.query_work_queue().await?;

        // Write_unwritten data in last block of the extent
        guest
            .write_unwritten(
                BlockIndex(4),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await?;

        // Write_unwritten again with different data and a larger
        // write size to include the first block in the 2nd extent.
        guest
            .write_unwritten(
                BlockIndex(4),
                BytesMut::from(vec![0x99; BLOCK_SIZE * 2].as_slice()),
            )
            .await?;

        // Read back both blocks
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        guest.read(BlockIndex(4), &mut buffer).await?;

        // Verify data in the first block is the data from the first write.
        assert_eq!(vec![0x55_u8; BLOCK_SIZE], &buffer[0..BLOCK_SIZE]);

        // Verify data in the second block is from the 2nd write
        assert_eq!(
            vec![0x99_u8; BLOCK_SIZE],
            &buffer[(BLOCK_SIZE)..(BLOCK_SIZE * 2)]
        );

        Ok(())
    }

    async fn integration_test_io_out_of_range(backend: Backend) -> Result<()> {
        // Test reads and writes outside the valid region return error.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None).unwrap();

        guest.activate().await.unwrap();

        // Write a block past the end of the extent
        let res = guest
            .write(
                BlockIndex(11),
                BytesMut::from(vec![0x55; BLOCK_SIZE].as_slice()),
            )
            .await;

        assert!(res.is_err());

        // Read a block past the end of the extent
        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        let res = guest.read(BlockIndex(11), &mut buffer).await;

        assert!(res.is_err());
        Ok(())
    }

    async fn integration_test_io_span_out_of_range(
        backend: Backend,
    ) -> Result<()> {
        // Test reads and writes that start inside and extend
        // past the end of the region will return error.
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();

        let (guest, io) = Guest::new(None);

        let _join_handle = up_main(opts, 1, None, io, None).unwrap();

        guest.activate().await.unwrap();

        // Write a block with a buffer that extends past the end of the region
        let res = guest
            .write(
                BlockIndex(10),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 2].as_slice()),
            )
            .await;

        assert!(res.is_err());

        // Read a block with buffer that extends past the end of the region
        let mut buffer = Buffer::new(2, BLOCK_SIZE);
        let res = guest.read(BlockIndex(10), &mut buffer).await;

        assert!(res.is_err());
        Ok(())
    }

    // The following tests are for the Pantry

    /// Given a &TestDownstairsSet, spawn a Pantry, attach a
    /// CruciblePantryClient, and return both plus the Volume ID.
    async fn get_pantry_and_client_for_tds(
        tds: &TestDownstairsSet,
    ) -> (Arc<Pantry>, Uuid, CruciblePantryClient) {
        const BLOCK_SIZE: usize = 512;

        // Start a new pantry

        let (log, pantry) = crucible_pantry::initialize_pantry().unwrap();
        let (pantry_addr, _join_handle) = crucible_pantry::server::run_server(
            &log,
            "127.0.0.1:0".parse().unwrap(),
            &pantry,
        )
        .unwrap();

        // Create a Volume out of it, and attach a CruciblePantryClient

        let volume_id = Uuid::new_v4();
        let opts = tds.opts();

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 1,
            }],
            read_only_parent: None,
        };

        let client =
            CruciblePantryClient::new(&format!("http://{}", pantry_addr));

        client
            .attach(
                &volume_id.to_string(),
                &crucible_pantry_client::types::AttachRequest {
                    // the type here is
                    // crucible_pantry_client::types::VolumeConstructionRequest,
                    // not
                    // crucible::VolumeConstructionRequest, but they are the
                    // same thing! take a trip through JSON
                    // to get to the right type
                    volume_construction_request: serde_json::from_str(
                        &serde_json::to_string(&vcr).unwrap(),
                    )
                    .unwrap(),
                },
            )
            .await
            .unwrap();

        (pantry, volume_id, client)
    }

    async fn test_pantry_import_from_url_ovmf(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::big(false, backend).await.unwrap();
        let opts = tds.opts();

        // Start a pantry, and get the client for it
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        let base_url = "https://oxide-omicron-build.s3.amazonaws.com";
        let url = format!("{}/OVMF_CODE_20220922.fd", base_url);

        let sha256_digest =
            "319d678f093c43502ca360911d52b475dea7fa6dcd962150c84fff18f5b32221";

        let response = client
            .import_from_url(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ImportFromUrlRequest {
                    url: url.clone(),
                    expected_digest: Some(
                        crucible_pantry_client::types::ExpectedDigest::Sha256(
                            sha256_digest.to_string(),
                        ),
                    ),
                },
            )
            .await
            .unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let result = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(result.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();

        // read the data to verify import

        let dur = std::time::Duration::from_secs(25);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();

        let bytes = client
            .get(&url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 2,
            }],
            read_only_parent: None,
        };
        let volume = Volume::construct(vcr, None, csl()).await.unwrap();
        volume.activate().await.unwrap();

        assert_eq!(bytes.len() % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(bytes.len() / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert!(bytes.len() % BLOCK_SIZE == 0);

        for i in (0..bytes.len()).step_by(BLOCK_SIZE) {
            let start = i;
            let end = i + BLOCK_SIZE;
            assert_eq!(&bytes[..][start..end], &buffer[start..end]);
            eprintln!("{} {} ok", start, end);
        }
        Ok(())
    }

    async fn test_pantry_import_from_url_ovmf_bad_digest(
        backend: Backend,
    ) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::big(false, backend).await.unwrap();

        // Start a pantry, and get the client for it
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        let base_url = "https://oxide-omicron-build.s3.amazonaws.com";

        // not the actual digest!
        let sha256_digest =
            "00000000000000000000000000000000000000000000000000000000f5b32221";

        let response = client
            .import_from_url(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ImportFromUrlRequest {
                    url: format!("{}/OVMF_CODE_20220922.fd", base_url),
                    expected_digest: Some(
                        crucible_pantry_client::types::ExpectedDigest::Sha256(
                            sha256_digest.to_string(),
                        ),
                    ),
                },
            )
            .await
            .unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let result = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(!result.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_pantry_import_from_local_server(
        backend: Backend,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 512;
        let log = csl();

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/img.raw"))
                .times(1..)
                .respond_with(status_code(200).body(vec![0x55; 5120])),
        );
        server.expect(
            Expectation::matching(request::method_path("HEAD", "/img.raw"))
                .times(1..)
                .respond_with(
                    status_code(200)
                        .append_header("Content-Length", format!("{}", 5120)),
                ),
        );

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();

        let volume_id = Uuid::new_v4();
        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 1,
            }],
            read_only_parent: None,
        };

        // Verify contents are zero on init
        {
            let volume = Volume::construct(vcr.clone(), None, log.clone())
                .await
                .unwrap();
            volume.activate().await.unwrap();

            let mut buffer = Buffer::new(10, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await.unwrap();

            assert_eq!(vec![0x00; 5120], &buffer[..]);

            volume.deactivate().await.unwrap();

            drop(volume);
        }

        // Start a pantry, and get the client for it, then use it to import img.raw
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        let response = client
            .import_from_url(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ImportFromUrlRequest {
                    url: server.url("/img.raw").to_string(),
                    expected_digest: None,
                },
            )
            .await
            .unwrap();

        // Test not polling here
        let result = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(result.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();

        // Attach, validate img.raw got imported

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 3,
            }],
            read_only_parent: None,
        };
        let volume = Volume::construct(vcr, None, log.clone()).await.unwrap();
        volume.activate().await.unwrap();

        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x55; 5120], &buffer[..]);
        Ok(())
    }

    async fn test_pantry_snapshot(backend: Backend) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.
        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to snapshot
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        client
            .snapshot(
                &volume_id.to_string(),
                &crucible_pantry_client::types::SnapshotRequest {
                    snapshot_id: "testpost".to_string(),
                },
            )
            .await
            .unwrap();

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_pantry_bulk_write(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();

        // Start a pantry, get the client for it, then use it to bulk_write in data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        for i in 0..10 {
            client
                .bulk_write(
                    &volume_id.to_string(),
                    &crucible_pantry_client::types::BulkWriteRequest {
                        offset: i * 512,
                        base64_encoded_data: engine::general_purpose::STANDARD
                            .encode(vec![i as u8; 512]),
                    },
                )
                .await
                .unwrap();
        }

        client.detach(&volume_id.to_string()).await.unwrap();

        // Attach, validate bulk write worked

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 2,
            }],
            read_only_parent: None,
        };
        let volume = Volume::construct(vcr, None, csl()).await.unwrap();
        volume.activate().await.unwrap();

        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        let buffer_data = &&buffer[..];

        for i in 0..10 {
            let start = i * 512;
            let end = (i + 1) * 512;
            assert_eq!(vec![i as u8; 512], buffer_data[start..end]);
        }
        Ok(())
    }

    async fn test_pantry_bulk_write_max_chunk_size(
        backend: Backend,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::big(false, backend).await.unwrap();
        let opts = tds.opts();

        // Start a pantry, get the client for it, then use it to bulk_write in data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        let base64_encoded_data = engine::general_purpose::STANDARD.encode(
            vec![0x99; crucible_pantry::pantry::PantryEntry::MAX_CHUNK_SIZE],
        );

        client
            .bulk_write(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkWriteRequest {
                    offset: 0,
                    base64_encoded_data,
                },
            )
            .await
            .unwrap();

        client.detach(&volume_id.to_string()).await.unwrap();

        // Attach, validate bulk write worked

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 2,
            }],
            read_only_parent: None,
        };
        let volume = Volume::construct(vcr, None, csl()).await.unwrap();
        volume.activate().await.unwrap();

        use crucible_pantry::pantry::PantryEntry;
        assert_eq!(PantryEntry::MAX_CHUNK_SIZE % BLOCK_SIZE, 0);
        let mut buffer =
            Buffer::new(PantryEntry::MAX_CHUNK_SIZE / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x99; PantryEntry::MAX_CHUNK_SIZE], &buffer[..]);
        Ok(())
    }

    /// Assert that the Pantry will fail for non-block sized writes
    async fn test_pantry_fail_bulk_write_one_byte(
        backend: Backend,
    ) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_write in data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // The Pantry should reject the one byte write
        let e = client
            .bulk_write(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkWriteRequest {
                    offset: 0,
                    base64_encoded_data: engine::general_purpose::STANDARD
                        .encode(vec![123; 1]),
                },
            )
            .await
            .unwrap_err();

        let crucible_pantry_client::Error::ErrorResponse(e) = e else {
            panic!("expected ErrorResponse, got {e}");
        };
        assert_eq!(e.status(), reqwest::StatusCode::BAD_REQUEST);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    /// Assert that the Pantry will fail for non-block sized reads
    async fn test_pantry_fail_bulk_read_one_byte(
        backend: Backend,
    ) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_read data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // The Pantry should reject the one byte write
        let e = client
            .bulk_read(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkReadRequest {
                    offset: 0,
                    size: 1,
                },
            )
            .await
            .unwrap_err();

        let crucible_pantry_client::Error::ErrorResponse(e) = e else {
            panic!("expected ErrorResponse, got {e}");
        };
        assert_eq!(e.status(), reqwest::StatusCode::BAD_REQUEST);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_pantry_scrub(backend: Backend) -> Result<()> {
        // Test scrubbing the OVMF image from a URL
        // XXX httptest::Server does not support range requests, otherwise that
        // should be used here instead.

        let base_url = "https://oxide-omicron-build.s3.amazonaws.com";
        let url = format!("{}/OVMF_CODE_20220922.fd", base_url);

        let data = {
            let dur = std::time::Duration::from_secs(25);
            let client = reqwest::ClientBuilder::new()
                .connect_timeout(dur)
                .timeout(dur)
                .build()
                .unwrap();

            client
                .get(&url)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        };

        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct (with a
        // read-only parent pointing to the random data above)

        let tds = TestDownstairsSet::big(false, backend).await.unwrap();
        let opts = tds.opts();

        let volume_id = Uuid::new_v4();
        let rop_id = Uuid::new_v4();
        let read_only_parent = Some(Box::new(VolumeConstructionRequest::Url {
            id: rop_id,
            block_size: BLOCK_SIZE as u64,
            url: url.clone(),
        }));

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 1,
            }],
            read_only_parent: read_only_parent.clone(),
        };

        // Verify contents match data on init
        {
            let volume = Volume::construct(vcr, None, csl()).await.unwrap();
            volume.activate().await.unwrap();

            assert_eq!(data.len() % BLOCK_SIZE, 0);
            let mut buffer = Buffer::new(data.len() / BLOCK_SIZE, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await.unwrap();

            assert_eq!(data, &buffer[..]);

            volume.deactivate().await.unwrap();

            drop(volume);
        }

        // Start the pantry, then use it to scrub

        let (log, pantry) = crucible_pantry::initialize_pantry().unwrap();
        let (pantry_addr, _join_handle) = crucible_pantry::server::run_server(
            &log,
            "127.0.0.1:0".parse().unwrap(),
            &pantry,
        )
        .unwrap();

        let client =
            CruciblePantryClient::new(&format!("http://{}", pantry_addr));

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 2,
            }],
            read_only_parent,
        };
        client
            .attach(
                &volume_id.to_string(),
                &crucible_pantry_client::types::AttachRequest {
                    // the type here is
                    // crucible_pantry_client::types::VolumeConstructionRequest,
                    // not
                    // crucible::VolumeConstructionRequest, but they are the
                    // same thing! take a trip through JSON
                    // to get to the right type
                    volume_construction_request: serde_json::from_str(
                        &serde_json::to_string(&vcr).unwrap(),
                    )
                    .unwrap(),
                },
            )
            .await
            .unwrap();

        let response = client.scrub(&volume_id.to_string()).await.unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let result = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(result.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();

        // Drop the read only parent from the volume construction request

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 3,
            }],
            read_only_parent: None,
        };

        // Attach, validate random data got imported

        let volume = Volume::construct(vcr, None, log.clone()).await.unwrap();
        volume.activate().await.unwrap();

        assert_eq!(data.len() % BLOCK_SIZE, 0);
        let mut buffer = Buffer::new(data.len() / BLOCK_SIZE, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(data, &buffer[..]);
        Ok(())
    }

    async fn test_pantry_bulk_read(backend: Backend) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_write then
        // bulk_read in data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // first, bulk_write some data in

        for i in 0..10 {
            client
                .bulk_write(
                    &volume_id.to_string(),
                    &crucible_pantry_client::types::BulkWriteRequest {
                        offset: i * 512,
                        base64_encoded_data: engine::general_purpose::STANDARD
                            .encode(vec![i as u8; 512]),
                    },
                )
                .await
                .unwrap();
        }

        // then bulk_read it out

        for i in 0..10 {
            let data = client
                .bulk_read(
                    &volume_id.to_string(),
                    &crucible_pantry_client::types::BulkReadRequest {
                        offset: i * 512,
                        size: 512,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                data.base64_encoded_data,
                engine::general_purpose::STANDARD.encode(vec![i as u8; 512]),
            );
        }

        // perform one giant bulk_read

        let data = client
            .bulk_read(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkReadRequest {
                    offset: 0,
                    size: 512 * 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            data.base64_encoded_data,
            engine::general_purpose::STANDARD.encode(
                [
                    vec![0_u8; 512],
                    vec![1_u8; 512],
                    vec![2_u8; 512],
                    vec![3_u8; 512],
                    vec![4_u8; 512],
                    vec![5_u8; 512],
                    vec![6_u8; 512],
                    vec![7_u8; 512],
                    vec![8_u8; 512],
                    vec![9_u8; 512],
                ]
                .concat()
            ),
        );

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_pantry_bulk_read_max_chunk_size(
        backend: Backend,
    ) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::big(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_write in
        // data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // bulk write in a bunch of data

        let base64_encoded_data = engine::general_purpose::STANDARD.encode(
            vec![0x99; crucible_pantry::pantry::PantryEntry::MAX_CHUNK_SIZE],
        );

        client
            .bulk_write(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkWriteRequest {
                    offset: 0,
                    base64_encoded_data,
                },
            )
            .await
            .unwrap();

        // then, bulk read it out

        let data = client
            .bulk_read(
                &volume_id.to_string(),
                &crucible_pantry_client::types::BulkReadRequest {
                    offset: 0,
                    size: crucible_pantry::pantry::PantryEntry::MAX_CHUNK_SIZE
                        as u32,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            data.base64_encoded_data,
            engine::general_purpose::STANDARD.encode(vec![
                    0x99;
                    crucible_pantry::pantry::PantryEntry::MAX_CHUNK_SIZE
                ],),
        );

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_pantry_validate(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_write in
        // data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // parallel bulk write in a bunch of random data in a random order

        const THREADS: usize = 10;
        const CHUNK_SIZE: usize = 512;

        let total_size = tds.blocks_per_extent() as usize
            * tds.extent_count() as usize
            * BLOCK_SIZE;

        assert_eq!(total_size % CHUNK_SIZE, 0);
        assert_eq!((total_size / CHUNK_SIZE) % THREADS, 0);

        let mut handles = Vec::with_capacity(THREADS);
        let mut senders = Vec::with_capacity(THREADS);

        for _i in 0..THREADS {
            let (tx, mut rx) = mpsc::channel(100);
            let client = client.clone();

            handles.push(tokio::spawn(async move {
                while let Some((offset, base64_encoded_data)) = rx.recv().await
                {
                    client
                        .bulk_write(
                            &volume_id.to_string(),
                            &crucible_pantry_client::types::BulkWriteRequest {
                                offset,
                                base64_encoded_data,
                            },
                        )
                        .await
                        .unwrap();
                }
            }));

            senders.push(tx);
        }

        for i in 0..(total_size / CHUNK_SIZE) {
            let mut data = vec![0u8; CHUNK_SIZE];
            rand::thread_rng().fill(&mut data[..]);
            let base64_encoded_data =
                engine::general_purpose::STANDARD.encode(&data);

            senders[i % THREADS]
                .send(((i * CHUNK_SIZE) as u64, base64_encoded_data))
                .await
                .unwrap();
        }

        for sender in senders {
            drop(sender);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // then, bulk read it out
        let mut buffer = Vec::with_capacity(total_size);

        for i in 0..(total_size / CHUNK_SIZE) {
            let response = client
                .bulk_read(
                    &volume_id.to_string(),
                    &crucible_pantry_client::types::BulkReadRequest {
                        offset: (i * CHUNK_SIZE) as u64,
                        size: CHUNK_SIZE as u32,
                    },
                )
                .await
                .unwrap();

            let mut data = engine::general_purpose::STANDARD
                .decode(&response.base64_encoded_data)
                .unwrap();

            buffer.append(&mut data);
        }

        // sha256 here, then send digest to validate function
        let mut hasher = sha2::Sha256::new();
        hasher.update(&*buffer.to_vec());
        let digest = hex::encode(hasher.finalize());

        let response = client
            .validate(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ValidateRequest {
                    expected_digest:
                        crucible_pantry_client::types::ExpectedDigest::Sha256(
                            digest.to_string(),
                        ),

                    // validate the whole volume's contents
                    size_to_validate: None,
                },
            )
            .await
            .unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let response = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(response.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    // Test validating a subset of the beginning of the volume
    async fn test_pantry_validate_subset(backend: Backend) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it, then use it to bulk_write in
        // data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // parallel bulk write in a bunch of random data in a random order

        const THREADS: usize = 10;
        const CHUNK_SIZE: usize = 512;

        let total_size = tds.blocks_per_extent() as usize
            * tds.extent_count() as usize
            * BLOCK_SIZE;

        assert_eq!(total_size % CHUNK_SIZE, 0);
        assert_eq!((total_size / CHUNK_SIZE) % THREADS, 0);

        let mut handles = Vec::with_capacity(THREADS);
        let mut senders = Vec::with_capacity(THREADS);

        for _i in 0..THREADS {
            let (tx, mut rx) = mpsc::channel(100);
            let client = client.clone();

            handles.push(tokio::spawn(async move {
                while let Some((offset, base64_encoded_data)) = rx.recv().await
                {
                    client
                        .bulk_write(
                            &volume_id.to_string(),
                            &crucible_pantry_client::types::BulkWriteRequest {
                                offset,
                                base64_encoded_data,
                            },
                        )
                        .await
                        .unwrap();
                }
            }));

            senders.push(tx);
        }

        for i in 0..(total_size / CHUNK_SIZE) {
            let mut data = vec![0u8; CHUNK_SIZE];
            rand::thread_rng().fill(&mut data[..]);
            let base64_encoded_data =
                engine::general_purpose::STANDARD.encode(&data);

            senders[i % THREADS]
                .send(((i * CHUNK_SIZE) as u64, base64_encoded_data))
                .await
                .unwrap();
        }

        for sender in senders {
            drop(sender);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // then, bulk read it out
        let mut buffer = Vec::with_capacity(total_size);

        for i in 0..(total_size / CHUNK_SIZE) {
            let response = client
                .bulk_read(
                    &volume_id.to_string(),
                    &crucible_pantry_client::types::BulkReadRequest {
                        offset: (i * CHUNK_SIZE) as u64,
                        size: CHUNK_SIZE as u32,
                    },
                )
                .await
                .unwrap();

            let mut data = engine::general_purpose::STANDARD
                .decode(&response.base64_encoded_data)
                .unwrap();

            buffer.append(&mut data);
        }

        // sha256 here, then send digest to validate function
        // only send half the bytes to the hasher
        let mut hasher = sha2::Sha256::new();
        hasher.update(&buffer.to_vec()[0..(total_size / 2)]);
        let digest = hex::encode(hasher.finalize());

        let response = client
            .validate(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ValidateRequest {
                    expected_digest:
                        crucible_pantry_client::types::ExpectedDigest::Sha256(
                            digest.to_string(),
                        ),

                    // validate half the volume
                    size_to_validate: Some(total_size as u64 / 2),
                },
            )
            .await
            .unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let response = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(response.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    // Test validating a non-block size amount fails
    async fn test_pantry_validate_fail(backend: Backend) -> Result<()> {
        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::small(false, backend).await.unwrap();

        // Start a pantry, get the client for it
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        let response = client
            .validate(
                &volume_id.to_string(),
                &crucible_pantry_client::types::ValidateRequest {
                    expected_digest:
                        crucible_pantry_client::types::ExpectedDigest::Sha256(
                            "wrong size".to_string(),
                        ),

                    // validate 100 bytes!
                    size_to_validate: Some(100),
                },
            )
            .await
            .unwrap();

        while !client
            .is_job_finished(&response.job_id)
            .await
            .unwrap()
            .job_is_finished
        {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let response = client.job_result_ok(&response.job_id).await.unwrap();
        assert!(!response.job_result_ok);

        client.detach(&volume_id.to_string()).await.unwrap();
        Ok(())
    }

    async fn test_volume_replace_vcr(backend: Backend) -> Result<()> {
        // Test of a replacement of a downstairs given two
        // VolumeConstructionRequests.
        // We create a volume, write some data to it, then replace a downstairs
        // in that volume.  After replacement, we verify we can read back the
        // data.
        const BLOCK_SIZE: usize = 512;
        let log = csl();

        info!(log, "test_volume_replace of a volume");
        // Make three downstairs
        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();
        let volume_id = Uuid::new_v4();

        let original = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 2,
            }],
            read_only_parent: None,
        };

        let volume = Volume::construct(original.clone(), None, log.clone())
            .await
            .unwrap();
        volume.activate().await.unwrap();

        // Write data in
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await
            .unwrap();

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);

        // Make one new downstairs
        let new_downstairs = tds.new_downstairs().await.unwrap();
        info!(log, "A New downstairs: {:?}", new_downstairs.address());

        let mut new_opts = tds.opts().clone();
        new_opts.target[0] = new_downstairs.address();
        info!(log, "Old ops target: {:?}", opts.target);
        info!(log, "New ops target: {:?}", new_opts.target);

        // Our "new" VCR must have a new downstairs in the opts, and have
        // the generation number be larger than the original.
        let replacement = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: new_opts.clone(),
                gen: 3,
            }],
            read_only_parent: None,
        };

        info!(log, "Replace VCR now: {:?}", replacement);
        volume.target_replace(original, replacement).await.unwrap();
        info!(log, "send read now");
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);
        Ok(())
    }

    async fn test_volume_replace_rop_in_vcr(backend: Backend) -> Result<()> {
        // Verify that we can do replacement of a read_only_parent target
        // for a volume.
        const BLOCK_SIZE: usize = 512;
        let log = csl();

        // Make three downstairs
        let tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let opts = tds.opts();
        let volume_id = Uuid::new_v4();

        let original = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts: opts.clone(),
                gen: 1,
            }],
            read_only_parent: None,
        };

        let volume = Volume::construct(original.clone(), None, log.clone())
            .await
            .unwrap();
        volume.activate().await.unwrap();

        // Write data in
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await
            .unwrap();

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);
        volume.deactivate().await.unwrap();

        drop(volume);

        // Make a new volume, and make the original volume the read only
        // parent.
        // Make three new downstairs for the new volume.
        let sv_tds = TestDownstairsSet::small(false, backend).await.unwrap();
        let sv_opts = sv_tds.opts();
        let sv_volume_id = Uuid::new_v4();

        // Use the original opts above, but now for the ROP.
        let rop = Box::new(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds.blocks_per_extent(),
            extent_count: tds.extent_count(),
            opts: opts.clone(),
            gen: 2,
        });

        let new_sub_vol = vec![VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: sv_tds.blocks_per_extent(),
            extent_count: sv_tds.extent_count(),
            opts: sv_opts.clone(),
            gen: 1,
        }];

        let new_vol = VolumeConstructionRequest::Volume {
            id: sv_volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: new_sub_vol.clone(),
            read_only_parent: Some(rop),
        };

        // Make our new volume that has a read_only_parent.
        let volume = Volume::construct(new_vol.clone(), None, log.clone())
            .await
            .unwrap();
        volume.activate().await.unwrap();

        // Make one new downstairs
        let new_downstairs = tds.new_downstairs().await.unwrap();
        info!(log, "A New downstairs: {:?}", new_downstairs.address());

        let mut new_opts = tds.opts().clone();
        new_opts.target[0] = new_downstairs.address();
        info!(log, "Old ops target: {:?}", opts.target);
        info!(log, "New ops target: {:?}", new_opts.target);
        let new_rop = Box::new(VolumeConstructionRequest::Region {
            block_size: BLOCK_SIZE as u64,
            blocks_per_extent: tds.blocks_per_extent(),
            extent_count: tds.extent_count(),
            opts: new_opts.clone(),
            gen: 3,
        });

        // Our "new" VCR must have a new downstairs in the opts, and have
        // the generation number be larger than the original.
        let replacement = VolumeConstructionRequest::Volume {
            id: sv_volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: new_sub_vol.clone(),
            read_only_parent: Some(new_rop),
        };

        info!(log, "Replace VCR now: {:?}", replacement);
        let rop_replace = volume.target_replace(new_vol, replacement).await;
        info!(log, "Replace VCR returns: {:?}", rop_replace);
        assert!(rop_replace.is_ok());
        info!(log, "send read now");
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x55_u8; BLOCK_SIZE * 10], &buffer[..]);
        Ok(())
    }

    /// Getting a volume's status should work even if something else took over
    async fn test_pantry_get_status_after_activation(
        backend: Backend,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Spin off three downstairs, build our Crucible struct.

        let tds = TestDownstairsSet::big(false, backend).await.unwrap();
        let opts = tds.opts();

        // Start a pantry, get the client for it, then use it to bulk_write
        // in data
        let (_pantry, volume_id, client) =
            get_pantry_and_client_for_tds(&tds).await;

        // volume status should return active = true, and seen_active = true.

        let status =
            client.volume_status(&volume_id.to_string()).await.unwrap();

        assert!(matches!(
            status.into_inner(),
            crucible_pantry_client::types::VolumeStatus {
                active: true,
                seen_active: true,
                num_job_handles: 0,
            }
        ));

        // Take over the Volume activation here

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: BLOCK_SIZE as u64,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: BLOCK_SIZE as u64,
                blocks_per_extent: tds.blocks_per_extent(),
                extent_count: tds.extent_count(),
                opts,
                gen: 2,
            }],
            read_only_parent: None,
        };

        let log = csl();
        let volume = Volume::construct(vcr, None, log.clone()).await.unwrap();
        volume.activate().await.unwrap();

        // volume status should return active = false, and seen_active = true.

        let status =
            client.volume_status(&volume_id.to_string()).await.unwrap();

        assert!(matches!(
            status.into_inner(),
            crucible_pantry_client::types::VolumeStatus {
                active: false,
                seen_active: true,
                num_job_handles: 0,
            }
        ));
        Ok(())
    }

    /// Macro defining the full integration test suite
    ///
    /// Functions in the test suite should take a `b: Backend` parameter and
    /// return a `Result<()>`, which is unwrapped by the generated test function
    ///
    /// Add new functions here to ensure that they're tested for every backend!
    macro_rules! integration_test_suite {
        ($b:ident) => {
            integration_test_suite!(
                $b,
                integration_test_region,
                integration_test_region_huge_io,
                volume_zero_length_io,
                integration_test_two_layers,
                integration_test_two_layers_write_unwritten,
                integration_test_three_layers,
                integration_test_url,
                integration_test_just_read,
                integration_test_volume_write_unwritten_1,
                integration_test_volume_write_unwritten_2,
                integration_test_volume_write_unwritten_sparse,
                integration_test_volume_write_unwritten_subvols,
                integration_test_volume_write_unwritten_subvols_sparse,
                integration_test_volume_write_unwritten_subvols_3,
                integration_test_two_layers_parent_smaller,
                integration_test_two_layers_parent_smaller_unwritten,
                integration_test_scrub,
                integration_test_scrub_short,
                integration_test_scrub_short_sparse,
                integration_test_scrub_useless,
                integration_test_volume_subvols_parent_scrub_sparse,
                integration_test_volume_subvols_parent_scrub_sparse_2,
                integration_test_multi_read_only,
                integration_test_scrub_no_rop,
                integration_test_snapshot_backed_vol,
                test_pantry_get_status_after_activation,
                integration_test_clone_raw,
                integration_test_volume_clone_replace_ro_downstairs,
                integration_test_volume_replace_bad_downstairs,
                integration_test_volume_twice_replace_downstairs,
                integration_test_volume_replace_active,
                integration_test_volume_replace_downstairs,
                integration_test_volume_inactive_replace_downstairs,
                integration_test_volume_replace_downstairs_then_takeover,
                integration_test_problematic_downstairs,
                integration_test_sqlite_backed_vol,
                integration_test_guest_replace_many_downstairs,
                integration_test_guest_downstairs_unwritten,
                integration_test_guest_drop,
                integration_test_guest_zero_length_io,
                integration_test_guest_replace_downstairs,
                integration_test_guest_replace_ds_before_active,
                integration_test_upstairs_read_only_rejects_write,
                integration_test_guest_downstairs_unwritten_sparse_1,
                integration_test_guest_downstairs_unwritten_sparse_mid,
                integration_test_guest_downstairs_unwritten_sparse_end,
                integration_test_guest_downstairs_unwritten_span,
                integration_test_guest_downstairs_unwritten_span_2,
                integration_test_io_out_of_range,
                integration_test_io_span_out_of_range,
                test_pantry_import_from_url_ovmf,
                test_pantry_import_from_url_ovmf_bad_digest,
                test_pantry_import_from_local_server,
                test_pantry_snapshot,
                test_pantry_bulk_write,
                test_pantry_bulk_write_max_chunk_size,
                test_pantry_fail_bulk_write_one_byte,
                test_pantry_fail_bulk_read_one_byte,
                test_pantry_scrub,
                test_pantry_bulk_read,
                test_pantry_bulk_read_max_chunk_size,
                test_pantry_validate,
                test_pantry_validate_subset,
                test_pantry_validate_fail,
                test_volume_replace_vcr,
                test_volume_replace_rop_in_vcr,
                integration_test_guest_drop_early,
                integration_test_guest_downstairs,
            );
        };

        ($b:ident, $($fs:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $fs() {
                super::$fs(Backend::$b).await.unwrap()
            }
         )+};
    }

    mod raw_file {
        use super::*;
        integration_test_suite!(RawFile);
    }
    mod raw_file_v2 {
        use super::*;
        integration_test_suite!(RawFileV2);
    }
    mod sqlite {
        use super::*;
        integration_test_suite!(SQLite);
    }
}
