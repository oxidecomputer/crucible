// Copyright 2022 Oxide Computer Company

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::sync::Arc;

    use anyhow::*;
    use crucible::{Bytes, *};
    use crucible_downstairs::*;
    use futures::lock::Mutex;
    use httptest::{matchers::*, responders::*, Expectation, Server};
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
                false, /* read_only */
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

    // Note each downstairs in each test must be unique!

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn integration_test_downstairs() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        let _downstairs1 =
            TestDownstairs::new("127.0.0.1".parse()?, 54001, true)?;
        let _downstairs2 =
            TestDownstairs::new("127.0.0.1".parse()?, 54002, true)?;
        let _downstairs3 =
            TestDownstairs::new("127.0.0.1".parse()?, 54003, true)?;

        let vcr: VolumeConstructionRequest =
            VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: BLOCK_SIZE as u64,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: BLOCK_SIZE as u64,
                    opts: CrucibleOpts {
                        target: vec![
                            "127.0.0.1:54001".parse()?,
                            "127.0.0.1:54002".parse()?,
                            "127.0.0.1:54003".parse()?,
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: Some(
                            "YVubErJfMsHeaM+v3UY+11IutzbiArT1woP91BWj/Zc="
                                .to_string(),
                        ),
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        metric_collect: None,
                        metric_register: None,
                        ..Default::default()
                    },
                    gen: 0,
                }],
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let volume =
            Arc::new(tokio::task::block_in_place(|| Volume::construct(vcr))?);

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

        let _downstairs1 =
            TestDownstairs::new("127.0.0.1".parse()?, 54004, true)?;
        let _downstairs2 =
            TestDownstairs::new("127.0.0.1".parse()?, 54005, true)?;
        let _downstairs3 =
            TestDownstairs::new("127.0.0.1".parse()?, 54006, true)?;

        // Create in memory block io full of 11
        let in_memory_data =
            Arc::new(InMemoryBlockIO::new(BLOCK_SIZE as u64, BLOCK_SIZE * 10));

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
        volume.add_subvolume_create_guest(
            CrucibleOpts {
                target: vec![
                    "127.0.0.1:54004".parse()?,
                    "127.0.0.1:54005".parse()?,
                    "127.0.0.1:54006".parse()?,
                ],
                lossy: false,
                flush_timeout: None,
                key: Some(
                    "6Yiim0tnK91G7O0KTRLumpuhkr9T0X1AVSVYJUhRcxs=".to_string(),
                ),
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                metric_collect: None,
                metric_register: None,
                ..Default::default()
            },
            0,
        )?;
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

        let _downstairs1 =
            TestDownstairs::new("127.0.0.1".parse()?, 54007, true)?;
        let _downstairs2 =
            TestDownstairs::new("127.0.0.1".parse()?, 54008, true)?;
        let _downstairs3 =
            TestDownstairs::new("127.0.0.1".parse()?, 54009, true)?;

        // Create in memory block io full of 11
        let in_memory_data =
            Arc::new(InMemoryBlockIO::new(BLOCK_SIZE as u64, BLOCK_SIZE * 10));

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
                    opts: CrucibleOpts {
                        target: vec![
                            "127.0.0.1:54007".parse()?,
                            "127.0.0.1:54008".parse()?,
                            "127.0.0.1:54009".parse()?,
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: Some(
                            "/Gud6zA+MoI/lvt+0dqf3wIMyTfNqp1Bw6FKuM+zFWM="
                                .to_string(),
                        ),
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        metric_collect: None,
                        metric_register: None,
                        ..Default::default()
                    },
                    gen: 0,
                }],
                read_only_parent: None,
            };

        // XXX Crucible uses std::sync::mpsc::Receiver, not
        // tokio::sync::mpsc::Receiver, so use tokio::task::block_in_place here.
        // Remove that when Crucible changes over to the tokio mpsc.
        let mut volume =
            tokio::task::block_in_place(|| Volume::construct(vcr))?;

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

        let _downstairs1 =
            TestDownstairs::new("127.0.0.1".parse()?, 54010, true)?;
        let _downstairs2 =
            TestDownstairs::new("127.0.0.1".parse()?, 54011, true)?;
        let _downstairs3 =
            TestDownstairs::new("127.0.0.1".parse()?, 54012, true)?;

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
                    opts: CrucibleOpts {
                        target: vec![
                            "127.0.0.1:54010".parse()?,
                            "127.0.0.1:54011".parse()?,
                            "127.0.0.1:54012".parse()?,
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: Some(
                            "+3AhoL47nkZPwj9XRmoCnOKa66Cfb8Q2gmQ84pVlsbw="
                                .to_string(),
                        ),
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        metric_collect: None,
                        metric_register: None,
                        ..Default::default()
                    },
                    gen: 0,
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Volume {
                        id: Uuid::new_v4(),
                        block_size: BLOCK_SIZE as u64,
                        sub_volumes: vec![VolumeConstructionRequest::Url {
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
        let volume = tokio::task::block_in_place(|| Volume::construct(vcr))?;
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
}
