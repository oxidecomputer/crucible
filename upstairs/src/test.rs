// Copyright 2023 Oxide Computer Company

use crate::guest::Guest;
use crate::*;

/*
 * Beware, if you change these defaults, then you will have to change
 * all the hard coded tests below that use make_upstairs().
 */
pub(crate) fn make_upstairs() -> crate::upstairs::Upstairs {
    let mut def = RegionDefinition::default();
    def.set_block_size(512);
    def.set_extent_size(Block::new_512(100));
    def.set_extent_count(10);

    let opts = CrucibleOpts {
        target: vec![],
        lossy: false,
        key: None,
        ..Default::default()
    };
    let (_guest, io) = Guest::new(None);
    crate::upstairs::Upstairs::new(&opts, 0, Some(def), io, None)
}

pub(crate) fn make_encrypted_upstairs() -> crate::upstairs::Upstairs {
    let mut def = RegionDefinition::default();
    def.set_block_size(512);
    def.set_extent_size(Block::new_512(100));
    def.set_extent_count(10);

    let opts = CrucibleOpts {
        target: vec![],
        lossy: false,
        key: Some("tCw7zw0hAsPuxMOTWwnPEFYjBK9qJRtYyGdEXKEnrg0=".to_owned()),
        ..Default::default()
    };

    let (_guest, io) = Guest::new(None);
    crate::upstairs::Upstairs::new(&opts, 0, Some(def), io, None)
}

#[cfg(test)]
pub(crate) mod up_test {
    use super::*;
    use crate::{
        client::{
            validate_encrypted_read_response,
            validate_unencrypted_read_response,
        },
        upstairs::Upstairs,
    };

    use base64::{engine, Engine};
    use pseudo_file::IOSpan;

    // Create a simple logger
    pub fn csl() -> Logger {
        build_logger()
    }

    #[test]
    fn test_iospan() {
        let span = IOSpan::new(512, 1024, 512);
        assert!(span.is_block_regular());
        assert_eq!(span.affected_block_count(), 2);

        let span = IOSpan::new(513, 1024, 512);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 3);

        let span = IOSpan::new(512, 500, 512);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 1);

        let span = IOSpan::new(512, 512, 4096);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 1);

        let span = IOSpan::new(500, 4096 * 10, 4096);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 10 + 1);

        let span = IOSpan::new(500, 4096 * 3 + (4096 - 500 + 1), 4096);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 3 + 2);

        // Some from hammer
        let span = IOSpan::new(137690, 1340, 512);
        assert!(!span.is_block_regular());
        assert_eq!(span.affected_block_count(), 4);
        assert_eq!(span.affected_block_numbers(), &vec![268, 269, 270, 271]);
    }

    #[test]
    fn test_iospan_buffer_read_write() {
        let mut span = IOSpan::new(500, 64, 512);
        assert_eq!(span.affected_block_count(), 2);
        assert_eq!(span.affected_block_numbers(), &vec![0, 1]);

        span.write_from_buffer_into_blocks(&Bytes::from(vec![1; 64]));

        for i in 0..500 {
            assert_eq!(span.buffer()[i], 0);
        }
        for i in 500..512 {
            assert_eq!(span.buffer()[i], 1);
        }
        for i in 512..(512 + 64 - 12) {
            assert_eq!(span.buffer()[i], 1);
        }
        for i in (512 + 64 - 12)..1024 {
            assert_eq!(span.buffer()[i], 0);
        }

        let mut data = [0u8; 64];
        span.read_from_blocks_into_buffer(&mut data[..]);

        assert_eq!(data, [1; 64]);
    }

    /*
     * Terrible wrapper, but it allows us to call extent_from_offset()
     * just like the program does.
     */
    fn up_efo(
        up: &Upstairs,
        offset: BlockIndex,
        num_blocks: u64,
    ) -> Vec<BlockIndex> {
        let ddef = up.get_region_definition();
        extent_from_offset(&ddef, offset, num_blocks)
            .blocks()
            .collect()
    }

    #[test]
    fn off_to_extent_one_block() {
        let up = make_upstairs();

        for i in 0..100 {
            let exv = vec![BlockIndex(i)];
            assert_eq!(up_efo(&up, BlockIndex(i), 1), exv);
        }

        for i in 0..100 {
            let exv = vec![BlockIndex(100 + i)];
            assert_eq!(up_efo(&up, BlockIndex(100 + i), 1), exv);
        }

        let exv = vec![BlockIndex(200)];
        assert_eq!(up_efo(&up, BlockIndex(200), 1), exv);

        let exv = vec![BlockIndex(999)];
        assert_eq!(up_efo(&up, BlockIndex(999), 1), exv);
    }

    #[test]
    fn off_to_extent_two_blocks() {
        let up = make_upstairs();

        for i in 0..99 {
            let exv = vec![BlockIndex(i), BlockIndex(i + 1)];
            assert_eq!(up_efo(&up, BlockIndex(i), 2), exv);
        }

        let exv = vec![BlockIndex(99), BlockIndex(100)];
        assert_eq!(up_efo(&up, BlockIndex(99), 2), exv);

        for i in 0..99 {
            let exv = vec![BlockIndex(100 + i)];
            assert_eq!(up_efo(&up, BlockIndex(100 + i), 1), exv);
        }

        let exv = vec![BlockIndex(199), BlockIndex(200)];
        assert_eq!(up_efo(&up, BlockIndex(199), 2), exv);

        let exv = vec![BlockIndex(200), BlockIndex(201)];
        assert_eq!(up_efo(&up, BlockIndex(200), 2), exv);

        let exv = vec![BlockIndex(998), BlockIndex(999)];
        assert_eq!(up_efo(&up, BlockIndex(998), 2), exv);
    }

    #[test]
    fn off_to_extent_bridge() {
        /*
         * Testing when our buffer crosses extents.
         */
        let up = make_upstairs();

        /*
         * 1024 buffer
         */
        assert_eq!(
            up_efo(&up, BlockIndex(99), 2),
            vec![BlockIndex(99), BlockIndex(100)],
        );
        assert_eq!(
            up_efo(&up, BlockIndex(98), 4),
            vec![
                BlockIndex(98),
                BlockIndex(99),
                BlockIndex(100),
                BlockIndex(101)
            ],
        );

        /*
         * Largest buffer at different offsets
         */
        for offset in 0..100 {
            let expected: Vec<_> =
                (0..100).map(|i| BlockIndex(offset + i)).collect();
            assert_eq!(up_efo(&up, BlockIndex(offset), 100), expected);
        }
    }

    /*
     * Testing various invalid inputs
     */
    #[test]
    fn off_to_extent_length_zero() {
        let up = make_upstairs();
        assert_eq!(up_efo(&up, BlockIndex(0), 0), vec![]);
    }

    #[test]
    fn off_to_extent_length_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, BlockIndex(0), 1000);
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_too_big() {
        let up = make_upstairs();
        up_efo(&up, BlockIndex(0), 1001);
    }

    #[test]
    fn off_to_extent_length_and_offset_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, BlockIndex(900), 100);
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_and_offset_too_big() {
        let up = make_upstairs();
        up_efo(&up, BlockIndex(1000), 1);
    }

    // key material made with `openssl rand -base64 32`
    #[test]
    pub fn test_upstairs_encryption_context_ok() -> Result<()> {
        use rand::{thread_rng, Rng};

        let key_bytes = engine::general_purpose::STANDARD
            .decode("ClENKTXD2bCyXSHnKXY7GGnk+NvQKbwpatjWP2fJzk0=")
            .unwrap();
        let context = EncryptionContext::new(key_bytes, 512);

        let mut block = [0u8; 512];
        thread_rng().fill(&mut block[..]);

        let orig_block = block;

        let (nonce, tag, _) = context.encrypt_in_place(&mut block[..]);
        assert_ne!(block, orig_block);

        context.decrypt_in_place(&mut block[..], &nonce, &tag)?;
        assert_eq!(block, orig_block);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_encryption_context_wrong_nonce() -> Result<()> {
        use rand::{thread_rng, Rng};

        let key_bytes = engine::general_purpose::STANDARD
            .decode("EVrH+ABhMP0MLfxynCalDq1vWCCWCWFfsSsJoJeDCx8=")
            .unwrap();
        let context = EncryptionContext::new(key_bytes, 512);

        let mut block = [0u8; 512];
        thread_rng().fill(&mut block[..]);

        let orig_block = block;

        let (_, tag, _) = context.encrypt_in_place(&mut block[..]);
        assert_ne!(block, orig_block);

        let nonce = context.get_random_nonce();

        let block_before_failing_decrypt_in_place = block;

        let result = context.decrypt_in_place(&mut block[..], &nonce, &tag);
        assert!(result.is_err());

        /*
         * Make sure encryption context does not overwrite data if it's given
         * a bad nonce - we rely on this and do not make a copy when
         * attempting to decrypt with multiple encryption contexts.
         */
        assert_eq!(block_before_failing_decrypt_in_place, block);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_encryption_context_wrong_tag() -> Result<()> {
        use rand::{thread_rng, Rng};

        let key_bytes = engine::general_purpose::STANDARD
            .decode("EVrH+ABhMP0MLfxynCalDq1vWCCWCWFfsSsJoJeDCx8=")
            .unwrap();
        let context = EncryptionContext::new(key_bytes, 512);

        let mut block = [0u8; 512];
        thread_rng().fill(&mut block[..]);

        let orig_block = block;

        let (nonce, mut tag, _) = context.encrypt_in_place(&mut block[..]);
        assert_ne!(block, orig_block);

        tag[2] = tag[2].wrapping_add(1);

        let block_before_failing_decrypt_in_place = block;

        let result = context.decrypt_in_place(&mut block[..], &nonce, &tag);
        assert!(result.is_err());

        /*
         * Make sure encryption context does not overwrite data if it's given
         * a bad tag - we rely on this and do not make a copy when attempting
         * to decrypt with multiple encryption contexts.
         */
        assert_eq!(block_before_failing_decrypt_in_place, block);

        Ok(())
    }

    // Validate that an encrypted read response with one context can be
    // decrypted
    #[test]
    pub fn test_upstairs_validate_encrypted_read_response() -> Result<()> {
        // Set up the encryption context
        use rand::{thread_rng, Rng};
        let mut key = vec![0u8; 32];
        thread_rng().fill(&mut key[..]);
        let context = EncryptionContext::new(key.clone(), 512);

        // Encrypt some random data
        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);
        thread_rng().fill(&mut data[..]);

        let original_data = data.clone();

        let (nonce, tag, _) = context.encrypt_in_place(&mut data[..]);

        assert_ne!(original_data, data);

        // Create the read response context
        let ctx = crucible_protocol::EncryptionContext {
            nonce: nonce.into(),
            tag: tag.into(),
        };

        // Validate it
        let r = validate_encrypted_read_response(
            Some(ctx),
            &mut data,
            &Arc::new(context),
            &csl(),
        );

        assert!(r.is_ok());

        // `validate_encrypted_read_response` will mutate the read
        // response's data value, make sure it decrypted

        assert_eq!(original_data, data);

        Ok(())
    }

    // TODO if such a set of nonces and tags can be found:
    //
    //   let hash1 = integrity_hash(
    //      &[&ctx1.nonce[..], &ctx1.tag[..], &response.data[..]]
    //   );
    //   let hash2 = integrity_hash(
    //      &[&ctx2.nonce[..], &ctx2.tag[..], &response.data[..]]
    //   );
    //
    //   hash1 == hash2
    //
    // then write a test which validates that an encrypted read response with
    // multiple contexts that match the integrity hash (where only one is
    // correct) can be decrypted.

    // Validate that reading a blank block works
    #[test]
    pub fn test_upstairs_validate_encrypted_read_response_blank_block(
    ) -> Result<()> {
        // Set up the encryption context
        use rand::{thread_rng, Rng};
        let mut key = vec![0u8; 32];
        thread_rng().fill(&mut key[..]);
        let context = EncryptionContext::new(key.clone(), 512);

        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);

        // Validate the read response
        let r = validate_encrypted_read_response(
            None,
            &mut data,
            &Arc::new(context),
            &csl(),
        );

        // The above function will return None for a blank block
        assert!(r.is_ok());
        assert_eq!(data, vec![0u8; 512]);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_validate_unencrypted_read_response() -> Result<()> {
        use rand::{thread_rng, Rng};

        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);
        thread_rng().fill(&mut data[..]);

        let read_response_hash = integrity_hash(&[&data[..]]);
        let original_data = data.clone();

        // Validate it
        let r = validate_unencrypted_read_response(
            Some(read_response_hash),
            &mut data,
            &csl(),
        );

        assert!(r.is_ok());
        assert_eq!(data, original_data);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_validate_unencrypted_read_response_blank_block(
    ) -> Result<()> {
        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);

        let original_data = data.clone();

        // Validate a read response
        let r = validate_unencrypted_read_response(None, &mut data, &csl());

        assert!(r.is_ok());
        assert_eq!(data, original_data);

        Ok(())
    }

    fn test_ddef(blocks_per_extent: u64) -> RegionDefinition {
        // Check the send_io_live_repair for a read below extent limit,
        // at extent limit, and above extent limit.
        let mut region_options = RegionOptions::default();
        region_options.set_block_size(512);
        region_options.set_extent_size(Block::new(blocks_per_extent, 9));
        region_options.set_uuid(Uuid::new_v4());
        region_options.set_encrypted(false);
        region_options.validate().unwrap();

        let mut ddef = RegionDefinition::from_options(&region_options).unwrap();
        ddef.set_extent_count(15);
        ddef
    }

    #[test]
    fn send_io_live_repair_read() {
        const BLOCKS_PER_EXTENT: u64 = 8;
        let ddef = test_ddef(BLOCKS_PER_EXTENT);

        // Below limit
        let op = IOop::Read {
            dependencies: vec![],
            start_block: BlockIndex(7),
            count: 1,
            block_size: 512,
        };
        assert!(op.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // At limit
        let op = IOop::Read {
            dependencies: vec![],
            start_block: BlockIndex(2 * BLOCKS_PER_EXTENT + 7),
            count: 1,
            block_size: 512,
        };
        assert!(op.send_io_live_repair(Some(ExtentId(2)), &ddef));

        let op = IOop::Read {
            dependencies: vec![],
            start_block: BlockIndex(3 * BLOCKS_PER_EXTENT + 7),
            count: 1,
            block_size: 512,
        };
        // We are past the extent limit, so this should return false
        assert!(!op.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // If we change the extent limit, it should become true
        assert!(op.send_io_live_repair(Some(ExtentId(3)), &ddef));
    }

    // Construct an IOop::Write or IOop::WriteUnwritten at the given extent
    fn write_at_extent(
        eid: ExtentId,
        wu: bool,
        ddef: &RegionDefinition,
    ) -> IOop {
        let blocks_per_extent = ddef.extent_size().value;
        let request = BlockContext {
            encryption_context: None,
            hash: 0,
        };
        let data = BytesMut::from(vec![1].as_slice()).freeze();
        let blocks = vec![request];
        let start_block = BlockIndex(u64::from(eid.0) * blocks_per_extent + 7);

        if wu {
            IOop::WriteUnwritten {
                dependencies: vec![],
                blocks,
                start_block,
                data,
            }
        } else {
            IOop::Write {
                dependencies: vec![],
                blocks,
                start_block,
                data,
            }
        }
    }

    #[test]
    fn send_io_live_repair_write() {
        // Check the send_io_live_repair for a write below extent limit,
        // at extent limit, and above extent limit.
        const BLOCKS_PER_EXTENT: u64 = 8;
        let ddef = test_ddef(BLOCKS_PER_EXTENT);

        // Below limit
        let wr = write_at_extent(ExtentId(0), false, &ddef);
        assert!(wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // At the limit
        let wr = write_at_extent(ExtentId(2), false, &ddef);
        assert!(wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // Above the limit
        let wr = write_at_extent(ExtentId(3), false, &ddef);
        assert!(!wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(ExtentId(3)), &ddef));
    }

    #[test]
    fn send_io_live_repair_unwritten_write() {
        // Check the send_io_live_repair for a write unwritten below extent
        // at extent limit, and above extent limit.
        const BLOCKS_PER_EXTENT: u64 = 8;
        let ddef = test_ddef(BLOCKS_PER_EXTENT);

        // Below limit
        let wr = write_at_extent(ExtentId(0), true, &ddef);
        assert!(wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // At the limit
        let wr = write_at_extent(ExtentId(2), true, &ddef);
        assert!(wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // Above the limit
        let wr = write_at_extent(ExtentId(3), true, &ddef);
        assert!(!wr.send_io_live_repair(Some(ExtentId(2)), &ddef));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(ExtentId(3)), &ddef));
    }
}
