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

    fn extent_tuple(eid: u32, offset: u64) -> (ExtentId, Block) {
        (ExtentId(eid), Block::new_512(offset))
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
        offset: Block,
        num_blocks: u64,
    ) -> Vec<(ExtentId, Block)> {
        let ddef = up.get_region_definition();
        let num_blocks = Block::new_with_ddef(num_blocks, &ddef);
        extent_from_offset(&ddef, offset, num_blocks)
            .blocks(&ddef)
            .collect()
    }

    #[test]
    fn off_to_extent_one_block() {
        let up = make_upstairs();

        for i in 0..100 {
            let exv = vec![extent_tuple(0, i)];
            assert_eq!(up_efo(&up, Block::new_512(i), 1), exv);
        }

        for i in 0..100 {
            let exv = vec![extent_tuple(1, i)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1), exv);
        }

        let exv = vec![extent_tuple(2, 0)];
        assert_eq!(up_efo(&up, Block::new_512(200), 1), exv);

        let exv = vec![extent_tuple(9, 99)];
        assert_eq!(up_efo(&up, Block::new_512(999), 1), exv);
    }

    #[test]
    fn off_to_extent_two_blocks() {
        let up = make_upstairs();

        for i in 0..99 {
            let exv = vec![extent_tuple(0, i), extent_tuple(0, i + 1)];
            assert_eq!(up_efo(&up, Block::new_512(i), 2), exv);
        }

        let exv = vec![extent_tuple(0, 99), extent_tuple(1, 0)];
        assert_eq!(up_efo(&up, Block::new_512(99), 2), exv);

        for i in 0..99 {
            let exv = vec![extent_tuple(1, i)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1), exv);
        }

        let exv = vec![extent_tuple(1, 99), extent_tuple(2, 0)];
        assert_eq!(up_efo(&up, Block::new_512(199), 2), exv);

        let exv = vec![extent_tuple(2, 0), extent_tuple(2, 1)];
        assert_eq!(up_efo(&up, Block::new_512(200), 2), exv);

        let exv = vec![extent_tuple(9, 98), extent_tuple(9, 99)];
        assert_eq!(up_efo(&up, Block::new_512(998), 2), exv);
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
            up_efo(&up, Block::new_512(99), 2),
            vec![extent_tuple(0, 99), extent_tuple(1, 0)],
        );
        assert_eq!(
            up_efo(&up, Block::new_512(98), 4),
            vec![
                extent_tuple(0, 98),
                extent_tuple(0, 99),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
            ],
        );

        /*
         * Largest buffer at different offsets
         */
        for offset in 0..100 {
            let expected: Vec<_> = (0..100)
                .map(|i| {
                    extent_tuple(
                        (offset + i) / 100,
                        u64::from((offset + i) % 100),
                    )
                })
                .collect();
            assert_eq!(
                up_efo(&up, Block::new_512(u64::from(offset)), 100),
                expected
            );
        }
    }

    /*
     * Testing various invalid inputs
     */
    #[test]
    fn off_to_extent_length_zero() {
        let up = make_upstairs();
        assert_eq!(up_efo(&up, Block::new_512(0), 0), vec![]);
    }

    #[test]
    fn off_to_extent_length_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1000);
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1001);
    }

    #[test]
    fn off_to_extent_length_and_offset_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(900), 100);
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_and_offset_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(1000), 1);
    }

    #[test]
    #[should_panic]
    fn not_right_block_size() {
        let up = make_upstairs();
        up_efo(&up, Block::new_4096(900), 1);
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

        let read_response_hash = integrity_hash(&[&nonce, &tag, &data[..]]);

        // Create the read response
        let block_context = BlockContext {
            hash: read_response_hash,
            encryption_context: Some(crucible_protocol::EncryptionContext {
                nonce: nonce.into(),
                tag: tag.into(),
            }),
        };

        // Validate it
        let successful_hash = validate_encrypted_read_response(
            Some(block_context),
            &mut data,
            &Arc::new(context),
            &csl(),
        )?;

        assert_eq!(successful_hash, Some(read_response_hash));

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
        let successful_hash = validate_encrypted_read_response(
            None,
            &mut data,
            &Arc::new(context),
            &csl(),
        )?;

        // The above function will return None for a blank block
        assert_eq!(successful_hash, None);
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

        // Create the read response
        let block_context = BlockContext {
            hash: read_response_hash,
            encryption_context: None,
        };

        // Validate it
        let successful_hash = validate_unencrypted_read_response(
            Some(block_context),
            &mut data,
            &csl(),
        )?;

        assert_eq!(successful_hash, Some(read_response_hash));
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
        let successful_hash =
            validate_unencrypted_read_response(None, &mut data, &csl())?;

        assert_eq!(successful_hash, None);
        assert_eq!(data, original_data);

        Ok(())
    }

    #[test]
    fn send_io_live_repair_read() {
        // Check the send_io_live_repair for a read below extent limit,
        // at extent limit, and above extent limit.

        // Below limit
        let request = ReadRequest {
            eid: ExtentId(0),
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        assert!(op.send_io_live_repair(Some(ExtentId(2))));

        // At limit
        let request = ReadRequest {
            eid: ExtentId(2),
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        assert!(op.send_io_live_repair(Some(ExtentId(2))));

        let request = ReadRequest {
            eid: ExtentId(3),
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        // We are past the extent limit, so this should return false
        assert!(!op.send_io_live_repair(Some(ExtentId(2))));

        // If we change the extent limit, it should become true
        assert!(op.send_io_live_repair(Some(ExtentId(3))));
    }

    // Construct an IOop::Write or IOop::WriteUnwritten at the given extent
    fn write_at_extent(eid: ExtentId, wu: bool) -> IOop {
        let request = crucible_protocol::WriteBlockMetadata {
            eid,
            offset: Block::new_512(7),
            block_context: BlockContext {
                encryption_context: None,
                hash: 0,
            },
        };
        let data = BytesMut::from(vec![1].as_slice());
        let blocks = vec![request];

        if wu {
            IOop::WriteUnwritten {
                dependencies: vec![],
                blocks,
                data: data.freeze(),
            }
        } else {
            IOop::Write {
                dependencies: vec![],
                blocks,
                data: data.freeze(),
            }
        }
    }

    #[test]
    fn send_io_live_repair_write() {
        // Check the send_io_live_repair for a write below extent limit,
        // at extent limit, and above extent limit.

        // Below limit
        let wr = write_at_extent(ExtentId(0), false);
        assert!(wr.send_io_live_repair(Some(ExtentId(2))));

        // At the limit
        let wr = write_at_extent(ExtentId(2), false);
        assert!(wr.send_io_live_repair(Some(ExtentId(2))));

        // Above the limit
        let wr = write_at_extent(ExtentId(3), false);
        assert!(!wr.send_io_live_repair(Some(ExtentId(2))));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(ExtentId(3))));
    }

    #[test]
    fn send_io_live_repair_unwritten_write() {
        // Check the send_io_live_repair for a write unwritten below extent
        // at extent limit, and above extent limit.

        // Below limit
        let wr = write_at_extent(ExtentId(0), true);
        assert!(wr.send_io_live_repair(Some(ExtentId(2))));

        // At the limit
        let wr = write_at_extent(ExtentId(2), true);
        assert!(wr.send_io_live_repair(Some(ExtentId(2))));

        // Above the limit
        let wr = write_at_extent(ExtentId(3), true);
        assert!(!wr.send_io_live_repair(Some(ExtentId(2))));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(ExtentId(3))));
    }
}
