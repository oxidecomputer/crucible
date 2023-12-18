// Copyright 2023 Oxide Computer Company

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

    crate::upstairs::Upstairs::new(
        &opts,
        0,
        Some(def),
        Arc::new(Guest::new()),
        None,
        crucible_common::build_logger(),
    )
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
    use rand::prelude::*;

    use base64::{engine, Engine};
    use pseudo_file::IOSpan;
    use ringbuffer::RingBuffer;

    // Create a simple logger
    pub fn csl() -> Logger {
        build_logger()
    }

    fn extent_tuple(eid: u64, offset: u64) -> (u64, Block) {
        (eid, Block::new_512(offset))
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

    #[tokio::test]
    async fn test_iospan_buffer_read_write() {
        let span = IOSpan::new(500, 64, 512);
        assert_eq!(span.affected_block_count(), 2);
        assert_eq!(span.affected_block_numbers(), &vec![0, 1]);

        span.write_from_buffer_into_blocks(&Bytes::from(vec![1; 64]))
            .await;

        for i in 0..500 {
            assert_eq!(span.buffer().as_vec().await[i], 0);
        }
        for i in 500..512 {
            assert_eq!(span.buffer().as_vec().await[i], 1);
        }
        for i in 512..(512 + 64 - 12) {
            assert_eq!(span.buffer().as_vec().await[i], 1);
        }
        for i in (512 + 64 - 12)..1024 {
            assert_eq!(span.buffer().as_vec().await[i], 0);
        }

        let data = Buffer::new(64);
        span.read_from_blocks_into_buffer(&mut data.as_vec().await[..])
            .await;

        for i in 0..64 {
            assert_eq!(data.as_vec().await[i], 1);
        }
    }

    /*
     * Terrible wrapper, but it allows us to call extent_from_offset()
     * just like the program does.
     */
    fn up_efo(
        up: &Upstairs,
        offset: Block,
        num_blocks: u64,
    ) -> Vec<(u64, Block)> {
        let ddef = up.get_region_definition();
        let num_blocks = Block::new_with_ddef(num_blocks, &ddef);
        extent_from_offset(&ddef, offset, num_blocks)
            .blocks(&ddef)
            .collect()
    }

    #[tokio::test]
    async fn off_to_extent_one_block() {
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

    #[tokio::test]
    async fn off_to_extent_two_blocks() {
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

    #[tokio::test]
    async fn off_to_extent_bridge() {
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
            let expected: Vec<(u64, Block)> = (0..100)
                .map(|i| extent_tuple((offset + i) / 100, (offset + i) % 100))
                .collect();
            assert_eq!(up_efo(&up, Block::new_512(offset), 100), expected);
        }
    }

    /*
     * Testing various invalid inputs
     */
    #[tokio::test]
    async fn off_to_extent_length_zero() {
        let up = make_upstairs();
        assert_eq!(up_efo(&up, Block::new_512(0), 0), vec![]);
    }

    #[tokio::test]
    async fn off_to_extent_length_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1000);
    }

    #[tokio::test]
    #[should_panic]
    async fn off_to_extent_length_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1001);
    }

    #[tokio::test]
    async fn off_to_extent_length_and_offset_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(900), 100);
    }

    #[tokio::test]
    #[should_panic]
    async fn off_to_extent_length_and_offset_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(1000), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn not_right_block_size() {
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

        let (nonce, tag, _) = context.encrypt_in_place(&mut block[..])?;
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

        let (_, tag, _) = context.encrypt_in_place(&mut block[..])?;
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

        let (nonce, mut tag, _) = context.encrypt_in_place(&mut block[..])?;
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

        let (nonce, tag, _) = context.encrypt_in_place(&mut data[..])?;

        assert_ne!(original_data, data);

        let read_response_hash = integrity_hash(&[&nonce, &tag, &data[..]]);

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![BlockContext {
                hash: read_response_hash,
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    },
                ),
            }],
        };

        // Validate it
        let successful_hash = validate_encrypted_read_response(
            &mut read_response,
            &Arc::new(context),
            &csl(),
        )?;

        assert_eq!(successful_hash, Some(read_response_hash));

        // `validate_encrypted_read_response` will mutate the read
        // response's data value, make sure it decrypted

        assert_eq!(original_data, read_response.data);

        Ok(())
    }

    // Validate that an encrypted read response with multiple contexts can be
    // decrypted (skipping ones that don't match)
    #[test]
    pub fn test_upstairs_validate_encrypted_read_response_multiple_contexts(
    ) -> Result<()> {
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

        let (nonce, tag, _) = context.encrypt_in_place(&mut data[..])?;

        assert_ne!(original_data, data);

        let read_response_hash = integrity_hash(&[&nonce, &tag, &data[..]]);

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![
                // The first context here doesn't match
                BlockContext {
                    hash: thread_rng().gen(),
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: thread_rng().gen::<[u8; 12]>(),
                            tag: thread_rng().gen::<[u8; 16]>(),
                        },
                    ),
                },
                // This context matches
                BlockContext {
                    hash: read_response_hash,
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: nonce.into(),
                            tag: tag.into(),
                        },
                    ),
                },
                // The last context does not
                BlockContext {
                    hash: thread_rng().gen(),
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: thread_rng().gen::<[u8; 12]>(),
                            tag: thread_rng().gen::<[u8; 16]>(),
                        },
                    ),
                },
            ],
        };

        // Validate it
        let successful_hash = validate_encrypted_read_response(
            &mut read_response,
            &Arc::new(context),
            &csl(),
        )?;

        assert_eq!(successful_hash, Some(read_response_hash));

        // `validate_encrypted_read_response` will mutate the read
        // response's data value, make sure it decrypted

        assert_eq!(original_data, read_response.data);

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

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data: data.clone(),
            block_contexts: vec![],
        };

        // Validate it
        let successful_hash = validate_encrypted_read_response(
            &mut read_response,
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
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![BlockContext {
                hash: read_response_hash,
                encryption_context: None,
            }],
        };

        // Validate it
        let successful_hash =
            validate_unencrypted_read_response(&mut read_response, &csl())?;

        assert_eq!(successful_hash, Some(read_response_hash));
        assert_eq!(read_response.data, original_data);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_validate_unencrypted_read_response_blank_block(
    ) -> Result<()> {
        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);

        let original_data = data.clone();

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![],
        };

        // Validate it
        let successful_hash =
            validate_unencrypted_read_response(&mut read_response, &csl())?;

        assert_eq!(successful_hash, None);
        assert_eq!(read_response.data, original_data);

        Ok(())
    }

    #[test]
    pub fn test_upstairs_validate_unencrypted_read_response_multiple_contexts(
    ) -> Result<()> {
        use rand::{thread_rng, Rng};

        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);
        thread_rng().fill(&mut data[..]);

        let read_response_hash = integrity_hash(&[&data[..]]);
        let original_data = data.clone();

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![
                // The context here doesn't match
                BlockContext {
                    hash: thread_rng().gen(),
                    encryption_context: None,
                },
                // The context here doesn't match
                BlockContext {
                    hash: thread_rng().gen(),
                    encryption_context: None,
                },
                // Correct one
                BlockContext {
                    hash: read_response_hash,
                    encryption_context: None,
                },
                // The context here doesn't match
                BlockContext {
                    hash: thread_rng().gen(),
                    encryption_context: None,
                },
            ],
        };

        // Validate it
        let successful_hash =
            validate_unencrypted_read_response(&mut read_response, &csl())?;

        assert_eq!(successful_hash, Some(read_response_hash));
        assert_eq!(read_response.data, original_data);

        Ok(())
    }

    // Validate that an unencrypted read response with multiple contexts that
    // match the integrity hash works. This can happen if the Upstairs
    // repeatedly writes the same block data.
    #[test]
    pub fn test_upstairs_validate_unencrypted_read_response_multiple_hashes(
    ) -> Result<()> {
        use rand::{thread_rng, Rng};

        let mut data = BytesMut::with_capacity(512);
        data.resize(512, 0u8);
        thread_rng().fill(&mut data[..]);

        let read_response_hash = integrity_hash(&[&data[..]]);
        let original_data = data.clone();

        // Create the read response
        let mut read_response = ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_contexts: vec![
                // Correct one
                BlockContext {
                    hash: read_response_hash,
                    encryption_context: None,
                },
                // Correct one
                BlockContext {
                    hash: read_response_hash,
                    encryption_context: None,
                },
            ],
        };

        // Validate it
        let successful_hash =
            validate_unencrypted_read_response(&mut read_response, &csl())?;

        assert_eq!(successful_hash, Some(read_response_hash));
        assert_eq!(read_response.data, original_data);

        Ok(())
    }

    macro_rules! assert_consumed {
        ($guest:expr) => {{
            let req = $guest.consume_req().await;
            assert!(req.is_some());
            // Send a completion for the req, rather than just dropping it.
            // This fulfills the invariant that BlockRes is completed prior to
            // being dropped.
            req.unwrap().res.send_ok();
        }};
    }
    macro_rules! assert_none_consumed {
        ($guest:expr) => {{
            assert!($guest.consume_req().await.is_none());
        }};
    }

    #[tokio::test]
    async fn test_no_iop_limit() -> Result<()> {
        let guest = Guest::new();

        assert_none_consumed!(&guest);

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(8000),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(16000),
            })
            .await;

        // With no IOP limit, all requests are consumed immediately
        assert_consumed!(&guest);
        assert_consumed!(&guest);
        assert_consumed!(&guest);

        assert_none_consumed!(&guest);

        // If no IOP limit set, don't track it
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_iop_limit() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_iop_limit(16000, 2);

        assert_none_consumed!(&guest);

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(8000),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(16000),
            })
            .await;

        // First two reads succeed
        assert_consumed!(&guest);
        assert_consumed!(&guest);

        // Next cannot be consumed until there's available IOP tokens so it
        // remains in the queue.
        assert_none_consumed!(&guest);
        assert!(!guest.reqs.lock().await.is_empty());
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 2);

        // Replenish one token, meaning next read can be consumed
        guest.leak_iop_tokens(1);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 1);

        assert_consumed!(&guest);
        assert!(guest.reqs.lock().await.is_empty());
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 2);

        guest.leak_iop_tokens(2);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        guest.leak_iop_tokens(16000);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_does_not_consume_iops() -> Result<()> {
        let mut guest = Guest::new();

        // Set 0 as IOP limit
        guest.set_iop_limit(16000, 0);
        assert_none_consumed!(&guest);

        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;

        assert_consumed!(&guest);
        assert_consumed!(&guest);
        assert_consumed!(&guest);

        assert_none_consumed!(&guest);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_bw_limit() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_bw_limit(1024 * 1024); // 1 KiB

        assert_none_consumed!(&guest);

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1024 * 1024 / 2),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1024 * 1024 / 2),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1024 * 1024 / 2),
            })
            .await;

        // First two reads succeed
        assert_consumed!(&guest);
        assert_consumed!(&guest);

        // Next cannot be consumed until there's available BW tokens so it
        // remains in the queue.
        assert_none_consumed!(&guest);
        assert!(!guest.reqs.lock().await.is_empty());
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        // Replenish enough tokens, meaning next read can be consumed
        guest.leak_bw_tokens(1024 * 1024 / 2);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024 / 2);

        assert_consumed!(&guest);
        assert!(guest.reqs.lock().await.is_empty());
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        guest.leak_bw_tokens(1024 * 1024);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        guest.leak_bw_tokens(1024 * 1024 * 1024);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_does_not_consume_bw() -> Result<()> {
        let mut guest = Guest::new();

        // Set 0 as bandwidth limit
        guest.set_bw_limit(0);
        assert_none_consumed!(&guest);

        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;
        let _ = guest
            .send(BlockOp::Flush {
                snapshot_details: None,
            })
            .await;

        assert_consumed!(&guest);
        assert_consumed!(&guest);
        assert_consumed!(&guest);

        assert_none_consumed!(&guest);

        Ok(())
    }

    #[tokio::test]
    async fn test_iop_and_bw_limit() -> Result<()> {
        let mut guest = Guest::new();

        guest.set_iop_limit(16384, 500); // 1 IOP is 16 KiB
        guest.set_bw_limit(6400 * 1024); // 16384 B * 400 = 6400 KiB/s
        assert_none_consumed!(&guest);

        // Don't use guest.read, that will send a block size query that will
        // never be answered.

        // Validate that BW limit activates by sending two 7000 KiB IOs. 7000
        // KiB is only 437.5 IOPs

        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(7000 * 1024),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(7000 * 1024),
            })
            .await;

        assert_consumed!(&guest);
        assert_none_consumed!(&guest);

        // Assert we've hit the BW limit before IOPS
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 438); // 437.5 rounded up
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 7000 * 1024);

        guest.leak_iop_tokens(438);
        guest.leak_bw_tokens(7000 * 1024);

        assert_consumed!(&guest);
        assert!(guest.reqs.lock().await.is_empty());

        // Back to zero
        guest.leak_iop_tokens(438);
        guest.leak_bw_tokens(7000 * 1024);

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        // Validate that IOP limit activates by sending 501 1024b IOs
        for _ in 0..500 {
            let _ = guest
                .send(BlockOp::Read {
                    offset: Block::new_512(0),
                    data: Buffer::new(1024),
                })
                .await;
            assert_consumed!(&guest);
        }

        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1024),
            })
            .await;
        assert_none_consumed!(&guest);

        // Assert we've hit the IOPS limit
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 500);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 500 * 1024);

        // Back to zero
        guest.leak_iop_tokens(500);
        guest.leak_bw_tokens(500 * 1024);
        guest
            .reqs
            .lock()
            .await
            .drain(..)
            .for_each(|req| req.res.send_ok());

        assert!(guest.reqs.lock().await.is_empty());
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        // From
        // https://aws.amazon.com/premiumsupport/knowledge-center/ebs-calculate-optimal-io-size/:
        //
        // Amazon EBS calculates the optimal I/O size using the following
        // equation: throughput / number of IOPS = optimal I/O size.

        let optimal_io_size: usize = 6400 * 1024 / 500;

        // Make sure this is <= an IOP size
        assert!(optimal_io_size <= 16384);

        // I mean, it makes sense: now we submit 500 of those to reach both
        // limits at the same time.
        for i in 0..500 {
            assert_eq!(*guest.iop_tokens.lock().unwrap(), i);
            assert_eq!(*guest.bw_tokens.lock().unwrap(), i * optimal_io_size);

            let _ = guest
                .send(BlockOp::Read {
                    offset: Block::new_512(0),
                    data: Buffer::new(optimal_io_size),
                })
                .await;

            assert_consumed!(&guest);
        }

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 500);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 500 * optimal_io_size);

        Ok(())
    }

    // Is it possible to submit an IO that will never be sent? It shouldn't be!
    #[tokio::test]
    async fn test_impossible_io() -> Result<()> {
        let mut guest = Guest::new();

        guest.set_iop_limit(1024 * 1024 / 2, 10); // 1 IOP is half a KiB
        guest.set_bw_limit(1024 * 1024); // 1 KiB
        assert_none_consumed!(&guest);

        // Sending an IO of 10 KiB is larger than the bandwidth limit and
        // represents 20 IOPs, larger than the IOP limit.
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(10 * 1024 * 1024),
            })
            .await;
        let _ = guest
            .send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(0),
            })
            .await;

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        // Even though the first IO is larger than the bandwidth and IOP limit,
        // it should still succeed. The next IO should not, even if it consumes
        // nothing, because the iops and bw tokens will be larger than the limit
        // for a while (until they leak enough).

        assert_consumed!(&guest);
        assert_none_consumed!(&guest);

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 20);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 10 * 1024 * 1024);

        // Bandwidth trigger is going to be larger and need more leaking to get
        // down to a point where the zero sized IO can fire.
        for _ in 0..9 {
            guest.leak_iop_tokens(10);
            guest.leak_bw_tokens(1024 * 1024);

            assert_none_consumed!(&guest);
        }

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        assert_none_consumed!(&guest);

        guest.leak_iop_tokens(10);
        guest.leak_bw_tokens(1024 * 1024);

        // We've leaked 10 KiB worth, it should fire now!
        assert_consumed!(&guest);

        Ok(())
    }

    #[tokio::test]
    async fn send_io_live_repair_read() {
        // Check the send_io_live_repair for a read below extent limit,
        // at extent limit, and above extent limit.

        // Below limit
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        assert!(op.send_io_live_repair(Some(2)));

        // At limit
        let request = ReadRequest {
            eid: 2,
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        assert!(op.send_io_live_repair(Some(2)));

        let request = ReadRequest {
            eid: 3,
            offset: Block::new_512(7),
        };
        let op = IOop::Read {
            dependencies: vec![],
            requests: vec![request],
        };
        // We are past the extent limit, so this should return false
        assert!(!op.send_io_live_repair(Some(2)));

        // If we change the extent limit, it should become true
        assert!(op.send_io_live_repair(Some(3)));
    }

    // Construct an IOop::Write or IOop::WriteUnwritten at the given extent
    fn write_at_extent(eid: u64, wu: bool) -> IOop {
        let request = crucible_protocol::Write {
            eid,
            offset: Block::new_512(7),
            data: Bytes::from(vec![1]),
            block_context: BlockContext {
                encryption_context: None,
                hash: 0,
            },
        };

        let writes = vec![request];

        if wu {
            IOop::WriteUnwritten {
                dependencies: vec![],
                writes,
            }
        } else {
            IOop::Write {
                dependencies: vec![],
                writes,
            }
        }
    }

    #[tokio::test]
    async fn send_io_live_repair_write() {
        // Check the send_io_live_repair for a write below extent limit,
        // at extent limit, and above extent limit.

        // Below limit
        let wr = write_at_extent(0, false);
        assert!(wr.send_io_live_repair(Some(2)));

        // At the limit
        let wr = write_at_extent(2, false);
        assert!(wr.send_io_live_repair(Some(2)));

        // Above the limit
        let wr = write_at_extent(3, false);
        assert!(!wr.send_io_live_repair(Some(2)));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(3)));
    }

    #[tokio::test]
    async fn send_io_live_repair_unwritten_write() {
        // Check the send_io_live_repair for a write unwritten below extent
        // at extent limit, and above extent limit.

        // Below limit
        let wr = write_at_extent(0, true);
        assert!(wr.send_io_live_repair(Some(2)));

        // At the limit
        let wr = write_at_extent(2, true);
        assert!(wr.send_io_live_repair(Some(2)));

        // Above the limit
        let wr = write_at_extent(3, true);
        assert!(!wr.send_io_live_repair(Some(2)));

        // Back to being below the limit
        assert!(wr.send_io_live_repair(Some(3)));
    }

    // Test that multiple GtoS downstairs jobs work
    #[tokio::test]
    async fn test_multiple_gtos_bulk_read_read() {
        let mut up = Upstairs::test_default(None);
        up.force_active().unwrap();
        crate::downstairs::test::set_all_active(&mut up.downstairs);

        let mut gw = up.guest.guest_work.lock().await;

        let gw_id = 12345;

        // Create two reads
        let first_id = JobId(1010);
        let second_id = JobId(1011);

        let mut data_buffers = HashMap::new();
        data_buffers.insert(first_id, Buffer::new(512));
        data_buffers.insert(second_id, Buffer::new(512));

        let mut sub = HashSet::new();
        sub.insert(first_id);
        sub.insert(second_id);

        let guest_job = GtoS::new_bulk(sub, data_buffers.clone(), None);

        gw.active.insert(gw_id, guest_job);

        let mut first_response_data = BytesMut::with_capacity(512);
        first_response_data.resize(512, 0u8);
        thread_rng().fill(&mut first_response_data[..]);
        let first_read_response_hash =
            integrity_hash(&[&first_response_data[..]]);

        let mut second_response_data = BytesMut::with_capacity(512);
        second_response_data.resize(512, 0u8);
        thread_rng().fill(&mut second_response_data[..]);
        let second_read_response_hash =
            integrity_hash(&[&second_response_data[..]]);

        let first_response = Some(vec![ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data: first_response_data.clone(),
            block_contexts: vec![BlockContext {
                hash: first_read_response_hash,
                encryption_context: None,
            }],
        }]);

        let second_response = Some(vec![ReadResponse {
            eid: 0,
            offset: Block::new_512(0),
            data: second_response_data.clone(),
            block_contexts: vec![BlockContext {
                hash: second_read_response_hash,
                encryption_context: None,
            }],
        }]);

        gw.gw_ds_complete(gw_id, first_id, first_response, Ok(()), &up.log)
            .await;
        assert!(!gw.completed.contains(&gw_id));

        gw.gw_ds_complete(gw_id, second_id, second_response, Ok(()), &up.log)
            .await;
        assert!(gw.completed.contains(&gw_id));

        assert_eq!(
            *data_buffers.get(&first_id).unwrap().as_vec().await,
            first_response_data.to_vec()
        );
        assert_eq!(
            *data_buffers.get(&second_id).unwrap().as_vec().await,
            second_response_data.to_vec()
        );
    }

    #[test]
    fn test_check_read_response_hashes() {
        // Regular happy path
        let job = DownstairsIO {
            ds_id: JobId(1),
            guest_id: 1,
            work: IOop::Read {
                dependencies: vec![],
                requests: vec![],
            },
            state: ClientData::new(IOState::New),
            acked: false,
            replay: false,
            data: None,
            read_response_hashes: vec![Some(123)],
        };

        assert!(job.check_read_response_hashes(&[ReadResponse {
            eid: 0,
            offset: Block::new_512(1),
            data: BytesMut::default(),
            block_contexts: vec![BlockContext {
                hash: 123,
                encryption_context: None,
            }],
        }]));

        // Mismatch!
        let job = DownstairsIO {
            ds_id: JobId(1),
            guest_id: 1,
            work: IOop::Read {
                dependencies: vec![],
                requests: vec![],
            },
            state: ClientData::new(IOState::New),
            acked: false,
            replay: false,
            data: None,
            read_response_hashes: vec![Some(456)],
        };

        assert!(!job.check_read_response_hashes(&[ReadResponse {
            eid: 0,
            offset: Block::new_512(1),
            data: BytesMut::default(),
            block_contexts: vec![BlockContext {
                hash: 123,
                encryption_context: None,
            }],
        }]));

        // Length mismatch
        let job = DownstairsIO {
            ds_id: JobId(1),
            guest_id: 1,
            work: IOop::Read {
                dependencies: vec![],
                requests: vec![],
            },
            state: ClientData::new(IOState::New),
            acked: false,
            replay: false,
            data: None,
            read_response_hashes: vec![Some(456), Some(123)],
        };

        assert!(!job.check_read_response_hashes(&[ReadResponse {
            eid: 0,
            offset: Block::new_512(1),
            data: BytesMut::default(),
            block_contexts: vec![BlockContext {
                hash: 123,
                encryption_context: None,
            }],
        }]));

        let job = DownstairsIO {
            ds_id: JobId(1),
            guest_id: 1,
            work: IOop::Read {
                dependencies: vec![],
                requests: vec![],
            },
            state: ClientData::new(IOState::New),
            acked: false,
            replay: false,
            data: None,
            read_response_hashes: vec![Some(777)],
        };

        assert!(!job.check_read_response_hashes(&[ReadResponse {
            eid: 0,
            offset: Block::new_512(1),
            data: BytesMut::default(),
            block_contexts: vec![
                BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
                BlockContext {
                    hash: 456,
                    encryption_context: None,
                },
            ],
        }]));
    }
}
