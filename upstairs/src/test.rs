// Copyright 2021 Oxide Computer Company

#[cfg(test)]
use super::*;

#[cfg(test)]
mod up_test {
    use super::*;
    use pseudo_file::IOSpan;
    use ringbuffer::RingBuffer;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn extent_tuple(eid: u64, offset: u64) -> (u64, Block) {
        (eid, Block::new_512(offset))
    }

    #[test]
    fn test_extent_from_offset() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(2));
        ddef.set_extent_count(10);

        // Test block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(1),)
                .unwrap(),
            vec![extent_tuple(0, 0)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(2),)
                .unwrap(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1),],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(4),)
                .unwrap(),
            vec![
                extent_tuple(0, 0),
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
            ],
        );

        // Test offsets
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(1), Block::new_512(4),)
                .unwrap(),
            vec![
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(4),)
                .unwrap(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(16),)
                .unwrap(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
                extent_tuple(3, 0),
                extent_tuple(3, 1),
                extent_tuple(4, 0),
                extent_tuple(4, 1),
                extent_tuple(5, 0),
                extent_tuple(5, 1),
                extent_tuple(6, 0),
                extent_tuple(6, 1),
                extent_tuple(7, 0),
                extent_tuple(7, 1),
                extent_tuple(8, 0),
                extent_tuple(8, 1),
            ],
        );
    }

    #[test]
    fn test_extent_from_offset_single_block_only() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(2));
        ddef.set_extent_count(10);

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(1), // num_blocks
            )
            .unwrap(),
            vec![extent_tuple(1, 0),]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(2), // num_blocks
            )
            .unwrap(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1),]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(3), // num_blocks
            )
            .unwrap(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1), extent_tuple(2, 0),]
        );
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
        let span = IOSpan::new(500, 64, 512);
        assert_eq!(span.affected_block_count(), 2);
        assert_eq!(span.affected_block_numbers(), &vec![0, 1]);

        span.write_from_buffer_into_blocks(&Bytes::from(vec![1; 64]));

        for i in 0..500 {
            assert_eq!(span.buffer().as_vec()[i], 0);
        }
        for i in 500..512 {
            assert_eq!(span.buffer().as_vec()[i], 1);
        }
        for i in 512..(512 + 64 - 12) {
            assert_eq!(span.buffer().as_vec()[i], 1);
        }
        for i in (512 + 64 - 12)..1024 {
            assert_eq!(span.buffer().as_vec()[i], 0);
        }

        let data = Buffer::new(64);
        span.read_from_blocks_into_buffer(&mut data.as_vec()[..]);

        for i in 0..64 {
            assert_eq!(data.as_vec()[i], 1);
        }
    }

    /*
     * Beware, if you change these defaults, then you will have to change
     * all the hard coded tests below that use make_upstairs().
     */
    fn make_upstairs() -> Arc<Upstairs> {
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

        Upstairs::new(&opts, 0, def, Arc::new(Guest::new()))
    }

    /*
     * Terrible wrapper, but it allows us to call extent_from_offset()
     * just like the program does.
     */
    fn up_efo(
        up: &Arc<Upstairs>,
        offset: Block,
        num_blocks: u64,
    ) -> Result<Vec<(u64, Block)>> {
        let ddef = up.ddef.lock().unwrap();
        let num_blocks = Block::new_with_ddef(num_blocks, &ddef);
        extent_from_offset(*ddef, offset, num_blocks)
    }

    #[test]
    fn off_to_extent_one_block() {
        let up = make_upstairs();

        for i in 0..100 {
            let exv = vec![extent_tuple(0, i)];
            assert_eq!(up_efo(&up, Block::new_512(i), 1).unwrap(), exv);
        }

        for i in 0..100 {
            let exv = vec![extent_tuple(1, i)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1).unwrap(), exv);
        }

        let exv = vec![extent_tuple(2, 0)];
        assert_eq!(up_efo(&up, Block::new_512(200), 1).unwrap(), exv);

        let exv = vec![extent_tuple(9, 99)];
        assert_eq!(up_efo(&up, Block::new_512(999), 1).unwrap(), exv);
    }

    #[test]
    fn off_to_extent_two_blocks() {
        let up = make_upstairs();

        for i in 0..99 {
            let exv = vec![extent_tuple(0, i), extent_tuple(0, i + 1)];
            assert_eq!(up_efo(&up, Block::new_512(i), 2).unwrap(), exv);
        }

        let exv = vec![extent_tuple(0, 99), extent_tuple(1, 0)];
        assert_eq!(up_efo(&up, Block::new_512(99), 2).unwrap(), exv);

        for i in 0..99 {
            let exv = vec![extent_tuple(1, i)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1).unwrap(), exv);
        }

        let exv = vec![extent_tuple(1, 99), extent_tuple(2, 0)];
        assert_eq!(up_efo(&up, Block::new_512(199), 2).unwrap(), exv);

        let exv = vec![extent_tuple(2, 0), extent_tuple(2, 1)];
        assert_eq!(up_efo(&up, Block::new_512(200), 2).unwrap(), exv);

        let exv = vec![extent_tuple(9, 98), extent_tuple(9, 99)];
        assert_eq!(up_efo(&up, Block::new_512(998), 2).unwrap(), exv);
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
            up_efo(&up, Block::new_512(99), 2).unwrap(),
            vec![extent_tuple(0, 99), extent_tuple(1, 0)],
        );
        assert_eq!(
            up_efo(&up, Block::new_512(98), 4).unwrap(),
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
            assert_eq!(
                up_efo(&up, Block::new_512(offset), 100).unwrap(),
                expected
            );
        }
    }

    /*
     * Testing various invalid inputs
     */
    #[test]
    #[should_panic]
    fn off_to_extent_length_zero() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 0).unwrap();
    }

    #[test]
    fn off_to_extent_length_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1000).unwrap();
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(0), 1001).unwrap();
    }

    #[test]
    fn off_to_extent_length_and_offset_almost_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(900), 100).unwrap();
    }

    #[test]
    #[should_panic]
    fn off_to_extent_length_and_offset_too_big() {
        let up = make_upstairs();
        up_efo(&up, Block::new_512(900), 101).unwrap();
    }

    #[test]
    #[should_panic]
    fn not_right_block_size() {
        let up = make_upstairs();
        up_efo(&up, Block::new(900 * 4096, 4096), 101).unwrap();
    }

    // key material made with `openssl rand -base64 32`
    #[test]
    pub fn test_upstairs_encryption_context_ok() -> Result<()> {
        use rand::{thread_rng, Rng};

        let key_bytes =
            base64::decode("ClENKTXD2bCyXSHnKXY7GGnk+NvQKbwpatjWP2fJzk0=")
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

        let key_bytes =
            base64::decode("EVrH+ABhMP0MLfxynCalDq1vWCCWCWFfsSsJoJeDCx8=")
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

        let key_bytes =
            base64::decode("EVrH+ABhMP0MLfxynCalDq1vWCCWCWFfsSsJoJeDCx8=")
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

    #[test]
    fn work_flush_three_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let op = create_flush(next_id, vec![], 10, 0, 0, None);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);
        assert_eq!(ds.completed.len(), 0);

        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        ds.ack(next_id);

        assert!(!ds
            .process_ds_completion(
                next_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 1);
    }

    #[test]
    fn work_flush_one_error_then_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let op = create_flush(next_id, vec![], 10, 0, 0, None);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(ds
            .process_ds_completion(
                next_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);

        ds.ack(next_id);
        ds.retire_check(next_id);

        assert_eq!(ds.completed.len(), 1);
    }

    #[test]
    fn work_flush_two_errors_equals_fail() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let op = create_flush(next_id, vec![], 10, 0, 0, None);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(ds
            .process_ds_completion(
                next_id,
                2,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);

        ds.ack(next_id);
        ds.retire_check(next_id);

        assert_eq!(ds.completed.len(), 1);
    }

    #[test]
    fn work_read_one_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);
        assert_eq!(ds.completed.len(), 0);

        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        ds.ack(next_id);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(!ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(!ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        // A flush is required to move work to completed
        assert_eq!(ds.completed.len(), 0);
    }

    #[test]
    fn work_read_one_bad_two_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);
        assert_eq!(ds.completed.len(), 0);

        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        ds.ack(next_id);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(!ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        // A flush is required to move work to completed
        // That this is still zero is part of the test
        assert_eq!(ds.completed.len(), 0);
    }

    #[test]
    fn work_read_two_bad_one_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);

        ds.ack(next_id);
        ds.retire_check(next_id);

        // A flush is required to move work to completed
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);
    }

    #[test]
    fn work_read_three_bad() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request]);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        assert!(ds
            .process_ds_completion(
                next_id,
                2,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);

        ds.ack(next_id);
        ds.retire_check(next_id);

        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);
    }

    #[test]
    fn work_read_two_ok_one_bad() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let next_id = {
            let mut ds = upstairs.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op =
                create_read_eob(next_id, vec![], 10, vec![request.clone()]);

            ds.enqueue(op);

            ds.in_progress(next_id, 0);
            ds.in_progress(next_id, 1);
            ds.in_progress(next_id, 2);

            next_id
        };

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        assert!(upstairs
            .process_ds_operation(next_id, 2, response.clone())
            .unwrap());

        assert!(!upstairs.process_ds_operation(next_id, 0, response).unwrap());

        {
            // emulated run in up_ds_listen

            let mut ds = upstairs.downstairs.lock().unwrap();
            let state = ds.active.get_mut(&next_id).unwrap().ack_status;
            assert_eq!(state, AckStatus::AckReady);
            ds.ack(next_id);

            ds.retire_check(next_id);
        }

        assert!(!upstairs
            .process_ds_operation(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string()))
            )
            .unwrap());

        {
            let mut ds = upstairs.downstairs.lock().unwrap();
            assert_eq!(ds.ackable_work().len(), 0);
            // Work won't be completed until we get a flush.
            assert_eq!(ds.completed.len(), 0);
        }
    }

    #[test]
    fn work_read_hash_mismatch() {
        // Test that a hash mismatch will trigger a panic.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[9])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // We must move the completed job along the process, this enables
        // process_ds_completion to know to compare future jobs to this
        // one.
        ds.ack(id);

        // Second read response, different hash
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_ack() {
        // Test that a hash mismatch will trigger a panic.
        // We check here after a ACK, because that is a different location.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[0])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // We must move the completed job along the process, this enables
        // process_ds_completion to know to compare future jobs to this
        // one.
        ds.ack(id);

        // Second read response, it matches the first.
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_third() {
        // Test that a hash mismatch on the third response will trigger a panic.
        let target = vec![];
        let mut ds = Downstairs::new(target);

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);
        ds.in_progress(id, 2);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // Second read response, it matches the first.
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            .unwrap();

        let r3 = Ok(vec![ReadResponse::from_request_with_data(&request, &[2])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 2, r3, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_third_ack() {
        // Test that a hash mismatch on the third response will trigger a panic.
        // This one checks after an ACK.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);
        ds.in_progress(id, 2);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // Second read response, it matches the first.
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        ds.ack(id);
        ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            .unwrap();

        let r3 = Ok(vec![ReadResponse::from_request_with_data(&request, &[2])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 2, r3, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_inside() {
        // Test that a hash length mismatch will panic
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[1, 2, 3, 4],
        )]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // Second read response, hash vec has different length/
        let r2 = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[1, 2, 3, 9],
        )]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_no_data() {
        // Test that empty data first, then data later will trigger
        // hash mismatch panic.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // Second read response, hash vec has different length/
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_read_hash_mismatch_no_data_next() {
        // Test that missing data on the 2nd read response will panic
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        ds.in_progress(id, 0);
        ds.in_progress(id, 1);

        // Generate the first read response, this will be what we compare
        // future responses with.
        let r1 = Ok(vec![ReadResponse::from_request_with_data(&request, &[1])]);

        ds.process_ds_completion(id, 0, r1, &None, UpState::Active)
            .unwrap();

        // Second read response, hash vec has different length/
        let r2 = Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(id, 1, r2, &None, UpState::Active)
            }));
        assert!(result.is_err());
    }

    #[test]
    fn work_transfer_of_read_after_downstairs_write_errors() {
        work_transfer_of_read_after_downstairs_errors(false);
    }

    #[test]
    fn work_ransfer_of_read_after_downstairs_write_unwritten_errors() {
        work_transfer_of_read_after_downstairs_errors(true);
    }

    // Instead of copying all the write tests, we put a wrapper around them
    // that takes write_unwritten as an arg.
    fn work_transfer_of_read_after_downstairs_errors(is_write_unwritten: bool) {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        // send a write, and clients 0 and 1 will return errors

        let op = create_write_eob(
            next_id,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );

        ds.enqueue(op);

        assert!(ds.in_progress(next_id, 0).is_some());
        assert!(ds.in_progress(next_id, 1).is_some());
        assert!(ds.in_progress(next_id, 2).is_some());

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        let response = Ok(vec![]);

        assert!(ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());

        assert!(ds.downstairs_errors.get(&0).is_some());
        assert!(ds.downstairs_errors.get(&1).is_some());
        assert!(ds.downstairs_errors.get(&2).is_none());

        // another read. Make sure only client 2 returns data.
        // The others should be skipped.

        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        assert!(ds.in_progress(next_id, 0).is_none());
        assert!(ds.in_progress(next_id, 1).is_none());
        assert!(ds.in_progress(next_id, 2).is_some());

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[3])]);

        assert!(ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());

        let responses = ds.active.get(&next_id).unwrap().data.as_ref();
        assert!(responses.is_some());
        assert_eq!(
            responses.map(|responses| responses
                .iter()
                .map(|response| response.data.clone().freeze())
                .collect()),
            Some(vec![Bytes::from_static(&[3])]),
        );
    }

    #[test]
    fn work_assert_reads_do_not_cause_failure_state_transition() {
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        // send a read, and clients 0 and 1 will return errors

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        assert!(ds.in_progress(next_id, 0).is_some());
        assert!(ds.in_progress(next_id, 1).is_some());
        assert!(ds.in_progress(next_id, 2).is_some());

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[3])]);

        assert!(ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());

        let responses = ds.active.get(&next_id).unwrap().data.as_ref();
        assert!(responses.is_some());
        assert_eq!(
            responses.map(|responses| responses
                .iter()
                .map(|response| response.data.clone().freeze())
                .collect()),
            Some(vec![Bytes::from_static(&[3])]),
        );

        assert!(ds.downstairs_errors.get(&0).is_none());
        assert!(ds.downstairs_errors.get(&1).is_none());
        assert!(ds.downstairs_errors.get(&2).is_none());

        // send another read, and expect all to return something
        // (reads shouldn't cause a Failed transition)

        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        assert!(ds.in_progress(next_id, 0).is_some());
        assert!(ds.in_progress(next_id, 1).is_some());
        assert!(ds.in_progress(next_id, 2).is_some());

        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        assert!(!ds
            .process_ds_completion(
                next_id,
                1,
                Err(CrucibleError::GenericError("bad".to_string())),
                &None,
                UpState::Active,
            )
            .unwrap());

        assert!(ds.active.get(&next_id).unwrap().data.is_none());

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[6])]);

        assert!(ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());

        let responses = ds.active.get(&next_id).unwrap().data.as_ref();
        assert!(responses.is_some());
        assert_eq!(
            responses.map(|responses| responses
                .iter()
                .map(|response| response.data.clone().freeze())
                .collect()),
            Some(vec![Bytes::from_static(&[6])]),
        );
    }

    #[test]
    fn work_completed_read_flush() {
        // Verify that a read remains on the active queue until a flush
        // comes through and clears it.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Build our read, put it into the work queue
        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);

        // Move the work to submitted like we sent it to each downstairs
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Downstairs 0 now has completed this work.
        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // One completion of a read means we can ACK
        assert_eq!(ds.ackable_work().len(), 1);

        // Complete downstairs 1 and 2
        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(!ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(!ds
            .process_ds_completion(next_id, 2, response, &None, UpState::Active)
            .unwrap());

        // Make sure the job is still active
        assert_eq!(ds.completed.len(), 0);

        // The job should still be ack ready
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Ack the job to the guest
        ds.ack(next_id);

        // Nothing left to ACK, but untill the flush we keep the IO data.
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        // A flush is required to move work to completed
        // Create the flush then send it to all downstairs.
        let next_id = ds.next_id();
        let op = create_flush(next_id, vec![], 10, 0, 0, None);

        ds.enqueue(op);

        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the Flush at each downstairs.
        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        // Two completed means we return true (ack ready now)
        assert!(ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(!ds
            .process_ds_completion(
                next_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // ACK the flush and let retire_check move things along.
        ds.ack(next_id);
        ds.retire_check(next_id);

        // Verify no more work to ack.
        assert_eq!(ds.ackable_work().len(), 0);
        // The read and the flush should now be moved to completed.
        assert_eq!(ds.completed.len(), 2);
    }

    #[test]
    fn work_delay_completion_flush_write() {
        work_delay_completion_flush(false);
    }

    #[test]
    fn work_delay_completion_flush_write_unwritten() {
        work_delay_completion_flush(true);
    }

    fn work_delay_completion_flush(is_write_unwritten: bool) {
        // Verify that a write/write_unwritten remains on the active
        // queue until a flush comes through and clears it.  In this case,
        // we only complete 2/3 for each IO.  We later come back and finish
        // the 3rd IO and the flush, which then allows the work to be
        // completed.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create two writes, put them on the work queue
        let id1 = ds.next_id();
        let id2 = ds.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        let op = create_write_eob(
            id2,
            vec![],
            1,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Simulate sending both writes to downstairs 0 and 1
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());
        assert!(ds.in_progress(id2, 0).is_some());
        assert!(ds.in_progress(id2, 1).is_some());

        // Simulate completing both writes to downstairs 0 and 1
        assert!(!ds
            .process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(!ds
            .process_ds_completion(id2, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id2, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // Both writes can now ACK to the guest.
        ds.ack(id1);
        ds.ack(id2);

        // Work stays on active queue till the flush
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        // Create the flush, put on the work queue
        let flush_id = ds.next_id();
        let op = create_flush(flush_id, vec![], 10, 0, 0, None);
        ds.enqueue(op);

        // Simulate sending the flush to downstairs 0 and 1
        ds.in_progress(flush_id, 0);
        ds.in_progress(flush_id, 1);

        // Simulate completing the flush to downstairs 0 and 1
        assert!(!ds
            .process_ds_completion(
                flush_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(ds
            .process_ds_completion(
                flush_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        // Ack the flush back to the guest
        ds.ack(flush_id);

        // Make sure downstairs 0 and 1 update their last flush id and
        // that downstairs 2 does not.
        assert_eq!(ds.ds_last_flush[0], flush_id);
        assert_eq!(ds.ds_last_flush[1], flush_id);
        assert_eq!(ds.ds_last_flush[2], 0);

        // Should not retire yet.
        ds.retire_check(flush_id);

        assert_eq!(ds.ackable_work().len(), 0);

        // Make sure all work is still on the active side
        assert_eq!(ds.completed.len(), 0);

        // Now, finish the writes to downstairs 2
        assert!(ds.in_progress(id1, 2).is_some());
        assert!(ds.in_progress(id2, 2).is_some());
        assert!(!ds
            .process_ds_completion(id1, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(!ds
            .process_ds_completion(id2, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // The job should not move to completed until the flush goes as well.
        assert_eq!(ds.completed.len(), 0);

        // Complete the flush on downstairs 2.
        ds.in_progress(flush_id, 2);
        assert!(!ds
            .process_ds_completion(
                flush_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        // All three jobs should now move to completed
        assert_eq!(ds.completed.len(), 3);
        // Downstairs 2 should update the last flush it just did.
        assert_eq!(ds.ds_last_flush[2], flush_id);
    }

    #[test]
    fn work_completed_write_flush() {
        work_completed_writeio_flush(false);
    }

    #[test]
    fn work_completed_write_unwritten_flush() {
        work_completed_writeio_flush(true);
    }

    fn work_completed_writeio_flush(is_write_unwritten: bool) {
        // Verify that a write or write_unwritten remains on the active
        // queue until a flush comes through and clears it.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Build our write IO.
        let next_id = ds.next_id();

        let op = create_write_eob(
            next_id,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        // Put the write on the queue.
        ds.enqueue(op);

        // Submit the write to all three downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the write on all three downstairs.
        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(!ds
            .process_ds_completion(
                next_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        // Ack the write to the guest
        ds.ack(next_id);

        // Work stays on active queue till the flush
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        // Create the flush IO
        let next_id = ds.next_id();
        let op = create_flush(next_id, vec![], 10, 0, 0, None);
        ds.enqueue(op);

        // Submit the flush to all three downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the flush on all three downstairs.
        assert!(!ds
            .process_ds_completion(
                next_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(ds
            .process_ds_completion(
                next_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(!ds
            .process_ds_completion(
                next_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        ds.ack(next_id);
        ds.retire_check(next_id);

        assert_eq!(ds.ackable_work().len(), 0);
        // The write and flush should now be completed.
        assert_eq!(ds.completed.len(), 2);
    }

    #[test]
    fn work_delay_completion_flush_order_write() {
        work_delay_completion_flush_order(false);
    }
    #[test]
    fn work_delay_completion_flush_order_write_unwritten() {
        work_delay_completion_flush_order(true);
    }

    fn work_delay_completion_flush_order(is_write_unwritten: bool) {
        // Verify that a write/write_unwritten remains on the active queue
        // until a flush comes through and clears it.  In this case, we only
        // complete 2 of 3 for each IO.  We later come back and finish the
        // 3rd IO and the flush, which then allows the work to be completed.
        // Also, we mix up which client finishes which job first.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Build two writes, put them on the work queue.
        let id1 = ds.next_id();
        let id2 = ds.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        let op = create_write_eob(
            id2,
            vec![],
            1,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Submit the two writes, to 2/3 of the downstairs.
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());
        assert!(ds.in_progress(id2, 1).is_some());
        assert!(ds.in_progress(id2, 2).is_some());

        // Complete the writes that we sent to the 2 downstairs.
        assert!(!ds
            .process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(!ds
            .process_ds_completion(id2, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id2, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // Ack the writes to the guest.
        ds.ack(id1);
        ds.ack(id2);

        // Work stays on active queue till the flush.
        assert_eq!(ds.ackable_work().len(), 0);
        assert_eq!(ds.completed.len(), 0);

        // Create and enqueue the flush.
        let flush_id = ds.next_id();
        let op = create_flush(flush_id, vec![], 10, 0, 0, None);
        ds.enqueue(op);

        // Send the flush to two downstairs.
        ds.in_progress(flush_id, 0);
        ds.in_progress(flush_id, 2);

        // Complete the flush on those downstairs.
        assert!(!ds
            .process_ds_completion(
                flush_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());
        assert!(ds
            .process_ds_completion(
                flush_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        // Ack the flush
        ds.ack(flush_id);

        // Should not retire yet
        ds.retire_check(flush_id);

        assert_eq!(ds.ackable_work().len(), 0);
        // Not done yet, until all clients do the work.
        assert_eq!(ds.completed.len(), 0);

        // Verify who has updated their last flush.
        assert_eq!(ds.ds_last_flush[0], flush_id);
        assert_eq!(ds.ds_last_flush[1], 0);
        assert_eq!(ds.ds_last_flush[2], flush_id);

        // Now, finish sending and completing the writes
        assert!(ds.in_progress(id1, 2).is_some());
        assert!(ds.in_progress(id2, 0).is_some());
        assert!(!ds
            .process_ds_completion(id1, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(!ds
            .process_ds_completion(id2, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // Completed work won't happen till the last flush is done
        assert_eq!(ds.completed.len(), 0);

        // Send and complete the flush
        ds.in_progress(flush_id, 1);
        assert!(!ds
            .process_ds_completion(
                flush_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active
            )
            .unwrap());

        // Now, all three jobs (w,w,f) will move to completed.
        assert_eq!(ds.completed.len(), 3);

        // downstairs 1 should now have that flush
        assert_eq!(ds.ds_last_flush[1], flush_id);
    }

    #[test]
    fn work_completed_read_replay() {
        // Verify that a single read will replay and move back from AckReady
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Build our read IO and submit it to the work queue.
        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        ds.enqueue(op);

        // Submit the read to all three downstairs
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the read on one downstairs.
        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // One completion should allow for an ACK
        assert_eq!(ds.ackable_work().len(), 1);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Be sure the job is not yet in replay
        assert!(!ds.active.get_mut(&next_id).unwrap().replay);
        ds.re_new(0);
        // Now the IO should be replay
        assert!(ds.active.get_mut(&next_id).unwrap().replay);

        // The act of taking a downstairs offline should move a read
        // back from AckReady if it was the only completed read.
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);
    }

    #[test]
    fn work_completed_two_read_replay() {
        // Verify that a read will replay and move not back from AckReady if
        // there is more than one done read.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Build a read and put it on the work queue.
        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        ds.enqueue(op);

        // Submit the read to each downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the read on one downstairs, verify it is ack ready.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[1, 2, 3, 4],
        )]);
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Complete the read on a 2nd downstairs.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[1, 2, 3, 4],
        )]);
        assert!(!ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());

        // Now, take the first downstairs offline.
        ds.re_new(0);

        // Should still be ok to ACK this IO
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Taking the second downstairs offline should revert the ACK.
        ds.re_new(1);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);

        // Redo the read on DS 0, IO should go back to ackable.
        ds.in_progress(next_id, 0);

        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 1);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
    }

    #[test]
    fn work_completed_ack_read_replay() {
        // Verify that a read we Acked will still replay if that downstairs
        // goes away. Make sure everything still finishes ok.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create the read and put it on the work queue.
        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        ds.enqueue(op);

        // Submit the read to each downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Complete the read on one downstairs.
        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // Verify the read is now AckReady
        assert_eq!(ds.ackable_work().len(), 1);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Ack the read to the guest.
        ds.ack(next_id);

        // Should not retire yet
        ds.retire_check(next_id);

        // No new ackable work.
        assert_eq!(ds.ackable_work().len(), 0);
        // Verify the IO has not completed yet.
        assert_eq!(ds.completed.len(), 0);

        // Now, take that downstairs offline
        ds.re_new(0);

        // Acked IO should remain so.
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);

        // Redo on DS 0, IO should remain acked.
        ds.in_progress(next_id, 0);
        let response =
            Ok(vec![ReadResponse::from_request_with_data(&request, &[])]);
        assert!(!ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());
        assert_eq!(ds.ackable_work().len(), 0);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);
    }

    // ZZZ another test that does read replay for the first IO
    // Also, the third IO?
    #[test]
    fn work_completed_ack_read_replay_hash_mismatch() {
        // Verify that a read replay won't cause a panic on hash mismatch.
        // During a replay, the same block may have been written after a read,
        // so the actual contents of the block are now different.  We can't
        // compare the read hash taken before a replay to one that happens
        // after.  A sample sequence is:
        //
        // Flush (all previous IO cleared, all blocks the same)
        // Read block 0
        // Write block 0
        // - Downstairs goes away here, causing a replay.
        //
        // In this case, the replay read will get the data again, but that
        // data came from the write, which is different than the original,
        // pre-replay read data (and hash).  In this case we can't compare
        // with the original hash that we stored for this read.
        //
        // For the test below, we don't actually need to do a write, we
        // can just change the "data" we fill the response with like we
        // received different data than the original read.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create the read and put it on the work queue.
        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        ds.enqueue(op);

        // Submit the read to each downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Construct our fake response
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[122], // Original data.
        )]);

        // Complete the read on one downstairs.
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // Ack the read to the guest.
        ds.ack(next_id);

        // Before re re_new, the IO is not replay
        assert!(!ds.active.get_mut(&next_id).unwrap().replay);
        // Now, take that downstairs offline
        ds.re_new(0);
        // Now the IO should be replay
        assert!(ds.active.get_mut(&next_id).unwrap().replay);

        // Move it to in-progress.
        ds.in_progress(next_id, 0);

        // Now, create a new response that has different data, and will
        // produce a different hash.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[123], // Different data than before
        )]);

        // Process the new read (with different data), make sure we don't
        // trigger the hash mismatch check.
        assert!(!ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // Some final checks.  The replay should behave in every other way
        // like a regular read.
        assert_eq!(ds.ackable_work().len(), 0);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);
    }

    #[test]
    fn work_completed_ack_read_replay_two_hash_mismatch() {
        // Verify that a read replay won't cause a panic on hash mismatch.
        // During a replay, the same block may have been written after a read,
        // so the actual contents of the block are now different.  We can't
        // compare the read hash taken before a replay to one that happens
        // after.  A sample sequence is:
        //
        // Flush (all previous IO cleared, all blocks the same)
        // Read block 0
        // Write block 0
        //
        // In this case, the replay read will get the data again, but that
        // data came from the write, which is different than the original,
        // pre-replay read data (and hash).  In this case we can't compare
        // with the original hash that we stored for this read.
        //
        // For the test below, we don't actually need to do a write, we
        // can just change the "data" we fill the response with like we
        // received different data than the original read.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create the read and put it on the work queue.
        let next_id = ds.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        ds.enqueue(op);

        // Submit the read to each downstairs.
        ds.in_progress(next_id, 0);
        ds.in_progress(next_id, 1);
        ds.in_progress(next_id, 2);

        // Construct our fake response
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[122], // Original data.
        )]);

        // Complete the read on one downstairs.
        assert!(ds
            .process_ds_completion(next_id, 0, response, &None, UpState::Active)
            .unwrap());

        // Construct our fake response for another downstairs.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[122], // Original data.
        )]);

        // Complete the read on the this downstairs as well
        assert!(!ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());

        // Ack the read to the guest.
        ds.ack(next_id);

        // Now, take the second downstairs offline
        ds.re_new(1);
        // Now the IO should be replay
        assert!(ds.active.get_mut(&next_id).unwrap().replay);

        // Move it to in-progress.
        ds.in_progress(next_id, 1);

        // Now, create a new response that has different data, and will
        // produce a different hash.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &[123], // Different data than before
        )]);

        // Process the new read (with different data), make sure we don't
        // trigger the hash mismatch check.
        assert!(!ds
            .process_ds_completion(next_id, 1, response, &None, UpState::Active)
            .unwrap());

        // Some final checks.  The replay should behave in every other way
        // like a regular read.
        assert_eq!(ds.ackable_work().len(), 0);
        let state = ds.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);
    }

    #[test]
    fn work_completed_write_ack_ready_replay_write() {
        work_completed_write_ack_ready_replay(false);
    }

    #[test]
    fn work_completed_write_ack_ready_replay_write_unwritten() {
        work_completed_write_ack_ready_replay(true);
    }

    fn work_completed_write_ack_ready_replay(is_write_unwritten: bool) {
        // Verify that a replay when we have two completed writes or
        // write_unwritten will change state from AckReady back to NotAcked.
        // If we then redo the work, it should go back to AckReady.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create the write and put it on the work queue.
        let id1 = ds.next_id();
        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Submit the read to two downstairs.
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());

        // Complete the write on two downstairs.
        assert!(!ds
            .process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // Verify AckReady
        let state = ds.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        /* Now, take that downstairs offline */
        // Before re re_new, the IO is not replay
        assert!(!ds.active.get_mut(&id1).unwrap().replay);
        ds.re_new(1);
        // Now the IO should be replay
        assert!(ds.active.get_mut(&id1).unwrap().replay);

        // State goes back to NotAcked
        let state = ds.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);

        // Re-submit and complete the write
        assert!(ds.in_progress(id1, 1).is_some());
        assert!(ds
            .process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // State should go back to acked.
        let state = ds.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
    }

    #[test]
    fn work_completed_write_acked_replay_write() {
        work_completed_write_acked_replay(false);
    }
    #[test]
    fn work_completed_write_acked_replay_write_unwritten() {
        work_completed_write_acked_replay(true);
    }

    fn work_completed_write_acked_replay(is_write_unwritten: bool) {
        // Verify that a replay when we have acked a write or write_unwritten
        // will not undo that ack.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        // Create the write and put it on the work queue.
        let id1 = ds.next_id();
        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Submit the write to two downstairs.
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());

        // Complete the write on two downstairs.
        assert!(!ds
            .process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(ds
            .process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap());

        // Verify it is ackable..
        assert_eq!(ds.ackable_work().len(), 1);

        // Send the ACK to the guest
        ds.ack(id1);

        // Verify no more ackable work
        assert_eq!(ds.ackable_work().len(), 0);

        // Now, take that downstairs offline
        ds.re_new(0);

        // State should stay acked
        let state = ds.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);

        // Finish the write all the way out.
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 2).is_some());

        assert!(!ds
            .process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap());
        assert!(!ds
            .process_ds_completion(id1, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap());
    }

    #[test]
    fn downstairs_transition_normal() {
        // Verify the correct downstairs progression
        // New -> WA -> WQ -> Active
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::Active);
    }

    #[test]
    fn downstairs_transition_replay() {
        // Verify offline goes to replay
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.set_active().unwrap();
        up.ds_transition(0, DsState::Active);
        up.ds_transition(0, DsState::Offline);
        up.ds_transition(0, DsState::Replay);
    }
    #[test]
    fn downstairs_transition_deactivate() {
        // Verify deactivate goes to new
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::Active);
        up.set_active().unwrap();
        up.ds_transition(0, DsState::Deactivated);
        up.ds_transition(0, DsState::New);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_new() {
        // Verify deactivate goes to new
        let up = Upstairs::default();
        up.ds_transition(0, DsState::Deactivated);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_wa() {
        // Verify no deactivate from wa
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::Deactivated);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_deactivate_not_wq() {
        // Verify no deactivate from wq
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::Deactivated);
    }

    // Deactivate tests
    #[test]
    fn deactivate_after_work_completed_write() {
        deactivate_after_work_completed(false);
    }

    #[test]
    fn deactivate_after_work_completed_write_unwritten() {
        deactivate_after_work_completed(true);
    }

    fn deactivate_after_work_completed(is_write_unwritten: bool) {
        // Verify that submitted IO will continue after a deactivate.
        // Verify that the flush takes three completions.
        // Verify that deactivate done returns the upstairs to init.

        let up = Upstairs::default();
        up.set_active().unwrap();
        let mut ds = up.downstairs.lock().unwrap();
        ds.ds_state[0] = DsState::Active;
        ds.ds_state[1] = DsState::Active;
        ds.ds_state[2] = DsState::Active;

        // Build a write, put it on the work queue.
        let id1 = ds.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Submit the writes
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());
        assert!(ds.in_progress(id1, 2).is_some());

        drop(ds);

        // Create and enqueue the flush by setting deactivate
        // The created flush should be the next ID
        up.set_deactivate(None).unwrap();
        let flush_id = id1 + 1;

        ds = up.downstairs.lock().unwrap();
        // Complete the writes
        ds.process_ds_completion(id1, 0, Ok(vec![]), &None, UpState::Active)
            .unwrap();
        ds.process_ds_completion(id1, 1, Ok(vec![]), &None, UpState::Active)
            .unwrap();
        ds.process_ds_completion(id1, 2, Ok(vec![]), &None, UpState::Active)
            .unwrap();

        // Ack the writes to the guest.
        ds.ack(id1);

        // Send the flush created for us when we set deactivated to
        // the two downstairs.
        ds.in_progress(flush_id, 0);
        ds.in_progress(flush_id, 2);

        // Complete the flush on those downstairs.
        // One flush won't result in an ACK
        assert!(!ds
            .process_ds_completion(
                flush_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Deactivating
            )
            .unwrap());

        // The 2nd ack when disconnecting still won't trigger an ack.
        assert!(!ds
            .process_ds_completion(
                flush_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Deactivating
            )
            .unwrap());

        // Verify we can deactivate the completed DS
        drop(ds);
        assert!(up.ds_deactivate(0));
        assert!(up.ds_deactivate(2));

        // Verify the remaining DS can not deactivate
        assert!(!up.ds_deactivate(1));

        // Verify the deactivate is not done yet.
        up.deactivate_transition_check();
        assert!(up.is_deactivating());

        ds = up.downstairs.lock().unwrap();
        // Make sure the correct DS have changed state.
        assert_eq!(ds.ds_state[0], DsState::Deactivated);
        assert_eq!(ds.ds_state[2], DsState::Deactivated);
        assert_eq!(ds.ds_state[1], DsState::Active);

        // Send and complete the flush
        ds.in_progress(flush_id, 1);
        assert!(ds
            .process_ds_completion(
                flush_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Deactivating
            )
            .unwrap());
        // Ack the flush..
        ds.ack(flush_id);

        drop(ds);
        assert!(up.ds_deactivate(1));

        // Report all three DS as missing, which moves them to New
        up.ds_missing(0);
        up.ds_missing(1);
        up.ds_missing(2);

        // Verify we have disconnected and can go back to init.
        up.deactivate_transition_check();
        assert!(!up.is_deactivating());

        // Verify after the ds_missing, all downstairs are New
        let ds = up.downstairs.lock().unwrap();
        assert_eq!(ds.ds_state[0], DsState::New);
        assert_eq!(ds.ds_state[1], DsState::New);
        assert_eq!(ds.ds_state[2], DsState::New);
    }

    #[test]
    fn deactivate_when_empty() {
        // Verify we can deactivate if no work is present, without
        // creating a flush (as their should already have been one).
        // Verify after all three downstairs are deactivated, we can
        // transition the upstairs back to init.

        let up = Upstairs::default();
        up.set_active().unwrap();
        let mut ds = up.downstairs.lock().unwrap();
        ds.ds_state[0] = DsState::Active;
        ds.ds_state[1] = DsState::Active;
        ds.ds_state[2] = DsState::Active;

        drop(ds);
        up.set_deactivate(None).unwrap();

        // Verify we can deactivate as there is no work
        assert!(up.ds_deactivate(0));
        assert!(up.ds_deactivate(1));
        assert!(up.ds_deactivate(2));

        ds = up.downstairs.lock().unwrap();
        // Make sure the correct DS have changed state.
        assert_eq!(ds.ds_state[0], DsState::Deactivated);
        assert_eq!(ds.ds_state[1], DsState::Deactivated);
        assert_eq!(ds.ds_state[2], DsState::Deactivated);
        drop(ds);

        // Mark all three DS as missing, which moves their state to New
        up.ds_missing(0);
        up.ds_missing(1);
        up.ds_missing(2);

        // Verify now we can go back to init.
        up.deactivate_transition_check();
        assert!(!up.is_deactivating());
    }

    #[test]
    fn deactivate_not_without_flush_write() {
        deactivate_not_without_flush(false);
    }

    #[test]
    fn deactivate_not_without_flush_write_unwritten() {
        deactivate_not_without_flush(true);
    }

    fn deactivate_not_without_flush(is_write_unwritten: bool) {
        // Verify that we can't deactivate without a flush as the
        // last job on the list

        let up = Upstairs::default();
        up.set_active().unwrap();
        let mut ds = up.downstairs.lock().unwrap();
        ds.ds_state[0] = DsState::Active;
        ds.ds_state[1] = DsState::Active;
        ds.ds_state[2] = DsState::Active;

        // Build a write, put it on the work queue.
        let id1 = ds.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                encryption_context: None,
                hash: 0,
            }],
            is_write_unwritten,
        );
        ds.enqueue(op);

        // Submit the writes
        assert!(ds.in_progress(id1, 0).is_some());
        assert!(ds.in_progress(id1, 1).is_some());
        assert!(ds.in_progress(id1, 2).is_some());

        drop(ds);
        up.set_deactivate(None).unwrap();
        ds = up.downstairs.lock().unwrap();

        // Complete the writes
        ds.process_ds_completion(
            id1,
            0,
            Ok(vec![]),
            &None,
            UpState::Deactivating,
        )
        .unwrap();
        ds.process_ds_completion(
            id1,
            1,
            Ok(vec![]),
            &None,
            UpState::Deactivating,
        )
        .unwrap();
        ds.process_ds_completion(
            id1,
            2,
            Ok(vec![]),
            &None,
            UpState::Deactivating,
        )
        .unwrap();

        // Ack the writes to the guest.
        ds.ack(id1);

        // Verify we will not transition to deactivated without a flush.
        drop(ds);
        assert!(!up.ds_deactivate(0));
        assert!(!up.ds_deactivate(1));
        assert!(!up.ds_deactivate(2));

        // Verify the deactivate is not done yet.
        up.deactivate_transition_check();
        assert!(up.is_deactivating());

        ds = up.downstairs.lock().unwrap();
        // Make sure no DS have changed state.
        assert_eq!(ds.ds_state[0], DsState::Active);
        assert_eq!(ds.ds_state[2], DsState::Active);
        assert_eq!(ds.ds_state[1], DsState::Active);
    }

    #[test]
    fn deactivate_not_when_active() {
        // Verify that we can't set deactivate on the upstairs when
        // the upstairs is still in init.
        // Verify that we can't set deactivate on the upstairs when
        // we are deactivating.
        // TODO: This test should change when we support this behavior.

        let up = Upstairs::default();
        assert!(up.set_deactivate(None).is_err());
        up.set_active().unwrap();
        up.set_deactivate(None).unwrap();
        assert!(up.set_deactivate(None).is_err());
    }

    #[test]
    fn deactivate_ds_not_when_active() {
        // No ds can deactivate when upstairs is not deactivating.

        let up = Upstairs::default();
        up.set_active().unwrap();
        let mut ds = up.downstairs.lock().unwrap();
        ds.ds_state[0] = DsState::Active;
        ds.ds_state[1] = DsState::Active;
        ds.ds_state[2] = DsState::Active;

        drop(ds);

        // Verify we cannot deactivate even when there is no work
        assert!(!up.ds_deactivate(0));
        assert!(!up.ds_deactivate(1));
        assert!(!up.ds_deactivate(2));

        ds = up.downstairs.lock().unwrap();
        // Make sure no DS have changed state.
        assert_eq!(ds.ds_state[0], DsState::Active);
        assert_eq!(ds.ds_state[1], DsState::Active);
        assert_eq!(ds.ds_state[2], DsState::Active);
    }

    #[test]
    fn deactivate_ds_not_when_initializing() {
        // No deactivate of downstairs when upstairs not active.

        let up = Upstairs::default();

        // Verify we cannot deactivate before the upstairs is active
        assert!(!up.ds_deactivate(0));
        assert!(!up.ds_deactivate(1));
        assert!(!up.ds_deactivate(2));

        let ds = up.downstairs.lock().unwrap();
        // Make sure no DS have changed state.
        assert_eq!(ds.ds_state[0], DsState::New);
        assert_eq!(ds.ds_state[1], DsState::New);
        assert_eq!(ds.ds_state[2], DsState::New);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_wa() {
        // Verify we can't go to the same state we are in
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitActive);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_wq() {
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::WaitQuorum);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_active() {
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::Active);
        up.ds_transition(0, DsState::Active);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_same_offline() {
        let up = Upstairs::default();
        up.ds_transition(0, DsState::Offline);
        up.ds_transition(0, DsState::Offline);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_backwards() {
        // Verify state can't go backwards
        // New -> WA -> WQ -> WA
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);
        up.ds_transition(0, DsState::WaitActive);
    }

    #[test]
    #[should_panic]
    fn downstairs_bad_transition_wq() {
        // Verify error when going straight to WQ
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitQuorum);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_replay() {
        // Verify new goes to replay will fail
        let up = Upstairs::default();
        up.ds_transition(0, DsState::Replay);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_offline() {
        // Verify offline cannot go to WQ
        let up = Upstairs::default();
        up.ds_transition(0, DsState::Offline);
        up.ds_transition(0, DsState::WaitQuorum);
    }

    #[test]
    #[should_panic]
    fn downstairs_transition_bad_active() {
        // Verify offline cannot go to WQ
        let up = Upstairs::default();
        up.ds_transition(0, DsState::Active);
        up.ds_transition(0, DsState::WaitQuorum);
    }

    #[test]
    fn reconcile_not_ready() {
        // Verify reconcile returns false when a downstairs is not ready
        let up = Upstairs::default();
        up.ds_transition(0, DsState::WaitActive);
        up.ds_transition(0, DsState::WaitQuorum);

        up.ds_transition(1, DsState::WaitActive);
        up.ds_transition(1, DsState::WaitQuorum);

        let (ds_work_tx, _) = watch::channel(1);
        let (ds_reconcile_work_tx, _) = watch::channel(1);
        let (ds_active_tx, _) = watch::channel(1);
        let (_, mut ds_reconcile_done_rx) = mpsc::channel::<Repair>(32);
        let t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let dst = Target {
            target: t,
            ds_work_tx,
            ds_active_tx,
            ds_reconcile_work_tx,
        };

        // We just make one target to keep the method happy.
        let d = vec![dst];
        let mut lastcast: u64 = 1;
        let res = tokio_test::block_on(up.connect_region_set(
            &d,
            &mut lastcast,
            &mut ds_reconcile_done_rx,
        ));
        assert!(res.is_ok());
        let active = up.active.lock().unwrap();
        assert_ne!(active.up_state, UpState::Active)
    }

    // Tests for rep_in_progress
    #[test]
    fn reconcile_rep_in_progress_none() {
        // No repairs on the queue, should return None
        let up = Upstairs::default();
        let mut ds = up.downstairs.lock().unwrap();
        ds.ds_state[0] = DsState::Repair;
        ds.ds_state[1] = DsState::Repair;
        ds.ds_state[2] = DsState::Repair;
        let w = ds.rep_in_progress(0);
        assert_eq!(w, None);
    }

    #[test]
    fn reconcile_repair_workflow_not_repair() {
        // Verify that rep_in_progress will not give out work if a
        // downstairs is not in the correct state, and that it will
        // clear the work queue and mark other downstairs as failed.
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            // Put a jobs on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
            // A downstairs is not in Repair state
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::WaitQuorum;
            ds.ds_state[2] = DsState::Repair;
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.is_err());
        let mut ds = up.downstairs.lock().unwrap();
        assert_eq!(ds.ds_state[0], DsState::FailedRepair);
        assert_eq!(ds.ds_state[1], DsState::WaitQuorum);
        assert_eq!(ds.ds_state[2], DsState::FailedRepair);

        // Verify rep_in_progress now returns none for all DS
        assert!(ds.reconcile_task_list.is_empty());
        assert!(!ds.rep_in_progress(0).is_some());
        assert!(!ds.rep_in_progress(1).is_some());
        assert!(!ds.rep_in_progress(2).is_some());
    }

    #[test]
    fn reconcile_repair_workflow_not_repair_later() {
        // Verify that rep_done still works even after we have a downstairs
        // in the FailedRepair state. Verify that attempts to get new work
        // after a failed repair now return none.
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put two jobs on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(1).is_some());
        assert!(ds.rep_in_progress(2).is_some());

        // Now verify we can be done even if a DS is gone
        ds.ds_state[1] = DsState::New;
        // Now, make sure we consider this done only after all three are done
        assert!(!ds.rep_done(0, rep_id));
        assert!(!ds.rep_done(1, rep_id));
        assert!(ds.rep_done(2, rep_id));

        // Getting the next work to do should verify the previous is done,
        // and handle a state change for a downstairs.
        drop(ds);
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.is_err());
        let mut ds = up.downstairs.lock().unwrap();
        assert_eq!(ds.ds_state[0], DsState::FailedRepair);
        assert_eq!(ds.ds_state[1], DsState::New);
        assert_eq!(ds.ds_state[2], DsState::FailedRepair);

        // Verify rep_in_progress now returns none for all DS
        assert!(ds.reconcile_task_list.is_empty());
        assert!(!ds.rep_in_progress(0).is_some());
        assert!(!ds.rep_in_progress(1).is_some());
        assert!(!ds.rep_in_progress(2).is_some());
    }

    #[test]
    fn reconcile_repair_workflow_repair_later() {
        // Verify that a downstairs not in repair mode will ignore new
        // work requests until it transitions to repair.
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(1).is_some());
        ds.ds_state[2] = DsState::New;
        assert!(!ds.rep_in_progress(2).is_some());

        // Okay, now the DS is back and ready for repair, verify it will
        // start taking work.
        ds.ds_state[2] = DsState::Repair;
        assert!(ds.rep_in_progress(2).is_some());
    }

    #[test]
    #[should_panic]
    fn reconcile_rep_in_progress_bad1() {
        // Verify the same downstairs can't mark a job in progress twice
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let _ = tokio_test::block_on(up.new_rec_work());
        let mut ds = up.downstairs.lock().unwrap();
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(0).is_some());
    }

    #[test]
    #[should_panic]
    fn reconcile_rep_done_too_soon() {
        // Verify a job can't go new -> done
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let _ = tokio_test::block_on(up.new_rec_work());
        let mut ds = up.downstairs.lock().unwrap();
        ds.rep_done(0, rep_id);
    }

    #[test]
    fn reconcile_repair_workflow_1() {
        let up = Upstairs::default();
        let mut rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put two jobs on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id + 1,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(1).is_some());
        assert!(ds.rep_in_progress(2).is_some());

        // Now, make sure we consider this done only after all three are done
        assert!(!ds.rep_done(0, rep_id));
        assert!(!ds.rep_done(1, rep_id));
        assert!(ds.rep_done(2, rep_id));

        // Getting the next work to do should verify the previous is done
        drop(ds);
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(1).is_some());
        assert!(ds.rep_in_progress(2).is_some());

        // Now, make sure we consider this done only after all three are done
        rep_id += 1;
        assert!(!ds.rep_done(0, rep_id));
        assert!(!ds.rep_done(1, rep_id));
        assert!(ds.rep_done(2, rep_id));

        drop(ds);
        // Now, we should be empty, so nw is false
        assert!(!tokio_test::block_on(up.new_rec_work()).unwrap());
    }

    #[test]
    #[should_panic]
    fn reconcile_leave_no_job_behind() {
        // Verify we can't start a new job before the old is finished.
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put two jobs on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id + 1,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        assert!(ds.rep_in_progress(1).is_some());
        assert!(ds.rep_in_progress(2).is_some());

        // Now, make sure we consider this done only after all three are done
        assert!(!ds.rep_done(0, rep_id));
        assert!(!ds.rep_done(1, rep_id));
        // don't finish

        // Getting the next work to do should verify the previous is done
        drop(ds);
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
    }

    #[test]
    fn reconcile_repair_workflow_2() {
        // Verify Done or Skipped works for rep_done
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark all three as in progress
        assert!(ds.rep_in_progress(0).is_some());
        if let Some(job) = &mut ds.reconcile_current_work {
            let oldstate = job.state.insert(1, IOState::Skipped);
            assert_eq!(oldstate, Some(IOState::New));
        } else {
            panic!("Failed to find next task");
        }

        assert!(ds.rep_in_progress(2).is_some());

        // Now, make sure we consider this done only after all three are done
        assert!(!ds.rep_done(0, rep_id));
        // This should panic: assert!(!ds.rep_done(1, rep_id));
        assert!(ds.rep_done(2, rep_id));
    }

    #[test]
    #[should_panic]
    fn reconcile_repair_inprogress_not_done() {
        // Verify Done or Skipped works for rep_done
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Mark one as skipped
        if let Some(job) = &mut ds.reconcile_current_work {
            let oldstate = job.state.insert(1, IOState::Skipped);
            assert_eq!(oldstate, Some(IOState::New));
        } else {
            panic!("Failed to find next task");
        }

        // Can't mark done a skipped job
        ds.rep_done(1, rep_id);
    }

    #[test]
    #[should_panic]
    fn reconcile_repair_workflow_too_soon() {
        // Verify that jobs must be in progress before done.
        let up = Upstairs::default();
        let rep_id = 0;
        {
            let mut ds = up.downstairs.lock().unwrap();
            ds.ds_state[0] = DsState::Repair;
            ds.ds_state[1] = DsState::Repair;
            ds.ds_state[2] = DsState::Repair;
            // Put a job on the todo list
            ds.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: 1,
                },
            ));
        }
        // Move that job to next to do.
        let nw = tokio_test::block_on(up.new_rec_work());
        assert!(nw.unwrap());
        let mut ds = up.downstairs.lock().unwrap();
        // Jump straight to done.
        // Now, make sure we consider this done only after all three are done
        ds.rep_done(0, rep_id);
    }

    #[test]
    fn reconcile_rc_to_message() {
        // Convert an extent fix to the crucible repair messages that
        // are sent to the downstairs.  Verify that the resulting
        // messages are what we expect
        let up = Upstairs::default();
        let mut ds = up.downstairs.lock().unwrap();
        let r0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 801);
        let r1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 802);
        let r2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 803);
        ds.ds_repair.insert(0, r0);
        ds.ds_repair.insert(1, r1);
        ds.ds_repair.insert(2, r2);

        let repair_extent = 9;
        let mut rec_list = HashMap::new();
        let ef = ExtentFix {
            source: 0,
            dest: vec![1, 2],
        };
        rec_list.insert(repair_extent, ef);
        let max_flush = 22;
        let max_gen = 33;
        ds.convert_rc_to_messages(rec_list, max_flush, max_gen);

        // Walk the list and check for messages we expect to find
        assert_eq!(ds.reconcile_task_list.len(), 4);

        // First task, flush
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 0);
        match rio.op {
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id,
                flush_number,
                gen_number,
            } => {
                assert_eq!(repair_id, 0);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(client_id, 0);
                assert_eq!(flush_number, max_flush);
                assert_eq!(gen_number, max_gen);
            }
            m => {
                panic!("{:?} not ExtentFlush()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Second task, close extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 1);
        match rio.op {
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, 1);
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Third task, repair extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 2);
        match rio.op {
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                assert_eq!(repair_id, rio.id);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(source_client_id, 0);
                assert_eq!(source_repair_address, r0);
                assert_eq!(dest_clients, vec![1, 2]);
            }
            m => {
                panic!("{:?} not ExtentRepair", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Third task, close extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 3);
        match rio.op {
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, 3);
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));
    }

    #[test]
    fn reconcile_rc_to_message_two() {
        // Convert another extent fix to the crucible repair messages that
        // are sent to the downstairs.  Verify that the resulting
        // messages are what we expect
        let up = Upstairs::default();
        let mut ds = up.downstairs.lock().unwrap();
        let r0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 801);
        let r1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 802);
        let r2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 803);
        ds.ds_repair.insert(0, r0);
        ds.ds_repair.insert(1, r1);
        ds.ds_repair.insert(2, r2);

        let repair_extent = 5;
        let mut rec_list = HashMap::new();
        let ef = ExtentFix {
            source: 2,
            dest: vec![0, 1],
        };
        rec_list.insert(repair_extent, ef);
        let max_flush = 66;
        let max_gen = 77;
        ds.convert_rc_to_messages(rec_list, max_flush, max_gen);

        // Walk the list and check for messages we expect to find
        assert_eq!(ds.reconcile_task_list.len(), 4);

        // First task, flush
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 0);
        match rio.op {
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id,
                flush_number,
                gen_number,
            } => {
                assert_eq!(repair_id, 0);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(client_id, 2);
                assert_eq!(flush_number, max_flush);
                assert_eq!(gen_number, max_gen);
            }
            m => {
                panic!("{:?} not ExtentFlush()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Second task, close extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 1);
        match rio.op {
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, 1);
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Third task, repair extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 2);
        match rio.op {
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                assert_eq!(repair_id, rio.id);
                assert_eq!(extent_id, repair_extent);
                assert_eq!(source_client_id, 2);
                assert_eq!(source_repair_address, r2);
                assert_eq!(dest_clients, vec![0, 1]);
            }
            m => {
                panic!("{:?} not ExtentRepair", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));

        // Third task, close extent
        let rio = ds.reconcile_task_list.pop_front().unwrap();
        assert_eq!(rio.id, 3);
        match rio.op {
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                assert_eq!(repair_id, 3);
                assert_eq!(extent_id, repair_extent);
            }
            m => {
                panic!("{:?} not ExtentClose()", m);
            }
        }
        assert_eq!(Some(&IOState::New), rio.state.get(&0));
        assert_eq!(Some(&IOState::New), rio.state.get(&1));
        assert_eq!(Some(&IOState::New), rio.state.get(&2));
    }

    #[test]
    fn bad_decryption_means_panic() {
        // Failure to decrypt means panic.
        // This result has a valid hash, but won't decrypt.
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        let context = Arc::new(EncryptionContext::new(
            vec![
                0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x1, 0x2, 0x3,
                0x4, 0x5, 0x6, 0x7, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
                0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
            ],
            512,
        ));

        ds.enqueue(op);

        ds.in_progress(next_id, 0);

        // fake read response from downstairs that will fail decryption

        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = context.encrypt_in_place(&mut data).unwrap();

        let nonce = nonce.to_vec();
        let mut tag = tag.to_vec();

        // alter tag
        if tag[3] == 0xFF {
            tag[3] = 0x00;
        } else {
            tag[3] = 0xFF;
        }

        // compute integrity hash after alteration above! It should still
        // validate
        let hash = integrity_hash(&[&nonce, &tag, &data]);

        let response = Ok(vec![ReadResponse {
            eid: request.eid,
            offset: request.offset,

            data: BytesMut::from(&data[..]),
            encryption_contexts: vec![crucible_protocol::EncryptionContext {
                nonce,
                tag,
            }],
            hashes: vec![hash],
        }]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(
                    next_id,
                    0,
                    response,
                    &Some(context),
                    UpState::Active,
                )
            }));
        assert!(result.is_err());
    }

    #[test]
    fn bad_read_hash_means_panic() {
        // Verify that a bad hash on a read will panic
        let upstairs = Upstairs::default();
        upstairs.set_active().unwrap();
        let mut ds = upstairs.downstairs.lock().unwrap();

        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        ds.enqueue(op);
        ds.in_progress(next_id, 0);

        // fake read response from downstairs that will fail integrity hash
        // check

        let data = Vec::from([1u8; 512]);

        let response = Ok(vec![ReadResponse {
            eid: request.eid,
            offset: request.offset,

            data: BytesMut::from(&data[..]),
            encryption_contexts: vec![],
            hashes: vec![
                10000, // junk hash
            ],
        }]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(
                    next_id,
                    0,
                    response,
                    &None,
                    UpState::Active,
                )
            }));
        assert!(result.is_err());
    }

    #[test]
    fn bad_hash_on_encrypted_read_panic() {
        // Verify that a decryption failure on a read will panic.
        let target = vec![];
        let mut ds = Downstairs::new(target);
        let next_id = ds.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        let context = Arc::new(EncryptionContext::new(
            vec![
                0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x1, 0x2, 0x3,
                0x4, 0x5, 0x6, 0x7, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
                0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
            ],
            512,
        ));

        ds.enqueue(op);

        ds.in_progress(next_id, 0);

        // fake read response from downstairs that will fail integrity hash
        // check
        let mut data = Vec::from([1u8; 512]);

        let (nonce, tag, _) = context.encrypt_in_place(&mut data).unwrap();

        let nonce = nonce.to_vec();
        let tag = tag.to_vec();

        let response = Ok(vec![ReadResponse {
            eid: request.eid,
            offset: request.offset,

            data: BytesMut::from(&data[..]),
            encryption_contexts: vec![crucible_protocol::EncryptionContext {
                nonce,
                tag,
            }],
            hashes: vec![
                10000, // junk hash
            ],
        }]);

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ds.process_ds_completion(
                    next_id,
                    0,
                    response,
                    &Some(context),
                    UpState::Active,
                )
            }));
        assert!(result.is_err());
    }

    #[test]
    fn test_no_iop_limit() -> Result<()> {
        let guest = Guest::new();
        guest.set_active();

        assert!(guest.consume_req().is_none());

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(8000),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(16000),
        });

        // With no IOP limit, all requests are consumed immediately
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());

        assert!(guest.consume_req().is_none());

        // If no IOP limit set, don't track it
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[test]
    fn test_set_iop_limit() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();
        guest.set_iop_limit(16000, 2);

        assert!(guest.consume_req().is_none());

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(8000),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(16000),
        });

        // First two reads succeed
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());

        // Next cannot be consumed until there's available IOP tokens so it
        // remains in the queue.
        assert!(guest.consume_req().is_none());
        assert!(!guest.reqs.lock().unwrap().is_empty());
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 2);

        // Replenish one token, meaning next read can be consumed
        guest.leak_iop_tokens(1);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 1);

        assert!(guest.consume_req().is_some());
        assert!(guest.reqs.lock().unwrap().is_empty());
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 2);

        guest.leak_iop_tokens(2);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        guest.leak_iop_tokens(16000);
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[test]
    fn test_flush_does_not_consume_iops() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();

        // Set 0 as IOP limit
        guest.set_iop_limit(16000, 0);
        assert!(guest.consume_req().is_none());

        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });
        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });
        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });

        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());

        assert!(guest.consume_req().is_none());

        Ok(())
    }

    #[test]
    fn test_set_bw_limit() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();
        guest.set_bw_limit(1024 * 1024); // 1 KiB

        assert!(guest.consume_req().is_none());

        // Don't use guest.read, that will send a block size query that will
        // never be answered.
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1024 * 1024 / 2),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1024 * 1024 / 2),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1024 * 1024 / 2),
        });

        // First two reads succeed
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());

        // Next cannot be consumed until there's available BW tokens so it
        // remains in the queue.
        assert!(guest.consume_req().is_none());
        assert!(!guest.reqs.lock().unwrap().is_empty());
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        // Replenish enough tokens, meaning next read can be consumed
        guest.leak_bw_tokens(1024 * 1024 / 2);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024 / 2);

        assert!(guest.consume_req().is_some());
        assert!(guest.reqs.lock().unwrap().is_empty());
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        guest.leak_bw_tokens(1024 * 1024);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        guest.leak_bw_tokens(1024 * 1024 * 1024);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        Ok(())
    }

    #[test]
    fn test_flush_does_not_consume_bw() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();

        // Set 0 as bandwidth limit
        guest.set_bw_limit(0);
        assert!(guest.consume_req().is_none());

        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });
        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });
        let _ = guest.send(BlockOp::Flush {
            snapshot_details: None,
        });

        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_some());

        assert!(guest.consume_req().is_none());

        Ok(())
    }

    #[test]
    fn test_iop_and_bw_limit() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();

        guest.set_iop_limit(16384, 500); // 1 IOP is 16 KiB
        guest.set_bw_limit(6400 * 1024); // 16384 B * 400 = 6400 KiB/s
        assert!(guest.consume_req().is_none());

        // Don't use guest.read, that will send a block size query that will
        // never be answered.

        // Validate that BW limit activates by sending two 7000 KiB IOs. 7000
        // KiB is only 437.5 IOPs

        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(7000 * 1024),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(7000 * 1024),
        });

        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_none());

        // Assert we've hit the BW limit before IOPS
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 438); // 437.5 rounded up
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 7000 * 1024);

        guest.leak_iop_tokens(438);
        guest.leak_bw_tokens(7000 * 1024);

        assert!(guest.consume_req().is_some());
        assert!(guest.reqs.lock().unwrap().is_empty());

        // Back to zero
        guest.leak_iop_tokens(438);
        guest.leak_bw_tokens(7000 * 1024);

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        // Validate that IOP limit activates by sending 501 1024b IOs
        for _ in 0..500 {
            let _ = guest.send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(1024),
            });
            assert!(guest.consume_req().is_some());
        }

        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(1024),
        });
        assert!(guest.consume_req().is_none());

        // Assert we've hit the IOPS limit
        assert_eq!(*guest.iop_tokens.lock().unwrap(), 500);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 500 * 1024);

        // Back to zero
        guest.leak_iop_tokens(500);
        guest.leak_bw_tokens(500 * 1024);
        guest.reqs.lock().unwrap().clear();

        assert!(guest.reqs.lock().unwrap().is_empty());
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

            let _ = guest.send(BlockOp::Read {
                offset: Block::new_512(0),
                data: Buffer::new(optimal_io_size),
            });

            assert!(guest.consume_req().is_some());
        }

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 500);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 500 * optimal_io_size);

        Ok(())
    }

    // Is it possible to submit an IO that will never be sent? It shouldn't be!
    #[test]
    fn test_impossible_io() -> Result<()> {
        let mut guest = Guest::new();
        guest.set_active();

        guest.set_iop_limit(1024 * 1024 / 2, 10); // 1 IOP is half a KiB
        guest.set_bw_limit(1024 * 1024); // 1 KiB
        assert!(guest.consume_req().is_none());

        // Sending an IO of 10 KiB is larger than the bandwidth limit and
        // represents 20 IOPs, larger than the IOP limit.
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(10 * 1024 * 1024),
        });
        let _ = guest.send(BlockOp::Read {
            offset: Block::new_512(0),
            data: Buffer::new(0),
        });

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 0);

        // Even though the first IO is larger than the bandwidth and IOP limit,
        // it should still succeed. The next IO should not, even if it consumes
        // nothing, because the iops and bw tokens will be larger than the limit
        // for a while (until they leak enough).

        assert!(guest.consume_req().is_some());
        assert!(guest.consume_req().is_none());

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 20);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 10 * 1024 * 1024);

        // Bandwidth trigger is going to be larger and need more leaking to get
        // down to a point where the zero sized IO can fire.
        for _ in 0..9 {
            guest.leak_iop_tokens(10);
            guest.leak_bw_tokens(1024 * 1024);

            assert!(guest.consume_req().is_none());
        }

        assert_eq!(*guest.iop_tokens.lock().unwrap(), 0);
        assert_eq!(*guest.bw_tokens.lock().unwrap(), 1024 * 1024);

        assert!(guest.consume_req().is_none());

        guest.leak_iop_tokens(10);
        guest.leak_bw_tokens(1024 * 1024);

        // We've leaked 10 KiB worth, it should fire now!
        assert!(guest.consume_req().is_some());

        Ok(())
    }

    #[test]
    fn work_writes_bad() {
        // Verify that three bad writes will ACK the IO, and set the
        // downstairs clients to failed.
        // This test also makes sure proper mutex behavior is used in
        // process_ds_operaion.
        let up = Upstairs::default();
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive);
            up.ds_transition(cid, DsState::WaitQuorum);
            up.ds_transition(cid, DsState::Active);
        }
        up.set_active().unwrap();

        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op = create_write_eob(
                next_id,
                vec![],
                10,
                vec![crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(7),
                    data: Bytes::from(vec![1]),
                    encryption_context: None,
                    hash: 0,
                }],
                false,
            );

            ds.enqueue(op);

            assert!(ds.in_progress(next_id, 0).is_some());
            assert!(ds.in_progress(next_id, 1).is_some());
            assert!(ds.in_progress(next_id, 2).is_some());

            next_id
        };

        // Set the error that everyone will use.
        let response = Err(CrucibleError::GenericError(format!("bad")));

        // Process the operation for client 0
        assert_eq!(
            up.process_ds_operation(next_id, 0, response.clone())
                .unwrap(),
            false
        );
        // client 0 is failed, the others should be okay still
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Active);
        assert_eq!(up.ds_state(2), DsState::Active);

        // Process the operation for client 1
        assert_eq!(
            up.process_ds_operation(next_id, 1, response.clone())
                .unwrap(),
            false
        );
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Failed);
        assert_eq!(up.ds_state(2), DsState::Active);

        {
            // Verify we are not ready to ACK yet.
            let mut ds = up.downstairs.lock().unwrap();
            let state = ds.active.get_mut(&next_id).unwrap().ack_status;
            assert_eq!(state, AckStatus::NotAcked);
        }
        // Three failures, process_ds_operaion should return true now.
        // Process the operation for client 2
        assert_eq!(
            up.process_ds_operation(next_id, 2, response.clone())
                .unwrap(),
            true
        );
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Failed);
        assert_eq!(up.ds_state(2), DsState::Failed);

        // Verify we can ack this (failed) work
        let mut ds = up.downstairs.lock().unwrap();
        assert_eq!(ds.ackable_work().len(), 1);
    }

    #[test]
    fn read_after_write_fail_is_alright() {
        // Verify that if a single write fails on a downstairs, reads can still
        // be acked.
        //
        // Verify after acking IOs, we can then send a flush and
        // clear the jobs (some now failed/skipped) from the work queue.
        let up = Upstairs::default();
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive);
            up.ds_transition(cid, DsState::WaitQuorum);
            up.ds_transition(cid, DsState::Active);
        }
        up.set_active().unwrap();

        // Create the write that fails on one DS
        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op = create_write_eob(
                next_id,
                vec![],
                10,
                vec![crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(7),
                    data: Bytes::from(vec![1]),
                    encryption_context: None,
                    hash: 0,
                }],
                false,
            );

            ds.enqueue(op);

            ds.in_progress(next_id, 0);
            ds.in_progress(next_id, 1);
            ds.in_progress(next_id, 2);

            next_id
        };

        // Set the error that everyone will use.
        let err_response = Err(CrucibleError::GenericError(format!("bad")));

        // Process the error operation for client 0
        assert_eq!(
            up.process_ds_operation(next_id, 0, err_response).unwrap(),
            false
        );
        // client 0 should be marked failed.
        assert_eq!(up.ds_state(0), DsState::Failed);

        let ok_response = Ok(vec![]);
        // Process the good operation for client 1
        assert_eq!(
            up.process_ds_operation(next_id, 1, ok_response.clone())
                .unwrap(),
            false
        );

        // process_ds_operaion should return true after we process this.
        assert_eq!(
            up.process_ds_operation(next_id, 2, ok_response).unwrap(),
            true
        );
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Active);
        assert_eq!(up.ds_state(2), DsState::Active);

        // Verify we can ack this work, then ack it.
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 1);
        up.downstairs.lock().unwrap().ack(next_id);

        // Now, do a read.
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();
            let op =
                create_read_eob(next_id, vec![], 10, vec![request.clone()]);

            ds.enqueue(op);

            // As this DS is failed, it should return none
            assert_eq!(ds.in_progress(next_id, 0), None);
            assert!(ds.in_progress(next_id, 1).is_some());
            assert!(ds.in_progress(next_id, 2).is_some());

            next_id
        };

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        // Process the operation for client 1 this should return true
        assert_eq!(
            up.process_ds_operation(next_id, 1, response.clone())
                .unwrap(),
            true
        );

        // Process the operation for client 2 this should return false
        assert_eq!(
            up.process_ds_operation(next_id, 2, response.clone())
                .unwrap(),
            false
        );

        // Verify we can ack this work, then ack it.
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 1);
        up.downstairs.lock().unwrap().ack(next_id);

        // Perform the flush.
        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();
            let op = create_flush(next_id, vec![], 10, 0, 0, None);
            ds.enqueue(op);

            // As this DS is failed, it should return none
            assert_eq!(ds.in_progress(next_id, 0), None);
            assert!(ds.in_progress(next_id, 1).is_some());
            assert!(ds.in_progress(next_id, 2).is_some());

            next_id
        };

        let ok_response = Ok(vec![]);
        // Process the operation for client 1
        assert_eq!(
            up.process_ds_operation(next_id, 1, ok_response.clone())
                .unwrap(),
            false
        );

        // process_ds_operaion should return true after we process this.
        assert_eq!(
            up.process_ds_operation(next_id, 2, ok_response).unwrap(),
            true
        );

        // ACK the flush and let retire_check move things along.
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 1);
        up.downstairs.lock().unwrap().ack(next_id);
        up.downstairs.lock().unwrap().retire_check(next_id);

        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 0);

        // The write, the read, and now the flush should be completed.
        assert_eq!(up.downstairs.lock().unwrap().completed.len(), 3);
    }

    #[test]
    fn read_after_two_write_fail_is_alright() {
        // Verify that if two writes fail, a read can still be acked.
        let up = Upstairs::default();
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive);
            up.ds_transition(cid, DsState::WaitQuorum);
            up.ds_transition(cid, DsState::Active);
        }
        up.set_active().unwrap();

        // Create the write that fails on two DS
        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op = create_write_eob(
                next_id,
                vec![],
                10,
                vec![crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(7),
                    data: Bytes::from(vec![1]),
                    encryption_context: None,
                    hash: 0,
                }],
                false,
            );

            ds.enqueue(op);

            ds.in_progress(next_id, 0);
            ds.in_progress(next_id, 1);
            ds.in_progress(next_id, 2);

            next_id
        };

        // Set the error that everyone will use.
        let err_response = Err(CrucibleError::GenericError(format!("bad")));

        // Process the operation for client 0
        assert_eq!(
            up.process_ds_operation(next_id, 0, err_response.clone())
                .unwrap(),
            false
        );
        // client 0 is failed, the others should be okay still
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Active);
        assert_eq!(up.ds_state(2), DsState::Active);

        // Process the operation for client 1
        assert_eq!(
            up.process_ds_operation(next_id, 1, err_response).unwrap(),
            false
        );
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Failed);
        assert_eq!(up.ds_state(2), DsState::Active);

        let ok_response = Ok(vec![]);
        // process_ds_operaion should return true after we process this.
        assert_eq!(
            up.process_ds_operation(next_id, 2, ok_response).unwrap(),
            true
        );
        assert_eq!(up.ds_state(0), DsState::Failed);
        assert_eq!(up.ds_state(1), DsState::Failed);
        assert_eq!(up.ds_state(2), DsState::Active);

        // Verify we can ack this work
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 1);

        // Now, do a read.
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
        };

        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op =
                create_read_eob(next_id, vec![], 10, vec![request.clone()]);

            ds.enqueue(op);

            // As this DS is failed, it should return none
            assert_eq!(ds.in_progress(next_id, 0), None);
            assert_eq!(ds.in_progress(next_id, 1), None);
            assert!(ds.in_progress(next_id, 2).is_some());

            next_id
        };

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        // Process the operation for client 1 this should return true
        assert_eq!(
            up.process_ds_operation(next_id, 2, response.clone())
                .unwrap(),
            true
        );
    }

    #[test]
    fn write_after_write_fail_is_alright() {
        // Verify that if a single write fails on a downstairs, a second
        // write can still be acked.
        // Then, send a flush and verify the work queue is cleared.
        let up = Upstairs::default();
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive);
            up.ds_transition(cid, DsState::WaitQuorum);
            up.ds_transition(cid, DsState::Active);
        }
        up.set_active().unwrap();

        // Create the write that fails on one DS
        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op = create_write_eob(
                next_id,
                vec![],
                10,
                vec![crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(7),
                    data: Bytes::from(vec![1]),
                    encryption_context: None,
                    hash: 0,
                }],
                false,
            );

            ds.enqueue(op);

            ds.in_progress(next_id, 0);
            ds.in_progress(next_id, 1);
            ds.in_progress(next_id, 2);

            next_id
        };

        // Make the error and ok responses
        let err_response = Err(CrucibleError::GenericError(format!("bad")));
        let ok_response = Ok(vec![]);

        // Process the operation for client 0
        assert_eq!(
            up.process_ds_operation(next_id, 0, ok_response.clone())
                .unwrap(),
            false
        );

        // Process the error for client 1
        assert_eq!(
            up.process_ds_operation(next_id, 1, err_response).unwrap(),
            false
        );

        // process_ds_operaion should return true after we process this.
        assert_eq!(
            up.process_ds_operation(next_id, 2, ok_response.clone())
                .unwrap(),
            true
        );

        // Verify client states
        assert_eq!(up.ds_state(0), DsState::Active);
        assert_eq!(up.ds_state(1), DsState::Failed);
        assert_eq!(up.ds_state(2), DsState::Active);

        // Verify we can ack this work
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 1);

        let first_id = next_id;
        // Now, do another write.
        let next_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();

            let op = create_write_eob(
                next_id,
                vec![],
                10,
                vec![crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(7),
                    data: Bytes::from(vec![1]),
                    encryption_context: None,
                    hash: 0,
                }],
                false,
            );

            ds.enqueue(op);

            ds.in_progress(next_id, 0);
            assert_eq!(ds.in_progress(next_id, 1), None);
            ds.in_progress(next_id, 2);

            next_id
        };

        // Process the operation for client 0, re-use ok_response from above.
        assert_eq!(
            up.process_ds_operation(next_id, 0, ok_response.clone())
                .unwrap(),
            false
        );

        // We don't process client 1, it had failed

        // process_ds_operaion should return true after we process this.
        assert_eq!(
            up.process_ds_operation(next_id, 2, ok_response).unwrap(),
            true
        );

        // Verify we can ack this work, the total is now 2 jobs to ack
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 2);

        // Perform the flush.
        let flush_id = {
            let mut ds = up.downstairs.lock().unwrap();

            let next_id = ds.next_id();
            let op = create_flush(next_id, vec![], 10, 0, 0, None);
            ds.enqueue(op);

            assert!(ds.in_progress(next_id, 0).is_some());
            // As this DS is failed, it should return none
            assert_eq!(ds.in_progress(next_id, 1), None);
            assert!(ds.in_progress(next_id, 2).is_some());

            next_id
        };

        let ok_response = Ok(vec![]);
        // Process the operation for client 0
        assert_eq!(
            up.process_ds_operation(flush_id, 0, ok_response.clone())
                .unwrap(),
            false
        );

        // process_ds_operaion should return true after we process client 2.
        assert_eq!(
            up.process_ds_operation(flush_id, 2, ok_response).unwrap(),
            true
        );

        // ACK all the jobs and let retire_check move things along.
        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 3);
        up.downstairs.lock().unwrap().ack(first_id);
        up.downstairs.lock().unwrap().ack(next_id);
        up.downstairs.lock().unwrap().ack(flush_id);
        up.downstairs.lock().unwrap().retire_check(flush_id);

        assert_eq!(up.downstairs.lock().unwrap().ackable_work().len(), 0);

        // The two writes and the flush should be completed.
        assert_eq!(up.downstairs.lock().unwrap().completed.len(), 3);
    }
}
