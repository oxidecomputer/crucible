// Copyright 2021 Oxide Computer Company

#[cfg(test)]
use super::*;

#[cfg(test)]
mod test {
    use super::*;
    use pseudo_file::IOSpan;
    use ringbuffer::RingBuffer;

    fn extent_tuple(eid: u64, offset: u64, len: u64) -> (u64, Block, Block) {
        (eid, Block::new_512(offset), Block::new_512(len))
    }

    #[test]
    fn test_extent_from_offset() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(2));
        ddef.set_extent_count(10);

        // Test block size, less than extent size
        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(0),
                Block::new_512(1),
                false
            )
            .unwrap(),
            vec![extent_tuple(0, 0, 1)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(0),
                Block::new_512(2),
                false
            )
            .unwrap(),
            vec![extent_tuple(0, 0, 2)],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(0),
                Block::new_512(4),
                false
            )
            .unwrap(),
            vec![extent_tuple(0, 0, 2), extent_tuple(1, 0, 2)],
        );

        // Test offsets
        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(1),
                Block::new_512(4),
                false
            )
            .unwrap(),
            vec![
                extent_tuple(0, 1, 1),
                extent_tuple(1, 0, 2),
                extent_tuple(2, 0, 1),
            ],
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2),
                Block::new_512(4),
                false
            )
            .unwrap(),
            vec![extent_tuple(1, 0, 2), extent_tuple(2, 0, 2)],
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2),
                Block::new_512(16),
                false
            )
            .unwrap(),
            vec![
                extent_tuple(1, 0, 2),
                extent_tuple(2, 0, 2),
                extent_tuple(3, 0, 2),
                extent_tuple(4, 0, 2),
                extent_tuple(5, 0, 2),
                extent_tuple(6, 0, 2),
                extent_tuple(7, 0, 2),
                extent_tuple(8, 0, 2),
            ],
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
        };

        Upstairs::new(&opts, def, Arc::new(Guest::new()))
    }

    /*
     * Terrible wrapper, but it allows us to call extent_from_offset()
     * just like the program does.
     */
    fn up_efo(
        up: &Arc<Upstairs>,
        offset: Block,
        num_blocks: u64,
    ) -> Result<Vec<(u64, Block, Block)>> {
        let ddef = up.ddef.lock().unwrap();
        let num_blocks = Block::new_with_ddef(num_blocks, &ddef);
        extent_from_offset(*ddef, offset, num_blocks, false)
    }

    #[test]
    fn off_to_extent_one_block() {
        let up = make_upstairs();

        for i in 0..100 {
            let exv = vec![extent_tuple(0, i, 1)];
            assert_eq!(up_efo(&up, Block::new_512(i), 1).unwrap(), exv);
        }

        for i in 0..100 {
            let exv = vec![extent_tuple(1, i, 1)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1).unwrap(), exv);
        }

        let exv = vec![extent_tuple(2, 0, 1)];
        assert_eq!(up_efo(&up, Block::new_512(200), 1).unwrap(), exv);

        let exv = vec![extent_tuple(9, 99, 1)];
        assert_eq!(up_efo(&up, Block::new_512(999), 1).unwrap(), exv);
    }

    #[test]
    fn off_to_extent_two_blocks() {
        let up = make_upstairs();

        for i in 0..99 {
            let exv = vec![extent_tuple(0, i, 2)];
            assert_eq!(up_efo(&up, Block::new_512(i), 2).unwrap(), exv);
        }

        let exv = vec![extent_tuple(0, 99, 1), extent_tuple(1, 0, 1)];
        assert_eq!(up_efo(&up, Block::new_512(99), 2).unwrap(), exv);

        for i in 0..99 {
            let exv = vec![extent_tuple(1, i, 1)];
            assert_eq!(up_efo(&up, Block::new_512(100 + i), 1).unwrap(), exv);
        }

        let exv = vec![extent_tuple(1, 99, 1), extent_tuple(2, 0, 1)];
        assert_eq!(up_efo(&up, Block::new_512(199), 2).unwrap(), exv);

        let exv = vec![extent_tuple(2, 0, 2)];
        assert_eq!(up_efo(&up, Block::new_512(200), 2).unwrap(), exv);

        let exv = vec![extent_tuple(9, 98, 2)];
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
            vec![extent_tuple(0, 99, 1), extent_tuple(1, 0, 1)],
        );
        assert_eq!(
            up_efo(&up, Block::new_512(98), 4).unwrap(),
            vec![extent_tuple(0, 98, 2), extent_tuple(1, 0, 2)],
        );

        /*
         * Largest buffer
         */
        assert_eq!(
            up_efo(&up, Block::new_512(1), 100).unwrap(),
            vec![extent_tuple(0, 1, 99), extent_tuple(1, 0, 1),],
        );
        assert_eq!(
            up_efo(&up, Block::new_512(2), 100).unwrap(),
            vec![extent_tuple(0, 2, 98), extent_tuple(1, 0, 2)],
        );
        assert_eq!(
            up_efo(&up, Block::new_512(4), 100).unwrap(),
            vec![extent_tuple(0, 4, 96), extent_tuple(1, 0, 4)],
        );

        /*
         * Largest buffer, last block offset possible
         */
        assert_eq!(
            up_efo(&up, Block::new_512(99), 100).unwrap(),
            vec![extent_tuple(0, 99, 1), extent_tuple(1, 0, 99)],
        );
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
    pub fn test_upstairs_encryption_context_ok() {
        use rand::{thread_rng, Rng};

        let key_bytes =
            base64::decode("ClENKTXD2bCyXSHnKXY7GGnk+NvQKbwpatjWP2fJzk0=")
                .unwrap();
        let context = EncryptionContext::new(Vec::<u8>::from(key_bytes), 512);

        let mut block = [0u8; 512];
        thread_rng().fill(&mut block[..]);

        let orig_block = block.clone();

        context.encrypt_in_place(&mut block[..], 0);
        assert_ne!(block, orig_block);

        context.decrypt_in_place(&mut block[..], 0);
        assert_eq!(block, orig_block);
    }

    #[test]
    pub fn test_upstairs_encryption_context_bad_index() {
        use rand::{thread_rng, Rng};

        let key_bytes =
            base64::decode("EVrH+ABhMP0MLfxynCalDq1vWCCWCWFfsSsJoJeDCx8=")
                .unwrap();
        let context = EncryptionContext::new(Vec::<u8>::from(key_bytes), 512);

        let mut block = [0u8; 512];
        thread_rng().fill(&mut block[..]);

        let orig_block = block.clone();

        // The wrong block index shouldn't work.
        context.encrypt_in_place(&mut block[..], 0);
        assert_ne!(block, orig_block);

        context.decrypt_in_place(&mut block[..], 1);
        assert_ne!(block, orig_block);
    }

    #[test]
    fn work_flush_three_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let op = create_flush(next_id, vec![], 10, 0);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        assert_eq!(work.complete(next_id, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);
        assert_eq!(work.completed.len(), 0);

        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        work.ack(next_id);

        assert_eq!(work.complete(next_id, 2, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 1);
    }

    #[test]
    fn work_flush_one_error_then_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let op = create_flush(next_id, vec![], 10, 0);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 2, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);

        work.ack(next_id);
        work.retire_check(next_id);

        assert_eq!(work.completed.len(), 1);
    }

    #[test]
    fn work_flush_two_errors_equals_fail() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let op = create_flush(next_id, vec![], 10, 0);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(
            work.complete(
                next_id,
                2,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            true
        );
        assert_eq!(work.ackable_work().len(), 1);

        work.ack(next_id);
        work.retire_check(next_id);

        assert_eq!(work.completed.len(), 1);
    }

    #[test]
    fn work_read_one_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);
        assert_eq!(work.completed.len(), 0);

        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        work.ack(next_id);

        assert_eq!(work.complete(next_id, 1, &response).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        // A flush is required to move work to completed
        assert_eq!(work.completed.len(), 0);
    }

    #[test]
    fn work_read_one_bad_two_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 1, &response).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);
        assert_eq!(work.completed.len(), 0);

        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        work.ack(next_id);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        // A flush is required to move work to completed
        // That this is still zero is part of the test
        assert_eq!(work.completed.len(), 0);
    }

    #[test]
    fn work_read_two_bad_one_ok() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(
            work.complete(
                next_id,
                1,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);

        work.ack(next_id);
        work.retire_check(next_id);

        // A flush is required to move work to completed
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);
    }

    #[test]
    fn work_read_three_bad() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(
            work.complete(
                next_id,
                1,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        assert_eq!(
            work.complete(
                next_id,
                2,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            true
        );
        assert_eq!(work.ackable_work().len(), 1);

        work.ack(next_id);
        work.retire_check(next_id);

        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);
    }

    #[test]
    fn work_read_two_ok_one_bad() {
        let upstairs = Upstairs::default();
        upstairs.set_active();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };

        let next_id = {
            let mut work = upstairs.downstairs.lock().unwrap();

            let next_id = work.next_id();

            let op =
                create_read_eob(next_id, vec![], 10, vec![request.clone()]);

            work.enqueue(op);

            work.in_progress(next_id, 0);
            work.in_progress(next_id, 1);
            work.in_progress(next_id, 2);

            next_id
        };

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);

        assert_eq!(
            upstairs.complete(next_id, 2, response.clone()).unwrap(),
            true
        );

        assert_eq!(upstairs.complete(next_id, 0, response).unwrap(), false);

        {
            // emulated run in up_ds_listen

            let mut work = upstairs.downstairs.lock().unwrap();
            let state = work.active.get_mut(&next_id).unwrap().ack_status;
            assert_eq!(state, AckStatus::AckReady);
            work.ack(next_id);

            work.retire_check(next_id);
        }

        assert_eq!(
            upstairs
                .complete(
                    next_id,
                    1,
                    Err(CrucibleError::GenericError(format!("bad")))
                )
                .unwrap(),
            false
        );

        {
            let mut work = upstairs.downstairs.lock().unwrap();
            assert_eq!(work.ackable_work().len(), 0);
            // Work won't be completed until we get a flush.
            assert_eq!(work.completed.len(), 0);
        }
    }

    #[test]
    fn work_assert_ok_transfer_of_read_after_downstairs_write_errors() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        // send a write, and clients 0 and 1 will return errors

        let op = create_write_eob(
            next_id,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );

        work.enqueue(op);

        assert!(work.in_progress(next_id, 0).is_some());
        assert!(work.in_progress(next_id, 1).is_some());
        assert!(work.in_progress(next_id, 2).is_some());

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        assert_eq!(
            work.complete(
                next_id,
                1,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        let response = Ok(vec![]);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), true);

        assert!(work.downstairs_errors.get(&0).is_some());
        assert!(work.downstairs_errors.get(&1).is_some());
        assert!(work.downstairs_errors.get(&2).is_none());

        // another read. Make sure only client 2 returns data.
        // The others should be skipped.

        let next_id = work.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        assert!(work.in_progress(next_id, 0).is_none());
        assert!(work.in_progress(next_id, 1).is_none());
        assert!(work.in_progress(next_id, 2).is_some());

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![3],
        )]);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), true);

        assert!(work.active.get(&next_id).unwrap().data.is_some());
        assert_eq!(
            work.active.get(&next_id).unwrap().data,
            Some(vec![ReadResponse::from_request_with_data(
                &request,
                &vec![3]
            )]),
        );
    }

    #[test]
    fn work_assert_reads_do_not_cause_failure_state_transition() {
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        let next_id = work.next_id();

        // send a read, and clients 0 and 1 will return errors

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        assert!(work.in_progress(next_id, 0).is_some());
        assert!(work.in_progress(next_id, 1).is_some());
        assert!(work.in_progress(next_id, 2).is_some());

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        assert_eq!(
            work.complete(
                next_id,
                1,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![3],
        )]);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), true);

        assert!(work.active.get(&next_id).unwrap().data.is_some());
        assert_eq!(
            work.active.get(&next_id).unwrap().data,
            Some(vec![ReadResponse::from_request_with_data(
                &request,
                &vec![3]
            )]),
        );

        assert!(work.downstairs_errors.get(&0).is_none());
        assert!(work.downstairs_errors.get(&1).is_none());
        assert!(work.downstairs_errors.get(&2).is_none());

        // send another read, and expect all to return something
        // (reads shouldn't cause a Failed transition)

        let next_id = work.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        assert!(work.in_progress(next_id, 0).is_some());
        assert!(work.in_progress(next_id, 1).is_some());
        assert!(work.in_progress(next_id, 2).is_some());

        assert_eq!(
            work.complete(
                next_id,
                0,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false,
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        assert_eq!(
            work.complete(
                next_id,
                1,
                &Err(CrucibleError::GenericError(format!("bad")))
            )
            .unwrap(),
            false,
        );

        assert!(work.active.get(&next_id).unwrap().data.is_none());

        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![6],
        )]);

        assert_eq!(work.complete(next_id, 2, &response).unwrap(), true);

        assert!(work.active.get(&next_id).unwrap().data.is_some());
        assert_eq!(
            work.active.get(&next_id).unwrap().data,
            Some(vec![ReadResponse::from_request_with_data(
                &request,
                &vec![6]
            )])
        );
    }

    #[test]
    fn work_completed_read_flush() {
        // Verify that a read remains on the active queue until a flush
        // comes through and clears it.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Build our read, put it into the work queue
        let next_id = work.next_id();

        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);

        work.enqueue(op);

        // Move the work to submitted like we sent it to each downstairs
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Downstairs 0 now has completed this work.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);

        // One completion of a read means we can ACK
        assert_eq!(work.ackable_work().len(), 1);

        // Complete downstairs 1 and 2
        assert_eq!(work.complete(next_id, 1, &response).unwrap(), false);
        assert_eq!(work.complete(next_id, 2, &response).unwrap(), false);

        // Make sure the job is still active
        assert_eq!(work.completed.len(), 0);

        // The job should still be ack ready
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Ack the job to the guest
        work.ack(next_id);

        // Nothing left to ACK, but untill the flush we keep the IO data.
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        // A flush is required to move work to completed
        // Create the flush then send it to all downstairs.
        let next_id = work.next_id();
        let op = create_flush(next_id, vec![], 10, 0);

        work.enqueue(op);

        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the Flush at each downstairs.
        assert_eq!(work.complete(next_id, 0, &Ok(vec![])).unwrap(), false);
        // Two completed means we return true (ack ready now)
        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.complete(next_id, 2, &Ok(vec![])).unwrap(), false);

        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // ACK the flush and let retire_check move things along.
        work.ack(next_id);
        work.retire_check(next_id);

        // Verify no more work to ack.
        assert_eq!(work.ackable_work().len(), 0);
        // The read and the flush should now be moved to completed.
        assert_eq!(work.completed.len(), 2);
    }

    #[test]
    fn work_delay_completion_flush() {
        // Verify that a write remains on the active queue until a flush
        // comes through and clears it.  In this case, we only complete
        // 2/3 for each IO.  We later come back and finish the 3rd IO
        // and the flush, which then allows the work to be completed.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Create two writes, put them on the work queue
        let id1 = work.next_id();
        let id2 = work.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        let op = create_write_eob(
            id2,
            vec![],
            1,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        // Simulate sending both writes to downstairs 0 and 1
        assert!(work.in_progress(id1, 0).is_some());
        assert!(work.in_progress(id1, 1).is_some());
        assert!(work.in_progress(id2, 0).is_some());
        assert!(work.in_progress(id2, 1).is_some());

        // Simulate completing both writes to downstairs 0 and 1
        assert_eq!(work.complete(id1, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id1, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.complete(id2, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id2, 1, &Ok(vec![])).unwrap(), true);

        // Both writes can now ACK to the guest.
        work.ack(id1);
        work.ack(id2);

        // Work stays on active queue till the flush
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        // Create the flush, put on the work queue
        let flush_id = work.next_id();
        let op = create_flush(flush_id, vec![], 10, 0);
        work.enqueue(op);

        // Simulate sending the flush to downstairs 0 and 1
        work.in_progress(flush_id, 0);
        work.in_progress(flush_id, 1);

        // Simulate completing the flush to downstairs 0 and 1
        assert_eq!(work.complete(flush_id, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(flush_id, 1, &Ok(vec![])).unwrap(), true);

        // Ack the flush back to the guest
        work.ack(flush_id);

        // Make sure downstairs 0 and 1 update their last flush id and
        // that downstairs 2 does not.
        assert_eq!(work.ds_last_flush[0], flush_id);
        assert_eq!(work.ds_last_flush[1], flush_id);
        assert_eq!(work.ds_last_flush[2], 0);

        // Should not retire yet.
        work.retire_check(flush_id);

        assert_eq!(work.ackable_work().len(), 0);

        // Make sure all work is still on the active side
        assert_eq!(work.completed.len(), 0);

        // Now, finish the writes to downstairs 2
        assert!(work.in_progress(id1, 2).is_some());
        assert!(work.in_progress(id2, 2).is_some());
        assert_eq!(work.complete(id1, 2, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id2, 2, &Ok(vec![])).unwrap(), false);

        // The job should not move to completed until the flush goes as well.
        assert_eq!(work.completed.len(), 0);

        // Complete the flush on downstairs 2.
        work.in_progress(flush_id, 2);
        assert_eq!(work.complete(flush_id, 2, &Ok(vec![])).unwrap(), false);

        // All three jobs should now move to completed
        assert_eq!(work.completed.len(), 3);
        // Downstairs 2 should update the last flush it just did.
        assert_eq!(work.ds_last_flush[2], flush_id);
    }

    #[test]
    fn work_completed_write_flush() {
        // Verify that a write remains on the active queue until a flush
        // comes through and clears it.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Build our write IO.
        let next_id = work.next_id();

        let op = create_write_eob(
            next_id,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        // Put the write on the queue.
        work.enqueue(op);

        // Submit the write to all three downstairs.
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the write on all three downstairs.
        assert_eq!(work.complete(next_id, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.complete(next_id, 2, &Ok(vec![])).unwrap(), false);

        // Ack the write to the guest
        work.ack(next_id);

        // Work stays on active queue till the flush
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        // Create the flush IO
        let next_id = work.next_id();
        let op = create_flush(next_id, vec![], 10, 0);
        work.enqueue(op);

        // Submit the flush to all three downstairs.
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the flush on all three downstairs.
        assert_eq!(work.complete(next_id, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(next_id, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.complete(next_id, 2, &Ok(vec![])).unwrap(), false);

        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        work.ack(next_id);
        work.retire_check(next_id);

        assert_eq!(work.ackable_work().len(), 0);
        // The write and flush should now be completed.
        assert_eq!(work.completed.len(), 2);
    }

    #[test]
    fn work_delay_completion_flush_order() {
        // Verify that a write remains on the active queue until a flush
        // comes through and clears it.  In this case, we only complete
        // 2 of 3 for each IO.  We later come back and finish the 3rd IO
        // and the flush, which then allows the work to be completed.
        // Also, we mix up which client finishes which job first.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Build two writes, put them on the work queue.
        let id1 = work.next_id();
        let id2 = work.next_id();

        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        let op = create_write_eob(
            id2,
            vec![],
            1,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        // Submit the two writes, to 2/3 of the downstairs.
        assert!(work.in_progress(id1, 0).is_some());
        assert!(work.in_progress(id1, 1).is_some());
        assert!(work.in_progress(id2, 1).is_some());
        assert!(work.in_progress(id2, 2).is_some());

        // Complete the writes that we sent to the 2 downstairs.
        assert_eq!(work.complete(id1, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id1, 1, &Ok(vec![])).unwrap(), true);
        assert_eq!(work.complete(id2, 1, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id2, 2, &Ok(vec![])).unwrap(), true);

        // Ack the writes to the guest.
        work.ack(id1);
        work.ack(id2);

        // Work stays on active queue till the flush.
        assert_eq!(work.ackable_work().len(), 0);
        assert_eq!(work.completed.len(), 0);

        // Create and enqueue the flush.
        let flush_id = work.next_id();
        let op = create_flush(flush_id, vec![], 10, 0);
        work.enqueue(op);

        // Send the flush to two downstairs.
        work.in_progress(flush_id, 0);
        work.in_progress(flush_id, 2);

        // Complete the flush on those downstairs.
        assert_eq!(work.complete(flush_id, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(flush_id, 2, &Ok(vec![])).unwrap(), true);

        // Ack the flush
        work.ack(flush_id);

        // Should not retire yet
        work.retire_check(flush_id);

        assert_eq!(work.ackable_work().len(), 0);
        // Not done yet, until all clients do the work.
        assert_eq!(work.completed.len(), 0);

        // Verify who has updated their last flush.
        assert_eq!(work.ds_last_flush[0], flush_id);
        assert_eq!(work.ds_last_flush[1], 0);
        assert_eq!(work.ds_last_flush[2], flush_id);

        // Now, finish sending and completing the writes
        assert!(work.in_progress(id1, 2).is_some());
        assert!(work.in_progress(id2, 0).is_some());
        assert_eq!(work.complete(id1, 2, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id2, 0, &Ok(vec![])).unwrap(), false);

        // Completed work won't happen till the last flush is done
        assert_eq!(work.completed.len(), 0);

        // Send and complete the flush
        work.in_progress(flush_id, 1);
        assert_eq!(work.complete(flush_id, 1, &Ok(vec![])).unwrap(), false);

        // Now, all three jobs (w,w,f) will move to completed.
        assert_eq!(work.completed.len(), 3);

        // downstairs 1 should now have that flush
        assert_eq!(work.ds_last_flush[1], flush_id);
    }

    #[test]
    fn work_completed_read_replay() {
        // Verify that a single read will replay and move back from AckReady
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Build our read IO and submit it to the work queue.
        let next_id = work.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        work.enqueue(op);

        // Submit the read to all three downstairs
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the read on one downstairs.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);

        // One completion should allow for an ACK
        assert_eq!(work.ackable_work().len(), 1);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Now, take that downstairs offline
        work.re_new(0);

        // The act of taking a downstairs offline should move a read
        // back from AckReady if it was the only completed read.
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);
    }

    #[test]
    fn work_completed_two_read_replay() {
        // Verify that a read will replay and move not back from AckReady if
        // there is more than one done read.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Build a read and put it on the work queue.
        let next_id = work.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        work.enqueue(op);

        // Submit the read to each downstairs.
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the read on one downstairs, verify it is ack ready.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Complete the read on a 2nd downstairs.
        assert_eq!(work.complete(next_id, 1, &response).unwrap(), false);

        // Now, take the first downstairs offline.
        work.re_new(0);

        // Should still be ok to ACK this IO
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Taking the second downstairs offline should revert the ACK.
        work.re_new(1);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);

        // Redo the read on DS 0, IO should go back to ackable.
        work.in_progress(next_id, 0);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);
        assert_eq!(work.ackable_work().len(), 1);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
    }

    #[test]
    fn work_completed_ack_read_replay() {
        // Verify that a read we Acked will still replay if that downstairs
        // goes away. Make sure everything still finishes ok.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Create the read and put it on the work queue.
        let next_id = work.next_id();
        let request = ReadRequest {
            eid: 0,
            offset: Block::new_512(7),
            num_blocks: 2,
        };
        let op = create_read_eob(next_id, vec![], 10, vec![request.clone()]);
        work.enqueue(op);

        // Submit the read to each downstairs.
        work.in_progress(next_id, 0);
        work.in_progress(next_id, 1);
        work.in_progress(next_id, 2);

        // Complete the read on one downstairs.
        let response = Ok(vec![ReadResponse::from_request_with_data(
            &request,
            &vec![],
        )]);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), true);

        // Verify the read is now AckReady
        assert_eq!(work.ackable_work().len(), 1);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        // Ack the read to the guest.
        work.ack(next_id);

        // Should not retire yet
        work.retire_check(next_id);

        // No new ackable work.
        assert_eq!(work.ackable_work().len(), 0);
        // Verify the IO has not completed yet.
        assert_eq!(work.completed.len(), 0);

        // Now, take that downstairs offline
        work.re_new(0);

        // Acked IO should remain so.
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);

        // Redo on DS 0, IO should remain acked.
        work.in_progress(next_id, 0);
        assert_eq!(work.complete(next_id, 0, &response).unwrap(), false);
        assert_eq!(work.ackable_work().len(), 0);
        let state = work.active.get_mut(&next_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);
    }

    #[test]
    fn work_completed_write_ack_ready_replay() {
        // Verify that a replay when we have two completed writes will
        // change state from AckReady back to NotAcked.
        // If we then redo the work, it should go back to AckReady.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Create the write and put it on the work queue.
        let id1 = work.next_id();
        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        // Submit the read to two downstairs.
        assert!(work.in_progress(id1, 0).is_some());
        assert!(work.in_progress(id1, 1).is_some());

        // Complete the write on two downstairs.
        assert_eq!(work.complete(id1, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id1, 1, &Ok(vec![])).unwrap(), true);

        // Verify AckReady
        let state = work.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);

        /* Now, take that downstairs offline */
        work.re_new(1);

        // State goes back to NotAcked
        let state = work.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::NotAcked);

        // Re-submit and complete the write
        assert!(work.in_progress(id1, 1).is_some());
        assert_eq!(work.complete(id1, 1, &Ok(vec![])).unwrap(), true);

        // State should go back to acked.
        let state = work.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
    }

    #[test]
    fn work_completed_write_acked_replay() {
        // Verify that a replay when we have acked a write will not
        // undo that ack.
        let upstairs = Upstairs::default();
        upstairs.set_active();
        let mut work = upstairs.downstairs.lock().unwrap();

        // Create the write and put it on the work queue.
        let id1 = work.next_id();
        let op = create_write_eob(
            id1,
            vec![],
            10,
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(7),
                data: Bytes::from(vec![1]),
                nonce: None,
                tag: None,
            }],
        );
        work.enqueue(op);

        // Submit the write to two downstairs.
        assert!(work.in_progress(id1, 0).is_some());
        assert!(work.in_progress(id1, 1).is_some());

        // Complete the write on two downstairs.
        assert_eq!(work.complete(id1, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id1, 1, &Ok(vec![])).unwrap(), true);

        // Verify it is ackable..
        assert_eq!(work.ackable_work().len(), 1);

        // Send the ACK to the guest
        work.ack(id1);

        // Verify no more ackable work
        assert_eq!(work.ackable_work().len(), 0);

        // Now, take that downstairs offline
        work.re_new(0);

        // State should stay acked
        let state = work.active.get_mut(&id1).unwrap().ack_status;
        assert_eq!(state, AckStatus::Acked);

        // Finish the write all the way out.
        assert!(work.in_progress(id1, 0).is_some());
        assert!(work.in_progress(id1, 2).is_some());

        assert_eq!(work.complete(id1, 0, &Ok(vec![])).unwrap(), false);
        assert_eq!(work.complete(id1, 2, &Ok(vec![])).unwrap(), false);
    }
}
