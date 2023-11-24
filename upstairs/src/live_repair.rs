// Copyright 2023 Oxide Computer Company

use super::*;

// Live Repair
// This handles the situation where one (or two) downstairs are no longer
// trusted to provide data, but the upstairs is still servicing IOs from the
// guest.  We need to make or verify all the data on the untrusted downstairs
// has the same data as the good downstairs, while IO is flowing.
//
// This situation could arise from a downstairs replacement, or if a downstairs
// went away longer than the upstairs could hold data for it, and it’s now
// missing a bunch of IO activity.
//
// While a downstairs is faulted, IOs for that downstairs are skipped
// automatically and, when it comes to ACK back to the guest, a skip is
// considered as a failed IO.  As long as there are two downstairs still
// working, Writes and Flushes can succeed.  A read only needs one working
// downstairs.
//
// * How repair will work
// A repair happens a single extent at a time.  Repair jobs will have a
// dependency with all IOs on the same extent.  Any existing IOs on that extent
// will need to finish, and any new IOs for that extent will depend on all the
// repair work for that extent before they can proceed.
// Dependencies will treat IOs to different extents as independent of each
// other, and repair on one extent should not effect IOs on other extents.
// IOs that span an extent under repair are considered as being on that extent
// and are discussed in detail later.
//
// There are specific IOop types that are used during live repair.  Repair IOs
// travel through the same work queue as regular IOs.  All repair jobs include
// the specific extent under repair.
//
// When a downstairs joins we check and see if LiveRepair is required, and
// if so, a repair task is created to manage the repair.  The three downstairs
// tasks that normally handle IO in the Upstairs will be used to send repair
// related IOs.
//
// Some special situations of note for LiveRepair.
//
// * IO while repairing
// When there is a repair in progress the upstairs keeps track of an extent
// high water point called `extent_limit` that indicates the extents at and
// below that are clear to receive IO.  When a new IO is received, each
// downstairs task will check to see if it is at (and below) or above this
// extent limit.  If at/below, then the IO is sent to the downstairs under
// repair.  If the IO is above, then the IO is moved to skipped for the
// downstairs under repair.
//
// * IOs that span extents.
// When an IO arrives that spans two extents, and the lower extent matches
// the current extent limit, this IO needs to be held back until all extents
// it covers have completed repair.  This may involve allocating and reserving
// repair job IDs, and making those job IDs dependencies for this spanning IO.
//
// * Skipped IOs and Dependencies “above” a repair command.
// For Repair operations (and operations that follow after them), a downstairs
// under repair will most likely have a bunch of skipped IOs.  Repair
// operations will often have dependencies that will need to finish on the
// Active downstairs, but should be ignored by the downstairs that is under
// repair.  This special case is handled by keeping track of skipped IOs, and,
// when LiveRepair is active, removing those dependencies for just the
// downstairs under repair.  This list of skipped jobs is ds_skipped_jobs in
// the Downstairs structure.
//
// * Failures during extent repair.
// When we encounter a failure during live repair, we must complete any
// repair in progress, though the nature of the failure may change what actual
// IO is sent to the downstairs.  Because we reserve IDs when a repair begins,
// and other IOs may depend on those IDs (and those IOs could have already
// been sent to the downstairs), we must follow through with issuing these
// IOs, including the final ExtentLiveReopen.  Depending on where the failure
// was encountered, these IOs may just be NoOps.

// When determining if an extent needs repair, we collect its current
// information from a downstairs and store the results in this struct.
#[derive(Debug, Copy, Clone)]
pub struct ExtentInfo {
    pub generation: u64,
    pub flush_number: u64,
    pub dirty: bool,
}

#[cfg(test)]
pub mod repair_test {
    use super::*;
    use slog::Drain;

    // Test function to create just enough of an Upstairs for our needs.
    // The caller will indicate which downstairs client it wished to be
    // moved to LiveRepair.
    async fn create_test_upstairs(fail_id: ClientId) -> Arc<Upstairs> {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));

        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
            // Give all downstairs a repair address
            up.ds_set_repair_address(cid, "0.0.0.0:1".parse().unwrap())
                .await;
        }

        // Move our downstairs client fail_id to LiveRepair.
        up.set_active().await.unwrap();
        up.ds_transition(fail_id, DsState::Faulted).await;
        up.ds_transition(fail_id, DsState::LiveRepairReady).await;
        up.ds_transition(fail_id, DsState::LiveRepair).await;
        up.downstairs.lock().await.repair_min_id = Some(JobId(1000));

        up
    }

    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    // Test function to create a downstairs.
    fn create_test_downstairs() -> Downstairs {
        let mut ds = Downstairs::new(csl(), ClientMap::new());
        for cid in ClientId::iter() {
            ds.clients[cid].repair_addr =
                Some("127.0.0.1:1234".parse().unwrap());
        }
        ds
    }

    // Create a test Target vec, we only really use one field here,
    // but we have to create them all.  Also return the ds_work_rx
    // so tests can send work over it without hitting an error.
    fn create_test_dst_rx() -> (Vec<Target>, Vec<mpsc::Receiver<u64>>) {
        let mut dst = Vec::new();
        let mut ds_work_rx_vec = Vec::new();

        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        for _cid in ClientId::iter() {
            let (ds_work_tx, ds_work_rx) = mpsc::channel(500);
            ds_work_rx_vec.push(ds_work_rx);
            let (ds_reconcile_work_tx, _ds_reconcile_work_rx) =
                watch::channel(1);
            let (ds_active_tx, _ds_active_rx) = watch::channel(0);

            let t = Target {
                ds_work_tx,
                ds_done_tx: ds_done_tx.clone(),
                ds_active_tx,
                ds_reconcile_work_tx,
            };
            dst.push(t);
        }
        // We return the work_rx so submitters to the work_tx won't
        // encounter errors.  This allows the actual code to return
        // an error if the channel is really down, and keeps up
        // apperance for test code.
        (dst, ds_work_rx_vec)
    }

    // Test function to move a job to in_progress, then complete it.
    // We will skip processing the job ID on the downstairs client ids
    // on the skip_ds vec.
    async fn move_and_complete_job(
        up: &Arc<Upstairs>,
        ds_id: JobId,
        skip_ds: Vec<ClientId>,
    ) -> Result<()> {
        for cid in ClientId::iter() {
            if skip_ds.contains(&cid) {
                continue;
            }
            up.downstairs.lock().await.in_progress(ds_id, cid);
            up.process_ds_operation(ds_id, cid, Ok(vec![]), None)
                .await?;
        }
        Ok(())
    }

    // Test function to ACK a job, given the ds_id and gw_id
    async fn ack_this_job(
        up: &Arc<Upstairs>,
        gw_id: u64,
        ds_id: JobId,
        result: Result<(), CrucibleError>,
    ) {
        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let state = ds.ds_active.get(&ds_id).unwrap().ack_status;
        assert_eq!(state, AckStatus::AckReady);
        ds.ack(ds_id);
        gw.gw_ds_complete(gw_id, ds_id, None, result, &up.log).await;
    }

    // A function that does some setup that other tests can use to avoid
    // having the same boilerplate code all over the place, and allows the
    // test to make clear what it's actually testing.
    //
    // Here we create an upstairs, then spawn off a task for the
    // repair_extent function that will start repairing an extent.
    //
    // We then wait for that spawned task to create the first two jobs
    // that are created when we start repairing.
    //
    // We create the channels required so we can behave as if we are the
    // different tasks that the real Upstairs will create to process IO
    // from the downstairs.  By returning these to the caller here, we
    // prevent them from being dropped and channels being closed that
    // we need to appear as if they are working.
    //
    // The join handle for the task we spawn is also returned.
    // The result of the repair_extent is unwrapped and this result is
    // returned. Callers expecting error from repair_extent should expect
    // this unwrap will fail.
    async fn start_up_and_repair(
        or_ds: ClientId,
    ) -> (
        Arc<Upstairs>,
        tokio::task::JoinHandle<()>,
        Vec<mpsc::Receiver<u64>>,
    ) {
        assert!(or_ds.get() < 3);
        let source_ds = ClientId::new((or_ds.get() + 1) % 3);
        let up = create_test_upstairs(or_ds).await;
        let (dst, ds_work_rx) = create_test_dst_rx();
        let mut ds_work_vec = Vec::with_capacity(3);
        for this_target in dst.iter().take(3) {
            ds_work_vec.push(this_target.ds_work_tx.clone());
        }
        let upc = Arc::clone(&up);
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let repair_handle = tokio::spawn(async move {
            upc.repair_extent(
                &ds_work_vec,
                0,
                source_ds,
                [or_ds].to_vec(),
                &ds_done_tx,
            )
            .await
            .unwrap();
        });

        // The first thing that should happen after we start repair_exetnt
        // is two jobs show up on the work queue, one for close and one for
        // the eventual re-open.  Wait here for those jobs to show up on the
        // work queue before returning.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 2 {
            info!(up.log, "Waiting for Close + ReOpen jobs");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }
        (up, repair_handle, ds_work_rx)
    }

    #[tokio::test]
    async fn test_check_for_repair_normal() {
        // No repair needed here.
        // Verify we can't repair when the upstairs is not active.
        // Verify we wont try to repair if it's not needed.
        let (dst, _ds_work_rx) = create_test_dst_rx();
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));

        // Before we are active, we return InvalidState
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::InvalidState
        );

        up.set_active().await.unwrap();
        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::NoRepairNeeded
        );

        // No downstairs should change state.
        let ds = up.downstairs.lock().await;
        for cid in ClientId::iter() {
            assert_eq!(ds.clients[cid].state, DsState::Active);
        }
        assert!(ds.repair_min_id.is_none())
    }

    #[tokio::test]
    async fn test_check_for_repair_do_repair() {
        // No repair needed here.
        let (dst, _ds_work_rx) = create_test_dst_rx();
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));
        up.set_active().await.unwrap();
        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::RepairStarted
        );
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.clients[ClientId::new(1)].state, DsState::LiveRepair);
        assert!(ds.repair_min_id.is_some())
    }

    #[tokio::test]
    async fn test_check_for_repair_do_two_repair() {
        // No repair needed here.
        let (dst, _ds_work_rx) = create_test_dst_rx();
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));
        up.set_active().await.unwrap();
        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(2), DsState::Faulted).await;
        up.ds_transition(ClientId::new(2), DsState::LiveRepairReady)
            .await;
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::RepairStarted
        );
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.clients[ClientId::new(0)].state, DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state, DsState::LiveRepair);
        assert_eq!(ds.clients[ClientId::new(2)].state, DsState::LiveRepair);
        assert!(ds.repair_min_id.is_some())
    }

    #[tokio::test]
    async fn test_check_for_repair_already_repair() {
        // No repair needed here.
        let (dst, _ds_work_rx) = create_test_dst_rx();
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));
        up.set_active().await.unwrap();
        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;
        // Make ds 0 ready for repair.
        up.ds_transition(ClientId::new(0), DsState::Faulted).await;
        up.ds_transition(ClientId::new(0), DsState::LiveRepairReady)
            .await;
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::RepairInProgress
        );
    }

    #[tokio::test]
    async fn test_check_for_repair_task_running() {
        let (dst, _ds_work_rx) = create_test_dst_rx();
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));
        up.set_active().await.unwrap();
        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(JobId(3));
        drop(ds);

        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::RepairInProgress
        );
    }
    // Test the permutations of calling repair_extent with each downstairs
    // as the one needing LiveRepair
    #[tokio::test]
    async fn test_repair_extent_no_action_all() {
        for or_ds in ClientId::iter() {
            test_repair_extent_no_action(or_ds).await;
        }
    }

    async fn test_repair_extent_no_action(or_ds: ClientId) {
        // Make sure repair jobs can flow through the work queue.
        // This is a pretty heavy test in that we simulate the downstairs tasks
        // and downstairs responses while we verify that the work queue side of
        // things does behaves as we expect it to.
        //
        // This test covers a simple case of calling repair_extent().
        // In this test, the simulated downstairs will return data that will
        // indicate that no repair is required for this extent.
        // We expect to see four jobs issued.
        //
        // Since some of the functionality for repair happens in io_send(),
        // which we don't call here, we are only testing that the proper number
        // of jobs are issued at the proper times.

        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let mut ds = up.downstairs.lock().await;
        for cid in ClientId::iter() {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                UpState::Active,
                Some(ei),
            )
            .unwrap();
        }
        drop(ds);

        ack_this_job(&up, gw_close_id, ds_close_id, Ok(())).await;
        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        // The repair job has shown up.  Move it forward.
        move_and_complete_job(&up, ds_repair_id, vec![])
            .await
            .unwrap();

        // Now ACK the repair job
        ack_this_job(&up, gw_repair_id, ds_repair_id, Ok(())).await;

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            show_all_work(&up).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        move_and_complete_job(&up, ds_noop_id, vec![])
            .await
            .unwrap();

        ack_this_job(&up, gw_noop_id, ds_noop_id, Ok(())).await;

        info!(up.log, "Finally, move the ReOpen job forward");
        move_and_complete_job(&up, ds_reopen_id, vec![])
            .await
            .unwrap();

        let mut ds = up.downstairs.lock().await;
        assert_eq!(ds.ackable_work().len(), 1);
        drop(ds);

        info!(up.log, "Now ACK the reopen job");
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, Ok(())).await;

        // The extent repair task should complete without error.
        assert!(re_handle.await.is_ok());

        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        info!(up.log, "jobs are: {:?}", jobs);
        let job = ds.ds_active.get(&ds_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = ds.ds_active.get(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = ds.ds_active.get(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);
    }

    // Loop over the possible downstairs to be in LiveRepair and
    // run through the do_repair code path.
    #[tokio::test]
    async fn test_repair_extent_do_repair_all() {
        for or_ds in ClientId::iter() {
            test_repair_extent_do_repair(or_ds).await;
        }
    }

    async fn test_repair_extent_do_repair(or_ds: ClientId) {
        // This test covers a simple case of calling repair_extent().
        // In this test, the simulated downstairs will return data that will
        // indicate that a repair is required for this extent.  We expect to
        // see four jobs issued.
        //
        // Since some of the functionality for repair happens in io_send(),
        // which we don't call here, we are only testing that the proper number
        // of jobs are issued at the proper times.
        //
        // As this test is similar to a previous test that verifies more,
        // we don't check all possible results and assume that the other
        // flow tests cover those cases.

        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        // Create two different ExtentInfo structs so we will have
        // a downstairs that requires repair.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let bad_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        for cid in ClientId::iter() {
            up.downstairs.lock().await.in_progress(ds_close_id, cid);
            if cid == or_ds {
                up.process_ds_operation(
                    ds_close_id,
                    cid,
                    Ok(vec![]),
                    Some(bad_ei),
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(ds_close_id, cid, Ok(vec![]), Some(ei))
                    .await
                    .unwrap();
            }
        }

        info!(up.log, "Now ACK the close job");
        ack_this_job(&up, gw_close_id, ds_close_id, Ok(())).await;

        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        // The repair job has shown up.  Move it forward.
        // Because the "or_ds" in this case is not returning an
        // error, we don't pass it to move_and_complete_job
        move_and_complete_job(&up, ds_repair_id, vec![])
            .await
            .unwrap();

        info!(up.log, "Now ACK the repair job");
        ack_this_job(&up, gw_repair_id, ds_repair_id, Ok(())).await;

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        move_and_complete_job(&up, ds_noop_id, vec![])
            .await
            .unwrap();

        info!(up.log, "Now ACK the NoOp job");
        ack_this_job(&up, gw_noop_id, ds_noop_id, Ok(())).await;

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        move_and_complete_job(&up, ds_reopen_id, vec![])
            .await
            .unwrap();

        info!(up.log, "Now ACK the repair job");
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, Ok(())).await;

        // The extent repair task should complete without error.
        assert!(re_handle.await.is_ok());

        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        let job = ds.ds_active.get(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveRepair { .. } => {}
            x => {
                panic!("Expected LiveRepair, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = ds.ds_active.get(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }

        assert_eq!(job.state_count().done, 3);
        assert_eq!(ds.clients[or_ds].state, DsState::LiveRepair);
    }

    #[tokio::test]
    async fn test_repair_extent_close_fails_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentFlushClose operation.
        for faild_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_close_fails(faild_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_close_fails(or_ds: ClientId, err_ds: ClientId) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the initial close command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in LiveRepair, and the
        // downstairs that will return error for the ExtentClose operation.
        // They may be the same downstairs.

        assert!(err_ds.get() < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };

        // Move the close job forward, but report error on the err_ds
        for cid in ClientId::iter() {
            up.downstairs.lock().await.in_progress(ds_close_id, cid);
            if cid == err_ds {
                up.process_ds_operation(
                    ds_close_id,
                    cid,
                    Err(CrucibleError::GenericError("bad".to_string())),
                    None,
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(ds_close_id, cid, Ok(vec![]), Some(ei))
                    .await
                    .unwrap();
            }
        }

        // process_ds_operation should force the downstairs to fail
        assert_eq!(
            up.downstairs.lock().await.clients[err_ds].state,
            DsState::Faulted
        );

        let my_err = Err(CrucibleError::GenericError("bad".to_string()));
        info!(up.log, "Now ACK the close job");
        ack_this_job(&up, gw_close_id, ds_close_id, my_err.clone()).await;

        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job (which will be a NoOp) to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }
        // The repair (NoOp) job has shown up.  Move it forward.
        move_and_complete_job(&up, ds_repair_id, vec![err_ds, or_ds])
            .await
            .unwrap();

        ack_this_job(&up, gw_repair_id, ds_repair_id, my_err.clone()).await;

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        move_and_complete_job(&up, ds_noop_id, vec![err_ds, or_ds])
            .await
            .unwrap();

        info!(up.log, "Now ACK the NoOp job");
        ack_this_job(&up, gw_noop_id, ds_noop_id, my_err.clone()).await;

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        move_and_complete_job(&up, ds_reopen_id, vec![err_ds, or_ds])
            .await
            .unwrap();
        info!(up.log, "Now ACK the Reopen job");
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, my_err.clone()).await;

        // repair_extent task should return to us an error.
        assert!(re_handle.await.is_err());

        show_all_work(&up).await;
        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        // Verify that the four jobs are the four we expect.
        let job = ds.ds_active.get(&ds_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        // Because the close failed, we sent a NoOp instead of repair.
        let job = ds.ds_active.get(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        if err_ds == or_ds {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        } else {
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        }

        let job = ds.ds_active.get(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        if err_ds == or_ds {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        } else {
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        }

        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        if err_ds == or_ds {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        } else {
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        }

        assert_eq!(ds.clients[err_ds].state, DsState::Faulted);
        assert_eq!(ds.clients[or_ds].state, DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_repair_fails_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveRepair operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_repair_fails(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_repair_fails(
        or_ds: ClientId,
        err_ds: ClientId,
    ) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the repair command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in LiveRepair, and the
        // downstairs that will return error for the ExtentLiveRepair
        // operation.  They may be the same downstairs.

        assert!(err_ds.get() < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let mut ds = up.downstairs.lock().await;
        for cid in ClientId::iter() {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                UpState::Active,
                Some(ei),
            )
            .unwrap();
        }
        drop(ds);

        info!(up.log, "Now ACK the close job");
        ack_this_job(&up, gw_close_id, ds_close_id, Ok(())).await;

        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        // Move the repair job forward, but report error on the err_ds
        for cid in ClientId::iter() {
            up.downstairs.lock().await.in_progress(ds_repair_id, cid);
            if cid == err_ds {
                up.process_ds_operation(
                    ds_repair_id,
                    cid,
                    Err(CrucibleError::GenericError("bad".to_string())),
                    None,
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(
                    ds_repair_id,
                    cid,
                    Ok(vec![]),
                    Some(ei),
                )
                .await
                .unwrap();
            }
        }
        let my_err = Err(CrucibleError::GenericError("bad".to_string()));
        ack_this_job(&up, gw_repair_id, ds_repair_id, my_err.clone()).await;

        // process_ds_completion should force both the downstairs that
        // reported the error, and the downstairs that is under repair to
        // fail.
        assert_eq!(
            up.downstairs.lock().await.clients[err_ds].state,
            DsState::Faulted
        );
        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        move_and_complete_job(&up, ds_noop_id, vec![err_ds, or_ds])
            .await
            .unwrap();

        info!(up.log, "Now ACK the NoOp job");
        ack_this_job(&up, gw_noop_id, ds_noop_id, my_err.clone()).await;

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        move_and_complete_job(&up, ds_reopen_id, vec![err_ds, or_ds])
            .await
            .unwrap();
        info!(up.log, "Now ACK the Reopen job");
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, my_err.clone()).await;

        // repair_extent task should return to us an error.
        assert!(re_handle.await.is_err());

        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        // Because the close failed, we expect a NoOp instead of repair.
        let job = ds.ds_active.get(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }

        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        let job = ds.ds_active.get(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }

        // What we expect changes a little depending on if we will fail two
        // different downstairs or just one.
        if err_ds != or_ds {
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        }

        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        }

        assert_eq!(ds.clients[err_ds].state, DsState::Faulted);
        assert_eq!(ds.clients[or_ds].state, DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_fail_noop_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveNoOp operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_fail_noop(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_fail_noop(or_ds: ClientId, err_ds: ClientId) {
        // Test repair_extent when the noop job fails.
        // We take input for both which downstairs is in LiveRepair, and
        // which downstairs will return error on the NoOp operation.

        assert!(err_ds.get() < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let mut ds = up.downstairs.lock().await;
        for cid in ClientId::iter() {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                UpState::Active,
                Some(ei),
            )
            .unwrap();
        }
        drop(ds);

        ack_this_job(&up, gw_close_id, ds_close_id, Ok(())).await;
        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        // The repair job has shown up.  Move it forward.
        move_and_complete_job(&up, ds_repair_id, vec![])
            .await
            .unwrap();

        // Now ACK the repair job
        ack_this_job(&up, gw_repair_id, ds_repair_id, Ok(())).await;

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            up.downstairs.lock().await.in_progress(ds_noop_id, cid);
            if cid == err_ds {
                up.process_ds_operation(
                    ds_noop_id,
                    cid,
                    Err(CrucibleError::GenericError("bad".to_string())),
                    None,
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(ds_noop_id, cid, Ok(vec![]), None)
                    .await
                    .unwrap();
            }
        }
        let my_err = Err(CrucibleError::GenericError("bad".to_string()));
        ack_this_job(&up, gw_noop_id, ds_noop_id, my_err.clone()).await;

        // Give repair_extent enough time to get the noop ACK and take
        // action on it.
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!(up.log, "Finally, move the ReOpen job forward");
        move_and_complete_job(&up, ds_reopen_id, vec![err_ds, or_ds])
            .await
            .unwrap();

        info!(up.log, "Now ACK the reopen job");
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, Ok(())).await;

        // The extent repair task should return to us the error.
        assert!(re_handle.await.is_err());
        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        // Differences from the usual path start here.
        let job = ds.ds_active.get(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            info!(up.log, "err:{} or:{}", err_ds, or_ds);
            assert_eq!(job.state_count().done, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().done, 2);
            assert_eq!(job.state_count().skipped, 1);
        }

        assert_eq!(ds.clients[err_ds].state, DsState::Faulted);
        assert_eq!(ds.clients[or_ds].state, DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_fail_reopen_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveReopen operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_fail_reopen(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_fail_reopen(or_ds: ClientId, err_ds: ClientId) {
        // Test repair_extent when the reopen job fails.
        // We take input for both which downstairs is in LiveRepair, and
        // which downstairs will return error on the NoOp operation.

        assert!(err_ds.get() < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let gw_close_id = 1;
        let gw_repair_id = 2;
        let gw_noop_id = 3;
        let gw_reopen_id = 4;

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let mut ds = up.downstairs.lock().await;
        for cid in ClientId::iter() {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                UpState::Active,
                Some(ei),
            )
            .unwrap();
        }
        drop(ds);

        ack_this_job(&up, gw_close_id, ds_close_id, Ok(())).await;
        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // Wait for this repair job to show up.
        let mut jobs = up.downstairs.lock().await.ds_active.len();
        while jobs != 3 {
            info!(up.log, "Waiting for 3 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        // The repair job has shown up.  Move it forward.
        move_and_complete_job(&up, ds_repair_id, vec![])
            .await
            .unwrap();

        // Now ACK the repair job
        ack_this_job(&up, gw_repair_id, ds_repair_id, Ok(())).await;

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let mut jobs = up.downstairs.lock().await.ds_active.len();

        // Wait for the NoOp jobs to show up.
        while jobs != 4 {
            info!(up.log, "Waiting for 4 jobs (currently {})", jobs);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            jobs = up.downstairs.lock().await.ds_active.len();
        }

        info!(up.log, "Now move the NoOp job forward");
        move_and_complete_job(&up, ds_noop_id, vec![])
            .await
            .unwrap();

        ack_this_job(&up, gw_noop_id, ds_noop_id, Ok(())).await;

        // Move the reopen job forward
        for cid in ClientId::iter() {
            up.downstairs.lock().await.in_progress(ds_reopen_id, cid);
            if cid == err_ds {
                up.process_ds_operation(
                    ds_reopen_id,
                    cid,
                    Err(CrucibleError::GenericError("bad".to_string())),
                    None,
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(ds_reopen_id, cid, Ok(vec![]), None)
                    .await
                    .unwrap();
            }
        }
        let my_err = Err(CrucibleError::GenericError("bad".to_string()));
        ack_this_job(&up, gw_reopen_id, ds_reopen_id, my_err.clone()).await;

        // The extent repair task should give us an error back
        assert!(re_handle.await.is_err());

        // We should have 4 jobs on the queue.
        let ds = up.downstairs.lock().await;
        assert_eq!(ds.ds_active.len(), 4);

        // All that is different from the normal path is the results from
        // the reopen job, so that is all we need to check here.
        let job = ds.ds_active.get(&ds_reopen_id).unwrap();
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        assert_eq!(ds.clients[err_ds].state, DsState::Faulted);
        assert_eq!(ds.clients[or_ds].state, DsState::Faulted);
    }

    #[tokio::test]
    async fn test_reserve_extent_repair_ids() {
        // Verify that we can reserve extent IDs for repair work, and they
        // are allocated as expected.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut ds = up.downstairs.lock().await;
        assert!(!ds.query_repair_ids(0));

        ds.reserve_repair_ids_for_extent(0);
        let (reserved_ids, _deps) = ds.repair_job_ids.get(&0).unwrap();
        assert_eq!(reserved_ids.close_id, JobId(1000));
        assert_eq!(reserved_ids.repair_id, JobId(1001));
        assert_eq!(reserved_ids.noop_id, JobId(1002));
        assert_eq!(reserved_ids.reopen_id, JobId(1003));

        let next_id = ds.next_id();
        assert_eq!(next_id, JobId(1004));

        // IDs should now be present
        assert!(ds.query_repair_ids(0));
        // No IDs for extent 1
        assert!(!ds.query_repair_ids(1));

        // Reserve the IDs again, make sure they are still the same.
        ds.reserve_repair_ids_for_extent(0);
        let (reserved_ids, _deps) = ds.repair_job_ids.get(&0).unwrap();
        assert_eq!(reserved_ids.close_id, JobId(1000));
        assert_eq!(reserved_ids.repair_id, JobId(1001));
        assert_eq!(reserved_ids.noop_id, JobId(1002));
        assert_eq!(reserved_ids.reopen_id, JobId(1003));

        // Get the IDs again this time.
        let (reserved_ids, _deps) = ds.get_repair_ids(0);
        assert_eq!(reserved_ids.close_id, JobId(1000));
        assert_eq!(reserved_ids.repair_id, JobId(1001));
        assert_eq!(reserved_ids.noop_id, JobId(1002));
        assert_eq!(reserved_ids.reopen_id, JobId(1003));

        // After the get, there should be no record any longer
        assert!(!ds.query_repair_ids(0));
    }

    #[tokio::test]
    async fn test_repair_io_no_el_skipped() {
        // Verify that IOs put on the queue when a downstairs is
        // in LiveRepair and extent_limit is still None will be skipped
        // only by the downstairs that is in LiveRepair..
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = 1000;

        let mut ds = up.downstairs.lock().await;
        assert!(ds.clients[ClientId::new(1)].extent_limit.is_none());
        // Check all three IOs.
        for job_id in (id..id + 3).map(JobId) {
            assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(1)).is_none());
            assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_below_el_sent() {
        // Verify that io put on the queue when a downstairs is
        // in LiveRepair and the IO is below the extent_limit
        // will be sent to all downstairs for processing.
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = 1000;
        let mut ds = up.downstairs.lock().await;
        // Check all three IOs.
        for job_id in (id..id + 3).map(JobId) {
            assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(1)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_at_el_sent() {
        // Verify that IOs put on the queue when a downstairs is
        // in LiveRepair and extent_limit is at the IO location
        // will be sent to all downstairs for processing.
        // For this situation, we are relying on the submit_write method
        // of the Upstairs to correctly add the dependencies for the
        // in flight or soon to be in flight repair jobs.
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = 1000;
        let mut ds = up.downstairs.lock().await;
        // Check all three IOs.
        for job_id in (id..id + 3).map(JobId) {
            assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(1)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_above_el_skipped() {
        // Verify that an IO put on the queue when a downstairs is
        // in LiveRepair and the IO is above the extent_limit will
        // be skipped by the downstairs that is in LiveRepair..
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        up.submit_write(
            Block::new_512(6),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(6),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(6),
            Bytes::from(vec![0xff; 512]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = 1000;
        let mut ds = up.downstairs.lock().await;
        // Check all three IOs.
        for job_id in (id..id + 3).map(JobId) {
            assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(1)).is_none());
            assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_span_el_sent() {
        // Verify that IOs put on the queue when a downstairs is
        // in LiveRepair and the IO starts at an extent that is below
        // the extent_limit, but extends to beyond the extent limit,
        // The IO will be sent to all downstairs.
        //
        // Note that this test does not check dependencies specifically,
        // but does expect that the three jobs it submits will all be
        // pushed down past the repair jobs that will be reserved on
        // their behalf.
        //
        // To visualise what is going on, here is what we are submitting:
        //
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 |
        // ----|-------|-------|-------|
        //   0 | W W W | W W W | W W W |
        //   1 | R R R | R R R | R R R |
        //   2 | WuWuWu| WuWuWu| WuWuWu|
        //
        // Because extent 1 is under repair, The first Write will detect
        // that and determine that it needs to create the future repair
        // IO job IDs, then add them to its dependency list.
        //
        // The same thing will happen for the Read and the WriteUnwritten,
        // but in their case the IDs will already be created so they can
        // just take them.
        //
        // So, what our actual dependency list will look like at the end
        // of creating and submitting this workload would be this:
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       | RpRpRp|
        //   1 |       |       | RpRpRp|
        //   2 |       |       | RpRpRp|
        //   3 |       |       | RpRpRp|
        //   4 | W W W | W W W | W W W | 3
        //   5 | R R R | R R R | R R R | 4
        //   6 | WuWuWu| WuWuWu| WuWuWu| 5
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512 * 9]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512 * 9),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512 * 9]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = 1004;
        let mut ds = up.downstairs.lock().await;
        // Check all three IOs.
        for job_id in (id..id + 3).map(JobId) {
            assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(1)).is_some());
            assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
        }

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs[0].work.deps(), &[JobId(1003)]);
        assert_eq!(jobs[1].work.deps(), &[JobId(1004)]);
        assert_eq!(jobs[2].work.deps(), &[JobId(1005)]);
    }

    #[tokio::test]
    async fn test_repair_read_span_el_sent() {
        // Verify that a read put on the queue when a downstairs is
        // in LiveRepair and the IO starts at an extent that is below
        // the extent_limit, but extends to beyond the extent limit,
        // The IO will be sent to all downstairs.
        //
        // To visualise what is going on, here is what we are submitting:
        //
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 |
        // ----|-------|-------|-------|
        //   0 | R R R | R R R | R R R |
        //
        // Because extent 1 is under repair, The read will detect that and
        // determine that it needs to create the future repair IO job IDs
        // then add them to this IOs dependency list.
        //
        // So, what our actual dependency list will look like is this:
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       | RpRpRp|
        //   1 |       |       | RpRpRp|
        //   2 |       |       | RpRpRp|
        //   3 |       |       | RpRpRp|
        //   4 | R R R | R R R | R R R | 3
        //
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_read(
            Block::new_512(0),
            Buffer::new(512 * 9),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let job_id = JobId(1004);
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
        assert!(ds.in_progress(job_id, ClientId::new(1)).is_some());
        assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        //
        // These future jobs are not actually created yet, so they
        // won't show up in the work queue.
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs[0].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_repair_write_span_el_sent() {
        // Verify that a write IO put on the queue when a downstairs is
        // in LiveRepair and the IO starts at an extent that is below
        // the extent_limit, but extends to beyond the extent limit,
        // The IO will be sent to all downstairs.
        //
        // To visualise what is going on, here is what we are submitting:
        //
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 |
        // ----|-------|-------|-------|
        //   0 | W W W | W W W | W W W |
        //
        // Because extent 1 is under repair, The first write will detect
        // that and determine that it needs to create the future repair
        // IO job IDs, then add them to this IOs dependency list.
        //
        // What our actual dependency list will look like at the end
        // of creating and submitting this workload would be this:
        //     |       | UNDER |       |
        //     |       | REPAIR|       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       | RpRpRp|
        //   1 |       |       | RpRpRp|
        //   2 |       |       | RpRpRp|
        //   3 |       |       | RpRpRp|
        //   4 | W W W | W W W | W W W | 3

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512 * 9]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = JobId(1004);
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(id, ClientId::new(0)).is_some());
        assert!(ds.in_progress(id, ClientId::new(1)).is_some());
        assert!(ds.in_progress(id, ClientId::new(2)).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs[0].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_repair_write_span_two_el_sent() {
        // Verify that a write IO put on the queue when a downstairs is
        // in LiveRepair and the IO starts at the extent that is under
        // repair and extends for an additional two extents.
        //
        // The IO will be sent to all downstairs.
        //
        // To visualise what is going on, here is what we are submitting:
        //
        //     | UNDER |       |       |
        //     | REPAIR|       |       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 |
        // ----|-------|-------|-------|
        //   0 | W W W | W W W | W W W |
        //
        // Because extent 0 is under repair, The write will detect
        // that and determine that its size extends to two additional extents
        // and that it needs to create the future repair IO job IDs,
        // then make itself dependencies of those future repairs.
        // Given the size of our extents, this may not even be possible, but
        // if it does happen, we know it will work..
        //
        // What our actual dependency list will look like at the end
        // of creating and submitting this workload would be this:
        //     | UNDER |       |       |
        //     | REPAIR|       |       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       | RpRpRp|       |
        //   1 |       | RpRpRp|       |
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       |       | RpRpRp|
        //   5 |       |       | RpRpRp|
        //   6 |       |       | RpRpRp|
        //   7 |       |       | RpRpRp|
        //   8 | W W W | W W W | W W W | 3,7
        //
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512 * 9]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let id = JobId(1008);
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(id, ClientId::new(0)).is_some());
        assert!(ds.in_progress(id, ClientId::new(1)).is_some());
        assert!(ds.in_progress(id, ClientId::new(2)).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs[0].work.deps(), &[JobId(1003), JobId(1007)]);
    }

    #[tokio::test]
    async fn test_live_repair_update() {
        // Make sure that process_ds_completion() will take extent info
        // result and put it on the live repair hashmap.
        // As we don't have an actual downstairs here, we "fake it" by
        // feeding the responses we expect back from the downstairs.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let eid = 1;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        let gw_close_id = 1;
        let (repair_ids, deps) = ds.get_repair_ids(eid);
        // Create and insert the close job on the work queue.
        let close_io = create_close_io(
            eid as usize,
            repair_ids.close_id,
            deps,
            gw_close_id,
            3,                      // flush
            2,                      // gen
            ClientId::new(0),       // source
            vec![ClientId::new(1)], // repair
        );

        let (send, recv) = oneshot::channel();
        let op = BlockOp::RepairOp;
        let close_br = BlockReq::new(op, send);
        let _close_brw = BlockReqWaiter::new(recv);
        let new_gtos = GtoS::new(repair_ids.close_id, None, Some(close_br));
        {
            gw.active.insert(gw_close_id, new_gtos);
        }

        ds.enqueue_repair(close_io);

        ds.in_progress(repair_ids.close_id, ClientId::new(0));
        ds.in_progress(repair_ids.close_id, ClientId::new(1));
        ds.in_progress(repair_ids.close_id, ClientId::new(2));

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        // First two are false, final job will return true
        assert!(!ds
            .process_ds_completion(
                repair_ids.close_id,
                ClientId::new(0),
                Ok(vec![]),
                UpState::Active,
                Some(ei)
            )
            .unwrap());

        // Verify the extent information has been added to the repair info
        // hashmap for client 0
        let new_ei = ds.clients[ClientId::new(0)].repair_info.unwrap();
        assert_eq!(new_ei.generation, 5);
        assert_eq!(new_ei.flush_number, 3);
        assert!(!new_ei.dirty);
        assert!(ds.clients[ClientId::new(1)].repair_info.is_none());
        assert!(ds.clients[ClientId::new(2)].repair_info.is_none());

        // Verify the extent information has been added to the repair info
        // hashmap for client 1
        let ei = ExtentInfo {
            generation: 2,
            flush_number: 4,
            dirty: true,
        };
        assert!(!ds
            .process_ds_completion(
                repair_ids.close_id,
                ClientId::new(1),
                Ok(vec![]),
                UpState::Active,
                Some(ei)
            )
            .unwrap());
        let new_ei = ds.clients[ClientId::new(1)].repair_info.unwrap();
        assert_eq!(new_ei.generation, 2);
        assert_eq!(new_ei.flush_number, 4);
        assert!(new_ei.dirty);
        assert!(ds.clients[ClientId::new(2)].repair_info.is_none());

        let ei = ExtentInfo {
            generation: 29,
            flush_number: 444,
            dirty: false,
        };
        assert!(ds
            .process_ds_completion(
                repair_ids.close_id,
                ClientId::new(2),
                Ok(vec![]),
                UpState::Active,
                Some(ei)
            )
            .unwrap());
        let new_ei = ds.clients[ClientId::new(2)].repair_info.unwrap();
        assert_eq!(new_ei.generation, 29);
        assert_eq!(new_ei.flush_number, 444);
        assert!(!new_ei.dirty);
    }

    // What follows is a number of tests for the repair solver code,
    // specifically the repair_or_noop() function.  This set of tests
    // makes use of a few layers of boilerplate to avoid a bunch of
    // code duplication and simplifies what the tests need to look like.
    //
    // The basic format of each test is.
    // Generate a desired good and bad ExtentInfo struct.
    // Decide what downstairs client should be the source for repair.
    // Decide which downstairs clients should be the destination for repair.
    //
    // To test, we send the good EI, the bad EI and the source/dest client IDs
    // and verify that the IOop::ExtentLiveRepair or IOop::ExtentNoOp is
    // generated (and populated) correctly.
    //
    #[tokio::test]
    async fn test_solver_no_work() {
        // Make sure that the repair solver will return NoOp when a repair
        // is not required.
        let mut ds = create_test_downstairs();

        for source in ClientId::iter() {
            let ei = ExtentInfo {
                generation: 5,
                flush_number: 3,
                dirty: false,
            };
            assert!(ds.clients[ClientId::new(0)]
                .repair_info
                .replace(ei)
                .is_none());
            assert!(ds.clients[ClientId::new(1)]
                .repair_info
                .replace(ei)
                .is_none());
            assert!(ds.clients[ClientId::new(2)]
                .repair_info
                .replace(ei)
                .is_none());

            let repair_extent = if source == ClientId::new(0) {
                vec![ClientId::new(1), ClientId::new(2)]
            } else if source == ClientId::new(1) {
                vec![ClientId::new(0), ClientId::new(2)]
            } else {
                vec![ClientId::new(0), ClientId::new(1)]
            };
            let eid = 0;
            let (repair_ids, deps) = ds.get_repair_ids(eid);

            let repair_op = repair_or_noop(
                &mut ds,
                eid as usize,         // Extent
                repair_ids.repair_id, // ds_id
                deps,                 // Vec<u64>
                1,                    // gw_id
                source,               // Source extent
                &repair_extent,       // Repair extent
            );

            println!("repair op: {:?}", repair_op);
            match repair_op.work {
                IOop::ExtentLiveNoOp { dependencies: _ } => {}
                x => {
                    panic!("Incorrect work type returned: {:?}", x);
                }
            }
            assert_eq!(repair_op.ds_id, repair_ids.repair_id);
            assert_eq!(repair_op.guest_id, 1);
            println!("Passed for source {}", source);
        }
    }

    // Sub-test that a ExtentLiveRepair IOop is returned from repair_or_noop,
    // This sub-function allows us to try different source and repair clients
    // and verifies the expected results.
    //
    // This function requires you have already populated the downstairs
    // repair_info field with the desired extent info you wish to compare, and
    // You are sending in the source and repair(Vec) the client IDs you
    // expect to see in the resulting IOop::ExtentLiveRepair.
    fn what_needs_repair(
        ds: &mut Downstairs,
        source: ClientId,
        repair: Vec<ClientId>,
    ) {
        let (repair_ids, deps) = ds.get_repair_ids(0);
        let repair_op = repair_or_noop(
            ds,
            0,                   // Extent
            repair_ids.close_id, // ds_id
            deps,                // Vec<u64>
            1,                   // gw_id
            source,
            &repair,
        );

        println!("repair op: {:?}", repair_op);

        match repair_op.work {
            IOop::ExtentLiveRepair {
                dependencies: _,
                extent,
                source_downstairs,
                source_repair_address: _,
                repair_downstairs,
            } => {
                assert_eq!(extent, 0);
                assert_eq!(source_downstairs, source);
                assert_eq!(repair_downstairs, repair);
            }
            x => {
                panic!("Incorrect work type returned: {:?}", x);
            }
        }
        assert_eq!(repair_op.ds_id, repair_ids.close_id);
        assert_eq!(repair_op.guest_id, 1);
    }

    // Given a good ExtentInfo, and a bad ExtentInfo, generate all the
    // possible combinations of downstairs clients with one source
    // downstairs client, and one repair downstairs client.
    //
    // We manually submit the good ExtentInfo and the bad ExtentInfo
    // into the downstairs hashmap at the proper place, then verify
    // that, after repair_or_noop() has run, we have generated the
    // expected IOop.
    fn submit_one_source_one_repair(
        ds: &mut Downstairs,
        good_ei: ExtentInfo,
        bad_ei: ExtentInfo,
    ) {
        for source in ClientId::iter() {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // First try one source, one repair
            let repair = if source == ClientId::new(0) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(1)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(0)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);

            // Next try the other downstairs to repair.
            let repair = if source == ClientId::new(2) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(1)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(2)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);
        }
    }

    // Given a good ExtentInfo, and a bad ExtentInfo, generate all the
    // possible combinations of downstairs clients with one source
    // extent, and two repair extents.
    //
    // We manually submit the good ExtentInfo and the bad ExtentInfo
    // into the downstairs hashmap at the proper place, then verify
    // that, after repair_or_noop() has run, we have generated the
    // expected IOop.
    fn submit_one_source_two_repair(
        ds: &mut Downstairs,
        good_ei: ExtentInfo,
        bad_ei: ExtentInfo,
    ) {
        for source in ClientId::iter() {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // One source, two repair
            let repair = if source == ClientId::new(0) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(1), ClientId::new(2)]
            } else if source == ClientId::new(1) {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                vec![ClientId::new(0), ClientId::new(2)]
            } else {
                assert!(ds.clients[ClientId::new(0)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(1)]
                    .repair_info
                    .replace(bad_ei)
                    .is_none());
                assert!(ds.clients[ClientId::new(2)]
                    .repair_info
                    .replace(good_ei)
                    .is_none());
                vec![ClientId::new(0), ClientId::new(1)]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);
        }
    }

    #[tokio::test]
    async fn test_solver_dirty_needs_repair_two() {
        // Make sure that the repair solver will see a dirty extent_info
        // field is true and mark that downstairs for repair.
        // We test with two downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: true,
        };
        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_dirty_needs_repair_one() {
        // Make sure that the repair solver will see a dirty extent_info
        // field is true and repair that downstairs.
        // We test with just one downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: true,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_gen_lower_needs_repair_one() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_gen_lower_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_gen_higher_needs_repair_one() {
        // Make sure that the repair solver will see a generation extent_info
        // field is higher on a downstairs client, and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 2,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_gen_higher_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 3,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 6,
            flush_number: 3,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_flush_lower_needs_repair_one() {
        // Make sure that the repair solver will see a flush extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 2,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_flush_lower_needs_repair_two() {
        // Make sure that the repair solver will see a flush extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 4,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 4,
            flush_number: 1,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_flush_higher_needs_repair_one() {
        // Make sure that the repair solver will see a flush extent_info
        // field is higher on a downstairs client, and mark that downstairs
        // for repair.  We test with one downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 9,
            flush_number: 3,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 9,
            flush_number: 4,
            dirty: false,
        };

        submit_one_source_one_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_solver_flush_higher_needs_repair_two() {
        // Make sure that the repair solver will see a generation extent_info
        // field is lower on a downstairs client  and mark that downstairs
        // for repair.  We test with two downstairs to repair here.

        let mut ds = create_test_downstairs();
        let g_ei = ExtentInfo {
            generation: 6,
            flush_number: 8,
            dirty: false,
        };
        let b_ei = ExtentInfo {
            generation: 6,
            flush_number: 9,
            dirty: false,
        };

        submit_one_source_two_repair(&mut ds, g_ei, b_ei);
    }

    #[tokio::test]
    async fn test_live_repair_enqueue_reopen() {
        // Make sure the create_and_enqueue_reopen_io() function does
        // what we expect it to do, which also tests create_reopen_io()
        // function as well.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        // Upstairs "guest" work IDs.
        let gw_r_id: u64 = 1;
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());
        deps.push(JobId(1000));
        deps.push(JobId(1001));

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)j
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        let _reopen_brw = create_and_enqueue_reopen_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            repair_ids.reopen_id,
            gw_r_id,
        );

        let job = ds.ds_active.get(&repair_ids.reopen_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveReopen {
                dependencies: d,
                extent: e,
            } => {
                assert_eq!(*d, &[JobId(1000), JobId(1001)]);
                assert_eq!(*e, eid as usize);
            }
            x => {
                panic!("Bad DownstairsIO type returned: {:?}", x);
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_live_repair_enqueue_close() {
        // Make sure the create_and_enqueue_close_io() function does
        // what we expect it to do, which also tests create_close_io()
        // function as well.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = 1;
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());
        deps.push(JobId(1000));
        deps.push(JobId(1001));

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)j
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        let next_flush = 2;
        let gen = 4;
        let source = ClientId::new(1);
        let repair = vec![ClientId::new(0), ClientId::new(2)];
        let _reopen_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            next_flush,
            gen,
            deps,
            repair_ids.close_id,
            gw_close_id,
            source,
            &repair,
        );

        let job = ds.ds_active.get(&repair_ids.close_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
                source_downstairs,
                repair_downstairs,
            } => {
                assert_eq!(*dependencies, &[JobId(1000), JobId(1001)]);
                assert_eq!(*extent, eid as usize);
                assert_eq!(*flush_number, next_flush);
                assert_eq!(*gen_number, gen);
                assert_eq!(*source_downstairs, source);
                assert_eq!(*repair_downstairs, repair);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentFlushClose, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_live_repair_enqueue_repair_noop() {
        // Make sure the create_and_enqueue_repair_io() function does
        // what we expect it to do, which also tests create_repair_io()
        // function as well.  In this case we expect the job created to
        // be a no-op job.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        // Upstairs "guest" work IDs.
        let gw_repair_id: u64 = gw.next_gw_id();
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());
        deps.push(JobId(1001));
        deps.push(JobId(1002));

        let source = ClientId::new(0);
        let repair = vec![ClientId::new(1), ClientId::new(2)];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            ds.clients[cid].repair_info = Some(ei);
        }

        let _repair_brw = create_and_enqueue_repair_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            repair_ids.repair_id,
            gw_repair_id,
            source,
            &repair,
        );

        let job = ds.ds_active.get(&repair_ids.repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveNoOp { dependencies } => {
                assert_eq!(*dependencies, [JobId(1001), JobId(1002)]);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveNoOp, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_live_repair_enqueue_repair_repair() {
        // Make sure the create_and_enqueue_repair_io() function does
        // what we expect it to do, which also tests create_repair_io()
        // function as well.
        let up = create_test_upstairs(ClientId::new(1)).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        // Upstairs "guest" work IDs.
        let gw_repair_id: u64 = gw.next_gw_id();
        let (repair_ids, mut deps) = ds.get_repair_ids(eid);
        assert!(deps.is_empty());
        deps.push(JobId(1001));
        deps.push(JobId(1002));
        let source = ClientId::new(0);
        let repair = vec![ClientId::new(1), ClientId::new(2)];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        ds.clients[ClientId::new(0)].repair_info = Some(ei);
        ds.clients[ClientId::new(1)].repair_info = Some(ei);
        let bad_ei = ExtentInfo {
            generation: 5,
            flush_number: 2,
            dirty: false,
        };
        ds.clients[ClientId::new(2)].repair_info = Some(bad_ei);
        // We also need a fake repair address
        for cid in ClientId::iter() {
            ds.clients[cid].repair_addr =
                Some("127.0.0.1:1234".parse().unwrap());
        }

        let _reopen_brw = create_and_enqueue_repair_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            repair_ids.repair_id,
            gw_repair_id,
            source,
            &repair,
        );

        let job = ds.ds_active.get(&repair_ids.repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs,
                source_repair_address,
                repair_downstairs,
            } => {
                assert_eq!(*dependencies, &[JobId(1001), JobId(1002)]);
                assert_eq!(*extent, eid as usize);
                assert_eq!(*source_downstairs, source);
                assert_eq!(
                    *source_repair_address,
                    "127.0.0.1:1234".parse().unwrap()
                );
                // We are only repairing 2 because it had the different
                // ExtentInfo.  0 and 1 were the same.
                assert_eq!(*repair_downstairs, vec![ClientId::new(2)]);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveRepair, got: {:?}",
                    x
                );
            }
        }
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    /// Create and enqueue a full set of repair functions
    ///
    /// This gets the required locks, determines the next job IDs, checks
    /// dependencies, and enqueues all four jobs (close / repair / noop /
    /// reopen).
    ///
    /// The repair job is **actually** enqueued as an `ExtentLiveNoop` as well
    /// (instead of an `ExtentLiveRepair`, because the latter requires more
    /// infrastructure to be set up in the `Downstairs`; specifically,
    /// `create_and_enqueue_repair_io` expects `Downstairs::repair_info` to be
    /// valid.
    ///
    /// This function creates the same dependency chain as `repair_extent`,
    /// which is actually used in a real system, but it doesn't wait for repairs
    /// to complete before enqueuing stuff.
    async fn create_and_enqueue_repair_ops(up: &Arc<Upstairs>, eid: u64) {
        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        let (extent_repair_ids, deps) = ds.get_repair_ids(eid);

        let gw_close_id = gw.next_gw_id();
        let _close_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            2, // next_flush
            4, // gen
            deps,
            extent_repair_ids.close_id,
            gw_close_id,
            ClientId::new(0),    // source downstairs
            &[ClientId::new(1)], // repair downstairs
        );
        let gw_repair_id = gw.next_gw_id();
        let _repair_brw = create_and_enqueue_noop_io(
            &mut ds,
            &mut gw,
            vec![extent_repair_ids.close_id],
            extent_repair_ids.repair_id,
            gw_repair_id,
        );
        let gw_noop_id = gw.next_gw_id();
        let _noop_brw = create_and_enqueue_noop_io(
            &mut ds,
            &mut gw,
            vec![extent_repair_ids.repair_id],
            extent_repair_ids.noop_id,
            gw_noop_id,
        );
        let gw_reopen_id = gw.next_gw_id();
        let _reopen_brw = create_and_enqueue_reopen_io(
            &mut ds,
            &mut gw,
            eid,
            vec![extent_repair_ids.noop_id],
            extent_repair_ids.reopen_id,
            gw_reopen_id,
        );
    }

    // The next section of tests verify dependencies are honored.

    // W is Write
    // R is read
    // F is flush
    // Rp is a Repair

    #[tokio::test]
    async fn test_live_repair_deps_writes() {
        // Test that writes on different blocks in the extent are all
        // captured by the repair at the end.
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 |   W   |
        //   2 |     W |
        //   3 | RpRpRp| 0,1,2
        //   4 | RpRpRp| 3
        //   5 | RpRpRp| 4
        //   6 | RpRpRp| 5

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Write operations 0 to 2
        for i in 0..3 {
            up.submit_write(
                Block::new_512(i),
                Bytes::from(vec![0xff; 512]),
                None,
                false,
                ds_done_tx.clone(),
            )
            .await
            .unwrap();
        }

        // Repair IO functions assume you have the locks
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 7);

        // The three writes don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(
            jobs[3].work.deps(),
            &[jobs[0].ds_id, jobs[1].ds_id, jobs[2].ds_id]
        );
    }

    #[tokio::test]
    async fn test_live_repair_deps_reads() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   R   |
        //   2 |     R |
        //   3 | RpRpRp| 0,1,2
        //   4 | RpRpRp| 3
        //   5 | RpRpRp| 4
        //   6 | RpRpRp| 5

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Create read operations 0 to 2
        for i in 0..3 {
            up.submit_read(
                Block::new_512(i),
                Buffer::new(512),
                None,
                ds_done_tx.clone(),
            )
            .await
            .unwrap();
        }

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 7);

        // The three reads don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(
            jobs[3].work.deps(),
            &[jobs[0].ds_id, jobs[1].ds_id, jobs[2].ds_id]
        );
    }

    #[tokio::test]
    async fn test_live_repair_deps_mix() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   W   |
        //   2 | F F F | 0,1
        //   3 |RpRpRp | 2
        //   4 |RpRpRp |
        //   5 |RpRpRp |
        //   6 |RpRpRp |

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 7);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        // The flush requires the read and the write
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id]);
        // The repair will have just the flush
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair() {
        // Basic test for inter-repair dependencies
        //
        // This only actually tests our test function
        // (`create_and_enqueue_repair_ops`), not the actual `repair_extent`,
        // but is still worthwhile
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp| 0
        //   2 | RpRpRp| 1
        //   3 | RpRpRp| 2
        let up = create_test_upstairs(ClientId::new(1)).await;
        let eid = 0;
        create_and_enqueue_repair_ops(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 4);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].ds_id, JobId(1001));
        assert_eq!(jobs[1].work.deps(), &[JobId(1000)]);
        assert_eq!(jobs[2].ds_id, JobId(1002));
        assert_eq!(jobs[2].work.deps(), &[JobId(1001)]);
        assert_eq!(jobs[3].ds_id, JobId(1003));
        assert_eq!(jobs[3].work.deps(), &[JobId(1002)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_write() {
        // Write after repair depends on the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp| 0
        //   2 | RpRpRp| 1
        //   3 | RpRpRp| 2
        //   4 |     W | 3

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());

        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());

        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_read() {
        // Read after repair requires the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp|
        //   2 | RpRpRp|
        //   3 | RpRpRp|
        //   4 | R     | 3
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;

        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        // The read depends on the last item of the repair
        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_flush() {
        // Flush after repair requires the flush
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp|
        //   2 | RpRpRp|
        //   3 | RpRpRp|
        //   4 | F F F | 3
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.clients[ClientId::new(1)].extent_limit =
            Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_repair_ops(&up, eid).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;

        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        // The flush depends on the repair close operation
        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_no_overlap() {
        // No overlap, no deps, IO before repair
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |     W |       |       |
        //   1 |       |       | R     |
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |       | RpRpRp|       |
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(6),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 6);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_live_repair_deps_after_no_overlap() {
        // No overlap no deps IO after repair.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       | RpRpRp|       |
        //   1 |       | RpRpRp|       |
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       |       |   R   |
        //   5 |  W    |       |       |
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_ops(&up, 1).await;

        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(6),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 6);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[4].work.deps().is_empty());
        assert!(jobs[5].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_live_repair_deps_flush_repair_flush() {
        // Flush Repair Flush
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | F F F | F F F |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |       | RpRpRp|
        //   5 | F F F | F F F | 0,4
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 6);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());

        assert_eq!(jobs[1].ds_id, JobId(1001));
        assert_eq!(jobs[1].work.deps(), &[JobId(1000)]);

        assert_eq!(jobs[5].ds_id, JobId(1005));
        assert_eq!(jobs[5].work.deps(), &[JobId(1000), JobId(1004)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_flush_repair() {
        // Repair Flush Repair
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | RpRpRp|       |
        //   1 | RpRpRp|       |
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | F F F | F F F | 3
        //   5 |       | RpRpRp| 4
        //   6 |       | RpRpRp|
        //   7 |       | RpRpRp|
        //   8 |       | RpRpRp|
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_ops(&up, 0).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 9);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
        assert_eq!(jobs[5].ds_id, JobId(1005));
        assert_eq!(jobs[5].work.deps(), &[JobId(1004)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_wspan_left() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   W W | W     |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 0
        //   3 |       | RpRpRp| 0
        //   4 |       | RpRpRp| 0

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A write of blocks 1,2 and 3 which spans the extent.
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_wspan_right() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | RpRpRp|       |
        //
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A write of blocks 2,3 and 4 which spans the extent.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 0).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_rspan_left() {
        // A repair will depend on a read spanning the extent

        // Read spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   R R | R     |
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |       | RpRpRp|

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A read of blocks 1,2 and 3 which spans the extent.
        up.submit_read(
            Block::new_512(1),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;
        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_rspan_right() {
        // A repair will depend on a read spanning the extent
        // Read spans other extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     R | R R   |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       |
        //   3 | RpRpRp|       |
        //   4 | RpRpRp|       |

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A read of blocks 2,3 and 4 which spans the extent.
        up.submit_read(
            Block::new_512(2),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 0).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_other() {
        // A write can be depended on by two different repairs, who won't
        // depend on each other.
        // This situation does not really exist, as a repair won't start
        // until the previous repair finishes.
        //       block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W     |
        //   1 | RpRpRp|       | 0
        //   2 | RpRpRp|       | 1
        //   3 | RpRpRp|       | 2
        //   4 | RpRpRp|       | 3
        //   5 |       | RpRpRp| 0
        //   6 |       | RpRpRp| 5
        //   7 |       | RpRpRp| 6
        //   8 |       | RpRpRp| 6

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A write of blocks 2 and 3 which spans the extent.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 2]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 0).await;
        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 9);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[5].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_super_spanner() {
        // Super spanner
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R R R | R R R |
        //   1 |       | RpRpRp|       | 0
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A read of blocks 0-8, spans three extents.
        up.submit_read(
            Block::new_512(0),
            Buffer::new(512 * 9),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_wafter() {
        // Write after repair spans extent.
        // This write needs to include a future repair that
        // does not exist yet.
        // The write below will update both extent 0 (blocks 0,1,2)
        // and extent 1 (blocks 3,4,5).  Since the write depends on
        // the repair for extent 0, but also will change extent 1, we
        // have to hold this write until both extents are repaired.
        //
        // This means that the write will reserve a new set of job IDs, which
        // will be allocated but not enqueued.
        //
        // The IO starts like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | Rclose|       |
        //   1 | Rrep  |       | 0
        //   2 | Rnoop |       | 1
        //   3 | Ropen |       | 2
        //     |     W | W W   |
        //
        // However, once the write notices the repair in progress, it will
        // create space for the three future repair jobs that will make
        // the dep list look like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //  *0 | Rclose|       |
        //  *1 | Rrep  |       | 0
        //  *2 | Rnoop |       | 1
        //  *3 | Ropen |       | 2
        //   4 |       | Rclose|
        //   5 |       | Rrep  | 4
        //   6 |       | Rnoop | 5
        //   7 |       | Ropen | 6
        //  *8 |     W | W W   | 3,7
        //
        //  (jobs marked with * are enqueued)
        //
        // We also verify that the job IDs make sense for our repair id
        // reservation that happens when we need to insert a job like this.

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_ops(&up, 0).await;

        // A write of blocks 2,3,4 which spans extent 0 and extent 1.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);
        assert!(jobs[0].work.deps().is_empty());

        // We know our first repair job will reserve 4 ids: 1000, 1001,
        // 1002, and 1003. The write to a spanning extent will go
        // and create 4 more job IDs, but will not enqueue them, so the write
        // will have job ID 1008 but will be at position 4 in the list

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[4].ds_id, JobId(1008));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003), JobId(1007)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_rafter() {
        // Read after spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 1
        //   3 |       | RpRpRp| 2
        //   4 |   R R | R     | 3

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_ops(&up, 1).await;

        up.submit_read(
            Block::new_512(1),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_overlappers() {
        // IOs that span both sides.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R     |       |
        //   1 |       |     W | W W W |
        //   2 |       | RpRpRp|       | 0,1
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |       | RpRpRp|       |
        //
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512 * 4),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // A write of blocks 5,6,7,8 which spans two extents.
        up.submit_write(
            Block::new_512(5),
            Bytes::from(vec![0xff; 512 * 4]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // The final repair command
        create_and_enqueue_repair_ops(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 6);

        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id]);
    }

    #[tokio::test]
    async fn test_live_repair_deps_repair_kitchen_sink() {
        // Repair simulator
        // In truth, you would never have more than one repair out at
        // the same time (the way it is now) but from a pure dependency point
        // of view, there is no reason you could not.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       |   W   |
        //   1 | RpRpRp|       |       |
        //   2 | RpRpRp|       |       | 1
        //   3 | RpRpRp|       |       | 2
        //   4 | RpRpRp|       |       | 3
        //   5 |       |   W   |       |
        //   6 |     R |       |       | 4
        //   7 |       | RpRpRp|       | 5
        //   8 |       | RpRpRp|       | 7
        //   9 |       | RpRpRp|       | 8
        //  10 |       | RpRpRp|       | 9
        //  11 |       |     W |       | 10
        //  12 |       |       | RpRpRp| 0
        //  13 |       |       | RpRpRp| 12
        //  14 |       |       | RpRpRp| 13
        //  15 |       |       | RpRpRp| 14
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_write(
            Block::new_512(7),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // The first repair command
        create_and_enqueue_repair_ops(&up, 0).await;

        up.submit_write(
            Block::new_512(4),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(2),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // The second repair command
        create_and_enqueue_repair_ops(&up, 1).await;

        up.submit_write(
            Block::new_512(5),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // The third repair command
        create_and_enqueue_repair_ops(&up, 2).await;

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 16);

        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());

        assert_eq!(jobs[1].ds_id, JobId(1001));
        assert!(jobs[1].work.deps().is_empty());

        assert_eq!(jobs[5].ds_id, JobId(1005));
        assert!(jobs[5].work.deps().is_empty());

        assert_eq!(jobs[6].ds_id, JobId(1006));
        assert_eq!(jobs[6].work.deps(), &[JobId(1004)]);

        assert_eq!(jobs[7].ds_id, JobId(1007));
        assert_eq!(jobs[7].work.deps(), &[JobId(1005)]);

        assert_eq!(jobs[11].ds_id, JobId(1011));
        assert_eq!(jobs[11].work.deps(), &[JobId(1010)]);

        assert_eq!(jobs[12].ds_id, JobId(1012));
        assert_eq!(jobs[12].work.deps(), &[JobId(1000)]);
    }

    #[tokio::test]
    async fn test_live_repair_no_repair_yet() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, but we have not yet started the actual repair
        // work. IOs that arrive at this point in time should go ahead
        // on the good downstairs client, and still be skipped on the
        // LiveRepair client
        //
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   0 |   R   |       |
        //   0 | F F F | F F F | 0,1

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // A write of blocks 2,3,4 which spans the extent.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // A read of block 1.
        up.submit_read(
            Block::new_512(1),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id, jobs[1].ds_id]);
        // Check that the IOs were skipped on downstairs 1.
        assert_eq!(jobs[0].state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(jobs[1].state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(jobs[2].state[ClientId::new(1)], IOState::Skipped);
    }

    #[tokio::test]
    async fn test_live_repair_repair_write_push() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  The write (that spans extents should have
        // created IDs for future repair work and then made itself
        // dependent on those repairs finishing.
        //
        // We start like this:
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //
        // But, end up with spots reserved for a future repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |     W | W W   | 3

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        // A write of blocks 2,3,4 which spans extents.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 1);

        assert_eq!(jobs[0].ds_id, JobId(1004));
        assert_eq!(jobs[0].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_repair_read_push() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  The read (that spans extents should have
        // created IDs for future repair work and then made itself
        // dependent on those repairs finishing.
        //
        // We start like this:
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     R | R R   |
        //
        // But, end up with spots reserved for a future repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp| 0
        //   2 |       | RpRpRp| 1
        //   3 |       | RpRpRp| 2
        //  *4 |     R | R R   | 3

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        // A read of blocks 2,3,4 which spans extents.
        up.submit_read(
            Block::new_512(2),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // Now enqueue the repair on extent 1, it should populate one of the
        // empty job slots.
        create_and_enqueue_repair_ops(&up, 1).await;
        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 5);

        // The close job is the first one in the job list
        assert_eq!(jobs[0].ds_id, JobId(1000));
        // The repair job should have no dependencies.
        assert!(jobs[0].work.deps().is_empty());
        // Our read should be ID 1004, and depends on the last repair job
        assert_eq!(jobs[4].ds_id, JobId(1004));
        assert_eq!(jobs[4].work.deps(), &[JobId(1003)]);
    }

    #[tokio::test]
    async fn test_live_repair_flush_is_flush() {
        // This is a special repair case.  We have a downstairs that is in
        // LiveRepair, and we have indicated that this extent is
        // under repair.  A flush should depend on any outstanding
        // repair operations, but won't generate future repair dependencies
        // like reads or writes do, as the flush will make use of the
        // extent_limit to allow it to slip in between repairs as well as
        // keep consistent what needs to be, and not flush what should not
        // be flushed.
        //
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | F F F | F F F |

        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_live_repair_send_io_write_below() {
        // Verify that we will send a write during LiveRepair when
        // the IO is an extent that is already repaired.
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        let job_id = JobId(1000);
        // A write of block 1 extents 0 (already repaired).
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // submit_write will call enqueue, so let's see where our
        // job is now.
        show_all_work(&up).await;

        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(job_id, ClientId::new(0)).is_some());
        assert!(ds.in_progress(job_id, ClientId::new(1)).is_some());
        assert!(ds.in_progress(job_id, ClientId::new(2)).is_some());
    }

    // Test function to put some work on the work queues.
    async fn submit_three_ios(
        up: &Arc<Upstairs>,
        ds_done_tx: &mpsc::Sender<()>,
    ) {
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // WriteUnwritten
        up.submit_write(
            Block::new_512(0),
            Bytes::from(vec![0xff; 512]),
            None,
            true,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_repair_abort_basic() {
        // Testing of abort_repair functions.
        // Starting with one downstairs in LiveRepair state, the functions
        // will:
        // Move the LiveRepair downstairs to Faulted.
        // Move all IO for that downstairs to skipped.
        // Clear the extent_limit setting for that downstairs.
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0u64;
        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(eid as usize);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        up.abort_repair_ds(&mut ds, UpState::Active, &ds_done_tx);
        up.abort_repair_extent(&mut gw, &mut ds, eid);

        assert_eq!(ds.clients[ClientId::new(0)].state, DsState::Active);
        assert_eq!(ds.clients[ClientId::new(1)].state, DsState::Faulted);
        assert_eq!(ds.clients[ClientId::new(0)].state, DsState::Active);

        // Check all three IOs again, downstairs 1 will be skipped..
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        for job in jobs.iter().take(3) {
            assert_eq!(job.state[ClientId::new(0)], IOState::New);
            assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
            assert_eq!(job.state[ClientId::new(2)], IOState::New);
        }
        assert!(ds.clients[ClientId::new(1)].extent_limit.is_none());
    }

    #[tokio::test]
    async fn test_repair_abort_reserved_jobs() {
        // Testing of abort_repair functions.
        // Starting with one downstairs in LiveRepair state and future
        // repair job IDs reserved (but not created yet). The functions
        // will verify that four noop repair jobs will be queued.
        let up = create_test_upstairs(ClientId::new(1)).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0u64;
        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(eid as usize);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        // Reserve some repair IDs
        ds.reserve_repair_ids_for_extent(eid);
        let (reserved_ids, _deps) =
            ds.repair_job_ids.get(&eid).unwrap().clone();

        up.abort_repair_ds(&mut ds, UpState::Active, &ds_done_tx);
        up.abort_repair_extent(&mut gw, &mut ds, eid);

        // Check all three IOs again, downstairs 1 will be skipped..
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 7);
        for job in jobs.iter().take(7) {
            assert_eq!(job.state[ClientId::new(0)], IOState::New);
            assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
            assert_eq!(job.state[ClientId::new(2)], IOState::New);
        }

        // Verify that the four jobs we added match what should have
        // been reserved for those four jobs.
        assert_eq!(jobs[3].ds_id, reserved_ids.close_id);
        assert_eq!(jobs[4].ds_id, reserved_ids.repair_id);
        assert_eq!(jobs[5].ds_id, reserved_ids.noop_id);
        assert_eq!(jobs[6].ds_id, reserved_ids.reopen_id);
    }

    #[tokio::test]
    async fn test_repair_abort_all_failed_reserved_jobs() {
        // Test that when we call the abort_repair functions with all
        // downstairs already in Failed state and future repair job IDs
        // have been reserved (but not created yet). The functions will
        // clear the reserved repair jobs, but not bother to submit them
        // as once a downstairs has failed, it's not doing any more work.
        let up = create_test_upstairs(ClientId::new(1)).await;
        up.ds_transition(ClientId::new(0), DsState::Faulted).await;
        up.ds_transition(ClientId::new(2), DsState::Faulted).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0u64;
        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(0)].extent_limit = Some(eid as usize);
        ds.clients[ClientId::new(1)].extent_limit = Some(eid as usize);
        ds.clients[ClientId::new(2)].extent_limit = Some(eid as usize);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        // Reserve some repair IDs
        ds.reserve_repair_ids_for_extent(eid);

        up.abort_repair_ds(&mut ds, UpState::Active, &ds_done_tx);
        up.abort_repair_extent(&mut gw, &mut ds, eid);

        // Check all three IOs again, all downstairs will be skipped..
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        for job in jobs.iter().take(3) {
            assert_eq!(job.state[ClientId::new(0)], IOState::Skipped);
            assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
            assert_eq!(job.state[ClientId::new(2)], IOState::Skipped);
        }

        // No repair jobs should be submitted
        assert_eq!(jobs.len(), 3);
        assert!(!ds.query_repair_ids(eid));
    }

    async fn test_upstairs_okay() -> Arc<Upstairs> {
        // Build an upstairs without a faulted downstairs.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));

        for cid in ClientId::iter() {
            up.ds_transition(cid, DsState::WaitActive).await;
            let dsr = RegionMetadata {
                generation: vec![1, 1, 1, 1],
                flush_numbers: vec![2, 2, 2, 2],
                dirty: vec![false, false, false, false],
            };
            up.downstairs.lock().await.clients[cid].region_metadata = Some(dsr);
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }

        up.set_active().await.unwrap();
        up
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_done() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered to reflect both the removal of skipped
        // jobs as well as removal of Done jobs that happened before the
        // downstairs went to LiveRepair.

        let up = test_upstairs_okay().await;
        // Channels we want to appear to be working
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);
        // Now, send some IOs.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        // Manually move all these jobs to done.
        for job_id in (1000..1003).map(JobId) {
            let mut handle = ds.ds_active.get_mut(&job_id).unwrap();
            let job = handle.job();
            for cid in ClientId::iter() {
                job.state.insert(cid, IOState::Done);
            }
        }
        drop(ds);

        // Fault the downstairs
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;

        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);
        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        show_all_work(&up).await;
        let mut ds = up.downstairs.lock().await;
        // Good downstairs don't need changes
        assert!(!ds.clients[ClientId::new(0)].dependencies_need_cleanup());
        assert!(!ds.clients[ClientId::new(2)].dependencies_need_cleanup());

        // LiveRepair downstairs might need a change
        assert!(ds.clients[ClientId::new(1)].dependencies_need_cleanup());
        for job_id in (1003..1006).map(JobId) {
            let job = ds.ds_active.get(&job_id).unwrap();
            // jobs 3,4,5 will be skipped for our LiveRepair downstairs.
            assert_eq!(job.state[ClientId::new(0)], IOState::New);
            assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
            assert_eq!(job.state[ClientId::new(2)], IOState::New);
        }

        // Walk the three new jobs, verify that the dependencies will be
        // updated for our downstairs under repair.
        // Make the empty dep list for comparison
        let job = ds.ds_active.get(&JobId(1003)).unwrap();
        let mut current_deps = job.work.deps().clone();

        assert_eq!(current_deps, &[JobId(1002)]);
        // No dependencies are valid for live repair
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1003),
        );
        assert!(current_deps.is_empty());

        let job = ds.ds_active.get(&JobId(1004)).unwrap();
        let mut current_deps = job.work.deps().clone();

        // Job 1001 is not a dep for 1004
        assert_eq!(current_deps, &[JobId(1003)]);
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1004),
        );
        assert!(current_deps.is_empty());

        let job = ds.ds_active.get(&JobId(1005)).unwrap();
        let mut current_deps = job.work.deps().clone();

        assert_eq!(current_deps, &[JobId(1004)]);
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1005),
        );
        assert!(current_deps.is_empty());
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_some() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered to reflect both the removal of skipped
        // jobs as well as removal of Done jobs that happened before the
        // downstairs went to LiveRepair, and also, won't remove jobs that
        // happened after the repair has started and should be allowed
        // through.  This test builds on the previous test, so some things
        // are not checked here.

        let up = test_upstairs_okay().await;

        // Channels we want to appear to be working
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);
        // Now, send some IOs.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        // Manually move all these jobs to done.
        for job_id in (1000..1003).map(JobId) {
            let mut handle = ds.ds_active.get_mut(&job_id).unwrap();
            let job = handle.job();
            for cid in ClientId::iter() {
                job.state.insert(cid, IOState::Done);
            }
        }

        drop(ds);
        // Fault the downstairs
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        ds.clients[ClientId::new(1)].extent_limit = Some(1);
        drop(ds);

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        // Good downstairs don't need changes
        assert!(!ds.clients[ClientId::new(0)].dependencies_need_cleanup());
        assert!(!ds.clients[ClientId::new(2)].dependencies_need_cleanup());

        // LiveRepair downstairs might need a change
        assert!(ds.clients[ClientId::new(1)].dependencies_need_cleanup());

        // For the three latest jobs, they should be New as they are IOs that
        // are on an extent we "already repaired".
        for job_id in (1006..1009).map(JobId) {
            let job = ds.ds_active.get(&job_id).unwrap();
            assert_eq!(job.state[ClientId::new(0)], IOState::New);
            assert_eq!(job.state[ClientId::new(1)], IOState::New);
            assert_eq!(job.state[ClientId::new(2)], IOState::New);
        }

        // Walk the three final jobs, verify that the dependencies will be
        // updated for our downstairs under repair, but will still include
        // the jobs that came after the repair.
        let job = ds.ds_active.get(&JobId(1006)).unwrap();
        let mut current_deps = job.work.deps().clone();

        assert_eq!(current_deps, &[JobId(1005)]);
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1006),
        );
        assert!(current_deps.is_empty());

        let job = ds.ds_active.get(&JobId(1007)).unwrap();
        let mut current_deps = job.work.deps().clone();

        assert_eq!(current_deps, &[JobId(1006)]);
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1007),
        );
        assert_eq!(current_deps, &[JobId(1006)]);

        let job = ds.ds_active.get(&JobId(1008)).unwrap();
        let mut current_deps = job.work.deps().clone();

        assert_eq!(current_deps, &[JobId(1007)]);
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1008),
        );
        assert_eq!(current_deps, &[JobId(1007)]);
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_repair() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered.
        // We want to be sure that a repair job that may have ended up
        // with work IDs below the lowest skipped job does not get skipped
        // as we require that job to be processed. To assist in
        // understanding what is going on, we have a table here to show
        // the IOs and where they are in each extent.

        //       block   block
        // op# | 0 1 2 | 3 4 5 |
        // ----|-------|-------|
        //   0 |   W W | W     |
        //   1 |     W | W W   |
        //   2 |       | W W W |
        //                       Space for future repair
        //   4 | RpRpRp|       |
        //                       Space for future repair
        //   7 |       | W W W |
        //   8 |   W   |       |
        //                       Space for future repair
        //  13 |   W W | W     |

        let up = test_upstairs_okay().await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Now, put three IOs on the queue
        for block in 1..4 {
            up.submit_write(
                Block::new_512(block),
                Bytes::from(vec![0xff; 512 * 3]),
                None,
                false,
                ds_done_tx.clone(),
            )
            .await
            .unwrap();
        }

        let mut ds = up.downstairs.lock().await;
        // Manually move all these jobs to done.
        for job_id in (1000..1003).map(JobId) {
            let mut handle = ds.ds_active.get_mut(&job_id).unwrap();
            let job = handle.job();
            for cid in ClientId::iter() {
                job.state.insert(cid, IOState::Done);
            }
        }

        drop(ds);

        // Fault the downstairs
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);

        // Put a repair job on the queue.
        create_and_enqueue_repair_ops(&up, 0).await;

        // Create a write on extent 1 (not yet repaired)
        up.submit_write(
            Block::new_512(3),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // Now, submit another write, this one will be on the extent
        // that is under repair.
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // Submit a final write.  This has a shadow that covers every
        // IO submitted so far, and will also require creation of
        // space for future repair work.
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // Previous tests have verified what happens before job 1007
        // Starting with job 7, this is special because it will have
        // different dependencies on the Active downstairs vs what the
        // dependencies will be on the LiveRepair downstairs.  On active,
        // it should require jobs 1 and 2. With the LiveRepair downstairs,
        // it will not depend on anything. This is okay, because the job
        // itself is Skipped there, so we won't actually send it.

        let mut ds = up.downstairs.lock().await;
        let job = ds.ds_active.get(&JobId(1007)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::New);
        assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(job.state[ClientId::new(2)], IOState::New);

        let mut current_deps = job.work.deps().clone();
        assert_eq!(current_deps, &[JobId(1002)]);

        // Verify that the Skipped job on the LiveRepair downstairs do not
        // have any dependencies, as, technically, this IO is the first IO to
        // happen after we started repair, and we should not look for any
        // dependencies before starting repair.
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1007),
        );
        assert!(current_deps.is_empty());

        // This second write after starting a repair should require job 6 (i.e.
        // the final job of the repair) on both the Active and LiveRepair
        // downstairs, since it masks job 0
        let job = ds.ds_active.get(&JobId(1008)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::New);
        assert_eq!(job.state[ClientId::new(1)], IOState::New);
        assert_eq!(job.state[ClientId::new(2)], IOState::New);

        let mut current_deps = job.work.deps().clone();
        assert_eq!(current_deps, &[JobId(1006)]);

        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1008),
        );
        // LiveRepair downstairs won't see past the repair.
        assert_eq!(current_deps, &[JobId(1006)]);

        // This final job depends on everything on Active downstairs, but
        // a smaller subset for the LiveRepair downstairs
        let job = ds.ds_active.get(&JobId(1013)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::New);
        assert_eq!(job.state[ClientId::new(1)], IOState::New);
        assert_eq!(job.state[ClientId::new(2)], IOState::New);

        // The last write depends on
        // 1) the final operation on the repair of extent 0
        // 2) the write operation (8) on extent 0
        // 3) a new repair operation on extent 1
        let mut current_deps = job.work.deps().clone();
        assert_eq!(current_deps, &[JobId(1006), JobId(1008), JobId(1012)]);

        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1013),
        );
        assert_eq!(current_deps, &[JobId(1006), JobId(1008), JobId(1012)]);
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_sk_repair() {
        // Verify that a downstairs in LiveRepair state will have its
        // dependency list altered.
        // Simulating what happens when we start repair with just the close
        // and reopen, but not the repair and noop jobs.
        // Be sure that the dependency removal for 4 won't remove 2,3, but
        // will remove 0.
        // A write after the repair should have the issued repairs, but not
        // the repairs that are not yet present.
        //       block   block
        // op# | 0 1 2 | 3 4 5 |
        // ----|-------|-------|
        //   0 |   W   |       |
        //                       Live Repair starts here
        //   1 | Rclose|       |
        //   2 |       |       | Reserved for future repair
        //   3 |       |       | Reserved for future repair
        //   4 | Reopen|       |
        //   5 |   W   |       |

        let up = test_upstairs_okay().await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Put the first write on the queue
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let mut ds = up.downstairs.lock().await;
        // Manually move this jobs to done.
        let mut handle = ds.ds_active.get_mut(&JobId(1000)).unwrap();
        let job = handle.job();
        for cid in ClientId::iter() {
            job.state.insert(cid, IOState::Done);
        }
        drop(handle);
        drop(ds);

        let eid = 0;

        // Fault the downstairs
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;

        // Simulate what happens when we first start repair
        // on extent 0
        let next_flush = up.next_flush_id();
        let gen = up.get_generation();

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        ds.clients[ClientId::new(1)].extent_limit = Some(eid as usize);

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = gw.next_gw_id();
        let gw_reopen_id: u64 = gw.next_gw_id();

        // The work IDs for the downstairs side of things.
        let (extent_repair_ids, mut deps) = ds.get_repair_ids(eid);
        let close_id = extent_repair_ids.close_id;
        let repair_id = extent_repair_ids.repair_id;
        let noop_id = extent_repair_ids.noop_id;
        let reopen_id = extent_repair_ids.reopen_id;

        // The initial close IO has the base set of dependencies.
        // Each additional job will depend on the previous.
        let close_deps = deps.clone();
        deps.push(close_id);
        deps.push(repair_id);
        deps.push(noop_id);
        let reopen_deps = deps.clone();

        let _reopen_brw = create_and_enqueue_reopen_io(
            &mut ds,
            &mut gw,
            eid,
            reopen_deps,
            reopen_id,
            gw_reopen_id,
        );

        // Next we create and insert the close job on the work queue.

        let _close_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            next_flush,
            gen,
            close_deps,
            close_id,
            gw_close_id,
            ClientId::new(0),
            &[ClientId::new(1)],
        );

        drop(ds);
        drop(gw);
        // Submit a write.
        up.submit_write(
            Block::new_512(1),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let mut ds = up.downstairs.lock().await;
        let job = ds.ds_active.get(&JobId(1004)).unwrap();

        let mut current_deps = job.work.deps().clone();
        // We start with repair jobs, plus the original jobs.
        assert_eq!(
            &current_deps,
            &[JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );

        // The downstairs in LiveRepair should not see the first write, but
        // should see all the repair IDs, including ones that don't actually
        // exist yet.
        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1004),
        );
        assert_eq!(current_deps, &[JobId(1001), JobId(1002), JobId(1003)]);

        // This second write after starting a repair should only require the
        // repair job on all downstairs
        let job = ds.ds_active.get(&JobId(1005)).unwrap();
        assert_eq!(job.state[ClientId::new(0)], IOState::New);
        assert_eq!(job.state[ClientId::new(1)], IOState::New);
        assert_eq!(job.state[ClientId::new(2)], IOState::New);

        let mut current_deps = job.work.deps().clone();
        assert_eq!(&current_deps, &[JobId(1004)]);

        ds.remove_dep_if_live_repair(
            ClientId::new(1),
            &mut current_deps,
            JobId(1005),
        );
        // LiveRepair downstairs won't see past the repair.
        assert_eq!(current_deps, &[JobId(1004)]);
    }
    //       block   block
    // op# | 0 1 2 | 3 4 5 |
    // ----|-------|-------|
    //   0 |   W   |       |
    //   1 | Rclose|       |
    //   2 |       |       | Reserved for future repair
    //   3 |       |       | Reserved for future repair
    //   4 | Reopn |       |
    //   8 |       | W W W | Skipped on DS 1?

    #[tokio::test]
    async fn test_live_repair_span_write_write() {
        // We have a downstairs that is in LiveRepair, and we have indicated
        // that this extent is under repair.  The write (that spans extents
        // should have created IDs for future repair work and then made itself
        // dependent on those repairs finishing. The write that comes next
        // lands on the extent that has future repairs reserved, and that
        // 2nd write needs to also depend on those repair jobs.
        //
        // We start with extent 0 under repair.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //
        // First, a write spanning extents 0 and 1
        //     | block | block |
        //     | 0 1 2 | 3 4 5 |
        // ----|-------|-------|-----
        //     |     W | W W   |
        //
        // Then, a write on the "spanned to" extent 1, but not overlapping
        // with the first write.
        //     | block | block |
        //     | 0 1 2 | 3 4 5 |
        // ----|-------|-------|-----
        //     |       |     W |
        //
        // The first write will create and depend on jobs reserved for a
        // future repair.  The second job should discover the repair jobs and
        // also have them as dependencies.
        //     | Under |       |
        //     | Repair|       |
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   3 |       | RpRpRp|
        //   4 |     W | W W   | 3
        //   6 |       |     W | 3
        //

        let up = create_test_upstairs(ClientId::new(1)).await;
        // Make downstairs 1 in LiveRepair
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;

        // Make extent 0 under repair
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        // A write of blocks 2,3,4 which spans extents 0-1.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 3]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // A write of block 5 which is on extent 1, but does not
        // overlap with the previous write
        up.submit_write(
            Block::new_512(5),
            Bytes::from(vec![0xff; 512]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 2);

        // The first job, should have the dependences for the new repair work
        assert_eq!(jobs[0].ds_id, JobId(1004));
        assert_eq!(jobs[0].work.deps(), &[JobId(1003)]);
        assert_eq!(jobs[0].state[ClientId::new(0)], IOState::New);
        assert_eq!(jobs[0].state[ClientId::new(1)], IOState::New);
        assert_eq!(jobs[0].state[ClientId::new(2)], IOState::New);

        // The 2nd job should aldo have the dependences for the new repair work
        assert_eq!(jobs[1].work.deps(), &[JobId(1003)]);
        assert_eq!(jobs[1].state[ClientId::new(0)], IOState::New);
        assert_eq!(jobs[1].state[ClientId::new(1)], IOState::New);
        assert_eq!(jobs[1].state[ClientId::new(2)], IOState::New);
    }

    #[tokio::test]
    async fn test_spicy_live_repair() {
        // We have a downstairs that is in LiveRepair, and send a write which is
        // above the extent above repair.  This is fine; it's skipped on the
        // being-repaired downstairs.
        //
        // Then, send a write which reserves repair job ids for extent 1 (blocks
        // 3-5).  This is the same as the test above.
        //
        // Finally, send a read which spans extents 1-2 (blocks 5-6).  This must
        // depend on the previously-inserted repair jobs.  However, that's not
        // enough: because the initial write was skipped in the under-repair
        // downstairs, the read won't depend on that write on that downstairs!
        // Instead, we have to add *new* repair dependencies for extent 2;
        // without these dependencies, it would be possible to read old data
        // from block 6 on the under-repair downstairs.
        //
        //     | Under |       |       |
        //     | Repair|       |       |
        //     | block | block | block |
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-------
        //   0 |       | W W W | W     | none; skipped in under-repair ds
        //   1 |       | RpRpRp|       | 0
        //   2 |       | RpRpRp|       |
        //   3 |       | RpRpRp|       |
        //   4 |       | RpRpRp|       |
        //   5 |     W | W     |       | 4
        //   6 |       |       | RpRpRp|
        //   7 |       |       | RpRpRp|
        //   8 |       |       | RpRpRp|
        //   9 |       |       | RpRpRp|
        //   10|       |     R | R     | 4,9
        //
        // More broadly: if a job is _not_ going to be skipped (e.g. the final
        // read job in this example), then it needs reserved repair job IDs for
        // every extent that it touches.

        let up = create_test_upstairs(ClientId::new(1)).await;
        // Make downstairs 1 in LiveRepair
        up.ds_transition(ClientId::new(1), DsState::Faulted).await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepairReady)
            .await;
        up.ds_transition(ClientId::new(1), DsState::LiveRepair)
            .await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;

        // Make extent 0 under repair
        ds.clients[ClientId::new(1)].extent_limit = Some(0);
        drop(ds);

        // A write of blocks 3,4,5,6 which spans extents 1-2.
        up.submit_write(
            Block::new_512(3),
            Bytes::from(vec![0xff; 512 * 4]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // A write of block 2-3, which overlaps the previous write and should
        // also trigger a repair.
        up.submit_write(
            Block::new_512(2),
            Bytes::from(vec![0xff; 512 * 2]),
            None,
            false,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        // A read of block 5-7, which overlaps the previous repair and should
        // also force waiting on a new repair.
        up.submit_read(
            Block::new_512(5),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;
        let jobs: Vec<&DownstairsIO> = ds.ds_active.values().collect();

        assert_eq!(jobs.len(), 3);

        // The first job should have no dependencies
        assert_eq!(jobs[0].ds_id, JobId(1000));
        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[0].state[ClientId::new(0)], IOState::New);
        assert_eq!(jobs[0].state[ClientId::new(1)], IOState::Skipped);
        assert_eq!(jobs[0].state[ClientId::new(2)], IOState::New);

        assert_eq!(jobs[1].ds_id, JobId(1005));
        assert_eq!(jobs[1].work.deps(), &[JobId(1004)]);
        assert_eq!(jobs[1].state[ClientId::new(0)], IOState::New);
        assert_eq!(jobs[1].state[ClientId::new(1)], IOState::New);
        assert_eq!(jobs[1].state[ClientId::new(2)], IOState::New);

        assert_eq!(jobs[2].ds_id, JobId(1010));
        assert_eq!(jobs[2].work.deps(), &[JobId(1004), JobId(1009)]);
        assert_eq!(jobs[2].state[ClientId::new(0)], IOState::New);
        assert_eq!(jobs[2].state[ClientId::new(1)], IOState::New);
        assert_eq!(jobs[2].state[ClientId::new(2)], IOState::New);
    }
}
