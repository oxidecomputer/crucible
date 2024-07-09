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
    use crate::{
        client::ClientAction,
        downstairs::DownstairsAction,
        upstairs::{
            test::{create_test_upstairs, start_up_and_repair},
            Upstairs, UpstairsAction,
        },
    };

    /// Test function to send fake replies for the given job, completing it
    ///
    /// Fake replies are applied through `Upstairs::apply`, so the reply may
    /// cause multiple things to happen:
    ///
    /// - Ackable jobs will be acked
    /// - Live-repair will continue processing, e.g. moving on to the next
    ///   extent automatically
    fn reply_to_repair_job(
        up: &mut Upstairs,
        job_id: JobId,
        client_id: ClientId,
        result: Result<(), CrucibleError>,
        ei: Option<ExtentInfo>,
    ) {
        let Some(job) = up.downstairs.get_job(&job_id) else {
            panic!("no such job");
        };
        let session_id = up.cfg.session_id;
        let upstairs_id = up.cfg.upstairs_id;
        let m = match &job.work {
            IOop::ExtentFlushClose { .. } => {
                let ei = ei.unwrap();
                Message::ExtentLiveCloseAck {
                    job_id,
                    session_id,
                    upstairs_id,
                    result: result
                        .map(|_| (ei.generation, ei.flush_number, ei.dirty)),
                }
            }
            IOop::ExtentLiveNoOp { .. } | IOop::ExtentLiveReopen { .. } => {
                Message::ExtentLiveAckId {
                    job_id,
                    session_id,
                    upstairs_id,
                    result,
                }
            }
            IOop::ExtentLiveRepair {
                repair_downstairs, ..
            } => {
                if repair_downstairs.contains(&client_id) {
                    Message::ExtentLiveRepairAckId {
                        job_id,
                        session_id,
                        upstairs_id,
                        result,
                    }
                } else {
                    Message::ExtentLiveAckId {
                        job_id,
                        session_id,
                        upstairs_id,
                        result,
                    }
                }
            }
            m => panic!("don't know how to complete {m:?}"),
        };
        up.apply(UpstairsAction::Downstairs(DownstairsAction::Client {
            client_id,
            action: ClientAction::Response(m),
        }))
    }

    // Test the permutations of calling repair_extent with each downstairs
    // as the one needing LiveRepair
    #[test]
    fn test_repair_extent_no_action_all() {
        for or_ds in ClientId::iter() {
            test_repair_extent_no_action(or_ds);
        }
    }

    fn test_repair_extent_no_action(or_ds: ClientId) {
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

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_next_close_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        }

        info!(up.log, "done replying to close job");
        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        let jobs = up.downstairs.active_count();
        assert_eq!(jobs, 3);

        // The repair job has shown up.  Move it forward.
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), None);
        }

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.  Loop here waiting for that job to arrive.
        let jobs = up.downstairs.active_count();
        assert_eq!(jobs, 4);

        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
        }

        info!(up.log, "Finally, move the ReOpen job forward");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
        }

        // The extent repair task should complete without error.
        assert_eq!(
            up.downstairs.repair().as_ref().unwrap().active_extent,
            ExtentId(1)
        );

        // We should have 6 jobs on the queue; 4 from the first extent, and 2
        // more for the next extent (which was started when the job was acked)
        assert_eq!(up.downstairs.active_count(), 6);

        info!(up.log, "jobs are: {:?}", jobs);
        let job = up.downstairs.get_job(&ds_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = up.downstairs.get_job(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        // Check that the subsequent job has started
        let job = up.downstairs.get_job(&ds_next_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().active, 3);
    }

    // Loop over the possible downstairs to be in LiveRepair and
    // run through the do_repair code path.
    #[test]
    fn test_repair_extent_do_repair_all() {
        for or_ds in ClientId::iter() {
            test_repair_extent_do_repair(or_ds);
        }
    }

    fn test_repair_extent_do_repair(or_ds: ClientId) {
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

        let mut up = start_up_and_repair(or_ds);
        info!(up.log, "started up");

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_next_close_id = JobId(1004);

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

        info!(up.log, "reply to close job");
        for cid in ClientId::iter() {
            reply_to_repair_job(
                &mut up,
                ds_close_id,
                cid,
                Ok(()),
                Some(if cid == or_ds { bad_ei } else { ei }),
            );
        }

        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        let jobs = up.downstairs.active_count();
        assert_eq!(jobs, 3);

        // The repair job has shown up.  Move it forward.
        // Because the "or_ds" in this case is not returning an
        // error, we don't pass it to move_and_complete_job
        info!(up.log, "Now reply to the repair job");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), None);
        }

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.
        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
        }

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
        }

        // The extent repair task should complete without error.
        assert_eq!(
            up.downstairs.repair().as_ref().unwrap().active_extent,
            ExtentId(1)
        );

        // We should have 6 jobs on the queue.
        // Four from the first extent we just repaired, and two more for the
        // next extent which starts when the prior extent repair finishes.
        assert_eq!(up.downstairs.active_count(), 6);

        let job = up.downstairs.get_job(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveRepair { .. } => {}
            x => {
                panic!("Expected LiveRepair, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 3);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::LiveRepair);

        // Check that the subsequent job has started
        let job = up.downstairs.get_job(&ds_next_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().active, 3);
    }

    #[test]
    fn test_repair_extent_close_fails_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentFlushClose operation.
        for failed_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_close_fails(failed_ds, err_ds);
            }
        }
    }

    fn test_repair_extent_close_fails(or_ds: ClientId, err_ds: ClientId) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the initial close command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in LiveRepair, and the
        // downstairs that will return error for the ExtentClose operation.
        // They may be the same downstairs.

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_flush_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };

        info!(up.log, "move the close jobs forward");
        // Move the close job forward, but report error on the err_ds
        for cid in ClientId::iter() {
            let r = if cid == err_ds {
                Err(CrucibleError::GenericError("bad".to_string()))
            } else {
                Ok(())
            };
            reply_to_repair_job(
                &mut up,
                ds_close_id,
                cid,
                r,
                Some(ei), // Err takes precedence
            );
        }

        // process_ds_completion should force the downstairs to fail
        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);

        info!(up.log, "repair job should have got here, move it forward");
        // The repair (NoOp) job should have shown up.  Move it forward.
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                info!(up.log, "replying to repair job on {cid}");
                reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), None);
            }
        }

        // When we completed the repair jobs, the main task should
        // have gone ahead and issued the NoOp that should be issued
        // next.
        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                info!(up.log, "replying to NoOp job on {cid}");
                reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
            }
        }

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                info!(up.log, "replying to ReOpen job on {cid}");
                reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
            }
        }

        // We should have the four repair jobs on the queue, along with
        // a final flush, as we should have ended this repair as failed and
        // sent that final flush.
        assert_eq!(up.downstairs.active_count(), 5);

        // Verify that the four jobs are the four we expect.
        let job = up.downstairs.get_job(&ds_close_id).unwrap();
        match &job.work {
            IOop::ExtentFlushClose { .. } => {}
            x => {
                panic!("Expected ExtentFlushClose, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        // Because the close failed, we sent a NoOp instead of repair.
        let job = up.downstairs.get_job(&ds_repair_id).unwrap();
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

        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
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

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
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

        // Because the repair has failed, the extent that was under
        // repair should also now be faulted.
        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        let job = up.downstairs.get_job(&ds_flush_id).unwrap();
        match &job.work {
            IOop::Flush { .. } => {}
            x => {
                panic!("Expected Flush, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().active, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().active, 2);
            assert_eq!(job.state_count().skipped, 1);
        }
    }

    #[test]
    fn test_repair_extent_repair_fails_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentFlushClose operation.
        for failed_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_repair_fails(failed_ds, err_ds);
            }
        }
    }

    fn test_repair_extent_repair_fails(or_ds: ClientId, err_ds: ClientId) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the repair command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in LiveRepair, and the
        // downstairs that will return error for the ExtentLiveRepair
        // operation.  They may be the same downstairs.

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_flush_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        info!(up.log, "reply to the close job");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        }

        // Once we process the IO completion the task
        // doing extent repair should submit the next IO.
        // Move the repair job forward, but report error on the err_ds
        for cid in ClientId::iter() {
            let r = if cid == err_ds {
                Err(CrucibleError::GenericError("bad".to_string()))
            } else {
                Ok(())
            };
            reply_to_repair_job(&mut up, ds_repair_id, cid, r, None);
        }

        // process_ds_completion should force both the downstairs that
        // reported the error, and the downstairs that is under repair to
        // fail.
        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued next.
        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
            }
        }

        // The reopen job should already be on the queue, move it forward.
        info!(up.log, "Finally, move the ReOpen job forward");
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
            }
        }

        // We should have four repair jobs on the queue along with the
        // final flush.  We only have a final flush here because we have
        // aborted the repair.
        assert_eq!(up.downstairs.active_count(), 5);

        let job = up.downstairs.get_job(&ds_repair_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }

        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
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

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
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

        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        let job = up.downstairs.get_job(&ds_flush_id).unwrap();
        match &job.work {
            IOop::Flush { .. } => {}
            x => {
                panic!("Expected Flush, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().active, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().active, 2);
            assert_eq!(job.state_count().skipped, 1);
        }
    }

    #[test]
    fn test_repair_extent_fail_noop_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveNoOp operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_fail_noop(or_ds, err_ds);
            }
        }
    }

    fn test_repair_extent_fail_noop(or_ds: ClientId, err_ds: ClientId) {
        // Test repair_extent when the noop job fails.
        // We take input for both which downstairs is in LiveRepair, and
        // which downstairs will return error on the NoOp operation.

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_flush_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        }

        // Once we process the IO completion, the task doing extent
        // repair should submit the next IO.  Move that job forward.
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), Some(ei));
        }

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.
        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            let r = if cid == err_ds {
                Err(CrucibleError::GenericError("bad".to_string()))
            } else {
                Ok(())
            };
            reply_to_repair_job(&mut up, ds_noop_id, cid, r, None);
        }

        info!(up.log, "Now ACK the reopen job");
        for cid in ClientId::iter() {
            if cid != err_ds && cid != or_ds {
                reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
            }
        }

        // We should have five jobs on the queue.
        assert_eq!(up.downstairs.active_count(), 5);

        // Differences from the usual path start here.
        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
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

        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        let job = up.downstairs.get_job(&ds_flush_id).unwrap();
        match &job.work {
            IOop::Flush { .. } => {}
            x => {
                panic!("Expected Flush, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().active, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().active, 2);
            assert_eq!(job.state_count().skipped, 1);
        }
    }

    #[test]
    fn test_repair_extent_fail_noop_out_of_order_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveNoOp operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_fail_noop_out_of_order(or_ds, err_ds);
            }
        }
    }

    fn test_repair_extent_fail_noop_out_of_order(
        or_ds: ClientId,
        err_ds: ClientId,
    ) {
        // Test repair_extent when the noop job fails, but the other
        // (non-failing) Downstairs have already replied to the final Reopen job

        // We take input for both which downstairs is in LiveRepair, and
        // which downstairs will return error on the NoOp operation.

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_flush_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        }

        // Once we process the IO completion, the task doing extent
        // repair should submit the next IO.  Move that job forward.
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), Some(ei));
        }

        // When we completed the repair jobs, the repair_extent should
        // have gone ahead and issued the NoOp that should be issued
        // next.
        info!(up.log, "Now move the NoOp job forward for working DS");
        for cid in ClientId::iter() {
            if cid != err_ds {
                reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
            }
        }

        info!(up.log, "Now ACK the reopen job");
        for cid in ClientId::iter() {
            if cid != err_ds {
                reply_to_repair_job(&mut up, ds_reopen_id, cid, Ok(()), None);
            }
        }

        info!(up.log, "Now fail the NoOp job for broken DS");
        reply_to_repair_job(
            &mut up,
            ds_noop_id,
            err_ds,
            Err(CrucibleError::GenericError("bad".to_string())),
            None,
        );

        // We should have five jobs on the queue.
        assert_eq!(up.downstairs.active_count(), 5);

        // Differences from the usual path start here.
        let job = up.downstairs.get_job(&ds_noop_id).unwrap();
        match &job.work {
            IOop::ExtentLiveNoOp { .. } => {}
            x => {
                panic!("Expected LiveNoOp, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        // The Reopen jobs were completed before the error was reported, so we
        // expect 2/3 of them to be done (and the error case to be skipped)
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().skipped, 1);

        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        let job = up.downstairs.get_job(&ds_flush_id).unwrap();
        match &job.work {
            IOop::Flush { .. } => {}
            x => {
                panic!("Expected Flush, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().active, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().active, 2);
            assert_eq!(job.state_count().skipped, 1);
        }
    }

    #[test]
    fn test_repair_extent_fail_reopen_all() {
        // Test all the permutations of
        // A downstairs that is in LiveRepair
        // A downstairs that returns error on the ExtentLiveReopen operation.
        for or_ds in ClientId::iter() {
            for err_ds in ClientId::iter() {
                test_repair_extent_fail_reopen(or_ds, err_ds);
            }
        }
    }

    fn test_repair_extent_fail_reopen(or_ds: ClientId, err_ds: ClientId) {
        // Test repair_extent when the reopen job fails.
        // We take input for both which downstairs is in LiveRepair, and
        // which downstairs will return error on the NoOp operation.

        let mut up = start_up_and_repair(or_ds);

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(1000);
        let ds_repair_id = JobId(1001);
        let ds_noop_id = JobId(1002);
        let ds_reopen_id = JobId(1003);
        let ds_flush_id = JobId(1004);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        }

        // Once we process the IO completion, and drop the lock, the task
        // doing extent repair should submit the next IO.

        // The repair job has shown up.  Move it forward.
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_repair_id, cid, Ok(()), None);
        }

        // When we completed the repair jobs, the main task should
        // have gone ahead and issued the NoOp that should be issued next.

        info!(up.log, "Now move the NoOp job forward");
        for cid in ClientId::iter() {
            reply_to_repair_job(&mut up, ds_noop_id, cid, Ok(()), None);
        }

        // Move the reopen job forward
        for cid in ClientId::iter() {
            let r = if cid == err_ds {
                Err(CrucibleError::GenericError("bad".to_string()))
            } else {
                Ok(())
            };
            reply_to_repair_job(&mut up, ds_reopen_id, cid, r, None);
        }

        // We should have four repair jobs on the queue along with the
        // final flush.  We only have a final flush here because we have
        // aborted the repair.
        assert_eq!(up.downstairs.active_count(), 5);

        // All that is different from the normal path is the results from
        // the reopen job, so that is all we need to check here.
        let job = up.downstairs.get_job(&ds_reopen_id).unwrap();
        match &job.work {
            IOop::ExtentLiveReopen { .. } => {}
            x => {
                panic!("Expected ExtentLiveReopen, got: {:?}", x);
            }
        }
        assert_eq!(job.state_count().done, 2);
        assert_eq!(job.state_count().error, 1);

        assert_eq!(up.downstairs.clients[err_ds].state(), DsState::Faulted);
        assert_eq!(up.downstairs.clients[or_ds].state(), DsState::Faulted);

        let job = up.downstairs.get_job(&ds_flush_id).unwrap();
        match &job.work {
            IOop::Flush { .. } => {}
            x => {
                panic!("Expected Flush, got: {:?}", x);
            }
        }
        if err_ds != or_ds {
            assert_eq!(job.state_count().active, 1);
            assert_eq!(job.state_count().skipped, 2);
        } else {
            assert_eq!(job.state_count().active, 2);
            assert_eq!(job.state_count().skipped, 1);
        }
    }

    #[test]
    fn test_reserve_extent_repair_ids() {
        // Verify that we can reserve extent IDs for repair work, and they
        // are allocated as expected.
        let mut up = create_test_upstairs();

        // Before repair has started, there should be nothing in repair
        // or last_repair_extent.
        assert_eq!(up.downstairs.last_repair_extent(), None);
        assert!(up.downstairs.repair().is_none());

        // Start the LiveRepair
        let client = &mut up.downstairs.clients[ClientId::new(1)];
        client.checked_state_transition(&up.state, DsState::Faulted);
        client.checked_state_transition(&up.state, DsState::LiveRepairReady);
        up.on_repair_check();
        assert!(up.downstairs.live_repair_in_progress());
        assert_eq!(up.downstairs.last_repair_extent(), Some(ExtentId(0)));

        // We should have reserved ids 1000 -> 1003
        assert_eq!(up.downstairs.peek_next_id(), JobId(1004));

        // Now, reserve IDs for extent 1
        up.downstairs.reserve_repair_ids_for_extent(ExtentId(1));
        // The reservation should have taken 1004 -> 1007
        assert_eq!(up.downstairs.peek_next_id(), JobId(1008));
    }

    // Test function to complete a LiveRepair.
    // This assumes a LiveRepair has been started and the first two repair
    // jobs have been issued.  We will use the starting job ID default of 1000.
    fn finish_live_repair(up: &mut Upstairs, ds_start: u64) {
        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = JobId(ds_start);
        let ds_repair_id = JobId(ds_start + 1);
        let ds_noop_id = JobId(ds_start + 2);
        let ds_reopen_id = JobId(ds_start + 3);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in ClientId::iter() {
            reply_to_repair_job(up, ds_close_id, cid, Ok(()), Some(ei));
        }

        for job in [ds_repair_id, ds_noop_id, ds_reopen_id] {
            for cid in ClientId::iter() {
                reply_to_repair_job(up, job, cid, Ok(()), None);
            }
        }
    }

    #[test]
    fn test_repair_io_below_repair_extent() {
        // Verify that io put on the queue when a downstairs is in LiveRepair
        // and the IO is below the extent_limit will be sent to all downstairs
        // for processing and not skipped.
        let mut up = start_up_and_repair(ClientId::new(1));

        // Do something to finish extent 0.  This reserves IDs 1004-1007 for the
        // next extent's repairs.
        finish_live_repair(&mut up, 1000);

        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512].as_slice()),
            false,
        );

        up.submit_dummy_read(Block::new_512(0), Buffer::new(1, 512));

        // WriteUnwritten
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512].as_slice()),
            true,
        );

        // All clients should send the jobs (no skipped)
        for ids in [JobId(1008), JobId(1009), JobId(1010)] {
            let job = up.downstairs.ds_active.get(&ids).unwrap();
            for cid in ClientId::iter() {
                assert_eq!(
                    job.state[cid],
                    IOState::New,
                    "bad state for {ids:?} {cid}"
                );
            }
        }
    }

    #[test]
    fn test_repair_io_at_repair_extent() {
        // Verify that io put on the queue when a downstairs is in LiveRepair
        // and the IO is for the extent under repair will be sent to all
        // downstairs for processing and not skipped.
        let mut up = start_up_and_repair(ClientId::new(1));

        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512].as_slice()),
            false,
        );

        up.submit_dummy_read(Block::new_512(0), Buffer::new(1, 512));

        // WriteUnwritten
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512].as_slice()),
            true,
        );

        // All clients should send the jobs (no skipped)
        for ids in [JobId(1004), JobId(1005), JobId(1006)] {
            let job = up.downstairs.ds_active.get(&ids).unwrap();
            for cid in ClientId::iter() {
                assert_eq!(job.state[cid], IOState::New);
            }
        }
    }

    #[test]
    fn test_repair_io_above_repair_extent() {
        // Verify that an IO put on the queue when a downstairs is in
        // LiveRepair and the IO is above the extent_limit will be skipped by
        // the downstairs that is in LiveRepair.
        let mut up = start_up_and_repair(ClientId::new(1));

        up.submit_dummy_write(
            Block::new_512(3),
            BytesMut::from(vec![0xff; 512].as_slice()),
            false,
        );

        up.submit_dummy_read(Block::new_512(3), Buffer::new(1, 512));

        // WriteUnwritten
        up.submit_dummy_write(
            Block::new_512(3),
            BytesMut::from(vec![0xff; 512].as_slice()),
            true,
        );

        // Client 0 and 2 will send the jobs
        for ids in [JobId(1004), JobId(1005), JobId(1006)] {
            let job = up.downstairs.ds_active.get(&ids).unwrap();
            assert_eq!(job.state[ClientId::new(0)], IOState::New);
            assert_eq!(job.state[ClientId::new(2)], IOState::New);
        }

        // Client 1 will skip the jobs
        for ids in [JobId(1004), JobId(1005), JobId(1006)] {
            let job = up.downstairs.ds_active.get(&ids).unwrap();
            assert_eq!(job.state[ClientId::new(1)], IOState::Skipped);
        }
    }
    #[test]
    fn test_repair_io_span_el_sent() {
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
        let mut up = start_up_and_repair(ClientId::new(1));

        // Extent 0 repair has jobs 1000 -> 1003.
        // This will finish the repair on extent 0 and start the repair
        // on extent 1.
        // Extent 1 repair will have jobs 1004 -> 1007.
        finish_live_repair(&mut up, 1000);

        // Our default extent size is 3, so 9 blocks will span 3 extents
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512 * 9].as_slice()),
            false,
        );

        up.submit_dummy_read(Block::new_512(0), Buffer::new(9, 512));

        // WriteUnwritten
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512 * 9].as_slice()),
            true,
        );

        // All clients should send the jobs (no skipped)
        // The future repair we had to reserve for extent 2 will have
        // taken jobs 1008 -> 1011, so our new IOs will start at 1012
        for ids in [JobId(1012), JobId(1013), JobId(1014)] {
            let job = up.downstairs.ds_active.get(&ids).unwrap();
            for cid in ClientId::iter() {
                assert_eq!(job.state[cid], IOState::New);
            }
        }
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
        let mut up = start_up_and_repair(ClientId::new(1));

        // Extent 0 repair has jobs 1000 -> 1003.
        // This will finish the repair on extent 0 and start the repair
        // on extent 1.
        // Extent 1 repair will have jobs 1004 -> 1007.
        finish_live_repair(&mut up, 1000);

        // Our default extent size is 3, so 9 blocks will span 3 extents
        up.submit_dummy_read(Block::new_512(0), Buffer::new(9, 512));

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        up.show_all_work();

        // All clients should send the jobs (no skipped)
        // The future repair we had to reserve for extent 2 will have
        // taken jobs 1008 -> 1011, so our new IO will start at 1012
        let job = up.downstairs.ds_active.get(&JobId(1012)).unwrap();
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }

        // Verify that the future final repair job were added to our IOs
        // dependency list.  We will also see the final repair jobs
        assert_eq!(job.work.deps(), &[JobId(1003), JobId(1007), JobId(1011)]);
    }

    #[test]
    fn test_repair_write_span_el_sent() {
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

        let mut up = start_up_and_repair(ClientId::new(1));

        // Extent 0 repair has jobs 1000 -> 1003.
        // This will finish the repair on extent 0 and start the repair
        // on extent 1.
        // Extent 1 repair will have jobs 1004 -> 1007.
        finish_live_repair(&mut up, 1000);

        // Our default extent size is 3, so 9 blocks will span 3 extents
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512 * 9].as_slice()),
            false,
        );

        // All clients should send the jobs (no skipped)
        // The future repair we had to reserve for extent 2 will have
        // taken jobs 1008 -> 1011, so our new IO will start at 1012
        let job = up.downstairs.ds_active.get(&JobId(1012)).unwrap();
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }

        // Verify that the future final repair job were added to our IOs
        // dependency list.  We will also see the final repair jobs
        assert_eq!(job.work.deps(), &[JobId(1003), JobId(1007), JobId(1011)]);
    }

    #[test]
    fn test_repair_write_span_two_el_sent() {
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

        let mut up = start_up_and_repair(ClientId::new(1));

        // Our default extent size is 3, so block 3 will be on extent 1
        up.submit_dummy_write(
            Block::new_512(0),
            BytesMut::from(vec![0xff; 512 * 9].as_slice()),
            false,
        );

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        // All clients should send the jobs (no skipped)
        // The future repairs we had to reserve for extents 1,2 will have
        // taken jobs 1004 -> 1011, so our new IO will start at 1012
        let job = up.downstairs.ds_active.get(&JobId(1012)).unwrap();
        for cid in ClientId::iter() {
            assert_eq!(job.state[cid], IOState::New);
        }

        // Verify that the future final repair job were added to our IOs
        // dependency list.  We will also see the final repair jobs
        assert_eq!(job.work.deps(), &[JobId(1003), JobId(1007), JobId(1011)]);
    }

    #[test]
    fn test_live_repair_update() {
        // Make sure that process_ds_completion() will take extent info
        // result and put it on the live repair repair_info.
        // As we don't have an actual downstairs here, we "fake it" by
        // feeding the responses we expect back from the downstairs.

        let mut up = start_up_and_repair(ClientId::new(1));

        let ds_close_id = JobId(1000);

        // Client 0
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        let cid = ClientId::new(0);
        reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));
        let new_ei = up.downstairs.clients[cid].repair_info.unwrap();
        // Verify the extent information has been added to the repair info
        assert_eq!(new_ei.generation, 5);
        assert_eq!(new_ei.flush_number, 3);
        assert!(!new_ei.dirty);

        // Client 1
        let ei = ExtentInfo {
            generation: 2,
            flush_number: 4,
            dirty: true,
        };
        let cid = ClientId::new(1);
        reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));

        // Verify the extent information has been added to the repair info
        // for client 1
        let new_ei = up.downstairs.clients[cid].repair_info.unwrap();
        assert_eq!(new_ei.generation, 2);
        assert_eq!(new_ei.flush_number, 4);
        assert!(new_ei.dirty);

        // Client 2
        let ei = ExtentInfo {
            generation: 29,
            flush_number: 444,
            dirty: false,
        };

        let cid = ClientId::new(2);
        reply_to_repair_job(&mut up, ds_close_id, cid, Ok(()), Some(ei));

        // The extent info is added to the repair info for client 2, but then we
        // proceed with the live-repair and the `repair_info` field is taken
        // (since it's used to decide whether to send a LiveRepair or NoOp).
        assert!(up.downstairs.clients[cid].repair_info.is_none());
    }
}
