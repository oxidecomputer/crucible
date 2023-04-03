// Copyright 2023 Oxide Computer Company

use super::*;
use tokio::sync::mpsc;

// When determining if an extent needs repair, we collect its current
// information into this struct.
#[derive(Debug, Clone)]
pub struct ExtentInfo {
    pub generation: u64,
    pub flush_number: u64,
    pub dirty: bool,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, PartialEq)]
pub enum RepairCheck {
    // We started a repair task
    RepairStarted,
    // No repair is needed
    NoRepairNeeded,
    // We need repair, but a repair was already in progress
    RepairInProgress,
    // Upstairs is not in a valid state for online repair
    InvalidState,
}
/**
 * Support functions for online repair.
 */
// ZZZ Rename this,
// Maybe return something to indicate a repair task has been started?
pub async fn check_for_repair(
    up: &Arc<Upstairs>,
    dst: &[Target],
) -> RepairCheck {
    info!(up.log, "Checking if online repair is needed");
    let active = up.active.lock().await;
    let up_state = active.up_state;
    if up_state != UpState::Active {
        return RepairCheck::InvalidState;
    }
    let mut ds = up.downstairs.lock().await;
    drop(active);
    /*
     * Make sure all downstairs are in the correct state before we
     * proceed.
     */
    let repair = ds
        .ds_state
        .iter()
        .filter(|state| **state == DsState::OnlineRepair)
        .count();

    let repair_ready = ds
        .ds_state
        .iter()
        .filter(|state| **state == DsState::OnlineRepairReady)
        .count();

    if repair_ready == 0 {
        info!(up.log, "No Online Repair required at this time");
        RepairCheck::NoRepairNeeded
    } else if repair > 0 {
        // This also means repair_ready > 0 too
        warn!(up.log, "Upstairs already in repair, no new repairs allowed");
        RepairCheck::RepairInProgress
    } else {
        warn!(up.log, "Downstairs are in need of repair");
        // Move the upstairs that were RR to officially ready.
        // We do this now while we have the lock to avoid having to do all
        // these checks again in online_repair_main
        for cid in 0..3 {
            if ds.ds_state[cid] == DsState::OnlineRepairReady {
                up.ds_transition_with_lock(
                    &mut ds,
                    up_state,
                    cid as u8,
                    DsState::OnlineRepair,
                );
            }
        }
        assert!(ds.repair_min_id.is_none());
        ds.repair_min_id = Some(ds.peek_next_id());

        drop(ds);
        let upc = Arc::clone(up);
        let mut ds_work_vec = Vec::with_capacity(3);
        for d in dst.iter().take(3) {
            ds_work_vec.push(d.ds_work_tx.clone());
        }
        let ds_done_tx = dst[0].ds_done_tx.clone();
        tokio::spawn(async move {
            let res = online_repair_main(&upc, ds_work_vec, ds_done_tx).await;
            warn!(upc.log, "Repair returns {:?}", res);
            // Any error from online_repair_main should finish with any
            // downstairs that was ORR going back to Faulted. But.. maybe
            // we should confirm that here.
        });
        RepairCheck::RepairStarted
    }
}

/**
 * The main task that coordinates repair.
 * This task does the work of turning a downstairs that is in OnlineRepair
 * state into either Active, or Faulted (if the repair fails).
 * When this task ends, the OnlineRepair downstairs will be in one of those
 * two states.
 */
async fn online_repair_main(
    up: &Arc<Upstairs>,
    ds_work_vec: Vec<mpsc::Sender<u64>>,
    ds_done_tx: mpsc::Sender<u64>,
) -> Result<()> {
    let log = up.log.new(o!("task" => "repair".to_string()));
    warn!(log, "Online Repair main task begins.");

    let mut repair_downstairs = Vec::new();
    let mut source_downstairs = None;

    // Before we do any repair, do some sanity checking to make sure we are
    // in a vaild state to begin.
    let active = up.active.lock().await;
    let up_state = active.up_state;
    if up_state != UpState::Active {
        // ZZZ This should return all DS to failed, and clear out the
        // repair_min_id
        bail!("Upstairs in invalid state for repair");
    }
    let mut ds = up.downstairs.lock().await;
    drop(active);

    assert!(ds.repair_job_ids.is_empty());
    // Verify no extent_limits are Some
    assert_eq!(ds.extent_limit.iter().flatten().count(), 0);
    // When we transitioned this downstairs to OR, it should have set
    // the minimum for repair., though we will update it again below.
    assert!(ds.repair_min_id.is_some());

    for cid in 0..3 {
        match ds.ds_state[cid] {
            DsState::OnlineRepair => {
                repair_downstairs.push(cid as u8);
            }
            DsState::Active => {
                source_downstairs = Some(cid as u8);
            }
            state => {
                warn!(
                    log,
                    "Unknown repair action for ds:{} in state {}", cid, state,
                );
                // TODO, what other states are okay?
                // offline could be source?
                // Other states, ignore?
            }
        }
    }

    // A note on failure situations.
    // If we exit this without repair, then any downstairs that was in
    // OnlineRepair should move back to Faulted.  This will kick it out
    // of the upstairs and start another repair attempt.
    if source_downstairs.is_none() {
        error!(log, "Failed to find source downstairs for repair");
        up.abort_repair_ds(&mut ds, up_state);

        bail!("Failed to find a vaild source downstairs for repair");
    }

    if repair_downstairs.is_empty() {
        error!(log, "Failed to find a downstairs needing repair");
        ds.repair_min_id = None;
        bail!("Failed to find a downstairs needing repair");
    }

    // This should have been set already when we first moved the downstairs
    // to OnlineRepair, but really, that was only because of the assertion
    // in enqueue,
    ds.repair_min_id = Some(ds.peek_next_id());
    drop(ds);

    let source_downstairs = source_downstairs.unwrap();

    let extent_count = match up.ddef.lock().await.get_def() {
        Some(ddef) => ddef.extent_count(),
        None => {
            panic!("Can't get ddef and we need to repair");
        }
    };

    let mut repair_fail = false;
    info!(log, "Start Online Repair of extents 0 to {}", extent_count);
    for eid in 0..extent_count {
        info!(log, "Start extent {} repair", eid);
        cdt::extent__or__start!(|| (eid));

        if repair_fail {
            info!(log, "extent {} repair has failed", eid);
            // This repair has failed, we only need to keep going if
            // we made future job reservations.
            let mut gw = up.guest.guest_work.lock().await;
            let mut ds = up.downstairs.lock().await;
            // Verify state has been cleared.
            for cid in 0..3 {
                assert_eq!(ds.extent_limit[cid], None);
                assert!(ds.ds_state[cid] != DsState::OnlineRepair);
            }
            assert_eq!(ds.repair_min_id, None);
            if ds.query_repair_ids(eid) {
                // There are some reservations we need to clear out.
                let _ = up.abort_repair_extent(&mut gw, &mut ds, eid);
            } else {
                // Nothing left to repair, we can stop looking now.
                warn!(log, "Exit repair at extent {}", eid);
                cdt::extent__or__done!(|| (eid));
                break;
            }
        } else if up
            .repair_extent(
                &ds_work_vec,
                eid,
                source_downstairs,
                repair_downstairs.clone(),
            )
            .await
            .is_err()
        {
            warn!(log, "Error After extent {} repair", eid);
            repair_fail = true;
        }
        cdt::extent__or__done!(|| (eid));
    }

    // Send a final flush, wait for it to finish so we know the downstairs
    // has cleared all the repair jobs and our "last flush" is set.

    // ZZZ Maybe don't send a final flush???
    let (send, recv) = mpsc::channel(1);
    let op = BlockOp::Flush {
        snapshot_details: None,
    };
    let flush_br = BlockReq::new(op, send);
    let flush_brw = BlockReqWaiter::new(recv);

    let res = up.submit_flush(Some(flush_br), None, ds_done_tx).await;
    match res {
        Ok(()) => {
            info!(up.log, "Final flush submitted");
        }
        Err(e) => {
            error!(up.log, "Final flush faild.. WTH {:?}", e);
            // ZZZ is this fatal?
        }
    }

    // Send a notification to all downstairs clients there is new work.
    for (cid, ds_work_tx) in ds_work_vec.iter().enumerate() {
        let res = ds_work_tx.send(1).await;
        if let Err(e) = res {
            // ZZZ handle this
            error!(
                up.log,
                "ERROR {:#?} Failed to notify client {} of OR flush", e, cid
            );
        }
    }
    // Wait on flush_brw
    let res = flush_brw.wait().await;
    info!(up.log, "RE: Final Got {:?} back from flush_brw", res);
    // Handle this too...
    assert!(res.is_ok());

    // A last check for state here.  If we are failing the repair, then
    // we should not transition to Active.
    // For either failed or passed, we should be sure to clear out any
    // OnlineRepair settings.

    let active = up.active.lock().await;
    let up_state = active.up_state;
    let mut ds = up.downstairs.lock().await;
    drop(active);

    if repair_fail {
        for cid in 0..3 {
            assert!(ds.extent_limit[cid].is_none());
            assert!(ds.ds_state[cid] != DsState::OnlineRepair);
        }
        for cid in repair_downstairs.iter() {
            ds.online_repair_aborted[*cid as usize] += 1;
        }
        assert!(ds.repair_min_id.is_none());
    } else {
        for cid in repair_downstairs.iter() {
            ds.extent_limit[*cid as usize] = None;
            ds.repair_min_id = None;
            up.ds_transition_with_lock(
                &mut ds,
                up_state,
                *cid,
                DsState::Active,
            );
            ds.online_repair_completed[*cid as usize] += 1;
        }
    }
    ds.end_online_repair();
    Ok(())
}

fn create_reopen_io(
    eid: usize,
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    impacted_blocks: ImpactedBlocks,
) -> DownstairsIO {
    let reopen_ioop = IOop::ExtentLiveReopen {
        dependencies,
        extent: eid,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: reopen_ioop,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

#[allow(clippy::too_many_arguments)]
fn create_close_io(
    eid: usize,
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    impacted_blocks: ImpactedBlocks,
    flush: u64,
    gen: u64,
    source: u8,
    repair: Vec<u8>,
) -> DownstairsIO {
    let close_ioop = IOop::ExtentFlushClose {
        dependencies,
        extent: eid,
        flush_number: flush,
        gen_number: gen,
        source_downstairs: source,
        repair_downstairs: repair,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }

    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: close_ioop,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

#[allow(clippy::too_many_arguments)]
fn create_repair_io(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    extent: usize,
    impacted_blocks: ImpactedBlocks,
    repair_address: SocketAddr,
    source_downstairs: u8,
    repair_downstairs: Vec<u8>,
) -> DownstairsIO {
    let repair_ioop = IOop::ExtentLiveRepair {
        dependencies,
        extent,
        source_downstairs,
        source_repair_address: repair_address,
        repair_downstairs,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: repair_ioop,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

fn create_noop_io(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    impacted_blocks: ImpactedBlocks,
) -> DownstairsIO {
    let noop_ioop = IOop::ExtentLiveNoOp { dependencies };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: noop_ioop,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

// Look to see if we need to repair extent(s), or create a no op job.
// Look at the source extent's gen/flush and compare with all the
// extents on the repair list.
// If a repair extent is dirty, then it needs repair.
// If a repair extent does not match gen or flush from the source, then
// repair it.
//
// Construct the correct job and return that to the caller.
// everything is good, then just submit a NoOp job.
#[allow(clippy::too_many_arguments)]
fn repair_or_noop(
    ds: &mut Downstairs,
    extent: usize,
    repair_id: u64,
    repair_deps: Vec<u64>,
    gw_repair_id: u64,
    impacted_blocks: ImpactedBlocks,
    source: u8,
    repair: Vec<u8>,
) -> DownstairsIO {
    assert!(repair.len() < 3);

    let mut need_repair = Vec::new();
    debug!(ds.log, "Get repair info for {} source", source);
    let good_ei = ds.repair_info.remove(&source).unwrap();
    for broken_extent in repair.iter() {
        debug!(ds.log, "Get repair info for {} bad", broken_extent);
        let repair_ei = ds.repair_info.remove(broken_extent).unwrap();

        let repair = if repair_ei.dirty {
            true
        } else if repair_ei.generation != good_ei.generation {
            true
        } else {
            repair_ei.flush_number != good_ei.flush_number
        };

        if repair {
            need_repair.push(*broken_extent);
        }
    }

    // Now that we have consumed the contents, be sure to clear
    // out anything we did not look at.  There could be something left
    ds.repair_info = HashMap::new();

    if need_repair.is_empty() {
        info!(ds.log, "No repair needed for extent {}", extent);
        for cid in repair.iter() {
            ds.extents_confirmed[*cid as usize] += 1;
        }
        create_noop_io(repair_id, repair_deps, gw_repair_id, impacted_blocks)
    } else {
        info!(
            ds.log,
            "Repair for extent {} s:{} d:{:?}", extent, source, need_repair
        );
        for cid in repair.iter() {
            ds.extents_repaired[*cid as usize] += 1;
        }
        let repair_address = ds.repair_addr(source);

        create_repair_io(
            repair_id,
            repair_deps,
            gw_repair_id,
            extent,
            impacted_blocks,
            repair_address,
            source,
            need_repair,
        )
    }
}

// Build the list of dependencies for an online repair job.
// Because we need all three repair jobs to happen lock step, we
// have to prevent any IO from hitting the same extent, which means
// any IO going to our ImpactedBlocks (the whole extent) must finish
// first (or come after) our job.
// Also included is the starting job ID we expect this repair to have.
// We need this starting ID to prevent us from adding a dependency on
// an IO that requires these repairs to finish before it can move
// forward.  We do this by not making dependencies on any IO that
// has a higher job ID than us, as if any exist in that situation they
// should be created for is.   ZZZ that comment is shite, fix it...
fn deps_for_online_repair(
    ds: &Downstairs,
    impacted_blocks: ImpactedBlocks,
    close_id: u64,
) -> Vec<u64> {
    let num_jobs = ds.ds_active.keys().len();
    let mut deps: Vec<u64> = Vec::with_capacity(num_jobs);

    // Search backwards in the list of active jobs, stop at the
    // last flush
    for job_id in ds
        .ds_active
        .keys()
        .sorted()
        .collect::<Vec<&u64>>()
        .iter()
        .rev()
    {
        let job = &ds.ds_active[job_id];

        // If we scheduled a job that required reservation of repair IDs,
        // and we are now determining that repair jobs dependencies, don't
        // include that original job in our dependency list as it already
        // has this repair job on its list.
        if job.ds_id > close_id {
            continue;
        }
        // If this operation impacts the same blocks as something
        // already active, create a dependency.
        if impacted_blocks.conflicts(&job.impacted_blocks) {
            deps.push(**job_id);
        }

        // A flush job won't show impacted blocks. We can stop looking
        // for dependencies beyond the last flush.
        if job.work.is_flush() {
            deps.push(**job_id);
            break;
        }
    }

    deps
}

#[allow(clippy::too_many_arguments)]
async fn create_and_enqueue_reopen_io(
    ds: &mut Downstairs,
    gw: &mut GuestWork,
    eid: u32,
    deps: Vec<u64>,
    reopen_id: u64,
    gw_reopen_id: u64,
    impacted_blocks: ImpactedBlocks,
) -> block_req::BlockReqWaiter {
    let reopen_io = create_reopen_io(
        eid as usize,
        reopen_id,
        deps,
        gw_reopen_id,
        impacted_blocks,
    );

    let mut sub = HashMap::new();
    sub.insert(reopen_id, 0);

    cdt::gw__reopen__start!(|| (gw_reopen_id, eid));
    let (send, recv) = mpsc::channel(1);
    let op = BlockOp::RepairOp;
    let reopen_br = BlockReq::new(op, send);
    let reopen_brw = BlockReqWaiter::new(recv);
    let new_gtos =
        GtoS::new(sub, Vec::new(), None, HashMap::new(), Some(reopen_br));
    {
        gw.active.insert(gw_reopen_id, new_gtos);
    }
    ds.enqueue_repair(reopen_io).await;
    reopen_brw
}

// This creates and enqueues an ExtentFlushClose operation onto the work queue.
#[allow(clippy::too_many_arguments)]
async fn create_and_enqueue_close_io(
    ds: &mut Downstairs,
    gw: &mut GuestWork,
    eid: u32,
    next_flush: u64,
    gen: u64,
    deps: Vec<u64>,
    close_id: u64,
    gw_close_id: u64,
    impacted_blocks: ImpactedBlocks,
    source: u8,
    repair: Vec<u8>,
) -> block_req::BlockReqWaiter {
    let close_io = create_close_io(
        eid as usize,
        close_id,
        deps,
        gw_close_id,
        impacted_blocks,
        next_flush,
        gen,
        source,
        repair.clone(),
    );

    let mut sub = HashMap::new();
    sub.insert(close_id, 0);

    cdt::gw__close__start!(|| (gw_close_id, eid));
    let (send, recv) = mpsc::channel(1);
    let op = BlockOp::RepairOp;
    let close_br = BlockReq::new(op, send);
    let close_brw = BlockReqWaiter::new(recv);
    let new_gtos =
        GtoS::new(sub, Vec::new(), None, HashMap::new(), Some(close_br));
    {
        gw.active.insert(gw_close_id, new_gtos);
    }
    ds.enqueue_repair(close_io).await;
    close_brw
}

#[allow(clippy::too_many_arguments)]
async fn create_and_enqueue_repair_io(
    ds: &mut Downstairs,
    gw: &mut GuestWork,
    eid: u32,
    deps: Vec<u64>,
    repair_id: u64,
    gw_repair_id: u64,
    impacted_blocks: ImpactedBlocks,
    source: u8,
    repair: Vec<u8>,
) -> block_req::BlockReqWaiter {
    let repair_io = repair_or_noop(
        ds,
        eid as usize,
        repair_id,
        deps,
        gw_repair_id,
        impacted_blocks,
        source,
        repair.clone(),
    );

    let mut sub = HashMap::new();
    sub.insert(repair_id, 0);

    cdt::gw__repair__start!(|| (gw_repair_id, eid));
    let (send, recv) = mpsc::channel(1);
    let op = BlockOp::RepairOp;
    let repair_br = BlockReq::new(op, send);
    let repair_brw = BlockReqWaiter::new(recv);
    let new_gtos =
        GtoS::new(sub, Vec::new(), None, HashMap::new(), Some(repair_br));
    {
        gw.active.insert(gw_repair_id, new_gtos);
    }
    ds.enqueue_repair(repair_io).await;
    repair_brw
}

async fn create_and_enqueue_noop_io(
    ds: &mut Downstairs,
    gw: &mut GuestWork,
    deps: Vec<u64>,
    noop_id: u64,
    gw_noop_id: u64,
    impacted_blocks: ImpactedBlocks,
) -> block_req::BlockReqWaiter {
    let nio = create_noop_io(noop_id, deps, gw_noop_id, impacted_blocks);

    let mut sub = HashMap::new();
    sub.insert(noop_id, 0);

    cdt::gw__noop__start!(|| (gw_noop_id));
    let (send, recv) = mpsc::channel(1);
    let op = BlockOp::RepairOp;
    let noop_br = BlockReq::new(op, send);
    let noop_brw = BlockReqWaiter::new(recv);
    let new_gtos =
        GtoS::new(sub, Vec::new(), None, HashMap::new(), Some(noop_br));
    {
        gw.active.insert(gw_noop_id, new_gtos);
    }
    ds.enqueue_repair(nio).await;
    noop_brw
}

// ZZZ make a test for this
// Check if any downstairs have changed state since starting repair.
fn abort_if_state_change(
    ds: &mut MutexGuard<'_, Downstairs>,
    source: u8,
    repair: &[u8],
) -> bool {
    for cid in repair.iter() {
        if ds.ds_state[*cid as usize] != DsState::OnlineRepair {
            return true;
        }
    }
    ds.ds_state[source as usize] != DsState::Active
}

impl Upstairs {
    // Abort a repair in progress and clear out any repair state
    fn abort_repair_ds(
        &self,
        ds: &mut MutexGuard<'_, Downstairs>,
        up_state: UpState,
    ) {
        for cid in 0..3 {
            if ds.ds_state[cid] == DsState::OnlineRepair {
                ds.extent_limit[cid] = None;
                ds.ds_set_faulted(cid as u8);
                self.ds_transition_with_lock(
                    ds,
                    up_state,
                    cid as u8,
                    DsState::Faulted,
                );
            }
        }
        ds.repair_min_id = None;
    }

    // If the downstairs is already faulted at this point, then a previous
    // extent repair failed, or an IO to an extent of the downstairs we
    // already fixed returned an error.
    async fn abort_repair_extent(
        &self,
        gw: &mut MutexGuard<'_, GuestWork>,
        ds: &mut MutexGuard<'_, Downstairs>,
        eid: u32,
    ) -> Result<()> {
        println!("Move all to failed");

        warn!(self.log, "Extent {} Aborting repair", eid);

        // If there were job IDs reserved for future repairs, we must
        // create NoOp jobs for those and submit them.  This will allow
        // any job that depend on them to have that dependency met.
        if ds.query_repair_ids(eid) {
            // If all three downstairs have faulted, then, we are dead
            // anyway, so just give up.  There is no way we can make
            // progress, and no point in creating any more work.
            if ds
                .ds_state
                .iter()
                .filter(|state| **state == DsState::Faulted)
                .count()
                == 3
            {
                // All downstairs faulted, let's just quit now as there is
                // no more work to do.
                ds.end_online_repair();
                error!(
                    self.log,
                    "Abort repair on extent {}: All downstairs are Faulted",
                    eid,
                );
                return Ok(());
            }
            warn!(self.log, "Extent {} Create and send noop jobs", eid);

            // Get the work IDs for the downstairs jobs we need to create.
            // We get the first one, then send three more based on it
            // as a starting value
            let extent_repair_ids = ds.get_repair_ids(eid);
            let ds_id = extent_repair_ids.close_id;

            // Get the list of deps all repair operations will start from.
            // We don't care about these jobs with respect to each other as
            // none are doing any work.
            let ddef = self.ddef.lock().await.get_def().unwrap();
            let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

            let deps = deps_for_online_repair(ds, impacted_blocks, ds_id);

            warn!(
                self.log,
                "Abort repair on extent {} starting with id {} deps:{:?}",
                eid,
                ds_id,
                deps,
            );

            // Create the NoOps
            let mut noops = Vec::new();
            for id in ds_id..ds_id + 4 {
                let gw_id = gw.next_gw_id();
                let noop_brw = create_and_enqueue_noop_io(
                    ds,
                    gw,
                    deps.clone(),
                    id,
                    gw_id,
                    impacted_blocks,
                )
                .await;
                noops.push(noop_brw);
            }
        }
        Ok(())
    }

    // Given an extent, with client_ids for both the source and the
    // "may need repair" downstairs, construct and send the work
    // required to repair the extent.
    //
    // We don't actually know if an extent needs repair until we close it
    // and get back the flush, gen, and dirty bit from each downstairs
    // and then compare them.
    //
    // The high level flow here is:
    // Reserve our job IDs, both the downstairs and the guest side.
    // Figure out any dependencies based on current in flight IOs
    // Insert the reopen job (that we will do last).
    // Insert the close job.
    // Wait for close job to send us results from all three downstairs.
    // Figure out if we need repair or no-op.
    // Insert repair or no-op job.
    // Wait for result from all three downstairs on repair/noop, then
    // Send NoOp job, wait for result.
    // Wait on reopen job to finish.
    //
    // When we Create and enqueue the Close and the Reopen jobs, we
    // leave a job ID gap for the Repair or NoOp job and the NoOp job that
    // will follow it. By enqueueing the reopen job (with a dependency on
    // the not yet created repair job), we
    // can create a "block" of four IOs that
    // 1. Will execute in order.
    // 2. Will prevent new IOs from getting between any of them..
    // 3. Allow us to "insert" a job in the middle, after getting the
    //    result from the close, but before the downstairs executes the
    //    reopen job which is already on the queue.
    // 4. By inserting the final reopen job first, we allow for other IOs
    //    to queue up after it.
    //
    // Now, If we get an error, then, that is terrible.  Because an Error
    // can leave us in an inconsistent state, we stop all repair actions
    // and move the upstairs to Faulted, and disconnect.  However, once we
    // have started a repair, we need to finish the jobs we created, and
    // fill in any gaps with NoOp jobs, depending where we failed.  We do
    // want to allow downstairs that are still working to continue working.
    async fn repair_extent(
        &self,
        ds_work_vec: &[mpsc::Sender<u64>],
        eid: u32,
        source: u8,
        repair: Vec<u8>,
    ) -> Result<()> {
        debug!(self.log, "RE:{} Repair extent begins", eid);
        if !self.guest_io_ready().await {
            info!(self.log, "Repairing while inactive");
            // ZZZ This should not happen, or maybe if we deactivated
            // while repairing?
        }

        // To consider:
        // ZZZ If our source downstairs skipped the IO, we will still consider
        // that a success I believe...  Make sure that we don't end up skipping
        // work on a downstairs we care about (like the close/reopen/repair
        // set of things.  If we are repairing from a downstairs that has failed,
        //  we have to throw up our hands and abort the whole thing.
        let active = self.active.lock().await;
        let up_state = active.up_state;
        let mut gw = self.guest.guest_work.lock().await;
        let mut ds = self.downstairs.lock().await;
        drop(active);

        // Make a last chance for easy abort if anything has changed since
        // we started this repair.  If we find things are different, it's
        // easier to abort now.  Once we move forward with this repair, the
        // cleanup is more complicated.
        let mut abort_repair = abort_if_state_change(&mut ds, source, &repair);
        if abort_repair || up_state != UpState::Active {
            error!(
                self.log,
                "RE: downstairs state change, aborting repair now"
            );
            self.abort_repair_ds(&mut ds, up_state);
            // Since we have done nothing yet, we can call abort here and
            // not have to issue jobs if none have been reserved.
            let res = self.abort_repair_extent(&mut gw, &mut ds, eid).await;

            drop(ds);
            drop(gw);
            // The abort_repair_extent might have put job(s) on the queue.
            // Send a notify to the downstairs tasks just in case.
            for (cid, ds_work_tx) in ds_work_vec.iter().enumerate() {
                let res = ds_work_tx.send(1).await;
                if let Err(e) = res {
                    error!(
                        self.log,
                        "ERROR {:#?} Failed to notify client {} of OR job",
                        e,
                        cid
                    );
                }
            }
            return res;
        }
        assert!(ds.repair_min_id.is_some());
        assert!(ds.repair_info.is_empty());

        // Repair has officially started for this extent.
        // Once we start sending repair jobs, we must finish. If we encounter
        // an error in the middle, then we still need to submit NoOp
        // jobs, and let the reopen job have its dependencies met for the
        // sake of the downstairs that are not being repaired.

        // Update our extent limit to this extent.
        for ds_repair in repair.iter() {
            // We should be walking up the extents one at a time.
            if eid > 0 {
                assert_eq!(
                    ds.extent_limit[*ds_repair as usize],
                    Some(eid as usize - 1)
                );
            } else {
                assert!(ds.extent_limit[*ds_repair as usize].is_none())
            }
            ds.extent_limit[*ds_repair as usize] = Some(eid as usize);
        }
        let ddef = self.ddef.lock().await.get_def().unwrap();

        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = gw.next_gw_id();
        let gw_repair_id: u64 = gw.next_gw_id();
        let gw_noop_id: u64 = gw.next_gw_id();
        let gw_reopen_id: u64 = gw.next_gw_id();

        // The work IDs for the downstairs side of things.
        let extent_repair_ids = ds.get_repair_ids(eid);
        let close_id = extent_repair_ids.close_id;
        let repair_id = extent_repair_ids.repair_id;
        let noop_id = extent_repair_ids.noop_id;
        let reopen_id = extent_repair_ids.reopen_id;

        // Get the list of dependencies all repair operations will start from.
        // Note that this list of dependencies will potentially include
        // skipped jobs for some downstairs.  The list of dependencies may
        // be further altered when we are about to send IO to an individual
        // downstairs that is under repair. At that time, we go through the
        // list of dependencies and remove jobs that we skipped or done for
        // that specific downstairs before we send the IO over the wire.
        let mut deps = deps_for_online_repair(&ds, impacted_blocks, close_id);

        info!(
            self.log,
            "RE:{} repair extent with ids {},{},{},{} deps:{:?}",
            eid,
            close_id,
            repair_id,
            noop_id,
            reopen_id,
            deps,
        );

        // The initial close IO has the base set of dependencies.
        // Each additional job will depend on the previous.
        let close_deps = deps.clone();

        deps.push(close_id);
        let repair_deps = deps.clone();

        deps.push(repair_id);
        let noop_deps = deps.clone();

        deps.push(noop_id);
        let reopen_deps = deps.clone();
        debug!(
            self.log,
            "RE:{} Repair with ids {},{},{},{} final deps:{:?}",
            eid,
            close_id,
            repair_id,
            noop_id,
            reopen_id,
            deps,
        );

        let reopen_brw = create_and_enqueue_reopen_io(
            &mut ds,
            &mut gw,
            eid,
            reopen_deps,
            reopen_id,
            gw_reopen_id,
            impacted_blocks,
        )
        .await;

        // Next we create and insert the close job on the work queue.
        let next_flush = self.next_flush_id().await;
        let gen = self.get_generation().await;

        let close_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            next_flush,
            gen,
            close_deps,
            close_id,
            gw_close_id,
            impacted_blocks,
            source,
            repair.clone(),
        )
        .await;

        // Now that we have enqueued both the close and the reopen, we
        // can release all the locks and wait for the result from our close.

        // Make sure we don't have any repair info yet
        drop(gw);
        drop(ds);

        debug!(
            self.log,
            "RE:{} close id:{} queued, notify DS", eid, close_id
        );
        // Send a notification to all downstairs clients there is new work.
        for (cid, ds_work_tx) in ds_work_vec.iter().enumerate() {
            let res = ds_work_tx.send(1).await;
            if let Err(e) = res {
                panic!(
                    "ERROR {:#?} Failed to notify client {} of OR work",
                    e, cid
                );
            }
        }

        debug!(
            self.log,
            "RE:{} Wait for result from close command {}:{}",
            eid,
            close_id,
            gw_close_id
        );
        // Wait on our close to report back from all three downstairs.
        match close_brw.wait().await {
            Ok(()) => {
                debug!(self.log, "Extent {} close id:{} Done", eid, close_id);
            }
            Err(e) => {
                error!(
                    self.log,
                    "Extent {} close id:{} Failed: {}", eid, close_id, e
                );
                let active = self.active.lock().await;
                let up_state = active.up_state;
                let mut ds = self.downstairs.lock().await;
                drop(active);
                self.abort_repair_ds(&mut ds, up_state);
                abort_repair = true;
            }
        }

        // Get the locks, and then submit either the repair, if it
        // is needed, or a noop job.  Every time we get the lock, we have
        // to re-verify that things are in a good place to continue.
        let active = self.active.lock().await;
        let up_state = active.up_state;
        let mut gw = self.guest.guest_work.lock().await;
        let mut ds = self.downstairs.lock().await;
        drop(active);

        if !abort_repair {
            abort_repair = abort_if_state_change(&mut ds, source, &repair);
            if abort_repair || up_state != UpState::Active {
                warn!(
                    self.log,
                    "RE: downstairs state change, aborting repair now"
                );
                self.abort_repair_ds(&mut ds, up_state);
            }
        }
        // If an downstairs went away, it's possible the IO was skipped on it.
        // We can't handle this currently, so if we don't have all downstairs
        // then just abort this online repair.
        let repair_brw = if abort_repair {
            create_and_enqueue_noop_io(
                &mut ds,
                &mut gw,
                repair_deps,
                repair_id,
                gw_repair_id,
                impacted_blocks,
            )
            .await
        } else {
            create_and_enqueue_repair_io(
                &mut ds,
                &mut gw,
                eid,
                repair_deps,
                repair_id,
                gw_repair_id,
                impacted_blocks,
                source,
                repair.clone(),
            )
            .await
        };
        drop(gw);
        drop(ds);

        // The next op is on the queue, now tell the downstairs
        for (cid, ds_work_tx) in ds_work_vec.iter().enumerate() {
            let res = ds_work_tx.send(1).await;
            if let Err(e) = res {
                panic!(
                    "ERROR {:#?} Failed to notify client {} of OR work",
                    e, cid
                );
            }
        }

        // Wait on the results of our repair (or noop) request.
        debug!(
            self.log,
            "RE:{} Wait for result from repair command {}:{}",
            eid,
            repair_id,
            gw_repair_id
        );
        let res = repair_brw.wait().await;
        debug!(
            self.log,
            "RE:{} Got {:?} back from repair_brw {}", eid, res, repair_id
        );

        // If we are already aborting this error, then more errors don't
        // change what we do.  If we have not reported an error yet, then
        // we need to tack action on it.
        if res.is_err() {
            error!(
                self.log,
                "Extent:{} job:{} Got {:?} back from repair_brw",
                eid,
                repair_id,
                res
            );
            let active = self.active.lock().await;
            let up_state = active.up_state;
            let mut ds = self.downstairs.lock().await;
            self.abort_repair_ds(&mut ds, up_state);
            abort_repair = true;
        }

        // put the next job on the queue.  This job acts like a barrier and
        // will not let IOs continue on an extent until we know for certain
        // that extent has been repaired. If we have already decided to
        // abort this error or not, it makes no difference, we still need
        // to send this NoOp to satisfy dependencies.
        let active = self.active.lock().await;
        let up_state = active.up_state;
        let mut gw = self.guest.guest_work.lock().await;
        let mut ds = self.downstairs.lock().await;

        drop(active);
        if !abort_repair {
            abort_repair = abort_if_state_change(&mut ds, source, &repair);
            if abort_repair {
                warn!(
                    self.log,
                    "RE: downstairs state change, aborting repair now"
                );
                self.abort_repair_ds(&mut ds, up_state);
            }
        }

        // This is the same if we are in error or not.
        let noop_brw = create_and_enqueue_noop_io(
            &mut ds,
            &mut gw,
            noop_deps,
            noop_id,
            gw_noop_id,
            impacted_blocks,
        )
        .await;

        drop(gw);
        drop(ds);

        // The noop is on the queue, now tell the downstairs
        for (cid, ds_work_tx) in ds_work_vec.iter().enumerate() {
            let res = ds_work_tx.send(1).await;
            if let Err(e) = res {
                panic!(
                    "ERROR {:#?} Failed to notify client {} of NoOp job",
                    e, cid
                );
            }
        }

        // Wait on the results of our NoOp command.
        debug!(
            self.log,
            "RE:{} Wait for result from NoOp command {}:{}",
            eid,
            noop_id,
            gw_noop_id
        );

        // ZZZ Maybe make this a sub function? Wait on a channel and
        // if error, then abort.
        let res = noop_brw.wait().await;
        debug!(
            self.log,
            "RE:{} Got {:?} back from noop_brw {}:{}",
            eid,
            res,
            noop_id,
            gw_noop_id
        );
        if res.is_err() {
            abort_repair = true;
            error!(
                self.log,
                "Extent:{} job:{} Got {:?} back from NoOp", eid, noop_id, res
            );
            let active = self.active.lock().await;
            let up_state = active.up_state;
            let mut ds = self.downstairs.lock().await;
            drop(active);
            self.abort_repair_ds(&mut ds, up_state);
        }

        // let abort_repair = result from wait command?
        // Pass in current abort_repair value.
        // If error back, report it.
        // If error + abort_fail not yet set, then take error path
        // and abort_repair_ds and return true
        // otherwise, return false
        // The reopen command will have already been queued so we can just
        // move to waiting for it to be completed..
        debug!(
            self.log,
            "RE:{} Wait for result from reopen command {}:{}",
            eid,
            reopen_id,
            gw_reopen_id
        );
        let res = reopen_brw.wait().await;
        debug!(
            self.log,
            "RE:{} Got {:?} back from reopen_brw {}", eid, res, reopen_id
        );
        if res.is_err() {
            abort_repair = true;
            error!(
                self.log,
                "Extent:{} job:{} Got {:?} back from ReOpen", eid, noop_id, res
            );
            let active = self.active.lock().await;
            let up_state = active.up_state;
            let mut ds = self.downstairs.lock().await;
            drop(active);
            self.abort_repair_ds(&mut ds, up_state);
        }

        // One final check to be sure nothing we need has gone astray
        // for these last few commands, but only if we are not already
        // failing this repair.
        if !abort_repair {
            let active = self.active.lock().await;
            let up_state = active.up_state;
            let mut ds = self.downstairs.lock().await;
            drop(active);
            abort_repair = abort_if_state_change(&mut ds, source, &repair);
            if abort_repair {
                error!(
                    self.log,
                    "RE: downstairs state change, aborting repair now"
                );
                self.abort_repair_ds(&mut ds, up_state);
            }
        }

        if abort_repair {
            warn!(self.log, "RE:{} Bailing with error", eid);
            let ds = self.downstairs.lock().await;
            assert_eq!(
                ds.ds_state
                    .iter()
                    .filter(|state| **state == DsState::OnlineRepair)
                    .count(),
                0
            );
            bail!("Failed to repair extent {}", eid)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
pub mod repair_test {
    use super::*;

    // Test function to create just enough of an Upstairs for our needs.
    // The caller will indicate which downstairs client it wished to be
    // moved to OnlineRepair.
    async fn create_test_upstairs(fail_id: u8) -> Arc<Upstairs> {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));

        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
            // Give all downstairs a repair address
            up.ds_set_repair_address(cid, "0.0.0.0:1".parse().unwrap())
                .await;
        }

        // Move our downstairs client fail_id to OnlineRepair.
        up.set_active().await.unwrap();
        up.ds_transition(fail_id, DsState::Faulted).await;
        up.ds_transition(fail_id, DsState::OnlineRepairReady).await;
        up.ds_transition(fail_id, DsState::OnlineRepair).await;
        // ZZZ This comes out?  We set it elsewhere now??
        up.downstairs.lock().await.repair_min_id = Some(1000);

        up
    }

    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    // Test function to create a downstairs.
    fn create_test_downstairs() -> Downstairs {
        let mut ds = Downstairs::new(csl(), vec![]);
        for cid in 0..3 {
            ds.ds_repair.insert(cid, "127.0.0.1:1234".parse().unwrap());
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

        for _cid in 0..3 {
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
        ds_id: u64,
        skip_ds: Vec<u8>,
    ) -> Result<()> {
        // let mut ds = up.downstairs.lock().await;
        for cid in 0..3 {
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
        ds_id: u64,
        result: Result<(), CrucibleError>,
    ) {
        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let state = ds.ds_active.get_mut(&ds_id).unwrap().ack_status;
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
        or_ds: u8,
    ) -> (
        Arc<Upstairs>,
        tokio::task::JoinHandle<()>,
        Vec<mpsc::Receiver<u64>>,
    ) {
        assert!(or_ds < 3);
        let source_ds = (or_ds + 1) % 3;
        let up = create_test_upstairs(or_ds).await;
        let (dst, ds_work_rx) = create_test_dst_rx();
        let mut ds_work_vec = Vec::with_capacity(3);
        for this_target in dst.iter().take(3) {
            ds_work_vec.push(this_target.ds_work_tx.clone());
        }
        let upc = Arc::clone(&up);

        let repair_handle = tokio::spawn(async move {
            upc.repair_extent(&ds_work_vec, 0, source_ds, [or_ds].to_vec())
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
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::NoRepairNeeded
        );
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
        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive).await;
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }
        up.ds_transition(1, DsState::Faulted).await;
        up.ds_transition(1, DsState::OnlineRepairReady).await;
        assert_eq!(
            check_for_repair(&up, &dst).await,
            RepairCheck::RepairStarted
        );
    }

    // ZZZ Add a test that will return retry if online repair is already going.

    // Test the permutations of calling repair_extent with each downstairs
    // as the one needing OnlineRepair
    #[tokio::test]
    async fn test_repair_extent_no_action_all() {
        for or_ds in 0..3 {
            test_repair_extent_no_action(or_ds).await;
        }
    }

    async fn test_repair_extent_no_action(or_ds: u8) {
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
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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
        for cid in 0..3 {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei.clone()),
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

    // Loop over the possible downstairs to be in OnlineRepair and
    // run through the do_repair code path.
    #[tokio::test]
    async fn test_repair_extent_do_repair_all() {
        for or_ds in 0..3 {
            test_repair_extent_do_repair(or_ds).await;
        }
    }

    async fn test_repair_extent_do_repair(or_ds: u8) {
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
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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

        for cid in 0..3 {
            up.downstairs.lock().await.in_progress(ds_close_id, cid);
            if cid == or_ds {
                up.process_ds_operation(
                    ds_close_id,
                    cid,
                    Ok(vec![]),
                    Some(bad_ei.clone()),
                )
                .await
                .unwrap();
            } else {
                up.process_ds_operation(
                    ds_close_id,
                    cid,
                    Ok(vec![]),
                    Some(ei.clone()),
                )
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
        assert_eq!(ds.ds_state[or_ds as usize], DsState::OnlineRepair);
    }

    #[tokio::test]
    async fn test_repair_extent_close_fails_all() {
        // Test all the permutations of
        // A downstairs that is in OnlineRepair
        // A downstairs that returns error on the ExtentFlushClose operation.
        for faild_ds in 0..3 {
            for err_ds in 0..3 {
                test_repair_extent_close_fails(faild_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_close_fails(or_ds: u8, err_ds: u8) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the initial close command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in OnlineRepair, and the
        // downstairs that will return error for the ExtentClose operation.
        // They may be the same downstairs.

        assert!(err_ds < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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
        for cid in 0..3 {
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
                up.process_ds_operation(
                    ds_close_id,
                    cid,
                    Ok(vec![]),
                    Some(ei.clone()),
                )
                .await
                .unwrap();
            }
        }

        // process_ds_operation should force the downstairs to fail
        assert_eq!(
            up.downstairs.lock().await.ds_state[err_ds as usize],
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

        assert_eq!(ds.ds_state[err_ds as usize], DsState::Faulted);
        assert_eq!(ds.ds_state[or_ds as usize], DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_repair_fails_all() {
        // Test all the permutations of
        // A downstairs that is in OnlineRepair
        // A downstairs that returns error on the ExtentLiveRepair operation.
        for or_ds in 0..3 {
            for err_ds in 0..3 {
                test_repair_extent_repair_fails(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_repair_fails(or_ds: u8, err_ds: u8) {
        // This test covers calling repair_extent() and tests that the
        // error handling when the repair command fails.
        // In this test, we will simulate responses from the downstairs tasks.
        //
        // We take two inputs, the downstairs that is in OnlineRepair, and the
        // downstairs that will return error for the ExtentLiveRepair
        // operation.  They may be the same downstairs.

        assert!(err_ds < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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
        for cid in 0..3 {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei.clone()),
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
        for cid in 0..3 {
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
                    Some(ei.clone()),
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
            up.downstairs.lock().await.ds_state[err_ds as usize],
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

        assert_eq!(ds.ds_state[err_ds as usize], DsState::Faulted);
        assert_eq!(ds.ds_state[or_ds as usize], DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_fail_noop_all() {
        // Test all the permutations of
        // A downstairs that is in OnlineRepair
        // A downstairs that returns error on the ExtentLiveNoOp operation.
        for or_ds in 0..3 {
            for err_ds in 0..3 {
                test_repair_extent_fail_noop(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_fail_noop(or_ds: u8, err_ds: u8) {
        // Test repair_extent when the noop job fails.
        // We take input for both which downstairs is in OnlineRepair, and
        // which downstairs will return error on the NoOp operation.

        assert!(err_ds < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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
        for cid in 0..3 {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei.clone()),
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
        for cid in 0..3 {
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

        assert_eq!(ds.ds_state[err_ds as usize], DsState::Faulted);
        assert_eq!(ds.ds_state[or_ds as usize], DsState::Faulted);
    }

    #[tokio::test]
    async fn test_repair_extent_fail_reopen_all() {
        // Test all the permutations of
        // A downstairs that is in OnlineRepair
        // A downstairs that returns error on the ExtentLiveReopen operation.
        for or_ds in 0..3 {
            for err_ds in 0..3 {
                test_repair_extent_fail_reopen(or_ds, err_ds).await;
            }
        }
    }

    async fn test_repair_extent_fail_reopen(or_ds: u8, err_ds: u8) {
        // Test repair_extent when the reopen job fails.
        // We take input for both which downstairs is in OnlineRepair, and
        // which downstairs will return error on the NoOp operation.

        assert!(err_ds < 3);
        let (up, re_handle, _ds_work_vec) = start_up_and_repair(or_ds).await;

        // By default, the job IDs always start at 1000 and the gw IDs
        // always start at 1. Take advantage of that and knowing that we
        // start from a clean slate to predict what the IDs are for jobs
        // we expect the work queues to have.
        let ds_close_id = 1000;
        let ds_repair_id = 1001;
        let ds_noop_id = 1002;
        let ds_reopen_id = 1003;
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
        for cid in 0..3 {
            ds.in_progress(ds_close_id, cid);
            ds.process_ds_completion(
                ds_close_id,
                cid,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei.clone()),
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
        for cid in 0..3 {
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

        assert_eq!(ds.ds_state[err_ds as usize], DsState::Faulted);
        assert_eq!(ds.ds_state[or_ds as usize], DsState::Faulted);
    }

    #[tokio::test]
    async fn test_reserve_extent_repair_ids() {
        // Verify that we can reserve extent IDs for repair work, and they
        // are allocated as expected.
        let up = create_test_upstairs(1).await;

        let mut ds = up.downstairs.lock().await;
        assert!(!ds.query_repair_ids(0));

        let reserved_ids = ds.reserve_repair_ids(0);
        assert_eq!(reserved_ids.close_id, 1000);
        assert_eq!(reserved_ids.repair_id, 1001);
        assert_eq!(reserved_ids.noop_id, 1002);
        assert_eq!(reserved_ids.reopen_id, 1003);

        let next_id = ds.next_id();
        assert_eq!(next_id, 1004);

        // IDs should now be present
        assert!(ds.query_repair_ids(0));
        // No IDs for extent 1
        assert!(!ds.query_repair_ids(1));

        // Reserve the IDs again, make sure they are still the same.
        let reserved_ids = ds.reserve_repair_ids(0);
        assert_eq!(reserved_ids.close_id, 1000);
        assert_eq!(reserved_ids.repair_id, 1001);
        assert_eq!(reserved_ids.noop_id, 1002);
        assert_eq!(reserved_ids.reopen_id, 1003);

        // Get the IDs again this time.
        let reserved_ids = ds.get_repair_ids(0);
        assert_eq!(reserved_ids.close_id, 1000);
        assert_eq!(reserved_ids.repair_id, 1001);
        assert_eq!(reserved_ids.noop_id, 1002);
        assert_eq!(reserved_ids.reopen_id, 1003);

        // After the get, there should be no record any longer
        assert!(!ds.query_repair_ids(0));
    }

    #[tokio::test]
    async fn test_repair_io_no_el_skipped() {
        // Verify that IOs put on the queue when a downstairs is
        // in OnlineRepair and extent_limit is still None will be skipped
        // only by the downstairs that is in OnlineRepair..
        let up = create_test_upstairs(1).await;
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
        assert!(ds.extent_limit[1].is_none());
        // Check all three IOs.
        for job_id in id..id + 3 {
            assert!(ds.in_progress(job_id, 0).is_some());
            assert!(ds.in_progress(job_id, 1).is_none());
            assert!(ds.in_progress(job_id, 2).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_below_el_sent() {
        // Verify that io put on the queue when a downstairs is
        // in OnlineRepair and the IO is below the extent_limit
        // will be sent to all downstairs for processing.
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
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
        for job_id in id..id + 3 {
            assert!(ds.in_progress(job_id, 0).is_some());
            assert!(ds.in_progress(job_id, 1).is_some());
            assert!(ds.in_progress(job_id, 2).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_at_el_sent() {
        // Verify that IOs put on the queue when a downstairs is
        // in OnlineRepair and extent_limit is at the IO location
        // will be sent to all downstairs for processing.
        // For this situation, we are relying on the submit_write method
        // of the Upstairs to correctly add the dependencies for the
        // in flight or soon to be in flight repair jobs.
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(0);
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
        for job_id in id..id + 3 {
            assert!(ds.in_progress(job_id, 0).is_some());
            assert!(ds.in_progress(job_id, 1).is_some());
            assert!(ds.in_progress(job_id, 2).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_above_el_skipped() {
        // Verify that an IO put on the queue when a downstairs is
        // in OnlineRepair and the IO is above the extent_limit will
        // be skipped by the downstairs that is in OnlineRepair..
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
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
        for job_id in id..id + 3 {
            assert!(ds.in_progress(job_id, 0).is_some());
            assert!(ds.in_progress(job_id, 1).is_none());
            assert!(ds.in_progress(job_id, 2).is_some());
        }
    }

    #[tokio::test]
    async fn test_repair_io_span_el_sent() {
        // Verify that IOs put on the queue when a downstairs is
        // in OnlineRepair and the IO starts at an extent that is below
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
        //   4 | W W W | W W W | W W W | 0,1,2,3
        //   5 | R R R | R R R | R R R | 0,1,2,3,4
        //   6 | WuWuWu| WuWuWu| WuWuWu| 0,1,2,3,4,5
        //
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
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
        for job_id in id..id + 3 {
            assert!(ds.in_progress(job_id, 0).is_some());
            assert!(ds.in_progress(job_id, 1).is_some());
            assert!(ds.in_progress(job_id, 2).is_some());
        }

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs[0].work.deps(), &[1000, 1001, 1002, 1003]);
        assert_eq!(jobs[1].work.deps(), &[1004, 1000, 1001, 1002, 1003]);
        assert_eq!(jobs[2].work.deps(), &[1005, 1004, 1000, 1001, 1002, 1003]);
    }

    #[tokio::test]
    async fn test_repair_read_span_el_sent() {
        // Verify that a read put on the queue when a downstairs is
        // in OnlineRepair and the IO starts at an extent that is below
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
        //   4 | R R R | R R R | R R R | 0,1,2,3
        //
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
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

        let job_id = 1004;
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(job_id, 0).is_some());
        assert!(ds.in_progress(job_id, 1).is_some());
        assert!(ds.in_progress(job_id, 2).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        //
        // These future jobs are not actually created yet, so they
        // won't show up in the work queue.
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs[0].work.deps(), &[1000, 1001, 1002, 1003]);
    }

    #[tokio::test]
    async fn test_repair_write_span_el_sent() {
        // Verify that a write IO put on the queue when a downstairs is
        // in OnlineRepair and the IO starts at an extent that is below
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
        //   4 | W W W | W W W | W W W | 0,1,2,3

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
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

        let id = 1004;
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(id, 0).is_some());
        assert!(ds.in_progress(id, 1).is_some());
        assert!(ds.in_progress(id, 2).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs[0].work.deps(), &[1000, 1001, 1002, 1003]);
    }

    #[tokio::test]
    async fn test_repair_write_span_two_el_sent() {
        // Verify that a write IO put on the queue when a downstairs is
        // in OnlineRepair and the IO starts at the extent that is under
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
        //   8 | W W W | W W W | W W W | 0,1,2,3,4,5,6,7
        //
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(0);
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

        let id = 1008;
        let mut ds = up.downstairs.lock().await;
        assert!(ds.in_progress(id, 0).is_some());
        assert!(ds.in_progress(id, 1).is_some());
        assert!(ds.in_progress(id, 2).is_some());

        // Verify that the future repair jobs were added to our IOs
        // dependency list.
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(
            jobs[0].work.deps(),
            &[1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007]
        );
    }

    #[tokio::test]
    async fn test_online_repair_update() {
        // Make sure that process_ds_completion() will take extent info
        // result and put it on the online repair hashmap.
        // As we don't have an actual downstairs here, we "fake it" by
        // feeding the responses we expect back from the downstairs.
        let up = create_test_upstairs(1).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let ddef = up.ddef.lock().await.get_def().unwrap();
        let eid = 1;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        let close_id = 1000;
        let gw_close_id = 1;
        // Create and insert the close job on the work queue.
        let close_io = create_close_io(
            eid as usize,
            close_id,
            Vec::new(), // deps
            gw_close_id,
            impacted_blocks,
            3,       // flush
            2,       // gen
            0,       // source
            vec![1], // repair
        );

        let mut sub = HashMap::new();
        sub.insert(close_id, 0);

        let (send, recv) = mpsc::channel(1);
        let op = BlockOp::RepairOp;
        let close_br = BlockReq::new(op, send);
        let _close_brw = BlockReqWaiter::new(recv);
        let new_gtos =
            GtoS::new(sub, Vec::new(), None, HashMap::new(), Some(close_br));
        {
            gw.active.insert(gw_close_id, new_gtos);
        }

        ds.enqueue_repair(close_io).await;

        ds.in_progress(close_id, 0);
        ds.in_progress(close_id, 1);
        ds.in_progress(close_id, 2);

        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        // First two are false, final job will return true
        assert!(!ds
            .process_ds_completion(
                close_id,
                0,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei)
            )
            .unwrap());

        // Verify the extent information has been added to the repair info
        // hashmap for client 0
        let new_ei = ds.repair_info.get(&0).unwrap();
        assert_eq!(new_ei.generation, 5);
        assert_eq!(new_ei.flush_number, 3);
        assert!(!new_ei.dirty);
        assert!(ds.repair_info.remove(&1).is_none());
        assert!(ds.repair_info.remove(&2).is_none());

        // Verify the extent information has been added to the repair info
        // hashmap for client 1
        let ei = ExtentInfo {
            generation: 2,
            flush_number: 4,
            dirty: true,
        };
        assert!(!ds
            .process_ds_completion(
                close_id,
                1,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei)
            )
            .unwrap());
        let new_ei = ds.repair_info.get(&1).unwrap();
        assert_eq!(new_ei.generation, 2);
        assert_eq!(new_ei.flush_number, 4);
        assert!(new_ei.dirty);
        assert!(ds.repair_info.remove(&2).is_none());

        let ei = ExtentInfo {
            generation: 29,
            flush_number: 444,
            dirty: false,
        };
        assert!(ds
            .process_ds_completion(
                close_id,
                2,
                Ok(vec![]),
                &None,
                UpState::Active,
                Some(ei)
            )
            .unwrap());
        let new_ei = ds.repair_info.get(&2).unwrap();
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

        for source in 0..3 {
            let ei = ExtentInfo {
                generation: 5,
                flush_number: 3,
                dirty: false,
            };
            assert!(ds.repair_info.insert(0, ei.clone()).is_none());
            assert!(ds.repair_info.insert(1, ei.clone()).is_none());
            assert!(ds.repair_info.insert(2, ei.clone()).is_none());

            let repair_extent = if source == 0 {
                vec![1, 2]
            } else if source == 1 {
                vec![0, 2]
            } else {
                vec![0, 1]
            };

            let repair_op = repair_or_noop(
                &mut ds,
                0,      // Extent
                1000,   // ds_id
                vec![], // Dependencies
                1,      // gw_id
                ImpactedBlocks::Empty,
                source,        // Source extent
                repair_extent, // Repair extent
            );

            println!("repair op: {:?}", repair_op);
            match repair_op.work {
                IOop::ExtentLiveNoOp { dependencies: _ } => {}
                x => {
                    panic!("Incorrect work type returned: {:?}", x);
                }
            }
            assert_eq!(repair_op.ds_id, 1000);
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
    fn what_needs_repair(ds: &mut Downstairs, source: u8, repair: Vec<u8>) {
        let repair_op = repair_or_noop(
            ds,
            0,      // Extent
            1000,   // ds_id
            vec![], // Dependencies
            1,      // gw_id
            ImpactedBlocks::Empty,
            source,
            repair.clone(),
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
        assert_eq!(repair_op.ds_id, 1000);
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
        for source in 0..3 {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // First try one source, one repair
            let repair = if source == 0 {
                assert!(ds.repair_info.insert(0, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, good_ei.clone()).is_none());
                vec![1]
            } else {
                assert!(ds.repair_info.insert(0, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, good_ei.clone()).is_none());
                vec![0]
            };

            println!("Testing repair with s:{} r:{:?}", source, repair);
            what_needs_repair(ds, source, repair);

            // Next try the other downstairs to repair.
            let repair = if source == 2 {
                assert!(ds.repair_info.insert(0, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, good_ei.clone()).is_none());
                vec![1]
            } else {
                assert!(ds.repair_info.insert(0, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, bad_ei.clone()).is_none());
                vec![2]
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
        for source in 0..3 {
            // Change what extent_info we set depending on which downstairs
            // is the source.
            // One source, two repair
            let repair = if source == 0 {
                assert!(ds.repair_info.insert(0, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, bad_ei.clone()).is_none());
                vec![1, 2]
            } else if source == 1 {
                assert!(ds.repair_info.insert(0, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, good_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, bad_ei.clone()).is_none());
                vec![0, 2]
            } else {
                assert!(ds.repair_info.insert(0, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(1, bad_ei.clone()).is_none());
                assert!(ds.repair_info.insert(2, good_ei.clone()).is_none());
                vec![0, 1]
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
    async fn test_online_repair_enqueue_reopen() {
        // Make sure the create_and_enqueue_reopen_io() function does
        // what we expect it to do, which also tests create_reopen_io()
        // function as well.
        let up = create_test_upstairs(1).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let ddef = up.ddef.lock().await.get_def().unwrap();
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_r_id: u64 = 1;
        let r_id = 1002;
        let deps = vec![1000, 1001];

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)j
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        let _reopen_brw = create_and_enqueue_reopen_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            r_id,
            gw_r_id,
            impacted_blocks,
        )
        .await;

        let job = ds.ds_active.get_mut(&r_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveReopen {
                dependencies: d,
                extent: e,
            } => {
                assert_eq!(*d, vec![1000, 1001]);
                assert_eq!(*e, eid as usize);
            }
            x => {
                panic!("Bad DownstairsIO type returned: {:?}", x);
            }
        }
        for cid in 0..3 {
            assert_eq!(job.state[&cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_online_repair_enqueue_close() {
        // Make sure the create_and_enqueue_close_io() function does
        // what we expect it to do, which also tests create_close_io()
        // function as well.
        let up = create_test_upstairs(1).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let ddef = up.ddef.lock().await.get_def().unwrap();
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = 1;
        let close_id = 1002;
        let deps = vec![1000, 1001];

        // create close/fclose jobs first.
        // create the reopen job second (but use final ID)j
        // create final new_gtos job, but populate ids needed without
        // actually enqueue'ing the middle repair job.

        let next_flush = 2;
        let gen = 4;
        let source = 1;
        let repair = vec![0, 2];
        let _reopen_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            next_flush,
            gen,
            deps,
            close_id,
            gw_close_id,
            impacted_blocks,
            source,
            repair.clone(),
        )
        .await;

        let job = ds.ds_active.get_mut(&close_id).unwrap();

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
                assert_eq!(*dependencies, vec![1000, 1001]);
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
        for cid in 0..3 {
            assert_eq!(job.state[&cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_online_repair_enqueue_repair_noop() {
        // Make sure the create_and_enqueue_repair_io() function does
        // what we expect it to do, which also tests create_repair_io()
        // function as well.  In this case we expect the job created to
        // be a no-op job.
        let up = create_test_upstairs(1).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        let eid = 1;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let ddef = up.ddef.lock().await.get_def().unwrap();
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_repair_id: u64 = gw.next_gw_id();
        let repair_id = 1003;
        let deps = vec![1001, 1002];

        let source = 0;
        let repair = vec![1, 2];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        for cid in 0..3 {
            ds.repair_info.insert(cid, ei.clone());
        }

        let _repair_brw = create_and_enqueue_repair_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            repair_id,
            gw_repair_id,
            impacted_blocks,
            source,
            repair.clone(),
        )
        .await;

        let job = ds.ds_active.get_mut(&repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveNoOp { dependencies } => {
                assert_eq!(*dependencies, vec![1001, 1002]);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveNoOp, got: {:?}",
                    x
                );
            }
        }
        for cid in 0..3 {
            assert_eq!(job.state[&cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    #[tokio::test]
    async fn test_online_repair_enqueue_repair_repair() {
        // Make sure the create_and_enqueue_repair_io() function does
        // what we expect it to do, which also tests create_repair_io()
        // function as well.
        let up = create_test_upstairs(1).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let ddef = up.ddef.lock().await.get_def().unwrap();

        let eid = 1;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_repair_id: u64 = gw.next_gw_id();
        let repair_id = 1003;
        let deps = vec![1001, 1002];
        let source = 0;
        let repair = vec![1, 2];

        // To allow the repair to work, we fake the return data from a
        // close operation so it has something to work with.
        let ei = ExtentInfo {
            generation: 5,
            flush_number: 3,
            dirty: false,
        };
        ds.repair_info.insert(0, ei.clone());
        ds.repair_info.insert(1, ei.clone());
        let bad_ei = ExtentInfo {
            generation: 5,
            flush_number: 2,
            dirty: false,
        };
        ds.repair_info.insert(2, bad_ei.clone());
        // We also need a fake repair address
        for cid in 0..3 {
            ds.ds_repair.insert(cid, "127.0.0.1:1234".parse().unwrap());
        }

        let _reopen_brw = create_and_enqueue_repair_io(
            &mut ds,
            &mut gw,
            eid,
            deps,
            repair_id,
            gw_repair_id,
            impacted_blocks,
            source,
            repair.clone(),
        )
        .await;

        let job = ds.ds_active.get_mut(&repair_id).unwrap();

        println!("Job is {:?}", job);
        match &job.work {
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs,
                source_repair_address,
                repair_downstairs,
            } => {
                assert_eq!(*dependencies, vec![1001, 1002]);
                assert_eq!(*extent, eid as usize);
                assert_eq!(*source_downstairs, source);
                assert_eq!(
                    *source_repair_address,
                    "127.0.0.1:1234".parse().unwrap()
                );
                // We are only repairing 2 because it had the different
                // ExtentInfo.  0 and 1 were the same.
                assert_eq!(*repair_downstairs, vec![2]);
            }
            x => {
                panic!(
                    "Bad DownstairsIO Expecting. ExtentLiveRepair, got: {:?}",
                    x
                );
            }
        }
        for cid in 0..3 {
            assert_eq!(job.state[&cid], IOState::New);
        }
        assert_eq!(job.ack_status, AckStatus::NotAcked);
        assert!(!job.replay);
        assert!(job.data.is_none());
    }

    // A test function that will Create and then enqueue a close
    // operation on the given extent.
    // This gets the required locks, determines the next job IDs,
    // Creates the dependencies, and enqueues the job.
    //
    // Note that this test function will call get_repair_ids()
    // which will allocate four job IDs (and take the first one for
    // itself).
    async fn create_and_enqueue_close_op(up: &Arc<Upstairs>, eid: u32) {
        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let ddef = up.ddef.lock().await.get_def().unwrap();
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = gw.next_gw_id();
        let close_id = ds.next_id();
        let deps = deps_for_online_repair(&ds, impacted_blocks, close_id);

        // let repair = vec![0, 2];
        let _reopen_brw = create_and_enqueue_close_io(
            &mut ds,
            &mut gw,
            eid,
            2, // next_flush
            4, // gen
            deps,
            close_id,
            gw_close_id,
            impacted_blocks,
            0,       // source downstairs
            vec![1], // repair downstairs
        )
        .await;
        drop(gw);
        drop(ds);
    }

    // A test function that will Create and then enqueue a repair
    // operation on the given extent.
    // This gets the required locks, determines the next job IDs,
    // Creates the dependencies, and enqueues the job.
    // Note that the repair operation can be either an actual repair,
    // or a NoOp, and we are just hard coding the NoOp as the
    // choice here.  If you wish to test that decision, look elsewhere.
    async fn create_and_enqueue_repair_op(up: &Arc<Upstairs>, eid: u32) {
        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        let ddef = up.ddef.lock().await.get_def().unwrap();

        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        let gw_repair_id: u64 = gw.next_gw_id();
        let extent_repair_ids = ds.get_repair_ids(eid);
        let repair_id = extent_repair_ids.repair_id;
        let deps = deps_for_online_repair(&ds, impacted_blocks, repair_id);

        let _repair_brw = create_and_enqueue_noop_io(
            &mut ds,
            &mut gw,
            deps,
            repair_id,
            gw_repair_id,
            impacted_blocks,
        )
        .await;
        drop(gw);
        drop(ds);
    }

    // The next section of tests verify dependencies are honored.

    // W is Write
    // R is read
    // F is flush
    // Rp is a Repair

    #[tokio::test]
    async fn test_online_repair_deps_writes() {
        // Test that writes on different blocks in the extent are all
        // captured by the repair at the end.
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | W     |
        //   1 |   W   |
        //   2 |     W |
        //   3 | RpRpRp| 0,1,2

        let up = create_test_upstairs(1).await;
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
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        // 4 Jobs were created
        assert_eq!(jobs.len(), 4);

        // The three writes don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(
            jobs[3].work.deps(),
            &[jobs[2].ds_id, jobs[1].ds_id, jobs[0].ds_id]
        );
    }

    #[tokio::test]
    async fn test_online_repair_deps_reads() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   R   |
        //   2 |     R |
        //   3 | RpRpRp| 0,1,2

        let up = create_test_upstairs(1).await;
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
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        // 4 Jobs were created
        assert_eq!(jobs.len(), 4);

        // The three reads don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
        // The repair will have all the previous jobs, as they all are on the
        // same extent.
        assert_eq!(
            jobs[3].work.deps(),
            &[jobs[2].ds_id, jobs[1].ds_id, jobs[0].ds_id]
        );
    }

    #[tokio::test]
    async fn test_online_repair_deps_mix() {
        // Test that the following job dependency graph is made:
        //
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | R     |
        //   1 |   W   |
        //   2 | F F F | 1
        //   3 |RpRpRp | 2

        let up = create_test_upstairs(1).await;
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
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        // 4 Jobs were created
        assert_eq!(jobs.len(), 4);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        // The flush requires the read and the write
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id, jobs[0].ds_id]);
        // The repair will have just the flush
        assert_eq!(jobs[3].work.deps(), &[jobs[2].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_write() {
        // Write after repair depends on the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 |     W | 0

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());

        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

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
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        // 2 Jobs were created
        assert_eq!(jobs.len(), 2);

        // The three writes don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_read() {
        // Read after repair requires the repair
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | R     | 0
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

        up.submit_read(
            Block::new_512(0),
            Buffer::new(512),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        // The read depends on the repair
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_flush() {
        // Flush after repair requires the flush
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | F F F | 0
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        // Repair IO functions assume you have the locks already
        let mut ds = up.downstairs.lock().await;
        let eid = 0;
        ds.extent_limit[1] = Some(eid.try_into().unwrap());
        drop(ds);

        create_and_enqueue_close_op(&up, eid).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        // The flush depends on the repair
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_no_overlap() {
        // No overlap, no deps, IO before repair
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |     W |       |       |
        //   1 |       |       | R     |
        //   2 |       | RpRpRp|       |
        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_online_repair_deps_after_no_overlap() {
        // No overlap no deps IO after repair.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       | RpRpRp|       |
        //   1 |       |       |   R   |
        //   2 |  W    |       |       |
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_op(&up, 1).await;

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
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        // The read and the write don't depend on anything
        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_online_repair_deps_flush_repair_flush() {
        // Flush Repair Flush
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | F F F | F F F |
        //   1 |       | RpRpRp| 0
        //   2 | F F F | F F F | 0,1
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        create_and_enqueue_repair_op(&up, 1).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id, jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_flush_repair() {
        // Repair Flush Repair
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | RpRpRp|       |
        //   1 | F F F | F F F | 0
        //   2 |       | RpRpRp| 1
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_op(&up, 0).await;

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_repair_repair() {
        // Repair Repair Repair
        // This is not an expected situation, but does verify that
        // any new repair operation will be dependent on existing
        // repair operations on that same extent.
        //       block
        // op# | 0 1 2 | deps
        // ----|-------|-----
        //   0 | RpRpRp|
        //   1 | RpRpRp| 1
        //   2 | RpRpRp| 0,1

        let up = create_test_upstairs(1).await;
        create_and_enqueue_repair_op(&up, 0).await;
        create_and_enqueue_repair_op(&up, 0).await;
        create_and_enqueue_repair_op(&up, 0).await;

        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id, jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_wspan_left() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   W W | W     |
        //   1 |       | RpRpRp| 0

        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_wspan_right() {
        // A repair will depend on a write spanning the extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   1 | RpRpRp|       | 0
        //
        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 0).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_rspan_left() {
        // A repair will depend on a read spanning the extent

        // Read spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |   R R | R     |
        //   1 |       | RpRpRp| 0

        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 1).await;
        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_rspan_right() {
        // A repair will depend on a read spanning the extent
        // Read spans other extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     R | R R   |
        //   1 | RpRpRp|       | 0

        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 0).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_other() {
        // A write can be depended on by two different repairs, who won't
        // depend on each other.
        // This situation does not really exist, as a repair won't start
        // until the previous repair finishes.
        //       block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W     |
        //   1 | RpRpRp|       | 0
        //   2 |       | RpRpRp| 0

        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 0).await;
        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
        assert_eq!(jobs[2].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_super_spanner() {
        // Super spanner
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R R R | R R R |
        //   1 |       | RpRpRp|       | 0

        let up = create_test_upstairs(1).await;
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

        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_wafter() {
        // Write after repair spans extent.
        // This write needs to include a future repair that
        // does not exist yet.
        // The write below will update both extent 0 (blocks 0,1,2)
        // and extent 1 (blocks 3,4,5).  Since the write depends on
        // the repair for extent 0, but also will change extent 1, we
        // have to hold this write until both extents are repaired.
        // To do this, the write operation needs to be aware that it
        // is writing to an extent that is under repair, but not repaired
        // yet, and include that future repair in it's dependency list.
        //
        // The IO starts like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | Rclose|       |
        //   1 | Rrep  |       |
        //   2 | Rnoop |       |
        //   3 | Ropen |       |
        //   1 |     W | W W   | 0, R?
        //
        // However, once the write notices the repair in progress, it will
        // create space for the three future repair jobs that will make
        // the dep list look like this:
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 | Rclose|       |
        //   1 | Rrep  |       |
        //   2 | Rnoop |       |
        //   3 | Ropen |       |
        //   4 |       | Rclose|
        //   5 |       | Rrep  |
        //   6 |       | Rnoop |
        //   7 |       | Ropen |
        //   5 |     W | W W   | 0,1,2,3
        //
        // We also verify that the job IDs make sense for our repair id
        // reservation that happens when we need to insert a job like this.

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_op(&up, 0).await;

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

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);
        assert!(jobs[0].work.deps().is_empty());

        // We know our first repair job will reserve 4 ids: 1000, 1001,
        // 1002, and 1003. The write to a spanning extent will go
        // and create 4 more job IDs. All of these would end up on our
        // writes dependency list, but, because this is test code and
        // it's only actually putting the "repair" job on the work queue,
        // we end up with just that job (1001) from the first four on the
        // dependency list for our write.

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].ds_id, 1008);
        assert_eq!(jobs[1].work.deps(), &[1001, 1004, 1005, 1006, 1007]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_rafter() {
        // Read after spans extent
        //       block   block
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |       | RpRpRp|
        //   1 |   R R | R     | 0

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        create_and_enqueue_repair_op(&up, 1).await;

        up.submit_read(
            Block::new_512(1),
            Buffer::new(512 * 3),
            None,
            ds_done_tx.clone(),
        )
        .await
        .unwrap();

        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        assert!(jobs[0].work.deps().is_empty());
        assert_eq!(jobs[1].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_overlappers() {
        // IOs that span both sides.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 | R R R | R     |       |
        //   1 |       |     W | W W W |
        //   2 |       | RpRpRp|       | 0,1
        //
        let up = create_test_upstairs(1).await;
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
        create_and_enqueue_repair_op(&up, 1).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert_eq!(jobs[2].work.deps(), &[jobs[1].ds_id, jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_deps_repair_kitchen_sink() {
        // Repair simulator
        // In truth, you would never have more than one repair out at
        // the same time (the way it is now) but from a pure dependency point
        // of view, there is no reason you could not.
        //       block   block   block
        // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
        // ----|-------|-------|-------|-----
        //   0 |       |       |   W   |
        //   1 | RpRpRp|       |       |
        //   2 |       |   W   |       |
        //   3 |     R |       |       | 1
        //   4 |       | RpRpRp|       | 2
        //   5 |       |     W |       | 4
        //   6 |       |       | RpRpRp| 0
        let up = create_test_upstairs(1).await;
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
        create_and_enqueue_repair_op(&up, 0).await;

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
        create_and_enqueue_repair_op(&up, 1).await;

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
        create_and_enqueue_repair_op(&up, 2).await;

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 7);

        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert!(jobs[2].work.deps().is_empty());
        assert_eq!(jobs[3].work.deps(), &[jobs[1].ds_id]);
        assert_eq!(jobs[4].work.deps(), &[jobs[2].ds_id]);
        assert_eq!(jobs[5].work.deps(), &[jobs[4].ds_id]);
        assert_eq!(jobs[6].work.deps(), &[jobs[0].ds_id]);
    }

    #[tokio::test]
    async fn test_online_repair_no_repair_yet() {
        // This is a special repair case.  We have a downstairs that is in
        // OnlineRepair, but we have not yet started the actual repair
        // work. IOs that arrive at this point in time should go ahead
        // on the good downstairs client, and still be skipped on the
        // OnlineRepair client
        //
        //     | block | block |
        // op# | 0 1 2 | 3 4 5 | deps
        // ----|-------|-------|-----
        //   0 |     W | W W   |
        //   0 |   R   |       |
        //   0 | F F F | F F F | 0,1

        let up = create_test_upstairs(1).await;
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

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 3);

        assert!(jobs[0].work.deps().is_empty());
        assert!(jobs[1].work.deps().is_empty());
        assert_eq!(jobs[2].work.deps(), &[1001, 1000]);
        // Check that the IOs were skipped on downstairs 1.
        assert_eq!(jobs[0].state[&1], IOState::Skipped);
        assert_eq!(jobs[1].state[&1], IOState::Skipped);
        assert_eq!(jobs[2].state[&1], IOState::Skipped);
    }

    #[tokio::test]
    async fn test_online_repair_repair_write_push() {
        // This is a special repair case.  We have a downstairs that is in
        // OnlineRepair, and we have indicated that this extent is
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
        //   4 |     W | W W   | 0,1,2

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(0);
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

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 1);

        assert_eq!(jobs[0].ds_id, 1004);
        assert_eq!(jobs[0].work.deps(), &[1000, 1001, 1002, 1003]);
    }

    #[tokio::test]
    async fn test_online_repair_repair_read_push() {
        // This is a special repair case.  We have a downstairs that is in
        // OnlineRepair, and we have indicated that this extent is
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
        //   1 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   2 |       | RpRpRp|
        //   3 |     R | R R   | 0,1,2

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(0);
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
        create_and_enqueue_repair_op(&up, 1).await;
        let ds = up.downstairs.lock().await;

        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 2);

        // The repair op will be the 2nd job ID reserved.
        assert_eq!(jobs[0].ds_id, 1001);
        // The repair job should have no dependencies.
        assert!(jobs[0].work.deps().is_empty());
        // Our read should be ID 1004, and have the four repair jobs
        // for dependencies
        assert_eq!(jobs[1].ds_id, 1004);
        assert_eq!(jobs[1].work.deps(), &[1000, 1001, 1002, 1003]);
    }

    #[tokio::test]
    async fn test_online_repair_flush_is_flush() {
        // This is a special repair case.  We have a downstairs that is in
        // OnlineRepair, and we have indicated that this extent is
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

        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(0);
        drop(ds);

        up.submit_flush(None, None, ds_done_tx.clone())
            .await
            .unwrap();

        let ds = up.downstairs.lock().await;
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].work.deps().is_empty());
    }

    #[tokio::test]
    async fn test_online_repair_send_io_write_below() {
        // Verify that we will send a write during OnlineRepair when
        // the IO is an extent that is already repaired.
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
        drop(ds);

        let job_id = 1000;
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
        assert!(ds.in_progress(job_id, 0).is_some());
        assert!(ds.in_progress(job_id, 1).is_some());
        assert!(ds.in_progress(job_id, 2).is_some());
    }

    // Test to verify that reserving job IDs for future repair work
    // is indeed used by the repair when it arrives to perform it.
    //
    // Also, what happens to the repair job IDs after the repair
    // has completed?  Flush needed to remove them...

    // Maybe make the cleanup remove them?

    // Deps don't see past previous repairs
    // R W Repair R W Repair R W
    //       block
    // op# | 0 1 2 | deps
    // ----|-------|-----
    //   0 | R     |
    //   1 |     W |
    //   2 | RpRpRp| 0,1
    //   3 | R     | 2
    //   4 |     W | 2

    // Blanks
    // op# | 0 1 2 | deps
    // ----|-------|-----
    //   0 |       |
    //   1 |       |
    //   2 |       |

    //       block   block   block
    // op# | 0 1 2 | 3 4 5 | 6 7 8 | deps
    // ----|-------|-------|-------|-----
    //   0 |       |       |       |
    //   1 |       |       |       |
    //   2 |       |       |       |
    //   3 |       |       |       |
    //   4 |       |       |       |
    //   5 |       |       |       |
    //   6 |       |       |       |
    //   7 |       |       |       |

    // Test function to put some work on the work queues.
    async fn submit_three_ios(
        up: &Arc<Upstairs>,
        ds_done_tx: &mpsc::Sender<u64>,
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
        // Starting with one downstairs in OnlineRepair state, the functions
        // will:
        // Move the OnlineRepair downstairs to Faulted.
        // Move all IO for that downstairs to skipped.
        // Clear the extent_limit setting for that downstairs.
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0;
        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(eid);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        up.abort_repair_ds(&mut ds, UpState::Active);
        up.abort_repair_extent(&mut gw, &mut ds, eid as u32)
            .await
            .unwrap();

        assert_eq!(ds.ds_state[0], DsState::Active);
        assert_eq!(ds.ds_state[1], DsState::Faulted);
        assert_eq!(ds.ds_state[0], DsState::Active);

        // Check all three IOs again, downstairs 1 will be skipped..
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        for job in jobs.iter().take(3) {
            assert_eq!(job.state[&0], IOState::New);
            assert_eq!(job.state[&1], IOState::Skipped);
            assert_eq!(job.state[&2], IOState::New);
        }
        assert_eq!(ds.extent_limit[1], None);
    }

    #[tokio::test]
    async fn test_repair_abort_reserved_jobs() {
        // Testing of abort_repair functions.
        // Starting with one downstairs in OnlineRepair state and future
        // repair job IDs reserved (but not created yet). The functions
        // will verify that four noop repair jobs will be queued.
        let up = create_test_upstairs(1).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0;
        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(eid);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        // Reserve some repair IDs
        let reserved_ids = ds.reserve_repair_ids(eid as u32);

        up.abort_repair_ds(&mut ds, UpState::Active);
        up.abort_repair_extent(&mut gw, &mut ds, eid as u32)
            .await
            .unwrap();

        // Check all three IOs again, downstairs 1 will be skipped..
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        assert_eq!(jobs.len(), 7);
        for job in jobs.iter().take(7) {
            assert_eq!(job.state[&0], IOState::New);
            assert_eq!(job.state[&1], IOState::Skipped);
            assert_eq!(job.state[&2], IOState::New);
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
        let up = create_test_upstairs(1).await;
        up.ds_transition(0, DsState::Faulted).await;
        up.ds_transition(2, DsState::Faulted).await;
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);

        let eid = 0;
        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[0] = Some(eid);
        ds.extent_limit[1] = Some(eid);
        ds.extent_limit[2] = Some(eid);
        drop(ds);

        submit_three_ios(&up, &ds_done_tx).await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;

        // Reserve some repair IDs
        let _reserved_ids = ds.reserve_repair_ids(eid as u32);

        up.abort_repair_ds(&mut ds, UpState::Active);
        up.abort_repair_extent(&mut gw, &mut ds, eid as u32)
            .await
            .unwrap();

        // Check all three IOs again, all downstairs will be skipped..
        let keys: Vec<&u64> = ds.ds_active.keys().sorted().collect();
        let jobs: Vec<&DownstairsIO> =
            keys.iter().map(|k| ds.ds_active.get(k).unwrap()).collect();

        for job in jobs.iter().take(3) {
            assert_eq!(job.state[&0], IOState::Skipped);
            assert_eq!(job.state[&1], IOState::Skipped);
            assert_eq!(job.state[&2], IOState::Skipped);
        }

        // No repair jobs should be submitted
        assert_eq!(jobs.len(), 3);
        assert!(!ds.query_repair_ids(eid as u32));
    }

    async fn test_upstairs_okay() -> Arc<Upstairs> {
        // Build an upstairs without a faulted downstairs.
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(3));
        ddef.set_extent_count(4);

        let up = Upstairs::test_default(Some(ddef));

        for cid in 0..3 {
            up.ds_transition(cid, DsState::WaitActive).await;
            let dsr = RegionMetadata {
                generation: vec![1, 1, 1, 1],
                flush_numbers: vec![2, 2, 2, 2],
                dirty: vec![false, false, false, false],
            };
            up.downstairs.lock().await.region_metadata.insert(cid, dsr);
            up.ds_transition(cid, DsState::WaitQuorum).await;
            up.ds_transition(cid, DsState::Active).await;
        }

        up.set_active().await.unwrap();
        up
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_done() {
        // Verify that a downstairs in OnlineRepair state will have its
        // dependency list altered to reflect both the removal of skipped
        // jobs as well as removal of Done jobs that happened before the
        // downstairs went to OnlineRepair.

        let up = test_upstairs_okay().await;
        // Channels we want to appear to be working
        let (ds_done_tx, _ds_done_rx) = mpsc::channel(500);
        // Now, send some IOs.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        // Manually move all these jobs to done.
        for job_id in 1000..1003 {
            let job = ds.ds_active.get_mut(&job_id).unwrap();
            for cid in 0..3 {
                job.state.insert(cid, IOState::Done);
            }
        }
        drop(ds);

        // Fault the downstairs
        up.ds_transition(1, DsState::Faulted).await;
        up.ds_transition(1, DsState::OnlineRepairReady).await;
        up.ds_transition(1, DsState::OnlineRepair).await;

        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);
        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        show_all_work(&up).await;
        let mut ds = up.downstairs.lock().await;
        // Good downstairs don't need changes
        assert!(!ds.dependencies_need_cleanup(0));
        assert!(!ds.dependencies_need_cleanup(2));

        // OnlineRepair downstairs might need a change
        assert!(ds.dependencies_need_cleanup(1));
        for job_id in 1003..1006 {
            let job = ds.ds_active.get_mut(&job_id).unwrap();
            // jobs 3,4,5 will be skipped for our OnlineRepair downstairs.
            assert_eq!(job.state[&0], IOState::New);
            assert_eq!(job.state[&1], IOState::Skipped);
            assert_eq!(job.state[&2], IOState::New);
        }

        // Walk the three new jobs, verify that the dependencies will be
        // updated for our downstairs under repair.
        // Make the empty dep list for comparison
        let cv: Vec<u64> = vec![];

        let job = ds.ds_active.get_mut(&1003).unwrap();
        let current_deps = job.work.deps().to_vec();

        assert_eq!(&current_deps, &[1002, 1001, 1000]);
        // No dependencies are valid for online repair
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, cv);

        let job = ds.ds_active.get_mut(&1004).unwrap();
        let current_deps = job.work.deps().to_vec();

        // Job 1001 is not a dep for 1004
        assert_eq!(&current_deps, &[1003, 1002, 1000]);
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, cv);

        let job = ds.ds_active.get_mut(&1005).unwrap();
        let current_deps = job.work.deps().to_vec();

        assert_eq!(&current_deps, &[1004, 1003, 1002, 1001, 1000]);
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, cv);
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_some() {
        // Verify that a downstairs in OnlineRepair state will have its
        // dependency list altered to reflect both the removal of skipped
        // jobs as well as removal of Done jobs that happened before the
        // downstairs went to OnlineRepair, and also, won't remove jobs that
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
        for job_id in 1000..1003 {
            let job = ds.ds_active.get_mut(&job_id).unwrap();
            for cid in 0..3 {
                job.state.insert(cid, IOState::Done);
            }
        }

        drop(ds);
        // Fault the downstairs
        up.ds_transition(1, DsState::Faulted).await;
        up.ds_transition(1, DsState::OnlineRepairReady).await;
        up.ds_transition(1, DsState::OnlineRepair).await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        ds.extent_limit[1] = Some(1);
        drop(ds);

        // New jobs will go -> Skipped for the downstairs in repair.
        submit_three_ios(&up, &ds_done_tx).await;

        let mut ds = up.downstairs.lock().await;
        // Good downstairs don't need changes
        assert!(!ds.dependencies_need_cleanup(0));
        assert!(!ds.dependencies_need_cleanup(2));

        // OnlineRepair downstairs might need a change
        assert!(ds.dependencies_need_cleanup(1));

        // For the three latest jobs, they should be New as they are IOs that
        // are on an extent we "already repaired".
        for job_id in 1006..1009 {
            let job = ds.ds_active.get_mut(&job_id).unwrap();
            assert_eq!(job.state[&0], IOState::New);
            assert_eq!(job.state[&1], IOState::New);
            assert_eq!(job.state[&2], IOState::New);
        }

        // Walk the three final jobs, verify that the dependencies will be
        // updated for our downstairs under repair, but will still include
        // the jobs that came after the repair.
        let cv: Vec<u64> = vec![];

        let job = ds.ds_active.get_mut(&1006).unwrap();
        let current_deps = job.work.deps().to_vec();

        assert_eq!(&current_deps, &[1005, 1004, 1003, 1002, 1001, 1000]);
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, cv);

        let job = ds.ds_active.get_mut(&1007).unwrap();
        let current_deps = job.work.deps().to_vec();

        assert_eq!(&current_deps, &[1006, 1005, 1003, 1002, 1000]);
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, &[1006]);

        let job = ds.ds_active.get_mut(&1008).unwrap();
        let current_deps = job.work.deps().to_vec();

        assert_eq!(
            &current_deps,
            &[1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000]
        );
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 0);
        assert_eq!(new_deps, &[1007, 1006]);
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_repair() {
        // Verify that a downstairs in OnlineRepair state will have its
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
        for job_id in 1000..1003 {
            let job = ds.ds_active.get_mut(&job_id).unwrap();
            for cid in 0..3 {
                job.state.insert(cid, IOState::Done);
            }
        }

        drop(ds);

        // Fault the downstairs
        up.ds_transition(1, DsState::Faulted).await;
        up.ds_transition(1, DsState::OnlineRepairReady).await;
        up.ds_transition(1, DsState::OnlineRepair).await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        drop(ds);

        // Put a repair job on the queue.
        create_and_enqueue_repair_op(&up, 0).await;

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
        // dependencies will be on the OnlineRepair downstairs.  On active,
        // it should require jobs 1 and 2. With the OnlineRepair downstairs,
        // it will not depend on anything. This is okay, because the job
        // itself is Skipped there, so we won't actually send it.

        // Empty vec to compare with and keep the compiler happy.
        let empty: Vec<u64> = vec![];

        let mut ds = up.downstairs.lock().await;
        let job = ds.ds_active.get_mut(&1007).unwrap();
        assert_eq!(job.state[&0], IOState::New);
        assert_eq!(job.state[&1], IOState::Skipped);
        assert_eq!(job.state[&2], IOState::New);

        let current_deps = job.work.deps().to_vec();
        assert_eq!(&current_deps, &[1002, 1001, 1000]);

        // Verify that the Skipped job on the OnlineRepair downstairs do not
        // have any dependencies, as, technically, this IO is the first IO to
        // happen after we started repair, and we should not look for any
        // dependencies before starting repair.
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 1007);
        assert_eq!(new_deps, empty);

        // This second write after starting a repair should require jobs 0,4
        // on Active downstairs, and only require the repair on the
        // OnlineRepair downstairs.
        let job = ds.ds_active.get_mut(&1008).unwrap();
        assert_eq!(job.state[&0], IOState::New);
        assert_eq!(job.state[&1], IOState::New);
        assert_eq!(job.state[&2], IOState::New);

        let current_deps = job.work.deps().to_vec();
        assert_eq!(&current_deps, &[1004, 1000]);

        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 1008);
        // OnlineRepair downstairs won't see past the repair.
        assert_eq!(new_deps, &[1004]);

        // This final job depends on everything on Active downstairs, but
        // a smaller subset for the OnlineRepair downstairs
        let job = ds.ds_active.get_mut(&1013).unwrap();
        assert_eq!(job.state[&0], IOState::New);
        assert_eq!(job.state[&1], IOState::New);
        assert_eq!(job.state[&2], IOState::New);

        // All the current operations, plus four future repair operations
        // that don't exist yet.
        let current_deps = job.work.deps().to_vec();
        assert_eq!(
            &current_deps,
            &[1008, 1007, 1004, 1002, 1001, 1000, 1009, 1010, 1011, 1012]
        );

        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 1013);
        // OnlineRepair downstairs won't see past the repair, and it won't
        // include the skipped IO at 1007, but will have the future repair
        // operations that don't yet exist.
        assert_eq!(new_deps, &[1008, 1004, 1009, 1010, 1011, 1012]);
    }

    #[tokio::test]
    async fn test_repair_dep_cleanup_sk_repair() {
        // Verify that a downstairs in OnlineRepair state will have its
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
        //                       Online Repair starts here
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
        let job = ds.ds_active.get_mut(&1000).unwrap();
        for cid in 0..3 {
            job.state.insert(cid, IOState::Done);
        }

        drop(ds);
        let eid = 0;
        let ddef = up.ddef.lock().await.get_def().unwrap();
        let impacted_blocks = extent_to_impacted_blocks(&ddef, eid);

        // Fault the downstairs
        up.ds_transition(1, DsState::Faulted).await;
        up.ds_transition(1, DsState::OnlineRepairReady).await;
        up.ds_transition(1, DsState::OnlineRepair).await;

        // Simulate what happens when we first start repair
        // on extent 0
        let next_flush = up.next_flush_id().await;
        let gen = up.get_generation().await;

        let mut gw = up.guest.guest_work.lock().await;
        let mut ds = up.downstairs.lock().await;
        ds.repair_min_id = Some(ds.peek_next_id());
        ds.extent_limit[1] = Some(eid as usize);

        // Upstairs "guest" work IDs.
        let gw_close_id: u64 = gw.next_gw_id();
        let gw_reopen_id: u64 = gw.next_gw_id();

        // The work IDs for the downstairs side of things.
        let extent_repair_ids = ds.get_repair_ids(eid);
        let close_id = extent_repair_ids.close_id;
        let repair_id = extent_repair_ids.repair_id;
        let noop_id = extent_repair_ids.noop_id;
        let reopen_id = extent_repair_ids.reopen_id;

        let mut deps = deps_for_online_repair(&ds, impacted_blocks, close_id);

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
            impacted_blocks,
        )
        .await;

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
            impacted_blocks,
            0,
            vec![1],
        )
        .await;

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
        let job = ds.ds_active.get_mut(&1004).unwrap();

        let current_deps = job.work.deps().to_vec();
        // We start with repair jobs, plus the original jobs.
        assert_eq!(&current_deps, &[1000, 1001, 1002, 1003]);

        // The downstairs in OnlineRepair should not see the first write, but
        // should see all the repair IDs, including ones that don't actually
        // exist yet.
        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 1004);
        assert_eq!(new_deps, &[1001, 1002, 1003]);

        // This second write after starting a repair should require jobs 0,1,4
        // on Active downstairs, and only require the repair on the
        // OnlineRepair downstairs.
        let job = ds.ds_active.get_mut(&1005).unwrap();
        assert_eq!(job.state[&0], IOState::New);
        assert_eq!(job.state[&1], IOState::New);
        assert_eq!(job.state[&2], IOState::New);

        let current_deps = job.work.deps().to_vec();
        assert_eq!(&current_deps, &[1004, 1001, 1000]);

        let new_deps = ds.remove_dep_if_skipped(1, current_deps, 1005);
        // OnlineRepair downstairs won't see past the repair.
        assert_eq!(new_deps, &[1004, 1001]);
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
}
