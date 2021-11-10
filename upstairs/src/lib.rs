// Copyright 2021 Oxide Computer Company
#![feature(asm)]
#![allow(clippy::mutex_atomic)]

use std::clone::Clone;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Result as IOResult, Seek, SeekFrom, Write};
use std::net::SocketAddrV4;
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

pub use crucible_common::*;
use crucible_protocol::*;

use anyhow::{anyhow, bail, Result};
pub use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use serde::Serialize;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{instrument, span, Level};
use usdt::register_probes;
use uuid::Uuid;

use aes::cipher::generic_array::GenericArray;
use aes::{Aes128, NewBlockCipher};
use xts_mode::{get_tweak_default, Xts128};

mod pseudo_file;
mod test;

pub use pseudo_file::CruciblePseudoFile;

#[usdt::provider]
mod cdt {
    use crate::Arg;
    fn up_status(_: String, arg: Arg) {}
    fn gw_read_start(_: u64) {}
    fn gw_write_start(_: u64) {}
    fn gw_flush_start(_: u64) {}
    fn gw_read_end(_: u64) {}
    fn gw_write_end(_: u64) {}
    fn gw_flush_end(_: u64) {}
}

#[derive(Debug, Clone)]
pub struct CrucibleOpts {
    pub target: Vec<SocketAddrV4>,
    pub lossy: bool,
    pub key: Option<String>,
}

impl CrucibleOpts {
    pub fn key_bytes(&self) -> Option<Vec<u8>> {
        if let Some(key) = &self.key {
            // For xts, key size must be 32 bytes
            let decoded_key =
                base64::decode(key).expect("could not base64 decode key!");

            if decoded_key.len() != 32 {
                panic!("Key length must be 32 bytes!");
            }

            Some(decoded_key)
        } else {
            None
        }
    }
}

pub fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

async fn process_message(
    u: &Arc<Upstairs>,
    m: &Message,
    up_coms: UpComs,
) -> Result<()> {
    match m {
        Message::Imok => Ok(()),
        Message::WriteAck(uuid, ds_id, result) => {
            if u.uuid != *uuid {
                println!(
                    "u.uuid {:?} != job uuid {:?} on WriteAck",
                    u.uuid, *uuid
                );
                return Err(CrucibleError::UuidMismatch.into());
            }

            Ok(io_completed(
                u,
                *ds_id,
                up_coms.client_id,
                None,
                up_coms.ds_done_tx,
                result.clone(),
            )
            .await?)
        }
        Message::FlushAck(uuid, ds_id, result) => {
            if u.uuid != *uuid {
                println!(
                    "u.uuid {:?} != job uuid {:?} on FlushAck",
                    u.uuid, *uuid
                );
                return Err(CrucibleError::UuidMismatch.into());
            }

            Ok(io_completed(
                u,
                *ds_id,
                up_coms.client_id,
                None,
                up_coms.ds_done_tx,
                result.clone(),
            )
            .await?)
        }
        Message::ReadResponse(uuid, ds_id, data, result) => {
            if u.uuid != *uuid {
                println!(
                    "u.uuid {:?} != job uuid {:?} on ReadResponse",
                    u.uuid, *uuid
                );
                return Err(CrucibleError::UuidMismatch.into());
            }

            Ok(io_completed(
                u,
                *ds_id,
                up_coms.client_id,
                Some(data.clone()),
                up_coms.ds_done_tx,
                result.clone(),
            )
            .await?)
        }
        /*
         * For this case, we will (TODO) want to log an error to someone, but
         * I don't think there is anything else we can do.
         */
        x => {
            println!("{} unexpected frame {:?}, IGNORED", up_coms.client_id, x);
            Ok(())
        }
    }
}

/*
 * Convert a virtual block offset and length into a Vec of tuples:
 *
 *     Extent number (EID), Block offset, Length in Blocks
 *
 * - length in blocks can be up to the region size
 */
pub fn extent_from_offset(
    ddef: RegionDefinition,
    offset: Block,
    num_blocks: Block,
) -> Result<Vec<(u64, Block, Block)>> {
    assert!(num_blocks.value > 0);
    assert!(
        (offset.value + num_blocks.value)
            <= (ddef.extent_size().value * ddef.extent_count() as u64)
    );
    assert_eq!(offset.block_size_in_bytes() as u64, ddef.block_size());

    /*
     *
     *  |eid0                  |eid1
     *  |────────────────────────────────────────────>│
     *  ┌──────────────────────|──────────────────────┐
     *  │                      |                      │
     *  └──────────────────────|──────────────────────┘
     *  |offset                                       |offset + len
     */
    let mut result = Vec::new();
    let mut o: u64 = offset.value;
    let mut blocks_left: u64 = num_blocks.value;

    while blocks_left > 0 {
        /*
         * XXX We only support a single region (downstairs). When we grow to
         * support a LBA size that is larger than a single region, then we
         * will need to write more code. But - that code may live
         * upstairs?
         */
        let eid: u64 = o / ddef.extent_size().value;
        assert!((eid as u32) < ddef.extent_count());

        let extent_offset: u64 = o % ddef.extent_size().value;
        let mut sz: u64 = ddef.extent_size().value - extent_offset;
        if blocks_left < sz {
            sz = blocks_left;
        }

        result.push((
            eid,
            Block::new_with_ddef(extent_offset, &ddef),
            Block::new_with_ddef(sz, &ddef),
        ));

        match blocks_left.checked_sub(sz) {
            Some(v) => {
                blocks_left = v;
            }
            None => {
                break;
            }
        }

        o += sz;
    }

    {
        let mut blocks = 0;
        for r in &result {
            blocks += r.2.value;
        }
        assert_eq!(blocks, num_blocks.value);
    }

    Ok(result)
}

/*
 * Decide what to do with a downstairs that has just connected and has
 * sent us information about its extents.
 *
 * XXX At the moment we are doing both wait quorum and verify consistency
 * in the same function. This will soon break out into two separate places
 * where we then decide what to do with each downstairs.
 */
fn process_downstairs(
    target: &SocketAddrV4,
    u: &Arc<Upstairs>,
    gens: Vec<u64>,
    versions: Vec<u64>,
    dirty: Vec<bool>,
) -> Result<()> {
    if versions.len() > 12 {
        println!("{} versions[0..12]: {:?}", target, versions[0..12].to_vec());
        println!("{} gens[0..12]: {:?}", target, gens[0..12].to_vec());
        println!("{} dirty[0..12]: {:?}", target, dirty[0..12].to_vec());
    } else {
        println!("{}  versions: {:?}", target, versions);
        println!("{}  gens: {:?}", target, gens);
        println!("{}  dirty: {:?}", target, dirty);
    }

    let mut fi = u.flush_info.lock().unwrap();
    if fi.flush_numbers.is_empty() {
        /*
         * This is the first version list we have, so
         * we will make it the original and compare
         * whatever comes next.
         * XXX This is not the final way to do it, but works for now.
         */
        fi.flush_numbers = versions;
        fi.next_flush = *fi.flush_numbers.iter().max().unwrap() + 1;
        print!("Set initial Extent versions to");
        if fi.flush_numbers.len() > 12 {
            println!(" [0..12]{:?}", fi.flush_numbers[0..12].to_vec());
        } else {
            println!("{:?}", fi.flush_numbers);
        }
        println!("Next flush: {}", fi.next_flush);
    } else if fi.flush_numbers.len() != versions.len() {
        /*
         * I don't think there is much we can do here, the expected number
         * of flush numbers does not match. Possibly we have grown one but
         * not the rest of the downstairs?
         */
        panic!(
            "Expected downstairs version \
              len:{:?} does not match new \
              downstairs:{:?}",
            fi.flush_numbers.len(),
            versions.len()
        );
    } else {
        /*
         * We already have a list of versions to compare with. Make that
         * comparison now against this new list
         */
        let ver_cmp = fi.flush_numbers.iter().eq(versions.iter());
        if !ver_cmp {
            println!(
                "{} MISMATCH expected: {:?} != new: {:?}",
                target, fi.flush_numbers, versions
            );
            // XXX Recovery process should start here
            println!("{} Ignoring this downstairs version info", target);
        }
    }

    Ok(())
}

/*
 * This function is called when the upstairs task is notified that
 * a downstairs operation has completed. We add the read buffer to the
 * IOop struct for later processing if required.
 */
#[instrument]
async fn io_completed(
    up: &Arc<Upstairs>,
    ds_id: u64,
    client_id: u8,
    data: Option<Bytes>,
    ds_done_tx: mpsc::Sender<u64>,
    result: Result<(), CrucibleError>,
) -> Result<()> {
    if up.complete(ds_id, client_id, data, result)? {
        ds_done_tx.send(ds_id).await?
    }

    Ok(())
}

/*
 * This function is called by a worker task after the main task has added
 * work to the hashmap and notified the worker tasks that new work is ready
 * to be serviced. The worker task will walk the hashmap and build a list
 * of new work that it needs to do. It will then iterate through those
 * work items and send them over the wire to this tasks waiting downstairs.
 *
 * V1 flow control, if we have more than X (where X = 100 for now, as we
 * don't know the best value yet, XXX) jobs submitted that we don't have
 * ACKs for, then stop sending more work and let the receive side catch up.
 * We return true if we have more work to do, false if we are all caught up.
 */
#[instrument(skip(fw))]
async fn io_send(
    u: &Arc<Upstairs>,
    fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
    client_id: u8,
    lossy: bool,
) -> Result<bool> {
    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     *
     * The length of this list (new work for a downstairs) can give us
     * an idea of how that downstairs is doing. If the number of jobs
     * to be submitted is too big (for some value of big) then there is
     * a problem. All sorts of back pressure information can be
     * gathered here. As (for the moment) the same task does both
     * transmit and receive, we can starve the receive side by spending
     * all our time sending work.
     *
     * This XXX is for coming back here and making a better job of
     * flow control.
     */
    let mut new_work = u.downstairs.lock().unwrap().new_work(client_id);

    /*
     * Now we have a list of all the job IDs that are new for our client id.
     * Walk this list and process each job, marking it InProgress as we
     * do the work. We do this in two loops because we can't hold the
     * lock for the hashmap while we do work, and if we release the lock
     * to do work, we would have to start over and look at all jobs in the
     * map to see if they are new.
     *
     * This also allows us to sort the job ids and do them in order they
     * were put into the hashmap, though I don't think that is required.
     */
    new_work.sort_unstable();

    let mut active_count =
        u.downstairs.lock().unwrap().submitted_work(client_id);
    for new_id in new_work.iter() {
        if active_count >= 100 {
            // Flow control enacted, stop sending work
            return Ok(true);
        }
        /*
         * Walk the list of work to do, update its status as in progress
         * and send the details to our downstairs.
         */
        if lossy && random() && random() {
            continue;
        }

        /*
         * If in_progress returns None, it means that this client should
         * be skipped.
         */
        let job = u.downstairs.lock().unwrap().in_progress(*new_id, client_id);
        if job.is_none() {
            continue;
        }

        active_count += 1;
        match job.unwrap() {
            IOop::Write {
                dependencies,
                eid,
                offset,
                data,
            } => {
                fw.send(Message::Write(
                    u.uuid,
                    *new_id,
                    eid,
                    dependencies.clone(),
                    offset,
                    data.clone(),
                ))
                .await?
            }
            IOop::Flush {
                dependencies,
                flush_number,
            } => {
                fw.send(Message::Flush(
                    u.uuid,
                    *new_id,
                    dependencies.clone(),
                    flush_number,
                ))
                .await?
            }
            IOop::Read {
                dependencies,
                eid,
                offset,
                num_blocks,
            } => {
                fw.send(Message::ReadRequest(
                    u.uuid,
                    *new_id,
                    dependencies.clone(),
                    eid,
                    offset,
                    num_blocks,
                ))
                .await?
            }
        }
    }
    Ok(false)
}

/*
 * Once we have a connection to a downstairs, this task takes over and
 * handles the initial negotiation.
 */
async fn proc(
    target: &SocketAddrV4,
    up: &Arc<Upstairs>,
    mut sock: TcpStream,
    connected: &mut bool,
    up_coms: &mut UpComs,
    lossy: bool,
) -> Result<()> {
    let (r, w) = sock.split();
    let mut fr = FramedRead::new(r, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(w, CrucibleEncoder::new());

    up.ds_state_show();
    let my_state = {
        let state = &up.downstairs.lock().unwrap().ds_state;
        state[up_coms.client_id as usize]
    };
    println!("Proc runs for {} in state {:?}", target, my_state);
    if my_state != DsState::New
        && my_state != DsState::Disconnected
        && my_state != DsState::Failed
        && my_state != DsState::Offline
    {
        panic!(
            "[{}] failed proc with state {:?}",
            up_coms.client_id, my_state
        );
    }
    /*
     * As the "client", we must begin the negotiation.
     */
    fw.send(Message::HereIAm(1, up.uuid)).await?;

    /*
     * Used to track where we are in the current negotiation.
     */
    let mut negotiated = 0;

    // XXX figure out what deadlines make sense here
    let mut ping_interval = deadline_secs(5);
    let mut timeout_deadline = deadline_secs(50);

    /*
     * Either we get all the way through the negotiation, or we hit the
     * timeout and exit to retry.
     *
     * XXX There are many ways we can handle this, but as we figure out
     * how the upstairs is notified that a DS is new or moving, or other
     * things, this way will work. We will revisit when we have more info.
     *
     * The negotiation flow starts as follows, with the value of the
     * negotiated variable on the left:
     *
     *          Upstairs             Downstairs
     * 0:          HereIAm(v)  --->
     *                         <---  YesItsMe(v)
     *
     * At this point, a downstairs will wait for a "PromoteToActive" message
     * to be sent to it.  If this is a new upstairs that has not yet
     * connected to a downstairs, then we will wait for the guest to send
     * us this message and pass it down to the downstairs.  If a downstairs
     * is reconnecting after having already been active, then we look at our
     * upstairs is_active() and, if the upstairs is active, we send the
     * downstairs the message ourselves.
     *
     * 1: PromoteToActive(uuid)--->
     *                         <---  YouAreNowActive(uuid)
     *
     * 2:    RegionInfoPlease  --->
     *                         <---  RegionInfo(r)
     *
     *    At this point the upstairs looks to see what state the downstairs
     *    is currently in.  It will be "New", "Disconnected" or "Offline".
     *
     *    For "New" or "Disconnected" it means this downstairs never was
     *    "Active" and we have to go through the full compare of this
     *    downstairs with other downstairs and make sure they are
     *    consistent (This code still TBW).  The New/Disconnected steps
     *    continue here:
     *
     *          Upstairs             Downstairs
     * 4: ExtentVersionsPlease --->
     *                         <---  ExtentVersions(g, v, d)
     *
     *    Now with the extent info, Upstairs calls process_downstairs() and
     *    if no problems, sends connected=true to the up_listen() task,
     *    we set the downstairs to DsState::WaitQuorum and we exit the
     *    while loop.
     *
     *    For the "Offline" state, the downstairs was connected and verified
     *    and after that point the connection was lost.  To handle this
     *    condition we follow these final steps to get this downstairs
     *    working again:
     *
     *          Upstairs             Downstairs
     * 3:       LastFlush(lf)) --->
     *                         <---  LastFlushAck(lf)
     *
     * 5: Now the downstairs is ready to receive replay IOs from the
     *    upstairs. We set the downstairs to DsState::Replay and the while
     *    loop is exited.
     */
    while !(*connected) {
        tokio::select! {
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * XXX Timeouts, timeouts: always wrong! Some too short and some
             * too long.
             * TODO: 50 is too long, but what is the correct value?
             */
            _ = sleep_until(timeout_deadline) => {
                up.ds_missing(up_coms.client_id);
                bail!("timed out during negotiation");
            }
            _ = sleep_until(ping_interval) => {
                fw.send(Message::Ruok).await?;
                ping_interval = deadline_secs(5);
            }
            _ = up_coms.ds_active_rx.changed(), if negotiated == 1 => {
                /*
                 * This check must only be done when the proper
                 * negotiation step is reached.  If we check too soon, then
                 * we can be out of order.
                *
                 * Promote self to active when message arrives from the Guest.
                 */
                fw.send(Message::PromoteToActive(up.uuid)).await?;
            }
            f = fr.next() => {
                // When the downstairs responds, push the deadlines
                timeout_deadline = deadline_secs(50);
                ping_interval = deadline_secs(5);

                match f.transpose()? {
                    None => {
                        // hung up
                        up.ds_missing(up_coms.client_id);
                        return Ok(())
                    }
                    Some(Message::YesItsMe(version)) => {
                        if negotiated != 0 {
                            bail!("Got version already!");
                        }

                        /*
                         * XXX Valid version to compare with should come
                         * from main task. In the future we will also have
                         * to handle a version mismatch.
                         */
                        if version != 1 {
                            up.ds_transition(
                                up_coms.client_id,
                                DsState::BadVersion
                            );
                            bail!("expected version 1, got {}", version);
                        }

                        negotiated = 1;
                        if up.is_active() {
                            /*
                             * On a downstairs reconnect, we are not going
                             * to receive another PromoteToActive from the
                             * guest, so send one ourselves.
                             */
                            fw.send(Message::PromoteToActive(up.uuid)).await?;
                        }
                    }
                    Some(Message::Imok) => {
                        if negotiated == 1 {
                            println!(
                                "{} client {} is waiting for promotion \
                                to active, received ping response.",
                                up.uuid, up_coms.client_id
                            );
                        }
                    }
                    Some(Message::YouAreNowActive(uuid)) => {
                        if up.uuid != uuid {
                            return Err(CrucibleError::UuidMismatch.into());
                        }

                        if negotiated != 1 {
                            bail!("Received YouAreNowActive out of order!");
                        }

                        /*
                         * Get region info.
                         */
                        negotiated = 2;
                        fw.send(Message::RegionInfoPlease).await?;
                    }
                    Some(Message::RegionInfo(region_def)) => {
                        if negotiated != 2 {
                            bail!("Received RegionInfo out of order!");
                        }
                        up.add_downstairs(up_coms.client_id, region_def)?;

                        /*
                         * If we are coming from state Offline, then it means
                         * the downstairs has departed then came back in
                         * short enough time that it does not have to go into
                         * full recovery/repair mode.  If we have verified
                         * that the UUID and region info is the same, we
                         * can reconnect and let any outstanding work
                         * be replayed to catch us up.  We do need to tell
                         * the downstairs the last flush ID it ACKd to us.
                         */
                        let my_state = {
                            let state = &up.downstairs.lock().unwrap().ds_state;
                            state[up_coms.client_id as usize]
                        };
                        if my_state == DsState::Offline {
                            let lf = up.last_flush_id(up_coms.client_id);
                            println!("[{}] send last flush ID to this DS: {}",
                                up_coms.client_id, lf);
                            negotiated = 3;
                            fw.send(Message::LastFlush(lf)).await?;
                        } else {
                            if my_state != DsState::New &&
                                my_state != DsState::Failed &&
                                my_state != DsState::Disconnected
                            {
                                panic!("[{}] Negotiation failed, in state {:?}",
                                    up_coms.client_id,
                                    my_state,
                                );
                            }
                            /*
                             * Ask for the current version of all extents.
                             */
                            negotiated = 4;
                            fw.send(Message::ExtentVersionsPlease).await?;
                        }
                        up.ds_state_show();
                    },
                    Some(Message::LastFlushAck(last_flush)) => {
                        if negotiated != 3 {
                            bail!("Received LastFlushAck out of order!");
                        }
                        let my_state = {
                            let state = &up.downstairs.lock().unwrap().ds_state;
                            state[up_coms.client_id as usize]
                        };
                        assert_eq!(my_state, DsState::Offline);
                        println!("[{}] replied this last flush ID: {}",
                            up_coms.client_id,
                            last_flush,
                        );
                        // Assert now, but this should eventually be an
                        // error and move the downstairs to failed. XXX
                        assert_eq!(
                            up.last_flush_id(up_coms.client_id), last_flush
                        );
                        up.ds_transition(
                            up_coms.client_id, DsState::Replay);

                        *connected = true;
                        negotiated = 5;
                    },
                    Some(Message::ExtentVersions(gen, flush, dirty)) => {
                        if negotiated != 4 {
                            bail!("Received ExtentVersions out of order!");
                        }

                        let my_state = {
                            let state = &up.downstairs.lock().unwrap().ds_state;
                            state[up_coms.client_id as usize]
                        };
                        assert_eq!(my_state, DsState::New);
                        /*
                         * XXX This logic may move to a different location
                         * when we actually get the code written to handle
                         * mismatch between downstairs extent versions.
                         * For comparing the region data, we need to collect
                         * version and dirty bit info from all three
                         * downstairs, and make the decision on which data is
                         * correct once we have everything.
                         */
                        process_downstairs(target, up, gen, flush, dirty)?;

                        negotiated = 5;
                        up.ds_transition(
                            up_coms.client_id, DsState::WaitQuorum
                        );
                        up.ds_state_show();

                        /*
                         * If we get here, we are ready to receive IO
                         */
                        *connected = true;
                        up_coms.ds_status_tx.send(Condition {
                            target: *target,
                            connected: true,
                        }).await
                        .unwrap();
                    }
                    Some(Message::UuidMismatch(expected_uuid)) => {
                        up.set_inactive();
                        bail!(
                            "{} received UuidMismatch during negotiation, \
                            expecting {:?}!",
                            up.uuid, expected_uuid,
                        );
                    }
                    Some(m) => {
                        bail!(
                            "unexpected command {:?} received in state {:?}",
                            m, up.ds_state(up_coms.client_id)
                        );
                    }
                }
            }
        }
    }
    /*
     * This check is just to make sure we have completed negotiation.
     * But, XXX, this will go away when we redo the state transition code
     * for a downstairs connection.
     */
    assert_eq!(negotiated, 5);
    println!("[{}] Client completed negotiation", up_coms.client_id);
    cmd_loop(up, fr, fw, up_coms, lossy).await
}

/*
 * Once we have negotiated a connection to a downstairs, this task takes
 * over and watches the input for changes, indicating that new work in on
 * the work hashmap. We will walk the hashmap on the input signal and get
 * any new work for this specific downstairs and mark that job as in
 * progress.
 *
 * V1 flow control: To enable flow control we have a few things.
 * 1. The boolean more_work variable, that indicates we are in a
 * flow control situation and should check for work to do even if new work
 * has not shown up.
 * 2. A resume timeout that is reset each time we try to do more work but
 * find the sending queue is "full" for some value of full we define in
 * the io_work function.
 * 3. Biased setting for the select loop. We start with looking for work
 * ACK messages before putting more new work on the list, which will
 * enable any downstairs to continue to send completed ACKs.
 *
 * Note that the more_work variable is also used when we have a disconnected
 * downstairs that comes back.  In that situation we also need to take our
 * work queue and resend everything since the last flush that was ACK'd.
 */
async fn cmd_loop(
    up: &Arc<Upstairs>,
    mut fr: FramedRead<
        tokio::net::tcp::ReadHalf<'_>,
        crucible_protocol::CrucibleDecoder,
    >,
    mut fw: FramedWrite<
        tokio::net::tcp::WriteHalf<'_>,
        crucible_protocol::CrucibleEncoder,
    >,
    up_coms: &mut UpComs,
    lossy: bool,
) -> Result<()> {
    println!("[{}] Starts cmd_loop", up_coms.client_id);

    /*
     * We set more_work if we arrive here on a re-connection, this will
     * allow us to replay any outstanding work.
     */
    let mut more_work = up.ds_replay_active(up_coms.client_id);

    /*
     * To keep things alive, initiate a ping any time we have been idle for
     * 10 seconds.
     *
     * XXX figure out what deadlines make sense here
     */
    let mut more_work_interval = deadline_secs(1);
    let mut ping_interval = deadline_secs(10);
    let mut timeout_deadline = deadline_secs(50);

    let (tx, mut rx) = mpsc::channel(100);

    {
        let up_c = up.clone();
        let up_coms_c = up_coms.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    x = rx.recv() => {
                        match x {
                            Some(m) => {
                                /*
                                 * TODO: Add a check here to make sure we are
                                 * connected and in the proper state before we
                                 * accept any commands.
                                 */
                                let _result =
                                    process_message(
                                        &up_c,
                                        &m,
                                        up_coms_c.clone()
                                    ).await;
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    up.ds_state_show();
    loop {
        tokio::select! {
            /*
             * We set biased so the loop will always start with looking for
             * an ACK from downstairs to avoid a backup.
             */
            biased;
            f = fr.next() => {
                // When the downstairs responds, push the deadlines
                timeout_deadline = deadline_secs(50);
                ping_interval = deadline_secs(10);

                match f.transpose()? {
                    None => {
                        return Ok(())
                    },
                    Some(Message::UuidMismatch(expected_uuid)) => {
                        up.set_inactive();
                        bail!(
                            "{} received UuidMismatch, expecting {:?}!",
                            up.uuid, expected_uuid
                        );
                    }
                    Some(m) => {
                        tx.send(m).await?;
                    }
                }
            }
            _ = up_coms.ds_work_rx.changed() => {
                /*
                 * A change here indicates the work hashmap has changed
                 * and we should go look for new work to do. It is possible
                 * that there is no new work but we won't know until we
                 * check.
                 */
                let more =
                    io_send(up, &mut fw, up_coms.client_id, lossy).await?;

                if more && !more_work {
                    println!("[{}] flow control start ", up_coms.client_id);

                    more_work = true;
                    more_work_interval = deadline_secs(1);
                }
            }
            _ = sleep_until(more_work_interval), if more_work => {
                println!(
                    "[{}] flow control sending more work",
                    up_coms.client_id
                );

                let more = io_send(
                                up, &mut fw, up_coms.client_id, lossy
                            ).await?;

                if more {
                    more_work = true;
                } else {
                    more_work = false;
                    println!("[{}] flow control end ", up_coms.client_id);
                }

                more_work_interval = deadline_secs(1);
            }
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * XXX Timeouts, timeouts: always wrong!  Some too short and
             * some too long.
             * TODO: 50 is too long, but what is the correct value?
             */
            _ = sleep_until(timeout_deadline) => {
                /*
                 * XXX What should happen here?  At the moment we just
                 * ignore it..
                 */
                println!("[{}] proc 2 Deadline ignored", up_coms.client_id);
                timeout_deadline = deadline_secs(50);
            }
            _ = sleep_until(ping_interval) => {
                fw.send(Message::Ruok).await?;

                if lossy {
                    /*
                     * When lossy is set, we don't always send work to a
                     * downstairs when we should. This means we need to,
                     * every now and then, signal the downstairs task to
                     * check and see if we skipped some work earlier.
                     */
                    io_send(up, &mut fw, up_coms.client_id, lossy).await?;
                }

                ping_interval = deadline_secs(10);
            }
        }
    }
}

/*
 * Things that allow the various tasks of Upstairs to communicate
 * with each other.
 */
#[derive(Clone)]
struct UpComs {
    /**
     * The client ID who will be using these channels.
     */
    client_id: u8,
    /**
     * This channel is used to receive a notification that new work has
     * (possibly) arrived on the work queue and this client should go
     * see what new work has arrived
     */
    ds_work_rx: watch::Receiver<u64>,
    /**
     * This channel is used to transmit that the state of the connection
     * to this downstairs has changed.
     */
    ds_status_tx: mpsc::Sender<Condition>,
    /**
     * This channel is used to transmit that an IO request sent by the
     * upstairs to all required downstairs has completed.
     */
    ds_done_tx: mpsc::Sender<u64>,
    /**
     * This channel is used to notify the proc task that it's time to
     * promote this downstairs to active.
     */
    ds_active_rx: watch::Receiver<bool>,
}

/*
 * This task is responsible for the connection to a specific downstairs
 * instance.
 */
async fn looper(
    target: SocketAddrV4,
    up: &Arc<Upstairs>,
    mut up_coms: UpComs,
    lossy: bool,
) {
    let mut firstgo = true;
    let mut connected = false;

    'outer: loop {
        if firstgo {
            firstgo = false;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Make connection to this downstairs.
         */
        let sock = TcpSocket::new_v4().expect("v4 socket");

        /*
         * Set a connect timeout, and connect to the target:
         */
        println!(
            "{0}[{1}] looper connecting to {0}",
            target, up_coms.client_id
        );
        let deadline = tokio::time::sleep_until(deadline_secs(10));
        tokio::pin!(deadline);
        let tcp = sock.connect(target.into());
        tokio::pin!(tcp);

        let tcp: TcpStream = loop {
            tokio::select! {
                _ = &mut deadline => {
                    println!("connect timeout");
                    continue 'outer;
                }
                tcp = &mut tcp => {
                    match tcp {
                        Ok(tcp) => {
                            println!("{0}[{1}] looper ok, connected to {0}",
                                target,
                                up_coms.client_id);
                            break tcp;
                        }
                        Err(e) => {
                            println!("{0} looper connect to {0} failure: {1:?}",
                                target, e);
                            continue 'outer;
                        }
                    }
                }
            }
        };

        /*
         * Once we have a connected downstairs, the proc task takes over and
         * handles negotiation and work processing.
         */
        if let Err(e) =
            proc(&target, up, tcp, &mut connected, &mut up_coms, lossy).await
        {
            eprintln!("ERROR: {}: proc: {:?}", target, e);
        }

        /*
         * Do not attempt to reconnect if we were marked inactive.
         */
        if !up.is_active() {
            drop(up_coms.ds_status_tx);
            drop(up_coms.ds_done_tx);
            return;
        }

        /*
         * If the connection goes down here, we need to know what state we
         * were in to decide what state to transition to.  The ds_missing
         * method will do that for us.
         */
        up.ds_missing(up_coms.client_id);
        up.ds_state_show();

        println!(
            "{0}[{1}] connection to {0} closed",
            target, up_coms.client_id
        );
        connected = false;
    }
}

/*
 * The structure that tracks information about the three downstairs
 * connections as well as the work that each is doing.
 */
#[derive(Debug)]
struct Downstairs {
    /*
     * UUID for each downstairs, index by client ID
     */
    ds_uuid: HashMap<u8, Uuid>,
    /*
     * The state of a downstairs connection, based on client ID
     * Ready here indicates it can receive IO.
     * TODO: When growing to more than one region, should this become
     * a 2d Vec? Index for region, then index for the DS?
     */
    ds_state: Vec<DsState>,
    /*
     * The last flush ID that this downstairs has acked.
     */
    ds_last_flush: Vec<u64>,
    downstairs_errors: HashMap<u8, u64>, // client id -> errors
    active: HashMap<u64, DownstairsIO>,
    next_id: u64,
    completed: AllocRingBuffer<u64>,
}

/*
 * These counts describe the various states that a Downstairs IO can
 * be in.
 */
#[derive(Debug, Default)]
pub struct WorkCounts {
    active: u64,  // New or in flight to downstairs.
    error: u64,   // This IO had an error.
    skipped: u64, // Skipped
    done: u64,    // This IO has completed
}

impl WorkCounts {
    fn completed_ok(&self) -> u64 {
        self.done
    }
}

impl Default for Downstairs {
    fn default() -> Self {
        Self {
            ds_uuid: HashMap::new(),
            ds_state: vec![DsState::New; 3],
            ds_last_flush: vec![0; 3],
            downstairs_errors: HashMap::new(),
            active: HashMap::new(),
            completed: AllocRingBuffer::with_capacity(2048),
            next_id: 1000,
        }
    }
}

impl Downstairs {
    /**
     * Assign a new downstairs ID.
     */
    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /**
     * Mark this request as in progress for this client, and return a copy
     * of the details of the request. If the downstairs client has
     * experienced errors in the past, return None and mark this as
     * Skipped.
     *
     * XXX Better error handling might mean clearing previous downstairs
     * errors, as for all we know it's a new downstairs.
     */
    fn in_progress(&mut self, ds_id: u64, client_id: u8) -> Option<IOop> {
        let job = self.active.get_mut(&ds_id).unwrap();

        let newstate = match &self.downstairs_errors.get(&client_id) {
            Some(_) => IOState::Skipped,
            None => IOState::InProgress,
        };

        let oldstate = job.state.insert(client_id, newstate.clone());
        assert_eq!(oldstate, Some(IOState::New));

        match newstate {
            IOState::Skipped => None,
            IOState::InProgress => Some(job.work.clone()),
            _ => panic!("bad state in in_progress!"),
        }
    }

    /**
     * We have disconnected from a downstairs. Move every job since the
     * last flush for this client_id back to New, even if we already have
     * an ACK back from the downstairs for this job. We must replay
     * everything since the last flush to guarantee persistence.
     *
     * If the job has already been acked back to the guest, then we don't
     * change that, but we do replay it to the downstairs.
     *
     * The special case we have to handle is a job that is AckReady.
     * In this case, we need to understand if a success from this IO to
     * this downstairs was used to decide that we can send an Ack back to
     * the guest. If it was, then we need to retract that AckReady state,
     * switch the overall job back to NotAcked, and then let the replay
     * happen.
     */
    fn re_new(&mut self, client_id: u8) {
        let lf = self.ds_last_flush[client_id as usize];
        let mut kvec: Vec<u64> =
            self.active.keys().cloned().collect::<Vec<u64>>();
        kvec.sort_unstable();

        println!("{} client re-new all jobs since flush {}", client_id, lf);
        for ds_id in kvec.iter() {
            let is_read = self.is_read(*ds_id).unwrap();
            let wc = self.state_count(*ds_id).unwrap();
            let jobs_completed_ok = wc.completed_ok();

            let job = self.active.get_mut(ds_id).unwrap();

            // We don't need to send anything before our last good flush
            if *ds_id <= lf {
                assert_eq!(Some(&IOState::Done), job.state.get(&client_id));
                continue;
            }

            /*
             * If the job is InProgress or New, then we can just go back
             * to New and no extra work is required.
             * If it's Done, then we need to look further
             */
            if Some(&IOState::Done) == job.state.get(&client_id) {
                /*
                 * If the job is acked, then we are good to go and
                 * we can re-send it downstairs and the upstairs ack
                 * path will handle a downstairs ack for a job that
                 * we already ack'd back to the guest.
                 *
                 * If the job is AckReady, then we need to decide
                 * if this downstairs job was part of what made it AckReady
                 * and if so, we need to undo that AckReady status.
                 */
                if job.ack_status == AckStatus::AckReady {
                    if is_read {
                        if jobs_completed_ok == 1 {
                            println!("Remove read data for {}", ds_id);
                            job.data = None;
                            job.ack_status = AckStatus::NotAcked;
                        }
                    } else {
                        /*
                         * For a write or flush, if we have 3 completed,
                         * then we can leave this job as AckReady, if not,
                         * then we have to undo the AckReady.
                         */
                        if jobs_completed_ok < 3 {
                            println!("Remove AckReady for W/F {}", ds_id);
                            job.ack_status = AckStatus::NotAcked;
                        }
                    }
                }
            }
            println!("{} job goes back to IOState::New", ds_id);
            job.state.insert(client_id, IOState::New);
        }
    }

    /**
     * Return a list of downstairs request IDs that represent unissued
     * requests for this client.
     */
    fn new_work(&self, client_id: u8) -> Vec<u64> {
        self.active
            .values()
            .filter_map(|job| {
                if let Some(IOState::New) = job.state.get(&client_id) {
                    Some(job.ds_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /**
     * Return a count of downstairs request IDs of work we have sent
     * for this client, but don't yet have a response.
     */
    fn submitted_work(&self, client_id: u8) -> usize {
        self.active
            .values()
            .filter(|job| {
                Some(&IOState::InProgress) == job.state.get(&client_id)
            })
            .count()
    }
    /**
     * Build a list of jobs that are ready to be acked.
     */
    fn ackable_work(&mut self) -> Vec<u64> {
        let mut ackable = Vec::new();
        for (ds_id, job) in &self.active {
            if job.ack_status == AckStatus::AckReady {
                ackable.push(*ds_id);
            }
        }
        ackable
    }

    /**
     * Enqueue a new downstairs request.
     */
    fn enqueue(&mut self, io: DownstairsIO) {
        self.active.insert(io.ds_id, io);
    }

    /**
     * Collect the state of the jobs from each client.
     */
    fn state_count(&mut self, ds_id: u64) -> Result<WorkCounts> {
        /* XXX Should this support invalid ds_ids? */
        let job = self
            .active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;
        Ok(job.state_count())
    }

    fn ack(&mut self, ds_id: u64) {
        /*
         * Move AckReady to Acked.
         */
        let job = self
            .active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))
            .unwrap();

        if job.ack_status != AckStatus::AckReady {
            panic!(
                "Job {} not in proper state to ACK:{:?}",
                ds_id, job.ack_status,
            );
        }
        job.ack_status = AckStatus::Acked;
    }

    fn result(&mut self, ds_id: u64) -> Result<(), CrucibleError> {
        /*
         * If enough downstairs returned an error, then return an error to
         * the Guest
         *
         * Not ok:
         * - 2+ errors for Write/Flush
         * - 3+ errors for Reads
         *
         * TODO: this doesn't tell the Guest what the error(s) were?
         * TODO: Add retries here as well.
         */
        let wc = self.state_count(ds_id).unwrap();

        let job = self
            .active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        /*
         * XXX: this code assumes that 3 downstairs is the max that we'll
         * ever support.
         */
        let bad_job = match &job.work {
            IOop::Read {
                dependencies: _dependencies,
                eid: _eid,
                offset: _offset,
                num_blocks: _num_blocks,
            } => wc.error == 3,
            IOop::Write {
                dependencies: _dependencies,
                eid: _eid,
                data: _data,
                offset: _offset,
            } => wc.error >= 2,
            IOop::Flush {
                dependencies: _dependencies,
                flush_number: _flush_number,
            } => wc.error >= 2,
        };

        if bad_job {
            Err(CrucibleError::IoError(format!(
                "{} out of 3 downstairs returned an error",
                wc.error
            )))
        } else {
            Ok(())
        }
    }

    /*
     * This function just does the match on IOop type and updates the dtrace
     * probe for that operation finishing.
     */
    fn cdt_gw_work_done(&self, ds_id: u64, gw_id: u64) {
        let job = self
            .active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))
            .unwrap();

        match &job.work {
            IOop::Read {
                dependencies: _,
                eid: _,
                offset: _,
                num_blocks: _,
            } => {
                cdt::gw_read_end!(|| (gw_id));
            }
            IOop::Write {
                dependencies: _,
                eid: _,
                offset: _,
                data: _,
            } => {
                cdt::gw_write_end!(|| (gw_id));
            }
            IOop::Flush {
                dependencies: _,
                flush_number: _,
            } => {
                cdt::gw_flush_end!(|| (gw_id));
            }
        }
    }

    /**
     * Mark this downstairs request as complete for this client. Returns
     * true if this completion is enough that we should message the
     * upstairs task that handles returning completions to the guest.
     *
     * This is where we decide the number of successful completions required
     * before setting the AckReady state on a ds_id, which another upstairs
     * task is looking for to then ACK back to the guest.
     */
    fn complete(
        &mut self,
        ds_id: u64,
        client_id: u8,
        read_data: Option<Bytes>,
        result: Result<(), CrucibleError>,
    ) -> Result<bool> {
        /*
         * Assume we don't have enough completed jobs, and only change
         * it if we have the exact amount required
         */
        let mut notify_guest = false;

        /*
         * Get the completed count now,
         * because the job self ref won't let us call state_count once we are
         * using that ref, and the number won't change while we are in
         * this method (you did get the lock first, right??).
         */
        let wc = self.state_count(ds_id)?;
        let mut jobs_completed_ok = wc.completed_ok();

        let job = self
            .active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        let newstate = if let Err(e) = result {
            IOState::Error(e)
        } else {
            jobs_completed_ok += 1;
            IOState::Done
        };

        let oldstate = job.state.insert(client_id, newstate.clone()).unwrap();
        // we shouldn't be transitioning to our current state
        assert_eq!(oldstate, IOState::InProgress);

        if matches!(newstate, IOState::Error(_)) {
            // Mark this downstairs as bad if this was a write or flush
            // XXX: reconcilation, retries?
            // XXX: Errors should be reported to nexus
            if matches!(
                job.work,
                IOop::Write {
                    dependencies: _,
                    eid: _,
                    data: _,
                    offset: _
                } | IOop::Flush {
                    dependencies: _,
                    flush_number: _
                }
            ) {
                let errors: u64 = match self.downstairs_errors.get(&client_id) {
                    Some(v) => *v,
                    None => 0,
                };
                self.downstairs_errors.insert(client_id, errors + 1);
                // XXX We don't count read errors here.
            }
        } else if job.ack_status == AckStatus::Acked {
            assert_eq!(newstate, IOState::Done);
            /*
             * If this job is already acked, then we don't have much
             * more to do here.  If it's a flush, then we want to be
             * sure to update the last flush for this client.
             */
            if let IOop::Flush {
                dependencies: _dependencies,
                flush_number: _flush_number,
            } = &job.work
            {
                self.ds_last_flush[client_id as usize] = ds_id;
            }
        } else {
            assert_eq!(newstate, IOState::Done);
            assert_ne!(job.ack_status, AckStatus::Acked);
            /*
             * Transition this job from Done to AckReady if enough have
             * returned ok.
             */
            match &job.work {
                IOop::Read {
                    dependencies: _dependencies,
                    eid: _eid,
                    offset: _offset,
                    num_blocks: _num_blocks,
                } => {
                    assert!(read_data.is_some());
                    if jobs_completed_ok == 1 {
                        assert!(job.data.is_none());
                        job.data = read_data;
                        notify_guest = true;
                        assert_eq!(job.ack_status, AckStatus::NotAcked);
                        job.ack_status = AckStatus::AckReady;
                    }
                }
                IOop::Write {
                    dependencies: _dependencies,
                    eid: _eid,
                    data: _data,
                    offset: _offset,
                } => {
                    assert!(read_data.is_none());
                    if jobs_completed_ok == 2 {
                        notify_guest = true;
                        job.ack_status = AckStatus::AckReady;
                    }
                }
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                } => {
                    assert!(read_data.is_none());
                    if jobs_completed_ok == 2 {
                        notify_guest = true;
                        job.ack_status = AckStatus::AckReady;
                    }
                    self.ds_last_flush[client_id as usize] = ds_id;
                }
            }
        }
        /*
         * If all 3 jobs are done, we can check here to see if we can
         * remove this job from the DS list. If we have completed the ack
         * to the guest, then there will be no more work on this job
         * but messages may still be unprocessed.
         */
        if job.ack_status == AckStatus::Acked {
            self.retire_check(ds_id);
        } else if job.ack_status == AckStatus::NotAcked {
            // If we reach this then the job probably has errors and
            // hasn't acked back yet. We check for NotAcked so we  don't
            // double count three done and return true if we already have
            // AckReady set.
            let wc = job.state_count();
            if (wc.error + wc.skipped + wc.done) == 3 {
                notify_guest = true;
                job.ack_status = AckStatus::AckReady;
            }
        }

        Ok(notify_guest)
    }

    /**
     * This request is now complete on all peers, but is is ready to retire?
     * Only when a flush is complete on all three do we check
     * to see if we can remove the job.  When we remove a job, we
     * also take all the previous jobs out of the queue as well.
     * Double check that all previous jobs have finished and panic
     * if not.
     */
    fn retire_check(&mut self, ds_id: u64) {
        // Only a completed flush will remove work from the active queue.
        if !self.is_flush(ds_id).unwrap() {
            return;
        }
        // Sort the job list, and retire all the work that is older than us.
        let wc = self.state_count(ds_id).unwrap();
        if (wc.error + wc.skipped + wc.done) == 3 {
            assert!(!self.completed.contains(&ds_id));
            assert_eq!(wc.active, 0);

            /*
             * Build the list of keys to iterate.  Don't bother to look
             * at any key ids greater than our ds_id.
             */
            let mut kvec: Vec<u64> = self
                .active
                .keys()
                .cloned()
                .filter(|&x| x <= ds_id)
                .collect::<Vec<u64>>();

            kvec.sort_unstable();
            for id in kvec.iter() {
                assert!(*id <= ds_id);
                let wc = self.state_count(*id).unwrap();
                assert_eq!(wc.active, 0);
                assert_eq!(wc.error + wc.skipped + wc.done, 3);
                assert!(!self.completed.contains(id));

                let oj = self.active.remove(id).unwrap();
                assert_eq!(oj.ack_status, AckStatus::Acked);
                self.completed.push(*id);
            }
        }
    }

    /**
     * Check if an active job is a flush or not.
     */
    fn is_flush(&self, ds_id: u64) -> Result<bool> {
        let job = self
            .active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        match &job.work {
            IOop::Flush {
                dependencies: _dependencies,
                flush_number: _flush_number,
            } => Ok(true),
            _ => Ok(false),
        }
    }
    /**
     * Check if an active job is a read or not.
     */
    fn is_read(&self, ds_id: u64) -> Result<bool> {
        let job = self
            .active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        match &job.work {
            IOop::Read {
                dependencies: _dependencies,
                eid: _eid,
                offset: _offset,
                num_blocks: _num_blocks,
            } => Ok(true),
            _ => Ok(false),
        }
    }
}

/// Implement XTS encryption
/// See: https://en.wikipedia.org/wiki/Disk_encryption_theory#XEX-based_\
/// tweaked-codebook_mode_with_ciphertext_stealing_(XTS)
pub struct EncryptionContext {
    xts: Xts128<Aes128>,
    key: Vec<u8>,
    block_size: usize,
}

impl Debug for EncryptionContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("EncryptionContext")
            .field("block_size", &self.block_size)
            .finish()
    }
}

impl Clone for EncryptionContext {
    fn clone(&self) -> Self {
        EncryptionContext::new(self.key.clone(), self.block_size)
    }

    fn clone_from(&mut self, source: &Self) {
        *self = EncryptionContext::new(source.key.clone(), source.block_size);
    }
}

impl EncryptionContext {
    pub fn new(key: Vec<u8>, block_size: usize) -> EncryptionContext {
        assert!(key.len() == 32);

        let cipher_1 = Aes128::new(GenericArray::from_slice(&key[..16]));
        let cipher_2 = Aes128::new(GenericArray::from_slice(&key[16..]));

        let xts = Xts128::<Aes128>::new(cipher_1, cipher_2);

        EncryptionContext {
            xts,
            key,
            block_size,
        }
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn encrypt_in_place(&self, data: &mut [u8], sector_index: u128) {
        self.xts.encrypt_area(
            data,
            self.block_size,
            sector_index,
            get_tweak_default,
        );
    }

    pub fn decrypt_in_place(&self, data: &mut [u8], sector_index: u128) {
        self.xts.decrypt_area(
            data,
            self.block_size,
            sector_index,
            get_tweak_default,
        );
    }
}

/*
 * XXX Track scheduled storage work in the central structure. Have the
 * target management task check for work to do here by changing the value in
 * its watch::channel. Have the main thread determine that an overflow of
 * work to do backing up in here means we need to do something like mark the
 * target as behind or institute some kind of back pressure, etc.
 */
#[derive(Debug)]
pub struct Upstairs {
    /*
     * Is this Upstairs active, or just attaching inactive?
     */
    active: Mutex<bool>,

    /*
     * Upstairs UUID
     */
    uuid: Uuid,

    /*
     * The guest struct keeps track of jobs accepted from the Guest as they
     * progress through crucible. A single job submitted can produce
     * multiple downstairs requests.
     */
    guest: Arc<Guest>,

    /*
     * This Downstairs struct keeps track of information about each
     * downstairs as well as tracking IO operations going between
     * upstairs and downstairs. New work for downstairs is generated
     * inside the upstairs on behalf of IO requests coming from the guest.
     */
    downstairs: Mutex<Downstairs>,

    /*
     * The flush info Vec is only used when first connecting or
     * re-connecting to a downstairs. It is populated with the versions
     * the upstairs considers the "correct". If a downstairs disconnects
     * and then comes back, it has to match or be made to match what was
     * decided as the correct list. This may involve having to refresh
     * the versions vec.
     *
     * The versions vec is not enough to solve a mismatch. We really need
     * Generation number, flush number, and dirty bit for every extent
     * when resolving conflicts.
     *
     * On Startup we determine the highest flush number from all three
     * downstairs. We add one to that and it becomes the next flush
     * number. Flush numbers increment by one each time.
     */
    flush_info: Mutex<FlushInfo>,

    /*
     * The global description of the downstairs region we are using.
     * This allows us to verify each downstairs is the same, as well as
     * enables us to translate an LBA to an extent and block offset.
     */
    ddef: Mutex<RegionDefinition>,

    /*
     * Optional encryption context - Some if a key was supplied in
     * the CrucibleOpts
     */
    encryption_context: Option<EncryptionContext>,

    /*
     * Upstairs keeps all IOs in memory until a flush is ACK'd back from
     * all three downstairs.  If there are IOs we have accepted into the
     * work queue that don't end with a flush, then we set this to indicate
     * that the upstairs may need to issue a flush of its own to be sure
     * that data is pushed to disk.  Note that this is not an indication of
     * an ACK'd flush, just that the last IO command we put on the work
     * queue was not a flush.
     */
    need_flush: Mutex<bool>,
}

impl Upstairs {
    pub fn default() -> Arc<Self> {
        let opts = CrucibleOpts {
            target: vec![],
            lossy: false,
            key: None,
        };
        Self::new(
            &opts,
            RegionDefinition::default(),
            Arc::new(Guest::default()),
        )
    }

    pub fn new(
        opt: &CrucibleOpts,
        def: RegionDefinition,
        guest: Arc<Guest>,
    ) -> Arc<Upstairs> {
        /*
         * XXX Make sure we have three and only three downstairs
         */
        #[cfg(not(test))]
        assert_eq!(opt.target.len(), 3);

        // create an encryption context if a key is supplied.
        let encryption_context = opt.key_bytes().map(|key| {
            EncryptionContext::new(
                key,
                /*
                 * XXX: It would be good to do BlockOp::QueryBlockSize here,
                 * but this creates a deadlock. Upstairs::new runs before
                 * up_ds_listen in up_main, and up_ds_listen needs to run
                 * to answer BlockOp::QueryBlockSize.
                 *
                 * At this point ddef is the default, the downstairs haven't
                 * reported in.
                 */
                512,
            )
        });

        Arc::new(Upstairs {
            active: Mutex::new(false),
            uuid: Uuid::new_v4(), // XXX get from Nexus?
            guest,
            downstairs: Mutex::new(Downstairs::default()),
            flush_info: Mutex::new(FlushInfo::new()),
            ddef: Mutex::new(def),
            encryption_context,
            need_flush: Mutex::new(false),
        })
    }

    fn set_active(&self) {
        let mut active = self.active.lock().unwrap();
        *active = true;
    }

    fn set_inactive(&self) {
        let mut active = self.active.lock().unwrap();
        *active = false;
    }

    fn is_active(&self) -> bool {
        *self.active.lock().unwrap()
    }

    /*
     * If we are doing a flush, the flush number and the rn number
     * must both go up together. We don't want a lower next_id
     * with a higher flush_number to be possible, as that can introduce
     * dependency deadlock.
     * To also avoid any problems, this method should be called only
     * during the submit_flush method so we know the downstairs and
     * guest_work locks are both held.
     */
    fn next_flush_id(&self) -> u64 {
        let mut fi = self.flush_info.lock().unwrap();
        fi.next_flush()
    }

    fn last_flush_id(&self, client_id: u8) -> u64 {
        let lf = self.downstairs.lock().unwrap();
        lf.ds_last_flush[client_id as usize]
    }

    fn set_flush_clear(&self) {
        let mut flush = self.need_flush.lock().unwrap();
        *flush = false;
    }
    fn set_flush_need(&self) {
        let mut flush = self.need_flush.lock().unwrap();
        *flush = true;
    }
    fn flush_needed(&self) -> bool {
        if !self.is_active() {
            return false;
        }
        *self.need_flush.lock().unwrap()
    }

    #[instrument]
    pub fn submit_flush(
        &self,
        sender: Option<std_mpsc::Sender<Result<(), CrucibleError>>>,
    ) -> Result<(), CrucibleError> {
        if !self.is_active() {
            crucible_bail!(UpstairsInactive);
        }

        /*
         * Lock first the guest_work struct where this new job will go,
         * then lock the downstairs struct. Once we have both we can proceed
         * to build our flush command.
         */
        let mut gw = self.guest.guest_work.lock().unwrap();
        let mut downstairs = self.downstairs.lock().unwrap();
        self.set_flush_clear();

        /*
         * Get the next ID for our new guest work job. Note that the flush
         * ID and the next_id are connected here, in that all future writes
         * should be flushed at the next flush ID.
         */
        let gw_id: u64 = gw.next_gw_id();
        let next_id = downstairs.next_id();
        let next_flush = self.next_flush_id();

        /*
         * Walk the downstairs work active list, and pull out all the active
         * jobs. Anything we have not submitted back to the guest.
         *
         * TODO, we can go faster if we:
         * 1. Ignore everything that was before and including the last flush.
         * 2. Ignore reads.
         */
        let mut dep = downstairs.active.keys().cloned().collect::<Vec<u64>>();
        dep.sort_unstable();
        /*
         * TODO: Walk the list of guest work structs and build the same list
         * and make sure it matches.
         */

        /*
         * Build the flush request, and take note of the request ID that
         * will be assigned to this new piece of work.
         */
        let fl = create_flush(next_id, dep, next_flush, gw_id);

        let mut sub = HashMap::new();
        sub.insert(next_id, 0);

        let new_gtos = GtoS::new(
            sub,
            Vec::new(),
            None,
            HashMap::new(),
            HashMap::new(),
            sender,
            None,
        );
        gw.active.insert(gw_id, new_gtos);
        cdt::gw_flush_start!(|| (gw_id));

        downstairs.enqueue(fl);

        Ok(())
    }

    /*
     * When we have a guest write request with offset and buffer, take them
     * and build both the upstairs work guest tracking struct as well as the
     * downstairs work struct. Once both are ready, submit them to the
     * required places.
     */
    #[instrument]
    fn submit_write(
        &self,
        offset: Block,
        data: Bytes,
        sender: std_mpsc::Sender<Result<(), CrucibleError>>,
    ) -> Result<(), CrucibleError> {
        if !self.is_active() {
            crucible_bail!(UpstairsInactive);
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().unwrap();
        let mut downstairs = self.downstairs.lock().unwrap();
        self.set_flush_need();

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span two extents, and eventually XXX, two regions.
         */
        let ddef = self.ddef.lock().unwrap();
        let nwo = extent_from_offset(
            *ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        )?;

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();

        /*
         * Now create a downstairs work job for each (eid, bi, len) returned
         * from extent_from_offset
         *
         * Create the list of downstairs request numbers (ds_id) we created
         * on behalf of this guest job.
         */
        let mut sub = HashMap::new();
        let mut new_ds_work = Vec::new();
        let mut next_id: u64;
        let mut cur_offset: usize = 0;

        let mut dep = downstairs.active.keys().cloned().collect::<Vec<u64>>();
        dep.sort_unstable();
        /* Lock here, through both jobs submitted */
        for (eid, bo, num_blocks) in nwo {
            {
                next_id = downstairs.next_id();
            }

            let byte_len: usize =
                num_blocks.value as usize * ddef.block_size() as usize;

            let sub_data = if let Some(context) = &self.encryption_context {
                // Encrypt here
                let mut mut_data =
                    data.slice(cur_offset..(cur_offset + byte_len)).to_vec();
                context.encrypt_in_place(&mut mut_data[..], bo.value as u128);
                Bytes::copy_from_slice(&mut_data)
            } else {
                // Unencrypted
                data.slice(cur_offset..(cur_offset + byte_len))
            };

            sub.insert(next_id, num_blocks.value);

            let wr = create_write_eob(
                next_id,
                dep.clone(),
                gw_id,
                eid,
                bo,
                sub_data,
            );

            new_ds_work.push(wr);
            cur_offset += byte_len;
        }

        /*
         * New work created, add to the guest_work HM
         */
        let new_gtos = GtoS::new(
            sub,
            Vec::new(),
            None,
            HashMap::new(),
            HashMap::new(),
            Some(sender),
            None,
        );
        {
            gw.active.insert(gw_id, new_gtos);
        }
        cdt::gw_write_start!(|| (gw_id));

        for wr in new_ds_work {
            downstairs.enqueue(wr);
        }

        Ok(())
    }

    /*
     * When we have a guest read request with offset and buffer, take them
     * and build both the upstairs work guest tracking struct as well as the
     * downstairs work struct. Once both are ready, submit them to the
     * required places.
     */
    #[instrument]
    fn submit_read(
        &self,
        offset: Block,
        data: Buffer,
        sender: std_mpsc::Sender<Result<(), CrucibleError>>,
    ) -> Result<(), CrucibleError> {
        if !self.is_active() {
            crucible_bail!(UpstairsInactive);
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().unwrap();
        let mut downstairs = self.downstairs.lock().unwrap();
        self.set_flush_need();
        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span many extents, and eventually, TODO, regions.
         */
        let ddef = self.ddef.lock().unwrap();
        let nwo = extent_from_offset(
            *ddef,
            offset,
            Block::from_bytes(data.len(), &ddef),
        )?;

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();

        /*
         * Create the tracking info for downstairs request numbers (ds_id) we
         * will create on behalf of this guest job.
         */
        let mut sub = HashMap::new();
        let mut new_ds_work = Vec::new();
        let mut downstairs_buffer_sector_index = HashMap::new();
        let mut next_id: u64;

        /*
         * Now create a downstairs work job for each (eid, bo, len) returned
         * from extent_from_offset
         */
        let mut dep = downstairs.active.keys().cloned().collect::<Vec<u64>>();
        dep.sort_unstable();
        for (eid, bo, num_blocks) in nwo {
            {
                next_id = downstairs.next_id();
            }

            /*
             * When multiple operations are needed to satisfy a read, The
             * offset and length will be divided across two downstairs
             * requests. It is required (for re-assembly on the other side)
             * that the lower offset corresponds to the lower next_id.
             * The ID's don't need to be sequential.
             */
            sub.insert(next_id, num_blocks.value);
            downstairs_buffer_sector_index.insert(next_id, bo.value as u128);
            let wr = create_read_eob(
                next_id,
                dep.clone(),
                gw_id,
                eid,
                bo,
                num_blocks.value,
            );
            new_ds_work.push(wr);
        }

        /*
         * New work created, add to the guest_work HM. New work must be put
         * on the guest_work active HM first, before it lands on the
         * downstairs lists. We don't want to miss a completion from
         * downstairs.
         */
        assert!(!sub.is_empty());
        let new_gtos = GtoS::new(
            sub,
            Vec::new(),
            Some(data),
            HashMap::new(),
            downstairs_buffer_sector_index,
            Some(sender),
            self.encryption_context.clone(),
        );
        {
            gw.active.insert(gw_id, new_gtos);
        }
        cdt::gw_read_start!(|| (gw_id));

        for wr in new_ds_work {
            downstairs.enqueue(wr);
        }

        Ok(())
    }

    /*
     * Our connection to a downstairs has been lost.  Depending on what
     * state the downstairs was in will indicate which state this downstairs
     * needs to go to.
     *
     * Any IOs since the last ACK'd flush will be reset back to new.
     */
    fn ds_missing(&self, client_id: u8) {
        let mut ds = self.downstairs.lock().unwrap();
        let current = ds.ds_state[client_id as usize];
        let new_state = match current {
            DsState::Active => DsState::Offline,
            DsState::Replay => DsState::Offline,
            DsState::Offline => DsState::Offline,
            DsState::_Migrating => DsState::Failed,
            _ => {
                /*
                 * Any other state means we had not yet enabled this
                 * downstairs to receive IO, so we go to the back of the
                 * line and have to re-verify it again.
                 */
                DsState::Disconnected
            }
        };
        println!(
            "[{}] Gone missing, transition from {:?} to {:?}",
            client_id, current, new_state,
        );
        ds.ds_state[client_id as usize] = new_state;

        /*
         * Mark any in progress jobs since the last good flush back to New,
         * as we are now disconnected from this downstairs and will need to
         * replay (or eventually discard) any work that it still needs to do.
         */
        ds.re_new(client_id);
    }

    /*
     * Check and see if our DS was in replay and if so move
     * it over to Active.  Return true if we did.
     */
    fn ds_replay_active(&self, client_id: u8) -> bool {
        let mut ds = self.downstairs.lock().unwrap();
        if ds.ds_state[client_id as usize] == DsState::Replay {
            println!("[{}] Transition from Replay to Active", client_id);
            ds.ds_state[client_id as usize] = DsState::Active;
            return true;
        }
        false
    }

    /*
     * Move a single downstairs to this new state.
     * XXX Perhaps state change logic can go here to prevent illegal
     * state transitions.
     */
    fn ds_transition(&self, client_id: u8, new_state: DsState) {
        let mut ds = self.downstairs.lock().unwrap();
        if ds.ds_state[client_id as usize] != new_state {
            println!(
                "Transition [{}] from {:?} to {:?}",
                client_id, ds.ds_state[client_id as usize], new_state,
            );
            ds.ds_state[client_id as usize] = new_state;
        } else {
            panic!("[{}] transition to same state: {:?}", client_id, new_state);
        }
    }

    fn ds_state(&self, client_id: u8) -> DsState {
        let ds = self.downstairs.lock().unwrap();
        ds.ds_state[client_id as usize]
    }

    fn all_ds_state_match(&self, state: DsState) -> bool {
        let ds = self.downstairs.lock().unwrap();

        for (_, dst) in ds.ds_state.iter().enumerate() {
            if *dst != state {
                return false;
            }
        }

        true
    }

    /**
     * Return a copy of the DsState vec.
     * DTraces uses this.
     */
    fn ds_state_copy(&self) -> Vec<DsState> {
        self.downstairs.lock().unwrap().ds_state.clone()
    }

    /**
     * Return a count of the jobs on the downstairs active list.
     * DTrace uses this.
     */
    fn ds_work_active(&self) -> u32 {
        self.downstairs.lock().unwrap().active.len() as u32
    }

    /**
     * Return a count of the jobs on the upstairs active list.
     * DTrace uses this.
     */
    fn up_work_active(&self) -> u32 {
        self.guest.guest_work.lock().unwrap().active.len() as u32
    }

    fn ds_state_show(&self) {
        let ds = self.downstairs.lock().unwrap();
        for (index, dst) in ds.ds_state.iter().enumerate() {
            println!("[{}] State {:?}", index, dst);
        }
    }

    /*
     * Move all downstairs to this new state.
     */
    fn ds_transition_all(&self, new_state: DsState) {
        let mut ds = self.downstairs.lock().unwrap();

        ds.ds_state.iter_mut().for_each(|ds_state| {
            println!("Transition from {:?} to {:?}", *ds_state, new_state,);
            match new_state {
                DsState::Active => {
                    assert_eq!(*ds_state, DsState::WaitQuorum);
                    *ds_state = new_state;
                }
                DsState::Deactivated => {
                    *ds_state = new_state;
                }
                _ => {
                    panic!(
                        "Unsupported state transition {:?} -> {:?}",
                        *ds_state, new_state
                    );
                }
            }
        });
    }

    /*
     * Check the region information for a downstairs and decide if we should
     * allow this downstairs to be added.
     * TODO: Still a bunch to do here around reconnecting.
     */
    fn add_downstairs(
        &self,
        client_id: u8,
        client_ddef: RegionDefinition,
    ) -> Result<()> {
        println!("[{}] Got region def {:?}", client_id, client_ddef);

        /*
         * XXX Eventually we will be provided UUIDs when the upstairs
         * starts, so we can compare those with what we get here.
         *
         * For now, we take whatever connects to us first.
         */
        let mut ds = self.downstairs.lock().unwrap();
        if let Some(uuid) = ds.ds_uuid.get(&client_id) {
            if *uuid != client_ddef.uuid() {
                panic!(
                    "New client:{} uuid:{}  does not match existing {}",
                    client_id,
                    client_ddef.uuid(),
                    uuid
                );
            } else {
                println!(
                    "Returning client:{} UUID:{} matches",
                    client_id, uuid
                );
            }
        } else {
            ds.ds_uuid.insert(client_id, client_ddef.uuid());
        }

        /*
         * XXX Until we are passed expected region info at start, we
         * can only compare the three downstairs to each other and move
         * forward if all three are the same.
         *
         * For now I'm using zero as an indication that we don't yet know
         * the valid values and non-zero meaning we have at least one
         * downstairs to compare with.
         *
         * 0 should never be a valid block size, so this hack will let us
         * move forward until we get the expected region info at startup.
         */
        let mut ddef = self.ddef.lock().unwrap();
        if ddef.block_size() == 0 {
            ddef.set_block_size(client_ddef.block_size());
            ddef.set_extent_size(client_ddef.extent_size());
            ddef.set_extent_count(client_ddef.extent_count());
            println!("Setting expected region info to: {:?}", client_ddef);
        }

        if ddef.block_size() != client_ddef.block_size()
            || ddef.extent_size().value != client_ddef.extent_size().value
            || ddef.extent_size().block_size_in_bytes()
                != client_ddef.extent_size().block_size_in_bytes()
            || ddef.extent_count() != client_ddef.extent_count()
        {
            // XXX Figure out if we can handle this error. Possibly not.
            panic!("New downstairs region info mismatch");
        }

        Ok(())
    }

    /*
     * Complete a downstairs operation.
     *
     * Returns true if the guest should be notified.
     */
    fn complete(
        &self,
        ds_id: u64,
        client_id: u8,
        data: Option<Bytes>,
        result: Result<(), CrucibleError>,
    ) -> Result<bool> {
        let mut work = self.downstairs.lock().unwrap();

        // Mark this ds_id for the client_id as completed.
        let notify_guest =
            work.complete(ds_id, client_id, data, result.clone())?;

        // Mark this downstairs as bad if this was a write or flush
        if let Some(err) = result.err() {
            if err == CrucibleError::UpstairsInactive {
                drop(work);

                println!(
                    "Saw CrucibleError::UpstairsInactive on client {}!",
                    client_id
                );
                self.ds_transition(client_id, DsState::Deactivated);
                self.set_inactive();
            }
            /*
             * After work.complete, it's possible that the job is gone
             * due to a retire check
             */
            else if let Some(job) = work.active.get_mut(&ds_id) {
                if matches!(
                    job.work,
                    IOop::Write {
                        dependencies: _,
                        eid: _,
                        data: _,
                        offset: _
                    } | IOop::Flush {
                        dependencies: _,
                        flush_number: _
                    }
                ) {
                    self.ds_transition(client_id, DsState::Failed);
                }
            }
        }

        Ok(notify_guest)
    }
}

#[derive(Debug)]
struct FlushInfo {
    flush_numbers: Vec<u64>,
    /*
     * The next flush number to use when a Flush is issued.
     */
    next_flush: u64,
}

impl FlushInfo {
    pub fn new() -> FlushInfo {
        FlushInfo {
            flush_numbers: Vec::new(),
            next_flush: 0,
        }
    }
    /*
     * Upstairs flush_info mutex must be held when calling this.
     * In addition, a downstairs request ID should be obtained at the
     * same time the next flush number is obtained, such that any IO that
     * is given a downstairs request number higher than the request number
     * for the flush will happen after this flush, never before.
     */
    fn next_flush(&mut self) -> u64 {
        let id = self.next_flush;
        self.next_flush += 1;
        id
    }
}

/*
 * States a downstairs can be in.
 * XXX This very much still under development. Most of these are place
 * holders and the final set of states will change.
 */
#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
enum DsState {
    /*
     * New connection
     */
    New,
    /*
     * Incompatible software version reported.
     */
    BadVersion,
    /*
     * Waiting for the minimum number of downstairs to be present.
     */
    WaitQuorum,
    /*
     * Incompatible region format reported.
     */
    _BadRegion,
    /*
     * We were connected, but did not transition all the way to
     * active before the connection went away.
     */
    Disconnected,
    /*
     * Comparing downstairs for consistency.
     */
    _Verifying,
    /*
     * Failed when attempting to make consistent.
     */
    _FailedRepair,
    /*
     * Ready for and/or currently receiving IO
     */
    Active,
    /*
     * IO attempts to this downstairs are failing at too high of a
     * rate, or it is not able to keep up, or it is having some
     * error such that we can no longer use it.
     */
    Failed,
    /*
     * This downstairs is being migrated to a new location
     */
    _Migrating,
    /*
     * This downstairs was active, but is now no longer connected.
     * We may have work for it in memory, so a replay is possible
     * if this downstairs reconnects in time.
     */
    Offline,
    /*
     * This downstairs was offline but is now back online and we are
     * sending it all the I/O it missed when it was unavailable.
     */
    Replay,
    /*
     * Another Upstairs has connected and is now active.
     */
    Deactivated,
}

/*
 * A unit of work for downstairs that is put into the hashmap.
 */
#[derive(Debug)]
struct DownstairsIO {
    ds_id: u64,    // This MUST match our hashmap index
    guest_id: u64, // The hahsmap ID from the parent guest work.
    work: IOop,
    /*
     * Hash of work status where key is the downstairs "client id" and the
     * hash value is the current state of the IO request with respect to the
     * upstairs.
     * The length and keys on this hashmap will be used to determine which
     * downstairs will receive the IO request.
     * XXX Determine if it is required for all downstairs to get an entry
     * or if by not putting a downstairs in the hash, if that is valid.
     */
    state: HashMap<u8, IOState>,
    /*
     * Has this been acked to the guest yet?
     */
    ack_status: AckStatus,
    /*
     * If the operation is a Read, this holds the resulting buffer
     */
    data: Option<Bytes>,
}

impl DownstairsIO {
    fn state_count(&self) -> WorkCounts {
        let mut wc: WorkCounts = Default::default();

        for state in self.state.values() {
            match state {
                IOState::New | IOState::InProgress => wc.active += 1,
                IOState::Error(_) => wc.error += 1,
                IOState::Skipped => wc.skipped += 1,
                IOState::Done => wc.done += 1,
            }
        }

        wc
    }
}

/*
 * Crucible to storage IO operations.
 */
#[derive(Debug, Clone)]
pub enum IOop {
    Write {
        dependencies: Vec<u64>, // Jobs that must finish before this
        eid: u64,
        offset: Block,
        data: Bytes,
    },
    Read {
        dependencies: Vec<u64>, // Jobs that must finish before this
        eid: u64,
        offset: Block,
        num_blocks: u64,
    },
    Flush {
        dependencies: Vec<u64>, // Jobs that must finish before this
        flush_number: u64,
    },
}

impl IOop {
    pub fn deps(&self) -> &Vec<u64> {
        match &self {
            IOop::Write {
                dependencies,
                eid: _eid,
                offset: _offset,
                data: _data,
            } => dependencies,
            IOop::Flush {
                dependencies,
                flush_number: _flush_number,
            } => dependencies,
            IOop::Read {
                dependencies,
                eid: _eid,
                offset: _offset,
                num_blocks: _num_blocks,
            } => dependencies,
        }
    }
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum IOState {
    // A new IO request.
    New,
    // The request has been sent to this tasks downstairs.
    InProgress,
    // The successful response came back from downstairs.
    Done,
    // The IO request should be ignored. Ex: we could be doing recovery and
    // we only want a specific downstairs to do that work.
    Skipped,
    // The IO returned an error.
    Error(CrucibleError),
}

impl fmt::Display for IOState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Make sure to right-align output on 4 characters
        match self {
            IOState::New => {
                write!(f, " New")
            }
            IOState::InProgress => {
                write!(f, "Sent")
            }
            IOState::Done => {
                write!(f, "Done")
            }
            IOState::Skipped => {
                write!(f, "Skip")
            }
            IOState::Error(e) => {
                write!(f, " Err: {:?}", e)
            }
        }
    }
}

#[derive(Debug)]
struct IOStateCount {
    new: [u32; 3],
    in_progress: [u32; 3],
    done: [u32; 3],
    skipped: [u32; 3],
    error: [u32; 3],
}

impl IOStateCount {
    fn new() -> IOStateCount {
        IOStateCount {
            new: [0; 3],
            in_progress: [0; 3],
            done: [0; 3],
            skipped: [0; 3],
            error: [0; 3],
        }
    }

    fn show_all(&mut self) {
        println!("   STATES      DS:0   DS:1   DS:2   TOTAL");
        self.show(IOState::New);
        self.show(IOState::InProgress);
        self.show(IOState::Done);
        self.show(IOState::Skipped);
        let e = CrucibleError::GenericError("x".to_string());
        self.show(IOState::Error(e));
    }

    fn show(&mut self, state: IOState) {
        let state_stat;
        match state {
            IOState::New => {
                state_stat = self.new;
                print!("    New        ");
            }
            IOState::InProgress => {
                state_stat = self.in_progress;
                print!("    Sent       ");
            }
            IOState::Done => {
                state_stat = self.done;
                print!("    Done       ");
            }
            IOState::Skipped => {
                state_stat = self.skipped;
                print!("    Skipped    ");
            }
            IOState::Error(_) => {
                state_stat = self.error;
                print!("    Error      ");
            }
        }
        let mut sum = 0;
        for ds_stat in state_stat {
            print!("{:4}   ", ds_stat);
            sum += ds_stat;
        }
        println!("{:4}", sum);
    }

    pub fn incr(&mut self, state: &IOState, cid: u8) {
        assert!(cid < 3);
        let cid = cid as usize;
        match state {
            IOState::New => {
                self.new[cid] += 1;
            }
            IOState::InProgress => {
                self.in_progress[cid] += 1;
            }
            IOState::Done => {
                self.done[cid] += 1;
            }
            IOState::Skipped => {
                self.skipped[cid] += 1;
            }
            IOState::Error(_) => {
                self.error[cid] += 1;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AckStatus {
    NotAcked,
    AckReady,
    Acked,
}

impl fmt::Display for AckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Make sure to right-align output on 8 characters
        match self {
            AckStatus::NotAcked => {
                write!(f, "NotAcked")
            }
            AckStatus::AckReady => {
                write!(f, "AckReady")
            }
            AckStatus::Acked => {
                write!(f, "   Acked")
            }
        }
    }
}

/*
 * Provides a shared Buffer that Read operations will write into.
 *
 * Originally BytesMut was used here, but it didn't guarantee that memory
 * was shared between cloned BytesMut objects.
 */
#[derive(Clone, Debug)]
pub struct Buffer {
    data: Arc<Mutex<Vec<u8>>>,
}

impl Buffer {
    pub fn from_vec(vec: Vec<u8>) -> Buffer {
        Buffer {
            data: Arc::new(Mutex::new(vec)),
        }
    }

    pub fn new(len: usize) -> Buffer {
        Buffer {
            data: Arc::new(Mutex::new(vec![0; len])),
        }
    }

    pub fn from_slice(buf: &[u8]) -> Buffer {
        let mut vec = Vec::<u8>::with_capacity(buf.len());
        for item in buf {
            vec.push(*item);
        }

        Buffer::from_vec(vec)
    }

    pub fn len(&self) -> usize {
        self.data.try_lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_vec(&self) -> MutexGuard<Vec<u8>> {
        self.data.try_lock().unwrap()
    }
}

#[test]
fn test_buffer_len() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);
}

#[test]
fn test_buffer_len_after_clone() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);

    let new_buffer = data.clone();
    assert_eq!(new_buffer.len(), READ_SIZE);
}

#[test]
#[should_panic(
    expected = "index out of bounds: the len is 512 but the index is 512"
)]
fn test_buffer_len_index_overflow() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);

    let mut vec = data.as_vec();
    assert_eq!(vec.len(), 512);

    for i in 0..(READ_SIZE + 1) {
        vec[i] = 0x99;
    }
}

#[test]
fn test_buffer_len_over_block_size() {
    const READ_SIZE: usize = 600;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);
}

/*
 * Inspired from Propolis block.rs
 *
 * The following are the operations that Crucible supports from outside
 * callers. We have extended this to cover a bunch of test operations as
 * well. The first three are the supported operations, the other operations
 * tell the upstairs to behave in specific ways.
 */
#[derive(Debug)]
enum BlockOp {
    Read { offset: Block, data: Buffer },
    Write { offset: Block, data: Bytes },
    Flush,
    GoActive,
    // Query ops
    QueryBlockSize { data: Arc<Mutex<u64>> },
    QueryTotalSize { data: Arc<Mutex<u64>> },
    QueryUpstairsActive { data: Arc<Mutex<bool>> },
    QueryUpstairsUuid { data: Arc<Mutex<Uuid>> },
    // Begin testing options.
    QueryExtentSize { data: Arc<Mutex<Block>> },
    QueryWorkQueue { data: Arc<Mutex<usize>> },
    // Send an update to all tasks that there is work on the queue.
    Commit,
    // Show internal work queue, return outstanding IO requests.
    ShowWork { data: Arc<Mutex<WQCounts>> },
}

/*
 * This structure is for tracking the underlying storage side operations
 * that map to a single Guest IO request. G to S stands for Guest
 * to Storage.
 *
 * The submitted hashmap is indexed by the request number (ds_id) for the
 * downstairs requests issued on behalf of this request.
 */
#[derive(Debug)]
struct GtoS {
    /*
     * Jobs we have submitted (or will soon submit) to the storage side
     * of the upstairs process to send on to the downstairs.
     * The key for the hashmap is the ds_id number in the hashmap for
     * downstairs work. The value is the buffer size of the operation in
     * blocks.
     */
    submitted: HashMap<u64, u64>,
    completed: Vec<u64>,

    /*
     * This buffer is provided by the guest request. If this is a read,
     * data will be written here.
     */
    guest_buffer: Option<Buffer>,

    /*
     * When we have an IO between the guest and crucible, it's possible
     * it will be broken into two smaller requests if the range happens
     * to cross an extent boundary. This hashmap is a list of those
     * buffers with the key being the downstairs request ID.
     *
     * Data moving in/out of this buffer will be encrypted or decrypted
     * depending on the operation.
     */
    downstairs_buffer: HashMap<u64, Bytes>,
    downstairs_buffer_sector_index: HashMap<u64, u128>,

    /*
     * Notify the caller waiting on the job to finish.
     * This is an Option for the case where we want to send an IO on behalf
     * of the Upstairs (not guest driven). Right now the only case where we
     * need that is to flush data to downstairs when the guest has not sent
     * us a flush in some time.  This allows us to free internal buffers.
     * If the sender is None, we know it's a request from the Upstairs and
     * we don't have to ACK it to anyone.
     */
    sender: Option<std_mpsc::Sender<Result<(), CrucibleError>>>,

    /*
     * Optional encryption context - Some if the corresponding Upstairs is
     * Some.
     */
    encryption_context: Option<EncryptionContext>,
}

impl GtoS {
    pub fn new(
        submitted: HashMap<u64, u64>,
        completed: Vec<u64>,
        guest_buffer: Option<Buffer>,
        downstairs_buffer: HashMap<u64, Bytes>,
        downstairs_buffer_sector_index: HashMap<u64, u128>,
        sender: Option<std_mpsc::Sender<Result<(), CrucibleError>>>,
        encryption_context: Option<EncryptionContext>,
    ) -> GtoS {
        GtoS {
            submitted,
            completed,
            guest_buffer,
            downstairs_buffer,
            downstairs_buffer_sector_index,
            sender,
            encryption_context,
        }
    }

    /*
     * When all downstairs jobs have completed, and all buffers have been
     * attached to the GtoS struct, we can do the final copy of the data
     * from upstairs memory back to the guest's memory.
     */
    #[instrument]
    fn transfer(&mut self) {
        if let Some(guest_buffer) = &mut self.guest_buffer {
            self.completed.sort_unstable();
            assert!(!self.completed.is_empty());

            let mut offset = 0;
            for ds_id in self.completed.iter() {
                let mut ds_vec =
                    self.downstairs_buffer.remove(ds_id).unwrap().to_vec();

                // if there's an encryption context, decrypt the
                // downstairs buffer.
                if let Some(context) = &self.encryption_context {
                    context.decrypt_in_place(
                        &mut ds_vec[..],
                        *self
                            .downstairs_buffer_sector_index
                            .get(ds_id)
                            .unwrap(),
                    );
                }

                // Copy over into guest memory.
                {
                    let _ignored =
                        span!(Level::TRACE, "copy to guest buffer").entered();
                    let mut vec = guest_buffer.as_vec();
                    for i in &ds_vec {
                        vec[offset] = *i;
                        offset += 1;
                    }
                }
            }
        } else {
            /*
             * Should this panic?  If the caller is requesting a transfer,
             * the guest_buffer should exist. If it does not exist, then
             * either there is a real problem, or the operation was a write
             * or flush and why are we requesting a transfer for those.
             */
            panic!("No guest buffer, no copy");
        }
    }

    /*
     * Notify corresponding BlockReqWaiter
     */
    pub fn notify(&mut self, result: Result<(), CrucibleError>) {
        /*
         * If present, send the result to the guest.  If this is a flush
         * issued on behalf of crucible, then there is no place to send
         * a result to.
         * XXX: If the guest is no longer listening and this returns an
         * error, do we care?  This could happen if the guest has
         * given up because an IO took too long, or other possible
         * guest side reasons.
         */
        if let Some(sender) = &self.sender {
            let _send_result = sender.send(result);
        }
    }
}

/**
 * This structure keeps track of work that Crucible has accepted from the
 * "Guest", aka, Propolis.
 *
 * The active is a hashmap of GtoS structures for all I/Os that are
 * outstanding. Either just created or in progress operations. The key
 * for a new job comes from next_gw_id and should always increment.
 *
 * Once we have decided enough downstairs requests are finished, we remove
 * the entry from the active and add the gw_id to the completed vec.
 *
 * TODO: The completed needs to implement some notify back to the Guest, and
 * it should probably be a ring buffer.
 */
#[derive(Debug)]
struct GuestWork {
    active: HashMap<u64, GtoS>,
    next_gw_id: u64,
    completed: AllocRingBuffer<u64>,
}

impl GuestWork {
    fn next_gw_id(&mut self) -> u64 {
        let id = self.next_gw_id;
        self.next_gw_id += 1;
        id
    }

    fn active_count(&mut self) -> usize {
        self.active.len()
    }

    /**
     * Move a GtoS job from the active to completed.
     * At this point we should have already sent the guest a message
     * saying their IO is done.
     */
    fn complete(&mut self, gw_id: u64) {
        let gtos_job = self.active.remove(&gw_id).unwrap();
        assert!(gtos_job.submitted.is_empty());
        self.completed.push(gw_id);
    }

    /*
     * When the required number of completions for a downstairs
     * ds_id have arrived, we call this method on the parent GuestWork
     * that requested them and include the Option<Bytes> from the IO.
     *
     * If this operation was a read, then we attach the Bytes read to the
     * GtoS struct for later transfer.
     *
     * A single GtoS job may have multiple downstairs jobs it created, so
     * we may not be done yet. When the required number of completions have
     * arrived from all the downstairs jobs we created, then we
     * can move forward with finishing up the guest work operation.
     * This may include moving/decrypting data buffers from completed reads.
     */
    #[instrument]
    fn ds_complete(
        &mut self,
        gw_id: u64,
        ds_id: u64,
        data: Option<Bytes>,
        result: Result<(), CrucibleError>,
    ) {
        /*
         * A gw_id that already finished and results were sent back to
         * the guest could still have an outstanding ds_id.
         */
        if let Some(gtos_job) = self.active.get_mut(&gw_id) {
            /*
             * If the ds_id is on the submitted list, then we will take it
             * off and, if it is a read, add the read result
             * buffer to the gtos job structure for later
             * copying.
             */
            if gtos_job.submitted.remove(&ds_id).is_some() {
                if let Some(data) = data {
                    /*
                     * The first read buffer will become the source for the
                     * final response back to the guest. This buffer will be
                     * combined with other buffers if the upstairs request
                     * required multiple jobs.
                     */
                    if gtos_job.downstairs_buffer.insert(ds_id, data).is_some()
                    {
                        /*
                         * Only the first successful read should fill the
                         * slot in the downstairs buffer for a ds_id. If
                         * more than one is trying to, then we have a
                         * problem.
                         */
                        panic!(
                            "gw_id:{} read buffer already present for {}",
                            gw_id, ds_id
                        );
                    }
                }
                gtos_job.completed.push(ds_id);
            } else {
                println!("gw_id:{} ({}) already removed???", gw_id, ds_id);
                assert!(gtos_job.completed.contains(&ds_id));
                panic!(
                    "{} Attempting to complete ds_id {} we already completed",
                    gw_id, ds_id
                );
            }

            /*
             * If all the downstairs jobs created for this have completed,
             * we can copy (if present) read data back to the guest buffer
             * they provided to us, and notify any waiters.
             */
            if gtos_job.submitted.is_empty() {
                if result.is_ok() && gtos_job.guest_buffer.is_some() {
                    gtos_job.transfer();
                }

                gtos_job.notify(result);
                self.complete(gw_id);
            }
        } else {
            /*
             * XXX This is just so I can see if ever does happen.
             */
            println!(
                "gw_id {} from removed job {} not on active list",
                gw_id, ds_id
            );
        }
    }
}

/**
 * Couple a BlockOp with a notifier for calling code.
 */
#[derive(Debug)]
pub struct BlockReq {
    op: BlockOp,
    send: std_mpsc::Sender<Result<(), CrucibleError>>,
}

impl BlockReq {
    // https://docs.rs/tokio/1.9.0/tokio/sync/mpsc\
    //     /index.html#communicating-between-sync-and-async-code
    // return the std::sync::mpsc Sender to non-tokio task callers
    fn new(
        op: BlockOp,
        send: std_mpsc::Sender<Result<(), CrucibleError>>,
    ) -> BlockReq {
        Self { op, send }
    }
}

/**
 * When BlockOps are sent to a guest, the calling function receives a
 * waiter that it can block on.
 */
pub struct BlockReqWaiter {
    recv: std_mpsc::Receiver<Result<(), CrucibleError>>,
}

impl BlockReqWaiter {
    fn new(
        recv: std_mpsc::Receiver<Result<(), CrucibleError>>,
    ) -> BlockReqWaiter {
        Self { recv }
    }

    pub fn block_wait(&mut self) -> Result<(), CrucibleError> {
        match self.recv.recv() {
            Ok(v) => v,
            Err(_) => crucible_bail!(RecvDisconnected),
        }
    }

    pub fn try_wait(&mut self) -> Option<Result<(), CrucibleError>> {
        match self.recv.try_recv() {
            Ok(v) => Some(v),
            Err(e) => match e {
                std_mpsc::TryRecvError::Empty => None,
                std_mpsc::TryRecvError::Disconnected => {
                    Some(Err(CrucibleError::RecvDisconnected))
                }
            },
        }
    }
}

/**
 * This is the structure we use to keep track of work passed into crucible
 * from the "Guest".
 *
 * Requests from the guest are put into the reqs VecDeque initially.
 *
 * A task on the Crucible side will receive a notification that a new
 * operation has landed on the reqs queue and will take action:
 *
 *   Pop the request off the reqs queue.
 *
 *   Copy (and optionally encrypt) any data buffers provided to us by the
 *   Guest.
 *
 *   Create one or more downstairs DownstairsIO structures.
 *
 *   Create a GtoS tracking structure with the id's for each
 *   downstairs task and the read result buffer if required.
 *
 *   Add the GtoS struct to the in GuestWork active work hashmap.
 *
 *   Put all the DownstairsIO structures on the downstairs work queue.
 *
 *   Send notification to the upstairs tasks that there is new work.
 *
 * Work here will be added to storage side queues and the responses will
 * be waited on and processed when they arrive.
 *
 * This structure and operations on in handle the translation between
 * outside requests and internal upstairs structures and work queues.
 */
#[derive(Debug)]
pub struct Guest {
    /*
     * Set to true when Upstairs reports as active.
     */
    active: Mutex<bool>,
    /*
     * New requests from outside go onto this VecDeque. The notify is how
     * the submission task tells the listening task that new work has been
     * added.
     */
    reqs: Mutex<VecDeque<BlockReq>>,
    notify: Notify,

    /*
     * When the crucible listening task has noticed a new IO request, it
     * will pull it from the reqs queue and create an GuestWork struct
     * as well as convert the new IO request into the matching
     * downstairs request(s). Each new GuestWork request will get a
     * unique gw_id, which is also the index for that operation into the
     * hashmap.
     *
     * It is during this process that data will encrypted. For a read, the
     * data is decrypted back to the guest provided buffer after all the
     * required downstairs operations are completed.
     */
    guest_work: Mutex<GuestWork>,
}

/*
 * These methods are how to add or checking for new work on the Guest struct
 */
impl Guest {
    pub fn new() -> Guest {
        Guest {
            active: Mutex::new(false),
            /*
             * Incoming I/O requests are added to this queue.
             */
            reqs: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            /*
             * The active hashmap is for in-flight I/O operations
             * that we have taken off the incoming queue, but we have not
             * received the response from downstairs.
             * Note that a single IO from outside may have multiple I/O
             * requests that need to finish before we can complete that IO.
             */
            guest_work: Mutex::new(GuestWork {
                active: HashMap::new(), // GtoS
                next_gw_id: 1,
                completed: AllocRingBuffer::with_capacity(2048),
            }),
        }
    }

    /*
     * This is used to submit a new BlockOp IO request to Crucible.
     */
    fn send(&self, op: BlockOp) -> BlockReqWaiter {
        let (send, recv) = std_mpsc::channel();

        self.reqs.lock().unwrap().push_back(BlockReq::new(op, send));
        self.notify.notify_one();

        BlockReqWaiter::new(recv)
    }

    /*
     * A crucible task will listen for new work using this.
     */
    async fn recv(&self) -> BlockReq {
        loop {
            if let Some(req) = self.reqs.lock().unwrap().pop_front() {
                return req;
            }
            self.notify.notified().await;
        }
    }

    pub fn byte_offset_to_block(
        &self,
        offset: u64,
    ) -> Result<Block, CrucibleError> {
        let bs = self.query_block_size()?;

        if (offset % bs) != 0 {
            crucible_bail!(OffsetUnaligned);
        }

        Ok(Block::new(offset / bs, bs.trailing_zeros()))
    }

    /*
     * `read` and `write` accept a block offset, and data must be a
     * multiple of block size.
     */
    pub fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let bs = self.query_block_size()?;

        if (data.len() % bs as usize) != 0 {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() as u64 != bs {
            crucible_bail!(BlockSizeMismatch);
        }

        let rio = BlockOp::Read { offset, data };
        Ok(self.send(rio))
    }

    pub fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let bs = self.query_block_size()?;

        if (data.len() % bs as usize) != 0 {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() as u64 != bs {
            crucible_bail!(BlockSizeMismatch);
        }

        let wio = BlockOp::Write { offset, data };
        Ok(self.send(wio))
    }

    /*
     * `read_from_byte_offset` and `write_to_byte_offset` accept a byte
     * offset, and data must be a multiple of block size.
     */
    pub fn read_from_byte_offset(
        &self,
        offset: u64,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.read(self.byte_offset_to_block(offset)?, data)
    }

    pub fn write_to_byte_offset(
        &self,
        offset: u64,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.write(self.byte_offset_to_block(offset)?, data)
    }

    pub fn flush(&self) -> Result<BlockReqWaiter, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        Ok(self.send(BlockOp::Flush))
    }

    pub fn set_active(&self) {
        let mut active = self.active.lock().unwrap();
        *active = true;
    }

    pub fn is_active(&self) -> bool {
        // A Guest is active if it's seen the Upstairs return that it's active
        let active = self.active.lock().unwrap();
        *active
    }

    pub fn activate(&self) -> Result<(), CrucibleError> {
        let mut waiter = self.send(BlockOp::GoActive);
        waiter.block_wait()?;

        // XXX Is this the right number of retries? The right delay between
        // retries?
        for _ in 0..10 {
            if self.query_is_active()? {
                println!(
                    "Upstairs is active, returning from activate function"
                );
                self.set_active();
                return Ok(());
            } else {
                println!(
                    "Upstairs is not yet active, waiting in activate function"
                );
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }

        Err(CrucibleError::UpstairsInactive)
    }

    pub fn query_is_active(&self) -> Result<bool, CrucibleError> {
        let data = Arc::new(Mutex::new(false));
        let active_query = BlockOp::QueryUpstairsActive { data: data.clone() };
        self.send(active_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn query_block_size(&self) -> Result<u64, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let data = Arc::new(Mutex::new(0));
        let size_query = BlockOp::QueryBlockSize { data: data.clone() };
        self.send(size_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn query_total_size(&self) -> Result<u64, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let data = Arc::new(Mutex::new(0));
        let size_query = BlockOp::QueryTotalSize { data: data.clone() };
        self.send(size_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn query_upstairs_uuid(&self) -> Result<Uuid, CrucibleError> {
        let data = Arc::new(Mutex::new(Uuid::default()));
        let uuid_query = BlockOp::QueryUpstairsUuid { data: data.clone() };
        self.send(uuid_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn query_extent_size(&self) -> Result<Block, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let data = Arc::new(Mutex::new(Block::new(0, 9)));
        let extent_query = BlockOp::QueryExtentSize { data: data.clone() };
        self.send(extent_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn query_work_queue(&self) -> Result<usize, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let data = Arc::new(Mutex::new(0));
        let work_queue_query = BlockOp::QueryWorkQueue { data: data.clone() };
        self.send(work_queue_query).block_wait()?;
        return Ok(*data.lock().map_err(|_| CrucibleError::DataLockError)?);
    }

    pub fn commit(&self) -> Result<(), CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.send(BlockOp::Commit).block_wait().unwrap();

        Ok(())
    }

    /*
     * Test call that displays the internal job queue on the upstairs, and
     * returns the guest side and downstairs side job queue depths.
     */
    pub fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        if !self.is_active() {
            return Err(CrucibleError::UpstairsInactive);
        }

        let wc = WQCounts {
            up_count: 0,
            ds_count: 0,
        };

        let data = Arc::new(Mutex::new(wc));
        let sw = BlockOp::ShowWork { data: data.clone() };
        self.send(sw).block_wait().unwrap();

        let wc = data.lock().unwrap();
        Ok(*wc)
    }
}

/*
 * Work Queue Counts, for debug ShowWork IO type
 */
#[derive(Debug, Copy, Clone)]
pub struct WQCounts {
    pub up_count: usize,
    pub ds_count: usize,
}

impl Default for Guest {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Target {
    #[allow(dead_code)]
    target: SocketAddrV4,
    ds_work_tx: watch::Sender<u64>,
    ds_active_tx: watch::Sender<bool>,
}

#[derive(Debug)]
struct Condition {
    target: SocketAddrV4,
    connected: bool,
}

/**
 * Send work to all the targets.
 * If a send fails, report an error.
 */
fn send_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        // println!("#### send to client {:?}", d_client.target);
        let res = d_client.ds_work_tx.send(val);
        if let Err(e) = res {
            println!(
                "#### error {:#?} Failed to notify {:?} of new work",
                e, d_client.target
            );
        }
    }
}

/**
 * Send active to all the targets.
 * If a send fails, print an error.
 */
fn send_active(t: &[Target]) {
    for d_client in t.iter() {
        // println!("#### send to client {:?}", d_client.target);
        let res = d_client.ds_active_tx.send(true);
        if let Err(e) = res {
            println!(
                "#### error {:#?} Failed 'active' notification to {:?}",
                e, d_client.target
            );
        }
    }
}

/**
 * We listen on the ds_done channel to know when enough of the downstairs
 * requests for a downstairs work task have finished and it is time to
 * complete any buffer transfers (reads) and then notify the guest that
 * their work has been completed.
 */
async fn up_ds_listen(up: &Arc<Upstairs>, mut ds_done_rx: mpsc::Receiver<u64>) {
    /*
     * Accept _any_ ds_done message, but work on the whole list of ackable
     * work.
     */
    while let Some(_ds_id) = ds_done_rx.recv().await {
        /*
         * XXX Do we need to hold the lock while we process all the
         * completed jobs?  We should be continuing to send message over
         * the ds_done_tx channel, so if new things show up while we
         * process the set of things we know are done now, then the
         * ds_done_rx.recv() should trigger when we loop.
         */
        let mut ack_list = up.downstairs.lock().unwrap().ackable_work();
        /*
         * This needs some sort order.  If we are not acking things in job
         * ID order, then we must use a queue or something that will allow
         * the jobs to be acked in the order they were completed on the
         * downstairs.
         */
        ack_list.sort_unstable();

        let mut gw = up.guest.guest_work.lock().unwrap();
        for ds_id_done in ack_list.iter() {
            let mut work = up.downstairs.lock().unwrap();

            let done = work.active.get_mut(ds_id_done).unwrap();
            /*
             * Make sure the job state has not changed since we made the
             * list.
             */
            if done.ack_status != AckStatus::AckReady {
                println!("Job {} no longer ready, skip for now", ds_id_done);
                continue;
            }

            let gw_id = done.guest_id;
            let ds_id = done.ds_id;
            assert_eq!(*ds_id_done, ds_id);

            let data = done.data.take();

            work.ack(ds_id);

            gw.ds_complete(gw_id, ds_id, data, work.result(ds_id));

            work.cdt_gw_work_done(ds_id, gw_id);

            work.retire_check(ds_id);
        }
    }
    println!("up_ds_listen loop done");
}

/**
 * The upstairs has received a new IO request from the guest. Here we
 * decide what to for that request.
 * For IO operations, we build the downstairs work and if required split
 * the single IO into multiple IOs to the downstairs. Once we have built
 * the work and updated the upstairs and downstairs work queues, we signal
 * to all the downstairs tasks there is new work for them to do.
 */
async fn process_new_io(
    up: &Arc<Upstairs>,
    dst: &[Target],
    req: BlockReq,
    lastcast: &mut u64,
) {
    /*
     * If any of the submit_* functions fail to send to the downstairs, they
     * return an error.  These are reported to the Guest.
     */
    match req.op {
        BlockOp::Read { offset, data } => {
            if let Err(e) = up.submit_read(offset, data, req.send.clone()) {
                let _ = req.send.send(Err(e));
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::Write { offset, data } => {
            if let Err(e) = up.submit_write(offset, data, req.send.clone()) {
                let _ = req.send.send(Err(e));
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::Flush => {
            if let Err(e) = up.submit_flush(Some(req.send.clone())) {
                let _ = req.send.send(Err(e));
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::GoActive => {
            send_active(dst);
            let _ = req.send.send(Ok(()));
        }
        // Query ops
        BlockOp::QueryBlockSize { data } => {
            *data.lock().unwrap() = up.ddef.lock().unwrap().block_size();
            let _ = req.send.send(Ok(()));
        }
        BlockOp::QueryTotalSize { data } => {
            *data.lock().unwrap() = up.ddef.lock().unwrap().total_size();
            let _ = req.send.send(Ok(()));
        }
        BlockOp::QueryUpstairsActive { data } => {
            *data.lock().unwrap() = up.is_active();
            let _ = req.send.send(Ok(()));
        }
        BlockOp::QueryUpstairsUuid { data } => {
            *data.lock().unwrap() = up.uuid;
            let _ = req.send.send(Ok(()));
        }
        // Testing options
        BlockOp::QueryExtentSize { data } => {
            // Yes, test only
            *data.lock().unwrap() = up.ddef.lock().unwrap().extent_size();
            let _ = req.send.send(Ok(()));
        }
        BlockOp::QueryWorkQueue { data } => {
            *data.lock().unwrap() =
                up.guest.guest_work.lock().unwrap().active_count();
            let _ = req.send.send(Ok(()));
        }
        BlockOp::ShowWork { data } => {
            *data.lock().unwrap() = show_all_work(up);
            let _ = req.send.send(Ok(()));
        }
        BlockOp::Commit => {
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Arg {
    up_count: u32,
    ds_count: u32,
    ds_state: Vec<DsState>,
}

/**
 * Update the counters used by dtrace probes.
 * This one method will update the fields of the
 * up_status counter.
 */
#[inline]
fn stat_update(up: &Arc<Upstairs>, msg: &str) {
    cdt::up_status!(|| {
        let arg = Arg {
            up_count: up.up_work_active(),
            ds_count: up.ds_work_active(),
            ds_state: up.ds_state_copy(),
        };
        (msg, arg)
    });
}

/*
 * This task will loop forever and wait for three downstairs to get into the
 * ready state. We are notified of that through the ds_status_rx channel.
 * Once we have three connections, we then also listen for work requests
 * to come over the guest channel.
 * If we lose a connection to downstairs, we just panic. XXX Eventually we
 * will handle that situation.
 */
async fn up_listen(
    up: &Arc<Upstairs>,
    dst: Vec<Target>,
    mut ds_status_rx: mpsc::Receiver<Condition>,
) {
    println!("Wait for all three downstairs to come online");
    let mut ds_count = 0u32;
    let mut lastcast = 1;

    stat_update(up, "start");

    loop {
        /*
         * For now, we need all three connections to proceed.
         */
        loop {
            tokio::select! {
                c = ds_status_rx.recv() => {
                    if let Some(c) = c {
                        if c.connected {
                            ds_count += 1;
                            println!(
                                "#### {:?} #### CONNECTED ######## {}/??",
                                c.target, ds_count,
                            );

                            if ds_count == 3 {
                                break;
                            }
                        } else {
                            println!(
                                "#### {:?} #### DISCONNECTED! ####",
                                c.target
                            );
                            ds_count -= 1;
                        }
                    } else {
                        println!("#### ? #### DISCONNECTED due to None! ####");
                        ds_count -= 1;
                    }
                }
                req = up.guest.recv() => {
                    // Wait for the pre-activate related messages
                    if matches!(req.op,
                        BlockOp::GoActive |
                        BlockOp::QueryUpstairsActive { data: _ } |
                        BlockOp::QueryUpstairsUuid { data: _ }
                    ) {
                        process_new_io(up, &dst, req, &mut lastcast).await;
                    } else {
                        println!(
                            "{} ignoring {:?}, not all \
                            downstairs are connected",
                            up.uuid, req.op
                        );
                    }
                }
            }
        }
        stat_update(up, "loop end");

        println!("All expected targets are online, Now accepting IO requests");
        /*
         * XXX The real check to transition from WaitQuorum to Active can
         * happen now, as all three are connected and ready. Right now this
         * happens in process_downstairs() and in the wrong way.
         */

        /*
         * Consider how the DsState::Failed is handled here, if necessary
         *
         * TODO: This logic does not cover the case where downstairs are
         * going up and down after the initial connection state.
         * Eventually the check here needs to verify that all downstairs
         * are still connected, and the actual work of making sure all
         * downstairs are in sync is done only with all downstairs
         * connected.
         */
        if !up.all_ds_state_match(DsState::WaitQuorum) {
            up.ds_state_show();
            panic!(
                "{} about to set all to active but not \
                all state is WaitQuorum!!",
                up.uuid
            );
        }

        up.ds_transition_all(DsState::Active);
        up.ds_state_show();

        up.set_active();

        stat_update(up, "active");
        /*
         * We have three connections, so we can now start listening for
         * more IO to come in. We also need to make sure our downstairs
         * stay connected, and we watch the ds_status_rx.recv() for that
         * to change which is our notification that a disconnect has
         * happened.
         *
         * In addition, we also send a periodic flush when we determine it
         * is time to do so. TODO: Figure out when is the best time to send
         * a upstairs generated flush.
         */
        let mut flush_check = deadline_secs(5);
        let mut show_work_interval = deadline_secs(5);

        loop {
            tokio::select! {
                _ = sleep_until(show_work_interval) => {
                    //show_all_work(up);
                    show_work_interval = deadline_secs(5);
                }
                c = ds_status_rx.recv() => {
                    // XXX We need some more thought here.  We can re-use
                    // this channel to enable flow control, but I'm not
                    // sure exactly how the FC will work.
                    if let Some(c) = &c {
                        if !c.connected {
                            println!("{} offline, pause IO ", c.target);
                        } else {
                            println!("{} online", c.target);
                        }
                    } else {
                        /*
                         * A None here means all senders were dropped, which
                         * means we should exit gracefully.
                         */
                        println!(
                            "Saw None in up_listen, draining in-flight IO"
                        );
                        show_all_work(up);

                        // Terminate all in-flight IO
                        loop {
                            up.guest.notify.notify_one();
                            tokio::select! {
                                req = up.guest.recv() => {
                                    let _ = req.send.send(
                                        Err(CrucibleError::GenericError(
                                            "Draining IO".to_string(),
                                        ))
                                    );
                                    println!("drained in-flight IO {:?}", req);
                                }
                                _ = sleep_until(deadline_secs(10)) => {
                                    println!(
                                        "Timed out for up.guest.recv drain, \
                                        returning gracefully"
                                    );
                                    return;
                                }
                            }
                        }
                    }
                }
                req = up.guest.recv() => {
                    process_new_io(up, &dst, req, &mut lastcast).await;
                }
                _ = sleep_until(flush_check) => {
                    /*
                     * This must fire every "flush_check" seconds to make sure
                     * we don't leave any work in the work queues longer
                     * than necessary.
                     */
                    if up.flush_needed() {
                        println!("Need a flush");

                        if let Err(e) = up.submit_flush(None) {
                            println!("flush send failed:{:?}", e);
                            // XXX What to do here?
                        } else {
                            send_work(&dst, 1);
                        }
                    }
                    /*
                     * Since this should run every time we check for
                     * a flush, we can also use this to update the dtrace
                     * counters with some regularity.
                     */
                    stat_update(up, "loop");

                    flush_check = deadline_secs(5);
                }
            }
        }
    }
}

/*
 * This is the main upstairs task that starts all the other async
 * tasks.
 *
 * XXX At the moment, this function is only half complete, and will
 * probably need a re-write.
 */
pub async fn up_main(opt: CrucibleOpts, guest: Arc<Guest>) -> Result<()> {
    match register_probes() {
        Ok(()) => {
            println!("DTrace probes registered okay");
        }
        Err(e) => {
            println!("Error registering DTrace probes: {:?}", e);
        }
    }

    let lossy = opt.lossy;
    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let up = Upstairs::new(&opt, RegionDefinition::default(), guest);

    /*
     * Use this channel to receive updates on target status from each task
     * we create to connect to a downstairs.
     */
    let (ds_status_tx, ds_status_rx) = mpsc::channel::<Condition>(32);

    /*
     * Use this channel to indicate in the upstairs that all downstairs
     * operations for a specific request have completed.
     */
    let (ds_done_tx, ds_done_rx) = mpsc::channel(500); // XXX 500?

    /*
     * spawn a task to listen for ds completed work which will then
     * take care of transitioning guest work structs to done.
     */
    let upc = Arc::clone(&up);
    tokio::spawn(async move {
        up_ds_listen(&upc, ds_done_rx).await;
    });

    let mut client_id = 0;
    /*
     * Create one downstairs task (dst) for each target in the opt
     * structure that was passed to us.
     */
    let dst = opt
        .target
        .iter()
        .map(|dst| {
            /*
             * Create the channel that we will use to request that the loop
             * check for work to do in the central structure.
             */
            let (ds_work_tx, ds_work_rx) = watch::channel(100);

            // Notify when it's time to go active.
            let (ds_active_tx, ds_active_rx) = watch::channel(false);

            let up = Arc::clone(&up);
            let t0 = *dst;
            let up_coms = UpComs {
                client_id,
                ds_work_rx,
                ds_status_tx: ds_status_tx.clone(),
                ds_done_tx: ds_done_tx.clone(),
                ds_active_rx,
            };
            tokio::spawn(async move {
                looper(t0, &up, up_coms, lossy).await;
            });
            client_id += 1;

            Target {
                target: *dst,
                ds_work_tx,
                ds_active_tx,
            }
        })
        .collect::<Vec<_>>();

    // Drop here, otherwise receivers will be kept waiting if looper quits
    drop(ds_done_tx);
    drop(ds_status_tx);

    /*
     * The final step is to call this function to wait for our downstairs
     * tasks to connect to their respective downstairs instance.
     * Once connected, we then take work requests from the guest and
     * submit them into the upstairs
     */
    up_listen(&up, dst, ds_status_rx).await;

    Ok(())
}

/*
 * Create a write DownstairsIO structure from an EID, and offset, and
 * the data buffer
 */
fn create_write_eob(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    eid: u64,
    offset: Block,
    data: Bytes,
) -> DownstairsIO {
    /*
     * Note to self:  Should the dependency list cover everything since
     * the last flush, or everything that is currently outstanding?
     */
    let awrite = IOop::Write {
        dependencies,
        eid,
        offset,
        data,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }

    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: awrite,
        state,
        ack_status: AckStatus::NotAcked,
        data: None,
    }
}

/*
 * Create a write DownstairsIO structure from an EID, and offset, and the
 * data buffer. Used for converting a guest IO read request into a
 * DownstairsIO that the downstairs can understand.
 */
fn create_read_eob(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    eid: u64,
    offset: Block,
    num_blocks: u64,
) -> DownstairsIO {
    let aread = IOop::Read {
        dependencies,
        eid,
        offset,
        num_blocks,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }

    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: aread,
        state,
        ack_status: AckStatus::NotAcked,
        data: None,
    }
}

/*
 * Create a flush DownstairsIO structure.
 */
fn create_flush(
    ds_id: u64,
    dependencies: Vec<u64>,
    flush_number: u64,
    guest_id: u64,
) -> DownstairsIO {
    let flush = IOop::Flush {
        dependencies,
        flush_number,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    DownstairsIO {
        ds_id,
        guest_id,
        work: flush,
        state,
        ack_status: AckStatus::NotAcked,
        data: None,
    }
}

/*
 * Debug function to display the work hashmap with status for all three of
 * the clients.
 */
fn show_all_work(up: &Arc<Upstairs>) -> WQCounts {
    let mut iosc: IOStateCount = IOStateCount::new();
    let up_count = up.guest.guest_work.lock().unwrap().active.len();

    let work = up.downstairs.lock().unwrap();
    let mut kvec: Vec<u64> = work.active.keys().cloned().collect::<Vec<u64>>();
    println!(
        "----------------------------------------------------------------"
    );
    println!(
        "     Crucible work queues:  Upstairs:{}  downstairs:{}",
        up_count,
        kvec.len()
    );
    if kvec.is_empty() {
        if up_count != 0 {
            show_guest_work(&up.guest);
        }
    } else {
        println!(
            "GW_ID      ACK   DSID   TYPE   ExtID BL_OFF BL_LEN \
            DS:0 DS:1 DS:2"
        );
        kvec.sort_unstable();
        for id in kvec.iter() {
            let mut io_eid = 0;
            let mut io_offset = 0;
            let mut io_len = 0;
            let job_type;
            let job = work.active.get(id).unwrap();
            match &job.work {
                IOop::Read {
                    dependencies: _dependencies,
                    eid,
                    offset,
                    num_blocks,
                } => {
                    job_type = "Read ".to_string();
                    io_eid = *eid;
                    io_offset = offset.value;
                    io_len = *num_blocks as usize;
                }
                IOop::Write {
                    dependencies: _dependencies,
                    eid,
                    offset,
                    data,
                } => {
                    job_type = "Write".to_string();
                    io_eid = *eid;
                    io_offset = offset.value;
                    io_len = data.len() / (1 << offset.shift);
                }
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                } => {
                    job_type = "Flush".to_string();
                }
            };
            let ack = job.ack_status;
            print!(
                " {:4}  {:8}  {:4}   {}   {:4}   {:4}   {:4} ",
                job.guest_id, ack, id, job_type, io_eid, io_offset, io_len
            );

            for cid in 0..3 {
                let state = job.state.get(&cid);
                match state {
                    Some(state) => {
                        print!("{} ", state);
                        iosc.incr(state, cid);
                    }
                    _x => {
                        print!("???? ");
                    }
                }
            }
            println!();
        }
        iosc.show_all();
        print!("Last Flush: ");
        for lf in work.ds_last_flush.iter() {
            print!("{} ", lf);
        }
        println!();
    }

    let done = work.completed.to_vec();
    let mut count = 0;
    print!("Downstairs last five completed:");
    for j in done.iter().rev() {
        count += 1;
        print!(" {:4}", j);
        if count > 5 {
            break;
        }
    }
    println!();
    drop(work);

    let up_done = up.guest.guest_work.lock().unwrap().completed.to_vec();
    print!("Upstairs last five completed:  ");
    let mut count = 0;
    for j in up_done.iter().rev() {
        count += 1;
        print!(" {:4}", j);
        if count > 5 {
            break;
        }
    }
    println!();
    drop(up_done);

    WQCounts {
        up_count,
        ds_count: kvec.len(),
    }
}

/*
 * Debug function to dump the guest work structure.
 * This does a bit while holding the mutex, so don't expect performance
 * to get better when calling it.
 *
 * TODO: make this one big dump, where we include the up.work.active
 * printing for each guest_work. It will be much more dense, but require
 * holding both locks for the duration.
 */
fn show_guest_work(guest: &Arc<Guest>) -> usize {
    println!("Guest work:  Active and Completed Jobs:");
    let gw = guest.guest_work.lock().unwrap();
    let mut kvec: Vec<u64> = gw.active.keys().cloned().collect::<Vec<u64>>();
    kvec.sort_unstable();
    for id in kvec.iter() {
        let job = gw.active.get(id).unwrap();
        println!(
            "GW_JOB active:[{:04}] S:{:?} C:{:?} ",
            id, job.submitted, job.completed
        );
    }
    let done = gw.completed.to_vec();
    println!("GW_JOB completed count:{:?} ", done.len());
    kvec.len()
}
