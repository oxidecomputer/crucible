use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crucible_common::*;
use crucible_protocol::*;

use anyhow::{anyhow, bail, Result};
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,
}

fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

async fn proc_frame(
    target: &SocketAddrV4,
    u: &Arc<Upstairs>,
    m: &Message,
    _fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
    client_id: u8,
) -> Result<()> {
    match m {
        Message::Imok => Ok(()),
        Message::WriteAck(rn) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            io_completed_wf(target, u, *rn, client_id)
        }
        Message::FlushAck(rn) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            // XXX Clear dirty bit, but we need the job info for that.
            io_completed_wf(target, u, *rn, client_id)
        }
        Message::ReadResponse(rn, data) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include which target responded
            save_read_buffer(target, u, *rn, data.clone())?;
            io_completed_read(target, u, *rn, client_id)
        }
        x => bail!("unexpected frame {:?}", x),
    }
}

/*
 * Convert a virtual block offset and length into a Vec of:
 *     Extent number (EID), Block offset, Length in bytes
 *
 * If the offset + length would fit into a single extent, then we only have
 * one tuple in the Vec.  If the offset + length does not fit into the
 * extent, then add a second tuple with EID + 1, 0, and whatever length
 * is remaining.
 *
 * We don't support a length greater than a single extent,
 * which means we only have to support spanning two extents at most.
 *
 */
fn extent_from_offset(
    up: &Arc<Upstairs>,
    offset: u64,
    len: usize,
) -> Result<Vec<(u64, u64, usize)>> {
    let ddef = up.ddef.lock().unwrap();

    // TODO Make asserts return error
    assert!(len as u64 >= ddef.block_size());
    assert!(len as u64 % ddef.block_size() == 0);
    assert!(offset % ddef.block_size() == 0);

    let space_per_extent = ddef.block_size() * ddef.extent_size();
    /*
     * XXX We only support a single region (downstairs).  When we grow to
     * support a LBA size that is larger than a single region, then we will
     * need to write more code.
     */
    let eid: u64 = offset / space_per_extent;
    assert!((len as u64) <= space_per_extent);
    assert!((eid as u32) < ddef.extent_count());

    let block_in_extent: u64 =
        (offset - (eid * space_per_extent)) / ddef.block_size();

    let mut res = Vec::new();

    /*
     * Check to see if our length extends past the end of this region.
     * If it fits, then we can just add the tuple to our Vec.  If it
     * does not fit, then we need to push two things into our Vec and
     * determine the length that each extent needs.
     */
    let data_blocks = (len as u64) / ddef.block_size();
    if data_blocks + block_in_extent <= ddef.extent_size() {
        res.push((eid, block_in_extent, len));
    } else {
        assert!((eid as u32) + 1 < ddef.extent_count());
        let new_len =
            (ddef.extent_size() - block_in_extent) * ddef.block_size();
        res.push((eid, block_in_extent, new_len as usize));
        res.push((eid + 1, 0, len - (new_len as usize)));
    }

    Ok(res)
}

/*
 * Decide what to do with a downstairs that has just connected and has
 * sent us information about its extents.
 *
 * This will eventually need to message the main thread so it knows
 * when it can starting doing I/O.
 */
fn process_downstairs(
    target: &SocketAddrV4,
    u: &Arc<Upstairs>,
    bs: u64,
    es: u64,
    ec: u32,
    versions: Vec<u64>,
) -> Result<()> {
    println!(
        "{} Evaluate new downstairs : bs:{} es:{} ec:{} versions: {:?}",
        target, bs, es, ec, versions
    );

    let mut v = u.versions.lock().unwrap();
    if v.len() == 0 {
        /*
         * This is the first version list we have, so
         * we will make it the original and compare
         * whatever comes next.
         */
        *v = versions;
        println!("Setting inital Extent versions to {:?}", v);

        /*
         * Create the dirty vector with the same length as our version list.
         * XXX Not used yet.
         */
        let mut d = u.dirty.lock().unwrap();
        *d = vec![false; v.len()];
    } else if v.len() != versions.len() {
        /*
         * I don't think there is much we can do here, the expected number
         * of flush numbers does not match.  Possibly we have grown one but
         * not the rest of the downstairs?
         */
        panic!(
            "Expected downstairs version \
              len:{:?} does not match new \
              downstairs:{:?}",
            v.len(),
            versions.len()
        );
    } else {
        /*
         * We already have a list of versions to compare with.  Make that
         * comparision now against this new list
         */
        let ver_cmp = v.iter().eq(versions.iter());
        if !ver_cmp {
            // XXX Recovery process should start here
            println!(
                "{} MISMATCH expected: {:?} != new: {:?}",
                target, v, versions
            );
        }
    }

    /*
     * XXX Here we have another workaround.  We don't know
     * the region info until after we connect to each
     * downstairs, but we share the ARC Upstairs before we
     * know what to expect.  For now I'm using zero as an
     * indication that we don't yet know the valid values
     * and non-zero meaning we have at least one downstairs
     * to compare with.  We might want to consider breaking
     * out the static config info into something different
     * that is updated on initial downstairs setup from the
     * structures we use for work submission.
     *
     * 0 should never be a valid block size
     */
    let mut ddef = u.ddef.lock().unwrap();
    if ddef.block_size() == 0 {
        ddef.set_block_size(bs);
        ddef.set_extent_size(es);
        ddef.set_extent_count(ec);
        println!("Global using: bs:{} es:{} ec:{}", bs, es, ec);
    }

    if ddef.block_size() != bs
        || ddef.extent_size() != es
        || ddef.extent_count() != ec
    {
        // XXX Figure out if we can hande this error.  Possibly not.
        panic!("New downstaris region info mismatch");
    }
    Ok(())
}

/**
 * When a read finishes, we need to keep track of the buffer we got back from
 * downstairs.  This will allow us to decrypt and copy the read data into the
 * buffer provided for us when the read was requested.
 */
fn save_read_buffer(
    target: &SocketAddrV4,
    up: &Arc<Upstairs>,
    rn: u64,
    buff: bytes::Bytes,
) -> Result<()> {
    let gw_id: u64;
    {
        let mut work = up.work.lock().unwrap();
        let job = work
            .active
            .get_mut(&rn)
            .ok_or_else(|| anyhow!("reqid {} is not active", rn))?;

        gw_id = job.guest_id;
    }

    let mut gw = up.guest.guest_work.lock().unwrap();
    /*
     * This gw_id should exist, But.. If a previous read has already finished
     * and we already sent that back to the guest, we could no longer have a
     * valid gw_id on the active list.  A rare but possible situation.
     */
    if let Some(gtos_job) = gw.active.get_mut(&gw_id) {
        /*
         * If the rn is on the submitted list, then we will take it off
         * and add the read result buffer to the gtos job structure for
         * later copying.
         *
         * If it's not, then verify our rn is already on the completed
         * list, just to catch any problems.
         */
        if gtos_job.submitted.remove(&rn).is_some() {
            /*
             * Take this job off of the submitted list.  The first read
             * buffer will become the source for the final response
             * back to the guest.  This buffer will be combined with other
             * buffers if the upstairs request required multiple jobs.
             */
            if gtos_job.downstairs_buffer.contains_key(&rn) {
                println!("Read buffer for {} already present at {}", gw_id, rn);
                // panic? XXX
            } else {
                println!("{} Read save_read_buffer for {}", target, rn);
                gtos_job.downstairs_buffer.insert(rn, buff);
            }
            gtos_job.completed.push(rn);
        } else {
            assert!(gtos_job.completed.contains(&rn));
        }
    }
    Ok(())
}

/*
 * This function is called when the upstairs task is notified that
 * a downstairs read has completed.  We add the read buffer to the
 * guest struct for later processing and determine if we have all
 * the results we need to finish up this read and transfer data
 * to the guest memory, or if there is more to do.
 */
fn io_completed_read(
    target: &SocketAddrV4,
    up: &Arc<Upstairs>,
    ds_id: u64,
    client_id: u8,
) -> Result<()> {
    let mut work = up.work.lock().unwrap();
    let counts = work.complete(ds_id, client_id)?;

    /*
     * We can send the results to the caller once we get one answer,
     * so this waiting for counts.active == 0 should not prevent the
     * guest_work from finishing.  TODO
     */
    if counts.active == 0 {
        let done = work.retire(ds_id);
        let gw_id = done.guest_id;
        println!(
            "{} Read  ds_id {} retired  gw_id:{:?}",
            target, ds_id, gw_id
        );

        drop(work);

        /*
         * If this read IO is done, it's time to check and see if the
         * guest IO is also done.  Some IOs from upstairs are broken into
         * several downstairs IOs and we need to see if our gw_id is
         * waiting for anything else.
         *
         * If it's not, then we can go ahead and do any read transfers
         * and move this gtos_job to completed state.
         */
        let mut gw = up.guest.guest_work.lock().unwrap();
        if let Some(gtos_job) = gw.active.get_mut(&gw_id) {
            /*
             * Reads will be removed from the submitted list when they
             * attach their buffer to the GtoS struct.
             * However, their may be other downstairs ids outstanding, so
             * we can't finish the guest work until those requests are
             * done as well.
             */
            if gtos_job.submitted.is_empty() {
                // Set in motion the final transfer of read buffers
                gtos_job.transfer();
                gw.complete(gw_id);
                //up.guest.complete_send(gw_id);
            } else {
                assert!(gtos_job.completed.contains(&ds_id));
            }
        }
        /*
         * TODO might want to consider a check for the else case when a
         * gw_id is not active.  Can we verify it is on the completed list?
         */
    }
    Ok(())
}

/*
 * This function is called when the upstairs task is notified that
 * a downstairs write or flush has completed.
 */
fn io_completed_wf(
    target: &SocketAddrV4,
    up: &Arc<Upstairs>,
    ds_id: u64,
    client_id: u8,
) -> Result<()> {
    let mut work = up.work.lock().unwrap();
    /*
     * XXX How do we deal with a write error, or a write that never
     * comes back?  We will need to update states to include an error
     * as a result.
     */
    let counts = work.complete(ds_id, client_id)?;

    /*
     * XXX We currently only look at marking a gw complete when
     * all the downstairs jobs have responded to us.
     * Eventually 2 out of 3 writes or flushes should trigger the
     * response to the guest.
     */
    if counts.active == 0 {
        let done = work.retire(ds_id);
        let gw_id = done.guest_id;
        println!(
            "{} Wr/Fl ds_id {} retired  gw_id:{:?}",
            target, ds_id, gw_id
        );

        drop(work);

        /*
         * If this IO is done, it's time to check and see if the
         * guest IO is also done.  Some IOs from upstairs are broken into
         * several downstairs IOs and we need to see if our gw_id is
         * waiting for anything else.
         */
        let mut gw = up.guest.guest_work.lock().unwrap();
        if let Some(gtos_job) = gw.active.get_mut(&gw_id) {
            let _ = gtos_job.submitted.remove(&ds_id).unwrap();
            gtos_job.completed.push(ds_id);
            /*
             * For multi-op writes, we may not be done yet/
             */
            if gtos_job.submitted.is_empty() {
                gw.complete(gw_id);
                //up.guest.complete_send(gw_id);
            } else {
                println!("{} gw_id:{} has more to do", target, gw_id);
            }
        } else {
            println!("{} No gw_id of {} is active", target, gw_id);
            // XXX assert it is on the completed list?  How long will
            // things on the completed list live?
        }
    }
    Ok(())
}

/*
 * This function is called by a worker task after the main task has added
 * work to the hashmap and notified the worker tasks that new work is ready
 * to be serviced.  The worker task will walk the hashmap and build a list
 * of new work that it needs to do.  It will then iterate through those
 * work items and send them over the wire to this tasks waiting downstaris.
 */
async fn io_send(
    u: &Arc<Upstairs>,
    fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
    client_id: u8,
) -> Result<()> {
    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     */
    let mut new_work = u.work.lock().unwrap().new_work(client_id);

    /*
     * Now we have a list of all the job IDs that are new for our client id.
     * Walk this list and process each job, marking it InProgress as we
     * do the work.  We do this in two loops because we can't hold the
     * lock for the hashmap while we do work, and if we release the lock
     * to do work, we would have to start over and look at all jobs in the
     * map to see if they are new.
     *
     * This also allows us to sort the job ids and do them in order they
     * were put into the hashmap, though I don't think that is required.
     */
    new_work.sort_unstable();

    for new_id in new_work.iter() {
        /*
         * We can't hold the hashmap mutex into the await send
         * below, so make a scope to get our next piece of work
         * from the hashmap and release the lock when we leave
         * this scope.
         */
        let job = u.work.lock().unwrap().in_progress(*new_id, client_id);

        match job {
            IOop::Write {
                dependencies,
                eid,
                block_offset,
                data,
            } => {
                /*
                 * XXX Before we write, we should set the dirty bit
                 * {
                 *    let eid = eid_from_offset(u, offset);
                 *    let dirt = u.dirty.lock().unwrap();
                 *}
                 */
                println!(
                    "[{}] Write ds_id:{} eid:{:?} bo:{:?}",
                    client_id, *new_id, eid, block_offset
                );
                fw.send(Message::Write(
                    *new_id,
                    eid,
                    block_offset,
                    data.clone(),
                ))
                .await?
            }
            IOop::Flush {
                dependencies,
                flush_numbers,
            } => {
                println!(
                    "Flush ds_id:{} dep:{:?} fl:{:?}",
                    *new_id, dependencies, flush_numbers
                );
                fw.send(Message::Flush(
                    *new_id,
                    dependencies.clone(),
                    flush_numbers.clone(),
                ))
                .await?
            }
            IOop::Read {
                eid,
                block_offset,
                blocks,
            } => {
                println!(
                    "Read  ds_id:{} eid:{:?} bo:{:?} blocks:{}",
                    *new_id, eid, block_offset, blocks,
                );
                fw.send(Message::ReadRequest(
                    *new_id,
                    eid,
                    block_offset,
                    blocks,
                ))
                .await?
            }
        }
    }
    Ok(())
}

/*
 * Once we have a connection to a downstairs, this task takes over and
 * handles both the initial negotiation and then watches the input for
 * changes, indicating that new work in on the work hashmap.  We will
 * walk the hashmap on the input signal and get any new work for this
 * specific downstairs and mark that job as in progress.
 */
async fn proc(
    target: &SocketAddrV4,
    input: &mut watch::Receiver<u64>,
    output: &mpsc::Sender<Condition>,
    up: &Arc<Upstairs>,
    mut sock: TcpStream,
    connected: &mut bool,
    client_id: u8,
) -> Result<()> {
    let (r, w) = sock.split();
    let mut fr = FramedRead::new(r, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(w, CrucibleEncoder::new());

    /*
     * As the "client", we must begin the negotiation.
     */
    fw.send(Message::HereIAm(1)).await?;

    /*
     * Don't wait more than 5 seconds to hear from the other side.
     * XXX Timeouts, timeouts: always wrong!  Some too short and some too long.
     */
    let mut deadline = deadline_secs(50);
    let mut negotiated = false;

    /*
     * To keep things alive, initiate a ping any time we have been idle for a
     * second.
     */
    let mut pingat = deadline_secs(10);
    let mut needping = false;

    loop {
        /*
         * XXX Just a thought here, could we send so much input that the
         * select would always have input.changed() and starve out the
         * fr.next() select?  Does this select ever work that way?
         */
        // println!("[{}] at the top of the loop", client_id);
        tokio::select! {
            _ = sleep_until(deadline) => {
                if !negotiated {
                    bail!("did not negotiate a protocol");
                } else {
                    bail!("inactivity timeout");
                }
            }
            _ = sleep_until(pingat), if needping => {
                fw.send(Message::Ruok).await?;
                needping = false;
            }
            _ = input.changed() => {
                /*
                let iv = *input.borrow();
                println!("[{}] Input changed with {}", client_id, iv);
                 */
                io_send(up, &mut fw, client_id).await?;
            }
            f = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                match f.transpose()? {
                    None => {
                        return Ok(())
                    }
                    Some(Message::YesItsMe(version)) => {
                        if negotiated {
                            bail!("negotiated already!");
                        }
                        /*
                         * XXX Valid version to compare with should come
                         * from main task
                         */
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = true;
                        needping = true;
                        deadline = deadline_secs(50);

                        /*
                         * Ask for the current version of all extents.
                         */
                        fw.send(Message::ExtentVersionsPlease).await?;
                    }
                    Some(Message::ExtentVersions(bs, es, ec, versions)) => {
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }
                        process_downstairs(target, up, bs, es, ec, versions)?;

                        /*
                         *  XXX I moved this here for now so we can use this
                         * signal to move forward with I/O.  Eventually this
                         * connected being true state should be sent from the
                         * initial YesItsMe case, and we send something else
                         * through a different watcher that tells the main
                         * task the list of versions, or something like that.
                         */

                        /*
                         * If we get here, we are ready to receive IO
                         */
                        *connected = true;
                        output.send(Condition {
                            target: *target,
                            connected: true,
                        }).await
                        .unwrap();
                    }
                    Some(m) => {
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }

                        proc_frame(&target, up, &m, &mut fw, client_id).await?;
                        deadline = deadline_secs(50);
                        pingat = deadline_secs(10);
                        needping = true;
                    }
                }
            }
        }
    }
}

/*
 * This task is responsible for the connection to and traffic between
 * a specific downstairs instance.  In here we handle taking work off of
 * the global work list and performing that work for a specific downstairs.
 */
async fn looper(
    target: SocketAddrV4,
    mut input: watch::Receiver<u64>,
    output: mpsc::Sender<Condition>,
    up: &Arc<Upstairs>,
    client_id: u8,
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
        println!("{0}[{1}] connecting to {0}", target, client_id);
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
                            println!("{0}[{1}] ok, connected to {0}",
                                target,
                                client_id);
                            break tcp;
                        }
                        Err(e) => {
                            println!("{0} connect to {0} failure: {1:?}",
                                target, e);
                            continue 'outer;
                        }
                    }
                }
            }
        };

        /*
         * Once we have a connected downstairs, the proc task takes over and
         * handles negiotation and work processing.
         */
        if let Err(e) = proc(
            &target,
            &mut input,
            &output,
            up,
            tcp,
            &mut connected,
            client_id,
        )
        .await
        {
            eprintln!("ERROR: {}: proc: {:?}", target, e);
        }

        if connected {
            output
                .send(Condition {
                    target,
                    connected: false,
                })
                .await
                .unwrap();
            connected = false;
        }
    }
}

#[derive(Debug)]
struct Work {
    active: HashMap<u64, IOWork>,
    next_id: u64,
    completed: AllocRingBuffer<u64>,
}

#[derive(Debug, Default)]
struct WorkCounts {
    active: u64,
    done: u64,
}

impl Work {
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
     * of the details of the request.
     */
    fn in_progress(&mut self, ds_id: u64, client_id: u8) -> IOop {
        let job = self.active.get_mut(&ds_id).unwrap();
        let oldstate = job.state.insert(client_id, IOState::InProgress);
        assert_eq!(oldstate, Some(IOState::New));
        job.work.clone()
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
     * Enqueue a new downstairs request.
     */
    fn enqueue(&mut self, io: IOWork) {
        self.active.insert(io.ds_id, io);
    }

    /**
     * Mark this downstairs request as complete for this client.  Returns
     * counts clients for which this request is still active or has been
     * completed already.
     */
    fn complete(&mut self, ds_id: u64, client_id: u8) -> Result<WorkCounts> {
        let job = self
            .active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;
        let oldstate = job.state.insert(client_id, IOState::Done);
        assert_ne!(oldstate, Some(IOState::Done));

        /*
         * Check to see if all I/O is completed:
         */
        let mut wc: WorkCounts = Default::default();
        for state in job.state.values() {
            match state {
                IOState::New | IOState::InProgress => wc.active += 1,
                IOState::Done | IOState::Skipped | IOState::Error => {
                    wc.done += 1;
                }
            }
        }

        Ok(wc)
    }

    /**
     * This request is now complete on all peers.  Remove it from the active set
     * and mark it in the completed ring buffer.
     */
    fn retire(&mut self, ds_id: u64) -> IOWork {
        assert!(!self.completed.contains(&ds_id));
        let old = self.active.remove(&ds_id).unwrap();
        self.completed.push(ds_id);
        old
    }
}

/*
 * XXX Track scheduled storage work in the central structure.  Have the
 * target management task check for work to do here by changing the value in
 * its watch::channel.  Have the main thread determine that an overflow of
 * work to do backing up in here means we need to do something like mark the
 * target as behind or institute some kind of back pressure, etc.
 */
#[derive(Debug)]
struct Upstairs {
    /*
     * A IOWork struct is added to this hashmap when the upstairs storage
     * task receives a new work request from a the upstairs guest side task
     * See details about the IOWork structure where it is defined.
     */
    work: Mutex<Work>,
    // The versions vec is not enough to solve a mismatch.  We really need
    // Generation number, flush number, and dirty bit for every extent
    // when resolving conflicts.
    versions: Mutex<Vec<u64>>,
    dirty: Mutex<Vec<bool>>,
    /*
     * The global description of the downstairs region we are using.
     * This allows us to verify each downstairs is the same, as well as
     * enables us to tranlate an LBA to an extent and block offset.
     */
    ddef: Mutex<RegionDefinition>,
    /*
     * The state of a downstairs connection, based on client ID
     * Ready here indicates it can receive IO.
     */
    downstairs: Mutex<Vec<DownstairsState>>,
    guest: Arc<Guest>,
}

/*
 * I think we will have more states.  If not, then this should just become
 * a bool.
 */
#[derive(Debug, Clone)]
pub enum DownstairsState {
    NotReady,
    Ready,
}

/*
 * A unit of work for downstairs that is put into the hashmap.
 */
#[derive(Debug)]
struct IOWork {
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
     * or if by not putting a downstars in the hash, if that is valid.
     */
    state: HashMap<u8, IOState>,
}

/*
 * Crucible to storage IO operations.
 */
#[derive(Debug, Clone)]
pub enum IOop {
    Write {
        dependencies: Vec<u64>, // Writes that must finish before this
        eid: u64,
        block_offset: u64,
        data: bytes::Bytes,
    },
    Read {
        eid: u64,
        block_offset: u64,
        blocks: u32,
    },
    Flush {
        dependencies: Vec<u64>, // Writes that must finish before this
        flush_numbers: Vec<u64>,
    },
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 *
 * New:  A new IO request.
 * InProgress:  The request has been sent to this tasks downstairs.
 * Done:        The response came back from downstairs.
 * Skipped:     The IO request should be ignored.  This situation could be
 *              A read that only needs one downstairs to answer, or we are
 *              doing recovery and we only want a specific downstairs to
 *              do that work.
 * Error:       The IO returned some error.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum IOState {
    New, // XXX Maybe name the Ready, or Available, or something else?
    InProgress,
    Done,
    Skipped,
    Error,
}

impl fmt::Display for IOState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            IOState::Error => {
                write!(f, " Err")
            }
        }
    }
}

/*
 * Inspired from Propolis block.rs
 *
 * The following are the operations that Crucible supports from outside callers.
 * We have extended this to cover a bunch of test operations as well.
 * The first three are the supported operations, the other operations
 * tell the upstaris to behave in specific ways.
 */
#[derive(Debug)]
pub enum BlockOp {
    Read { offset: u64, data: bytes::BytesMut },
    Write { offset: u64, data: bytes::Bytes },
    Flush,
    // Begin testing options.
    Commit,   // Send update to all tasks that there is work on the queue.
    ShowWork, // Show the status of the internal work hashmap and done Vec.
}

/*
 * This structure is for tracking the underlying storage side operations
 * that map to a single Guest IO request. G to S stands for Guest
 * to Storage.
 *
 * The submitted hashmap is indexd by the request number (ds_id) for the
 * downstairs requests issued on behaf of this reuqest.
 */
#[derive(Debug)]
struct GtoS {
    /*
     * Jobs we have submitted (or will soon submit) to the storage side
     * of the upstairs process to send on to the downstairs.
     * The key for the hashmap is the ds_id number in the hashmap for
     * downstairs work.  The value is the buffer size of the operation.
     */
    submitted: HashMap<u64, usize>,
    completed: Vec<u64>,
    /*
     * This buffer is provided by the guest request.  It is either where
     * data will come from on a write, or the location where data will
     * be put for a read.
     */
    guest_buffer: Option<bytes::BytesMut>,
    /*
     * When we have an IO between the guest and crucible, it's possible
     * it will be broken into two smaller requests if the range happens
     * to cross an extent boundary.  This hashmap is a list of those
     * buffers with the key being the downstairs request ID.
     *
     * Data moving in/out of this buffer will be encrypted or decrypted
     * depending on the operation.
     */
    downstairs_buffer: HashMap<u64, bytes::Bytes>,
}

impl GtoS {
    pub fn new(
        submitted: HashMap<u64, usize>,
        completed: Vec<u64>,
        guest_buffer: Option<bytes::BytesMut>,
        downstairs_buffer: HashMap<u64, bytes::Bytes>,
    ) -> GtoS {
        GtoS {
            submitted,
            completed,
            guest_buffer,
            downstairs_buffer,
        }
    }
    /*
     * When all downstairs jobs have completed, and all buffers have been
     * attached to the GtoS struct, we can do the final copy of the data
     * from upstairs memory back to the guest's memory.
     *
     * XXX When encryption/decryption is supported, here is where you will be
     * writing the code to decrypt.
     */
    pub fn transfer(&mut self) {
        if let Some(guest_buffer) = &mut self.guest_buffer {
            self.completed.sort_unstable();
            guest_buffer.clear();
            for rn in self.completed.iter() {
                // println!("Copy buff from {:?}", rn);
                let ds_buf = self.downstairs_buffer.remove(rn).unwrap();
                guest_buffer.put(ds_buf);
            }
            println!(
                "Final data copy {:?} to {:p}",
                self.completed,
                guest_buffer.as_ptr(),
            );
        } else {
            /*
             * Should this panic?  If the caller is requesting a transfer,
             * the guest_buffer should exist.  If it does not exist, then
             * either there is a real problem, or the operation was a write
             * or flush and why are we requesting a transfer for those.
             */
            panic!("No guest buffer, no copy");
        }
    }
}

/*
 * This structure keeps track of work that Crucible has accepted from the
 * "Guest", aka, Propolis.
 *
 * The active is a hashmap of GtoS structures for all I/Os that are
 * outstanding.  Either just created or in progress operations.  The key
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
    completed: Vec<u64>,
}

impl GuestWork {
    fn next_gw_id(&mut self) -> u64 {
        let id = self.next_gw_id;
        self.next_gw_id += 1;
        id
    }

    /*
     * Move a GtoS job from the active to completed.
     * TODO, Should the logic for when to complete live in this
     * method as well?
     */
    fn complete(&mut self, gw_id: u64) {
        let _gtos_job = self.active.remove(&gw_id).unwrap();
        self.completed.push(gw_id);
    }
}

/*
 * This is the structure we use to keep track of work passed into crucible
 * from the the "Guest".
 *
 * Requests from the guest are put into the reqs VecDeque initally.
 *
 * A task on the Crucible side will receive a notification that a new
 * operation has landed on the reqs queue and will take action:
 *   Pop the request off the reqs queue.
 *   Copy and encrypt any data buffers provided to us by the Guest.
 *   Create one or more downstairs IOWork structures.
 *   Create a GtoS tracking structure with the id's for each
 *   downstairs task and the read result buffer if required.
 *   Add the GtoS struct to the in GuestWork active work hashmap.
 *   Put all the IOWork strucutres on the downstairs work queue.
 *   Send notification to the upstairs tasks that there is new work.
 *
 * Work here will be added to storage side queues and the responses will
 * be waited on and processed when they arrive.
 *
 * This structure and operations on in handle the translation between
 * outside requests and internal upstairs structures and work queues.
 */
#[derive(Debug)]
struct Guest {
    /*
     * New requests from outside go onto this VecDeque.  The notify is how
     * the submittion task tells the listening task that new work has been
     * added.
     */
    reqs: Mutex<VecDeque<BlockOp>>,
    notify: Notify,
    /*
     * When the crucible listening task has noticed a new IO request, it will
     * pull it from the reqs queue and create an GuestWork struct as well as
     * convert the new IO request into the matching downstairs request(s).
     * Each new GuestWork request will get a unique gw_id, which is also
     * the index for that operation into the hashmap.
     *
     * It is during this process that data will encrypted.  For a read, the
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
            /*
             * Incoming I/O requests are added to this queue.
             */
            reqs: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            /*
             * The in_progress hashmap is for in-flight I/O operations
             * that we have taken off the incoming queue, but we have not
             * received the response from downstairs.
             * Note that a single IO from outside may have multiple I/O
             * requests that need to finish before we can complete that IO.
             */
            guest_work: Mutex::new(GuestWork {
                active: HashMap::new(), // GtoS
                next_gw_id: 1,
                completed: Vec::new(),
            }),
        }
    }

    /*
     * Get the next available ID for a new job in the active hashmap.
     */
    pub fn next_gw_id(&self) -> u64 {
        let mut gw = self.guest_work.lock().unwrap();
        gw.next_gw_id()
    }

    /*
     * This is used to submit a new IO request to Crucible.
     */
    pub fn send(&self, req: BlockOp) {
        self.reqs.lock().unwrap().push_back(req);

        self.notify.notify_one();
    }

    /*
     * A crucible task will listen for new work using this.
     */
    pub async fn recv(&self) -> BlockOp {
        loop {
            if let Some(req) = self.reqs.lock().unwrap().pop_front() {
                return req;
            }
            self.notify.notified().await;
        }
    }
}

struct Target {
    #[allow(dead_code)]
    target: SocketAddrV4,
    input: watch::Sender<u64>,
}

#[derive(Debug)]
struct Condition {
    target: SocketAddrV4,
    connected: bool,
}

fn main() -> Result<()> {
    let opt = opts()?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(10)
        .thread_name("crucible-tokio")
        .enable_all()
        .build()
        .unwrap();

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the run_scope() function to submit test work.
     */
    let guest = Arc::new(Guest::new());

    runtime.spawn(up_main(opt, guest.clone()));
    println!("runtime is spawned");

    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside (Propolis).
     * This is essentially how we create test commands and send them
     * to the up_listen task.
     */
    //runtime.spawn(run_scope(prop_work));

    /*
     * XXX The rest of this is just test code, and will be removed shortly
     * as we transition to a library/crate for crucible.
     */
    std::thread::sleep(std::time::Duration::from_secs(5));
    //_run_big_workload(&guest, 2)?;
    for _ in 0..1000 {
        run_single_workload(&guest)?;
        /*
         * This helps us get around async/non-async issues.
         * Keeing this process busy means some async tasks will never get
         * time to run.  Give a little pause here and let some other
         * tasks go.  Yes, this is a hack.  XXX
         */
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    // show_guest_work(&guest);
    println!("Tests done, wait");
    std::thread::sleep(std::time::Duration::from_secs(5));
    // show_guest_work(&guest);
    println!("Tests done");
    std::thread::sleep(std::time::Duration::from_secs(10));
    println!("all Tests done");
    Ok(())
}

/*
 * Send work to all the targets on this vector.
 * This can be much simpler, but we need to (eventually) take special action
 * when we fail to send a message to a task.
 */
fn _send_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        // println!("#### send to client {:?}", d_client.target);
        let res = d_client.input.send(val);
        if let Err(e) = res {
            panic!("#### error {:#?} sending work to {:?}", e, d_client.target);
            /*
             * XXX
             * Write more code for this error,  If one downstairs
             * never receives a request, it may get picked up on the
             * next request.  However, if the downstairs has gone away,
             * then action will need to be taken, and soon.
             */
        }
    }
}

/*
 * This is a test workload that generates a write spanning an extent
 * then trys to read the same.
 */
fn run_single_workload(guest: &Arc<Guest>) -> Result<()> {
    let my_offset = 512 * 99;
    let mut data = BytesMut::with_capacity(512 * 2);
    for seed in 4..6 {
        data.put(&[seed; 512][..]);
    }
    let data = data.freeze();
    let wio = BlockOp::Write {
        offset: my_offset,
        data,
    };
    guest.send(wio);

    guest.send(BlockOp::Flush);
    //guest.send(BlockOp::ShowWork);

    let read_offset = my_offset;
    const READ_SIZE: usize = 1024;
    println!("generate a read 1");
    let mut data = BytesMut::with_capacity(READ_SIZE);
    data.put(&[0x99; READ_SIZE][..]);
    println!("send read, data at {:p}", data.as_ptr());
    let rio = BlockOp::Read {
        offset: read_offset,
        data,
    };
    guest.send(rio);
    // guest.send(BlockOp::ShowWork);

    println!("Final offset: {}", my_offset);

    Ok(())
}
/*
 * This is basically just a test loop that generates a workload then sends the
 * workload to Crucible.
 */
fn _run_big_workload(guest: &Arc<Guest>, loops: u32) -> Result<()> {
    for _ll in 0..loops {
        let mut my_offset: u64 = 0;
        for olc in 0..10 {
            for lc in 0..100 {
                let seed = (my_offset % 255) as u8;
                let mut data = BytesMut::with_capacity(512);
                data.put(&[seed; 512][..]);
                let data = data.freeze();
                let wio = BlockOp::Write {
                    offset: my_offset,
                    data,
                };
                println!("[{}][{}] send write  offset:{}", olc, lc, my_offset);
                guest.send(wio);

                let read_offset = my_offset;
                const READ_SIZE: usize = 512;
                let mut data = BytesMut::with_capacity(READ_SIZE);
                data.put(&[0x99; READ_SIZE][..]);
                println!(
                    "[{}][{}] send read   offset:{}, data at {:p}",
                    olc,
                    lc,
                    read_offset,
                    data.as_ptr()
                );
                let rio = BlockOp::Read {
                    offset: read_offset,
                    data,
                };
                guest.send(rio);

                println!("[{}][{}] send flush", olc, lc);
                guest.send(BlockOp::Flush);
                // guest.send(BlockOp::ShowWork);
                my_offset += 512;
            }
        }
        println!("Final offset: {}", my_offset);
    }
    Ok(())
}

async fn _run_scope(guest: Arc<Guest>) -> Result<()> {
    let scope =
        crucible_scope::Server::new(".scope.upstairs.sock", "upstairs").await?;
    let mut my_offset = 512 * 99;
    scope.wait_for("Send all the IOs").await;
    loop {
        let mut data = BytesMut::with_capacity(512 * 2);
        for seed in 44..46 {
            data.put(&[seed; 512][..]);
        }
        let data = data.freeze();
        let wio = BlockOp::Write {
            offset: my_offset,
            data,
        };
        my_offset += 512 * 2;
        scope.wait_for("write 1").await;
        println!("send write 1");
        guest.send(wio);
        scope.wait_for("show work").await;
        guest.send(BlockOp::ShowWork);

        let mut read_offset = 512 * 99;
        const READ_SIZE: usize = 4096;
        for _ in 0..4 {
            let mut data = BytesMut::with_capacity(READ_SIZE);
            data.put(&[0x99; READ_SIZE][..]);
            println!("send read, data at {:p}", data.as_ptr());
            let rio = BlockOp::Read {
                offset: read_offset,
                data,
            };
            // scope.wait_for("send Read").await;
            guest.send(rio);
            read_offset += READ_SIZE as u64;
            // scope.wait_for("show work").await;
            guest.send(BlockOp::ShowWork);
        }

        // scope.wait_for("Flush step").await;
        println!("send flush");
        guest.send(BlockOp::Flush);

        let mut data = BytesMut::with_capacity(512);
        data.put(&[0xbb; 512][..]);
        let data = data.freeze();
        let wio = BlockOp::Write {
            offset: my_offset,
            data,
        };
        // scope.wait_for("write 2").await;
        println!("send write 2");
        guest.send(wio);
        my_offset += 512;
        // scope.wait_for("show work").await;
        guest.send(BlockOp::ShowWork);
        //scope.wait_for("at the bottom").await;
    }
}

/*
 * When we have a guest read request with offset and buffer, take them and
 * build both the upstairs work guest tracking struct as well as the downstairs
 * work struct. Once both are ready, submit them to the required places.
 */
fn guest_submit_read(up: &Arc<Upstairs>, offset: u64, data: bytes::BytesMut) {
    /*
     * We need to know the block size to allow us to convert between
     * bytes and blocks.  Bytes for when we have to slice buffers,
     * blocks for what we send to the downstairs IO.
     */
    let block_size: u32;
    {
        let ddef = up.ddef.lock().unwrap();
        block_size = ddef.block_size() as u32;
    }

    /*
     * Get the next ID for the guest work struct we will make at the
     * end.  This ID is also put into the IO struct we create that
     * handles the operation(s) on the storage side.
     */
    let gw_id: u64 = up.guest.next_gw_id();

    /*
     * Given the offset and buffer size, figure out what extent and
     * block offset that translates into.  Keep in mind that an offset
     * and length may span two extents, and eventually, TODO, two regions.
     */
    let nwo = extent_from_offset(up, offset, data.len()).unwrap();
    println!(
        "nwo: {:?} from offset:{} data: {:p} len:{}",
        nwo,
        offset,
        data.as_ptr(),
        data.len()
    );
    /*
     * Create the list of downstairs request numbers (ds_id) we created
     * on behalf of this guest job.
     */
    let mut sub = HashMap::new();
    let mut ds_work = Vec::new();
    let mut next_id: u64;

    /*
     * Now create a downstairs work job for each (eid, bi, len) returned
     * from extent_from_offset
     */
    for (eid, bo, len) in nwo {
        let blocks: u32 = len as u32 / block_size;
        {
            let mut work = up.work.lock().unwrap();
            next_id = work.next_id();
        }
        sub.insert(next_id, len);
        let wr = create_read_eob(next_id, gw_id, eid, bo, blocks);
        ds_work.push(wr);
    }

    println!("READ:  gw_id:{} rns:{:?}", gw_id, sub,);
    /*
     * New work created, add to the guest_work HM
     */
    assert!(!sub.is_empty());
    let new_gtos = GtoS::new(sub, Vec::new(), Some(data), HashMap::new());
    {
        let mut gw = up.guest.guest_work.lock().unwrap();
        gw.active.insert(gw_id, new_gtos);
    }

    let mut work = up.work.lock().unwrap();
    for wr in ds_work {
        work.enqueue(wr);
    }
}

/*
 * When we have a guest write request with offset and buffer, take them and
 * build both the upstairs work guest tracking struct as well as the downstairs
 * work struct. Once both are ready, submit them to the required places.
 */
fn guest_submit_write(
    up: &Arc<Upstairs>,
    offset: u64,
    data: bytes::Bytes, // Where the data comes from
) {
    /*
     * Get the next ID for the guest work struct we will make at the
     * end.  This ID is also put into the IO struct we create that
     * handles the operation(s) on the storage side.
     */
    let gw_id: u64 = up.guest.next_gw_id();

    /*
     * Given the offset and buffer size, figure out what extent and
     * block offset that translates into.  Keep in mind that an offset
     * and length may span two extents, and eventually XXX, two regions.
     */
    let nwo = extent_from_offset(up, offset, data.len()).unwrap();
    println!(
        "nwo: {:?} from offset:{} data: {:p} len:{}",
        nwo,
        offset,
        data.as_ptr(),
        data.len()
    );

    /*
     * Now create a downstairs work job for each (eid, bi, len) returned
     * from extent_from_offset
     */

    /*
     * Create the list of downstairs request numbers (ds_id) we created
     * on behalf of this guest job.
     */
    let mut sub = HashMap::new();
    let mut ds_work = Vec::new();
    let mut next_id: u64;
    let mut cur_offset = 0;
    for (eid, bo, len) in nwo {
        {
            let mut work = up.work.lock().unwrap();
            next_id = work.next_id();
        }
        /*
         * XXX This is where encryption will happen, which will probably
         * mean a refactor of how this job is built.
         */
        let sub_data = data.slice(cur_offset..(cur_offset + len));
        sub.insert(next_id, len);

        let wr = create_write_eob(next_id, gw_id, eid, bo, sub_data);
        ds_work.push(wr);
        cur_offset = len;
    }
    println!("WRITE: gw_id:{} rns:{:?}", gw_id, sub,);
    /*
     * New work created, add to the guest_work HM
     */
    let new_gtos = GtoS::new(sub, Vec::new(), None, HashMap::new());
    {
        let mut gw = up.guest.guest_work.lock().unwrap();
        gw.active.insert(gw_id, new_gtos);
    }

    let mut work = up.work.lock().unwrap();
    for wr in ds_work {
        work.enqueue(wr);
    }
}

/*
 * Turn a guest flush request into both a guest_work active operaion
 * and put an entry on the work hashmap for downstairs.
 */
fn guest_submit_flush(up: &Arc<Upstairs>) {
    /*
     * Get the next ID for our new guest work job
     */
    let gw_id: u64 = up.guest.next_gw_id();

    /*
     * Build the flush request, and take note of the request ID that
     * will be assigned to this new piece of work.
     */
    let next_id: u64;
    let fl: IOWork;
    {
        // XXX double locking... think about this one
        let mut work = up.work.lock().unwrap();
        next_id = work.next_id();
        let ver = up.versions.lock().unwrap();
        fl = create_flush(next_id, ver.clone(), gw_id);
    }

    let mut sub = HashMap::new();
    sub.insert(next_id, 0);

    println!("FLUSH: gw_id:{} rns:{:?}", gw_id, sub,);
    let new_gtos = GtoS::new(sub, Vec::new(), None, HashMap::new());
    {
        let mut gw = up.guest.guest_work.lock().unwrap();
        gw.active.insert(gw_id, new_gtos);
    }

    let mut work = up.work.lock().unwrap();
    work.enqueue(fl);
}

/*
 * This task will loop forever and watch the Guest structure for new IO
 * operations showing up.  When one is detected, the type is checked and the
 * operation is translated into the corresponding upstairs IO type and put on
 * the internal upstairs queue.
 */
async fn up_listen(up: &Arc<Upstairs>, dst: Vec<Target>) {
    /*
     * XXX Once we move this function to being called after all downstairs are
     * online, we can remove this sleep
     */
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut lastcast = 1;
    loop {
        let req = up.guest.recv().await;
        match req {
            BlockOp::Read { offset, data } => {
                println!("recv read data at  {:p}", data.as_ptr());
                guest_submit_read(up, offset, data);
                // Send the message that there is new work to do
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::Write { offset, data } => {
                guest_submit_write(up, offset, data);
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::Flush => {
                guest_submit_flush(up);
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::ShowWork => {
                show_guest_work(&up.guest);
            }
            BlockOp::Commit => {
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
        }
    }
}

/*
 * This is the main upstairs task that starts all the other async
 * tasks.
 * XXX At the moment, this function is only half complete, and will
 * probably need a re-write.
 */
async fn up_main(opt: Opt, guest: Arc<Guest>) -> Result<()> {
    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let up = Arc::new(Upstairs {
        work: Mutex::new(Work {
            active: HashMap::new(),
            completed: AllocRingBuffer::with_capacity(2048),
            next_id: 1000,
        }),
        versions: Mutex::new(Vec::new()),
        dirty: Mutex::new(Vec::new()),
        ddef: Mutex::new(RegionDefinition::default()),
        downstairs: Mutex::new(Vec::with_capacity(opt.target.len())),
        guest,
    });

    /*
     * Use this channel to receive updates on target status from each task
     * we create to connect to a downstairs.
     */
    let (ctx, mut crx) = mpsc::channel::<Condition>(32);

    let mut client_id = 0;
    /*
     * Create one downstaris task (dst) for each target in the opt
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
            let (itx, irx) = watch::channel(0);

            let up = Arc::clone(&up);
            let ctx = ctx.clone();
            let t0 = *dst;
            tokio::spawn(async move {
                looper(t0, irx, ctx, &up, client_id).await;
            });
            client_id += 1;

            Target {
                target: *dst,
                input: itx,
            }
        })
        .collect::<Vec<_>>();

    /*
     * Create a task to listen for work from outside.
     *
     * The role of this task is to move work between the outside
     * work queue and the internal Upstairs work queue, as well as send
     * completion messages and/or copy data back to the outside.
     *
     * XXX This needs a little more work.  We should not start to listen
     * to the outside until we know that all our downstairs are ready to
     * take IO operations.
     */
    tokio::spawn(async move {
        up_listen(&up, dst).await;
    });

    // async tasks need to tell us they are alive, but they also need to
    // tell us the extent list from any attached downstairs.
    // That part is not connected yet. XXX
    let mut ds_count = 0u32;
    loop {
        let c = crx.recv().await.unwrap();
        if c.connected {
            ds_count += 1;
            println!(
                "#### {:?} #### CONNECTED ######## {}/{}",
                c.target,
                ds_count,
                opt.target.len()
            );
        } else {
            println!("#### {:?} #### DISCONNECTED! ####", c.target);
            ds_count -= 1;
        }
        /*
         * We need some additional way to indicate that this upstairs is ready
         * to receive work.  Just connecting to n downstairs is not enough,
         * we need to also know that they all have the same data.
         */
    }
}

/*
 * Create a write IOWork structure from an EID, and offset, and the data buffer
 */
fn create_write_eob(
    ds_id: u64,
    gw_id: u64,
    eid: u64,
    block_offset: u64,
    data: bytes::Bytes,
) -> IOWork {
    let awrite = IOop::Write {
        dependencies: Vec::new(), // XXX Coming soon
        eid,
        block_offset,
        data,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        ds_id,
        guest_id: gw_id,
        work: awrite,
        state,
    }
}

/*
 * Create a write IOWork structure from an EID, and offset, and the data
 * buffer.  Used for converting a guest IO reead request into an IOWork that
 * the downstairs can understand.
 */
fn create_read_eob(
    ds_id: u64,
    gw_id: u64,
    eid: u64,
    block_offset: u64,
    blocks: u32,
) -> IOWork {
    let aread = IOop::Read {
        eid,
        block_offset,
        blocks,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        ds_id,
        guest_id: gw_id,
        work: aread,
        state,
    }
}

/*
 * Create a flush IOWork structure.
 */
fn create_flush(ds_id: u64, fln: Vec<u64>, guest_id: u64) -> IOWork {
    let flush = IOop::Flush {
        dependencies: Vec::new(), // XXX coming soon
        flush_numbers: fln,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        ds_id,
        guest_id,
        work: flush,
        state,
    }
}

/*
 * Debug function to display the work hashmap with status for all three of
 * the clients.
 */
#[allow(unused_variables)]
fn _show_work(up: &Arc<Upstairs>) {
    let mut work = up.work.lock().unwrap();
    for (id, job) in work.active.iter_mut() {
        let job_type = match &job.work {
            IOop::Read {
                eid,
                block_offset,
                blocks,
            } => "Read ".to_string(),
            IOop::Write {
                dependencies,
                eid,
                block_offset,
                data,
            } => "Write".to_string(),
            IOop::Flush {
                dependencies,
                flush_numbers,
            } => "Flush".to_string(),
        };
        print!("JOB:[{:04}] {} ", id, job_type);
        for cid in 0..3 {
            let state = job.state.get(&cid);
            match state {
                Some(state) => {
                    print!("[{}] state: {}  ", cid, state);
                }
                x => {
                    print!("[{}] unknown state:{:#?}", cid, x);
                }
            }
        }
        println!();
    }
    let done = work.completed.to_vec();
    println!("Done: {:?}", done);
    let mut gw = up.guest.guest_work.lock().unwrap();
    for (id, job) in gw.active.iter_mut() {
        print!("GW_JOB:[{:04}] {:?} ", id, job);
    }
}

/*
 * Debug function to dump the guest work structure.
 * This does a bit while holding the mutex, so don't expect performance
 * to get better when calling it.
 */
fn show_guest_work(guest: &Arc<Guest>) {
    println!("Guest work:  Active and Completed Jobs:");
    let gw = guest.guest_work.lock().unwrap();
    let mut kvec: Vec<u64> = gw.active.keys().cloned().collect::<Vec<u64>>();
    kvec.sort();
    for id in kvec.iter() {
        let job = gw.active.get(id).unwrap();
        println!(
            "GW_JOB active:[{:04}] S:{:?} C:{:?} ",
            id, job.submitted, job.completed
        );
    }
    println!("GW_JOB completed:{:?} ", gw.completed);
}

#[cfg(test)]
mod test {
    use super::*;
    /*
     * Beware, if you change these defaults, then you will have to change
     * all the hard coded tests below that use make_upstairs().
     */
    fn make_upstairs() -> Arc<Upstairs> {
        let def = RegionDefinition {
            block_size: 512,
            extent_size: 100,
            extent_count: 10,
        };

        Arc::new(Upstairs {
            work: Mutex::new(Work {
                active: HashMap::new(),
                completed: AllocRingBuffer::with_capacity(2),
                next_id: 1000,
            }),
            versions: Mutex::new(Vec::new()),
            dirty: Mutex::new(Vec::new()),
            ddef: Mutex::new(def),
            downstairs: Mutex::new(Vec::with_capacity(1)),
            guest: Arc::new(Guest::new()),
        })
    }

    #[test]
    fn off_to_extent_basic() {
        /*
         * Verify the offsets match the expected block_offset for the
         * default size region.
         */
        let up = make_upstairs();

        let exv = vec![(0, 0, 512)];
        assert_eq!(extent_from_offset(&up, 0, 512).unwrap(), exv);
        let exv = vec![(0, 1, 512)];
        assert_eq!(extent_from_offset(&up, 512, 512).unwrap(), exv);
        let exv = vec![(0, 2, 512)];
        assert_eq!(extent_from_offset(&up, 1024, 512).unwrap(), exv);
        let exv = vec![(0, 3, 512)];
        assert_eq!(extent_from_offset(&up, 1024 + 512, 512).unwrap(), exv);
        let exv = vec![(0, 99, 512)];
        assert_eq!(extent_from_offset(&up, 51200 - 512, 512).unwrap(), exv);

        let exv = vec![(1, 0, 512)];
        assert_eq!(extent_from_offset(&up, 51200, 512).unwrap(), exv);
        let exv = vec![(1, 1, 512)];
        assert_eq!(extent_from_offset(&up, 51200 + 512, 512).unwrap(), exv);
        let exv = vec![(1, 2, 512)];
        assert_eq!(extent_from_offset(&up, 51200 + 1024, 512).unwrap(), exv);
        let exv = vec![(1, 99, 512)];
        assert_eq!(extent_from_offset(&up, 102400 - 512, 512).unwrap(), exv);

        let exv = vec![(2, 0, 512)];
        assert_eq!(extent_from_offset(&up, 102400, 512).unwrap(), exv);

        let exv = vec![(9, 99, 512)];
        assert_eq!(
            extent_from_offset(&up, (512 * 100 * 10) - 512, 512).unwrap(),
            exv
        );
    }

    #[test]
    fn off_to_extent_buffer() {
        /*
         * Testing a buffer size larger than the default 512
         */
        let up = make_upstairs();

        let exv = vec![(0, 0, 1024)];
        assert_eq!(extent_from_offset(&up, 0, 1024).unwrap(), exv);
        let exv = vec![(0, 1, 1024)];
        assert_eq!(extent_from_offset(&up, 512, 1024).unwrap(), exv);
        let exv = vec![(0, 2, 1024)];
        assert_eq!(extent_from_offset(&up, 1024, 1024).unwrap(), exv);
        let exv = vec![(0, 98, 1024)];
        assert_eq!(extent_from_offset(&up, 51200 - 1024, 1024).unwrap(), exv);

        let exv = vec![(1, 0, 1024)];
        assert_eq!(extent_from_offset(&up, 51200, 1024).unwrap(), exv);
        let exv = vec![(1, 1, 1024)];
        assert_eq!(extent_from_offset(&up, 51200 + 512, 1024).unwrap(), exv);
        let exv = vec![(1, 2, 1024)];
        assert_eq!(extent_from_offset(&up, 51200 + 1024, 1024).unwrap(), exv);
        let exv = vec![(1, 98, 1024)];
        assert_eq!(extent_from_offset(&up, 102400 - 1024, 1024).unwrap(), exv);

        let exv = vec![(2, 0, 1024)];
        assert_eq!(extent_from_offset(&up, 102400, 1024).unwrap(), exv);

        let exv = vec![(9, 98, 1024)];
        assert_eq!(
            extent_from_offset(&up, (512 * 100 * 10) - 1024, 1024).unwrap(),
            exv
        );
    }

    #[test]
    fn off_to_extent_vbuff() {
        let up = make_upstairs();

        /*
         * Walk the buffer sizes from 512 to the whole extent, make sure
         * it all works as expected
         */
        for bsize in (512..=51200).step_by(512) {
            let exv = vec![(0, 0, bsize)];
            assert_eq!(extent_from_offset(&up, 0, bsize).unwrap(), exv);
        }
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
        let exv = vec![(0, 99, 512), (1, 0, 512)];
        assert_eq!(extent_from_offset(&up, 51200 - 512, 1024).unwrap(), exv);
        let exv = vec![(0, 98, 1024), (1, 0, 1024)];
        assert_eq!(extent_from_offset(&up, 51200 - 1024, 2048).unwrap(), exv);

        /*
         * Largest buffer
         */
        let exv = vec![(0, 1, 51200 - 512), (1, 0, 512)];
        assert_eq!(extent_from_offset(&up, 512, 51200).unwrap(), exv);
        let exv = vec![(0, 2, 51200 - 1024), (1, 0, 1024)];
        assert_eq!(extent_from_offset(&up, 1024, 51200).unwrap(), exv);
        let exv = vec![(0, 4, 51200 - 2048), (1, 0, 2048)];
        assert_eq!(extent_from_offset(&up, 2048, 51200).unwrap(), exv);

        /*
         * Largest buffer, last block offset possible
         */
        let exv = vec![(0, 99, 512), (1, 0, 51200 - 512)];
        assert_eq!(extent_from_offset(&up, 51200 - 512, 51200).unwrap(), exv);
    }

    /*
     * Testing various invalid inputs
     */
    #[test]
    #[should_panic]
    fn off_to_extent_length_zero() {
        let up = make_upstairs();
        extent_from_offset(&up, 0, 0).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_block_align() {
        let up = make_upstairs();
        extent_from_offset(&up, 0, 511).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_block_align2() {
        let up = make_upstairs();
        extent_from_offset(&up, 0, 513).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_length_big() {
        let up = make_upstairs();
        extent_from_offset(&up, 0, 51200 + 512).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_offset_align() {
        let up = make_upstairs();
        extent_from_offset(&up, 511, 512).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_offset_align2() {
        let up = make_upstairs();
        extent_from_offset(&up, 513, 512).unwrap();
    }
    #[test]
    #[should_panic]
    fn off_to_extent_offset_big() {
        let up = make_upstairs();
        extent_from_offset(&up, 512000, 512).unwrap();
    }
}
