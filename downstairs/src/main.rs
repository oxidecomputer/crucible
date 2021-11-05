// Copyright 2021 Oxide Computer Company
use futures::lock::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible::*;
use crucible_common::{Block, CrucibleError, MAX_BLOCK_SIZE};
use crucible_protocol::*;

use anyhow::{bail, Result};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use structopt::StructOpt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

mod dump;
mod region;
use dump::dump_region;
use region::Region;

#[derive(Debug, StructOpt)]
#[structopt(about = "disk-side storage component")]

enum Args {
    Create {
        #[structopt(long, default_value = "512")]
        block_size: u64,

        #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
        data: PathBuf,

        #[structopt(long, default_value = "100")]
        extent_size: u64,

        #[structopt(long, default_value = "15")]
        extent_count: u64,

        #[structopt(short, long, parse(from_os_str), name = "FILE")]
        import_path: Option<PathBuf>,

        #[structopt(short, long, name = "UUID", parse(try_from_str))]
        uuid: Uuid,
    },
    /*
     * Dump region information.
     * Multiple directories can be passed (up to 3)
     * With -e, you can dump just a single extent which will include
     * a block by block comparison.
     */
    Dump {
        /*
         * Directories containing a region.
         */
        #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
        data: Vec<PathBuf>,

        /*
         * Just dump this extent number
         */
        #[structopt(short, long)]
        extent: Option<u32>,
    },
    Export {
        /*
         * Number of blocks to export.
         */
        #[structopt(long, default_value = "0", name = "COUNT")]
        count: u64,

        #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
        data: PathBuf,

        #[structopt(short, long, parse(from_os_str), name = "OUT_FILE")]
        export_path: PathBuf,

        #[structopt(short, long, default_value = "0", name = "SKIP")]
        skip: u64,
    },
    Run {
        #[structopt(short, long, default_value = "0.0.0.0")]
        address: Ipv4Addr,

        #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
        data: PathBuf,
        /*
         * Test option, makes the search for new work sleep and sometimes
         * skip doing work.  XXX Note that the flow control between upstairs
         * and downstairs is not yet implemented.  By turning on this option
         * it's possible to deadlock.
         */
        #[structopt(long)]
        lossy: bool,

        #[structopt(short, long, default_value = "9000")]
        port: u16,

        #[structopt(long)]
        return_errors: bool,

        #[structopt(short, long)]
        trace_endpoint: Option<String>,
    },
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

/*
 * Export the contents or partial contents of a Downstairs Region to
 * the file indicated.
 *
 * We will start from the provided start_block.
 * We will stop after "count" blocks are written to the export_path.
 */
fn downstairs_export<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    export_path: P,
    start_block: u64,
    mut count: u64,
) -> Result<()> {
    /*
     * Export an existing downstairs region to a file
     */
    let (block_size, extent_size, extent_count) = region.region_def();
    let space_per_extent = extent_size.byte_value();
    assert!(block_size > 0);
    assert!(space_per_extent > 0);
    assert!(extent_count > 0);
    assert!(space_per_extent > 0);
    let file_size = space_per_extent * extent_count as u64;

    if count == 0 {
        count = extent_size.value * extent_count as u64;
    }

    println!(
        "Export total_size: {}  Extent size:{}  Total Extents:{}",
        file_size, space_per_extent, extent_count
    );
    println!(
        "Exporting from start_block: {}  count:{}",
        start_block, count
    );

    let mut data = BytesMut::with_capacity(block_size as usize);
    data.resize(block_size as usize, 0);

    let mut out_file = File::create(export_path)?;
    let mut blocks_copied = 0;

    'eid_loop: for eid in 0..extent_count {
        let extent_offset = space_per_extent * eid as u64;
        for block_offset in 0..extent_size.value {
            if (extent_offset + block_offset) >= start_block {
                blocks_copied += 1;
                region
                    .region_read(
                        eid as u64,
                        Block::new_with_ddef(block_offset, &region.def()),
                        &mut data,
                    )
                    .unwrap();
                out_file.write_all(&data).unwrap();
                data.resize(block_size as usize, 0);

                if blocks_copied >= count {
                    break 'eid_loop;
                }
            }
        }
    }

    println!("Read and wrote out {} blocks", blocks_copied);

    Ok(())
}

/*
 * Import the contents of a file into a new Region.
 * The total size of the region will be rounded up to the next largest
 * extent multiple.
 */
fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    import_path: P,
) -> Result<()> {
    /*
     * Open the file to import and determine how many extents we will need
     * based on the length.
     */
    let mut f = File::open(&import_path)?;
    let file_size = f.metadata()?.len();
    let (_, extent_size, _) = region.region_def();
    let space_per_extent = extent_size.byte_value();

    let mut extents_needed = file_size / space_per_extent;
    if file_size % space_per_extent != 0 {
        extents_needed += 1;
    }
    println!(
        "Import file_size: {}  Extent size: {}  Needed extents: {}",
        file_size, space_per_extent, extents_needed
    );

    if extents_needed > region.def().extent_count().into() {
        /*
         * The file to import would require more extents than we have.
         * Extend the region to fit the file.
         */
        println!("Extending region to fit image");
        region.extend(extents_needed as u32)?;
    } else {
        println!("Region already large enough for image");
    }

    println!("Importing {:?} to region", import_path);
    let rm = region.def();

    /*
     * We want to read and write large chunks of data, rather than individual
     * blocks, to improve import performance.  The chunk buffer must be a
     * whole number of the largest block size we are able to support.
     */
    const CHUNK_SIZE: usize = 32 * 1024 * 1024;
    assert_eq!(CHUNK_SIZE % MAX_BLOCK_SIZE, 0);
    let mut buffer = vec![0; CHUNK_SIZE];

    let mut offset = Block::new_with_ddef(0, &region.def());
    loop {
        buffer.resize(CHUNK_SIZE, 0);

        /*
         * Read data into the buffer until it is full, or we hit EOF.
         */
        let mut total = 0;
        loop {
            assert!(total <= CHUNK_SIZE);
            if total == CHUNK_SIZE {
                break;
            }

            let n = f.read(&mut buffer[total..(CHUNK_SIZE - total)])?;

            if n == 0 {
                /*
                 * We have hit EOF.  Extend the read buffer with zeroes until
                 * it is a multiple of the block size.
                 */
                while !Block::is_valid_byte_size(total, &rm) {
                    buffer[total] = 0;
                    total += 1;
                }
                break;
            }

            total += n;
        }

        if total == 0 {
            /*
             * If we read zero bytes without error, then we are done.
             */
            break;
        }

        /*
         * Use the same function upsairs uses to decide where to put the
         * data based on the LBA offset.
         */
        let nblocks = Block::from_bytes(total, &rm);
        let mut pos = Block::from_bytes(0, &rm);
        for (eid, offset, len) in extent_from_offset(rm, offset, nblocks)? {
            let data = &buffer[pos.bytes()..(pos.bytes() + len.bytes())];
            region.region_write(eid, offset, data)?;
            pos.advance(len);
        }
        assert_eq!(nblocks, pos);
        assert_eq!(total, pos.bytes());
        offset.advance(nblocks);
    }

    /*
     * As there is no EOF indication in the downstairs, print the
     * number of total blocks we wrote to so the caller can, if they
     * want, use that to extract just this imported file.
     */
    println!(
        "Populated {} extents by copying {} bytes ({} blocks)",
        extents_needed,
        offset.byte_value(),
        offset.value,
    );

    Ok(())
}

/*
 * Look at all the work outstanding for this downstairs and make a list
 * if jobs that are new or are waiting for dependencies.
 *
 * Once we have that list, walk them and see if any are ready to go.  If
 * so, then do that work.
 * so, then push that work.
 *
 * Upstairs UUID is passed in so we can skip jobs that aren't for this
 * Upstairs thread (all jobs are pushed onto the same Downstairs).
 *
 * We return the number of jobs completed so any caller can make use of
 * We return the number of jobs pushed so any caller can make use of
 * that.
 */
async fn push_next_jobs(
    upstairs_uuid: Uuid,
    ds: &Downstairs,
    job_channel_tx: &Arc<Mutex<Sender<u64>>>,
) -> Result<usize> {
    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     */
    let mut jobs = vec![];
    let new_work = ds.new_work(upstairs_uuid).await;

    if new_work.is_err() {
        // This means we couldn't unblock jobs for this UUID, so bail.
        return Ok(0);
    }

    let mut new_work = new_work.unwrap();

    /*
     * We don't have to do jobs in order, but the dependencies are, at
     * least for now, always going to be in order of job id.  So, to best
     * move things forward it is going to be fewer laps through the list
     * if we take the lowest job id first.
     */
    new_work.sort_unstable();

    for new_id in new_work.iter() {
        if ds.lossy && random() && random() {
            // Skip a job that needs to be done. Sometimes
            continue;
        }

        /*
         * If this job is still new, take it and go to work. The in_progress
         * method will only return a job if all dependencies are met.
         * Because we build the list of potential work, then release
         * the lock, it is possible to have things change, so we need
         * to verify that the job is still in a new or dep wait
         * state.
         */
        let job = ds.in_progress(*new_id).await;
        match job {
            Some(job) => {
                jobs.push(job);
            }
            None => {
                continue;
            }
        }
    }

    let jobs_count = jobs.len();
    for job in jobs {
        job_channel_tx.lock().await.send(job).await?;
    }

    Ok(jobs_count)
}

/*
 * Debug function to dump the work list.
 */
async fn _show_work(ds: &Downstairs) {
    println!("Active Upstairs UUID: {:?}", ds.active_upstairs());
    let work = ds.work.lock().await;

    let mut kvec: Vec<u64> = work.active.keys().cloned().collect::<Vec<u64>>();

    if kvec.is_empty() {
        println!("Crucible Downstairs work queue:  Empty");
    } else {
        println!("Crucible Downstairs work queue:");
        kvec.sort_unstable();
        for id in kvec.iter() {
            let dsw = work.active.get(id).unwrap();
            let dsw_type;
            let dep_list;
            match &dsw.work {
                IOop::Read {
                    dependencies,
                    eid: _eid,
                    offset: _offset,
                    num_blocks: _num_blocks,
                } => {
                    dsw_type = "Read ".to_string();
                    dep_list = dependencies.to_vec();
                }
                IOop::Write {
                    dependencies,
                    eid: _eid,
                    offset: _offset,
                    data: _data,
                } => {
                    dsw_type = "Write".to_string();
                    dep_list = dependencies.to_vec();
                }
                IOop::Flush {
                    dependencies,
                    flush_number: _flush_number,
                } => {
                    dsw_type = "Flush".to_string();
                    dep_list = dependencies.to_vec();
                }
            };
            println!(
                "DSW:[{:04}] {} {:?} deps:{:?}",
                id, dsw_type, dsw.state, dep_list,
            );
        }
    }

    println!("Done tasks {:?}", work.completed);
    println!("last_flush: {:?}", work.last_flush);
    println!("--------------------------------------");
}

/*
 * A new IO request has been received.
 * If the message is a ping or negotiation message, send the correct
 * response. If the message is an IO, then put the new IO the work hashmap.
 */
async fn proc_frame(
    upstairs_uuid: Uuid,
    ad: &mut Arc<Mutex<Downstairs>>,
    m: &Message,
    fw: &mut Arc<Mutex<FramedWrite<OwnedWriteHalf, CrucibleEncoder>>>,
    job_channel_tx: Arc<Mutex<Sender<u64>>>,
) -> Result<()> {
    match m {
        Message::Ruok => {
            let mut fw = fw.lock().await;
            fw.send(Message::Imok).await?;
        }
        // Regular work path
        Message::Write(uuid, ds_id, eid, dependencies, offset, data) => {
            if upstairs_uuid != *uuid {
                let mut fw = fw.lock().await;
                fw.send(Message::UuidMismatch(upstairs_uuid)).await?;
                return Ok(());
            }

            let new_write = IOop::Write {
                dependencies: dependencies.to_vec(),
                eid: *eid,
                offset: *offset,
                data: data.clone(),
            };

            let d = ad.lock().await;
            d.add_work(*uuid, *ds_id, new_write).await?;
        }
        Message::Flush(uuid, ds_id, dependencies, flush_number) => {
            if upstairs_uuid != *uuid {
                let mut fw = fw.lock().await;
                fw.send(Message::UuidMismatch(upstairs_uuid)).await?;
                return Ok(());
            }

            let new_flush = IOop::Flush {
                dependencies: dependencies.to_vec(),
                flush_number: *flush_number,
            };

            let d = ad.lock().await;
            d.add_work(*uuid, *ds_id, new_flush).await?;
        }
        Message::ReadRequest(
            uuid,
            ds_id,
            dependencies,
            eid,
            offset,
            num_blocks,
        ) => {
            if upstairs_uuid != *uuid {
                let mut fw = fw.lock().await;
                fw.send(Message::UuidMismatch(upstairs_uuid)).await?;
                return Ok(());
            }

            let new_read = IOop::Read {
                dependencies: dependencies.to_vec(),
                eid: *eid,
                offset: *offset,
                num_blocks: *num_blocks,
            };

            let d = ad.lock().await;
            d.add_work(*uuid, *ds_id, new_read).await?;
        }
        x => bail!("unexpected frame {:?}", x),
    }

    /*
     * After adding work, start those that can be started.
     */
    {
        let d = ad.lock().await;
        d.unblock_jobs(upstairs_uuid, &job_channel_tx).await?;
    }

    Ok(())
}

async fn do_work_task(
    ads: &mut Arc<Mutex<Downstairs>>,
    mut job_channel_rx: Receiver<u64>,
    ack_ready_tx: Sender<u64>,
) -> Result<()> {
    loop {
        tokio::select! {
            job_id = job_channel_rx.recv() => {
                match job_id {
                    Some(job_id) => {
                        let ds = ads.lock().await;

                        if ds.lossy && random() && random() {
                            // Add a little time to completion for this
                            // operation.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }

                        ds.do_work(job_id, &ack_ready_tx).await?;
                    }
                    None => {
                        // hung up
                         return Ok(());
                    }
                }
            }
        }
    }
}

async fn ack_sender(
    ads: &Arc<Mutex<Downstairs>>,
    fw: &mut Arc<Mutex<FramedWrite<OwnedWriteHalf, CrucibleEncoder>>>,
    job_channel_tx: &Arc<Mutex<Sender<u64>>>,
    mut ack_ready_rx: Receiver<u64>,
) -> Result<()> {
    while let Some(job_id) = ack_ready_rx.recv().await {
        let mut ds = ads.lock().await;
        ds.complete_work(job_id, fw, job_channel_tx).await?;
    }

    Ok(())
}

/*
 * This function handles the initial negotiation steps between the
 * upstairs and the downstairs.  Either we return error, or we call
 * the next function if everything was successful and we can start
 * taking IOs from the upstairs.
 */
async fn proc(ads: &mut Arc<Mutex<Downstairs>>, sock: TcpStream) -> Result<()> {
    let (read, write) = sock.into_split();
    let mut fr = FramedRead::new(read, CrucibleDecoder::new());
    let fw =
        Arc::new(Mutex::new(FramedWrite::new(write, CrucibleEncoder::new())));

    let mut negotiated = 0;
    let mut upstairs_uuid = None;

    let (_another_upstairs_active_tx, mut another_upstairs_active_rx) =
        channel(1);
    let another_upstairs_active_tx = Arc::new(_another_upstairs_active_tx);

    /*
     * See the comment in the proc() function on the upstairs side that
     * describes how this negotiation takes place.
     *
     * The final step in negotiation (as dictated by the upstairs) is
     * either LastFlush, or ExtentVersionsPlease.  Once we respond to
     * that message, we can move forward and start receiving IO from
     * the upstairs.
     */
    while negotiated < 4 {
        tokio::select! {
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * XXX Timeouts, timeouts: always wrong!  Some too short and
             * some too long.
             */
            _ = sleep_until(deadline_secs(50)) => {
                bail!("did not negotiate a protocol");
            }
            /*
             * This Upstairs' thread will receive this signal when another
             * Upstairs promotes itself to active. The only way this path is
             * reached is if this Upstairs promoted itself to active, storing
             * another_upstairs_active_tx in the Downstairs active_upstairs
             * tuple.
             *
             * The two unwraps here should be safe: this thread negotiated and
             * activated, and then another did (in order to send this thread
             * this signal).
             */
            _ = another_upstairs_active_rx.recv() => {
                let upstairs_uuid = upstairs_uuid.unwrap();
                println!("Another upstairs promoted to active, \
                    shutting down connection for {:?}", upstairs_uuid);

                let active_upstairs = {
                    let ds = ads.lock().await;
                    ds.active_upstairs().unwrap()
                };
                let mut fw = fw.lock().await;
                fw.send(Message::UuidMismatch(active_upstairs)).await?;

                return Ok(());
            }
            new_read = fr.next() => {
                /*
                 * Negotiate protocol before we take any IO requests.
                 */
                match new_read.transpose()? {
                    None => {
                        let mut ds = ads.lock().await;

                        if let Some(upstairs_uuid) = upstairs_uuid {
                            println!(
                                "upstairs {:?} disconnected, {} jobs left",
                                upstairs_uuid, ds.jobs().await,
                            );

                            if ds.is_active(upstairs_uuid) {
                                println!("upstairs {:?} was previously \
                                    active, clearing", upstairs_uuid);
                                ds.clear_active();
                            }
                        } else {
                            println!(
                                "upstairs disconnected, {} jobs left",
                                ds.jobs().await,
                            );
                        }

                        return Ok(());
                    }
                    Some(Message::Ruok) => {
                        let mut fw = fw.lock().await;
                        fw.send(Message::Imok).await?;
                    }
                    Some(Message::HereIAm(version, uuid)) => {
                        if negotiated != 0 {
                            bail!("Received connect out of order {}",
                                negotiated);
                        }
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = 1;
                        upstairs_uuid = Some(uuid);
                        println!("upstairs {:?} connected",
                            upstairs_uuid.unwrap());
                        let mut fw = fw.lock().await;
                        fw.send(Message::YesItsMe(1)).await?;
                    }
                    Some(Message::PromoteToActive(uuid)) => {
                        if negotiated != 1 {
                            bail!("Received activate out of order {}",
                                negotiated);
                        }
                        // Only allowed to promote or demote self
                        if upstairs_uuid.unwrap() != uuid {
                            let mut fw = fw.lock().await;
                            fw.send(
                                Message::UuidMismatch(upstairs_uuid.unwrap())
                            ).await?;
                            /*
                             * At this point, should we just return error?
                             * XXX
                             */
                        } else {
                            {
                                let mut ds = ads.lock().await;
                                ds.promote_to_active(
                                    uuid,
                                    another_upstairs_active_tx.clone()
                                ).await;
                            }
                            negotiated = 2;

                            let mut fw = fw.lock().await;
                            fw.send(Message::YouAreNowActive(uuid)).await?;
                        }
                    }
                    Some(Message::RegionInfoPlease) => {
                        if negotiated != 2 {
                            bail!("Received RegionInfo out of order {}",
                                negotiated);
                        }
                        negotiated = 3;
                        let rd = {
                            let ds = ads.lock().await;
                            ds.region.def()
                        };

                        let mut fw = fw.lock().await;
                        fw.send(Message::RegionInfo(rd)).await?;
                    }
                    Some(Message::LastFlush(last_flush)) => {
                        if negotiated != 3 {
                            bail!("Received LastFlush out of order {}",
                                negotiated);
                        }
                        negotiated = 4;
                        {
                            let ds = ads.lock().await;
                            let mut work = ds.work_lock(
                                upstairs_uuid.unwrap()
                            ).await?;
                            work.last_flush = last_flush;
                            println!("Set last flush {}", last_flush);
                        }

                        let mut fw = fw.lock().await;
                        fw.send(Message::LastFlushAck(last_flush)).await?;
                        /*
                         * Once this command is sent, we are ready to exit
                         * the loop and move forward with receiving IOs
                         */
                    }
                    Some(Message::ExtentVersionsPlease) => {
                        if negotiated != 3 {
                            bail!("Received ExtentVersions out of order {}",
                                negotiated);
                        }
                        negotiated = 4;
                        let ds = ads.lock().await;
                        let flush_numbers = ds.region.flush_numbers()?;
                        let generation_numbers = ds.region.gen_numbers()?;
                        let dirty_bits = ds.region.dirty()?;
                        drop(ds);

                        let mut fw = fw.lock().await;
                        fw.send(Message::ExtentVersions(
                            generation_numbers,
                            flush_numbers,
                            dirty_bits,
                        ))
                        .await?;

                        /*
                         * Once this command is sent, we are ready to exit
                         * the loop and move forward with receiving IOs
                         */
                    }
                    Some(_msg) => {
                        println!("Ignored message received during negotiation");
                    }
                }
            }
        }
    }

    println!("Downstairs has completed Negotiation");
    assert!(upstairs_uuid.is_some());
    let u_uuid = upstairs_uuid.unwrap();

    resp_loop(ads, fr, fw, another_upstairs_active_rx, u_uuid).await
}

/*
 * This function listens for and answers requests from the upstairs.
 * We assume here that correct negotiation has taken place and this
 * downstairs is ready to receive IO.
 */
async fn resp_loop(
    ads: &mut Arc<Mutex<Downstairs>>,
    mut fr: FramedRead<OwnedReadHalf, CrucibleDecoder>,
    mut fw: Arc<Mutex<FramedWrite<OwnedWriteHalf, CrucibleEncoder>>>,
    mut another_upstairs_active_rx: mpsc::Receiver<u64>,
    upstairs_uuid: Uuid,
) -> Result<()> {
    let mut lossy_interval = deadline_secs(5);
    let mut more_work_interval = deadline_secs(5);

    // XXX flow control size to 100?
    let (_job_channel_tx, job_channel_rx) = channel(100);
    let job_channel_tx = Arc::new(Mutex::new(_job_channel_tx));

    let (ack_ready_tx, ack_ready_rx) = channel(100);

    {
        let mut adc = ads.clone();
        tokio::spawn(async move {
            do_work_task(&mut adc, job_channel_rx, ack_ready_tx).await
        });
    }

    {
        let adc = ads.clone();
        let mut fwc = fw.clone();
        let tx = job_channel_tx.clone();
        tokio::spawn(async move {
            ack_sender(&adc, &mut fwc, &tx, ack_ready_rx).await
        });
    }

    loop {
        tokio::select! {
            /*
             * If we have set "lossy", then we need to check every now and
             * then that there were not skipped jobs that we need to go back
             * and finish up. If lossy is not set, then this should only
             * trigger once then never again.
             */
            _ = sleep_until(lossy_interval) => {
                let lossy = {
                    let ds = ads.lock().await;
                    //show_work(&ds);
                    ds.lossy
                };
                if lossy {
                    let ds = ads.lock().await;
                    ds.unblock_jobs(upstairs_uuid, &job_channel_tx).await?;
                }
                lossy_interval = deadline_secs(5);
            }
            _ = sleep_until(more_work_interval) => {
                /*
                 * Unblock any stuck jobs. XXX how does this happen?
                 */
                let ds = ads.lock().await;
                ds.unblock_jobs(upstairs_uuid, &job_channel_tx).await?;

                more_work_interval = deadline_secs(5);
             }
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * XXX Timeouts, timeouts: always wrong!  Some too short and
             * some too long.
             */
            _ = sleep_until(deadline_secs(50)) => {
                bail!("inactivity timeout");
            }
            /*
             * This Upstairs' thread will receive this signal when another
             * Upstairs promotes itself to active. The only way this path is
             * reached is if this Upstairs promoted itself to active, storing
             * another_upstairs_active_tx in the Downstairs active_upstairs
             * tuple.
             *
             * The two unwraps here should be safe: this thread negotiated and
             * activated, and then another did (in order to send this thread
             * this signal).
             */
            _ = another_upstairs_active_rx.recv() => {
                println!("Another upstairs promoted to active, \
                    shutting down connection for {:?}", upstairs_uuid);

                let active_upstairs = {
                    let ds = ads.lock().await;
                    ds.active_upstairs().unwrap()
                };

                let mut fw = fw.lock().await;
                fw.send(Message::UuidMismatch(active_upstairs)).await?;

                return Ok(());
            }
            new_read = fr.next() => {
                // When the downstairs responds, push the deadlines
                more_work_interval = deadline_secs(5);

                match new_read.transpose()? {
                    None => {
                        let mut ds = ads.lock().await;

                        println!(
                            "upstairs {:?} disconnected, {} jobs left",
                            upstairs_uuid, ds.jobs().await,
                        );

                        if ds.is_active(upstairs_uuid) {
                            println!("upstairs {:?} was previously \
                                active, clearing", upstairs_uuid);
                            ds.clear_active();
                        }

                        return Ok(());
                    }
                    Some(msg) => {
                        proc_frame(
                            upstairs_uuid,
                            ads,
                            &msg,
                            &mut fw,
                            job_channel_tx.clone(),
                        ).await?;
                    }
                }
            }
        }
    }
}

/*
 * Overall structure for things the downstairs is tracking.
 * This includes the extents and their status as well as the
 * downstairs work queue.
 */
#[derive(Debug)]
struct Downstairs {
    region: Region,
    work: Mutex<Work>,
    lossy: bool,         // Test flag, enables pauses and skipped jobs
    return_errors: bool, // Test flag
    active_upstairs: Option<(Uuid, Arc<Sender<u64>>)>,
}

impl Downstairs {
    fn new(region: Region, lossy: bool, return_errors: bool) -> Self {
        Downstairs {
            region,
            work: Mutex::new(Work::default()),
            lossy,
            return_errors,
            active_upstairs: None,
        }
    }

    /*
     * Only grab the lock if the Upstairs UUID matches.
     *
     * Multiple Upstairs connecting to this Downstairs will spawn multiple
     * threads that all can potentially add work to the same `active` hash
     * map. Only one Upstairs can be "active" at any one time though.
     * When promote_to_active takes the work lock, it will clear out the
     * `active` hash map and (if applicable) will signal to the currently
     * active Upstairs to terminate the connection.
     *
     * `new_work` and `add_work` both grab their work lock through this
     * function. Let's say `promote_to_active` and `add_work` are racing for
     * the work lock. If `add_work` wins the race it will put work into
     * `active`, then `promote_to_active` will clear it out. If
     * `promote_to_active` wins the race, it will change the UUID and
     * signal to the previously active Upstairs that it should close this
     * connection. If `add_work` does fire, it will fail to grab the lock
     * because the UUID is no longer active, and the connection thread
     * should close.
     *
     * Let's say `new_work` and `promote_to_active` are racing. If `new_work`
     * wins, then it will return and run those jobs in `do_work_task`.
     * However, `promote_to_active` will grab the lock and change the
     * UUID, causing `do_work` to return UpstairsInactive for the jobs
     * that were just returned. If `promote_to_active` wins, it will
     * clear out the jobs of the old UUID.
     *
     * Grabbing the lock in this way should properly clear out the previously
     * active Upstairs without causing jobs to be incorrectly sent to the
     * newly active Upstairs.
     */
    async fn work_lock(
        &self,
        upstairs_uuid: Uuid,
    ) -> Result<MutexGuard<'_, Work>> {
        if let Some(active_upstairs) = &self.active_upstairs {
            let active_uuid = active_upstairs.0;
            if active_uuid != upstairs_uuid {
                println!(
                    "{:?} cannot grab lock, {:?} is active!",
                    upstairs_uuid, active_uuid
                );
                bail!(CrucibleError::UpstairsInactive)
            } else {
                Ok(self.work.lock().await)
            }
        } else {
            panic!("cannot grab work lock, nothing is active!");
        }
    }

    async fn jobs(&self) -> usize {
        let work = self.work.lock().await;
        work.jobs()
    }

    async fn new_work(&self, upstairs_uuid: Uuid) -> Result<Vec<u64>> {
        let work = self.work_lock(upstairs_uuid).await?;
        Ok(work.new_work(upstairs_uuid))
    }

    async fn add_work(
        &self,
        upstairs_uuid: Uuid,
        ds_id: u64,
        work: IOop,
    ) -> Result<()> {
        let dsw = DownstairsWork {
            upstairs_uuid,
            ds_id,
            work,
            state: WorkState::New,
        };

        let mut work = self.work_lock(upstairs_uuid).await?;
        work.active.insert(ds_id, dsw);

        Ok(())
    }

    async fn in_progress(&self, ds_id: u64) -> Option<u64> {
        let mut work = self.work.lock().await;
        if let Some((job_id, upstairs_uuid)) = work.in_progress(ds_id) {
            if !self.is_active(upstairs_uuid) {
                // Don't return a job with the wrong uuid! `promote_to_active`
                // should have removed any active jobs, and
                // `work.new_work` should have filtered on the correct UUID.
                panic!("Don't return a job with the wrong uuid!");
            }

            Some(job_id)
        } else {
            None
        }
    }

    /// Given a job ID, do the work for that IO.
    async fn do_work(
        &self,
        job_id: u64,
        ack_ready_tx: &Sender<u64>,
    ) -> Result<()> {
        let result = {
            let mut work = self.work.lock().await;
            work.do_work(self, job_id).await?
        };

        if result.is_some() {
            ack_ready_tx.send(job_id).await?;
        }

        Ok(())
    }

    /*
     * Complete work by:
     *
     * - notifying the upstairs with the response
     * - removing the job from active
     * - removing the response
     * - putting the id on the completed list.
     */
    async fn complete_work(
        &mut self,
        ds_id: u64,
        fw: &mut Arc<Mutex<FramedWrite<OwnedWriteHalf, CrucibleEncoder>>>,
        job_channel_tx: &Arc<Mutex<Sender<u64>>>,
    ) -> Result<()> {
        let mut work = self.work.lock().await;

        let m = work.responses.get(&ds_id).unwrap();

        // Notify the upstairs
        let mut fw = fw.lock().await;
        fw.send(m).await?;

        // Complete the job
        let is_flush = matches!(m, Message::FlushAck(_, _, _));

        let upstairs_uuid = match &m {
            Message::WriteAck(uuid, _, _) => *uuid,
            Message::ReadResponse(uuid, _, _, _) => *uuid,
            Message::FlushAck(uuid, _, _) => *uuid,
            _ => {
                panic!("Unexpected {:?} message in ack_sender", m);
            }
        };

        // _ can be None if promote_to_active ran and cleared out active.
        let _ = work.active.remove(&ds_id);

        let existing = work.responses.remove(&ds_id);
        assert!(existing.is_some());

        if is_flush {
            work.last_flush = ds_id;
            work.completed = Vec::with_capacity(32);
        } else {
            work.completed.push(ds_id);
        }

        drop(work);

        /*
         * Immediately unblock jobs that were waiting on this one to
         * complete.
         */
        self.unblock_jobs(upstairs_uuid, job_channel_tx).await?;

        Ok(())
    }

    async fn promote_to_active(&mut self, uuid: Uuid, tx: Arc<Sender<u64>>) {
        let mut work = self.work.lock().await;

        println!("{:?} is now active", uuid);

        /*
         * If there's an existing Upstairs connection, signal to terminate
         * it. Do this while holding the work lock so the previously
         * active Upstairs isn't adding more work.
         */
        if let Some(old_upstairs) = &self.active_upstairs {
            println!("Signaling to {:?} thread", old_upstairs.0);
            match futures::executor::block_on(old_upstairs.1.send(0)) {
                Ok(_) => {}
                Err(e) => {
                    /*
                     * It's possible the old thread died due to some
                     * connection error. In that case the
                     * receiver will have closed and
                     * the above send will fail.
                     */
                    println!(
                        "Error while signaling to {:?} thread: {:?}",
                        old_upstairs.0, e,
                    );
                }
            }
        }

        self.active_upstairs = Some((uuid, tx));

        /*
         * Note: in the future, differentiate between new upstairs connecting
         * vs same upstairs reconnecting here.
         *
         * Clear out active jobs, the last flush, and completed information,
         * as that will not be valid any longer.
         *
         * Don't clear out responses, we need to inform the Upstairs of what
         * happened. Clear out active jobs so no more work is done for the
         * now non-active Upstairs.
         *
         * TODO: Really work through this error case
         */
        if work.active.keys().len() > 0 {
            println!(
                "Crucible Downstairs promoting {} to active, \
                discarding {} jobs",
                uuid,
                work.active.keys().len()
            );

            /*
             * In the future, we may decide there is some way to continue
             * working on outstanding jobs, or a way to merge. But for now,
             * we just throw out what we have and let the upstairs resend
             * anything to us that it did not get an ACK for.
             */
            work.active = HashMap::new();
        }

        work.completed = Vec::with_capacity(32);
        work.last_flush = 0;
    }

    fn is_active(&self, uuid: Uuid) -> bool {
        match self.active_upstairs.as_ref() {
            None => false,
            Some(tuple) => tuple.0 == uuid,
        }
    }

    fn active_upstairs(&self) -> Option<Uuid> {
        self.active_upstairs.as_ref().map(|e| e.0)
    }

    fn clear_active(&mut self) {
        self.active_upstairs = None;
    }

    async fn unblock_jobs(
        &self,
        upstairs_uuid: Uuid,
        job_channel_tx: &Arc<Mutex<Sender<u64>>>,
    ) -> Result<()> {
        loop {
            let pushed_jobs =
                push_next_jobs(upstairs_uuid, self, job_channel_tx).await?;

            // If any jobs were unblocked, try to unblock more.
            if pushed_jobs == 0 {
                break;
            }
        }

        Ok(())
    }
}

/*
 * The structure that tracks downstairs work in progress
 */
#[derive(Debug, Default)]
pub struct Work {
    active: HashMap<u64, DownstairsWork>,
    outstanding_deps: HashMap<u64, usize>,
    responses: HashMap<u64, Message>,

    /*
     * We have to keep track of all IOs that have been issued since
     * our last flush, as that is how we make sure dependencies are
     * respected. The last_flush is the downstairs job ID number (ds_id
     * typically) for the most recent flush.
     */
    last_flush: u64,
    completed: Vec<u64>,
}

#[derive(Debug, Clone)]
struct DownstairsWork {
    upstairs_uuid: Uuid,
    ds_id: u64,
    work: IOop,
    state: WorkState,
}

impl Work {
    fn jobs(&self) -> usize {
        self.active.len()
    }

    /**
     * Return a list of downstairs request IDs that are new or have
     * been waiting for other dependencies to finish.
     */
    fn new_work(&self, upstairs_uuid: Uuid) -> Vec<u64> {
        let mut result = Vec::with_capacity(self.active.len());

        for job in self.active.values() {
            if job.upstairs_uuid != upstairs_uuid {
                panic!("Old Upstairs Job in new_work!");
            }

            if job.state == WorkState::New || job.state == WorkState::DepWait {
                result.push(job.ds_id);
            }
        }

        result
    }

    /**
     * If the requested job is still new, and the dependencies are all met,
     * return the DownstairsWork struct and let the caller take action
     * with it, leaving the state as InProgress.
     * If this job is not new, then just return none.  This can be okay as
     * we build or work list with the new_work fn above, but we drop and
     * re-aquire the Work mutex and things can change.
     */
    fn in_progress(&mut self, ds_id: u64) -> Option<(u64, Uuid)> {
        /*
         * Once we support multiple threads, we can obtain a ds_id that
         * looked valid when we made a list of jobs, but something
         * else moved that job along and now it no longer exists.  We
         * need to handle that case correctly.
         */
        if let Some(job) = self.active.get_mut(&ds_id) {
            if job.state == WorkState::New || job.state == WorkState::DepWait {
                /*
                 * Before we can make this in_progress, we have to, while
                 * holding this locked, check the dep list if there is one
                 * and make sure all dependencies are completed.
                 */
                let dep_list = match &job.work {
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
                };

                /*
                 * See which of our dependencies are met.
                 * XXX Make this better/faster by removing the ones that
                 * are met, so next lap we don't have to check again?  There
                 * may be some debug value to knowing what the dep list was,
                 * so consider that before making this faster.
                 */
                let mut deps_outstanding: Vec<u64> =
                    Vec::with_capacity(dep_list.len());

                for dep in dep_list.iter() {
                    if dep <= &self.last_flush {
                        continue;
                    }

                    if !self.completed.contains(dep) {
                        deps_outstanding.push(*dep);
                    }
                }

                if !deps_outstanding.is_empty() {
                    let print = if let Some(existing_outstanding_deps) =
                        self.outstanding_deps.get(&ds_id)
                    {
                        *existing_outstanding_deps != deps_outstanding.len()
                    } else {
                        false
                    };

                    if print {
                        println!(
                            "{} job {} for uuid {:?} waiting on {} deps",
                            ds_id,
                            match &job.work {
                                IOop::Write {
                                    dependencies: _,
                                    eid: _eid,
                                    offset: _offset,
                                    data: _data,
                                } => "Write",
                                IOop::Flush {
                                    dependencies: _,
                                    flush_number: _flush_number,
                                } => "Flush",
                                IOop::Read {
                                    dependencies: _,
                                    eid: _eid,
                                    offset: _offset,
                                    num_blocks: _num_blocks,
                                } => "Read",
                            },
                            job.upstairs_uuid,
                            deps_outstanding.len(),
                        );
                    }

                    let _ = self
                        .outstanding_deps
                        .insert(ds_id, deps_outstanding.len());

                    /*
                     * If we got here, then the dep is not met.
                     * Set DepWait if not already set.
                     */
                    if job.state == WorkState::New {
                        job.state = WorkState::DepWait;
                    }

                    return None;
                }

                /*
                 * We had no dependencies, or they are all completed, we
                 * can go ahead and work on this job.
                 */
                job.state = WorkState::InProgress;

                Some((job.ds_id, job.upstairs_uuid))
            } else {
                /*
                 * job id is not new, we can't run it.
                 */
                None
            }
        } else {
            /*
             * This ID is no longer a valid job id.  That would be ok
             * if there a multiple things running at the same time.
             */
            None
        }
    }

    /*
     * This method calls into the Downstair's region and performs the read /
     * write / flush action. A reference to Downstairs is required to do this
     * so that the job can continue to be owned by Work.
     *
     * If by the time this job_id is processed here the job is no longer on
     * the active work queue, return None. If this happens no response
     * will have been put onto the response queue.
     */
    async fn do_work(
        &mut self,
        ds: &Downstairs,
        job_id: u64,
    ) -> Result<Option<()>> {
        let job = match self.active.get(&job_id) {
            Some(job) => job,
            None => {
                /*
                 * This branch occurs when another Upstairs has promoted
                 * itself to active, causing active work to
                 * be cleared (in promote_to_active).
                 *
                 * If this has happened, work.completed and work.last_flush
                 * have also been reset. Do nothing here,
                 * especially since the Upstairs has already
                 * been notified.
                 */
                return Ok(None);
            }
        };

        assert_eq!(job.state, WorkState::InProgress);
        assert_eq!(job_id, job.ds_id);

        match &job.work {
            IOop::Read {
                dependencies: _dependencies,
                eid,
                offset,
                num_blocks,
            } => {
                /*
                 * XXX Some thought will need to be given to where the read
                 * data buffer is created, both on this side and the remote.
                 * Also, we (I) need to figure out how to read data into an
                 * uninitialized buffer. Until then, we have this workaround.
                 */
                let (bs, _, _) = ds.region.region_def();
                let sz = *num_blocks as usize * bs as usize;
                let mut data = BytesMut::with_capacity(sz);
                data.resize(sz, 1);

                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let result = if ds.return_errors && random() && random() {
                    println!("returning error on read!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !ds.is_active(job.upstairs_uuid) {
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    ds.region.region_read(*eid, *offset, &mut data)
                };

                let existing = self.responses.insert(
                    job_id,
                    Message::ReadResponse(
                        job.upstairs_uuid,
                        job.ds_id,
                        data.freeze(),
                        result,
                    ),
                );
                assert!(existing.is_none());

                Ok(Some(()))
            }
            IOop::Write {
                dependencies: _dependencies,
                eid,
                offset,
                data,
            } => {
                let result = if ds.return_errors && random() && random() {
                    println!("returning error on write!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !ds.is_active(job.upstairs_uuid) {
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    ds.region.region_write(*eid, *offset, data)
                };

                let existing = self.responses.insert(
                    job_id,
                    Message::WriteAck(job.upstairs_uuid, job.ds_id, result),
                );
                assert!(existing.is_none());

                Ok(Some(()))
            }
            IOop::Flush {
                dependencies: _dependencies,
                flush_number,
            } => {
                let result = if ds.return_errors && random() && random() {
                    println!("returning error on flush!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !ds.is_active(job.upstairs_uuid) {
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    ds.region.region_flush(*flush_number)
                };

                let existing = self.responses.insert(
                    job_id,
                    Message::FlushAck(job.upstairs_uuid, job.ds_id, result),
                );
                assert!(existing.is_none());

                Ok(Some(()))
            }
        }
    }
}

/*
 * We may not need Done or Error.  At the moment all we actually look
 * at is New or InProgress.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum WorkState {
    New,
    DepWait,
    InProgress,
    Done,
    Error,
}

impl fmt::Display for WorkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkState::New => {
                write!(f, " New")
            }
            WorkState::DepWait => {
                write!(f, "DepW")
            }
            WorkState::InProgress => {
                write!(f, "In P")
            }
            WorkState::Done => {
                write!(f, "Done")
            }
            WorkState::Error => {
                write!(f, " Err")
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args_safe()?;

    /*
     * Everyone needs a region
     */
    let mut region;

    match args {
        Args::Create {
            block_size,
            data,
            extent_size,
            extent_count,
            import_path,
            uuid,
        } => {
            /*
             * Create the region options, then the region.
             */
            let mut region_options: crucible_common::RegionOptions =
                Default::default();
            region_options.set_block_size(block_size);
            region_options.set_extent_size(Block::new(
                extent_size,
                block_size.trailing_zeros(),
            ));
            region_options.set_uuid(uuid);

            region = Region::create(&data, region_options)?;
            region.extend(extent_count as u32)?;

            if let Some(ref ip) = import_path {
                downstairs_import(&mut region, ip).unwrap();
                /*
                 * The region we just created should now have a flush so the
                 * new data and inital flush number is written to disk.
                 */
                region.region_flush(1)?;
            }

            println!("UUID: {:?}", region.def().uuid());
            println!(
                "Blocks per extent:{} Total Extents: {}",
                region.def().extent_size().value,
                region.def().extent_count(),
            );
            Ok(())
        }
        Args::Dump { data, extent } => {
            dump_region(data, extent)?;
            Ok(())
        }
        Args::Export {
            count,
            data,
            export_path,
            skip,
        } => {
            region = Region::open(&data, Default::default(), true)?;

            downstairs_export(&mut region, export_path, skip, count).unwrap();
            Ok(())
        }
        Args::Run {
            address,
            data,
            lossy,
            port,
            return_errors,
            trace_endpoint,
        } => {
            region = Region::open(&data, Default::default(), true)?;

            println!("UUID: {:?}", region.def().uuid());
            println!(
                "Blocks per extent:{} Total Extents: {}",
                region.def().extent_size().value,
                region.def().extent_count(),
            );

            let d = Arc::new(Mutex::new(Downstairs::new(
                region,
                lossy,
                return_errors,
            )));

            /*
             * If any of our async tasks in our runtime panic, then we should
             * exit the program right away.
             */
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default_panic(info);
                std::process::exit(1);
            }));

            if let Some(endpoint) = trace_endpoint {
                let tracer = opentelemetry_jaeger::new_pipeline()
                    .with_agent_endpoint(endpoint) // usually port 6831
                    .with_service_name("downstairs")
                    .install_simple()
                    .expect("Error initializing Jaeger exporter");

                let telemetry =
                    tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(telemetry)
                    .try_init()
                    .expect("Error init tracing subscriber");
            }

            /*
             * Establish a listen server on the port.
             */
            let listen_on = SocketAddrV4::new(address, port);
            let listener = TcpListener::bind(&listen_on).await?;

            /*
             * We now loop listening for a connection from the Upstairs.
             * When we get one, we then spawn the proc() function to handle
             * it and wait for another connection. Downstairs can handle
             * multiple Upstairs connecting but only one active one.
             */
            println!("listening on {}", listen_on);
            loop {
                let (sock, raddr) = listener.accept().await?;

                println!("connection from {:?}", raddr);

                let mut dd = d.clone();

                tokio::spawn(async move {
                    if let Err(e) = proc(&mut dd, sock).await {
                        println!("ERROR: connection({}): {:?}", raddr, e);
                    } else {
                        println!("OK: connection({}): all done", raddr);
                    }
                });
            }
        }
    }
}
