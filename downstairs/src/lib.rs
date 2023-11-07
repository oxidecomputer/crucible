// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]

use futures::executor;
use futures::lock::{Mutex, MutexGuard};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible::*;
use crucible_common::{build_logger, Block, CrucibleError, MAX_BLOCK_SIZE};

use anyhow::{bail, Result};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use slog::{debug, error, info, o, warn, Logger};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub mod admin;
mod dump;
mod dynamometer;
mod extent;
pub mod region;
pub mod repair;
mod stats;

mod extent_inner_raw;
mod extent_inner_sqlite;

use region::Region;

pub use admin::run_dropshot;
pub use dump::dump_region;
pub use dynamometer::*;
pub use stats::*;

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
pub async fn downstairs_export<P: AsRef<Path> + std::fmt::Debug>(
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

    let mut out_file = File::create(export_path)?;
    let mut blocks_copied = 0;

    'eid_loop: for eid in 0..extent_count {
        let extent_offset = space_per_extent * eid as u64;
        for block_offset in 0..extent_size.value {
            if (extent_offset + block_offset) >= start_block {
                blocks_copied += 1;

                let mut responses = region
                    .region_read(
                        &[ReadRequest {
                            eid: eid as u64,
                            offset: Block::new_with_ddef(
                                block_offset,
                                &region.def(),
                            ),
                        }],
                        JobId(0),
                    )
                    .await?;
                let response = responses.pop().unwrap();

                out_file.write_all(&response.data).unwrap();

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
pub async fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
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
        region.extend(extents_needed as u32).await?;
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

    let mut offset = Block::new_with_ddef(0, &region.def());
    loop {
        let mut buffer = vec![0; CHUNK_SIZE];

        /*
         * Read data into the buffer until it is full, or we hit EOF.
         */
        let mut total = 0;
        loop {
            assert!(total <= CHUNK_SIZE);
            if total == CHUNK_SIZE {
                break;
            }

            /*
             * Rust's read guarantees that if it returns Ok(n) then
             * `0 <= n <= buffer.len()`. We have to repeatedly read until our
             * buffer is full.
             */
            let n = f.read(&mut buffer[total..])?;

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
         * Use the same function upstairs uses to decide where to put the
         * data based on the LBA offset.
         */
        let nblocks = Block::from_bytes(total, &rm);
        let mut pos = Block::from_bytes(0, &rm);
        let mut writes = vec![];
        for (eid, offset) in
            extent_from_offset(&rm, offset, nblocks).blocks(&rm)
        {
            let len = Block::new_with_ddef(1, &region.def());
            let data = &buffer[pos.bytes()..(pos.bytes() + len.bytes())];
            let mut buffer = BytesMut::with_capacity(data.len());
            buffer.resize(data.len(), 0);
            buffer.copy_from_slice(data);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: buffer.freeze(),
                block_context: BlockContext {
                    hash: integrity_hash(&[data]),
                    encryption_context: None,
                },
            });

            pos.advance(len);
        }

        // We have no job ID, so it makes no sense for accounting.
        region.region_write(&writes, JobId(0), false).await?;

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
 * Debug function to dump the work list.
 */
pub async fn show_work(ds: &mut Downstairs) {
    let active_upstairs_connections = ds.active_upstairs();
    println!(
        "Active Upstairs connections: {:?}",
        active_upstairs_connections
    );

    for upstairs_connection in active_upstairs_connections {
        let work = ds.work_lock(upstairs_connection).await.unwrap();

        let mut kvec: Vec<JobId> = work.active.keys().cloned().collect();

        if kvec.is_empty() {
            println!("Crucible Downstairs work queue:  Empty");
        } else {
            println!("Crucible Downstairs work queue:");
            kvec.sort_unstable();
            for id in kvec.iter() {
                let dsw = work.active.get(id).unwrap();
                let (dsw_type, dep_list) = match &dsw.work {
                    IOop::Read {
                        dependencies,
                        requests: _,
                    } => ("Read", dependencies),
                    IOop::Write {
                        dependencies,
                        writes: _,
                    } => ("Write", dependencies),
                    IOop::Flush {
                        dependencies,
                        flush_number: _flush_number,
                        gen_number: _gen_number,
                        snapshot_details: _,
                        extent_limit: _,
                    } => ("Flush", dependencies),
                    IOop::WriteUnwritten {
                        dependencies,
                        writes: _,
                    } => ("WriteU", dependencies),
                    IOop::ExtentClose {
                        dependencies,
                        extent: _,
                    } => ("EClose", dependencies),
                    IOop::ExtentFlushClose {
                        dependencies,
                        extent: _,
                        flush_number: _flush_number,
                        gen_number: _gen_number,
                        source_downstairs: _,
                        repair_downstairs: _,
                    } => ("EFClose", dependencies),
                    IOop::ExtentLiveRepair {
                        dependencies,
                        extent: _,
                        source_downstairs: _,
                        source_repair_address: _,
                        repair_downstairs: _,
                    } => ("Repair", dependencies),
                    IOop::ExtentLiveReopen {
                        dependencies,
                        extent: _,
                    } => ("ReOpen", dependencies),
                    IOop::ExtentLiveNoOp { dependencies } => {
                        ("NoOp", dependencies)
                    }
                };
                println!(
                    "DSW:[{:04}] {:>07} {:>05} deps:{:?}",
                    id, dsw_type, dsw.state, dep_list,
                );
            }
        }

        println!("Done tasks {:?}", work.completed);
        println!("last_flush: {:?}", work.last_flush);
        println!("--------------------------------------");
    }
}

// DTrace probes for the downstairs
#[usdt::provider(provider = "crucible_downstairs")]
pub mod cdt {
    use crate::Arg;
    fn submit__read__start(_: u64) {}
    fn submit__writeunwritten__start(_: u64) {}
    fn submit__write__start(_: u64) {}
    fn submit__flush__start(_: u64) {}
    fn submit__el__close__start(_: u64) {}
    fn submit__el__flush__close__start(_: u64) {}
    fn submit__el__repair__start(_: u64) {}
    fn submit__el__reopen__start(_: u64) {}
    fn submit__el__noop__start(_: u64) {}
    fn work__start(_: u64) {}
    fn os__read__start(_: u64) {}
    fn os__writeunwritten__start(_: u64) {}
    fn os__write__start(_: u64) {}
    fn os__flush__start(_: u64) {}
    fn work__process(_: u64) {}
    fn os__read__done(_: u64) {}
    fn os__writeunwritten__done(_: u64) {}
    fn os__write__done(_: u64) {}
    fn os__flush__done(_: u64) {}
    fn submit__read__done(_: u64) {}
    fn submit__writeunwritten__done(_: u64) {}
    fn submit__write__done(_: u64) {}
    fn submit__flush__done(_: u64) {}
    fn extent__flush__start(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__done(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__file__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__file__done(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__collect__hashes__start(
        job_id: u64,
        extent_id: u32,
        num_dirty: u64,
    ) {
    }
    fn extent__flush__collect__hashes__done(
        job_id: u64,
        extent_id: u32,
        num_rehashed: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__get__hashes__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__get__hashes__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__sqlite__insert__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__raw__context__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__raw__context__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__read__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__get__contexts__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__get__contexts__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}

    fn extent__context__truncate__start(n_deletions: u64) {}
    fn extent__context__truncate__done() {}
    fn submit__el__close__done(_: u64) {}
    fn submit__el__flush__close__done(_: u64) {}
    fn submit__el__repair__done(_: u64) {}
    fn submit__el__reopen__done(_: u64) {}
    fn submit__el__noop__done(_: u64) {}
    fn work__done(_: u64) {}
}

// Check if a Message is valid on this downstairs or not.
// If not, then send the correct error on the provided channel, return false.
// If correct, then return true.
async fn is_message_valid<WT>(
    upstairs_connection: UpstairsConnection,
    upstairs_id: Uuid,
    session_id: Uuid,
    fw: &Mutex<FramedWrite<WT, CrucibleEncoder>>,
) -> Result<bool>
where
    WT: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    if upstairs_connection.upstairs_id != upstairs_id {
        let mut fw = fw.lock().await;
        fw.send(Message::UuidMismatch {
            expected_id: upstairs_connection.upstairs_id,
        })
        .await?;
        Ok(false)
    } else if upstairs_connection.session_id != session_id {
        let mut fw = fw.lock().await;
        fw.send(Message::UuidMismatch {
            expected_id: upstairs_connection.session_id,
        })
        .await?;
        Ok(false)
    } else {
        Ok(true)
    }
}

/*
 * A new IO request has been received.
 * If the message is a ping or negotiation message, send the correct
 * response. If the message is an IO, then put the new IO the work hashmap.
 * If the message is a repair message, then we handle it right here.
 */
async fn proc_frame<WT>(
    upstairs_connection: UpstairsConnection,
    ad: &Mutex<Downstairs>,
    m: Message,
    fw: &Mutex<FramedWrite<WT, CrucibleEncoder>>,
    job_channel_tx: &Mutex<Sender<()>>,
) -> Result<()>
where
    WT: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    let new_ds_id = match m {
        Message::Write {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            writes,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }
            cdt::submit__write__start!(|| job_id.0);

            let new_write = IOop::Write {
                dependencies,
                writes,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_write).await?;
            Some(job_id)
        }
        Message::Flush {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            flush_number,
            gen_number,
            snapshot_details,
            extent_limit,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }
            cdt::submit__flush__start!(|| job_id.0);

            let new_flush = IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_flush).await?;
            Some(job_id)
        }
        Message::WriteUnwritten {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            writes,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }
            cdt::submit__writeunwritten__start!(|| job_id.0);

            let new_write = IOop::WriteUnwritten {
                dependencies,
                writes,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_write).await?;
            Some(job_id)
        }
        Message::ReadRequest {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            requests,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }
            cdt::submit__read__start!(|| job_id.0);

            let new_read = IOop::Read {
                dependencies,
                requests,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_read).await?;
            Some(job_id)
        }
        // These are for repair while taking live IO
        Message::ExtentLiveClose {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            extent_id,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }

            cdt::submit__el__close__start!(|| job_id.0);
            // TODO: Add dtrace probes
            let ext_close = IOop::ExtentClose {
                dependencies,
                extent: extent_id,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, ext_close).await?;
            Some(job_id)
        }
        Message::ExtentLiveFlushClose {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            extent_id,
            flush_number,
            gen_number,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }

            cdt::submit__el__flush__close__start!(|| job_id.0);
            // Do both the flush, and then the close
            let new_flush = IOop::ExtentFlushClose {
                dependencies,
                extent: extent_id,
                flush_number,
                gen_number,
                source_downstairs: ClientId::new(0), // Unused in the downstairs
                repair_downstairs: vec![],           // Unused in the downstairs
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_flush).await?;
            Some(job_id)
        }
        Message::ExtentLiveRepair {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            extent_id,
            source_client_id,
            source_repair_address,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }

            cdt::submit__el__repair__start!(|| job_id.0);
            // Do both the flush, and then the close
            let new_repair = IOop::ExtentLiveRepair {
                dependencies,
                extent: extent_id,
                source_downstairs: source_client_id,
                source_repair_address,
                repair_downstairs: vec![],
            };

            let d = ad.lock().await;
            debug!(d.log, "Received ExtentLiveRepair {}", job_id);
            d.add_work(upstairs_connection, job_id, new_repair).await?;
            Some(job_id)
        }
        Message::ExtentLiveReopen {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
            extent_id,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }

            cdt::submit__el__reopen__start!(|| job_id.0);
            let new_open = IOop::ExtentLiveReopen {
                dependencies,
                extent: extent_id,
            };

            let d = ad.lock().await;
            d.add_work(upstairs_connection, job_id, new_open).await?;
            Some(job_id)
        }
        Message::ExtentLiveNoOp {
            upstairs_id,
            session_id,
            job_id,
            dependencies,
        } => {
            if !is_message_valid(
                upstairs_connection,
                upstairs_id,
                session_id,
                fw,
            )
            .await?
            {
                return Ok(());
            }
            cdt::submit__el__noop__start!(|| job_id.0);
            let new_open = IOop::ExtentLiveNoOp { dependencies };

            let d = ad.lock().await;
            debug!(d.log, "Received NoOP {}", job_id);
            d.add_work(upstairs_connection, job_id, new_open).await?;
            Some(job_id)
        }

        // These messages arrive during initial reconciliation.
        Message::ExtentFlush {
            repair_id,
            extent_id,
            client_id: _,
            flush_number,
            gen_number,
        } => {
            let msg = {
                let d = ad.lock().await;
                debug!(
                    d.log,
                    "{} Flush extent {} with f:{} g:{}",
                    repair_id,
                    extent_id,
                    flush_number,
                    gen_number
                );

                match d
                    .region
                    .region_flush_extent(
                        extent_id,
                        gen_number,
                        flush_number,
                        repair_id,
                    )
                    .await
                {
                    Ok(()) => Message::RepairAckId { repair_id },
                    Err(error) => Message::ExtentError {
                        repair_id,
                        extent_id,
                        error,
                    },
                }
            };
            let mut fw = fw.lock().await;
            fw.send(msg).await?;
            return Ok(());
        }
        Message::ExtentClose {
            repair_id,
            extent_id,
        } => {
            let msg = {
                let d = ad.lock().await;
                debug!(d.log, "{} Close extent {}", repair_id, extent_id);
                match d.region.close_extent(extent_id).await {
                    Ok(_) => Message::RepairAckId { repair_id },
                    Err(error) => Message::ExtentError {
                        repair_id,
                        extent_id,
                        error,
                    },
                }
            };
            let mut fw = fw.lock().await;
            fw.send(msg).await?;
            return Ok(());
        }
        Message::ExtentRepair {
            repair_id,
            extent_id,
            source_client_id,
            source_repair_address,
            dest_clients,
        } => {
            let msg = {
                let d = ad.lock().await;
                debug!(
                    d.log,
                    "{} Repair extent {} source:[{}] {:?} dest:{:?}",
                    repair_id,
                    extent_id,
                    source_client_id,
                    source_repair_address,
                    dest_clients
                );
                match d
                    .region
                    .repair_extent(extent_id, source_repair_address)
                    .await
                {
                    Ok(()) => Message::RepairAckId { repair_id },
                    Err(error) => Message::ExtentError {
                        repair_id,
                        extent_id,
                        error,
                    },
                }
            };
            let mut fw = fw.lock().await;
            fw.send(msg).await?;
            return Ok(());
        }
        Message::ExtentReopen {
            repair_id,
            extent_id,
        } => {
            let msg = {
                let mut d = ad.lock().await;
                debug!(d.log, "{} Reopen extent {}", repair_id, extent_id);
                match d.region.reopen_extent(extent_id).await {
                    Ok(()) => Message::RepairAckId { repair_id },
                    Err(error) => Message::ExtentError {
                        repair_id,
                        extent_id,
                        error,
                    },
                }
            };
            let mut fw = fw.lock().await;
            fw.send(msg).await?;
            return Ok(());
        }
        x => bail!("unexpected frame {:?}", x),
    };

    /*
     * If we added work, tell the work task to get busy.
     */
    if let Some(new_ds_id) = new_ds_id {
        cdt::work__start!(|| new_ds_id.0);
        job_channel_tx.lock().await.send(()).await?;
    }

    Ok(())
}

async fn do_work_task<T>(
    ads: &Mutex<Downstairs>,
    upstairs_connection: UpstairsConnection,
    mut job_channel_rx: Receiver<()>,
    fw: &Mutex<FramedWrite<T, CrucibleEncoder>>,
) -> Result<()>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    /*
     * job_channel_rx is a notification that we should look for new work.
     */
    while job_channel_rx.recv().await.is_some() {
        // Add a little time to completion for this operation.
        if ads.lock().await.lossy && random() && random() {
            info!(ads.lock().await.log, "[lossy] sleeping 1 second");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        if !ads.lock().await.is_active(upstairs_connection) {
            // We are not an active downstairs, wait until we are
            continue;
        }

        /*
         * Build ourselves a list of all the jobs on the work hashmap that
         * are New or DepWait.
         */
        let mut new_work: Vec<JobId> = {
            if let Ok(new_work) =
                ads.lock().await.new_work(upstairs_connection).await
            {
                new_work
            } else {
                // This means we couldn't unblock jobs for this UUID
                continue;
            }
        };

        /*
         * We don't have to do jobs in order, but the dependencies are, at
         * least for now, always going to be in order of job id.  So,
         * to best move things forward it is going to be fewer laps
         * through the list if we take the lowest job id first.
         */
        new_work.sort_unstable();

        while !new_work.is_empty() {
            let mut repeat_work = Vec::with_capacity(new_work.len());

            for new_id in new_work.drain(..) {
                if ads.lock().await.lossy && random() && random() {
                    // Skip a job that needs to be done. Sometimes
                    info!(ads.lock().await.log, "[lossy] skipping {}", new_id);
                    repeat_work.push(new_id);
                    continue;
                }

                /*
                 * If this job is still new, take it and go to work. The
                 * in_progress method will only return a job if all
                 * dependencies are met.
                 */
                let job_id = ads
                    .lock()
                    .await
                    .in_progress(upstairs_connection, new_id)
                    .await?;

                if let Some(job_id) = job_id {
                    cdt::work__process!(|| job_id.0);
                    let m = ads
                        .lock()
                        .await
                        .do_work(upstairs_connection, job_id)
                        .await?;

                    // If this is a repair job, and that repair failed, we
                    // can do no more work on this downstairs and should
                    // force everything to come down before more work arrives.
                    //
                    // However, we can respond to the upstairs with the failed
                    // result, and let the upstairs take action that will
                    // allow it to abort the repair and continue working in
                    // some degraded state.
                    let mut abort_needed = false;
                    if let Some(m) = m {
                        abort_needed = check_message_for_abort(&m);

                        if let Some(error) = m.err() {
                            let mut fw = fw.lock().await;
                            fw.send(&Message::ErrorReport {
                                upstairs_id: upstairs_connection.upstairs_id,
                                session_id: upstairs_connection.session_id,
                                job_id: new_id,
                                error: error.clone(),
                            })
                            .await?;
                            drop(fw);

                            // If the job errored, do not consider it completed.
                            // Retry it.
                            repeat_work.push(new_id);
                        } else {
                            // The job completed successfully, so inform the
                            // Upstairs

                            ads.lock()
                                .await
                                .complete_work_stat(
                                    upstairs_connection,
                                    &m,
                                    job_id,
                                )
                                .await?;

                            // Notify the upstairs before completing work
                            let mut fw = fw.lock().await;
                            fw.send(&m).await?;
                            drop(fw);

                            ads.lock()
                                .await
                                .complete_work(upstairs_connection, job_id, m)
                                .await?;

                            cdt::work__done!(|| job_id.0);
                        }
                    }

                    // Now, if the message requires an abort, we handle
                    // that now by exiting this task with error.
                    if abort_needed {
                        bail!("Repair has failed, exiting task");
                    }
                }
            }

            new_work = repeat_work;
        }
    }

    // None means the channel is closed
    Ok(())
}

// Check and see if this message is A LiveRepair, and if it has failed. If you
// change this, change how the Upstairs processes ErrorReports!
fn check_message_for_abort(m: &Message) -> bool {
    if let Message::ExtentLiveRepairAckId { result, .. } = m {
        if result.is_err() {
            return true;
        }
    }

    false
}

async fn proc_stream(
    ads: &Arc<Mutex<Downstairs>>,
    stream: WrappedStream,
) -> Result<()> {
    match stream {
        WrappedStream::Http(sock) => {
            let (read, write) = sock.into_split();

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = Arc::new(Mutex::new(FramedWrite::new(
                write,
                CrucibleEncoder::new(),
            )));

            proc(ads, fr, fw).await
        }
        WrappedStream::Https(stream) => {
            let (read, write) = tokio::io::split(stream);

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = Arc::new(Mutex::new(FramedWrite::new(
                write,
                CrucibleEncoder::new(),
            )));

            proc(ads, fr, fw).await
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UpstairsConnection {
    upstairs_id: Uuid,
    session_id: Uuid,
    gen: u64,
}

/*
 * This function handles the initial negotiation steps between the
 * upstairs and the downstairs.  Either we return error, or we call
 * the next function if everything was successful and we can start
 * taking IOs from the upstairs.
 */
async fn proc<RT, WT>(
    ads: &Arc<Mutex<Downstairs>>,
    mut fr: FramedRead<RT, CrucibleDecoder>,
    fw: Arc<Mutex<FramedWrite<WT, CrucibleEncoder>>>,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    // In this function, repair address should exist, and shouldn't change. Grab
    // it here.
    let repair_addr = ads.lock().await.repair_address.unwrap();

    let mut upstairs_connection: Option<UpstairsConnection> = None;

    let (_another_upstairs_active_tx, mut another_upstairs_active_rx) =
        channel::<UpstairsConnection>(1);
    let another_upstairs_active_tx = Arc::new(_another_upstairs_active_tx);

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum NegotiationState {
        Start,
        ConnectedToUpstairs,
        PromotedToActive,
        SentRegionInfo,
        Ready,
    }
    let mut negotiated = NegotiationState::Start;
    let log = ads.lock().await.log.new(o!("task" => "proc".to_string()));
    /*
     * See the comment in the proc() function on the upstairs side that
     * describes how this negotiation takes place.
     *
     * The final step in negotiation (as dictated by the upstairs) is
     * either LastFlush, or ExtentVersionsPlease.  Once we respond to
     * that message, we can move forward and start receiving IO from
     * the upstairs.
     */
    while negotiated != NegotiationState::Ready {
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
            new_upstairs_connection = another_upstairs_active_rx.recv() => {
                match new_upstairs_connection {
                    None => {
                        // There shouldn't be a path through the code where we
                        // close the channel before sending a message through it
                        // (see [`promote_to_active`]), though [`clear_active`]
                        // simply drops the active_upstairs tuple - but the only
                        // place that calls `clear_active` is below when the
                        // Upstairs disconnects.
                        //
                        // We have to bail here though - the Downstairs can't be
                        // running without the ability for another Upstairs to
                        // kick out the previous one during activation.
                        bail!("another_upstairs_active_rx closed during \
                            negotiation");
                    }

                    Some(new_upstairs_connection) => {
                        // another upstairs negotiated and went active after
                        // this one did (and before this one completed
                        // negotiation)
                        let upstairs_connection = upstairs_connection.unwrap();
                        warn!(
                            log,
                            "Another upstairs {:?} promoted to active, \
                            shutting down connection for {:?}",
                            new_upstairs_connection, upstairs_connection);

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::YouAreNoLongerActive {
                            new_upstairs_id:
                                new_upstairs_connection.upstairs_id,
                            new_session_id:
                                new_upstairs_connection.session_id,
                            new_gen: new_upstairs_connection.gen,
                        }).await {
                            warn!(
                                log,
                                "Notify upstairs:{} session:{} not active failed:{}",
                                upstairs_connection.upstairs_id,
                                upstairs_connection.session_id,
                                e,
                            );
                        }
                        return Ok(());
                    }
                }
            }

            new_read = fr.next() => {
                /*
                 * Negotiate protocol before we take any IO requests.
                 */
                match new_read.transpose()? {
                    None => {
                        // Upstairs disconnected
                        let mut ds = ads.lock().await;

                        if let Some(upstairs_connection) = upstairs_connection {

                            // If our upstairs never completed activation,
                            // or some other upstairs activated, we won't
                            // be able to report how many jobs.
                            match ds.jobs(upstairs_connection).await {
                                Ok(jobs) => {
                                    info!(
                                        log,
                                        "upstairs {:?} disconnected, {} jobs left",
                                        upstairs_connection,
                                        jobs,
                                    );
                                }
                                Err(e) => {
                                    info!(
                                        log,
                                        "upstairs {:?} disconnected, {}",
                                        upstairs_connection, e
                                    );
                                }
                            }

                            if ds.is_active(upstairs_connection) {
                                info!(
                                    log,
                                    "upstairs {:?} was previously \
                                    active, clearing", upstairs_connection);
                                ds.clear_active(upstairs_connection).await?;
                            }
                        } else {
                            info!(log, "unknown upstairs disconnected");
                        }

                        return Ok(());
                    }
                    Some(Message::Ruok) => {
                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::Imok).await {
                            bail!("Failed to answer ping: {}", e);
                        }
                    }
                    Some(Message::HereIAm {
                        version,
                        upstairs_id,
                        session_id,
                        gen,
                        read_only,
                        encrypted,
                        alternate_versions,
                    }) => {
                        if negotiated != NegotiationState::Start {
                            bail!("Received connect out of order {:?}",
                                negotiated);
                        }
                        info!(log, "Connection request from {} with version {}",
                            upstairs_id, version);

                        // Verify we can communicate with the upstairs.  First
                        // check our message version.  If that fails,  check
                        // to see if our version is one of the supported
                        // versions the upstairs has told us it can support.
                        if version != CRUCIBLE_MESSAGE_VERSION {
                            if alternate_versions
                                .contains(&CRUCIBLE_MESSAGE_VERSION)
                            {
                                warn!(
                                    log,
                                    "downstairs and upstairs using different \
                                     but compatible versions, Upstairs is {}, \
                                     but supports {:?}, downstairs is {}",
                                    version,
                                    alternate_versions,
                                    CRUCIBLE_MESSAGE_VERSION,
                                );
                            } else {
                                let m = Message::VersionMismatch {
                                    version: CRUCIBLE_MESSAGE_VERSION,
                                };
                                let mut fw = fw.lock().await;
                                if let Err(e) = fw.send(m).await {
                                    warn!(
                                        log,
                                        "Failed to send VersionMismatch: {}",
                                        e
                                    );
                                }
                                bail!(
                                    "Required version {}, Or {:?} got {}",
                                    CRUCIBLE_MESSAGE_VERSION,
                                    alternate_versions,
                                    version,
                                );
                            }
                        }

                        // Reject an Upstairs negotiation if there is a mismatch
                        // of expectation, and terminate the connection - the
                        // Upstairs will not be able to successfully negotiate.
                        {
                            let ds = ads.lock().await;
                            if ds.read_only != read_only {
                                let mut fw = fw.lock().await;

                                if let Err(e) = fw.send(Message::ReadOnlyMismatch {
                                    expected: ds.read_only,
                                }).await {
                                    warn!(log, "Failed to send ReadOnlyMismatch: {}", e);
                                }

                                bail!("closing connection due to read-only \
                                    mismatch");
                            }

                            if ds.encrypted != encrypted {
                                let mut fw = fw.lock().await;

                                if let Err(e) = fw.send(Message::EncryptedMismatch {
                                    expected: ds.encrypted,
                                }).await {
                                    warn!(log, "Failed to send EncryptedMismatch: {}", e);
                                }

                                bail!("closing connection due to encryption \
                                    mismatch");
                            }
                        }

                        negotiated = NegotiationState::ConnectedToUpstairs;
                        upstairs_connection = Some(UpstairsConnection {
                            upstairs_id,
                            session_id,
                            gen,
                        });
                        info!(
                            log, "upstairs {:?} connected, version {}",
                            upstairs_connection.unwrap(),
                            CRUCIBLE_MESSAGE_VERSION);

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(
                            Message::YesItsMe {
                                version: CRUCIBLE_MESSAGE_VERSION,
                                repair_addr
                            }
                        ).await {
                            bail!("Failed sending YesItsMe: {}", e);
                        }
                    }
                    Some(Message::PromoteToActive {
                        upstairs_id,
                        session_id,
                        gen,
                    }) => {
                        if negotiated != NegotiationState::ConnectedToUpstairs {
                            bail!("Received activate out of order {:?}",
                                negotiated);
                        }

                        // Only allowed to promote or demote self
                        let upstairs_connection =
                            upstairs_connection.as_mut().unwrap();
                        let matches_self =
                            upstairs_connection.upstairs_id == upstairs_id &&
                            upstairs_connection.session_id == session_id;

                        if !matches_self {
                            let mut fw = fw.lock().await;
                            if let Err(e) = fw.send(
                                Message::UuidMismatch {
                                    expected_id:
                                        upstairs_connection.upstairs_id,
                                }
                            ).await {
                                warn!(log, "Failed sending UuidMismatch: {}", e);
                            }
                            bail!(
                                "Upstairs connection expected \
                                upstairs_id:{} session_id:{}  received \
                                upstairs_id:{} session_id:{}",
                                upstairs_connection.upstairs_id,
                                upstairs_connection.session_id,
                                upstairs_id,
                                session_id
                            );

                        } else {
                            if upstairs_connection.gen != gen {
                                warn!(
                                    log,
                                    "warning: generation number at \
                                    negotiation was {} and {} at \
                                    activation, updating",
                                    upstairs_connection.gen,
                                    gen,
                                );

                                upstairs_connection.gen = gen;
                            }

                            {
                                let mut ds = ads.lock().await;

                                ds.promote_to_active(
                                    *upstairs_connection,
                                    another_upstairs_active_tx.clone()
                                ).await?;
                            }
                            negotiated = NegotiationState::PromotedToActive;

                            let mut fw = fw.lock().await;
                            if let Err(e) = fw.send(Message::YouAreNowActive {
                                upstairs_id,
                                session_id,
                                gen,
                            }).await {
                                bail!("Failed sending YouAreNewActive: {}", e);
                            }
                        }
                    }
                    Some(Message::RegionInfoPlease) => {
                        if negotiated != NegotiationState::PromotedToActive {
                            bail!("Received RegionInfo out of order {:?}",
                                negotiated);
                        }
                        negotiated = NegotiationState::SentRegionInfo;
                        let region_def = {
                            let ds = ads.lock().await;
                            ds.region.def()
                        };

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::RegionInfo { region_def }).await {
                            bail!("Failed sending RegionInfo: {}", e);
                        }
                    }
                    Some(Message::LastFlush { last_flush_number }) => {
                        if negotiated != NegotiationState::SentRegionInfo {
                            bail!("Received LastFlush out of order {:?}",
                                negotiated);
                        }

                        negotiated = NegotiationState::Ready;

                        {
                            let ds = ads.lock().await;
                            let mut work = ds.work_lock(
                                upstairs_connection.unwrap(),
                            ).await?;
                            work.last_flush = last_flush_number;
                            info!(
                                log,
                                "Set last flush {}", last_flush_number);
                        }

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::LastFlushAck {
                            last_flush_number
                        }).await {
                            bail!("Failed sending LastFlushAck: {}", e);
                        }

                        /*
                         * Once this command is sent, we are ready to exit
                         * the loop and move forward with receiving IOs
                         */
                    }
                    Some(Message::ExtentVersionsPlease) => {
                        if negotiated != NegotiationState::SentRegionInfo {
                            bail!("Received ExtentVersions out of order {:?}",
                                negotiated);
                        }
                        negotiated = NegotiationState::Ready;
                        let ds = ads.lock().await;
                        let meta_info = ds.region.meta_info().await?;
                        drop(ds);

                        let flush_numbers: Vec<_> = meta_info
                            .iter()
                            .map(|m| m.flush_number)
                            .collect();
                        let gen_numbers: Vec<_> = meta_info
                            .iter()
                            .map(|m| m.gen_number)
                            .collect();
                        let dirty_bits: Vec<_> = meta_info
                            .iter()
                            .map(|m| m.dirty)
                            .collect();
                        if flush_numbers.len() > 12 {
                            info!(
                                log,
                                "Current flush_numbers [0..12]: {:?}",
                                &flush_numbers[0..12]
                            );
                        } else {
                            info!(
                                log,
                                "Current flush_numbers [0..12]: {:?}",
                                flush_numbers);
                        }

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::ExtentVersions {
                            gen_numbers,
                            flush_numbers,
                            dirty_bits,
                        })
                        .await {
                            bail!("Failed sending ExtentVersions: {}", e);
                        }

                        /*
                         * Once this command is sent, we are ready to exit
                         * the loop and move forward with receiving IOs
                         */
                    }
                    Some(_msg) => {
                        warn!(
                            log,
                            "Ignored message received during negotiation"
                        );
                    }
                }
            }
        }
    }

    info!(log, "Downstairs has completed Negotiation");
    assert!(upstairs_connection.is_some());
    let upstairs_connection = upstairs_connection.unwrap();

    resp_loop(ads, fr, fw, another_upstairs_active_rx, upstairs_connection)
        .await
}

/*
 * This function listens for and answers requests from the upstairs.
 * We assume here that correct negotiation has taken place and this
 * downstairs is ready to receive IO.
 */
async fn resp_loop<RT, WT>(
    ads: &Arc<Mutex<Downstairs>>,
    mut fr: FramedRead<RT, CrucibleDecoder>,
    fw: Arc<Mutex<FramedWrite<WT, CrucibleEncoder>>>,
    mut another_upstairs_active_rx: mpsc::Receiver<UpstairsConnection>,
    upstairs_connection: UpstairsConnection,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    let mut lossy_interval = deadline_secs(5);
    // Create the log for this task to use.
    let log = ads.lock().await.log.new(o!("task" => "main".to_string()));

    // Give our work queue a little more space than we expect the upstairs
    // to ever send us.
    let (_job_channel_tx, job_channel_rx) = channel(MAX_ACTIVE_COUNT + 50);
    let job_channel_tx = Arc::new(Mutex::new(_job_channel_tx));

    /*
     * Create tasks for:
     *  Doing the work then sending the ACK
     *  Pulling work off the socket and putting on the work queue.
     *
     * These tasks and this function must be able to handle the
     * Upstairs connection going away at any time, as well as a forced
     * migration where a new Upstairs connects and the old (current from
     * this threads point of view) work is discarded.
     * As migration or upstairs failure can happen at any time, this
     * function must watch for tasks going away and handle that
     * gracefully.  By exiting the loop here, we allow the calling
     * function to take over and handle a reconnect or a new upstairs
     * takeover.
     */
    let dw_task = {
        let adc = ads.clone();
        let fwc = fw.clone();
        tokio::spawn(async move {
            do_work_task(&adc, upstairs_connection, job_channel_rx, &fwc).await
        })
    };

    let (message_channel_tx, mut message_channel_rx) =
        channel(MAX_ACTIVE_COUNT + 50);
    let pf_task = {
        let adc = ads.clone();
        let tx = job_channel_tx.clone();
        let fwc = fw.clone();
        tokio::spawn(async move {
            while let Some(m) = message_channel_rx.recv().await {
                if let Err(e) =
                    proc_frame(upstairs_connection, &adc, m, &fwc, &tx).await
                {
                    bail!("Proc frame returns error: {}", e);
                }
            }
            Ok(())
        })
    };
    let lossy = ads.lock().await.lossy;

    tokio::pin!(dw_task);
    tokio::pin!(pf_task);
    loop {
        tokio::select! {
            e = &mut dw_task => {
                bail!("do_work_task task has ended: {:?}", e);
            }
            e = &mut pf_task => {
                bail!("pf task ended: {:?}", e);
            }
            /*
             * If we have set "lossy", then we need to check every now and
             * then that there were not skipped jobs that we need to go back
             * and finish up. If lossy is not set, then this should only
             * trigger once then never again.
             */
            _ = sleep_until(lossy_interval), if lossy => {
                job_channel_tx.lock().await.send(()).await?;
                lossy_interval = deadline_secs(5);
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
            new_upstairs_connection = another_upstairs_active_rx.recv() => {
                match new_upstairs_connection {
                    None => {
                        // There shouldn't be a path through the code where we
                        // close the channel before sending a message through it
                        // (see [`promote_to_active`]), though [`clear_active`]
                        // simply drops the active_upstairs tuple - but the only
                        // place that calls `clear_active` is below when the
                        // Upstairs disconnects.
                        //
                        // We have to bail here though - the Downstairs can't be
                        // running without the ability for another Upstairs to
                        // kick out the previous one during activation.
                        bail!("another_upstairs_active_rx closed during \
                            resp_loop");
                    }

                    Some(new_upstairs_connection) => {
                        // another upstairs negotiated and went active after
                        // this one did
                        warn!(
                            log,
                            "Another upstairs {:?} promoted to active, \
                            shutting down connection for {:?}",
                            new_upstairs_connection, upstairs_connection);

                        let mut fw = fw.lock().await;
                        if let Err(e) = fw.send(Message::YouAreNoLongerActive {
                            new_upstairs_id:
                                new_upstairs_connection.upstairs_id,
                            new_session_id:
                                new_upstairs_connection.session_id,
                            new_gen: new_upstairs_connection.gen,
                        }).await {
                            warn!(log, "Failed sending YouAreNoLongerActive: {}", e);
                        }

                        return Ok(());
                    }
                }
            }
            new_read = fr.next() => {
                match new_read {
                    None => {
                        // Upstairs disconnected
                        let mut ds = ads.lock().await;

                        warn!(
                            log,
                            "upstairs {:?} disconnected, {} jobs left",
                            upstairs_connection,
                            ds.jobs(upstairs_connection).await?,
                        );

                        if ds.is_active(upstairs_connection) {
                            warn!(log, "upstairs {:?} was previously \
                                active, clearing", upstairs_connection);
                            ds.clear_active(upstairs_connection).await?;
                        }

                        return Ok(());
                    }
                    Some(Ok(msg)) => {
                        if matches!(msg, Message::Ruok) {
                            // Respond instantly to pings, don't wait.
                            let mut fw = fw.lock().await;
                            if let Err(e) = fw.send(Message::Imok).await {
                                bail!("Failed sending Imok: {}", e);
                            }
                        } else if let Err(e) = message_channel_tx.send(msg).await {
                            bail!("Failed sending message to proc_frame: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        // XXX "unexpected end of file" can occur if upstairs
                        // terminates, we don't yet have a HangUp message
                        bail!("Error reading from Upstairs: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ActiveUpstairs {
    pub upstairs_connection: UpstairsConnection,
    pub work: Mutex<Work>,
    pub terminate_sender: Arc<Sender<UpstairsConnection>>,
}

/*
 * Overall structure for things the downstairs is tracking.
 * This includes the extents and their status as well as the
 * downstairs work queue.
 */
#[derive(Debug)]
pub struct Downstairs {
    pub region: Region,
    lossy: bool,        // Test flag, enables pauses and skipped jobs
    read_errors: bool,  // Test flag
    write_errors: bool, // Test flag
    flush_errors: bool, // Test flag
    active_upstairs: HashMap<Uuid, ActiveUpstairs>,
    dss: DsStatOuter,
    read_only: bool,
    encrypted: bool,
    pub address: Option<SocketAddr>,
    pub repair_address: Option<SocketAddr>,
    log: Logger,
}

#[allow(clippy::too_many_arguments)]
impl Downstairs {
    fn new(
        region: Region,
        lossy: bool,
        read_errors: bool,
        write_errors: bool,
        flush_errors: bool,
        read_only: bool,
        encrypted: bool,
        log: Logger,
    ) -> Self {
        let dss = DsStatOuter {
            ds_stat_wrap: Arc::new(Mutex::new(DsCountStat::new(
                region.def().uuid(),
            ))),
        };
        Downstairs {
            region,
            lossy,
            read_errors,
            write_errors,
            flush_errors,
            active_upstairs: HashMap::new(),
            dss,
            read_only,
            encrypted,
            address: None,
            repair_address: None,
            log,
        }
    }

    /*
     * Only grab the lock if the UpstairsConnection matches.
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
     * `promote_to_active` wins the race, it will change the Downstairs'
     * active UpstairsConnection, and send the terminate signal to the
     * tasks that are communicating to the previously active Upstairs
     * (along with terminating the Downstairs tasks). If `add_work` for
     * the previous Upstairs then does fire, it will fail to
     * grab the lock because the UpstairsConnection is no longer active, and
     * that `add_work` thread should close.
     *
     * Let's say `new_work` and `promote_to_active` are racing. If `new_work`
     * wins, then it will return and run those jobs in `do_work_task`.
     * However, `promote_to_active` will grab the lock and change the
     * active UpstairsConnection, causing `do_work` to return
     * UpstairsInactive for the jobs that were just returned. If
     * `promote_to_active` wins, it will clear out the jobs of the old
     * Upstairs.
     *
     * Grabbing the lock in this way should properly clear out the previously
     * active Upstairs without causing jobs to be incorrectly sent to the
     * newly active Upstairs.
     */
    async fn work_lock(
        &self,
        upstairs_connection: UpstairsConnection,
    ) -> Result<MutexGuard<'_, Work>> {
        let upstairs_uuid = upstairs_connection.upstairs_id;
        if !self.active_upstairs.contains_key(&upstairs_uuid) {
            warn!(
                self.log,
                "{:?} cannot grab work lock, {} is not active!",
                upstairs_connection,
                upstairs_uuid,
            );

            bail!(CrucibleError::UpstairsInactive);
        }

        let active_upstairs = self.active_upstairs.get(&upstairs_uuid).unwrap();

        if active_upstairs.upstairs_connection != upstairs_connection {
            warn!(
                self.log,
                "{:?} cannot grab lock, does not match {:?}!",
                upstairs_connection,
                active_upstairs.upstairs_connection,
            );

            bail!(CrucibleError::UpstairsInactive)
        }

        Ok(active_upstairs.work.lock().await)
    }

    async fn jobs(
        &self,
        upstairs_connection: UpstairsConnection,
    ) -> Result<usize> {
        let work = self.work_lock(upstairs_connection).await?;
        Ok(work.jobs())
    }

    async fn new_work(
        &self,
        upstairs_connection: UpstairsConnection,
    ) -> Result<Vec<JobId>> {
        let work = self.work_lock(upstairs_connection).await?;
        Ok(work.new_work(upstairs_connection))
    }

    // Add work to the Downstairs
    async fn add_work(
        &self,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        work: IOop,
    ) -> Result<()> {
        // The Upstairs will send Flushes periodically, even in read only mode
        // we have to accept them. But read-only should never accept writes!
        if self.read_only {
            let is_write = match work {
                IOop::Write { .. }
                | IOop::WriteUnwritten { .. }
                | IOop::ExtentClose { .. }
                | IOop::ExtentFlushClose { .. }
                | IOop::ExtentLiveRepair { .. }
                | IOop::ExtentLiveReopen { .. }
                | IOop::ExtentLiveNoOp { .. } => true,
                IOop::Read { .. } | IOop::Flush { .. } => false,
            };

            if is_write {
                error!(self.log, "read-only but received write {:?}", work);
                bail!(CrucibleError::ModifyingReadOnlyRegion);
            }
        }

        let dsw = DownstairsWork {
            upstairs_connection,
            ds_id,
            work,
            state: WorkState::New,
        };

        let mut work = self.work_lock(upstairs_connection).await?;
        work.add_work(ds_id, dsw);

        Ok(())
    }

    #[cfg(test)]
    async fn get_job(
        &self,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
    ) -> Result<DownstairsWork> {
        let mut work = self.work_lock(upstairs_connection).await?;
        Ok(work.get_job(ds_id))
    }

    // Downstairs, move a job to in_progress, if we can
    async fn in_progress(
        &self,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
    ) -> Result<Option<JobId>> {
        let job = {
            let log = self.log.new(o!("role" => "work".to_string()));
            let mut work = self.work_lock(upstairs_connection).await?;
            work.in_progress(ds_id, log)
        };

        if let Some((job_id, upstairs_connection)) = job {
            if !self.is_active(upstairs_connection) {
                // Don't return a job with the wrong uuid! `promote_to_active`
                // should have removed any active jobs, and
                // `work.new_work` should have filtered on the correct UUID.
                panic!("Don't return a job for a non-active connection!");
            }

            Ok(Some(job_id))
        } else {
            Ok(None)
        }
    }

    // Given a job ID, do the work for that IO.
    //
    // Take a IOop type and (after some error checking), do the work
    // required for that IOop, storing the result.
    // On completion, construct the corresponding Crucible Message
    // containing the response to it.  The caller is responsible for sending
    // that response back to the upstairs.
    async fn do_work(
        &mut self,
        upstairs_connection: UpstairsConnection,
        job_id: JobId,
    ) -> Result<Option<Message>> {
        let job = {
            let mut work = self.work_lock(upstairs_connection).await?;
            let job = work.get_ready_job(job_id);

            // `promote_to_active` can clear out the Work struct for this
            // UpstairsConnection, but the tasks can still be working on
            // outdated job IDs. If that happens, `get_ready_job` will return a
            // None, so bail early here.
            if job.is_none() {
                return Ok(None);
            }

            job.unwrap()
        };

        assert_eq!(job.ds_id, job_id);
        match &job.work {
            IOop::Read {
                dependencies,
                requests,
            } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let responses = if self.read_errors && random() && random() {
                    warn!(self.log, "returning error on read!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region.region_read(requests, job_id).await
                };
                debug!(
                    self.log,
                    "Read      :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    responses.is_ok(),
                );

                Ok(Some(Message::ReadResponse {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    responses,
                }))
            }
            IOop::WriteUnwritten {
                dependencies: _dependencies,
                writes,
            } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let result = if self.write_errors && random() && random() {
                    warn!(self.log, "returning error on writeunwritten!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    // The region_write will handle what happens to each block
                    // based on if they have data or not.
                    self.region.region_write(writes, job_id, true).await
                };

                Ok(Some(Message::WriteUnwrittenAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
            IOop::Write {
                dependencies,
                writes,
            } => {
                let result = if self.write_errors && random() && random() {
                    warn!(self.log, "returning error on write!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region.region_write(writes, job_id, false).await
                };
                debug!(
                    self.log,
                    "Write     :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );

                Ok(Some(Message::WriteAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
            IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            } => {
                let result = if self.flush_errors && random() && random() {
                    warn!(self.log, "returning error on flush!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region
                        .region_flush(
                            *flush_number,
                            *gen_number,
                            snapshot_details,
                            job_id,
                            *extent_limit,
                        )
                        .await
                };
                debug!(
                    self.log,
                    "Flush     :{} extent_limit {:?} deps:{:?} res:{} f:{} g:{}",
                    job_id, extent_limit, dependencies, result.is_ok(),
                    flush_number, gen_number,
                );

                Ok(Some(Message::FlushAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
            IOop::ExtentClose {
                dependencies,
                extent,
            } => {
                let result = if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region.close_extent(*extent).await
                };
                debug!(
                    self.log,
                    "JustClose :{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Ok(Some(Message::ExtentLiveCloseAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }))
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
                source_downstairs: _,
                repair_downstairs: _,
            } => {
                let result = if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    // If flush fails, return that result.
                    // Else, if close fails, return that result.
                    // Else, return the f/g/d from the close.
                    match self
                        .region
                        .region_flush_extent(
                            *extent,
                            *gen_number,
                            *flush_number,
                            job_id,
                        )
                        .await
                    {
                        Err(f_res) => Err(f_res),
                        Ok(_) => self.region.close_extent(*extent).await,
                    }
                };

                debug!(
                    self.log,
                    "FlushClose:{} extent {} deps:{:?} res:{} f:{} g:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                    flush_number,
                    gen_number,
                );

                Ok(Some(Message::ExtentLiveCloseAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }))
            }
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs: _,
                source_repair_address,
                repair_downstairs: _,
            } => {
                debug!(
                    self.log,
                    "ExtentLiveRepair: extent {} sra:{:?}",
                    extent,
                    source_repair_address
                );
                let result = if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region
                        .repair_extent(*extent, *source_repair_address)
                        .await
                };
                debug!(
                    self.log,
                    "LiveRepair:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Ok(Some(Message::ExtentLiveRepairAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                let result = if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    self.region.reopen_extent(*extent).await
                };
                debug!(
                    self.log,
                    "LiveReopen:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );
                Ok(Some(Message::ExtentLiveAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
            IOop::ExtentLiveNoOp { dependencies } => {
                debug!(self.log, "Work of: LiveNoOp {}", job_id);
                let result = if !self.is_active(job.upstairs_connection) {
                    error!(self.log, "Upstairs inactive error");
                    Err(CrucibleError::UpstairsInactive)
                } else {
                    Ok(())
                };
                debug!(
                    self.log,
                    "LiveNoOp  :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );
                Ok(Some(Message::ExtentLiveAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }))
            }
        }
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
        &self,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        m: Message,
    ) -> Result<()> {
        let mut work = self.work_lock(upstairs_connection).await?;

        // Complete the job
        let is_flush = matches!(m, Message::FlushAck { .. });

        // If upstairs_connection grabs the work lock, it is the active
        // connection for this Upstairs UUID. The job should exist in the Work
        // struct. If it does not, then we're in the case where the same
        // Upstairs has reconnected and been promoted to active, meaning
        // `work.clear()` was run. If that's the case, then do not alter the
        // Work struct, because there's now two tasks running for the same
        // UpstairsConnection, and we're the one that should be on the way out
        // due to a message on the terminate_sender channel.
        if work.active.remove(&ds_id).is_some() {
            if is_flush {
                work.last_flush = ds_id;
                work.completed = Vec::with_capacity(32);
            } else {
                work.completed.push(ds_id);
            }
        }

        Ok(())
    }

    /*
     * After we complete a read/write/flush on a region, update the
     * Oximeter counter for the operation.
     */
    async fn complete_work_stat(
        &mut self,
        _upstairs_connection: UpstairsConnection,
        m: &Message,
        ds_id: JobId,
    ) -> Result<()> {
        // XXX dss per upstairs connection?
        match m {
            Message::FlushAck { .. } => {
                cdt::submit__flush__done!(|| ds_id.0);
                self.dss.add_flush().await;
            }
            Message::WriteAck { .. } => {
                cdt::submit__write__done!(|| ds_id.0);
                self.dss.add_write().await;
            }
            Message::WriteUnwrittenAck { .. } => {
                cdt::submit__writeunwritten__done!(|| ds_id.0);
                self.dss.add_write().await;
            }
            Message::ReadResponse { .. } => {
                cdt::submit__read__done!(|| ds_id.0);
                self.dss.add_read().await;
            }
            Message::ExtentLiveClose { .. } => {
                cdt::submit__el__close__done!(|| ds_id.0);
            }
            Message::ExtentLiveFlushClose { .. } => {
                cdt::submit__el__flush__close__done!(|| ds_id.0);
            }
            Message::ExtentLiveRepair { .. } => {
                cdt::submit__el__repair__done!(|| ds_id.0);
            }
            Message::ExtentLiveReopen { .. } => {
                cdt::submit__el__reopen__done!(|| ds_id.0);
            }
            Message::ExtentLiveNoOp { .. } => {
                cdt::submit__el__noop__done!(|| ds_id.0);
            }
            _ => (),
        }

        Ok(())
    }

    async fn promote_to_active(
        &mut self,
        upstairs_connection: UpstairsConnection,
        tx: Arc<Sender<UpstairsConnection>>,
    ) -> Result<()> {
        if self.read_only {
            // Multiple active read-only sessions are allowed, but multiple
            // sessions for the same Upstairs UUID are not. Kick out a
            // previously active session for this UUID if one exists. Do this
            // while holding the work lock so the previously active Upstairs
            // isn't adding more work.
            if let Some(active_upstairs) =
                self.active_upstairs.get(&upstairs_connection.upstairs_id)
            {
                let mut work = active_upstairs.work.lock().await;

                info!(
                    self.log,
                    "Signaling to {:?} thread that {:?} is being \
                    promoted (read-only)",
                    active_upstairs.upstairs_connection,
                    upstairs_connection,
                );

                match active_upstairs
                    .terminate_sender
                    .send(upstairs_connection)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        /*
                         * It's possible the old thread died due to some
                         * connection error. In that case the
                         * receiver will have closed and
                         * the above send will fail.
                         */
                        error!(
                            self.log,
                            "Error while signaling to {:?} thread: {:?}",
                            active_upstairs.upstairs_connection,
                            e,
                        );
                    }
                }

                // Note: in the future, differentiate between new upstairs
                // connecting vs same upstairs reconnecting here.
                //
                // Clear out active jobs, the last flush, and completed
                // information, as that will not be valid any longer.
                //
                // TODO: Really work through this error case
                if work.active.keys().len() > 0 {
                    warn!(
                        self.log,
                        "Crucible Downstairs promoting {:?} to active, \
                        discarding {} jobs",
                        upstairs_connection,
                        work.active.keys().len()
                    );
                }

                // In the future, we may decide there is some way to continue
                // working on outstanding jobs, or a way to merge. But for now,
                // we just throw out what we have and let the upstairs resend
                // anything to us that it did not get an ACK for.
                work.clear();
            } else {
                // There is no current session for this Upstairs UUID.
            }

            // Insert a new session, overwritting the previous entry if the
            // Upstairs UUID has an entry already.
            self.active_upstairs.insert(
                upstairs_connection.upstairs_id,
                ActiveUpstairs {
                    upstairs_connection,
                    work: Mutex::new(Work::new()),
                    terminate_sender: tx,
                },
            );

            Ok(())
        } else {
            // Only one active read-write session is allowed. Kick out the
            // currently active Upstairs session if one exists.
            let currently_active_upstairs_uuids: Vec<Uuid> =
                self.active_upstairs.keys().copied().collect();

            match currently_active_upstairs_uuids.len() {
                0 => {
                    // No currently active Upstairs sessions
                    self.active_upstairs.insert(
                        upstairs_connection.upstairs_id,
                        ActiveUpstairs {
                            upstairs_connection,
                            work: Mutex::new(Work::new()),
                            terminate_sender: tx,
                        },
                    );

                    assert_eq!(self.active_upstairs.len(), 1);

                    // Re-open any closed extents
                    self.region.reopen_all_extents().await?;

                    info!(
                        self.log,
                        "{:?} is now active (read-write)", upstairs_connection,
                    );

                    Ok(())
                }

                1 => {
                    // There is an existing session.  Determine if this new
                    // request to promote to active should move forward or
                    // be blocked.
                    let active_upstairs = self
                        .active_upstairs
                        .get(&currently_active_upstairs_uuids[0])
                        .unwrap();

                    warn!(
                        self.log,
                        "Attempting RW takeover from {:?} to {:?}",
                        active_upstairs.upstairs_connection,
                        upstairs_connection,
                    );

                    // Compare the new generaion number to what the existing
                    // connection is and take action based on that.
                    match upstairs_connection
                        .gen
                        .cmp(&active_upstairs.upstairs_connection.gen)
                    {
                        Ordering::Less => {
                            // If the new connection has a lower generation
                            // number than the current connection, we don't
                            // allow it to take over.
                            bail!(
                                "Current gen {} is > requested gen of {}",
                                active_upstairs.upstairs_connection.gen,
                                upstairs_connection.gen,
                            );
                        }
                        Ordering::Equal => {
                            // The generation numbers match, the only way we
                            // allow this new connection to take over is if the
                            // upstairs_id and the session_id are the same,
                            // which means the whole structures need to be
                            // identical.
                            if active_upstairs.upstairs_connection
                                != upstairs_connection
                            {
                                bail!(
                                    "Same gen, but UUIDs {:?} don't match {:?}",
                                    active_upstairs.upstairs_connection,
                                    upstairs_connection,
                                );
                            }
                        }
                        // The only remaining case is the new generation
                        // number is higher than the existing.
                        Ordering::Greater => {}
                    }

                    // Now that we know we can remove/replace it, go ahead
                    // and take it off the list.
                    let active_upstairs = self
                        .active_upstairs
                        .remove(&currently_active_upstairs_uuids[0])
                        .unwrap();

                    let mut work = active_upstairs.work.lock().await;

                    warn!(
                        self.log,
                        "Signaling to {:?} thread that {:?} is being \
                        promoted (read-write)",
                        active_upstairs.upstairs_connection,
                        upstairs_connection,
                    );

                    match active_upstairs
                        .terminate_sender
                        .send(upstairs_connection)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            /*
                             * It's possible the old thread died due to some
                             * connection error. In that case the
                             * receiver will have closed and
                             * the above send will fail.
                             */
                            error!(
                                self.log,
                                "Error while signaling to {:?} thread: {:?}",
                                active_upstairs.upstairs_connection,
                                e,
                            );
                        }
                    }

                    // Note: in the future, differentiate between new upstairs
                    // connecting vs same upstairs reconnecting here.
                    //
                    // Clear out active jobs, the last flush, and completed
                    // information, as that will not be valid any longer.
                    //
                    // TODO: Really work through this error case
                    if work.active.keys().len() > 0 {
                        warn!(
                            self.log,
                            "Crucible Downstairs promoting {:?} to active, \
                            discarding {} jobs",
                            upstairs_connection,
                            work.active.keys().len()
                        );
                    }

                    // In the future, we may decide there is some way to
                    // continue working on outstanding jobs, or a way to merge.
                    // But for now, we just throw out what we have and let the
                    // upstairs resend anything to us that it did not get an ACK
                    // for.
                    work.clear();

                    // Insert or replace the session

                    self.active_upstairs.insert(
                        upstairs_connection.upstairs_id,
                        ActiveUpstairs {
                            upstairs_connection,
                            work: Mutex::new(Work::new()),
                            terminate_sender: tx,
                        },
                    );

                    assert_eq!(self.active_upstairs.len(), 1);

                    // Re-open any closed extents
                    self.region.reopen_all_extents().await?;

                    info!(
                        self.log,
                        "{:?} is now active (read-write)", upstairs_connection,
                    );

                    Ok(())
                }

                _ => {
                    // Panic - we shouldn't be running with more than one
                    // active read-write Upstairs
                    panic!(
                        "More than one currently active upstairs! {:?}",
                        currently_active_upstairs_uuids,
                    );
                }
            }
        }
    }

    fn is_active(&self, connection: UpstairsConnection) -> bool {
        let uuid = connection.upstairs_id;
        if let Some(active_upstairs) = self.active_upstairs.get(&uuid) {
            active_upstairs.upstairs_connection == connection
        } else {
            false
        }
    }

    fn active_upstairs(&self) -> Vec<UpstairsConnection> {
        self.active_upstairs
            .values()
            .map(|x| x.upstairs_connection)
            .collect()
    }

    async fn clear_active(
        &mut self,
        upstairs_connection: UpstairsConnection,
    ) -> Result<()> {
        let mut work = self.work_lock(upstairs_connection).await?;
        work.clear();
        drop(work);

        self.active_upstairs
            .remove(&upstairs_connection.upstairs_id);

        Ok(())
    }
}

/*
 * The structure that tracks downstairs work in progress
 */
#[derive(Debug)]
pub struct Work {
    active: HashMap<JobId, DownstairsWork>,
    outstanding_deps: HashMap<JobId, usize>,

    /*
     * We have to keep track of all IOs that have been issued since
     * our last flush, as that is how we make sure dependencies are
     * respected. The last_flush is the downstairs job ID number (ds_id
     * typically) for the most recent flush.
     */
    last_flush: JobId,
    completed: Vec<JobId>,
}

#[derive(Debug, Clone)]
struct DownstairsWork {
    upstairs_connection: UpstairsConnection,
    ds_id: JobId,
    work: IOop,
    state: WorkState,
}

impl Work {
    fn new() -> Self {
        Work {
            active: HashMap::new(),
            outstanding_deps: HashMap::new(),
            last_flush: JobId(0), // TODO(matt) make this an Option?
            completed: Vec::with_capacity(32),
        }
    }

    fn clear(&mut self) {
        self.active = HashMap::new();
        self.outstanding_deps = HashMap::new();
        self.last_flush = JobId(0);
        self.completed = Vec::with_capacity(32);
    }

    fn jobs(&self) -> usize {
        self.active.len()
    }

    /**
     * Return a list of downstairs request IDs that are new or have
     * been waiting for other dependencies to finish.
     */
    fn new_work(&self, upstairs_connection: UpstairsConnection) -> Vec<JobId> {
        let mut result = Vec::with_capacity(self.active.len());

        for job in self.active.values() {
            if job.upstairs_connection != upstairs_connection {
                panic!("Old Upstairs Job in new_work!");
            }

            if job.state == WorkState::New || job.state == WorkState::DepWait {
                result.push(job.ds_id);
            }
        }

        result.sort_unstable();

        result
    }

    fn add_work(&mut self, ds_id: JobId, dsw: DownstairsWork) {
        self.active.insert(ds_id, dsw);
    }

    #[cfg(test)]
    fn get_job(&mut self, ds_id: JobId) -> DownstairsWork {
        self.active.get(&ds_id).unwrap().clone()
    }

    /**
     * If the requested job is still new, and the dependencies are all met,
     * return the job ID and the upstairs UUID, moving the state of the job as
     * InProgress. If the dependencies are not met, move the state to DepWait.
     *
     * If this job is not new, then just return none. This can be okay as we
     * build our work list with the new_work fn above, but we drop and re-aquire
     * the Work mutex and things can change.
     *
     * If the job is InProgress, return itself.
     */
    fn in_progress(
        &mut self,
        ds_id: JobId,
        log: Logger,
    ) -> Option<(JobId, UpstairsConnection)> {
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
                let dep_list = job.work.deps();

                /*
                 * See which of our dependencies are met.
                 * XXX Make this better/faster by removing the ones that
                 * are met, so next lap we don't have to check again?  There
                 * may be some debug value to knowing what the dep list was,
                 * so consider that before making this faster.
                 */
                let mut deps_outstanding: Vec<JobId> =
                    Vec::with_capacity(dep_list.len());

                for dep in dep_list.iter() {
                    // The Downstairs currently assumes that all jobs previous
                    // to the last flush have completed, hence this early out.
                    //
                    // Currently `work.completed` is cleared out when
                    // `Downstairs::complete_work` (or `complete` in mod test)
                    // is called with a FlushAck so this early out cannot be
                    // removed unless that is changed too.
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
                        warn!(
                            log,
                            "{} job {} for connection {:?} waiting on {} deps",
                            ds_id,
                            match &job.work {
                                IOop::Write {
                                    dependencies: _,
                                    writes: _,
                                } => "Write",
                                IOop::WriteUnwritten {
                                    dependencies: _,
                                    writes: _,
                                } => "WriteUnwritten",
                                IOop::Flush {
                                    dependencies: _,
                                    flush_number: _flush_number,
                                    gen_number: _gen_number,
                                    snapshot_details: _,
                                    extent_limit: _,
                                } => "Flush",
                                IOop::Read {
                                    dependencies: _,
                                    requests: _,
                                } => "Read",
                                IOop::ExtentClose {
                                    dependencies: _,
                                    extent: _,
                                } => "ECLose",
                                IOop::ExtentFlushClose {
                                    dependencies: _,
                                    extent: _,
                                    flush_number: _flush_number,
                                    gen_number: _gen_number,
                                    source_downstairs: _,
                                    repair_downstairs: _,
                                } => "EFlushCLose",
                                IOop::ExtentLiveRepair {
                                    dependencies: _,
                                    extent: _,
                                    source_downstairs: _,
                                    source_repair_address: _,
                                    repair_downstairs: _,
                                } => "ELiveRepair",
                                IOop::ExtentLiveReopen {
                                    dependencies: _,
                                    extent: _,
                                } => "ELiveReopen",
                                IOop::ExtentLiveNoOp { dependencies: _ } =>
                                    "NoOp",
                            },
                            job.upstairs_connection,
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

                Some((job.ds_id, job.upstairs_connection))
            } else if job.state == WorkState::InProgress {
                // A previous call of this function put this job in progress, so
                // return idempotently.
                Some((job.ds_id, job.upstairs_connection))
            } else {
                /*
                 * job id is not new, we can't run it.
                 */
                None
            }
        } else {
            /*
             * XXX If another upstairs took over, a job ID could be
             * invalid.  Check here to verify that this set of
             * downstairs tasks is no longer active.
             */
            warn!(log, "This ID is no longer a valid job id");
            None
        }
    }

    // Return a job that's ready to have the work done
    fn get_ready_job(&mut self, job_id: JobId) -> Option<DownstairsWork> {
        match self.active.get(&job_id) {
            Some(job) => {
                assert_eq!(job.state, WorkState::InProgress);
                assert_eq!(job_id, job.ds_id);

                // validate that deps are done
                let dep_list = job.work.deps();
                for dep in dep_list {
                    let last_flush_satisfied = dep <= &self.last_flush;
                    let complete_satisfied = self.completed.contains(dep);

                    assert!(last_flush_satisfied || complete_satisfied);
                }

                Some(job.clone())
            }

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
                None
            }
        }
    }
}

/*
 * XXX We may not need Done. At the moment all we actually look at is New or
 * InProgress.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum WorkState {
    New,
    DepWait,
    InProgress,
    Done,
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
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WrappedStream {
    Http(tokio::net::TcpStream),
    Https(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

pub async fn create_region(
    block_size: u64,
    data: PathBuf,
    extent_size: u64,
    extent_count: u64,
    uuid: Uuid,
    encrypted: bool,
    log: Logger,
) -> Result<Region> {
    /*
     * Create the region options, then the region.
     */
    let mut region_options: crucible_common::RegionOptions = Default::default();
    region_options.set_block_size(block_size);
    region_options
        .set_extent_size(Block::new(extent_size, block_size.trailing_zeros()));
    region_options.set_uuid(uuid);
    region_options.set_encrypted(encrypted);

    let mut region = Region::create(data, region_options, log).await?;
    region.extend(extent_count as u32).await?;

    Ok(region)
}

// Build the downstairs struct given a region directory and some additional
// needed information.  If a logger is passed in, we will use that, otherwise
// a logger will be created.
pub async fn build_downstairs_for_region(
    data: &Path,
    lossy: bool,
    read_errors: bool,
    write_errors: bool,
    flush_errors: bool,
    read_only: bool,
    log_request: Option<Logger>,
) -> Result<Arc<Mutex<Downstairs>>> {
    let log = match log_request {
        Some(log) => log,
        None => build_logger(),
    };
    let region =
        Region::open(data, Default::default(), true, read_only, &log).await?;

    info!(log, "UUID: {:?}", region.def().uuid());
    info!(
        log,
        "Blocks per extent:{} Total Extents: {}",
        region.def().extent_size().value,
        region.def().extent_count(),
    );

    let encrypted = region.encrypted();

    Ok(Arc::new(Mutex::new(Downstairs::new(
        region,
        lossy,
        read_errors,
        write_errors,
        flush_errors,
        read_only,
        encrypted,
        log,
    ))))
}

/// Returns Ok if everything spawned ok, Err otherwise
///
/// Return Ok(main task join handle) if all the necessary tasks spawned
/// successfully, and Err otherwise.
#[allow(clippy::too_many_arguments)]
pub async fn start_downstairs(
    d: Arc<Mutex<Downstairs>>,
    address: IpAddr,
    oximeter: Option<SocketAddr>,
    port: u16,
    rport: u16,
    cert_pem: Option<String>,
    key_pem: Option<String>,
    root_cert_pem: Option<String>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    if let Some(oximeter) = oximeter {
        let dssw = d.lock().await;
        let dss = dssw.dss.clone();
        let log = dssw.log.new(o!("task" => "oximeter".to_string()));

        tokio::spawn(async move {
            let new_address = match address {
                IpAddr::V4(ipv4) => {
                    SocketAddr::new(std::net::IpAddr::V4(ipv4), 0)
                }
                IpAddr::V6(ipv6) => {
                    SocketAddr::new(std::net::IpAddr::V6(ipv6), 0)
                }
            };

            if let Err(e) =
                stats::ox_stats(dss, oximeter, new_address, &log).await
            {
                error!(log, "ERROR: oximeter failed: {:?}", e);
            } else {
                warn!(log, "OK: oximeter all done");
            }
        });
    }

    // Setup a log for this task
    let log = d.lock().await.log.new(o!("task" => "main".to_string()));

    let listen_on = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), port),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), port),
    };

    // Establish a listen server on the port.
    let listener = TcpListener::bind(&listen_on).await?;
    let local_addr = listener.local_addr()?;
    {
        let mut ds = d.lock().await;
        ds.address = Some(local_addr);
    }
    let info = crucible_common::BuildInfo::default();
    info!(log, "Crucible Version: {}", info);
    info!(
        log,
        "Upstairs <-> Downstairs Message Version: {}", CRUCIBLE_MESSAGE_VERSION
    );

    let repair_address = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), rport),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), rport),
    };

    let dss = d.clone();
    let repair_log = d.lock().await.log.new(o!("task" => "repair".to_string()));

    let repair_listener =
        match repair::repair_main(&dss, repair_address, &repair_log).await {
            Err(e) => {
                // TODO tear down other things if repair server can't be
                // started?
                bail!("got {:?} from repair main", e);
            }

            Ok(socket_addr) => socket_addr,
        };

    {
        let mut ds = d.lock().await;
        ds.repair_address = Some(repair_listener);
    }
    info!(log, "Using repair address: {:?}", repair_listener);

    // Optionally require SSL connections
    let ssl_acceptor = if let Some(cert_pem_path) = cert_pem {
        let key_pem_path = key_pem.unwrap();
        let root_cert_pem_path = root_cert_pem.unwrap();

        let context = crucible_common::x509::TLSContext::from_paths(
            &cert_pem_path,
            &key_pem_path,
            &root_cert_pem_path,
        )?;

        let config = context.get_server_config()?;

        info!(log, "Configured SSL acceptor");

        Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    } else {
        // unencrypted
        info!(log, "No SSL acceptor configured");
        None
    };

    let join_handle = tokio::spawn(async move {
        /*
         * We now loop listening for a connection from the Upstairs.
         * When we get one, we then spawn the proc() function to handle
         * it and wait for another connection. Downstairs can handle
         * multiple Upstairs connecting but only one active one.
         */
        info!(log, "listening on {}", listen_on);
        loop {
            let (sock, raddr) = listener.accept().await?;

            /*
             * We have a new connection; before we wrap it, set TCP_NODELAY
             * to assure that we don't get Nagle'd.
             */
            sock.set_nodelay(true).expect("could not set TCP_NODELAY");

            let stream: WrappedStream = if let Some(ssl_acceptor) =
                &ssl_acceptor
            {
                let ssl_acceptor = ssl_acceptor.clone();
                WrappedStream::Https(match ssl_acceptor.accept(sock).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            log,
                            "rejecting connection from {:?}: {:?}", raddr, e,
                        );
                        continue;
                    }
                })
            } else {
                WrappedStream::Http(sock)
            };

            info!(log, "accepted connection from {:?}", raddr);
            {
                /*
                 * Add one to the counter every time we have a connection
                 * from an upstairs
                 */
                let mut ds = d.lock().await;
                ds.dss.add_connection().await;
            }

            let dd = d.clone();

            tokio::spawn(async move {
                if let Err(e) = proc_stream(&dd, stream).await {
                    error!(
                        dd.lock().await.log,
                        "connection ({}) Exits with error: {:?}", raddr, e
                    );
                } else {
                    info!(
                        dd.lock().await.log,
                        "connection ({}): all done", raddr
                    );
                }
            });
        }
    });

    Ok(join_handle)
}

#[cfg(test)]
mod test {
    use super::*;
    use rand_chacha::ChaCha20Rng;
    use std::net::Ipv4Addr;
    use tempfile::{tempdir, TempDir};
    use tokio::net::TcpSocket;
    use tokio::sync::mpsc::error::TryRecvError;

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    fn add_work(
        work: &mut Work,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        deps: Vec<JobId>,
        is_flush: bool,
    ) {
        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: if is_flush {
                    IOop::Flush {
                        dependencies: deps,
                        flush_number: 10,
                        gen_number: 0,
                        snapshot_details: None,
                        extent_limit: None,
                    }
                } else {
                    IOop::Read {
                        dependencies: deps,
                        requests: vec![ReadRequest {
                            eid: 1,
                            offset: Block::new_512(1),
                        }],
                    }
                },
                state: WorkState::New,
            },
        );
    }

    fn add_work_rf(
        work: &mut Work,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        deps: Vec<JobId>,
    ) {
        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: IOop::WriteUnwritten {
                    dependencies: deps,
                    writes: Vec::with_capacity(1),
                },
                state: WorkState::New,
            },
        );
    }

    fn complete(work: &mut Work, ds_id: JobId) {
        let is_flush = {
            let job = work.active.get(&ds_id).unwrap();

            // validate that deps are done
            let dep_list = job.work.deps();
            for dep in dep_list {
                let last_flush_satisfied = dep <= &work.last_flush;
                let complete_satisfied = work.completed.contains(dep);

                assert!(last_flush_satisfied || complete_satisfied);
            }

            matches!(
                job.work,
                IOop::Flush {
                    dependencies: _,
                    flush_number: _,
                    gen_number: _,
                    snapshot_details: _,
                    extent_limit: _,
                }
            )
        };

        let _ = work.active.remove(&ds_id);

        if is_flush {
            work.last_flush = ds_id;
            work.completed = Vec::with_capacity(32);
        } else {
            work.completed.push(ds_id);
        }
    }

    fn test_push_next_jobs(
        work: &mut Work,
        upstairs_connection: UpstairsConnection,
    ) -> Vec<JobId> {
        let mut jobs = vec![];
        let mut new_work = work.new_work(upstairs_connection);

        new_work.sort_unstable();

        for new_id in new_work.iter() {
            let job = work.in_progress(*new_id, csl());
            match job {
                Some(job) => {
                    jobs.push(job.0);
                }
                None => {
                    continue;
                }
            }
        }

        for job in &jobs {
            assert_eq!(
                work.active.get(job).unwrap().state,
                WorkState::InProgress
            );
        }

        jobs
    }

    fn test_do_work(work: &mut Work, jobs: Vec<JobId>) {
        for job_id in jobs {
            complete(work, job_id);
        }
    }

    #[test]
    fn you_had_one_job() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);

        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work, upstairs_connection).is_empty());
    }

    #[tokio::test]
    async fn test_simple_read() -> Result<()> {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let ads = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // For the other_active_upstairs, unused.
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        let deps = vec![JobId(1000)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: vec![ReadRequest {
                eid: 1,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        show_work(&mut ds).await;

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        for id in new_work.iter() {
            let ip_id =
                ds.in_progress(upstairs_connection, *id).await?.unwrap();
            assert_eq!(ip_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(upstairs_connection, *id).await?.unwrap();
            println!("Got m: {:?}", m);
            ds.complete_work(upstairs_connection, *id, m).await?;
        }
        show_work(&mut ds).await;
        Ok(())
    }

    // Test function to create a simple downstairs with the given block
    // and extent values.  Returns the Downstairs.
    async fn create_test_downstairs(
        block_size: u64,
        extent_size: u64,
        extent_count: u32,
        dir: &TempDir,
    ) -> Result<Arc<Mutex<Downstairs>>> {
        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        mkdir_for_file(dir.path())?;
        let mut region = Region::create(dir, region_options, csl()).await?;
        region.extend(extent_count).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let ads = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        Ok(ads)
    }

    #[tokio::test]
    async fn test_extent_simple_close_flush_close() -> Result<()> {
        // Test creating these IOops:
        // IOop::ExtentClose
        // IOop::ExtentFlushClose
        // IOop::Read
        // IOop::ExtentLiveNoOp
        // IOop::ExtentLiveReopen
        // Put them on the work queue and verify they flow through the
        // queue as expected, No actual work results are checked.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // For the other_active_upstairs, unused.
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: 0,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: 1,
            flush_number: 1,
            gen_number: 2,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        let deps = vec![JobId(1000), JobId(1001)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: vec![ReadRequest {
                eid: 2,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection, JobId(1002), rio).await?;

        let deps = vec![JobId(1000), JobId(1001), JobId(1002)];
        let rio = IOop::ExtentLiveNoOp { dependencies: deps };
        ds.add_work(upstairs_connection, JobId(1003), rio).await?;

        let deps = vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)];
        let rio = IOop::ExtentLiveReopen {
            dependencies: deps,
            extent: 0,
        };
        ds.add_work(upstairs_connection, JobId(1004), rio).await?;

        println!("Before doing work we have:");
        show_work(&mut ds).await;

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 5);

        for id in new_work.iter() {
            let ip_id =
                ds.in_progress(upstairs_connection, *id).await?.unwrap();
            assert_eq!(ip_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(upstairs_connection, *id).await?.unwrap();
            println!("Got m: {:?}", m);
            ds.complete_work(upstairs_connection, *id, m).await?;
        }

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_new_close_flush_close() -> Result<()> {
        // Test sending IOops for ExtentClose and ExtentFlushClose when we
        // have an unwritten block.  We are verifying here that only the
        // dirty bit is set, as the other values (gen/flush) should remain
        // as if the extent is unwritten.
        // After closing, reopen both extents and verify no issues.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        // For the other_active_upstairs, unused.
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: 0,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: 1,
            flush_number: 1,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        // Add the two reopen commands for the two extents we closed.
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1000)],
            extent: 0,
        };
        ds.add_work(upstairs_connection, JobId(1002), rio).await?;
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1001)],
            extent: 1,
        };
        ds.add_work(upstairs_connection, JobId(1003), rio).await?;
        show_work(&mut ds).await;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the ExtentClose
        ds.in_progress(upstairs_connection, JobId(1000))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1000)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1000), m)
            .await?;

        // Process the ExtentFlushClose
        ds.in_progress(upstairs_connection, JobId(1001))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1001)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1001), m)
            .await?;

        // Process the two ExtentReopen commands
        for id in (1002..=1003).map(JobId) {
            ds.in_progress(upstairs_connection, id).await?.unwrap();
            let m = ds.do_work(upstairs_connection, id).await?.unwrap();
            match m {
                Message::ExtentLiveAckId {
                    upstairs_id,
                    session_id,
                    job_id,
                    ref result,
                } => {
                    assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                    assert_eq!(session_id, upstairs_connection.session_id);
                    assert_eq!(job_id, id);
                    assert!(result.is_ok());
                }
                _ => {
                    panic!("Incorrect message: {:?} for id: {}", m, id);
                }
            }
            ds.complete_work(upstairs_connection, id, m).await?;
        }

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    // A test function that will return a generic crucible write command
    // for use when building the IOop::Write structure.  The data (of 9's)
    // matches the hash.
    fn create_generic_test_write(eid: u64) -> Vec<crucible_protocol::Write> {
        let data = BytesMut::from(&[9u8; 512][..]);
        let offset = Block::new_512(1);

        vec![crucible_protocol::Write {
            eid,
            offset,
            data: data.freeze(),
            block_context: BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            },
        }]
    }

    #[tokio::test]
    async fn test_extent_write_flush_write_close() -> Result<()> {
        // Send IOops for write, flush, write, then ExtentClose
        // We are testing here that the flush will make the metadata for
        // the first write (the gen/flush) persistent.  The 2nd write won't
        // change what is returned by the ExtentClose, as that data
        // was specifically not flushed, but the dirty bit should be set.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let eid = 3;

        // Create and add the first write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        // add work for flush 1001
        let rio = IOop::Flush {
            dependencies: vec![],
            flush_number: 3,
            gen_number: gen,
            snapshot_details: None,
            extent_limit: None,
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        // Add work for 2nd write 1002
        let writes = create_generic_test_write(eid);

        let rio = IOop::Write {
            dependencies: vec![JobId(1000), JobId(1001)],
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1002), rio).await?;

        // Now close the extent
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000), JobId(1001), JobId(1002)],
            extent: eid as usize,
        };
        ds.add_work(upstairs_connection, JobId(1003), rio).await?;

        show_work(&mut ds).await;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the first Write
        ds.in_progress(upstairs_connection, JobId(1000))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1000)).await?.unwrap();
        ds.complete_work(upstairs_connection, JobId(1000), m)
            .await?;

        // Process the flush
        ds.in_progress(upstairs_connection, JobId(1001))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1001)).await?.unwrap();
        ds.complete_work(upstairs_connection, JobId(1001), m)
            .await?;

        // Process write 2
        ds.in_progress(upstairs_connection, JobId(1002))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1002)).await?.unwrap();
        ds.complete_work(upstairs_connection, JobId(1002), m)
            .await?;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        ds.in_progress(upstairs_connection, JobId(1003))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1003)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 10); // From the flush
                assert_eq!(*f, 3); // From the flush
                assert!(*d); // Dirty should be true.
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1003), m)
            .await?;

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_close() -> Result<()> {
        // Test sending IOops for Write then ExtentClose.
        // Because we are not sending a flush here, we expect only the
        // dirty bit to be set when we look at the close results.
        // For the write, we do verify that the return contents in the
        // message are as we expect.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let eid = 0;

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000)],
            extent: eid as usize,
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        show_work(&mut ds).await;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        ds.in_progress(upstairs_connection, JobId(1000))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1000)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1000), m)
            .await?;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        ds.in_progress(upstairs_connection, JobId(1001))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1001)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1001), m)
            .await?;

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_flush_close() -> Result<()> {
        // Test sending IOops for Write then ExtentFlushClose
        // Verify we get the expected results.  Because we will be
        // writing to the extents, we expect to get different results
        // than in the non-writing case.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        // For the other_active_upstairs, unused.
        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let eid = 1;

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid as usize,
            flush_number: 3,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        show_work(&mut ds).await;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        ds.in_progress(upstairs_connection, JobId(1000))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1000)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1000), m)
            .await?;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentFlushClose
        ds.in_progress(upstairs_connection, JobId(1001))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1001)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 3);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1001), m)
            .await?;

        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_write_flush_close() -> Result<()> {
        // Test sending IOops for two different Writes then an
        // ExtentFlushClose on one extent, and then a ExtentClose
        // on the other extent.
        // Verify that the first extent close returns data including
        // a flush, and the second extent remains dirty (no flush pollution
        // from the flush on the first extent).
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let ads =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);
        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx.clone())
            .await?;

        let eid_one = 1;
        let eid_two = 2;

        // Create the write for extent 1
        let writes = create_generic_test_write(eid_one);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1000), rio).await?;

        // Create the write for extent 2
        let writes = create_generic_test_write(eid_two);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.add_work(upstairs_connection, JobId(1001), rio).await?;

        // Flush and close extent 1
        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid_one as usize,
            flush_number: 6,
            gen_number: gen,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        ds.add_work(upstairs_connection, JobId(1002), rio).await?;

        // Just close extent 2
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1001)],
            extent: eid_two as usize,
        };
        ds.add_work(upstairs_connection, JobId(1003), rio).await?;

        show_work(&mut ds).await;

        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the Writes
        for id in (1000..=1001).map(JobId) {
            ds.in_progress(upstairs_connection, id).await?.unwrap();
            let m = ds.do_work(upstairs_connection, id).await?.unwrap();
            ds.complete_work(upstairs_connection, id, m).await?;
        }

        // Process the ExtentFlushClose
        ds.in_progress(upstairs_connection, JobId(1002))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1002)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1002));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 6);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1002), m)
            .await?;

        // Process the ExtentClose
        ds.in_progress(upstairs_connection, JobId(1003))
            .await?
            .unwrap();
        let m = ds.do_work(upstairs_connection, JobId(1003)).await?.unwrap();
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(upstairs_connection, JobId(1003), m)
            .await?;
        // Nothing should be left on the queue.
        let new_work = ds.new_work(upstairs_connection).await.unwrap();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[test]
    fn jobs_write_unwritten() {
        // Verify WriteUnwritten jobs move through the work queue
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        add_work_rf(&mut work, upstairs_connection, JobId(1000), vec![]);

        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work, upstairs_connection).is_empty());
    }

    fn test_misc_work_through_work_queue(ds_id: JobId, ioop: IOop) {
        // Verify that a IOop work request will move through the work queue.
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: ioop,
                state: WorkState::New,
            },
        );

        assert_eq!(work.new_work(upstairs_connection), vec![ds_id]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![ds_id]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![ds_id]);

        assert!(test_push_next_jobs(&mut work, upstairs_connection).is_empty());
    }

    #[test]
    fn jobs_extent_close() {
        // Verify ExtentClose jobs move through the work queue
        let eid = 1;
        let ioop = IOop::ExtentClose {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_flush_close() {
        // Verify ExtentFlushClose jobs move through the work queue

        let eid = 1;
        let ioop = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: eid,
            flush_number: 1,
            gen_number: 2,
            source_downstairs: ClientId::new(0),
            repair_downstairs: vec![ClientId::new(1)],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_repair() {
        // Verify ExtentLiveRepair jobs move through the work queue

        let eid = 1;
        let source_repair_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let ioop = IOop::ExtentLiveRepair {
            dependencies: vec![],
            extent: eid,
            source_downstairs: ClientId::new(0),
            source_repair_address,
            repair_downstairs: vec![ClientId::new(1)],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_reopen() {
        // Verify ExtentLiveReopen jobs move through the work queue
        let eid = 1;

        let ioop = IOop::ExtentLiveReopen {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_noop() {
        // Verify ExtentLiveNoOp jobs move through the work queue

        let ioop = IOop::ExtentLiveNoOp {
            dependencies: vec![],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_independent() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add two independent jobs
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(&mut work, upstairs_connection, JobId(1001), vec![], false);

        // new_work returns all new jobs
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1000), JobId(1001)]
        );

        // should push both, they're independent
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000), JobId(1001)]);

        // new work returns only jobs in new or dep wait
        assert!(work.new_work(upstairs_connection).is_empty());

        // do work
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);

        assert!(test_push_next_jobs(&mut work, upstairs_connection).is_empty());
    }

    #[test]
    fn unblock_job() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add two jobs, one blocked on another
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1000), JobId(1001)]
        );

        // only one is ready to run
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        // new_work returns all new or dep wait jobs
        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1001)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
    }

    #[test]
    fn unblock_job_chain() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1001), JobId(1002)]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work(upstairs_connection).is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_first_is_flush() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain, first is flush
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], true);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1001), JobId(1002)]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work(upstairs_connection).is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_second_is_flush() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain, second is flush
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            true,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(
            work.new_work(upstairs_connection),
            vec![JobId(1001), JobId(1002)]
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work(upstairs_connection).is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert_eq!(work.completed, vec![JobId(1002)]);
    }

    #[test]
    fn unblock_job_upstairs_sends_big_deps() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            true,
        );

        // Downstairs is really fast!
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        // Upstairs sends a job with these three in deps, not knowing Downstairs
        // has done the jobs already
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1004),
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1004)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert_eq!(work.completed, vec![JobId(1003), JobId(1004)]);
    }

    #[test]
    fn job_dep_not_satisfied() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            true,
        );

        // Add one that can't run yet
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(2000)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        assert_eq!(work.new_work(upstairs_connection), vec![JobId(1003)]);
        assert_eq!(
            work.active.get(&JobId(1003)).unwrap().state,
            WorkState::DepWait
        );
    }

    #[test]
    fn two_job_chains() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // Add another set of jobs blocked on each other
        add_work(&mut work, upstairs_connection, JobId(2000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(2001),
            vec![JobId(2000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(2002),
            vec![JobId(2000), JobId(2001)],
            true,
        );

        // should do each chain in sequence
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000), JobId(2000)]);
        test_do_work(&mut work, next_jobs);
        assert_eq!(work.completed, vec![JobId(1000), JobId(2000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001), JobId(2001)]);
        test_do_work(&mut work, next_jobs);
        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(2000), JobId(1001), JobId(2001)]
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002), JobId(2002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(2002));
        assert!(work.completed.is_empty());
    }

    #[test]
    fn out_of_order_arrives_after_first_push_next_jobs() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_first_do_work() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_1001_completes() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        // can't run anything, dep not satisfied
        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert!(next_jobs.is_empty());
        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work, upstairs_connection);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[tokio::test]
    async fn import_test_basic() -> Result<()> {
        /*
         * import + export test where data matches region size
         */

        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_small() -> Result<()> {
        /*
         * import + export test where data is smaller than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 fewer bytes than region size)

        let total_bytes = region.def().total_size() - 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 fewer bytes imported than
        // region size still means the whole region is exported)

        let export_path = tempdir.path().join("exported_data");
        let region_size = region.def().total_size();
        downstairs_export(
            &mut region,
            &export_path,
            0,
            region_size / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded

        let padding_size = actual.len() - total_bytes;
        assert_eq!(padding_size, 100);

        let mut padding = vec![0; padding_size];
        padding.resize(padding_size, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_large() -> Result<()> {
        /*
         * import + export test where data is larger than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 more bytes than region size)

        let total_bytes = region.def().total_size() + 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 more bytes will have caused
        // 10 more extents to be added, but someone running the export command
        // will use the number of blocks copied by the import command)
        assert_eq!(region.def().extent_count(), 11);

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size + 1,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded
        // the export only exported the extra block, not the extra extent
        let padding_in_extra_block: usize = 512 - 100;

        let mut padding = vec![0; padding_in_extra_block];
        padding.resize(padding_in_extra_block, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_basic_read_blocks() -> Result<()> {
        /*
         * import + export test where data matches region size, and read the
         * blocks
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0u8; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0u8);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // read block by block
        let mut read_data = Vec::with_capacity(total_bytes as usize);
        for eid in 0..region.def().extent_count() {
            for offset in 0..region.def().extent_size().value {
                let responses = region
                    .region_read(
                        &[crucible_protocol::ReadRequest {
                            eid: eid.into(),
                            offset: Block::new_512(offset),
                        }],
                        JobId(0),
                    )
                    .await?;

                assert_eq!(responses.len(), 1);

                let response = &responses[0];
                assert_eq!(response.hashes().len(), 1);
                assert_eq!(
                    integrity_hash(&[&response.data[..]]),
                    response.hashes()[0],
                );

                read_data.extend_from_slice(&response.data[..]);
            }
        }

        assert_eq!(random_data, read_data);

        Ok(())
    }

    async fn build_test_downstairs(
        read_only: bool,
    ) -> Result<Arc<Mutex<Downstairs>>> {
        let block_size: u64 = 512;
        let extent_size = 4;

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();

        build_downstairs_for_region(
            &path_dir,
            false, // lossy
            false, // read errors
            false, // write errors
            false, // flush errors
            read_only,
            Some(csl()),
        )
        .await
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_write() -> Result<()> {
        let ads = build_test_downstairs(false).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx).await?;

        assert_eq!(ds.active_upstairs().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_only() -> Result<()> {
        let ads = build_test_downstairs(true).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (_tx, mut _rx) = channel(1);
        let tx = Arc::new(_tx);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection, tx).await?;

        assert_eq!(ds.active_upstairs().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with the same gen should be blocked
        let ads = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        let res = ds.promote_to_active(upstairs_connection_2, tx2).await;
        assert!(res.is_err());

        assert!(matches!(rx1.try_recv().unwrap_err(), TryRecvError::Empty));
        assert!(matches!(
            rx2.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));

        assert_eq!(ds.active_upstairs().len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_lower_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with a lower gen should be blocked.
        let ads = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        println!("ds1: {:?}", ds);
        ds.promote_to_active(upstairs_connection_1, tx1).await?;
        println!("\nds2: {:?}\n", ds);

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        let res = ds.promote_to_active(upstairs_connection_2, tx2).await;
        assert!(res.is_err());

        assert!(matches!(rx1.try_recv().unwrap_err(), TryRecvError::Empty));
        assert!(matches!(
            rx2.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));

        assert_eq!(ds.active_upstairs().len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's the same
        // Upstairs but a different session) will block the "new" connection
        // if it has the same generation number.
        let ads = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        let res = ds.promote_to_active(upstairs_connection_2, tx2).await;
        assert!(res.is_err());

        assert!(matches!(rx1.try_recv().unwrap_err(), TryRecvError::Empty));
        assert!(matches!(
            rx2.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_larger_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write where it's the same
        // Upstairs, but a different session, and with a larger generation
        // should allow the new connection to take over.
        let ads = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        ds.promote_to_active(upstairs_connection_2, tx2).await?;
        assert_eq!(rx1.try_recv().unwrap(), upstairs_connection_2);
        assert!(matches!(rx2.try_recv().unwrap_err(), TryRecvError::Empty));

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_different_uuid(
    ) -> Result<()> {
        // Activating multiple read-only with different Upstairs UUIDs should
        // work.
        let ads = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        ds.promote_to_active(upstairs_connection_2, tx2).await?;
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        assert_eq!(ds.active_upstairs().len(), 2);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_same_uuid() -> Result<()> {
        // Activating multiple read-only with the same Upstairs UUID should
        // kick out the other active one.
        let ads = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        ds.promote_to_active(upstairs_connection_2, tx2).await?;
        assert_eq!(rx1.try_recv().unwrap(), upstairs_connection_2);
        assert!(matches!(rx2.try_recv().unwrap_err(), TryRecvError::Empty));

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_read_only_no_job_id_collision() -> Result<()> {
        // Two read-only Upstairs shouldn't see each other's jobs
        let ads = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let (tx2, mut rx2) = channel(1);
        let tx2 = Arc::new(tx2);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().err().unwrap(), TryRecvError::Empty));

        ds.promote_to_active(upstairs_connection_2, tx2).await?;
        assert!(matches!(rx1.try_recv().err().unwrap(), TryRecvError::Empty));
        assert!(matches!(rx2.try_recv().unwrap_err(), TryRecvError::Empty));

        assert_eq!(ds.active_upstairs().len(), 2);

        let read_1 = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection_1, JobId(1000), read_1.clone())
            .await?;

        let read_2 = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 1,
                offset: Block::new_512(2),
            }],
        };
        ds.add_work(upstairs_connection_2, JobId(1000), read_2.clone())
            .await?;

        let work_1 = ds.new_work(upstairs_connection_1).await?;
        let work_2 = ds.new_work(upstairs_connection_2).await?;

        assert_eq!(work_1, work_2);

        let job_1 = ds.get_job(upstairs_connection_1, JobId(1000)).await?;
        let job_2 = ds.get_job(upstairs_connection_2, JobId(1000)).await?;

        assert_eq!(job_1.upstairs_connection, upstairs_connection_1);
        assert_eq!(job_1.ds_id, JobId(1000));
        assert_eq!(job_1.work, read_1);
        assert_eq!(job_1.state, WorkState::New);

        assert_eq!(job_2.upstairs_connection, upstairs_connection_2);
        assert_eq!(job_2.ds_id, JobId(1000));
        assert_eq!(job_2.work, read_2);
        assert_eq!(job_2.state, WorkState::New);

        Ok(())
    }

    // Validate that `complete_work` cannot see None if the same Upstairs ID
    // (but a different session) goes active.
    #[tokio::test]
    async fn test_complete_work_cannot_see_none_same_upstairs_id() -> Result<()>
    {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let ads = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        // This happens in proc() function.
        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // For the other_active_upstairs, unused.
        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        // Add one job, id 1000
        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection_1, JobId(1000), rio).await?;

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_connection_1).await.unwrap();
        assert_eq!(new_work.len(), 1);

        let ip_id = ds
            .in_progress(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();
        assert_eq!(ip_id, JobId(1000));
        let m = ds
            .do_work(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();

        // Before complete_work, say promote_to_active runs again for another
        // connection - same UUID, different session
        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 11,
        };

        let (_tx2, mut _rx2) = channel(1);
        let tx2 = Arc::new(_tx2);

        ds.promote_to_active(upstairs_connection_2, tx2).await?;

        assert_eq!(rx1.try_recv().unwrap(), upstairs_connection_2);

        // This should error with UpstairsInactive - upstairs_connection_1 isn't
        // active anymore and can't grab the work lock.
        let result = ds
            .complete_work(upstairs_connection_1, JobId(1000), m)
            .await;
        assert!(matches!(
            result.unwrap_err().downcast::<CrucibleError>().unwrap(),
            CrucibleError::UpstairsInactive,
        ));

        Ok(())
    }

    // Validate that `complete_work` cannot see None if a different Upstairs ID
    // goes active.
    #[tokio::test]
    async fn test_complete_work_cannot_see_none_different_upstairs_id(
    ) -> Result<()> {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let ads = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        // This happens in proc() function.
        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // For the other_active_upstairs, unused.
        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        // Add one job, id 1000
        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection_1, JobId(1000), rio).await?;

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_connection_1).await.unwrap();
        assert_eq!(new_work.len(), 1);

        let ip_id = ds
            .in_progress(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();
        assert_eq!(ip_id, JobId(1000));
        let m = ds
            .do_work(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();

        // Before complete_work, say promote_to_active runs again for another
        // connection
        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 11,
        };

        let (_tx2, mut _rx2) = channel(1);
        let tx2 = Arc::new(_tx2);

        ds.promote_to_active(upstairs_connection_2, tx2).await?;

        assert_eq!(rx1.try_recv().unwrap(), upstairs_connection_2);

        // This should error with UpstairsInactive - upstairs_connection_1 isn't
        // active anymore and can't grab the work lock.
        let result = ds
            .complete_work(upstairs_connection_1, JobId(1000), m)
            .await;
        assert!(matches!(
            result.unwrap_err().downcast::<CrucibleError>().unwrap(),
            CrucibleError::UpstairsInactive,
        ));

        Ok(())
    }

    // Validate that `complete_work` can see None if the same Upstairs
    // reconnects.  We know it's the same Upstairs because the session and
    // upstairs ids will match.
    #[tokio::test]
    async fn test_complete_work_can_see_none() -> Result<()> {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let ads = build_downstairs_for_region(
            &path_dir,
            false,
            false,
            false,
            false,
            false,
            Some(csl()),
        )
        .await?;

        // This happens in proc() function.
        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // For the other_active_upstairs, unused.
        let (tx1, mut rx1) = channel(1);
        let tx1 = Arc::new(tx1);

        let mut ds = ads.lock().await;
        ds.promote_to_active(upstairs_connection_1, tx1).await?;

        // Add one job, id 1000
        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: vec![ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            }],
        };
        ds.add_work(upstairs_connection_1, JobId(1000), rio).await?;

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.new_work(upstairs_connection_1).await.unwrap();
        assert_eq!(new_work.len(), 1);

        let ip_id = ds
            .in_progress(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();
        assert_eq!(ip_id, JobId(1000));
        let m = ds
            .do_work(upstairs_connection_1, JobId(1000))
            .await?
            .unwrap();

        // Before complete_work, the same Upstairs reconnects and goes active
        let (_tx2, mut _rx2) = channel(1);
        let tx2 = Arc::new(_tx2);

        ds.promote_to_active(upstairs_connection_1, tx2).await?;

        // In the real downstairs, there would be two tasks now that both
        // correspond to upstairs_connection_1.

        // Validate that the original set of tasks were sent the termination
        // signal.

        assert_eq!(rx1.try_recv().unwrap(), upstairs_connection_1);

        // If the original set of tasks don't end right away, they'll try to run
        // complete_work:

        let result = ds
            .complete_work(upstairs_connection_1, JobId(1000), m)
            .await;

        // `complete_work` will return Ok(()) despite not doing anything to the
        // Work struct.
        assert!(result.is_ok());

        Ok(())
    }

    /*
     * Test function that will start up a downstairs (at the provided port)
     * then create a tcp connection to that downstairs, returning the tcp
     * connection to the caller.
     */
    async fn start_ds_and_connect(
        listen_port: u16,
        repair_port: u16,
    ) -> Result<tokio::net::TcpStream> {
        /*
         * Pick some small enough values for what we need.
         */
        let bs = 512;
        let es = 4;
        let ec = 5;
        let dir = tempdir()?;

        let ads = create_test_downstairs(bs, es, ec, &dir).await?;

        let _jh = start_downstairs(
            ads,
            "127.0.0.1".parse().unwrap(),
            None,
            listen_port,
            repair_port,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let sock = TcpSocket::new_v4().unwrap();

        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            listen_port,
        );
        Ok(sock.connect(addr).await.unwrap())
    }

    #[tokio::test]
    async fn test_version_match() -> Result<()> {
        // A simple test to verify that sending the current crucible
        // message version to the downstairs will trigger a response
        // indicating the version is supported.
        let tcp = start_ds_and_connect(5555, 5556).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: Vec::new(),
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5556".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_downrev() -> Result<()> {
        // Test that a newer crucible version will result in a message
        // indicating there is a version mismatch.
        let tcp = start_ds_and_connect(5557, 5558).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION - 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION - 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_uprev_only() -> Result<()> {
        // Test sending only the +1 version to the DS, verify it rejects
        // this version as unsupported.
        let tcp = start_ds_and_connect(5579, 5560).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION + 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_compatable() -> Result<()> {
        // Test sending the +1 version to the DS, but also include the
        // current version on the supported list.  The downstairs should
        // see that and respond with the version it does support.
        let tcp = start_ds_and_connect(5561, 5562).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5562".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_list() -> Result<()> {
        // Test sending an invalid version to the DS, but also include the
        // current version on the supported list, but with several
        // choices.
        let tcp = start_ds_and_connect(5563, 5564).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 4,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION - 1,
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5564".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
}
