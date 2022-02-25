// Copyright 2022 Oxide Computer Company
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::runtime::Builder;

mod cli;
mod protocol;

use crucible::*;

/*
 * The various tests this program supports.
 */
#[derive(Debug, PartialEq, StructOpt)]
enum Workload {
    Balloon,
    Big,
    Biggest,
    Burst,
    Cli {
        /// Address to connect to
        #[structopt(long, short, default_value = "0.0.0.0:5050")]
        attach: SocketAddr,
    },
    /// Start a server and listen on the given address and port
    CliServer {
        /// Address to listen on
        #[structopt(long, short, default_value = "0.0.0.0")]
        listen: IpAddr,
        /// Port to listen on
        #[structopt(long, short, default_value = "5050")]
        port: u16,
    },
    Deactivate,
    Demo,
    Dep,
    Dirty,
    Generic,
    Nothing,
    One,
    Rand,
    Span,
    Verify,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "crucible upstairs test client")]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
pub struct Opt {
    /*
     * For tests that support it, pass this count value for the number
     * of loops the test should do.
     */
    #[structopt(short, long, global = true, default_value = "0")]
    count: u32,

    #[structopt(short, long, global = true, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddr>,

    #[structopt(subcommand)]
    workload: Workload,

    /*
     * This allows the Upstairs to run in a mode where it will not
     * always submit new work to downstairs when it first receives
     * it.  This is for testing dependencies and should not be
     * used in production.  Passing args like this to the upstairs
     * may not be the best way to test, but until we have something
     * better... XXX
     */
    #[structopt(long, global = true)]
    lossy: bool,

    /*
     * quit after all crucible work queues are empty.
     */
    #[structopt(short, global = true, long)]
    quit: bool,

    #[structopt(short, global = true, long)]
    key: Option<String>,

    #[structopt(short, global = true, long, default_value = "0")]
    gen: u64,

    /*
     * Retry for activate, as long as it takes.  If we pass this arg, the
     * test will retry the initial activate command as long as it takes.
     */
    #[structopt(long, global = true)]
    retry_activate: bool,

    /*
     * For tests that support it, load the expected write count from
     * the provided file.
     */
    #[structopt(long, global = true, parse(from_os_str), name = "INFILE")]
    verify_in: Option<PathBuf>,

    /*
     * For tests that support it, save the write count into the
     * provided file.
     */
    #[structopt(long, global = true, parse(from_os_str), name = "FILE")]
    verify_out: Option<PathBuf>,

    // TLS options
    #[structopt(long)]
    cert_pem: Option<String>,
    #[structopt(long)]
    key_pem: Option<String>,
    #[structopt(long)]
    root_cert_pem: Option<String>,

    /// Start upstairs control http server
    #[structopt(long, global = true)]
    control: Option<SocketAddr>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();

    Ok(opt)
}

fn history_file<P: AsRef<Path>>(file: P) -> PathBuf {
    let out = file.as_ref().to_path_buf();
    out
}

/*
 * All the tests need this basic info about the region.
 * Not all tests make use of the write_count yet, but perhaps someday..
 */
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegionInfo {
    block_size: u64,
    extent_size: Block,
    total_size: u64,
    total_blocks: usize,
    write_count: Vec<u32>,
    max_block_io: usize,
}

/*
 * All the tests need this basic set of information about the region.
 * We also load and verify the write count if an input file is provided.
 */
fn get_region_info(guest: &Arc<Guest>) -> Result<RegionInfo, CrucibleError> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let block_size = guest.query_block_size()?;
    let extent_size = guest.query_extent_size()?;
    let total_size = guest.query_total_size()?;
    let total_blocks = (total_size / block_size) as usize;

    /*
     * Limit the max IO size (in blocks) to be 1M or the size
     * of the volume, whichever is smaller
     */
    const MAX_IO_BYTES: usize = 1024 * 1024;
    let mut max_block_io = MAX_IO_BYTES / block_size as usize;
    if total_blocks < max_block_io as usize {
        max_block_io = total_blocks as usize;
    }

    println!(
        "Region: es:{:?}  bs:{}  ts:{}  tb:{}  max_io:{} or {}",
        extent_size.value,
        block_size,
        total_size,
        total_blocks,
        max_block_io,
        (max_block_io as u64 * block_size),
    );

    /*
     * Create an array that tracks the number of writes to each block, so
     * we can know what to expect for reads.
     */
    let write_count = vec![0_u32; total_blocks];

    Ok(RegionInfo {
        block_size,
        extent_size,
        total_size,
        total_blocks,
        write_count,
        max_block_io,
    })
}

fn update_region_info(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    vi: Option<PathBuf>,
    verify: bool,
) -> Result<()> {
    /*
     * If requested, fill the write count from a provided file.
     */
    if let Some(vi) = &vi {
        let cp = history_file(vi);
        ri.write_count = match read_json(&cp) {
            Ok(write_count) => write_count,
            Err(e) => bail!("Error {:?} reading verify config {:?}", e, cp),
        };
        println!("Load write count information from file {:?}", cp);
        if ri.write_count.len() != ri.total_blocks {
            bail!(
                "Verify file {:?} blocks:{} does not match regions:{}",
                cp,
                ri.write_count.len(),
                ri.total_blocks
            );
        }
        /*
         * Only verify the volume if requested.
         */
        if verify {
            if let Err(e) = verify_volume(guest, ri) {
                bail!("Initial volume verify failed: {:?}", e)
            }
        }
    }
    Ok(())
}

/**
 * This is an example Crucible client.
 * Here we make use of the interfaces that Crucible exposes.
 */
fn main() -> Result<()> {
    /*
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let opt = opts()?;

    if opt.workload == Workload::Verify && opt.verify_in.is_none() {
        bail!("Verify requires verify_in file");
    }

    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: opt.lossy,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
    };

    /*
     * Crucible needs a runtime as it will create several async tasks to
     * handle adding new IOs, communication with the three downstairs
     * instances, and completing IOs.
     */
    let runtime = Builder::new_multi_thread()
        .worker_threads(10)
        .thread_name("crucible-tokio")
        .enable_all()
        .build()
        .unwrap();

    /*
     * If just want the cli, then start that after our runtime.  The cli
     * does not need upstairs started, as that should happen in the
     * cli-server code.
     */
    if let Workload::Cli { attach } = opt.workload {
        runtime.block_on(cli::start_cli_client(attach))?;
        return Ok(());
    }

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let guest = Arc::new(Guest::new());

    runtime.spawn(up_main(crucible_opts, guest.clone()));
    println!("Crucible runtime is spawned");

    if let Workload::CliServer { listen, port } = opt.workload {
        runtime.block_on(cli::start_cli_server(&guest, listen, port))?;
        return Ok(());
    }

    if opt.retry_activate {
        while let Err(e) = guest.activate(opt.gen) {
            println!("Activate returns: {:#}  Retrying", e);
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        println!("Activate successful");
    } else {
        guest.activate(opt.gen)?;
    }

    println!("Wait for a show_work command to finish before sending IO");
    guest.show_work()?;
    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside
     * (Propolis).  XXX Test code here..
     * runtime.spawn(run_scope(prop_work));
     */

    /*
     * Build the region info struct that all the tests will use.
     * This includes importing and verifying from a write log, if requested.
     */
    let mut region_info = match get_region_info(&guest) {
        Ok(region_info) => region_info,
        Err(e) => bail!("failed to get region info: {:?}", e),
    };

    /*
     * Now that we have the region info from the Upstaris, apply any
     * info from the verify file, and verify it matches what we expect
     * if we are expecting anything.
     */
    update_region_info(&guest, &mut region_info, opt.verify_in, true)?;

    /*
     * Call the function for the workload option passed from the command
     * line.
     */
    match opt.workload {
        Workload::Balloon => {
            println!("Run balloon test");
            runtime.block_on(balloon_workload(&guest, &mut region_info))?;
        }
        Workload::Big => {
            println!("Run big test");
            big_workload(&guest, &mut region_info)?;
        }
        Workload::Biggest => {
            println!("Run biggest IO test");
            biggest_io_workload(&guest, &mut region_info)?;
        }
        Workload::Burst => {
            println!("Run burst test (demo in a loop)");
            runtime.block_on(burst_workload(
                &guest,
                60,
                190,
                &mut region_info,
                &opt.verify_out,
            ))?;
        }
        Workload::Deactivate => {
            /*
             * A small default of 5 is okay for a functional test, but
             * not enough for a more exhaustive test.
             */
            let count = {
                if opt.count == 0 {
                    5
                } else {
                    opt.count
                }
            };
            println!("Run deactivate test");
            runtime.block_on(deactivate_workload(
                &guest,
                count,
                &mut region_info,
                opt.gen,
            ))?;
        }
        Workload::Demo => {
            println!("Run Demo test");
            println!("Pause for 10 seconds, then start testing");
            std::thread::sleep(std::time::Duration::from_secs(10));

            /*
             * The count provided here should be greater than the flow
             * control limit if we wish to test flow control.  Also, set
             * lossy on a downstairs otherwise it will probably keep up.
             */
            runtime.block_on(demo_workload(&guest, 300, &mut region_info))?;
        }
        Workload::Dep => {
            println!("Run dep test");
            runtime.block_on(dep_workload(&guest, &mut region_info))?;
        }

        Workload::Dirty => {
            println!("Run dirty test");
            runtime.block_on(dirty_workload(&guest, &mut region_info))?;

            /*
             * Saving state here when we have not waited for a flush
             * to finish means that the state recorded here may not be
             * what ends up being in the downstairs.  All we guarantee is
             * that everything before the flush will be there, and possibly
             * things that came after the flush.
             */
            if let Some(vo) = &opt.verify_out {
                let cp = history_file(vo);
                write_json(&cp, &region_info.write_count, true)?;
                println!("Wrote out file {:?}", cp);
            }
            return Ok(());
        }

        Workload::Generic => {
            runtime.block_on(generic_workload(
                &guest,
                500,
                &mut region_info,
            ))?;
        }

        Workload::One => {
            println!("One test");
            runtime.block_on(one_workload(&guest, &mut region_info))?;
        }
        Workload::Nothing => {
            println!("Do nothing test, just start");
            /*
             * If we don't want to quit right away, then just loop
             * forever
             */
            if !opt.quit {
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
            }
        }
        Workload::Rand => {
            println!("Run random test");
            runtime.block_on(rand_workload(&guest, 1000, &mut region_info))?;
        }
        Workload::Span => {
            println!("Span test");
            span_workload(&guest, &mut region_info)?;
        }
        Workload::Verify => {
            /*
             * For verify, if -q, we quit right away.  If we don't quit, then
             * this turns into a read verify loop, sleep for some duration
             * and then re-check the volume.
             */
            if opt.quit {
                println!("Verify test completed at import");
            } else {
                println!("Verify read loop begins");
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    if let Err(e) = verify_volume(&guest, &mut region_info) {
                        bail!("Volume verify failed: {:?}", e)
                    }
                    let mut wc = guest.show_work()?;
                    while wc.up_count + wc.ds_count > 0 {
                        println!("Waiting for all work to be completed");
                        std::thread::sleep(std::time::Duration::from_secs(10));
                        wc = guest.show_work()?;
                    }
                }
            }
        }
        c => {
            panic!("Unsupported cmd {:?}", c);
        }
    }

    if let Some(vo) = &opt.verify_out {
        let cp = history_file(vo);
        write_json(&cp, &region_info.write_count, true)?;
        println!("Wrote out file {:?}", cp);
    }

    println!("CLIENT: Tests done.  All submitted work has been ACK'd");
    loop {
        let wc = guest.show_work()?;
        println!("CLIENT: Up:{} ds:{}", wc.up_count, wc.ds_count);
        if opt.quit && wc.up_count + wc.ds_count == 0 {
            println!("CLIENT: All crucible jobs finished, exiting program");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_secs(4));
    }
}

/*
 * Read/Verify every possible block, one block at a time.
 */
fn verify_volume(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    assert_eq!(ri.write_count.len(), ri.total_blocks);

    println!("Read and Verify all blocks (0..{})", ri.total_blocks);
    for block_index in 0..ri.total_blocks {
        let vec: Vec<u8> = vec![255; ri.block_size as usize];
        let data = crucible::Buffer::from_vec(vec);
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());
        let mut waiter = guest.read(offset, data.clone())?;
        waiter.block_wait()?;

        let dl = data.as_vec().to_vec();
        if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
            bail!("Error at {}", block_index);
        }
    }
    Ok(())
}
/*
 * Given the write count vec and the index, pick the seed value we use when
 * writing or expect when reading block.
 * TODO: This can be improved on.  The mod at 250 was me leaving some room
 * for some special values, though I may or may not use them.
 */
fn get_seed(index: usize, wc: &[u32]) -> u8 {
    (wc[index] % 250) as u8
}

/*
 * Fill a vec based on the write count at our index.
 * This is fine for an initial fill/verify framework of sorts, but there
 * are many kinds of errors this will not find.  There are also many high
 * performance better coverage kinds of data integrity tests, and the intent
 * here is to balance urgency with rigor in that we can make use of external
 * tests for the more complicated cases, and catch the easy ones here.
 *
 * block_index: What block we started reading from.
 * blocks:      The length of the read in blocks.
 * wc:          The write count vec, indexed by block number.
 * bs:          Crucible's block size.
 */
fn fill_vec(block_index: usize, blocks: usize, wc: &[u32], bs: u64) -> Vec<u8> {
    assert_ne!(blocks, 0);
    assert_ne!(bs, 0);
    /*
     * Each block we are filling the buffer for can have a different
     * seed value.  For multiple block sized writes, we need to create
     * the write buffer with the correct seed value.
     */
    let mut vec: Vec<u8> = Vec::with_capacity(blocks * bs as usize);
    for block_offset in block_index..(block_index + blocks) {
        /*
         * The start of each block contains that blocks index mod 255
         */
        vec.push((block_offset % 255) as u8);
        /*
         * Fill the rest of the buffer with the new write count
         */
        let seed = get_seed(block_offset, wc);
        for _ in 1..bs {
            vec.push(seed);
        }
    }
    vec
}

/*
 * Compare a vec buffer with what we expect to be written for that offset.
 * This assumes you used the fill_vec function (with get_seed) to write
 * the buffer originally.
 *
 * data:        The filled in buffer to be verified.
 * block_index: What block we started reading from.
 * wc:          The write count vec, indexed by block number.
 * bs:          Crucible's block size.
 */
fn validate_vec(
    data: Vec<u8>,
    block_index: usize,
    wc: &[u32],
    bs: u64,
) -> bool {
    let bs = bs as usize;
    assert_eq!(data.len() % bs, 0);
    assert_ne!(data.len(), 0);

    let blocks = data.len() / bs;
    let mut data_offset: usize = 0;
    let mut res = true;
    /*
     * The outer loop walks the buffer by blocks, as each block will have
     * its own unique write count.
     */
    for block_offset in block_index..(block_index + blocks) {
        /*
         * Skip blocks we don't know the expected value of
         */
        if wc[block_offset] == 0 {
            data_offset += bs;
            continue;
        }

        /*
         * First check the initial value to verify it has the block number.
         */
        if data[data_offset] != (block_offset % 255) as u8 {
            let byte_offset = bs as u64 * block_offset as u64;
            println!(
                "BO:{} Offset:{}  Expected: {} != Got: {} Block Index",
                block_offset,
                byte_offset,
                block_offset % 255,
                data[data_offset],
            );
            res = false;
        }

        let seed = get_seed(block_offset, wc);
        for i in 1..bs {
            if data[data_offset + i] != seed {
                let byte_offset = bs as u64 * block_offset as u64;
                println!(
                    "BO:{} Offset:{}  Expected: {} != Got: {}",
                    block_offset,
                    byte_offset + i as u64,
                    seed,
                    data[i],
                );
                res = false;
            }
        }
        data_offset += bs as usize;
    }
    res
}

/*
 * Write then read (and verify) to every possible block, with every size that
 * block can possibly support.
 * I named it balloon because each loop on a block "balloons" from the
 * minimum IO size to the largest possible IO size.
 */
async fn balloon_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
) -> Result<()> {
    for block_index in 0..ri.total_blocks {
        /*
         * Loop over all the IO sizes (in blocks) that an IO can
         * have, given our starting block and the total number of blocks
         * We always have at least one block, and possibly more.
         */
        for size in 1..=(ri.max_block_io - block_index) {
            /*
             * Update the write count for all blocks we plan to write to.
             */
            for i in 0..size {
                ri.write_count[block_index + i] += 1;
            }

            let vec =
                fill_vec(block_index, size, &ri.write_count, ri.block_size);
            let data = Bytes::from(vec);
            /*
             * Convert block_index to its byte value.
             */
            let offset =
                Block::new(block_index as u64, ri.block_size.trailing_zeros());

            println!("IO at block:{}  size in blocks:{}", block_index, size);
            let mut waiter = guest.write(offset, data)?;
            waiter.block_wait()?;

            let mut waiter = guest.flush(None)?;
            waiter.block_wait()?;

            let length: usize = size * ri.block_size as usize;
            let vec: Vec<u8> = vec![255; length];
            let data = crucible::Buffer::from_vec(vec);
            let mut waiter = guest.read(offset, data.clone())?;
            waiter.block_wait()?;

            let dl = data.as_vec().to_vec();
            if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
                bail!("Error at {}", block_index);
            }
        }
    }

    verify_volume(guest, ri)?;
    print_write_count(ri);
    Ok(())
}

/*
 * Print out the contents of the write count vector where each line
 * is an extent and each column is a block.
 */
fn print_write_count(ri: &mut RegionInfo) {
    println!(" Write IO count to each extent");
    println!("###############################");
    for (i, wc) in ri.write_count.iter().enumerate().take(ri.total_blocks) {
        print!("{:5} ", wc);
        if (i + 1) % (ri.extent_size.value as usize) == 0 {
            println!();
        }
    }
}

/*
 * Generic workload.  Do a random R/W/F, but wait for the operation to be
 * ACK'd before sending the next.  Limit the size of the IO to 10 blocks.
 * Read data is verified.
 */
async fn generic_workload(
    guest: &Arc<Guest>,
    count: u32,
    ri: &mut RegionInfo,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * TODO: Let the user select the number of loops
     */
    for i in 0..count {
        let op = rng.gen_range(0..10);
        if op == 0 {
            // flush
            println!("{:4}/{:4} FLUSH", i, count);
            let mut waiter = guest.flush(None)?;
            waiter.block_wait()?;
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to 10
            let size = rng.gen_range(1..=10) as usize;

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = ri.total_blocks - size + 1;
            let block_index = rng.gen_range(0..block_max) as usize;

            // Convert offset and length to their byte values.
            let offset =
                Block::new(block_index as u64, ri.block_size.trailing_zeros());

            if op <= 4 {
                // Write
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    ri.write_count[block_index + i] += 1;
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_count, ri.block_size);
                let data = Bytes::from(vec);

                println!(
                    "{:4}/{:4} WRITE {}:{}",
                    i,
                    count,
                    offset.value,
                    data.len()
                );
                let mut waiter = guest.write(offset, data)?;
                waiter.block_wait()?;
            } else {
                // Read (+ verify)
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                println!(
                    "{:4}/{:4} READ  {}:{}",
                    i,
                    count,
                    offset.value,
                    data.len()
                );
                let mut waiter = guest.read(offset, data.clone())?;
                waiter.block_wait()?;

                let dl = data.as_vec().to_vec();
                if !validate_vec(
                    dl,
                    block_index,
                    &ri.write_count,
                    ri.block_size,
                ) {
                    bail!("Verify Error at {} len:{}", block_index, length);
                }
            }
        }
    }

    Ok(())
}

/*
 * Do a few writes to random offsets then exit as soon as they finish.
 * We are trying to leave extents "dirty" so we want to exit before the
 * automatic flush can come through and sync our data.
 */
async fn dirty_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * To store our write requests
     */
    let mut waiterlist = Vec::new();

    let size = 1;
    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let block_max = ri.total_blocks - size + 1;
    for _ in 0..10 {
        let block_index = rng.gen_range(0..block_max) as usize;
        /*
         * Convert offset and length to their byte values.
         */
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        /*
         * Update the write count for the block we plan to write to.
         */
        ri.write_count[block_index] += 1;

        let vec = fill_vec(block_index, size, &ri.write_count, ri.block_size);
        let data = Bytes::from(vec);

        println!("Write at block {}, len:{}", offset.value, data.len());

        let waiter = guest.write(offset, data)?;
        waiterlist.push(waiter);
    }

    println!("loop over {} waiters", waiterlist.len());
    for wa in waiterlist.iter_mut() {
        wa.block_wait()?;
    }
    Ok(())
}

/*
 * Generate a random offset and length, and write to then read from
 * that offset/length.  Verify the data is what we expect.
 */
async fn one_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let size = 1;
    let block_max = ri.total_blocks - size + 1;
    let block_index = rng.gen_range(0..block_max) as usize;

    /*
     * Convert offset and length to their byte values.
     */
    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());

    /*
     * Update the write count for the block we plan to write to.
     */
    ri.write_count[block_index] += 1;

    let vec = fill_vec(block_index, size, &ri.write_count, ri.block_size);
    let data = Bytes::from(vec);

    println!("Write at block {:5}, len:{:7}", offset.value, data.len());

    let mut waiter = guest.write(offset, data)?;
    waiter.block_wait()?;

    let length: usize = size * ri.block_size as usize;
    let vec: Vec<u8> = vec![255; length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Read  at block {:5}, len:{:7}", offset.value, data.len());
    let mut waiter = guest.read(offset, data.clone())?;
    waiter.block_wait()?;

    let dl = data.as_vec().to_vec();
    if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
        bail!("Error at {}", block_index);
    }

    println!("Flush");
    let mut waiter = guest.flush(None)?;
    waiter.block_wait()?;

    Ok(())
}

/*
 * A test of deactivation and re-activation.
 * In a loop, do some IO, then deactivate, then activate.  Verify that
 * written data is read back.  We make use of the generic_workload test
 * for the IO parts of this.
 */
async fn deactivate_workload(
    guest: &Arc<Guest>,
    count: u32,
    ri: &mut RegionInfo,
    mut gen: u64,
) -> Result<()> {
    for c in 0..count {
        println!("{}/{} CLIENT: run rand test", c, count);
        generic_workload(guest, 20, ri).await?;
        println!("{}/{} CLIENT: Now disconnect", c, count);
        let mut waiter = guest.deactivate()?;
        waiter.block_wait()?;
        println!("{}/{} CLIENT: Now disconnect done.", c, count);
        let wc = guest.show_work()?;
        println!(
            "{}/{} CLIENT: Up:{} ds:{}",
            c, count, wc.up_count, wc.ds_count
        );
        let mut retry = 1;
        while let Err(e) = guest.activate(gen) {
            println!("{}/{} Retry:{} activate {:?}", c, count, retry, e);
            std::thread::sleep(std::time::Duration::from_secs(5));
            if retry > 100 {
                bail!("Too many retries {} for activate", retry);
            }
            retry += 1;
        }
        gen += 1;
    }
    println!("One final");
    generic_workload(guest, 20, ri).await?;

    if let Err(e) = verify_volume(guest, ri) {
        bail!("Final volume verify failed: {:?}", e)
    }
    Ok(())
}

/*
 * Generate a random offset and length, and write, flush, then read from
 * that offset/length.  Verify the data is what we expect.
 */
async fn rand_workload(
    guest: &Arc<Guest>,
    count: u32,
    ri: &mut RegionInfo,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * TODO: Let the user select the number of loops
     */
    for c in 0..count {
        /*
         * Pick a random size (in blocks) for the IO, up to the size of the
         * entire region.
         */
        let size = rng.gen_range(1..=ri.max_block_io) as usize;

        /*
         * Once we have our IO size, decide where the starting offset should
         * be, which is the total possible size minus the randomly chosen
         * IO size.
         */
        let block_max = ri.total_blocks - size + 1;
        let block_index = rng.gen_range(0..block_max) as usize;

        /*
         * Convert offset and length to their byte values.
         */
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        /*
         * Update the write count for all blocks we plan to write to.
         */
        for i in 0..size {
            ri.write_count[block_index + i] += 1;
        }

        let vec = fill_vec(block_index, size, &ri.write_count, ri.block_size);
        let data = Bytes::from(vec);

        println!(
            "{:4}/{:4} IO at block {:5}, len:{:7}",
            c,
            count,
            offset.value,
            data.len()
        );
        let mut waiter = guest.write(offset, data)?;
        waiter.block_wait()?;

        let mut waiter = guest.flush(None)?;
        waiter.block_wait()?;

        let length: usize = size * ri.block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        let mut waiter = guest.read(offset, data.clone())?;
        waiter.block_wait()?;

        let dl = data.as_vec().to_vec();
        if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
            bail!("Error at {}", block_index);
        }
    }

    if let Err(e) = verify_volume(guest, ri) {
        bail!("Final volume verify failed: {:?}", e)
    }

    if count >= 10 {
        print_write_count(ri);
    }

    Ok(())
}

/*
 * Send bursts of work to the demo_workload function.
 * Wait for each burst to finish, pause, then loop.
 */
async fn burst_workload(
    guest: &Arc<Guest>,
    count: u32,
    demo_count: u32,
    ri: &mut RegionInfo,
    verify_out: &Option<PathBuf>,
) -> Result<()> {
    // TODO: let user pick loop count
    for c in 0..count {
        demo_workload(guest, demo_count, ri).await?;
        let mut wc = guest.show_work()?;
        while wc.up_count + wc.ds_count != 0 {
            std::thread::sleep(std::time::Duration::from_secs(1));
            println!("{}/{} Up:{} ds:{}", c, count, wc.up_count, wc.ds_count);
            std::thread::sleep(std::time::Duration::from_secs(4));
            wc = guest.show_work()?;
        }

        /*
         * Once everyone is caught up, save the state just in case
         * the user wants to quit at this pause step
         */
        println!();
        if let Some(vo) = &verify_out {
            let cp = history_file(vo);
            write_json(&cp, &ri.write_count, true)?;
            println!("Wrote out file {:?} at this time", cp);
        }
        println!(
            "{}/{}: 5 second pause, then run another test loop",
            c, count
        );
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
    Ok(())
}

/*
 * Like the random test, but with IO not as large, and with frequent
 * showing of the internal work queues.  Submit a bunch of random IOs,
 * then watch them complete.
 */
async fn demo_workload(
    guest: &Arc<Guest>,
    count: u32,
    ri: &mut RegionInfo,
) -> Result<()> {
    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    let mut waiterlist = Vec::new();
    // TODO: Let the user select the number of loops
    // TODO: Allow user to request r/w/f percentage (how???)
    for i in 0..count {
        let op = rng.gen_range(0..10);
        if op == 0 {
            // flush
            let waiter = guest.flush(None)?;
            waiterlist.push(waiter);
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to 10
            let size = rng.gen_range(1..=10) as usize;

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = ri.total_blocks - size + 1;
            let block_index = rng.gen_range(0..block_max) as usize;

            // Convert offset and length to their byte values.
            let offset =
                Block::new(block_index as u64, ri.block_size.trailing_zeros());

            if op <= 4 {
                // Write
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    ri.write_count[block_index + i] += 1;
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_count, ri.block_size);
                let data = Bytes::from(vec);

                let waiter = guest.write(offset, data)?;
                waiterlist.push(waiter);
            } else {
                // Read
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                let waiter = guest.read(offset, data.clone())?;
                waiterlist.push(waiter);
            }

            if i == 10 || i == 20 {
                guest.show_work()?;
            }
        }
    }
    let mut wc = WQCounts {
        up_count: 0,
        ds_count: 0,
    };

    println!("loop over {} waiters", waiterlist.len());
    for wa in waiterlist.iter_mut() {
        wa.block_wait()?;
    }

    /*
     * Continue loping until all downstairs jobs finish also.
     */
    println!("All submitted jobs completed, waiting for downstairs");
    while wc.up_count + wc.ds_count > 0 {
        wc = guest.show_work()?;
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
    println!("All downstairs jobs completed.");
    if let Err(e) = verify_volume(guest, ri) {
        bail!("Final volume verify failed: {:?}", e)
    }

    Ok(())
}

/*
 * This is a test workload that generates a single write spanning an extent
 * then will try to read the same.
 */
fn span_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    /*
     * Pick the last block in the first extent
     */
    let block_index = (ri.extent_size.value - 1) as usize;

    /*
     * Update the counter for the blocks we are about to write.
     */
    ri.write_count[block_index] += 1;
    ri.write_count[block_index + 1] += 1;

    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());
    let vec = fill_vec(block_index, 2, &ri.write_count, ri.block_size);
    let data = Bytes::from(vec);

    println!("Sending a write spanning two extents");
    let mut waiter = guest.write(offset, data)?;
    waiter.block_wait()?;

    println!("Sending a flush");
    let mut waiter = guest.flush(None)?;
    waiter.block_wait()?;

    let length: usize = 2 * ri.block_size as usize;
    let vec: Vec<u8> = vec![99; length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Sending a read spanning two extents");
    waiter = guest.read(offset, data.clone())?;
    waiter.block_wait()?;

    let dl = data.as_vec().to_vec();
    if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
        bail!("Span read verify failed");
    }
    Ok(())
}

/*
 * Write, flush, then read every block in the volume.
 * We wait for each op to finish, so this is all sequential.
 */
fn big_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    for block_index in 0..ri.total_blocks {
        /*
         * Update the write count for all blocks we plan to write to.
         */
        ri.write_count[block_index] += 1;

        let vec = fill_vec(block_index, 1, &ri.write_count, ri.block_size);
        let data = Bytes::from(vec);
        /*
         * Convert block_index to its byte value.
         */
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        let mut waiter = guest.write(offset, data)?;
        waiter.block_wait()?;

        let mut waiter = guest.flush(None)?;
        waiter.block_wait()?;

        let length: usize = ri.block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        let mut waiter = guest.read(offset, data.clone())?;
        waiter.block_wait()?;

        let dl = data.as_vec().to_vec();
        if !validate_vec(dl, block_index, &ri.write_count, ri.block_size) {
            bail!("Verify error at block:{}", block_index);
        }
    }

    println!("All IOs sent");
    guest.show_work()?;

    Ok(())
}

fn biggest_io_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    /*
     * Based on our protocol, send the biggest IO we can.
     */
    let biggest_io_in_blocks =
        crucible_protocol::CrucibleEncoder::max_io_blocks(
            ri.block_size as usize,
        )?;

    for block_index in 0..(ri.total_blocks - biggest_io_in_blocks) {
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        let sz = biggest_io_in_blocks * (ri.block_size as usize);
        let mut data = Vec::with_capacity(sz);
        data.resize(sz, 1);

        println!(
            "IO at block:{}  size in blocks:{}",
            block_index, biggest_io_in_blocks
        );

        let mut waiter = guest.write(offset, Bytes::from(data))?;
        waiter.block_wait()?;
    }

    Ok(())
}

/*
 * A loop that generates a bunch of random reads and writes, increasing the
 * offset each operation.  After 20 are submitted, we wait for all to finish.
 * Use this test and pass the --lossy flag and upstairs will at random skip
 * sending jobs to the downstairs, creating dependencys that it will
 * eventually resolve.
 *
 * TODO: Make this test use the global write count.
 */
async fn dep_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    let final_offset = ri.total_size - ri.block_size;

    let mut my_offset: u64 = 0;
    for my_count in 1..150 {
        let mut waiterlist = Vec::new();

        /*
         * Generate some number of operations
         */
        for ioc in 0..200 {
            my_offset = (my_offset + ri.block_size) % final_offset;
            if random() {
                /*
                 * Generate a write buffer with a locally unique value.
                 */
                let mut vec: Vec<u8> =
                    Vec::with_capacity(ri.block_size as usize);
                let seed = ((my_offset % 254) + 1) as u8;
                for _ in 0..ri.block_size {
                    vec.push(seed);
                }
                let data = Bytes::from(vec);

                println!(
                    "Loop:{} send write {} @ offset:{}  len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );
                let waiter = guest.write_to_byte_offset(my_offset, data)?;
                waiterlist.push(waiter);
            } else {
                let vec: Vec<u8> = vec![0; ri.block_size as usize];
                let data = crucible::Buffer::from_vec(vec);

                println!(
                    "Loop:{} send read  {} @ offset:{} len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );
                let waiter = guest.read_from_byte_offset(my_offset, data)?;
                waiterlist.push(waiter);
            }
        }

        guest.show_work()?;

        // The final flush is to help prevent the pause that we get when the
        // last command is a write or read and we have to wait x seconds for the
        // flush check to trigger.
        println!("Loop:{} send a final flush and wait", my_count);
        let mut flush_waiter = guest.flush(None)?;
        flush_waiter.block_wait()?;

        println!("Loop:{} loop over {} waiters", my_count, waiterlist.len());
        for wa in waiterlist.iter_mut() {
            wa.block_wait()?;
        }
        println!("Loop:{} all waiters done", my_count);
        guest.show_work()?;
    }

    println!("dep test done");
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
        my_offset += 512 * 2;
        scope.wait_for("write 1").await;

        println!("send write 1");
        guest.write_to_byte_offset(my_offset, data.freeze())?;

        scope.wait_for("show work").await;
        guest.show_work()?;

        let mut read_offset = 512 * 99;
        const READ_SIZE: usize = 4096;
        for _ in 0..4 {
            let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

            println!("send a read");
            // scope.wait_for("send Read").await;
            guest.read_from_byte_offset(read_offset, data)?;
            read_offset += READ_SIZE as u64;
            // scope.wait_for("show work").await;
            guest.show_work()?;
        }

        // scope.wait_for("Flush step").await;
        println!("send flush");
        guest.flush(None)?;

        let mut data = BytesMut::with_capacity(512);
        data.put(&[0xbb; 512][..]);

        // scope.wait_for("write 2").await;
        println!("send write 2");
        guest.write_to_byte_offset(my_offset, data.freeze())?;
        my_offset += 512;
        // scope.wait_for("show work").await;
        guest.show_work()?;
        //scope.wait_for("at the bottom").await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_compare() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 1;

        let vec = fill_vec(0, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, 0, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_fail() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let vec = fill_vec(0, 1, &write_count, bs);
        write_count[0] = 1;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_fail_buf() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let mut vec = fill_vec(0, 1, &write_count, bs);
        vec[2] = 1;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_fail_block() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let mut vec = fill_vec(0, 1, &write_count, bs);
        vec[0] = 3;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_1() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] += 1;

        let vec = fill_vec(block_index, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_large() {
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let write_count = vec![1_u32; total_blocks];
        let vec = fill_vec(block_index, total_blocks, &write_count, bs);

        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_large_fail() {
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let write_count = vec![1_u32; total_blocks];
        let mut vec = fill_vec(block_index, total_blocks, &write_count, bs);
        let x = vec.len() - 1;
        vec[x] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_span() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let vec = fill_vec(block_index, 3, &write_count, bs);
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_span_fail() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let mut vec = fill_vec(block_index, 3, &write_count, bs);
        /*
         * Replace the first value in the second block
         */
        vec[(bs + 1) as usize] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_span_fail_2() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let mut vec = fill_vec(block_index, 3, &write_count, bs);
        /*
         * Replace the second value in the second block
         */
        vec[(bs + 2) as usize] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_empty() {
        let bs: u64 = 512;
        let write_count = vec![0_u32; 10];

        let vec = fill_vec(0, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, 0, &write_count, bs), true);
    }
}
