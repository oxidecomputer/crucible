// Copyright 2022 Oxide Computer Company
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use csv::WriterBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::time::{Duration, Instant};

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
    /// Starts a CLI client
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
    Fill,
    Generic,
    Nothing,
    One,
    /// Run the perf test, random writes, then random reads
    Perf {
        /// Size in blocks of each IO
        #[structopt(long, default_value = "1")]
        io_size: usize,
        /// Number of outstanding IOs at the same time.
        #[structopt(long, default_value = "1")]
        io_depth: usize,
        /// Output file for IO times
        #[structopt(long, global = true, parse(from_os_str), name = "PERF")]
        perf_out: Option<PathBuf>,
    },
    Rand,
    Repair,
    Span,
    Verify,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "crucible upstairs test client")]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
pub struct Opt {
    ///  For tests that support it, pass this count value for the number
    ///  of loops the test should do.
    #[structopt(short, long, global = true, default_value = "0")]
    count: usize,

    #[structopt(short, long, global = true, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddr>,

    #[structopt(subcommand)]
    workload: Workload,

    ///  This allows the Upstairs to run in a mode where it will not
    ///  always submit new work to downstairs when it first receives
    ///  it.  This is for testing dependencies and should not be
    ///  used in production.  Passing args like this to the upstairs
    ///  may not be the best way to test, but until we have something
    ///  better... XXX
    #[structopt(long, global = true)]
    lossy: bool,

    ///  quit after all crucible work queues are empty.
    #[structopt(short, global = true, long)]
    quit: bool,

    #[structopt(short, global = true, long)]
    key: Option<String>,

    #[structopt(short, global = true, long, default_value = "0")]
    gen: u64,

    /// For the verify test, if this option is included we will allow
    /// the write log range of data to pass the verify_volume check.
    #[structopt(long, global = true)]
    range: bool,

    /// Retry for activate, as long as it takes.  If we pass this arg, the
    /// test will retry the initial activate command as long as it takes.
    #[structopt(long, global = true)]
    retry_activate: bool,

    /// In addition to any tests, verify the volume on startup.
    /// This only has value if verify_in is also set.
    #[structopt(long, global = true)]
    verify: bool,

    /// For tests that support it, load the expected write count from
    /// the provided file.  The addition of a --verify option will also
    /// have the test verify what it imports from the file is valid.
    #[structopt(long, global = true, parse(from_os_str), name = "INFILE")]
    verify_in: Option<PathBuf>,

    ///  For tests that support it, save the write count into the
    ///  provided file.
    #[structopt(long, global = true, parse(from_os_str), name = "FILE")]
    verify_out: Option<PathBuf>,

    // TLS options
    #[structopt(long)]
    cert_pem: Option<String>,
    #[structopt(long)]
    key_pem: Option<String>,
    #[structopt(long)]
    root_cert_pem: Option<String>,

    /// IP:Port for the upstairs control http server
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
 * Not all tests make use of the write_log yet, but perhaps someday..
 */
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegionInfo {
    block_size: u64,
    extent_size: Block,
    total_size: u64,
    total_blocks: usize,
    write_log: WriteLog,
    max_block_io: usize,
}

/*
 * All the tests need this basic set of information about the region.
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
     * Create the write log that tracks the number of writes to each block,
     * so we can know what to expect for reads.
     */
    let write_log = WriteLog::new(total_blocks);

    Ok(RegionInfo {
        block_size,
        extent_size,
        total_size,
        total_blocks,
        write_log,
        max_block_io,
    })
}

/**
 * The write log is a recording of the number of times we have written to
 * a specific block (index in the Vec).  The write count is used to generate
 * a known pattern to either fill the block with, or to expect from the
 * block when reading.
 *
 * This is fine for an initial fill/verify framework of sorts, but there
 * are many kinds of errors this will not find.  There are also many high
 * performance better coverage kinds of data integrity tests, and the intent
 * here is to balance urgency with rigor in that we can make use of external
 * tests for the more complicated cases, and catch the easy ones here.
 *
 * In addition to the current write count, we make a second copy of the
 * write count when the commit method is called.  This can be used to record
 * the write count of a region at a specific time (like a flush) and then
 * later used to verify that a given block has data in it from at minimum
 * that commit, but up to the current write count.
 *
 * The "seed" is the current counter as a u8 for a given block.
 */
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WriteLog {
    count_cur: Vec<u32>,
    count_min: Vec<u32>,
}

impl WriteLog {
    pub fn new(size: usize) -> Self {
        let count_cur = vec![0_u32; size];
        let count_min = vec![0_u32; size];

        WriteLog {
            count_cur,
            count_min,
        }
    }

    pub fn is_empty(&mut self) -> bool {
        self.count_cur.is_empty()
    }

    pub fn len(&mut self) -> usize {
        self.count_cur.len()
    }

    // If the write count is zero then we have no record of what this
    // block contains.
    pub fn unwritten(&self, index: usize) -> bool {
        self.count_cur[index] == 0
    }

    // This is called when we are about to write to a block and we want
    // to indicate that the write counter should be updated.
    pub fn update_wc(&mut self, index: usize) {
        assert!(self.count_cur[index] >= self.count_min[index]);
        // TODO: handle more than u32 max writes to the same location.
        self.count_cur[index] += 1;
    }

    // This returns the value we should expect to find at the given index,
    // and will fit in a u8.  If we are using the seed to fill a write
    // volume, then update_wc() should be called first to increment the
    // counter before we get a new seed.
    fn get_seed(&self, index: usize) -> u8 {
        (self.count_cur[index] % 250) as u8
    }

    // This is called before a test where we expect to be recovering and we
    // want to record the current write log values as a minimum of what
    // we expect the counters to be.
    pub fn commit(&mut self) {
        self.count_min = self.count_cur.clone();
    }

    // In repair/recovery, when there is IO after a flush, it's possible
    // that data never made it to storage.  We are asking to verify a
    // given value is in the range of possible values that could exist
    // for the index. Any valid value in the range between count_min to
    // count_cur.
    //
    // For this to work correctly, the test must issue a commit() of the
    // WriteLog when it knows that the current write count is the minimum.
    // This is only acceptable in a very specific recovery/repair situation
    // and not part of a normal test.
    //
    // If update is set to true, then we also change the count_cur to match
    // the given value (corrected to be a u32 and not a u8).
    pub fn validate_seed_range(
        &mut self,
        index: usize,
        value: u8,
        update: bool,
    ) -> bool {
        let max = (self.count_cur[index] % 250) as u8;
        let min = (self.count_min[index] % 250) as u8;

        let res;
        let mut new_max = self.count_cur[index];
        // A little bit of work here when max rolls over and is < min.
        // Because we need to handle the update case, we also need
        // to determine what the "correct" count_cur should be if we
        // are going to update our internal counter to match the value
        // passed to us from the caller.
        if min > max {
            if value >= min && value < 250 {
                res = true;
                new_max = self.count_cur[index] + 250 - value as u32;
            } else if value <= max {
                res = true;
                new_max = self.count_cur[index] + (max - value) as u32;
            } else {
                res = false;
            }
        } else if value >= min && value <= max {
            res = true;
            new_max = self.count_cur[index] - (max - value) as u32;
        } else {
            res = false;
        }
        if update {
            println!("Update block {} to {}", index, new_max);
            self.count_cur[index] = new_max;
        }
        res
    }

    // Set the current write count to a specific value.
    // You should only be using this if you know what you are doing.
    pub fn set_wc(&mut self, index: usize, value: u32) {
        self.count_cur[index] = value;
    }

    // Set the write count value for the minimum.
    // You should only be using this if you know what you are doing.
    pub fn set_wc_min(&mut self, index: usize, value: u32) {
        self.count_min[index] = value;
    }
}

fn load_write_log(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    vi: PathBuf,
    verify: bool,
) -> Result<()> {
    /*
     * Fill the write count from a provided file.
     */
    let cp = history_file(vi);
    ri.write_log = match read_json(&cp) {
        Ok(write_log) => write_log,
        Err(e) => bail!("Error {:?} reading verify config {:?}", e, cp),
    };
    println!("Loading write count information from file {:?}", cp);
    if ri.write_log.len() != ri.total_blocks {
        bail!(
            "Verify file {:?} blocks:{} does not match regions:{}",
            cp,
            ri.write_log.len(),
            ri.total_blocks
        );
    }
    /*
     * Only verify the volume if requested.
     */
    if verify {
        if let Err(e) = verify_volume(guest, ri, false) {
            bail!("Initial volume verify failed: {:?}", e)
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
    if opt.verify && opt.verify_in.is_none() {
        bail!("Initial verify requires verify_in file");
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
        runtime.block_on(cli::start_cli_server(
            &guest,
            listen,
            port,
            opt.verify_in,
            opt.verify_out,
        ))?;
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

    println!("Wait for a query_work_queue command to finish before sending IO");
    guest.query_work_queue()?;
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
    if let Some(verify_in) = opt.verify_in {
        /*
         * If we are running the verify test, then don't verify while
         * loading the file.  Otherwise, do whatever the opt.verify
         * option has in it.
         */
        let verify = {
            if opt.workload == Workload::Verify {
                false
            } else {
                opt.verify
            }
        };
        load_write_log(&guest, &mut region_info, verify_in, verify)?;
    }

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
            let count = {
                if opt.count == 0 {
                    10
                } else {
                    opt.count
                }
            };
            runtime.block_on(dirty_workload(
                &guest,
                &mut region_info,
                count,
            ))?;

            /*
             * Saving state here when we have not waited for a flush
             * to finish means that the state recorded here may not be
             * what ends up being in the downstairs.  All we guarantee is
             * that everything before the flush will be there, and possibly
             * things that came after the flush.
             */
            if let Some(vo) = &opt.verify_out {
                let cp = history_file(vo);
                write_json(&cp, &region_info.write_log, true)?;
                println!("Wrote out file {:?}", cp);
            }
            return Ok(());
        }

        Workload::Fill => {
            println!("Fill test");
            runtime.block_on(fill_workload(&guest, &mut region_info))?;
        }

        Workload::Generic => {
            let count = {
                if opt.count == 0 {
                    500
                } else {
                    opt.count
                }
            };
            runtime.block_on(generic_workload(
                &guest,
                count,
                &mut region_info,
            ))?;
        }

        Workload::One => {
            println!("One test");
            runtime.block_on(one_workload(&guest, &mut region_info))?;
        }
        Workload::Perf {
            io_size,
            io_depth,
            perf_out,
        } => {
            println!("Perf test");
            let ops = {
                if opt.count == 0 {
                    5000_usize
                } else {
                    opt.count
                }
            };

            let mut opt_wtr = None;
            let mut wtr;
            if let Some(perf_out) = perf_out {
                wtr = WriterBuilder::new()
                    .has_headers(false)
                    .from_path(perf_out)
                    .unwrap();
                opt_wtr = Some(&mut wtr);
            }

            for _ in 0..5 {
                runtime.block_on(perf_workload(
                    &guest,
                    &mut region_info,
                    &mut opt_wtr,
                    ops,
                    io_depth,
                    io_size,
                ))?;
            }
            if opt.quit {
                return Ok(());
            }
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
            let count = {
                if opt.count == 0 {
                    10
                } else {
                    opt.count
                }
            };
            runtime.block_on(rand_workload(&guest, count, &mut region_info))?;
        }
        Workload::Repair => {
            println!("Run Repair workload");
            let count = {
                if opt.count == 0 {
                    10
                } else {
                    opt.count
                }
            };
            runtime.block_on(repair_workload(
                &guest,
                count,
                &mut region_info,
            ))?;
            drop(guest);
            if let Some(vo) = &opt.verify_out {
                let cp = history_file(vo);
                write_json(&cp, &region_info.write_log, true)?;
                println!("Wrote out file {:?}", cp);
            }
            return Ok(());
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
            if let Err(e) = verify_volume(&guest, &mut region_info, opt.range) {
                bail!("Initial volume verify failed: {:?}", e)
            }
            if let Some(vo) = &opt.verify_out {
                let cp = history_file(vo);
                write_json(&cp, &region_info.write_log, true)?;
                println!("Wrote out file {:?}", cp);
            }
            if opt.quit {
                println!("Verify test completed");
            } else {
                println!("Verify read loop begins");
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    if let Err(e) =
                        verify_volume(&guest, &mut region_info, opt.range)
                    {
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
        write_json(&cp, &region_info.write_log, true)?;
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
 * Read/Verify every possible block, up to 100 blocks at a time.
 * If range is set to true, we allow the write log to consider any valid
 * value for a block since the last commit was called.
 */
fn verify_volume(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    range: bool,
) -> Result<()> {
    assert_eq!(ri.write_log.len(), ri.total_blocks);

    println!("Read and Verify all blocks (0..{})", ri.total_blocks);

    let pb = ProgressBar::new(ri.total_blocks as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .progress_chars("#>-"));

    let io_sz = 100;
    let mut block_index = 0;
    while block_index < ri.total_blocks {
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        let next_io_blocks = if block_index + io_sz > ri.total_blocks {
            ri.total_blocks - block_index
        } else {
            io_sz
        };

        let length: usize = next_io_blocks * ri.block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        let mut waiter = guest.read(offset, data.clone())?;
        waiter.block_wait()?;

        let dl = data.as_vec().to_vec();
        match validate_vec(
            dl,
            block_index,
            &mut ri.write_log,
            ri.block_size,
            range,
        ) {
            ValidateStatus::Bad => {
                pb.finish_with_message("Error");
                bail!(
                    "Error in range {} -> {}",
                    block_index,
                    block_index + io_sz
                );
            }
            ValidateStatus::InRange => {
                if range {
                    {}
                } else {
                    pb.finish_with_message("Error");
                    bail!("Error at {}", block_index);
                }
            }
            ValidateStatus::Good => {}
        }

        block_index += next_io_blocks;
        pb.set_position(block_index as u64);
    }
    pb.finish();
    Ok(())
}

/*
 * Fill a vec based on the write count at our index.
 *
 * block_index: What block we started reading from.
 * blocks:      The length of the read in blocks.
 * wc:          The write count vec, indexed by block number.
 * bs:          Crucible's block size.
 */
fn fill_vec(
    block_index: usize,
    blocks: usize,
    wl: &WriteLog,
    bs: u64,
) -> Vec<u8> {
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
        let seed = wl.get_seed(block_offset);
        for _ in 1..bs {
            vec.push(seed);
        }
    }
    vec
}

/*
 * Status for the validate vec function.
 * If the buffer and write log data match, we return Good.
 * If the buffer and the write log don't match on the highest
 * write count, but is within range, we return InRange.
 * If the buffer and the write log don't match within range, then
 * we return Bad.
 *
 * If we return InRange, it means we have also updated the internal
 * counters to match exactly, meaning the write log (--verify-out) now
 * should be written back out, and future calls to verify_vec will
 * expect the same value (i.e. we cut off any higher write count).
 */
#[derive(Debug, PartialEq)]
enum ValidateStatus {
    Good,
    InRange,
    Bad,
}
/*
 * Compare a vec buffer with what we expect to be written for that offset.
 * This assumes you used the fill_vec function (with get_seed) to write
 * the buffer originally.
 *
 * data:        The filled in buffer to be verified.
 * block_index: What block we started reading from.
 * wl:          The WriteLog struct where we store the write counter.
 * bs:          Crucible's block size.
 * range:       If the validation should consider the write log range
 *              for acceptable values in the data buffer.
 */
fn validate_vec(
    data: Vec<u8>,
    block_index: usize,
    wl: &mut WriteLog,
    bs: u64,
    range: bool,
) -> ValidateStatus {
    let bs = bs as usize;
    assert_eq!(data.len() % bs, 0);
    assert_ne!(data.len(), 0);

    let blocks = data.len() / bs;
    let mut data_offset: usize = 0;
    let mut res = ValidateStatus::Good;
    /*
     * The outer loop walks the buffer by blocks, as each block will have
     * its own unique write count.
     */
    for block_offset in block_index..(block_index + blocks) {
        /*
         * Skip blocks we don't know the expected value of
         */
        if wl.unwritten(block_offset) {
            data_offset += bs;
            continue;
        }

        /*
         * First check the initial value to verify it has the block number.
         */
        if data[data_offset] != (block_offset % 255) as u8 {
            let byte_offset = bs as u64 * block_offset as u64;
            println!(
                "Mismatch Block Index Block:{} Offset:{} Expected:{} Got:{}",
                block_offset,
                byte_offset,
                block_offset % 255,
                data[data_offset],
            );
            res = ValidateStatus::Bad;
        }

        let seed = wl.get_seed(block_offset);
        for i in 1..bs {
            if data[data_offset + i] != seed {
                let byte_offset = bs as u64 * block_offset as u64;
                println!(
                    "Mismatch in Block:{} Volume offset:{}  Expected:{} Got:{}",
                    block_offset,
                    byte_offset + i as u64,
                    seed,
                    data[data_offset + i],
                );
                if range {
                    if wl.validate_seed_range(
                        block_offset,
                        data[data_offset + i],
                        true,
                    ) {
                        println!(
                            "Block {} is in valid seed range",
                            block_offset
                        );
                        res = ValidateStatus::InRange;
                    } else {
                        println!(
                            "Block {} Also fails valid seed range",
                            block_offset
                        );
                        res = ValidateStatus::Bad;
                    }
                } else {
                    res = ValidateStatus::Bad;
                }
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
                ri.write_log.update_wc(block_index + i);
            }

            let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
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
            match validate_vec(
                dl,
                block_index,
                &mut ri.write_log,
                ri.block_size,
                false,
            ) {
                ValidateStatus::Bad | ValidateStatus::InRange => {
                    bail!("Error at {}", block_index);
                }
                ValidateStatus::Good => {}
            }
        }
    }

    verify_volume(guest, ri, false)?;
    Ok(())
}

/*
 * Write then read (and verify) to every possible block.
 */
async fn fill_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    let pb = ProgressBar::new(ri.total_blocks as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .progress_chars("#>-"));

    let io_sz = 100;

    let mut block_index = 0;
    while block_index < ri.total_blocks {
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        let next_io_blocks = if block_index + io_sz > ri.total_blocks {
            ri.total_blocks - block_index
        } else {
            io_sz
        };

        for i in 0..next_io_blocks {
            ri.write_log.update_wc(block_index + i);
        }

        let vec =
            fill_vec(block_index, next_io_blocks, &ri.write_log, ri.block_size);
        let data = Bytes::from(vec);

        let mut waiter = guest.write(offset, data)?;
        waiter.block_wait()?;

        block_index += next_io_blocks;
        pb.set_position(block_index as u64);
    }

    let mut waiter = guest.flush(None)?;
    waiter.block_wait()?;
    pb.finish();

    verify_volume(guest, ri, false)?;
    Ok(())
}

/*
 * Generic workload.  Do a random R/W/F, but wait for the operation to be
 * ACK'd before sending the next.  Limit the size of the IO to 10 blocks.
 * Read data is verified.
 */
async fn generic_workload(
    guest: &Arc<Guest>,
    count: usize,
    ri: &mut RegionInfo,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

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
                    ri.write_log.update_wc(block_index + i);
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_log, ri.block_size);
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
                match validate_vec(
                    dl,
                    block_index,
                    &mut ri.write_log,
                    ri.block_size,
                    false,
                ) {
                    ValidateStatus::Bad | ValidateStatus::InRange => {
                        bail!("Verify Error at {} len:{}", block_index, length);
                    }
                    ValidateStatus::Good => {}
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
async fn dirty_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    count: usize,
) -> Result<()> {
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
    for c in 1..=count {
        let block_index = rng.gen_range(0..block_max) as usize;
        /*
         * Convert offset and length to their byte values.
         */
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        /*
         * Update the write count for the block we plan to write to.
         */
        ri.write_log.update_wc(block_index);

        let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
        let data = Bytes::from(vec);

        println!(
            "[{}/{}] Write at block {}, len:{}",
            c,
            count,
            offset.value,
            data.len()
        );

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
 * Take the Vec of Durations for IOs and write it out in CSV format using
 * the provided CSV Writer.
 */
pub fn perf_csv(
    wtr: &mut csv::Writer<File>,
    msg: &str,
    count: usize,
    io_depth: usize,
    io_size: usize,
    iotimes: Vec<Duration>,
) {
    // Convert all Durations to u64 nanoseconds.
    let times = iotimes
        .iter()
        .map(|x| (x.as_secs() as u64 * 100000000) + x.subsec_nanos() as u64)
        .collect::<Vec<u64>>();

    wtr.serialize(Record {
        label: msg.to_string(),
        io_depth,
        io_size,
        count,
        time: times,
    })
    .unwrap();
    wtr.flush().unwrap();
}

/*
 * Display the summary results from a perf run.
 */
fn perf_summary(
    msg: &str,
    count: usize,
    io_depth: usize,
    times: Vec<Duration>,
    total_time: Duration,
) {
    // Convert all the Durations into floats.
    let mut times = times
        .iter()
        .map(|x| x.as_secs() as f32 + (x.subsec_nanos() as f32 / 1e9))
        .collect::<Vec<f32>>();
    times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Determine the number of seconds as a float elapsed to perform
    // all the IOs. Then, divide the total operations performed to get
    // the average IOPs
    let time_f =
        total_time.as_secs() as f32 + (total_time.subsec_nanos() as f32 / 1e9);

    println!(
        "{}:{:.2}sec {} {} IOPs:{:.2} \
        mean:{:.5} stdv:{:.5} \
        min:{:.5} max:{:.5}",
        msg,
        time_f,
        count,
        io_depth,
        count as f32 / time_f,
        statistical::mean(&times),
        statistical::standard_deviation(&times, None),
        times.first().unwrap(),
        times.last().unwrap(),
    );
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    label: String,
    io_depth: usize,
    io_size: usize,
    count: usize,
    time: Vec<u64>,
}
/**
 * A simple IO test in two stages: 100% random writes, then 100% random
 * reads. The caller can select:
 * io_depth: The number of outstanding IOs issued at a time
 * blocks_per_io: The size of each io (in multiple of block size).
 * count: The number of loops to perform for each test (all IOs in io_depth
 *        are considered as a single loop).
 * A summary is printed at the end of each stage.
 */
async fn perf_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    wtr: &mut Option<&mut csv::Writer<File>>,
    count: usize,
    io_depth: usize,
    blocks_per_io: usize,
) -> Result<()> {
    let mut rng = rand::thread_rng();
    let io_size = blocks_per_io * ri.block_size as usize;

    let write_buffers: Vec<Bytes> = (0..io_depth)
        .map(|_| {
            Bytes::from(
                (0..io_size)
                    .map(|_| rng.sample(rand::distributions::Standard))
                    .collect::<Vec<u8>>(),
            )
        })
        .collect();
    let read_buffers: Vec<Buffer> = (0..io_depth)
        .map(|_| Buffer::new(io_size as usize))
        .collect();

    println!(
        "Performance test io_size:({}){} io_depth:{} total ops:{}",
        blocks_per_io, io_size, io_depth, count,
    );
    // To make a random block offset modulus, we take the total
    // block number and subtract the IO size in blocks.
    let offset_mod =
        rng.gen::<u64>() % (ri.total_blocks - blocks_per_io) as u64;

    let mut wtime = Vec::with_capacity(count);
    let big_start = Instant::now();
    for _ in 0..count {
        let burst_start = Instant::now();
        let mut write_waiters = Vec::with_capacity(io_depth);
        for write_buffer in write_buffers.iter().take(io_depth) {
            let offset: u64 = rng.gen::<u64>() % offset_mod;
            // println!("Write at: {}", offset);

            let waiter = guest.write_to_byte_offset(
                offset * ri.block_size,
                write_buffer.clone(),
            )?;
            write_waiters.push(waiter);
        }

        for mut waiter in write_waiters {
            waiter.block_wait()?;
        }
        wtime.push(burst_start.elapsed());
    }
    let big_end = big_start.elapsed();

    guest.flush(None)?;
    perf_summary("rwrites", count, io_depth, wtime.clone(), big_end);
    if let Some(wtr) = wtr {
        perf_csv(wtr, "rwrite", count, io_depth, blocks_per_io, wtime.clone());
    }

    // Before we start reads, make sure the work queues are empty.
    loop {
        let wc = guest.query_work_queue()?;
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    let mut rtime = Vec::with_capacity(count);
    let big_start = Instant::now();
    for _ in 0..count {
        let burst_start = Instant::now();
        let mut read_waiters = Vec::with_capacity(io_depth);
        for read_buffer in read_buffers.iter().take(io_depth) {
            let offset: u64 = rng.gen::<u64>() % offset_mod;

            let waiter = guest.read_from_byte_offset(
                offset * ri.block_size,
                read_buffer.clone(),
            )?;
            read_waiters.push(waiter);
        }
        for mut waiter in read_waiters {
            waiter.block_wait()?;
        }
        rtime.push(burst_start.elapsed());
    }
    let big_end = big_start.elapsed();

    perf_summary(" rreads", count, io_depth, rtime.clone(), big_end);

    if let Some(wtr) = wtr {
        perf_csv(wtr, "rread", count, io_depth, blocks_per_io, rtime.clone());
    }

    guest.flush(None)?;
    // Before we return, make sure the work queues are empty.
    loop {
        let wc = guest.query_work_queue()?;
        if wc.up_count + wc.ds_count == 0 {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_secs(4));
    }
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
    ri.write_log.update_wc(block_index);

    let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
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
    match validate_vec(dl, block_index, &mut ri.write_log, ri.block_size, false)
    {
        ValidateStatus::Bad | ValidateStatus::InRange => {
            bail!("Error at {}", block_index);
        }
        ValidateStatus::Good => {}
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
    count: usize,
    ri: &mut RegionInfo,
    mut gen: u64,
) -> Result<()> {
    for c in 1..=count {
        println!("{}/{} CLIENT: run rand test", c, count);
        generic_workload(guest, 20, ri).await?;
        println!("{}/{} CLIENT: Now disconnect", c, count);
        let mut waiter = guest.deactivate()?;
        println!("{}/{} CLIENT: Now disconnect wait", c, count);
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

    println!("final verify");
    if let Err(e) = verify_volume(guest, ri, false) {
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
    count: usize,
    ri: &mut RegionInfo,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    for c in 1..=count {
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
            ri.write_log.update_wc(block_index + i);
        }

        let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
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
        match validate_vec(
            dl,
            block_index,
            &mut ri.write_log,
            ri.block_size,
            false,
        ) {
            ValidateStatus::Bad | ValidateStatus::InRange => {
                bail!("Error at {}", block_index);
            }
            ValidateStatus::Good => {}
        }
    }

    if let Err(e) = verify_volume(guest, ri, false) {
        bail!("Final volume verify failed: {:?}", e)
    }

    Ok(())
}

/*
 * Send bursts of work to the demo_workload function.
 * Wait for each burst to finish, pause, then loop.
 */
async fn burst_workload(
    guest: &Arc<Guest>,
    count: usize,
    demo_count: usize,
    ri: &mut RegionInfo,
    verify_out: &Option<PathBuf>,
) -> Result<()> {
    // TODO: let user pick loop count
    for c in 1..=count {
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
            write_json(&cp, &ri.write_log, true)?;
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
 * issue some random number of IOs, then wait for an ACK for all.
 * We try to exit this test and leave jobs outstanding.
 */
async fn repair_workload(
    guest: &Arc<Guest>,
    count: usize,
    ri: &mut RegionInfo,
) -> Result<()> {
    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    // Any state coming in should have been verified, so we can
    // consider the current write log to be the minimum possible values.
    ri.write_log.commit();
    let mut waiterlist = Vec::new();
    // TODO: Allow user to request r/w/f percentage (how???)
    for c in 1..=count {
        let op = rng.gen_range(0..10);
        if op == 0 {
            // flush
            println!("{:4}/{:4} Flush", c, count);
            let waiter = guest.flush(None)?;
            waiterlist.push(waiter);
            // Commit the current write log because we know this flush
            // will make it out on at least two DS, so any writes before this
            // point should also be persistent.
            // Note that we don't want to commit on every write, because
            // those writes might not make it if we have three dirty extents
            // and the one we choose could be the one that does not have
            // the write (no flush, no guarantee of persistence).
            ri.write_log.commit();
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
                    ri.write_log.update_wc(block_index + i);
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_log, ri.block_size);
                let data = Bytes::from(vec);

                println!(
                    "{:4}/{:4} Write at block {:5}, len:{:7}",
                    c,
                    count,
                    offset.value,
                    data.len()
                );
                let waiter = guest.write(offset, data)?;
                waiterlist.push(waiter);
            } else {
                // Read
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                println!(
                    "{:4}/{:4} Read  at block {:5}, len:{:7}",
                    c,
                    count,
                    offset.value,
                    data.len()
                );
                let waiter = guest.read(offset, data.clone())?;
                waiterlist.push(waiter);
            }
        }
    }
    println!("loop over {} waiters", waiterlist.len());
    for wa in waiterlist.iter_mut() {
        wa.block_wait()?;
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
    count: usize,
    ri: &mut RegionInfo,
) -> Result<()> {
    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    let mut waiterlist = Vec::new();
    // TODO: Let the user select the number of loops
    // TODO: Allow user to request r/w/f percentage (how???)
    for i in 1..=count {
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
                    ri.write_log.update_wc(block_index + i);
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_log, ri.block_size);
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
    if let Err(e) = verify_volume(guest, ri, false) {
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
    ri.write_log.update_wc(block_index);
    ri.write_log.update_wc(block_index + 1);

    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());
    let vec = fill_vec(block_index, 2, &ri.write_log, ri.block_size);
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
    match validate_vec(dl, block_index, &mut ri.write_log, ri.block_size, false)
    {
        ValidateStatus::Bad | ValidateStatus::InRange => {
            bail!("Span read verify failed");
        }
        ValidateStatus::Good => {}
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
        ri.write_log.update_wc(block_index);

        let vec = fill_vec(block_index, 1, &ri.write_log, ri.block_size);
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
        match validate_vec(
            dl,
            block_index,
            &mut ri.write_log,
            ri.block_size,
            false,
        ) {
            ValidateStatus::Bad | ValidateStatus::InRange => {
                bail!("Verify error at block:{}", block_index);
            }
            ValidateStatus::Good => {}
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
    println!("determine blocks for large io");
    let biggest_io_in_blocks = {
        let crucible_max_io =
            crucible_protocol::CrucibleEncoder::max_io_blocks(
                ri.block_size as usize,
            )?;

        if crucible_max_io < ri.total_blocks {
            crucible_max_io
        } else {
            println!(
                "Volume total blocks {} smaller than max IO blocks {}",
                ri.total_blocks, crucible_max_io,
            );
            ri.total_blocks
        }
    };

    println!(
        "Using {} as the largest single IO (in blocks)",
        biggest_io_in_blocks
    );
    let mut block_index = 0;
    while block_index < ri.total_blocks {
        let offset =
            Block::new(block_index as u64, ri.block_size.trailing_zeros());

        let next_io_blocks =
            if block_index + biggest_io_in_blocks > ri.total_blocks {
                ri.total_blocks - block_index
            } else {
                biggest_io_in_blocks
            };

        for i in 0..next_io_blocks {
            ri.write_log.update_wc(block_index + i);
        }

        let vec =
            fill_vec(block_index, next_io_blocks, &ri.write_log, ri.block_size);
        let data = Bytes::from(vec);

        println!(
            "IO at block:{}  size in blocks:{}",
            block_index, next_io_blocks
        );

        let mut waiter = guest.write(offset, data)?;
        waiter.block_wait()?;

        block_index += next_io_blocks;
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
    fn test_wl_update() {
        // Basic test to update write log
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.get_seed(1), 0);
        // Non zero size is not empty
        assert_eq!(write_log.is_empty(), false);
    }

    #[test]
    fn test_wl_update_rollover() {
        // Rollover of u8 at 250
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 249);
        assert_eq!(write_log.get_seed(0), 249);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 0);
        // Seed at zero does not mean the counter is zero
        assert_eq!(write_log.unwritten(0), false);
    }

    #[test]
    fn test_wl_empty() {
        // No size is empty
        let mut write_log = WriteLog::new(0);
        assert_eq!(write_log.is_empty(), true);
    }

    #[test]
    fn test_wl_update_commit() {
        // Write log returns highest after a commit, zero on one side
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(0);
        write_log.commit();
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.get_seed(1), 0);
    }

    #[test]
    fn test_wl_update_commit_rollover() {
        // Write log returns highest after a commit, but before u8 conversion
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 249);
        write_log.commit();
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 0);
    }

    #[test]
    fn test_wl_update_commit_2() {
        // Write log keeps going up after a commit
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(1);
        write_log.commit();
        write_log.update_wc(1);
        assert_eq!(write_log.get_seed(1), 2);
    }

    #[test]
    fn test_wl_commit_range() {
        // Verify that validate seed range returns true for all possible
        // values between min and max
        let bi = 1; // Block index
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(bi); // 1
        write_log.update_wc(bi); // 2
        write_log.commit();
        write_log.update_wc(bi); // 3
        write_log.update_wc(bi); // 4

        // 2 is the minimum
        assert_eq!(write_log.validate_seed_range(bi, 1, false), false);
        assert_eq!(write_log.validate_seed_range(bi, 2, false), true);
        assert_eq!(write_log.validate_seed_range(bi, 3, false), true);
        assert_eq!(write_log.validate_seed_range(bi, 4, false), true);
        assert_eq!(write_log.validate_seed_range(bi, 5, false), false);
    }

    #[test]
    fn test_wl_commit_range_vv() {
        // Test expected return values from Validate_vec when we are
        // working with ranges.  An InRange will change the expected value for
        // future calls.
        let bi = 1; // Block index
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(bi); // 1
        let vec_at_one = fill_vec(bi, 1, &write_log, bs);
        write_log.update_wc(bi); // 2
        write_log.commit();
        let vec_at_two = fill_vec(bi, 1, &write_log, bs);
        write_log.update_wc(bi); // 3
        write_log.update_wc(bi); // 4
        let vec_at_four = fill_vec(bi, 1, &write_log, bs);

        // Too low
        assert_eq!(
            validate_vec(vec_at_one, bi, &mut write_log, bs, true),
            ValidateStatus::Bad
        );
        // The current good. Ignore range
        assert_eq!(
            validate_vec(vec_at_four.clone(), bi, &mut write_log, bs, false),
            ValidateStatus::Good
        );
        // In range, but will change the future
        assert_eq!(
            validate_vec(vec_at_two.clone(), bi, &mut write_log, bs, true),
            ValidateStatus::InRange
        );
        // The new good. The previous InRange should now be Good.
        assert_eq!(
            validate_vec(vec_at_two, bi, &mut write_log, bs, true),
            ValidateStatus::Good
        );
        // The original good is now bad.
        assert_eq!(
            validate_vec(vec_at_four.clone(), bi, &mut write_log, bs, true),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_wl_commit_range_update() {
        // Verify that a validate_seed_range also updates the internal
        // max value to match the passed in value.
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(1);
        write_log.update_wc(1);
        write_log.commit();
        write_log.update_wc(1);
        write_log.update_wc(1);
        assert_eq!(write_log.get_seed(1), 4);
        // Once we call this, it becomes the new expected value
        assert_eq!(write_log.validate_seed_range(1, 3, true), true);
        assert_eq!(write_log.get_seed(1), 3);
    }
    #[test]
    fn test_wl_commit_range_update_min() {
        // Verify that a validate_seed_range also updates the internal
        // max value to match the passed in value.
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(1);
        write_log.update_wc(1);
        write_log.commit();
        write_log.update_wc(1);
        write_log.update_wc(1);
        assert_eq!(write_log.get_seed(1), 4);
        // Once we call this, it becomes the new expected value
        assert_eq!(write_log.validate_seed_range(1, 2, true), true);
        assert_eq!(write_log.get_seed(1), 2);
    }
    #[test]
    fn test_wl_commit_range_update_max() {
        // Verify that a validate_seed_range also updates the internal
        // max value to match the passed in value.
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(1);
        write_log.update_wc(1);
        write_log.commit();
        write_log.update_wc(1);
        write_log.update_wc(1);
        assert_eq!(write_log.get_seed(1), 4);
        // Once we call this, it becomes the new expected value
        assert_eq!(write_log.validate_seed_range(1, 4, true), true);
        // Still the same after the update
        assert_eq!(write_log.get_seed(1), 4);
    }

    #[test]
    fn test_wl_commit_range_rollover() {
        // validate seed range works if write log seed rolls over.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 248);
        write_log.commit();
        write_log.update_wc(0); // 249
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.validate_seed_range(0, 247, false), false);
        assert_eq!(write_log.validate_seed_range(0, 248, false), true);
        assert_eq!(write_log.validate_seed_range(0, 249, false), true);
        assert_eq!(write_log.validate_seed_range(0, 0, false), true);
        assert_eq!(write_log.validate_seed_range(0, 1, false), true);
        assert_eq!(write_log.validate_seed_range(0, 2, false), false);
    }

    #[test]
    fn test_wl_set() {
        // Write log returns highest after a set
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(1, 4);
        assert_eq!(write_log.get_seed(1), 4);
    }

    #[test]
    fn test_wl_is_zero() {
        // Write log returns true when unwritten
        let mut write_log = WriteLog::new(10);
        assert_eq!(write_log.unwritten(0), true);
        // Even after updating a different index
        write_log.update_wc(1);
        assert_eq!(write_log.unwritten(0), true);
    }

    #[test]
    fn test_read_compare() {
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(0);

        let vec = fill_vec(0, 1, &write_log, bs);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_commit() {
        // Verify that a commit will still return the highest value even
        // if we have not written to the other side of the WriteLog buffer.
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        write_log.update_wc(0);

        let vec = fill_vec(0, 1, &write_log, bs);
        write_log.commit();
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_fail() {
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 2);

        let vec = fill_vec(0, 1, &write_log, bs);
        write_log.update_wc(0);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_read_compare_fail_under() {
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 2);

        let vec = fill_vec(0, 1, &write_log, bs);
        write_log.set_wc(0, 1);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_read_compare_1() {
        // Block 1 works the same as block 0
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        let block_index = 1;
        write_log.update_wc(block_index);

        let vec = fill_vec(block_index, 1, &write_log, bs);
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_large() {
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let mut write_log = WriteLog::new(total_blocks);
        for i in 0..total_blocks {
            write_log.update_wc(i);
        }

        let vec = fill_vec(block_index, total_blocks, &write_log, bs);
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_large_fail() {
        // The last block in the data is wrong
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let mut write_log = WriteLog::new(total_blocks);
        for i in 0..total_blocks {
            write_log.update_wc(i);
        }
        let mut vec = fill_vec(block_index, total_blocks, &write_log, bs);
        let x = vec.len() - 1;
        vec[x] = 9;
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_read_compare_span() {
        // Verify a region larger than one block
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        let block_index = 1;
        write_log.set_wc(block_index, 1);
        write_log.set_wc(block_index + 1, 2);
        write_log.set_wc(block_index + 2, 3);

        let vec = fill_vec(block_index, 3, &write_log, bs);
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_span_fail() {
        // Verify a data mismatch in a region larger than one block
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        let block_index = 1;
        write_log.set_wc(block_index, 1);
        write_log.set_wc(block_index + 1, 2);
        write_log.set_wc(block_index + 2, 3);

        let mut vec = fill_vec(block_index, 3, &write_log, bs);
        /*
         * Replace the first value in the second block
         */
        vec[(bs + 1) as usize] = 9;
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_read_compare_span_fail_2() {
        // Verify the second value in the second block on a multi block
        // span will be discovered and reported.
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        let block_index = 1;
        write_log.set_wc(block_index, 1);
        write_log.set_wc(block_index + 1, 2);
        write_log.set_wc(block_index + 2, 3);

        let mut vec = fill_vec(block_index, 3, &write_log, bs);
        /*
         * Replace the second value in the second block
         */
        vec[(bs + 2) as usize] = 9;
        assert_eq!(
            validate_vec(vec, block_index, &mut write_log, bs, false),
            ValidateStatus::Bad
        );
    }

    #[test]
    fn test_read_compare_empty() {
        // A new array has no expectations.
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);

        let vec = fill_vec(0, 1, &write_log, bs);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }

    #[test]
    fn test_read_compare_empty_data() {
        // A new array has no expectations, even if the buffer has
        // data in it.
        let bs: u64 = 512;
        let mut write_log = WriteLog::new(10);
        let mut fill_log = WriteLog::new(10);
        fill_log.set_wc(1, 1);
        fill_log.set_wc(2, 2);
        fill_log.set_wc(3, 3);

        // This should seed our fill_vec with zeros, as we don't
        // have any expectations for block 0.
        let vec = fill_vec(0, 1, &fill_log, bs);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Good
        );

        // Now fill the vec as if it read data from an already written block.
        // We fake this by using block one's data (which should be 1).
        let vec = fill_vec(1, 1, &fill_log, bs);
        assert_eq!(
            validate_vec(vec, 0, &mut write_log, bs, false),
            ValidateStatus::Good
        );
    }
}
