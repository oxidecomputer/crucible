// Copyright 2022 Oxide Computer Company
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use clap::Parser;
use csv::WriterBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

mod cli;
mod protocol;
mod stats;
pub use stats::*;

use crucible::*;

/*
 * The various tests this program supports.
 */
/// Client: A Crucible Upstairs test program
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Parser, PartialEq)]
#[clap(name = "workload", term_width = 80)]
#[clap(about = "Workload the program will execute.", long_about = None)]
enum Workload {
    Balloon,
    Big,
    Biggest,
    Burst,
    /// Starts a CLI client
    Cli {
        /// Address the cli client will try to connect to
        #[clap(long, short, default_value = "0.0.0.0:5050", action)]
        attach: SocketAddr,
    },
    /// Start a server and listen on the given address and port
    CliServer {
        /// Address for the cliserver to listen on
        #[clap(long, short, default_value = "0.0.0.0", action)]
        listen: IpAddr,
        /// Port for the cliserver to listen on
        #[clap(long, short, default_value = "5050", action)]
        port: u16,
    },
    Deactivate,
    Demo,
    Dep,
    Dirty,
    Fill {
        /// Don't do the verify step after filling the region.
        #[clap(long, action)]
        skip_verify: bool,
    },
    Generic,
    Nothing,
    One,
    /// Run the perf test, random writes, then random reads
    Perf {
        /// Size in blocks of each IO
        #[clap(long, default_value = "1", action)]
        io_size: usize,
        /// Number of outstanding IOs at the same time.
        #[clap(long, default_value = "1", action)]
        io_depth: usize,
        /// Output file for IO times
        #[clap(long, global = true, name = "PERF", action)]
        perf_out: Option<PathBuf>,
        /// Number of read test loops to do.
        #[clap(long, default_value = "2", action)]
        read_loops: usize,
        /// Number of write test loops to do.
        #[clap(long, default_value = "2", action)]
        write_loops: usize,
    },
    Rand,
    Repair,
    Span,
    Verify,
}

#[derive(Debug, Parser)]
#[clap(name = "client", term_width = 80)]
#[clap(about = "A Crucible upstairs test client", long_about = None)]
pub struct Opt {
    ///  For tests that support it, pass this count value for the number
    ///  of loops the test should do.
    #[clap(short, long, global = true, default_value = "0", action)]
    count: usize,

    #[clap(
        short,
        long,
        global = true,
        default_value = "127.0.0.1:9000",
        action
    )]
    target: Vec<SocketAddr>,

    #[clap(subcommand)]
    workload: Workload,

    ///  This allows the Upstairs to run in a mode where it will not
    ///  always submit new work to downstairs when it first receives
    ///  it.  This is for testing dependencies and should not be
    ///  used in production.  Passing args like this to the upstairs
    ///  may not be the best way to test, but until we have something
    ///  better... XXX
    #[clap(long, global = true, action)]
    lossy: bool,

    ///  quit after all crucible work queues are empty.
    #[clap(short, global = true, long, action)]
    quit: bool,

    #[clap(short, global = true, long, action)]
    key: Option<String>,

    #[clap(short, global = true, long, default_value = "0", action)]
    gen: u64,

    /// For the verify test, if this option is included we will allow
    /// the write log range of data to pass the verify_volume check.
    #[clap(long, global = true, action)]
    range: bool,

    /// Retry for activate, as long as it takes.  If we pass this arg, the
    /// test will retry the initial activate command as long as it takes.
    #[clap(long, global = true, action)]
    retry_activate: bool,

    /// In addition to any tests, verify the volume on startup.
    /// This only has value if verify_in is also set.
    #[clap(long, global = true, action)]
    verify: bool,

    /// For tests that support it, load the expected write count from
    /// the provided file.  The addition of a --verify option will also
    /// have the test verify what it imports from the file is valid.
    #[clap(long, global = true, name = "INFILE", action)]
    verify_in: Option<PathBuf>,

    ///  For tests that support it, save the write count into the
    ///  provided file.
    #[clap(long, global = true, name = "FILE", action)]
    verify_out: Option<PathBuf>,

    // TLS options
    #[clap(long, action)]
    cert_pem: Option<String>,
    #[clap(long, action)]
    key_pem: Option<String>,
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    /// IP:Port for the upstairs control http server
    #[clap(long, global = true, action)]
    control: Option<SocketAddr>,

    /// How long to wait before the auto flush check fires
    #[clap(long, global = true, action)]
    flush_timeout: Option<u32>,

    /// IP:Port for the Oximeter register address, which is Nexus.
    #[clap(long, global = true, default_value = "127.0.0.1:12221", action)]
    metric_register: SocketAddr,

    /// IP:Port for the Oximeter listen address
    #[clap(long, global = true, default_value = "127.0.0.1:55443", action)]
    metric_collect: SocketAddr,

    /// Spin up a dropshot endpoint and serve metrics from it.
    /// This will use the values in metric-register and metric-collect
    #[clap(long, global = true, action)]
    metrics: bool,

    /// A UUID to use for the upstairs.
    #[clap(long, global = true, action)]
    uuid: Option<Uuid>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();

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
#[allow(clippy::derive_partial_eq_without_eq)]
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
async fn get_region_info(
    guest: &Arc<Guest>,
) -> Result<RegionInfo, CrucibleError> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let block_size = guest.get_block_size().await?;
    let extent_size = guest.query_extent_size().await?;
    let total_size = guest.total_size().await?;
    let total_blocks = (total_size / block_size) as usize;

    /*
     * Limit the max IO size (in blocks) to be 1MiB or the size
     * of the volume, whichever is smaller
     */
    const MAX_IO_BYTES: usize = 1024 * 1024;
    let mut max_block_io = MAX_IO_BYTES / block_size as usize;
    if total_blocks < max_block_io as usize {
        max_block_io = total_blocks as usize;
    }

    println!(
        "Region: es:{} ec:{} bs:{}  ts:{}  tb:{}  max_io:{} or {}",
        extent_size.value,
        total_blocks as u64 / extent_size.value,
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
#[allow(clippy::derive_partial_eq_without_eq)]
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
            println!(
                "Update block {} to {} (min:{} max:{} res:{}",
                index, new_max, min, max, res
            );
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

async fn load_write_log(
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
        if let Err(e) = verify_volume(guest, ri, false).await {
            bail!("Initial volume verify failed: {:?}", e)
        }
    }
    Ok(())
}

/**
 * This is an example Crucible client.
 * Here we make use of the interfaces that Crucible exposes.
 */
#[tokio::main]
async fn main() -> Result<()> {
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

    let up_uuid = opt.uuid.unwrap_or_else(Uuid::new_v4);

    let crucible_opts = CrucibleOpts {
        id: up_uuid,
        target: opt.target,
        lossy: opt.lossy,
        flush_timeout: opt.flush_timeout,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
        read_only: false,
    };

    /*
     * If just want the cli, then start that after our runtime.  The cli
     * does not need upstairs started, as that should happen in the
     * cli-server code.
     */
    if let Workload::Cli { attach } = opt.workload {
        cli::start_cli_client(attach).await?;
        return Ok(());
    }

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let guest = Arc::new(Guest::new());

    let pr;
    if opt.metrics {
        // If metrics are desired, we create and register the server
        // first. Once we have the server, we clone the ProducerRegister
        // so we can pass that on to the upstairs.
        // Finally, spin out a task with the server to provide the endpoint
        // so metrics can be collected by Oximeter.
        println!(
            "Creating a metric collect endpoint at {}",
            opt.metric_collect
        );
        match client_oximeter(opt.metric_collect, opt.metric_register).await {
            Err(e) => {
                println!("Failed to register with Oximeter {:?}", e);
                pr = None;
            }
            Ok(server) => {
                pr = Some(server.registry().clone());
                // Now Spawn the metric endpoint.
                tokio::spawn(async move {
                    server.serve_forever().await.unwrap();
                });
            }
        }
    } else {
        pr = None;
    }

    let _join_handle =
        up_main(crucible_opts, opt.gen, None, guest.clone(), pr).await?;
    println!("Crucible runtime is spawned");

    if let Workload::CliServer { listen, port } = opt.workload {
        cli::start_cli_server(
            &guest,
            listen,
            port,
            opt.verify_in,
            opt.verify_out,
        )
        .await?;
        return Ok(());
    }

    if opt.retry_activate {
        while let Err(e) = guest.activate_with_gen(opt.gen).await {
            println!("Activate returns: {:#}  Retrying", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
        println!("Activate successful");
    } else {
        guest.activate_with_gen(opt.gen).await?;
    }

    println!("Wait for a query_work_queue command to finish before sending IO");
    guest.query_work_queue().await?;

    /*
     * Build the region info struct that all the tests will use.
     * This includes importing and verifying from a write log, if requested.
     */
    let mut region_info = match get_region_info(&guest).await {
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
        load_write_log(&guest, &mut region_info, verify_in, verify).await?;
    }

    /*
     * Call the function for the workload option passed from the command
     * line.
     */
    match opt.workload {
        Workload::Balloon => {
            println!("Run balloon test");
            balloon_workload(&guest, &mut region_info).await?;
        }
        Workload::Big => {
            println!("Run big test");
            big_workload(&guest, &mut region_info).await?;
        }
        Workload::Biggest => {
            println!("Run biggest IO test");
            biggest_io_workload(&guest, &mut region_info).await?;
        }
        Workload::Burst => {
            println!("Run burst test (demo in a loop)");
            burst_workload(&guest, 460, 190, &mut region_info, &opt.verify_out)
                .await?;
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
            deactivate_workload(&guest, count, &mut region_info, opt.gen)
                .await?;
        }
        Workload::Demo => {
            println!("Run Demo test");
            let count = {
                if opt.count == 0 {
                    300
                } else {
                    opt.count
                }
            };
            /*
             * The count provided here should be greater than the flow
             * control limit if we wish to test flow control.  Also, set
             * lossy on a downstairs otherwise it will probably keep up.
             */
            demo_workload(&guest, count, &mut region_info).await?;
        }
        Workload::Dep => {
            println!("Run dep test");
            dep_workload(&guest, &mut region_info).await?;
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

            dirty_workload(&guest, &mut region_info, count).await?;

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

        Workload::Fill { skip_verify } => {
            println!("Fill test");
            fill_workload(&guest, &mut region_info, skip_verify).await?;
        }

        Workload::Generic => {
            let count = {
                if opt.count == 0 {
                    500
                } else {
                    opt.count
                }
            };

            generic_workload(&guest, count, &mut region_info).await?;
        }

        Workload::One => {
            println!("One test");
            one_workload(&guest, &mut region_info).await?;
        }
        Workload::Perf {
            io_size,
            io_depth,
            perf_out,
            read_loops,
            write_loops,
        } => {
            println!("Perf test");
            let ops = {
                if opt.count == 0 {
                    5000_usize
                } else {
                    opt.count
                }
            };

            // NOTICE: The graphing code REQUIRES the data to be in a
            // specific format, where the first 7 columns are as described
            // below, and the 8 through whatever are the data samples from
            // the performance test.
            let mut opt_wtr = None;
            let mut wtr;
            if let Some(perf_out) = perf_out {
                wtr = WriterBuilder::new()
                    .flexible(true)
                    .from_path(perf_out)
                    .unwrap();
                wtr.serialize((
                    "type",
                    "total_time_ns",
                    "io_depth",
                    "io_size",
                    "count",
                    "es",
                    "ec",
                    "times",
                ))?;
                opt_wtr = Some(&mut wtr);
            }

            // The header for all perf tests
            perf_header();
            perf_workload(
                &guest,
                &mut region_info,
                &mut opt_wtr,
                ops,
                io_depth,
                io_size,
                write_loops,
                read_loops,
            )
            .await?;
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
                    tokio::time::sleep(tokio::time::Duration::from_secs(10))
                        .await;
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
            rand_workload(&guest, count, &mut region_info).await?;
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
            repair_workload(&guest, count, &mut region_info).await?;
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
            span_workload(&guest, &mut region_info).await?;
        }
        Workload::Verify => {
            /*
             * For verify, if -q, we quit right away.  If we don't quit, then
             * this turns into a read verify loop, sleep for some duration
             * and then re-check the volume.
             */
            if let Err(e) =
                verify_volume(&guest, &mut region_info, opt.range).await
            {
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
                    tokio::time::sleep(tokio::time::Duration::from_secs(10))
                        .await;
                    if let Err(e) =
                        verify_volume(&guest, &mut region_info, opt.range).await
                    {
                        bail!("Volume verify failed: {:?}", e)
                    }
                    let mut wc = guest.show_work().await?;
                    while wc.up_count + wc.ds_count > 0 {
                        println!("Waiting for all work to be completed");
                        tokio::time::sleep(tokio::time::Duration::from_secs(
                            10,
                        ))
                        .await;
                        wc = guest.show_work().await?;
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
        let wc = guest.show_work().await?;
        println!("CLIENT: Up:{} ds:{}", wc.up_count, wc.ds_count);
        if opt.quit && wc.up_count + wc.ds_count == 0 {
            println!("CLIENT: All crucible jobs finished, exiting program");
            return Ok(());
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
    }
}

/*
 * Read/Verify every possible block, up to 100 blocks at a time.
 * If range is set to true, we allow the write log to consider any valid
 * value for a block since the last commit was called.
 */
async fn verify_volume(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    range: bool,
) -> Result<()> {
    assert_eq!(ri.write_log.len(), ri.total_blocks);

    println!(
        "Read and Verify all blocks (0..{} range:{})",
        ri.total_blocks, range
    );

    let pb = ProgressBar::new(ri.total_blocks as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .unwrap()
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
        guest.read(offset, data.clone()).await?;

        let dl = data.as_vec().await.to_vec();
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
                    "Error in block range {} -> {}",
                    block_index,
                    block_index + next_io_blocks
                );
            }
            ValidateStatus::InRange => {
                if range {
                    {}
                } else {
                    pb.finish_with_message("Error");
                    bail!("Error at block {}", block_index);
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
#[allow(clippy::derive_partial_eq_without_eq)]
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
            guest.write(offset, data).await?;
            guest.flush(None).await?;

            let length: usize = size * ri.block_size as usize;
            let vec: Vec<u8> = vec![255; length];
            let data = crucible::Buffer::from_vec(vec);
            guest.read(offset, data.clone()).await?;

            let dl = data.as_vec().await.to_vec();
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

    verify_volume(guest, ri, false).await?;
    Ok(())
}

/*
 * Write then read (and verify) to every possible block.
 */
async fn fill_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    skip_verify: bool,
) -> Result<()> {
    let pb = ProgressBar::new(ri.total_blocks as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .unwrap()
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

        guest.write(offset, data).await?;

        block_index += next_io_blocks;
        pb.set_position(block_index as u64);
    }

    guest.flush(None).await?;
    pb.finish();

    if !skip_verify {
        verify_volume(guest, ri, false).await?;
    }
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

    let count_width = count.to_string().len();
    for c in 1..=count {
        let op = rng.gen_range(0..10);
        if op == 0 {
            // flush
            println!(
                "{:>0width$}/{:>0width$} FLUSH",
                c,
                count,
                width = count_width,
            );
            guest.flush(None).await?;
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
                    "{:>0width$}/{:>0width$} WRITE {}:{}",
                    c,
                    count,
                    offset.value,
                    data.len(),
                    width = count_width,
                );
                guest.write(offset, data).await?;
            } else {
                // Read (+ verify)
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                println!(
                    "{:>0width$}/{:>0width$} READ  {}:{}",
                    c,
                    count,
                    offset.value,
                    data.len(),
                    width = count_width,
                );
                guest.read(offset, data.clone()).await?;

                let dl = data.as_vec().await.to_vec();
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
    let mut futureslist = Vec::new();

    let size = 1;
    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let block_max = ri.total_blocks - size + 1;
    let count_width = count.to_string().len();
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
            "[{:>0width$}/{:>0width$}] Write at block {}, len:{}",
            c,
            count,
            offset.value,
            data.len(),
            width = count_width,
        );

        let future = guest.write(offset, data);
        futureslist.push(future);
    }
    println!("loop over {} futures", futureslist.len());
    crucible::join_all(futureslist).await?;
    Ok(())
}

/*
 * Print the perf header.
 */
pub fn perf_header() {
    println!(
        "{:>8} {:7} {:5} {:4} {:>7} {:>7} {:>7} {:>7} {:>8} {:>5} {:>5}",
        "TEST",
        "SECONDS",
        "COUNT",
        "DPTH",
        "IOPS",
        "MEAN",
        "P95",
        "P99",
        "MAX",
        "ES",
        "EC",
    );
}

/*
 * Take the Vec of Durations for IOs and write it out in CSV format using
 * the provided CSV Writer.
 */
#[allow(clippy::too_many_arguments)]
pub fn perf_csv(
    wtr: &mut csv::Writer<File>,
    msg: &str,
    count: usize,
    io_depth: usize,
    io_size: usize,
    duration: Duration,
    iotimes: Vec<Duration>,
    es: u64,
    ec: u64,
) {
    // Convert all Durations to u64 nanoseconds.
    let times = iotimes
        .iter()
        .map(|x| (x.as_secs() as u64 * 100000000) + x.subsec_nanos() as u64)
        .collect::<Vec<u64>>();

    let time_in_nsec =
        duration.as_secs() as u64 * 100000000 + duration.subsec_nanos() as u64;

    wtr.serialize(Record {
        label: msg.to_string(),
        total_time: time_in_nsec,
        io_depth,
        io_size,
        count,
        es,
        ec,
        time: times,
    })
    .unwrap();
    wtr.flush().unwrap();
}

// Percentile
// Given a SORTED vec of f32's (I'm trusting you to provide that),
// and a value between 1 and 99 (the desired percentile),
// determine which index (or which indexes to average) contain our desired
// percentile.
// Once we have the value at our index (or the average of two values at the
// desired indices), return that to the caller.
//
// Remember, the array index is one less (zero-based index)
fn percentile(times: &[f32], perc: u8) -> Result<f32> {
    if times.is_empty() {
        bail!("Array for percentile too short");
    }
    if perc == 0 || perc >= 100 {
        bail!("Requested percentile not: 0 < {} < 100", perc);
    }

    let position = times.len() as f32 * (perc as f32 / 100.0);

    if position == position.trunc() {
        // Our position is a whole number.
        // We use the rounded up position as our second index because the
        // array index is zero based.
        let index_two = position.ceil() as usize;
        let index_one = index_two - 1;

        Ok((times[index_one] + times[index_two]) / 2.0)
    } else {
        // Our position is not an integer, so round up to get the correct
        // position for our percentile.  However, since we need to subtract
        // one to get the zero-based index, we can just round down here.
        // This is the same as rounding up, then subtracting one.
        let index = position.trunc() as usize;
        Ok(times[index])
    }
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
    es: u64,
    ec: u64,
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
        "{:>8} {:>7.2} {:5} {:4} {:>7.2} {:.5} {:.5} {:.5} {:8.5} {:>5} {:>5}",
        msg,
        time_f,
        count,
        io_depth,
        count as f32 / time_f,
        statistical::mean(&times),
        percentile(&times, 95).unwrap(),
        percentile(&times, 99).unwrap(),
        times.last().unwrap(),
        es,
        ec,
    );
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    label: String,
    total_time: u64,
    io_depth: usize,
    io_size: usize,
    count: usize,
    es: u64,
    ec: u64,
    time: Vec<u64>,
}
/**
 * A simple IO test in two stages: 100% random writes, then 100% random
 * reads. The caller can select:
 * io_depth:      The number of outstanding IOs issued at a time
 * blocks_per_io: The size of each io (in multiple of block size).
 * count:         The number of loops to perform for each test (all IOs
 *                in io_depth are considered as a single loop).
 * write_loop:    The number of times to do the 100% write loop.
 * read_loop:     The number of times to do the 100% read loop.
 *
 * A summary is printed at the end of each stage.
 */
#[allow(clippy::too_many_arguments)]
async fn perf_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    wtr: &mut Option<&mut csv::Writer<File>>,
    count: usize,
    io_depth: usize,
    blocks_per_io: usize,
    write_loop: usize,
    read_loop: usize,
) -> Result<()> {
    // Before we start, make sure the work queues are empty.
    loop {
        let wc = guest.query_work_queue().await?;
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

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

    let es = ri.extent_size.value;
    let ec = ri.total_blocks as u64 / es;

    // To make a random block offset modulus, we take the total
    // block number and subtract the IO size in blocks.
    let offset_mod =
        rng.gen::<u64>() % (ri.total_blocks - blocks_per_io) as u64;

    for _ in 0..write_loop {
        let mut wtime = Vec::with_capacity(count);
        let big_start = Instant::now();
        for _ in 0..count {
            let burst_start = Instant::now();
            let mut write_futures = Vec::with_capacity(io_depth);

            for write_buffer in write_buffers.iter().take(io_depth) {
                let offset: u64 = rng.gen::<u64>() % offset_mod;
                let future = guest.write_to_byte_offset(
                    offset * ri.block_size,
                    write_buffer.clone(),
                );
                write_futures.push(future);
            }

            crucible::join_all(write_futures).await?;
            wtime.push(burst_start.elapsed());
        }
        let big_end = big_start.elapsed();

        guest.flush(None).await?;
        perf_summary(
            "rwrites",
            count,
            io_depth,
            wtime.clone(),
            big_end,
            es,
            ec,
        );
        if let Some(wtr) = wtr {
            perf_csv(
                wtr,
                "rwrite",
                count,
                io_depth,
                blocks_per_io,
                big_end,
                wtime.clone(),
                es,
                ec,
            );
        }

        // Before we loop or end, make sure the work queues are empty.
        loop {
            let wc = guest.query_work_queue().await?;
            if wc.up_count + wc.ds_count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    let mut rtime = Vec::with_capacity(count);
    for _ in 0..read_loop {
        let big_start = Instant::now();
        for _ in 0..count {
            let burst_start = Instant::now();
            let mut read_futures = Vec::with_capacity(io_depth);

            for read_buffer in read_buffers.iter().take(io_depth) {
                let offset: u64 = rng.gen::<u64>() % offset_mod;
                let future = guest.read_from_byte_offset(
                    offset * ri.block_size,
                    read_buffer.clone(),
                );
                read_futures.push(future);
            }
            crucible::join_all(read_futures).await?;
            rtime.push(burst_start.elapsed());
        }
        let big_end = big_start.elapsed();

        perf_summary("rreads", count, io_depth, rtime.clone(), big_end, es, ec);

        if let Some(wtr) = wtr {
            perf_csv(
                wtr,
                "rread",
                count,
                io_depth,
                blocks_per_io,
                big_end,
                rtime.clone(),
                es,
                ec,
            );
        }

        guest.flush(None).await?;

        // Before we finish, make sure the work queues are empty.
        loop {
            let wc = guest.query_work_queue().await?;
            if wc.up_count + wc.ds_count == 0 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }
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
    ri.write_log.update_wc(block_index);

    let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
    let data = Bytes::from(vec);

    println!("Write at block {:5}, len:{:7}", offset.value, data.len());

    guest.write(offset, data).await?;

    let length: usize = size * ri.block_size as usize;
    let vec: Vec<u8> = vec![255; length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Read  at block {:5}, len:{:7}", offset.value, data.len());
    guest.read(offset, data.clone()).await?;

    let dl = data.as_vec().await.to_vec();
    match validate_vec(dl, block_index, &mut ri.write_log, ri.block_size, false)
    {
        ValidateStatus::Bad | ValidateStatus::InRange => {
            bail!("Error at {}", block_index);
        }
        ValidateStatus::Good => {}
    }

    println!("Flush");
    guest.flush(None).await?;

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
    let count_width = count.to_string().len();
    for c in 1..=count {
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: run rand test",
            c,
            count,
            width = count_width
        );
        generic_workload(guest, 20, ri).await?;
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: Now disconnect",
            c,
            count,
            width = count_width
        );
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: Now disconnect wait",
            c,
            count,
            width = count_width
        );
        guest.deactivate().await?;
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: Now disconnect done.",
            c,
            count,
            width = count_width
        );
        let wc = guest.show_work().await?;
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: Up:{} ds:{}",
            c,
            count,
            wc.up_count,
            wc.ds_count,
            width = count_width
        );
        let mut retry = 1;
        gen += 1;
        while let Err(e) = guest.activate_with_gen(gen).await {
            println!(
                "{:>0width$}/{:>0width$}, Retry:{} activate {:?}",
                c,
                count,
                retry,
                e,
                width = count_width
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            if retry > 100 {
                bail!("Too many retries {} for activate", retry);
            }
            retry += 1;
        }
    }
    println!("One final");
    generic_workload(guest, 20, ri).await?;

    println!("final verify");
    if let Err(e) = verify_volume(guest, ri, false).await {
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

    let count_width = count.to_string().len();
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
            "{:>0width$}/{:>0width$} IO at block {:5}, len:{:7}",
            c,
            count,
            offset.value,
            data.len(),
            width = count_width,
        );
        guest.write(offset, data).await?;

        guest.flush(None).await?;

        let length: usize = size * ri.block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        guest.read(offset, data.clone()).await?;

        let dl = data.as_vec().await.to_vec();
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

    if let Err(e) = verify_volume(guest, ri, false).await {
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
    let count_width = count.to_string().len();
    for c in 1..=count {
        demo_workload(guest, demo_count, ri).await?;
        let mut wc = guest.show_work().await?;
        while wc.up_count + wc.ds_count != 0 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            println!(
                "{:>0width$}/{:>0width$} Up:{} ds:{}",
                c,
                count,
                wc.up_count,
                wc.ds_count,
                width = count_width
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
            wc = guest.show_work().await?;
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
            "{:>0width$}/{:>0width$}: 2 second pause, then another test loop",
            c,
            count,
            width = count_width
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
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
    // TODO: Allow user to request r/w/f percentage (how???)
    // We want at least one write, otherwise there will be nothing to
    // repair.
    let mut one_write = false;
    // These help the printlns use the minimum white space
    let count_width = count.to_string().len();
    let block_width = ri.total_blocks.to_string().len();
    let size_width = (10 * ri.block_size).to_string().len();
    for c in 1..=count {
        let op = rng.gen_range(0..10);
        // Make sure the last few commands are not a flush
        if c + 3 < count && op == 0 {
            // flush
            println!(
                "{:>0width$}/{:>0width$} Flush",
                c,
                count,
                width = count_width,
            );
            guest.flush(None).await?;
            // Commit the current write log because we know this flush
            // will make it out on at least two DS, so any writes before this
            // point should also be persistent.
            // Note that we don't want to commit on every write, because
            // those writes might not make it if we have three dirty extents
            // and the one we choose could be the one that does not have
            // the write (no flush, no guarantee of persistence).
            ri.write_log.commit();
            // Make sure a write comes next.
            one_write = false;
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

            if !one_write || op <= 4 {
                // Write
                one_write = true;
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    ri.write_log.update_wc(block_index + i);
                }

                let vec =
                    fill_vec(block_index, size, &ri.write_log, ri.block_size);
                let data = Bytes::from(vec);

                println!(
                    "{:>0width$}/{:>0width$} Write \
                    block {:>bw$}  len {:>sw$}  data:{:>3}",
                    c,
                    count,
                    offset.value,
                    data.len(),
                    data[1],
                    width = count_width,
                    bw = block_width,
                    sw = size_width,
                );
                guest.write(offset, data).await?;
            } else {
                // Read
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                println!(
                    "{:>0width$}/{:>0width$} Read  \
                    block {:>bw$}  len {:>sw$}",
                    c,
                    count,
                    offset.value,
                    data.len(),
                    width = count_width,
                    bw = block_width,
                    sw = size_width,
                );
                guest.read(offset, data.clone()).await?;
            }
        }
    }
    guest.show_work().await?;
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

    // Because this workload issues a bunch of IO all at the same time,
    // we can't be sure the order will be preserved for our IOs.
    // We take the state of the volume now as our minimum, and verify
    // that the read at the end of this loop finds some value between
    // what it is now and what it is at the end of test.
    ri.write_log.commit();

    let mut futureslist = Vec::new();
    // TODO: Let the user select the number of loops
    // TODO: Allow user to request r/w/f percentage (how???)
    for _ in 1..=count {
        let op = rng.gen_range(0..10);
        if op == 0 {
            // flush
            let future = guest.flush(None);
            futureslist.push(future);
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

                let future = guest.write(offset, data);
                futureslist.push(future);
            } else {
                // Read
                let length: usize = size * ri.block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);

                let future = guest.read(offset, data.clone());
                futureslist.push(future);
            }
        }
    }
    let mut wc = WQCounts {
        up_count: 0,
        ds_count: 0,
    };
    println!(
        "Submit work and wait for {} jobs to finish",
        futureslist.len()
    );
    crucible::join_all(futureslist).await?;

    /*
     * Continue loping until all downstairs jobs finish also.
     */
    println!("All submitted jobs completed, waiting for downstairs");
    while wc.up_count + wc.ds_count > 0 {
        wc = guest.show_work().await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
    println!("All downstairs jobs completed.");
    if let Err(e) = verify_volume(guest, ri, true).await {
        bail!("Final volume verify failed: {:?}", e)
    }

    // Commit the current state as the new minimum for any future tests.
    ri.write_log.commit();

    Ok(())
}

/*
 * This is a test workload that generates a single write spanning an extent
 * then will try to read the same.
 */
async fn span_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
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
    guest.write(offset, data).await?;

    println!("Sending a flush");
    guest.flush(None).await?;

    let length: usize = 2 * ri.block_size as usize;
    let vec: Vec<u8> = vec![99; length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Sending a read spanning two extents");
    guest.read(offset, data.clone()).await?;

    let dl = data.as_vec().await.to_vec();
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
async fn big_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
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

        guest.write(offset, data).await?;

        guest.flush(None).await?;

        let length: usize = ri.block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        guest.read(offset, data.clone()).await?;

        let dl = data.as_vec().await.to_vec();
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
    guest.show_work().await?;

    Ok(())
}

async fn biggest_io_workload(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
) -> Result<()> {
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

        guest.write(offset, data).await?;

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
 * TODO: Make this test use the global write count, but remember, async.
 */
async fn dep_workload(guest: &Arc<Guest>, ri: &mut RegionInfo) -> Result<()> {
    let final_offset = ri.total_size - ri.block_size;

    let mut my_offset: u64 = 0;
    for my_count in 1..150 {
        let mut futureslist = Vec::new();

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
                let future = guest.write_to_byte_offset(my_offset, data);
                futureslist.push(future);
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
                let future = guest.read_from_byte_offset(my_offset, data);
                futureslist.push(future);
            }
        }

        guest.show_work().await?;

        // The final flush is to help prevent the pause that we get when the
        // last command is a write or read and we have to wait x seconds for the
        // flush check to trigger.
        println!("Loop:{} send a final flush and wait", my_count);
        let flush_future = guest.flush(None);
        futureslist.push(flush_future);

        println!("Loop:{} loop over {} futures", my_count, futureslist.len());
        crucible::join_all(futureslist).await?;
        println!("Loop:{} all futures done", my_count);
        guest.show_work().await?;
    }

    println!("dep test done");
    Ok(())
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
        assert!(!write_log.is_empty());
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
        assert!(!write_log.unwritten(0));
    }

    #[test]
    fn test_wl_empty() {
        // No size is empty
        let mut write_log = WriteLog::new(0);
        assert!(write_log.is_empty());
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

        // 2 is the minimum, less than should fail
        assert!(!write_log.validate_seed_range(bi, 1, false));
        assert!(write_log.validate_seed_range(bi, 2, false));
        assert!(write_log.validate_seed_range(bi, 3, false));
        assert!(write_log.validate_seed_range(bi, 4, false));
        // More than 4 should fail
        assert!(!write_log.validate_seed_range(bi, 5, false));
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
            validate_vec(vec_at_four, bi, &mut write_log, bs, true),
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
        assert!(write_log.validate_seed_range(1, 3, true));
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
        assert!(write_log.validate_seed_range(1, 2, true));
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
        assert!(write_log.validate_seed_range(1, 4, true));
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
        assert!(!write_log.validate_seed_range(0, 247, false));
        assert!(write_log.validate_seed_range(0, 248, false));
        assert!(write_log.validate_seed_range(0, 249, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
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
        assert!(write_log.unwritten(0));
        // Even after updating a different index
        write_log.update_wc(1);
        assert!(write_log.unwritten(0));
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

    #[test]
    fn test_95_small() {
        // Test of one element
        let fv = vec![10.0];
        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 10.0);
    }

    #[test]
    fn test_perc_bad_perc() {
        // Should fail on a bad percentile value
        let fv = vec![10.0];
        let res = percentile(&fv, 0);
        assert!(res.is_err());
    }

    #[test]
    fn test_perc_bad_big_perc() {
        // Should fail on a bad percentile value
        let fv = vec![10.0];
        let res = percentile(&fv, 100);
        assert!(res.is_err());
    }

    #[test]
    fn test_95_2() {
        // Determine the 95th percentile value with 2 elements
        // We must round up.
        let fv = vec![10.0, 20.0];
        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 20.0);
    }

    #[test]
    fn test_95_10() {
        // Determine the 95th percentile value with 10 elements
        // We must round up.
        let fv = vec![1.1, 2.2, 3.3, 4.4, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 10.0);
    }
    #[test]
    fn test_95_20() {
        // Determine the 95th percentile value with 20 elements
        // There is a whole number position for this array, so we must
        // return the average of two elements.
        let fv = vec![
            1.1, 2.2, 3.3, 4.4, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
            13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
        ];

        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 19.5);
    }
    #[test]
    fn test_95_21() {
        // Determine the 95th percentile value with 21 elements
        let fv = vec![
            1.1, 2.2, 3.3, 4.4, 5.0, 6.0, 7.0, 8.0, 9.0, 8.0, 10.0, 11.0, 12.0,
            13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
        ];

        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 19.0);
    }
    #[test]
    fn test_perc_mixed() {
        // Determine the 95th, 90th, and 20th percentile values
        let fv = vec![
            43.0, 54.0, 56.0, 61.0, 62.0, 66.0, 68.0, 69.0, 69.0, 70.0, 71.0,
            72.0, 77.0, 78.0, 79.0, 85.0, 87.0, 88.0, 89.0, 93.0, 95.0, 96.0,
            98.0, 99.0, 99.4,
        ];

        let pp = percentile(&fv, 95).unwrap();
        assert_eq!(pp, 99.0);
        let pp = percentile(&fv, 90).unwrap();
        assert_eq!(pp, 98.0);
        let pp = percentile(&fv, 20).unwrap();
        assert_eq!(pp, 64.0);
    }
}
