// Copyright 2023 Oxide Computer Company
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use clap::Parser;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use human_bytes::human_bytes;
use indicatif::{ProgressBar, ProgressStyle};
use oximeter::types::ProducerRegistry;
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use serde::{Deserialize, Serialize};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use slog::{info, o, warn, Logger};
use std::fmt;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod cli;
mod protocol;
mod stats;
pub use stats::*;

use crucible::volume::{VolumeBuilder, VolumeInfo};
use crucible::*;
use crucible_client_types::RegionExtentInfo;
use crucible_protocol::CRUCIBLE_MESSAGE_VERSION;
use dsc_client::{types::DownstairsState, Client};
use repair_client::Client as repair_client;

/*
 * The various tests this program supports.
 */
/// Client: A Crucible Upstairs test program
#[allow(clippy::derive_partial_eq_without_eq, clippy::upper_case_acronyms)]
#[derive(Debug, Parser)]
#[clap(name = "workload", term_width = 80)]
#[clap(about = "Workload the program will execute.", long_about = None)]
enum Workload {
    Balloon,
    Big,
    Biggest,
    /// Send many random writes, then report how long a single read takes
    Bufferbloat {
        #[clap(flatten)]
        cfg: BufferbloatWorkload,
    },
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
        #[clap(long, short, default_value_t = 5050, action)]
        port: u16,
    },
    Deactivate,
    Demo,
    Dep,
    Dirty,
    /// Write to one random block in every extent, then flush.
    FastFill,
    Fill {
        /// Don't do the verify step after filling the disk.
        #[clap(long, action)]
        skip_verify: bool,
    },
    Generic,
    /// Do random read and flush IOs
    GenericRead,
    Nothing,
    One,
    /// Measure performance with a random read workload
    RandRead {
        #[clap(flatten)]
        cfg: RandReadWriteWorkload,
    },
    /// Measure performance with a random write workload
    RandWrite {
        #[clap(flatten)]
        cfg: RandReadWriteWorkload,
    },
    /// Run IO, and as soon as we get a final ACK, drop the volume to
    /// see if we can leave IOs outstanding on one of the downstairs.
    /// This test works best if one of the downstairs is running with
    /// lossy option set, which will make it go slower than the others.
    Repair,
    /// Test the downstairs replay path.
    /// Stop a downstairs, then run some IO, then start that downstairs back
    /// up.  Verify all IO to all downstairs finishes.
    /// This test requires a dsc server to control the downstairs.
    Replay,
    /// Test the downstairs replacement path.
    /// Run IO to the upstairs, then replace a downstairs, then run
    /// more IO and verify it all works as expected.
    Replace {
        /// Before each replacement, do a fill of the disk so the replace will
        /// have to copy the entire disk..
        #[clap(long, action)]
        fast_fill: bool,

        /// The address:port of a running downstairs for replacement
        #[clap(long, action)]
        replacement: SocketAddr,
    },
    /// Test that we can replace a downstairs when the upstairs is not active.
    ReplaceBeforeActive {
        /// The address:port of a running downstairs for replacement
        #[clap(long, action)]
        replacement: SocketAddr,
    },
    /// Test replacement of a downstairs while doing the initial reconciliation.
    ReplaceReconcile {
        /// The address:port of a running downstairs for replacement
        #[clap(long, action)]
        replacement: SocketAddr,
    },
    Span,
    Verify,
    Version,
    /// Select a random offset/length, then Write/Flush/Read that
    /// offset/length.
    WFR,
}

#[derive(Debug, Parser)]
#[clap(name = "client", term_width = 80)]
#[clap(about = "A Crucible upstairs test client", long_about = None)]
pub struct Opt {
    // TLS options
    #[clap(long, action)]
    cert_pem: Option<String>,

    /// For tests that support it, run until a SIGUSR1 signal is received.
    #[clap(long, global = true, action, conflicts_with = "count")]
    continuous: bool,

    /// IP:Port for the upstairs control http server
    #[clap(long, global = true, action)]
    control: Option<SocketAddr>,

    /// For tests that support it, pass this count value for the number
    /// of loops the test should do.
    #[clap(short, long, global = true, action)]
    count: Option<usize>,

    /// IP:Port for a dsc server.
    /// Some tests require a dsc enpoint to control the downstairs.
    /// A dsc endpoint can also be used to construct the initial Volume.
    #[clap(long, global = true, action)]
    dsc: Option<SocketAddr>,

    /// How long to wait before the auto flush check fires
    #[clap(long, global = true, action)]
    flush_timeout: Option<f32>,

    #[clap(short, global = true, long, default_value_t = 0, action)]
    gen: u64,

    /// The key for an encrypted downstairs.
    #[clap(short, global = true, long, action)]
    key: Option<String>,

    /// TLS option
    #[clap(long, action)]
    key_pem: Option<String>,

    /// Spin up a dropshot endpoint and serve metrics from it.
    /// This will use the values in metric-register and metric-collect
    #[clap(long, global = true, action)]
    metrics: bool,

    /// IP:Port for the Oximeter register address, which is Nexus.
    #[clap(long, global = true, default_value = "127.0.0.1:12221", action)]
    metric_register: SocketAddr,

    /// IP:Port for the Oximeter listen address
    #[clap(long, global = true, default_value = "127.0.0.1:55443", action)]
    metric_collect: SocketAddr,

    /// Don't print out IOs as we do them.
    #[clap(long, global = true, action)]
    quiet: bool,

    ///  quit after all crucible work queues are empty.
    #[clap(short, global = true, long, action, conflicts_with = "stable")]
    quit: bool,

    /// For the verify test, if this option is included we will allow
    /// the write log range of data to pass the verify_volume check.
    #[clap(long, global = true, action)]
    range: bool,

    /// Set the read_only option when starting the upstairs.
    /// Note that setting this won't prevent you from sending writes to the
    /// downstairs.  You are responsible for dealing with the fallout.
    #[clap(long, global = true, action)]
    read_only: bool,

    /// Retry for activate, as long as it takes.  If we pass this arg, the
    /// test will retry the initial activate command as long as it takes.
    #[clap(long, global = true, action)]
    retry_activate: bool,

    /// TLS option
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    /// Quit only after all crucible work queues are empty and all downstairs
    /// are reporting active.
    #[clap(global = true, long, action, conflicts_with = "quit")]
    stable: bool,

    /// The IP:Port where each downstairs is listening.
    #[clap(short, long, global = true, action)]
    target: Vec<SocketAddr>,

    /// A UUID to use for the upstairs.
    #[clap(long, global = true, action)]
    uuid: Option<Uuid>,

    /// Read in a VCR from a file for use in constructing a volume
    #[clap(long, global = true, value_name = "VCRFILE", action)]
    vcr_file: Option<PathBuf>,

    /// In addition to any tests, verify the volume on startup.
    /// This only has value if verify_in is also set.
    #[clap(long, global = true, requires = "verify_in")]
    verify_at_start: bool,

    /// In addition to any tests, verify the volume after the tests
    /// have completed.  If you don't supply a verify_in file, then the
    /// verify will only check what this test run has written.
    #[clap(long, global = true, action)]
    verify_at_end: bool,

    /// For tests that support it, load the expected write count from
    /// the provided file.  The addition of a --verify-at-start option will
    /// also have the test verify what it imports from the file is valid.
    #[clap(long, global = true, value_name = "INFILE", action)]
    verify_in: Option<PathBuf>,

    ///  For tests that support it, save the write count into the
    ///  provided file.
    #[clap(long, global = true, value_name = "FILE", action)]
    verify_out: Option<PathBuf>,

    /// A test workload that crutest will execute.
    #[clap(subcommand)]
    workload: Workload,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();

    Ok(opt)
}

#[derive(Copy, Clone, Debug, clap::Args)]
struct BufferbloatWorkload {
    /// Size in blocks of each IO
    #[clap(long, default_value_t = 1, action)]
    io_size: usize,
    /// Number of outstanding IOs at the same time.
    #[clap(long, default_value_t = 1, action)]
    io_depth: usize,
    /// Number of seconds to run
    #[clap(long, default_value_t = 10, action)]
    time: u64,
    /// Print the volume log (at `INFO` level) to `stderr`
    ///
    /// If this is not set, then only `ERROR` messages are logged
    ///
    /// By default, this is not set, because the volume log is noisy and
    /// interrupts our intentional logging.
    #[clap(short, long)]
    verbose: bool,
}

#[derive(Copy, Clone, Debug, clap::Args)]
struct RandReadWriteWorkload {
    /// Size in blocks of each IO
    #[clap(long, default_value_t = 1, action)]
    io_size: usize,
    /// Number of outstanding IOs at the same time.
    #[clap(long, default_value_t = 1, action)]
    io_depth: usize,
    /// Number of seconds to run
    #[clap(long, default_value_t = 60, action)]
    time: u64,
    /// Completely fill the disk with random data first
    #[clap(long)]
    fill: bool,
    /// Print the volume log (at `INFO` level) to `stderr`
    ///
    /// If this is not set, then only `ERROR` messages are logged
    ///
    /// By default, this is not set, because the volume log is noisy and
    /// interrupts our intentional logging.
    #[clap(short, long)]
    verbose: bool,
    /// Print values in bytes, without human-readable units
    #[clap(short, long)]
    raw: bool,
    /// Time per sample printed to the output
    #[clap(long, default_value_t = 1.0)]
    sample_time: f64,
    /// Number of subsamples per sample
    #[clap(long, default_value_t = NonZeroU64::new(10).unwrap())]
    subsample_count: NonZeroU64,
}

/// Mode flags for `rand_write_read_workload`
#[derive(Copy, Clone, Debug)]
enum RandReadWriteMode {
    Read,
    Write,
}

/// Configuration for `rand_read_write_workload`
#[derive(Copy, Clone, Debug)]
struct RandReadWriteConfig {
    mode: RandReadWriteMode,
    encrypted: bool,
    io_depth: usize,
    blocks_per_io: usize,

    /// Print raw bytes, without human-friendly formatting
    raw: bool,
    /// Total amount of time to run
    time_secs: u64,
    /// Rate at which we should print samples
    sample_time_secs: f64,
    /// Number of subsamples for each `sample_time_secs`, for standard deviation
    subsample_count: NonZeroU64,
    fill: bool,
}

impl RandReadWriteConfig {
    fn new(
        cfg: RandReadWriteWorkload,
        encrypted: bool,
        mode: RandReadWriteMode,
    ) -> Self {
        RandReadWriteConfig {
            encrypted,
            io_depth: cfg.io_depth,
            blocks_per_io: cfg.io_size,
            time_secs: cfg.time,
            raw: cfg.raw,
            sample_time_secs: cfg.sample_time,
            subsample_count: cfg.subsample_count,
            fill: cfg.fill,
            mode,
        }
    }
}

/// Configuration for `bufferbloat_workload`
#[derive(Copy, Clone, Debug)]
struct BufferbloatConfig {
    encrypted: bool,
    io_depth: usize,
    blocks_per_io: usize,
    time_secs: u64,
}

impl BufferbloatConfig {
    fn new(cfg: BufferbloatWorkload, encrypted: bool) -> Self {
        BufferbloatConfig {
            encrypted,
            io_depth: cfg.io_depth,
            blocks_per_io: cfg.io_size,
            time_secs: cfg.time,
        }
    }
}

/*
 * All the tests need this basic info about the disk.
 * Not all tests make use of the write_log yet, but perhaps someday..
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiskInfo {
    volume_info: VolumeInfo,
    write_log: WriteLog,
    max_block_io: usize,
}

impl DiskInfo {
    pub fn block_size(&self) -> u64 {
        self.volume_info.block_size
    }
}

/*
 * All the tests need this basic set of information about the disk.
 */
async fn get_disk_info(volume: &Volume) -> Result<DiskInfo, CrucibleError> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let volume_info = volume.volume_extent_info().await?;
    let total_size = volume.total_size().await?;
    let total_blocks = (total_size / volume_info.block_size) as usize;

    /*
     * Limit the max IO size (in blocks) to be 1MiB or the size
     * of the volume, whichever is smaller
     */
    const MAX_IO_BYTES: usize = 1024 * 1024;
    let mut max_block_io = MAX_IO_BYTES / volume_info.block_size as usize;
    if total_blocks < max_block_io {
        max_block_io = total_blocks;
    }

    println!(
        "Disk: sv:{} bs:{}  ts:{}  tb:{}  max_io:{} or {}",
        volume_info.volumes.len(),
        volume_info.block_size,
        total_size,
        total_blocks,
        max_block_io,
        (max_block_io as u64 * volume_info.block_size),
    );

    /*
     * Create the write log that tracks the number of writes to each block,
     * so we can know what to expect for reads.
     */
    let write_log = WriteLog::new(total_blocks);

    Ok(DiskInfo {
        volume_info,
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
 * the write count of a disk at a specific time (like a flush) and then
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

    pub fn is_empty(&self) -> bool {
        self.count_cur.is_empty()
    }

    pub fn len(&self) -> usize {
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
        (self.count_cur[index] & 0xff) as u8
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
        let res;

        if self.count_min[index] & 0xff > self.count_cur[index] & 0xff {
            // Special case when the min and max cross a u8 boundary.
            let min_adjusted_value = (self.count_min[index] & 0xff) as u8;
            let cur_adjusted_value = (self.count_cur[index] & 0xff) as u8;
            println!(
                "SPEC  v:{}  min_av:{} cur_av:{}  cm:{} cc:{}",
                value,
                min_adjusted_value,
                cur_adjusted_value,
                self.count_min[index],
                self.count_cur[index],
            );

            let mut new_cur = value as u32;
            if value >= min_adjusted_value {
                res = true;
                // Figure out the delta between value and the minimum,
                // then add that to the non-adjusted minimum and make
                // that our new maximum.
                let delta = (value - min_adjusted_value) as u32;
                new_cur = self.count_min[index] + delta;
                println!("new cur is {} from min", new_cur);
            } else if value <= cur_adjusted_value {
                res = true;
                // Figure out the delta between value and the max(cur)
                // and then subtract that from the current cur to set
                // our new expected value.
                let delta = (cur_adjusted_value - value) as u32;
                new_cur = self.count_cur[index] - delta;
                println!("new cur is {} from cur", new_cur);
            } else {
                // The value in not in the expected range
                res = false;
            }

            // If update requested (and we are in the range) then update
            // the counter to reflect the new "max".
            if update && res {
                if new_cur != self.count_cur[index] {
                    println!("Adjusting new cur to {}", new_cur);
                    self.count_cur[index] = new_cur;
                } else {
                    println!("No adjustment necessary");
                }
            }
        } else {
            // The regular case, just be sure we are between the
            // lower and upper expected values.
            let shift = self.count_min[index] / 256;

            let s_value = value as u32 + (256 * shift);
            println!(
                "Shift {}, v:{} sv:{} min:{} cur:{}",
                shift,
                value,
                s_value,
                self.count_min[index],
                self.count_cur[index],
            );

            res = s_value >= self.count_min[index]
                && s_value <= self.count_cur[index];

            // Only update if requested and the range was valid.
            if update && res && self.count_cur[index] != s_value {
                println!(
                    "Update block {} to {} (min:{} max:{} res:{})",
                    index,
                    s_value,
                    self.count_min[index],
                    self.count_cur[index],
                    res,
                );
                self.count_cur[index] = s_value;
            }
        }
        res
    }

    // Set the current write count to a specific value.
    // You should only be using this if you know what you are doing.
    #[cfg(test)]
    fn set_wc(&mut self, index: usize, value: u32) {
        self.count_cur[index] = value;
    }
}

async fn load_write_log(
    volume: &Volume,
    di: &mut DiskInfo,
    vi: PathBuf,
    verify: bool,
) -> Result<()> {
    /*
     * Fill the write count from a provided file.
     */
    di.write_log = match read_json(&vi) {
        Ok(write_log) => write_log,
        Err(e) => bail!("Error {:?} reading verify config {:?}", e, vi),
    };
    println!("Loading write count information from file {vi:?}");
    if di.write_log.len() != di.volume_info.total_blocks() {
        bail!(
            "Verify file {vi:?} blocks:{} does not match disk:{}",
            di.write_log.len(),
            di.volume_info.total_blocks()
        );
    }
    /*
     * Only verify the volume if requested.
     */
    if verify {
        if let Err(e) = verify_volume(volume, di, false).await {
            bail!("Initial volume verify failed: {:?}", e)
        }
    }
    Ok(())
}

// How to determine when a test will stop running.
// Either by count, or a message over a channel.
enum WhenToQuit {
    Count {
        count: usize,
    },
    Signal {
        shutdown_rx: mpsc::Receiver<SignalAction>,
    },
}

#[derive(Debug)]
enum SignalAction {
    Shutdown,
    Verify,
}

// When a signal is received, send a message over a channel.
async fn handle_signals(
    mut signals: Signals,
    shutdown_tx: mpsc::Sender<SignalAction>,
) {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGUSR1 => {
                shutdown_tx.send(SignalAction::Shutdown).await.unwrap();
            }
            SIGUSR2 => {
                shutdown_tx.send(SignalAction::Verify).await.unwrap();
            }
            x => {
                panic!("Received unsupported signal {}", x);
            }
        }
    }
}

// Construct a volume and a list of targets for use by the tests.
// Our choice of how to construct the volume depends on what options we
// have been given.
//
// If we have been provided a vcr file, this will get first priority and all
// other options will be ignored.
//
// Second choice is if we are provided the address for a dsc server.  We can
// use the dsc server to determine part of what we need to create a Volume.
// The rest of what we need we can gather from the CrucibleOpts, which are
// built from options provided on the command line, or their defaults.
//
// For the final choice we have to construct a Volume by asking our downstairs
// for information that we need up front, which we then combine with
// CrucibleOpts.  This will work as long as one of the downstairs is up
// already.  If we have a test that requires no downstairs to be running on
// startup, then we need to provide a VCR file, or use the dsc server.
//
// While making our volume, we also record the targets that become part of
// the volume in a separate Vec.  In some cases we no longer have access to
// the target information after the volume is constructed, and some tests also
// want the specific targets, so we make and return that list here.
async fn make_a_volume(
    opt: &Opt,
    volume_logger: Logger,
    test_log: &Logger,
    pr: Option<ProducerRegistry>,
) -> Result<(Volume, Vec<SocketAddr>)> {
    let up_uuid = opt.uuid.unwrap_or_else(Uuid::new_v4);
    let mut crucible_opts = CrucibleOpts {
        id: up_uuid,
        target: opt.target.clone(),
        lossy: false,
        flush_timeout: opt.flush_timeout,
        key: opt.key.clone(),
        cert_pem: opt.cert_pem.clone(),
        key_pem: opt.key_pem.clone(),
        root_cert_pem: opt.root_cert_pem.clone(),
        control: opt.control,
        read_only: opt.read_only,
    };

    if let Some(vcr_file) = &opt.vcr_file {
        let vcr: VolumeConstructionRequest = match read_json(vcr_file) {
            Ok(vcr) => vcr,
            Err(e) => {
                bail!("Error {:?} reading VCR from {:?}", e, vcr_file)
            }
        };
        info!(test_log, "Using VCR: {:?}", vcr);

        if opt.gen != 0 {
            warn!(test_log, "gen option is ignored when VCR is provided");
        }
        if !opt.target.is_empty() {
            warn!(test_log, "targets are ignored when VCR is provided");
        }
        let targets = vcr.targets();

        let volume = Volume::construct(vcr, pr, volume_logger).await.unwrap();
        Ok((volume, targets))
    } else if opt.dsc.is_some() {
        // We were given a dsc endpoint, use that to create a VCR that
        // represents our Volume.
        if !opt.target.is_empty() {
            warn!(test_log, "targets are ignored when dsc option is provided");
        }
        let dsc = opt.dsc.unwrap();
        let dsc_url = format!("http://{}", dsc);
        let dsc_client = Client::new(&dsc_url);
        let ri = match dsc_client.dsc_get_region_info().await {
            Ok(res) => res.into_inner(),
            Err(e) => {
                bail!("Failed to get region info from {:?}: {}", dsc_url, e);
            }
        };
        info!(test_log, "Use this region info from dsc: {:?}", ri);
        let extent_info = RegionExtentInfo {
            block_size: ri.block_size,
            blocks_per_extent: ri.blocks_per_extent,
            extent_count: ri.extent_count,
        };

        let res = dsc_client.dsc_get_region_count().await.unwrap();
        let regions = res.into_inner();
        if regions < 3 {
            bail!("Found {regions} regions.  We need at least 3");
        }

        let sv_count = regions / 3;
        let region_remainder = regions % 3;
        info!(
            test_log,
            "dsc has {} regions.  This means {} sub_volumes", regions, sv_count
        );
        if region_remainder != 0 {
            warn!(
                test_log,
                "{} regions from dsc will not be part of any sub_volume",
                region_remainder,
            );
        }

        // We start by creating the volume builder
        let mut builder =
            VolumeBuilder::new(extent_info.block_size, volume_logger);

        // Now, loop over regions we found from dsc and make a
        // sub_volume at every three.
        let mut targets = Vec::new();
        let mut cid = 0;
        for sv in 0..sv_count {
            let mut sv_targets = Vec::new();
            for _ in 0..3 {
                let port = dsc_client.dsc_get_port(cid).await.unwrap();
                let tar = SocketAddr::new(
                    dsc.ip(),
                    port.into_inner().try_into().unwrap(),
                );
                sv_targets.push(tar);
                targets.push(tar);
                cid += 1;
            }
            info!(test_log, "SV {:?} has targets: {:?}", sv, sv_targets);
            crucible_opts.target = sv_targets;

            builder
                .add_subvolume_create_guest(
                    crucible_opts.clone(),
                    extent_info.clone(),
                    opt.gen,
                    pr.clone(),
                )
                .await
                .unwrap();
        }

        Ok((Volume::from(builder), targets))
    } else {
        // We were not provided a VCR, so, we have to make one by using
        // the repair port on a downstairs to get region information that
        // we require.  Once we have that information, we can build a VCR
        // from it.

        // For each sub-volume, we need to know:
        // block_size, blocks_per_extent, and extent_size.  We can get any
        // of the target downstairs to give us this info, if they are
        // running.  We don't care which one responds.  Any mismatch will
        // be detected later in the process and handled by the upstairs.
        let mut extent_info_result = None;
        for target in &crucible_opts.target {
            let port = target.port() + crucible_common::REPAIR_PORT_OFFSET;
            let addr = SocketAddr::new(target.ip(), port);
            let repair_url = format!("http://{addr}");
            info!(test_log, "look at: {repair_url}");
            let repair_client = repair_client::new(&repair_url);
            match repair_client.get_region_info().await {
                Ok(ri) => {
                    info!(test_log, "RI is: {:?}", ri);
                    extent_info_result = Some(RegionExtentInfo {
                        block_size: ri.block_size(),
                        blocks_per_extent: ri.extent_size().value,
                        extent_count: ri.extent_count(),
                    });
                    break;
                }
                Err(e) => {
                    warn!(
                        test_log,
                        "Failed to get info from {:?} {:?}", repair_url, e
                    );
                }
            }
        }
        let extent_info = match extent_info_result {
            Some(ei) => ei,
            None => {
                bail!("Can't determine extent info to build a Volume");
            }
        };
        let targets = crucible_opts.target.clone();

        let mut builder =
            VolumeBuilder::new(extent_info.block_size, volume_logger);
        builder
            .add_subvolume_create_guest(
                crucible_opts.clone(),
                extent_info,
                opt.gen,
                pr,
            )
            .await
            .unwrap();

        Ok((Volume::from(builder), targets))
    }
}

/**
 * A test program that makes use use of the interfaces that Crucible exposes.
 */
#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;
    let is_encrypted = opt.key.is_some();

    // If we just want the version, print that and exit.
    if let Workload::Version = opt.workload {
        let info = crucible_common::BuildInfo::default();
        println!("{}", info);
        println!(
            "Upstairs <-> Downstairs Message Version: {}",
            CRUCIBLE_MESSAGE_VERSION
        );
        return Ok(());
    }

    if matches!(opt.workload, Workload::Verify) && opt.verify_in.is_none() {
        bail!("Verify requires verify_in file");
    }

    // If just want the cli, then start that after our runtime.  The cli
    // does not need upstairs started, as that should happen in the
    // cli-server code.
    if let Workload::Cli { attach } = opt.workload {
        cli::start_cli_client(attach).await?;
        return Ok(());
    }

    // Opt out of verbose logs for certain tests
    let log_level = match opt.workload {
        Workload::RandRead {
            cfg: RandReadWriteWorkload { verbose, .. },
        }
        | Workload::RandWrite {
            cfg: RandReadWriteWorkload { verbose, .. },
        }
        | Workload::Bufferbloat {
            cfg: BufferbloatWorkload { verbose, .. },
        } => {
            if verbose {
                slog::Level::Info
            } else {
                slog::Level::Error
            }
        }
        _ => slog::Level::Info,
    };
    let volume_logger = crucible_common::build_logger_with_level(log_level);

    let test_log = volume_logger.new(o!("task" => "crutest".to_string()));

    let pr;
    if opt.metrics {
        // If metrics are desired, we create and register the server
        // first. Once we have the server, we clone the ProducerRegistry
        // so we can pass that on to the upstairs.
        // Finally, spin out a task with the server to provide the endpoint
        // so metrics can be collected by Oximeter.
        println!(
            "Creating a metric collect endpoint at {}",
            opt.metric_collect
        );
        match client_oximeter(opt.metric_collect, opt.metric_register) {
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

    // Build a Volume for all the tests to use.
    let (volume, mut targets) =
        make_a_volume(&opt, volume_logger.clone(), &test_log, pr).await?;

    let downstairs_in_volume = targets.len() - (targets.len() % 3);
    info!(test_log, "Downstairs in volume = {downstairs_in_volume}");

    if let Workload::CliServer { listen, port } = opt.workload {
        cli::start_cli_server(
            &volume,
            listen,
            port,
            opt.verify_in,
            opt.verify_out,
        )
        .await?;
        return Ok(());
    }

    if opt.retry_activate {
        while let Err(e) = volume.activate_with_gen(opt.gen).await {
            println!("Activate returns: {:#}  Retrying", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
        println!("Activate successful");
    } else {
        volume.activate_with_gen(opt.gen).await?;
    }

    println!("Wait for a query_work_queue command to finish before sending IO");
    volume.query_work_queue().await?;

    loop {
        match volume.query_is_active().await {
            Ok(true) => {
                break;
            }
            _ => {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                println!("Waiting for upstairs to be active");
            }
        }
    }

    /*
     * Build the disk info struct that all the tests will use.
     * This includes importing and verifying from a write log, if requested.
     */
    let mut disk_info = match get_disk_info(&volume).await {
        Ok(disk_info) => disk_info,
        Err(e) => bail!("failed to get disk info: {:?}", e),
    };

    /*
     * Now that we have the region info from the Upstairs, apply any
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
            if matches!(opt.workload, Workload::Verify) {
                false
            } else {
                opt.verify_at_start
            }
        };
        load_write_log(&volume, &mut disk_info, verify_in, verify).await?;
    }

    let (shutdown_tx, shutdown_rx) = mpsc::channel::<SignalAction>(4);
    if opt.continuous {
        println!("Setup signal handler");
        let signals = Signals::new([SIGUSR1, SIGUSR2])?;
        tokio::spawn(handle_signals(signals, shutdown_tx));
    }

    /*
     * Call the function for the workload option passed from the command
     * line.
     */
    match opt.workload {
        Workload::Balloon => {
            println!("Run balloon test");
            balloon_workload(&volume, &mut disk_info).await?;
        }
        Workload::Big => {
            println!("Run big test");
            big_workload(&volume, &mut disk_info).await?;
        }
        Workload::Biggest => {
            println!("Run biggest IO test");
            biggest_io_workload(&volume, &mut disk_info).await?;
        }
        Workload::Burst => {
            println!("Run burst test (demo in a loop)");
            burst_workload(&volume, 460, 190, &mut disk_info, &opt.verify_out)
                .await?;
        }
        Workload::Cli { .. } => {
            unreachable!("This case handled above");
        }
        Workload::CliServer { .. } => {
            unreachable!("This case handled above");
        }
        Workload::Deactivate => {
            /*
             * A small default of 5 is okay for a functional test, but
             * not enough for a more exhaustive test.
             */
            let count = opt.count.unwrap_or(5);
            println!("Run deactivate test");
            deactivate_workload(&volume, count, &mut disk_info, opt.gen)
                .await?;
        }
        Workload::Demo => {
            println!("Run Demo test");
            let count = opt.count.unwrap_or(300);
            demo_workload(&volume, count, &mut disk_info).await?;
        }
        Workload::Dep => {
            println!("Run dep test");
            dep_workload(&volume, &mut disk_info).await?;
        }

        Workload::Dirty => {
            println!("Run dirty test");
            let count = opt.count.unwrap_or(10);
            dirty_workload(&volume, &mut disk_info, count).await?;

            /*
             * Saving state here when we have not waited for a flush
             * to finish means that the state recorded here may not be
             * what ends up being in the downstairs.  All we guarantee is
             * that everything before the flush will be there, and possibly
             * things that came after the flush.
             */
            if let Some(vo) = &opt.verify_out {
                write_json(vo, &disk_info.write_log, true)?;
                println!("Wrote out file {vo:?}");
            }
            return Ok(());
        }

        Workload::FastFill => {
            println!("FastFill test");
            fill_sparse_workload(&volume, &mut disk_info).await?;
        }

        Workload::Fill { skip_verify } => {
            println!("Fill test");
            fill_workload(&volume, &mut disk_info, skip_verify).await?;
        }

        Workload::Generic => {
            // Either we have a count, or we run until we get a signal.
            let mut wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(500);
                    WhenToQuit::Count { count }
                }
            };

            generic_workload(
                &volume,
                &mut wtq,
                &mut disk_info,
                opt.quiet,
                false,
            )
            .await?;
        }

        Workload::GenericRead => {
            // Either we have a count, or we run until we get a signal.
            let mut wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(500);
                    WhenToQuit::Count { count }
                }
            };

            generic_workload(
                &volume,
                &mut wtq,
                &mut disk_info,
                opt.quiet,
                true,
            )
            .await?;
        }

        Workload::One => {
            println!("One test");
            one_workload(&volume, &mut disk_info).await?;
        }
        Workload::RandRead { cfg } => {
            rand_read_write_workload(
                &volume,
                &mut disk_info,
                RandReadWriteConfig::new(
                    cfg,
                    is_encrypted,
                    RandReadWriteMode::Read,
                ),
            )
            .await?;
            if opt.quit {
                return Ok(());
            }
        }
        Workload::RandWrite { cfg } => {
            rand_read_write_workload(
                &volume,
                &mut disk_info,
                RandReadWriteConfig::new(
                    cfg,
                    is_encrypted,
                    RandReadWriteMode::Write,
                ),
            )
            .await?;
            if opt.quit {
                return Ok(());
            }
        }
        Workload::Bufferbloat { cfg } => {
            bufferbloat_workload(
                &volume,
                &mut disk_info,
                BufferbloatConfig::new(cfg, is_encrypted),
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
        Workload::Repair => {
            println!("Run Repair workload");
            let count = opt.count.unwrap_or(10);
            repair_workload(&volume, count, &mut disk_info).await?;
            drop(volume);
            if let Some(vo) = &opt.verify_out {
                write_json(vo, &disk_info.write_log, true)?;
                println!("Wrote out file {vo:?}");
            }
            return Ok(());
        }
        Workload::Replay => {
            // Either we have a count, or we run until we get a signal.
            let mut wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(1);
                    WhenToQuit::Count { count }
                }
            };
            let dsc_client = match opt.dsc {
                Some(dsc_addr) => {
                    let dsc_url = format!("http://{}", dsc_addr);
                    Client::new(&dsc_url)
                }
                None => {
                    bail!("Replay workload requires a dsc endpoint");
                }
            };
            replay_workload(
                &volume,
                &mut wtq,
                &mut disk_info,
                dsc_client,
                downstairs_in_volume as u32,
            )
            .await?;
        }
        Workload::Replace {
            fast_fill,
            replacement,
        } => {
            // Either we have a count, or we run until we get a signal.
            let mut wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(1);
                    WhenToQuit::Count { count }
                }
            };

            // Add to the list of targets for our volume the replacement
            // target provided on the command line
            targets.push(replacement);
            replace_workload(
                &volume,
                &mut wtq,
                &mut disk_info,
                targets,
                fast_fill,
            )
            .await?;
        }
        Workload::ReplaceBeforeActive { replacement } => {
            let dsc_client = match opt.dsc {
                Some(dsc_addr) => {
                    let dsc_url = format!("http://{}", dsc_addr);
                    Client::new(&dsc_url)
                }
                None => {
                    bail!("Replace before active requires a dsc endpoint");
                }
            };
            // Either we have a count, or we run until we get a signal.
            let wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(5);
                    WhenToQuit::Count { count }
                }
            };

            // Add to the list of targets for our volume the replacement
            // target provided on the command line
            targets.push(replacement);

            // Verify the number of targets dsc has matches what the number
            // of targets we found.
            let res = dsc_client.dsc_get_region_count().await.unwrap();
            let region_count = res.into_inner();
            if region_count != targets.len() as u32 {
                bail!(
                    "Downstairs targets:{} does not match dsc targets: {}",
                    region_count,
                    targets.len(),
                );
            }
            replace_before_active(
                &volume,
                wtq,
                &mut disk_info,
                targets,
                dsc_client,
                opt.gen,
                test_log,
            )
            .await?;
        }
        Workload::ReplaceReconcile { replacement } => {
            let dsc_client = match opt.dsc {
                Some(dsc_addr) => {
                    let dsc_url = format!("http://{}", dsc_addr);
                    Client::new(&dsc_url)
                }
                None => {
                    bail!("Replace reconcile requires a dsc endpoint");
                }
            };
            // Either we have a count, or we run until we get a signal.
            let wtq = {
                if opt.continuous {
                    WhenToQuit::Signal { shutdown_rx }
                } else {
                    let count = opt.count.unwrap_or(5);
                    WhenToQuit::Count { count }
                }
            };

            // Add to the list of targets for our volume the replacement
            // target provided on the command line
            targets.push(replacement);

            // Verify the number of targets dsc has matches what the number
            // of targets we found.
            let res = dsc_client.dsc_get_region_count().await.unwrap();
            let region_count = res.into_inner();
            if region_count != targets.len() as u32 {
                bail!(
                    "Downstairs targets:{} does not match dsc targets: {}",
                    region_count,
                    targets.len(),
                );
            }
            replace_while_reconcile(
                &volume,
                wtq,
                &mut disk_info,
                targets,
                dsc_client,
                opt.gen,
                test_log,
            )
            .await?;
        }
        Workload::Span => {
            println!("Span test");
            span_workload(&volume, &mut disk_info).await?;
        }
        Workload::Verify => {
            /*
             * For verify, if -q, we quit right away.  If we don't quit, then
             * this turns into a read verify loop, sleep for some duration
             * and then re-check the volume.
             */
            if let Err(e) =
                verify_volume(&volume, &mut disk_info, opt.range).await
            {
                bail!("Initial volume verify failed: {:?}", e)
            }
            if let Some(vo) = &opt.verify_out {
                write_json(vo, &disk_info.write_log, true)?;
                println!("Wrote out file {vo:?}");
            }
            if opt.quit {
                println!("Verify test completed");
            } else {
                println!("Verify read loop begins");
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10))
                        .await;
                    if let Err(e) =
                        verify_volume(&volume, &mut disk_info, opt.range).await
                    {
                        bail!("Volume verify failed: {:?}", e)
                    }
                    let mut wc = volume.show_work().await?;
                    while wc.up_count + wc.ds_count > 0 {
                        println!("Waiting for all work to be completed");
                        tokio::time::sleep(tokio::time::Duration::from_secs(
                            10,
                        ))
                        .await;
                        wc = volume.show_work().await?;
                    }
                }
            }
        }
        Workload::Version => {
            panic!("This case handled above");
        }
        Workload::WFR => {
            println!("Run Write-Flush-Read random IO test");
            let count = opt.count.unwrap_or(10);
            write_flush_read_workload(&volume, count, &mut disk_info).await?;
        }
    }

    if opt.verify_at_end {
        if let Err(e) = verify_volume(&volume, &mut disk_info, false).await {
            bail!("Final volume verify failed: {:?}", e)
        }
    }

    if let Some(vo) = &opt.verify_out {
        write_json(vo, &disk_info.write_log, true)?;
        println!("Wrote out file {vo:?}");
    }

    println!("CLIENT: Tests done.  All submitted work has been ACK'd");
    loop {
        let wc = volume.show_work().await?;
        println!(
            "CLIENT: Up:{} ds:{} act:{}",
            wc.up_count, wc.ds_count, wc.active_count
        );
        if opt.quit && wc.up_count + wc.ds_count == 0 {
            println!("CLIENT: All crucible jobs finished, exiting program");
            return Ok(());
        } else if opt.stable
            && wc.up_count + wc.ds_count == 0
            && wc.active_count == downstairs_in_volume
        {
            println!("CLIENT: All jobs finished, all DS active.");
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
    volume: &Volume,
    di: &mut DiskInfo,
    range: bool,
) -> Result<()> {
    assert_eq!(di.write_log.len(), di.volume_info.total_blocks());

    println!(
        "Read and Verify all blocks (0..{} range:{})",
        di.volume_info.total_blocks(),
        range
    );

    let pb = ProgressBar::new(di.volume_info.total_blocks() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .unwrap()
        .progress_chars("#>-"));

    const IO_SIZE: usize = 256;
    const NUM_WORKERS: usize = 8;

    // Steal the write log, so we can share it between threads
    let write_log = std::mem::replace(&mut di.write_log, WriteLog::new(0));
    let write_log = Arc::new(std::sync::RwLock::new(write_log));
    let blocks_done = Arc::new(AtomicUsize::new(0));

    let tasks = futures::stream::FuturesUnordered::new();
    for i in 0..NUM_WORKERS {
        let mut block_index = i * IO_SIZE;
        let volume = volume.clone();
        let write_log = write_log.clone();
        let blocks_done = blocks_done.clone();
        let total_blocks = di.volume_info.total_blocks();
        let block_size = di.volume_info.block_size;
        let pb = pb.clone();
        tasks.push(tokio::task::spawn(async move {
            let mut result = Ok(());
            let mut data = crucible::Buffer::new(0, block_size as usize);
            while block_index < total_blocks {
                let offset = BlockIndex(block_index as u64);

                let next_io_blocks = (total_blocks - block_index).min(IO_SIZE);
                data.reset(next_io_blocks, block_size as usize);
                volume.read(offset, &mut data).await?;

                let mut write_log = write_log.write().unwrap();
                match validate_vec(
                    &*data,
                    block_index,
                    &mut write_log,
                    block_size,
                    range,
                ) {
                    ValidateStatus::Bad => {
                        println!(
                            "Error in block range {} -> {}",
                            block_index,
                            block_index + next_io_blocks
                        );
                        result = Err(anyhow!("Validation error".to_string()));
                    }
                    ValidateStatus::InRange => {
                        if range {
                            {}
                        } else {
                            println!(
                                "Error in block range {} -> {}",
                                block_index,
                                block_index + next_io_blocks
                            );
                            result =
                                Err(anyhow!("Validation error".to_string()));
                        }
                    }
                    ValidateStatus::Good => {}
                }

                block_index += NUM_WORKERS * IO_SIZE;
                let progress =
                    blocks_done.fetch_add(next_io_blocks, Ordering::Relaxed);
                pb.set_position(progress as u64);
            }
            result
        }));
    }

    // Wait for the tasks to finish
    let mut result = Ok(());
    for t in tasks {
        let r = t.await?;
        if r.is_err() {
            result = r;
        }
    }
    pb.finish();

    // Now that all the workers are done, put the write log back into place
    di.write_log = std::mem::replace(
        &mut Arc::try_unwrap(write_log)
            .expect("could not unwrap write log")
            .write()
            .unwrap(),
        WriteLog::new(0),
    );
    result
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
) -> BytesMut {
    if blocks == 0 {
        println!("Warning: fill requested of zero length buffer");
    }
    assert_ne!(bs, 0);

    /*
     * Each block we are filling the buffer for can have a different
     * seed value.  For multiple block sized writes, we need to create
     * the write buffer with the correct seed value.
     */
    let mut vec = BytesMut::with_capacity(blocks * bs as usize);
    for (block_offset, chunk) in (block_index..(block_index + blocks))
        .zip(vec.spare_capacity_mut().chunks_mut(bs as usize))
    {
        // The start of each block contains that block's index mod 255
        chunk[0].write((block_offset % 255) as u8);

        // Fill the rest of the buffer with the new write count
        let seed = wl.get_seed(block_offset);
        chunk[1..].iter_mut().for_each(|b| {
            b.write(seed);
        });
    }

    // SAFETY:
    // We initialized the entire buffer in the loop above
    unsafe {
        let n = vec.capacity();
        vec.set_len(n);
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
#[derive(Debug, Clone, PartialEq)]
enum ValidateStatus {
    Good,
    InRange,
    Bad,
}

// Block mismatch summary.
// When we have a mismatch on a block, don't print a line for every single
// byte that is wrong, collect the range and print a single summary at the
// end of the range.
struct BlockErr {
    block: usize,
    starting_offset: usize,
    ending_offset: usize,
    starting_volume_offset: u64,
    ending_volume_offset: u64,
    expected_value: u8,
    found_value: u8,
    status: ValidateStatus,
    msg: String,
}

impl fmt::Display for BlockErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} bo:{}-{} Volume offset:{}-{}  Expected:{} Got:{}",
            self.msg,
            self.block,
            self.starting_offset,
            self.ending_offset,
            self.starting_volume_offset,
            self.ending_volume_offset,
            self.expected_value,
            self.found_value,
        )
    }
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
fn validate_vec<V: AsRef<[u8]>>(
    data: V,
    block_index: usize,
    wl: &mut WriteLog,
    bs: u64,
    range: bool,
) -> ValidateStatus {
    let data = data.as_ref();

    let bs = bs as usize;
    assert_eq!(data.len() % bs, 0);
    if data.is_empty() {
        println!("Warning: Validation of zero length buffer");
    }

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
        let mut block_err: Option<BlockErr> = None;
        for i in 1..bs {
            if data[data_offset + i] != seed {
                // Our data is not what we expect.
                // Figure out if it is in range if requested, and print a
                // message reflecting what the situation is.
                let byte_offset = bs as u64 * block_offset as u64;
                let msg;
                if range {
                    if wl.validate_seed_range(
                        block_offset,
                        data[data_offset + i],
                        true,
                    ) {
                        msg = "In Range   Block:".to_string();
                        // Only change if it is currently good.
                        if res == ValidateStatus::Good {
                            res = ValidateStatus::InRange;
                        }
                    } else {
                        msg = "Out of Range Block:".to_string();
                        res = ValidateStatus::Bad;
                    }
                } else {
                    msg = "Mismatch     Block:".to_string();
                    res = ValidateStatus::Bad;
                }

                // Check to see if we have an error already
                block_err = match block_err {
                    // The error is contiguous with our current error
                    Some(mut be)
                        if be.status == res
                            && be.found_value == data[data_offset + i]
                            && be.ending_offset + 1 == i =>
                    {
                        be.ending_offset = i;
                        be.ending_volume_offset = byte_offset;
                        Some(be)
                    }
                    _ => {
                        // Print the previous range, if it exists
                        if let Some(be) = block_err {
                            println!("{}", be);
                        }

                        // Start a new range.
                        Some(BlockErr {
                            block: block_offset,
                            starting_offset: i,
                            ending_offset: i,
                            starting_volume_offset: byte_offset,
                            ending_volume_offset: byte_offset,
                            expected_value: seed,
                            found_value: data[data_offset + i],
                            status: res.clone(),
                            msg,
                        })
                    }
                };
            } else {
                // This check was valid. If we have seen previous errors,
                // print out the summary report now.
                if let Some(be) = block_err.take() {
                    println!("{}", be);
                }
            }
        }

        // If the last check was bad, we will exit the loop without having
        // printed any errors yet. Print the summary now.
        if let Some(be) = block_err {
            println!("{}", be);
        }
        data_offset += bs;
    }
    res
}

/*
 * Write then read (and verify) to every possible block, with every size that
 * block can possibly support.
 * I named it balloon because each loop on a block "balloons" from the
 * minimum IO size to the largest possible IO size.
 */
async fn balloon_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    for block_index in 0..di.volume_info.total_blocks() {
        /*
         * Loop over all the IO sizes (in blocks) that an IO can
         * have, given our starting block and the total number of blocks
         * We always have at least one block, and possibly more.
         */
        for size in 1..=(di.max_block_io - block_index) {
            /*
             * Update the write count for all blocks we plan to write to.
             */
            for i in 0..size {
                di.write_log.update_wc(block_index + i);
            }

            let data = fill_vec(
                block_index,
                size,
                &di.write_log,
                di.volume_info.block_size,
            );
            /*
             * Convert block_index to its byte value.
             */
            let offset = BlockIndex(block_index as u64);

            println!("IO at block:{}  size in blocks:{}", block_index, size);
            volume.write(offset, data).await?;
            volume.flush(None).await?;

            let mut data = crucible::Buffer::repeat(
                255,
                size,
                di.volume_info.block_size as usize,
            );
            volume.read(offset, &mut data).await?;

            let dl = data.into_bytes();
            match validate_vec(
                dl,
                block_index,
                &mut di.write_log,
                di.volume_info.block_size,
                false,
            ) {
                ValidateStatus::Bad | ValidateStatus::InRange => {
                    bail!("Error at {}", block_index);
                }
                ValidateStatus::Good => {}
            }
        }
    }

    Ok(())
}

/*
 * Write then read (and verify) to every possible block.
 */
async fn fill_workload(
    volume: &Volume,
    di: &mut DiskInfo,
    skip_verify: bool,
) -> Result<()> {
    let pb = ProgressBar::new(di.volume_info.total_blocks() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})"
        )
        .unwrap()
        .progress_chars("#>-"));

    const IO_SIZE: usize = 256;
    const NUM_WORKERS: usize = 8;

    // Steal the write log, so we can share it between threads
    let write_log = std::mem::replace(&mut di.write_log, WriteLog::new(0));
    let write_log = Arc::new(std::sync::RwLock::new(write_log));
    let blocks_done = Arc::new(AtomicUsize::new(0));

    let tasks = futures::stream::FuturesUnordered::new();
    for i in 0..NUM_WORKERS {
        let mut block_index = i * IO_SIZE;
        let volume = volume.clone();
        let write_log = write_log.clone();
        let blocks_done = blocks_done.clone();
        let total_blocks = di.volume_info.total_blocks();
        let block_size = di.volume_info.block_size;
        let pb = pb.clone();
        tasks.push(tokio::task::spawn(async move {
            while block_index < total_blocks {
                let offset = BlockIndex(block_index as u64);

                let next_io_blocks = (total_blocks - block_index).min(IO_SIZE);

                {
                    // Lock and update the write log
                    let mut write_log = write_log.write().unwrap();
                    for i in 0..next_io_blocks {
                        write_log.update_wc(block_index + i);
                    }
                };

                let data = {
                    let write_log = write_log.read().unwrap();
                    fill_vec(
                        block_index,
                        next_io_blocks,
                        &write_log,
                        block_size,
                    )
                };

                volume.write(offset, data).await?;

                block_index += NUM_WORKERS * IO_SIZE;
                let progress =
                    blocks_done.fetch_add(next_io_blocks, Ordering::Relaxed);
                pb.set_position(progress as u64);
            }
            Result::<(), CrucibleError>::Ok(())
        }));
    }

    for t in tasks {
        t.await??;
    }

    volume.flush(None).await?;
    pb.finish();

    // Now that all the workers are done, put the write log back into place
    di.write_log = std::mem::replace(
        &mut Arc::try_unwrap(write_log)
            .expect("could not unwrap write log")
            .write()
            .unwrap(),
        WriteLog::new(0),
    );

    if !skip_verify {
        verify_volume(volume, di, false).await?;
    }
    Ok(())
}

/*
 * Do a single random write to every extent, results in every extent being
 * touched without having to write to every block.
 */
async fn fill_sparse_workload(
    volume: &Volume,
    di: &mut DiskInfo,
) -> Result<()> {
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    let mut extent_block_start = 0;
    // We loop over all sub volumes, doing one write to each extent.
    for (index, sv) in di.volume_info.volumes.iter().enumerate() {
        let extents = sv.extent_count as usize;
        let extent_size = sv.blocks_per_extent as usize;
        for extent in 0..extents {
            let mut block_index: usize =
                extent_block_start + (extent * extent_size);
            let random_offset = rng.random_range(0..extent_size);
            block_index += random_offset;

            let offset = BlockIndex(block_index.try_into().unwrap());

            di.write_log.update_wc(block_index);

            let data = fill_vec(
                block_index,
                1,
                &di.write_log,
                di.volume_info.block_size,
            );

            println!(
                "[{index}][{extent}/{extents}] Write to block {}",
                block_index
            );
            volume.write(offset, data).await?;
        }
        extent_block_start += extent_size * sv.extent_count as usize;
    }

    volume.flush(None).await?;
    Ok(())
}

/*
 * Generic workload.  Do a random R/W/F, but wait for the operation to be
 * ACK'd before sending the next.  Limit the size of the IO to 10 blocks.
 * Read data is verified.
 */
async fn generic_workload(
    volume: &Volume,
    wtq: &mut WhenToQuit,
    di: &mut DiskInfo,
    quiet: bool,
    read_only: bool,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    let count_width = match wtq {
        WhenToQuit::Count { count } => count.to_string().len(),
        _ => 5,
    };
    let total_blocks = di.volume_info.total_blocks();
    let block_width = total_blocks.to_string().len();
    let size_width = (10 * di.volume_info.block_size).to_string().len();

    let max_io_size = std::cmp::min(10, total_blocks);

    for c in 1.. {
        let op = rng.random_range(0..10);
        if op == 0 {
            // flush
            if !quiet {
                match wtq {
                    WhenToQuit::Count { count } => {
                        println!(
                            "{:>0width$}/{:>0width$} Flush",
                            c,
                            count,
                            width = count_width,
                        );
                    }
                    WhenToQuit::Signal { .. } => {
                        println!("{:>0width$} Flush", c, width = count_width);
                    }
                }
            }
            volume.flush(None).await?;
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to max_io_size
            let size = rng.random_range(1..=max_io_size);

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = total_blocks - size + 1;
            let block_index = rng.random_range(0..block_max);

            // Convert offset and length to their byte values.
            let offset = BlockIndex(block_index as u64);

            if !read_only && op <= 4 {
                // Write
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    di.write_log.update_wc(block_index + i);
                }

                let data = fill_vec(
                    block_index,
                    size,
                    &di.write_log,
                    di.volume_info.block_size,
                );

                if !quiet {
                    match wtq {
                        WhenToQuit::Count { count } => {
                            print!(
                                "{:>0width$}/{:>0width$}",
                                c,
                                count,
                                width = count_width,
                            );
                        }
                        WhenToQuit::Signal { .. } => {
                            print!("{:>0width$}", c, width = count_width);
                        }
                    }
                }

                assert_eq!(data[1], di.write_log.get_seed(block_index));
                if !quiet {
                    print!(
                        " Write block {:>bw$}  len {:>sw$}  data:",
                        offset.0,
                        data.len(),
                        bw = block_width,
                        sw = size_width,
                    );
                    for i in 0..size {
                        print!(
                            " {:>3}",
                            di.write_log.get_seed(block_index + i)
                        );
                    }
                    println!();
                }
                volume.write(offset, data).await?;
            } else {
                // Read (+ verify)
                let mut data = crucible::Buffer::repeat(
                    255,
                    size,
                    di.volume_info.block_size as usize,
                );
                if !quiet {
                    match wtq {
                        WhenToQuit::Count { count } => {
                            print!(
                                "{:>0width$}/{:>0width$}",
                                c,
                                count,
                                width = count_width,
                            );
                        }
                        WhenToQuit::Signal { .. } => {
                            print!("{:>0width$}", c, width = count_width);
                        }
                    }
                    println!(
                        " Read  block {:>bw$}  len {:>sw$}",
                        offset.0,
                        data.len(),
                        bw = block_width,
                        sw = size_width,
                    );
                }
                volume.read(offset, &mut data).await?;

                let data_len = data.len();
                let dl = data.into_bytes();
                match validate_vec(
                    dl,
                    block_index,
                    &mut di.write_log,
                    di.volume_info.block_size,
                    false,
                ) {
                    ValidateStatus::Bad | ValidateStatus::InRange => {
                        bail!("Verify Error at {block_index} len:{data_len}")
                    }
                    ValidateStatus::Good => {}
                }
            }
        }
        match wtq {
            WhenToQuit::Count { count } => {
                if c > *count {
                    break;
                }
            }
            WhenToQuit::Signal { shutdown_rx } => {
                match shutdown_rx.try_recv() {
                    Ok(SignalAction::Shutdown) => {
                        println!("shutting down in response to SIGUSR1");
                        break;
                    }
                    Ok(SignalAction::Verify) => {
                        println!("Verify Volume");
                        if let Err(e) = verify_volume(volume, di, false).await {
                            bail!("Requested volume verify failed: {:?}", e)
                        }
                    }
                    _ => {} // Ignore everything else
                }
            }
        }
    }

    Ok(())
}

// Make use of dsc to stop and start a downstairs while sending IO.  This
// should trigger the replay code path.
async fn replay_workload(
    volume: &Volume,
    wtq: &mut WhenToQuit,
    di: &mut DiskInfo,
    dsc_client: Client,
    ds_count: u32,
) -> Result<()> {
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();
    let mut generic_wtq = WhenToQuit::Count { count: 300 };

    for c in 1.. {
        // Pick a DS at random
        let stopped_ds = rng.random_range(0..ds_count);
        dsc_client.dsc_stop(stopped_ds).await.unwrap();
        loop {
            let res = dsc_client.dsc_get_ds_state(stopped_ds).await.unwrap();
            let state = res.into_inner();
            if state == DownstairsState::Exit {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        generic_workload(volume, &mut generic_wtq, di, false, false).await?;

        let res = dsc_client.dsc_start(stopped_ds).await;
        println!("[{c}] Replay: started {stopped_ds}, returned:{:?}", res);

        // Wait for all IO to finish before we continue
        loop {
            let wc = volume.show_work().await?;
            println!(
                "CLIENT: Up:{} ds:{} act:{}",
                wc.up_count, wc.ds_count, wc.active_count
            );
            if wc.up_count + wc.ds_count == 0 {
                println!("Replay: All jobs finished");
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        match wtq {
            WhenToQuit::Count { count } => {
                if c > *count {
                    break;
                }
            }
            WhenToQuit::Signal { shutdown_rx } => {
                match shutdown_rx.try_recv() {
                    Ok(SignalAction::Shutdown) => {
                        println!("shutting down in response to SIGUSR1");
                        break;
                    }
                    Ok(SignalAction::Verify) => {
                        println!("Verify Volume");
                        if let Err(e) = verify_volume(volume, di, false).await {
                            bail!("Requested volume verify failed: {:?}", e)
                        }
                    }
                    _ => {} // Ignore everything else
                }
            }
        }
    }

    println!("Test replay has completed");
    Ok(())
}

// Test that a downstairs can be replaced while the initial reconciliation
// is underway.
//
// This test makes use of the dsc client to stop and start downstairs that
// will allow us to create a mismatch between downstairs which will then
// trigger a reconcile.
async fn replace_while_reconcile(
    volume: &Volume,
    mut wtq: WhenToQuit,
    di: &mut DiskInfo,
    targets: Vec<SocketAddr>,
    dsc_client: Client,
    mut gen: u64,
    log: Logger,
) -> Result<()> {
    assert!(targets.len() % 3 == 1);

    // The total number of downstairs we have that are part of the Volume.
    let ds_total = targets.len() - 1;
    let mut old_ds = 0;
    let mut new_ds = targets.len() - 1;
    let mut c = 1;
    // How long we wait for reconcile to start before we replace
    let mut active_wait = 6;

    info!(log, "Begin replacement while reconciliation test");
    loop {
        info!(log, "[{c}] Touch every extent part 1");
        fill_sparse_workload(volume, di).await?;

        info!(log, "[{c}] Stop a downstairs");
        // Stop a downstairs, wait for dsc to confirm it is stopped.
        dsc_client.dsc_stop(old_ds).await.unwrap();
        loop {
            let res = dsc_client.dsc_get_ds_state(old_ds).await.unwrap();
            let state = res.into_inner();
            if state == DownstairsState::Exit {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }
        info!(log, "[{c}] Touch every extent part 2");
        fill_sparse_workload(volume, di).await?;

        info!(log, "[{c}] Deactivate");
        volume.deactivate().await.unwrap();
        loop {
            let is_active = volume.query_is_active().await.unwrap();
            if !is_active {
                break;
            }
            info!(log, "[{c}] Waiting for deactivation");
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        // We now have touched every extent with one downstairs missing,
        // so on restart of that downstairs we will require reconciliation.

        // Start a downstairs, wait for dsc to confirm it is started.
        info!(log, "[{c}] Deactivate complete, now Start a downstairs");
        dsc_client.dsc_start(old_ds).await.unwrap();
        loop {
            let res = dsc_client.dsc_get_ds_state(old_ds).await.unwrap();
            let state = res.into_inner();
            if state == DownstairsState::Running {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        info!(log, "[{c}] Request the upstairs activate");
        // Spawn a task to re-activate, this will not finish till all three
        // downstairs have reconciled.
        gen += 1;
        let gc = volume.clone();
        let handle =
            tokio::spawn(async move { gc.activate_with_gen(gen).await });

        info!(log, "[{c}] wait {active_wait} for reconcile to start");
        tokio::time::sleep(tokio::time::Duration::from_secs(active_wait)).await;

        //  Give the activation request time to percolate in the upstairs.
        let is_active = volume.query_is_active().await.unwrap();
        info!(log, "[{c}] activate should now be waiting {:?}", is_active);
        // If this check fails, then the reconciliation has finished
        // before we had a chance to replace our downstairs. We try here to
        // reduce the wait time and do another loop.
        if is_active {
            if active_wait > 1 {
                active_wait -= 1;
                warn!(
                    log,
                    "[{c}] Adjusting wait time smaller to catch reconcile",
                );
                continue;
            } else {
                // A second was not long enough to catch the reconcile, so
                // the downstairs region size is too small for this test.
                panic!(
                    "Downstairs reconciliation too fast for this test, \
                    consider making a larger downstairs region"
                );
            }
        }

        info!(
            log,
            "[{c}] Replacing DS {old_ds}:{} with {new_ds}:{}",
            targets[old_ds as usize],
            targets[new_ds],
        );
        match volume
            .replace_downstairs(
                Uuid::new_v4(),
                targets[old_ds as usize],
                targets[new_ds],
            )
            .await
        {
            Ok(ReplaceResult::Started) => {}
            x => {
                bail!("Failed replace: {:?}", x);
            }
        }

        info!(log, "[{c}] Wait for activation after replacement");
        loop {
            let is_active = volume.query_is_active().await.unwrap();
            if is_active {
                break;
            }
            info!(
                log,
                "[{c}] Upstairs query_is_active reports: {:?}", is_active
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }

        // Our activate request task should have ended.
        if let Err(e) = handle.await.unwrap() {
            bail!("Failure reported in activate task {:?}", e);
        }

        info!(log, "[{c}] Verify volume after replacement");
        if let Err(e) = verify_volume(volume, di, false).await {
            bail!("Requested volume verify failed: {:?}", e)
        }

        // Wait for all IO to finish before we continue
        loop {
            let wc = volume.show_work().await?;
            info!(
                log,
                "[{c}] Jobs Up:{} Ds:{}  DS active:{}",
                wc.up_count,
                wc.ds_count,
                wc.active_count
            );
            if wc.up_count + wc.ds_count == 0 && wc.active_count == ds_total {
                info!(log, "[{c}] All jobs finished, all DS active.");
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        old_ds = (old_ds + 1) % (ds_total as u32 + 1);
        new_ds = (new_ds + 1) % (ds_total + 1);

        c += 1;
        match wtq {
            WhenToQuit::Count { count } => {
                if c > count {
                    break;
                }
            }
            WhenToQuit::Signal {
                ref mut shutdown_rx,
            } => {
                match shutdown_rx.try_recv() {
                    Ok(SignalAction::Shutdown) => {
                        println!("shutting down in response to SIGUSR1");
                        break;
                    }
                    Ok(SignalAction::Verify) => {
                        println!("Verify Volume");
                        if let Err(e) = verify_volume(volume, di, false).await {
                            bail!("Requested volume verify failed: {:?}", e)
                        }
                    }
                    _ => {} // Ignore everything else
                }
            }
        }
    }

    info!(log, "Test replace_while_reconcile has completed");
    Ok(())
}

// Test that a downstairs can be replaced before activation when the original
// downstairs is offline.
// Make use of dsc to stop our downstairs before activation.
// Do a fill on each loop so every extent will need to be repaired.
async fn replace_before_active(
    volume: &Volume,
    mut wtq: WhenToQuit,
    di: &mut DiskInfo,
    targets: Vec<SocketAddr>,
    dsc_client: Client,
    mut gen: u64,
    log: Logger,
) -> Result<()> {
    assert!(targets.len() % 3 == 1);

    info!(log, "Begin replacement before activation test");
    // We need to start from a known state and be sure that all three of the
    // current downstairs are consistent with each other. To guarantee this
    // we write to every block, then flush, then read.  This way we know
    // that the initial downstairs are all synced up on the same flush and
    // generation numbers.
    fill_workload(volume, di, true).await?;
    let ds_total = targets.len() - 1;
    let mut old_ds = 0;
    let mut new_ds = targets.len() - 1;
    for c in 1.. {
        info!(log, "[{c}] Touch every extent");
        fill_sparse_workload(volume, di).await?;

        volume.deactivate().await.unwrap();
        loop {
            let is_active = volume.query_is_active().await.unwrap();
            if !is_active {
                break;
            }
            info!(log, "[{c}] Waiting for deactivation");
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        // Stop a downstairs, wait for dsc to confirm it is stopped.
        dsc_client.dsc_stop(old_ds).await.unwrap();
        loop {
            let res = dsc_client.dsc_get_ds_state(old_ds).await.unwrap();
            let state = res.into_inner();
            if state == DownstairsState::Exit {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        info!(log, "[{c}] Request the upstairs activate");
        // Spawn a task to re-activate, this will not finish till all three
        // downstairs respond.
        gen += 1;
        let gc = volume.clone();
        let handle =
            tokio::spawn(async move { gc.activate_with_gen(gen).await });

        //  Give the activation request time to percolate in the upstairs.
        tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        let is_active = volume.query_is_active().await.unwrap();
        info!(log, "[{c}] activate should now be waiting {:?}", is_active);
        assert!(!is_active);

        info!(
            log,
            "[{c}] Replacing DS {old_ds}:{} with {new_ds}:{}",
            targets[old_ds as usize],
            targets[new_ds],
        );
        match volume
            .replace_downstairs(
                Uuid::new_v4(),
                targets[old_ds as usize],
                targets[new_ds],
            )
            .await
        {
            Ok(ReplaceResult::Started) => {}
            x => {
                bail!("Failed replace: {:?}", x);
            }
        }

        info!(log, "[{c}] Wait for activation after replacement");
        loop {
            let is_active = volume.query_is_active().await.unwrap();
            if is_active {
                break;
            }
            info!(
                log,
                "[{c}] Upstairs query_is_active reports: {:?}", is_active
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }

        // Our activate request task should have ended.
        if let Err(e) = handle.await.unwrap() {
            bail!("Failure reported in activate task {:?}", e);
        }

        info!(log, "[{c}] Verify volume after replacement");
        if let Err(e) = verify_volume(volume, di, false).await {
            bail!("Requested volume verify failed: {:?}", e)
        }

        // Start up the old downstairs so it is ready for the next loop.
        let res = dsc_client.dsc_start(old_ds).await;
        info!(log, "[{c}] Replay: started {old_ds}, returned:{:?}", res);

        // Wait for all IO to finish before we continue
        loop {
            let wc = volume.show_work().await?;
            info!(
                log,
                "[{c}] Jobs Up:{} Ds:{}  DS active:{}",
                wc.up_count,
                wc.ds_count,
                wc.active_count
            );
            if wc.up_count + wc.ds_count == 0 && wc.active_count == ds_total {
                info!(log, "[{c}] All jobs finished, all DS active.");
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }

        old_ds = (old_ds + 1) % (ds_total as u32 + 1);
        new_ds = (new_ds + 1) % (ds_total + 1);

        match wtq {
            WhenToQuit::Count { count } => {
                if c > count {
                    break;
                }
            }
            WhenToQuit::Signal {
                ref mut shutdown_rx,
            } => {
                match shutdown_rx.try_recv() {
                    Ok(SignalAction::Shutdown) => {
                        println!("shutting down in response to SIGUSR1");
                        break;
                    }
                    Ok(SignalAction::Verify) => {
                        println!("Verify Volume");
                        if let Err(e) = verify_volume(volume, di, false).await {
                            bail!("Requested volume verify failed: {:?}", e)
                        }
                    }
                    _ => {} // Ignore everything else
                }
            }
        }
    }

    info!(log, "Test replace_before_active has completed");
    Ok(())
}

// Test the replacement of a downstairs.
// Send a little IO, send in a request to replace a downstairs, then send a
// bunch more IO.  Wait for all IO to finish (on all three downstairs) before
// we continue.
async fn replace_workload(
    volume: &Volume,
    wtq: &mut WhenToQuit,
    di: &mut DiskInfo,
    targets: Vec<SocketAddr>,
    fill: bool,
) -> Result<()> {
    assert!(targets.len() % 3 == 1);

    // The total number of downstairs we have that are part of the Volume.
    let ds_total = targets.len() - 1;

    if fill {
        fill_sparse_workload(volume, di).await?;
    }
    // Make a copy of the stop at counter if one was provided so the
    // IO task and the replace task don't have to share wtq
    let stop_at = match wtq {
        WhenToQuit::Count { count } => Some(*count),
        _ => None,
    };

    // Start up a task to do the replacement workload
    let stop_token = CancellationToken::new();
    let stop_token_c = stop_token.clone();
    let (work_count_tx, mut work_count_rx) = mpsc::channel(1);
    let volume_c = volume.clone();
    let handle = tokio::spawn(async move {
        let mut old_ds = 0;
        let mut new_ds = ds_total;
        let mut c = 1;
        loop {
            println!(
                "[{c}] Replacing DS {old_ds}:{} with {new_ds}:{}",
                targets[old_ds], targets[new_ds],
            );
            match volume_c
                .replace_downstairs(
                    Uuid::new_v4(),
                    targets[old_ds],
                    targets[new_ds],
                )
                .await
            {
                Ok(ReplaceResult::Started) => {}
                x => {
                    bail!("[{c}] Failed replace: {:?}", x);
                }
            }
            // Wait for the replacement to be reflected in the downstairs status.
            let mut wc = volume_c.show_work().await?;
            while wc.active_count == ds_total {
                // Wait for one of the DS to start repair
                println!(
                    "[{c}] Waiting for replace to start: up:{} ds:{} act:{}",
                    wc.up_count, wc.ds_count, wc.active_count
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
                wc = volume_c.show_work().await?;
            }

            // We have started live repair, now wait for it to finish.
            while wc.active_count != ds_total {
                println!(
                    "[{c}] Waiting for replace to finish: up:{} ds:{} act:{}",
                    wc.up_count, wc.ds_count, wc.active_count
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
                wc = volume_c.show_work().await?;
            }
            // Tell the global thread that a repair is done and what count we
            // are at.
            work_count_tx.send(c).await.expect("Failed to send count");

            // Check if the IO task has asked us to quit.
            if stop_token_c.is_cancelled() {
                break;
            }
            // See (if provided) we have reached a requested number of loops
            if let Some(stop_at) = stop_at {
                if c >= stop_at {
                    println!("[{c}] Replace task ends as count was reached");
                    break;
                }
            }

            // No stopping yet, let's do another loop.
            old_ds = (old_ds + 1) % (ds_total + 1);
            new_ds = (new_ds + 1) % (ds_total + 1);
            c += 1;
        }
        println!("Replace tasks ends after {c} loops");
        Ok(())
    });

    // The replace is started, now generate traffic through the volume
    // in a loop, checking to see if it is time to stop.
    loop {
        let mut workload_wtq = WhenToQuit::Count { count: 100 };
        generic_workload(volume, &mut workload_wtq, di, false, false)
            .await
            .unwrap();

        let io_count = work_count_rx.try_recv().unwrap_or(0);
        println!("IO task with {io_count} replacements ");

        match wtq {
            WhenToQuit::Count { count } => {
                if io_count >= *count {
                    println!("IO task shutting down for count");
                    break;
                }
            }
            WhenToQuit::Signal { shutdown_rx } => {
                match shutdown_rx.try_recv() {
                    Ok(SignalAction::Shutdown) => {
                        println!("IO task shutting down from SIGUSR1");
                        break;
                    }
                    Ok(SignalAction::Verify) => {
                        if let Err(e) = verify_volume(volume, di, false).await {
                            bail!("Requested volume verify failed: {:?}", e)
                        }
                    }
                    _ => {} // Ignore everything else
                }
            }
        }
    }

    // The replace task may have already exited.
    stop_token.cancel();
    if let Err(e) = handle.await.unwrap() {
        bail!("Failed in replace workload: {:?}", e);
    }

    // Wait for all IO to settle down and all downstairs to be active
    // before we do the next loop.
    loop {
        let wc = volume.show_work().await?;
        println!(
            "Replace test done: up:{} ds:{} act:{}",
            wc.up_count, wc.ds_count, wc.active_count
        );
        if wc.up_count + wc.ds_count == 0 && wc.active_count == ds_total {
            println!("Replace: All jobs finished, all DS active.");
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
    }

    println!("Test replace has completed");
    Ok(())
}

/*
 * Do a few writes to random offsets then exit as soon as they finish.
 * We are trying to leave extents "dirty" so we want to exit before the
 * automatic flush can come through and sync our data.
 */
async fn dirty_workload(
    volume: &Volume,
    di: &mut DiskInfo,
    count: usize,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    /*
     * To store our write requests
     */
    let mut futureslist = FuturesOrdered::new();

    let size = 1;
    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let block_max = di.volume_info.total_blocks() - size + 1;
    let count_width = count.to_string().len();
    for c in 1..=count {
        let block_index = rng.random_range(0..block_max);
        /*
         * Convert offset and length to their byte values.
         */
        let offset = BlockIndex(block_index as u64);

        /*
         * Update the write count for the block we plan to write to.
         */
        di.write_log.update_wc(block_index);

        let data = fill_vec(
            block_index,
            size,
            &di.write_log,
            di.volume_info.block_size,
        );

        println!(
            "[{:>0width$}/{:>0width$}] Write at block {}, len:{}",
            c,
            count,
            offset.0,
            data.len(),
            width = count_width,
        );

        let future = volume.write(offset, data);
        futureslist.push_back(future);
    }
    println!("loop over {} futures", futureslist.len());
    while let Some(r) = futureslist.next().await {
        r?;
    }
    Ok(())
}

/// Prints a pleasant summary of the given disk
fn print_disk_description(di: &DiskInfo, encrypted: bool) {
    println!("disk info:");
    let block_size = di.volume_info.block_size;
    println!("  block size:                   {} bytes", block_size);
    for (index, sv) in di.volume_info.volumes.iter().enumerate() {
        println!(
            "  sub_volume {index} blocks / extent: {}",
            sv.blocks_per_extent
        );
        println!("  sub_volume {index} extent count:    {}", sv.extent_count);
        println!(
            "  sub_volume {index} extent size:     {}",
            human_bytes((block_size * sv.blocks_per_extent) as f64)
        );
    }
    println!(
        "  total blocks:                 {}",
        di.volume_info.total_blocks()
    );
    let total_size = di.volume_info.total_size();
    println!(
        "  total size:                   {}",
        human_bytes(total_size as f64)
    );
    println!(
        "  encryption:                   {}",
        if encrypted { "yes" } else { "no" }
    );
}

async fn rand_read_write_workload(
    volume: &Volume,
    di: &mut DiskInfo,
    cfg: RandReadWriteConfig,
) -> Result<()> {
    // Before we start, make sure the work queues are empty.
    loop {
        let wc = volume.query_work_queue().await?;
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let desc = match cfg.mode {
        RandReadWriteMode::Read => "read",
        RandReadWriteMode::Write => "write",
    };

    if cfg.fill {
        println!("filling disk before {desc}");
        fill_workload(volume, di, true).await?;
    }

    let total_blocks = di.volume_info.total_blocks();
    if cfg.blocks_per_io > total_blocks {
        bail!("too many blocks per IO; can't exceed {}", total_blocks);
    }

    let block_size = di.volume_info.block_size as usize;
    println!(
        "\n----------------------------------------------\
        \nrandom {desc} with {} chunks ({} block{})",
        human_bytes((cfg.blocks_per_io as u64 * block_size as u64) as f64),
        cfg.blocks_per_io,
        if cfg.blocks_per_io > 1 { "s" } else { "" },
    );
    print_disk_description(di, cfg.encrypted);
    println!("----------------------------------------------");

    let stop = Arc::new(AtomicBool::new(false));
    let byte_count = Arc::new(AtomicUsize::new(0));

    let mut workers = vec![];
    for _ in 0..cfg.io_depth {
        let stop = stop.clone();
        let volume = volume.clone();
        let byte_count = byte_count.clone();
        let handle = tokio::spawn(async move {
            match cfg.mode {
                RandReadWriteMode::Write => {
                    let mut buf = BytesMut::new();
                    buf.resize(cfg.blocks_per_io * block_size, 0u8);
                    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();
                    rng.fill_bytes(&mut buf);
                    while !stop.load(Ordering::Acquire) {
                        let offset = rng
                            .random_range(0..=total_blocks - cfg.blocks_per_io);
                        volume
                            .write(BlockIndex(offset as u64), buf.clone())
                            .await
                            .unwrap();
                        byte_count.fetch_add(buf.len(), Ordering::Relaxed);
                    }
                }
                RandReadWriteMode::Read => {
                    let mut buf = Buffer::new(cfg.blocks_per_io, block_size);
                    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();
                    while !stop.load(Ordering::Acquire) {
                        let offset = rng
                            .random_range(0..=total_blocks - cfg.blocks_per_io);
                        volume
                            .read(BlockIndex(offset as u64), &mut buf)
                            .await
                            .unwrap();
                        byte_count.fetch_add(buf.len(), Ordering::Relaxed);
                    }
                }
            }
            Ok::<(), CrucibleError>(())
        });
        workers.push(handle);
    }

    // Initial sleep
    let mut prev = 0;

    let subsample_delay = Duration::from_secs_f64(
        cfg.sample_time_secs / cfg.subsample_count.get() as f64,
    );
    let mut samples = vec![];
    let mut first = true;
    let mut next_time = Instant::now() + subsample_delay;
    let stop_time = Instant::now() + Duration::from_secs(cfg.time_secs);
    'outer: loop {
        // Store speeds in bytes/sec, correcting for our sub-second sample time
        let start = samples.len();
        for _ in 0..cfg.subsample_count.get() {
            tokio::time::sleep_until(next_time).await;
            let bytes = byte_count.load(Ordering::Acquire);
            next_time += subsample_delay;
            samples.push((bytes - prev) as f64 / subsample_delay.as_secs_f64());
            prev = bytes;
            if Instant::now() >= stop_time {
                break 'outer;
            }
        }
        // Ignore the first round of samples, to reduce startup effects
        if std::mem::take(&mut first) {
            samples.clear();
            continue;
        }
        let slice = &samples[start..];
        let mean = slice.iter().sum::<f64>() / slice.len() as f64;
        let stdev = (slice.iter().map(|&d| (d - mean).powi(2)).sum::<f64>()
            / slice.len() as f64)
            .sqrt();
        if cfg.raw {
            println!("{mean:>14} {}", stdev as usize);
        } else {
            println!(
                "{:>14} ± {:<16}  ({} IOPS)",
                format!("{}/sec", human_bytes(mean)),
                format!("{}/sec", human_bytes(stdev)),
                mean as usize / (cfg.blocks_per_io * block_size),
            );
        }
        std::io::stdout().lock().flush().unwrap();
    }
    stop.store(true, Ordering::Release);

    // Join all workers
    for h in workers {
        h.await??;
    }

    // Spawn a secondary worker to wait and log during volume deactivation
    let stop = Arc::new(AtomicBool::new(false));
    {
        let volume = volume.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok(wq) = volume.query_work_queue().await else {
                    break;
                };
                print!(
                    "\r stopping volume [{} jobs remaining]    ",
                    wq.ds_count + wq.up_count
                );
                std::io::stdout().lock().flush().unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    volume.deactivate().await?;
    stop.store(true, Ordering::Relaxed);

    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let stdev = (samples.iter().map(|&d| (d - mean).powi(2)).sum::<f64>()
        / samples.len() as f64)
        .sqrt();
    println!(
        "\r----------------------------------------------\
        \naverage {desc} performance: {}/sec ± {}/sec ({} IOPS)",
        human_bytes(mean),
        human_bytes(stdev),
        mean as usize / (cfg.blocks_per_io * block_size),
    );

    Ok(())
}

async fn bufferbloat_workload(
    volume: &Volume,
    di: &mut DiskInfo,
    cfg: BufferbloatConfig,
) -> Result<()> {
    // Before we start, make sure the work queues are empty.
    loop {
        let wc = volume.query_work_queue().await?;
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let total_blocks = di.volume_info.total_blocks();
    if cfg.blocks_per_io > total_blocks {
        bail!("too many blocks per IO; can't exceed {}", total_blocks);
    }

    let block_size = di.volume_info.block_size as usize;
    println!(
        "\n----------------------------------------------\
        \nbufferbloat test\
        \n----------------------------------------------\
        \ninitial {:?} sec random write with {} chunks ({} block{})",
        cfg.time_secs,
        human_bytes((cfg.blocks_per_io as u64 * block_size as u64) as f64),
        cfg.blocks_per_io,
        if cfg.blocks_per_io > 1 { "s" } else { "" },
    );
    print_disk_description(di, cfg.encrypted);
    println!("----------------------------------------------");

    let stop = Arc::new(AtomicBool::new(false));
    let byte_count = Arc::new(AtomicUsize::new(0));

    let mut workers = vec![];
    for _ in 0..cfg.io_depth {
        let stop = stop.clone();
        let volume = volume.clone();
        let byte_count = byte_count.clone();
        let handle = tokio::spawn(async move {
            let mut buf = BytesMut::new();
            buf.resize(cfg.blocks_per_io * block_size, 0u8);
            let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();
            rng.fill_bytes(&mut buf);
            while !stop.load(Ordering::Acquire) {
                let offset =
                    rng.random_range(0..=total_blocks - cfg.blocks_per_io);
                volume
                    .write(BlockIndex(offset as u64), buf.clone())
                    .await
                    .unwrap();
                byte_count.fetch_add(buf.len(), Ordering::Relaxed);
            }
            Ok::<(), CrucibleError>(())
        });
        workers.push(handle);
    }

    let start = std::time::Instant::now();
    let sleep_time = Duration::from_secs(cfg.time_secs);
    {
        let volume = volume.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let Ok(wq) = volume.query_work_queue().await else {
                    break;
                };
                if sleep_time > start.elapsed() {
                    let dt = sleep_time - start.elapsed();
                    print!(
                        "\r{} jobs in queue, {:.1?} secs remaining      ",
                        wq.ds_count + wq.up_count,
                        dt,
                    );
                    std::io::stdout().lock().flush().unwrap();
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    // Sleep to fill up the buffers
    tokio::time::sleep(Duration::from_secs(cfg.time_secs)).await;

    // Create a read action
    let read = {
        let g = volume.clone();
        tokio::spawn(async move {
            let mut buffer = Buffer::new(1, block_size);
            let start = std::time::Instant::now();
            g.read(BlockIndex(0), &mut buffer).await.unwrap();
            start.elapsed()
        })
    };
    stop.store(true, Ordering::Release);

    // Join all workers
    for h in workers {
        h.await??;
    }

    println!("\nawaiting final read...");
    let read_time = read.await?;
    println!("read took {read_time:?}");

    volume.flush(None).await?;

    Ok(())
}

/*
 * Generate a random offset and length, and write to then read from
 * that offset/length.  Verify the data is what we expect.
 */
async fn one_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let size = 1;
    let block_max = di.volume_info.total_blocks() - size + 1;
    let block_index = rng.random_range(0..block_max);

    /*
     * Convert offset and length to their byte values.
     */
    let offset = BlockIndex(block_index as u64);

    /*
     * Update the write count for the block we plan to write to.
     */
    di.write_log.update_wc(block_index);

    let block_size = di.volume_info.block_size;
    let data = fill_vec(block_index, size, &di.write_log, block_size);

    println!("Write at block {:5}, len:{:7}", offset.0, data.len());

    volume.write(offset, data).await?;

    let mut data = crucible::Buffer::repeat(255, size, block_size as usize);

    println!("Read  at block {:5}, len:{:7}", offset.0, data.len());
    volume.read(offset, &mut data).await?;

    let dl = data.into_bytes();
    match validate_vec(dl, block_index, &mut di.write_log, block_size, false) {
        ValidateStatus::Bad | ValidateStatus::InRange => {
            bail!("Error at {}", block_index);
        }
        ValidateStatus::Good => {}
    }

    println!("Flush");
    volume.flush(None).await?;

    Ok(())
}

/*
 * A test of deactivation and re-activation.
 * In a loop, do some IO, then deactivate, then activate.  Verify that
 * written data is read back.  We make use of the generic_workload test
 * for the IO parts of this.
 */
async fn deactivate_workload(
    volume: &Volume,
    count: usize,
    di: &mut DiskInfo,
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
        let mut wtq = WhenToQuit::Count { count: 20 };
        generic_workload(volume, &mut wtq, di, false, false).await?;
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
        volume.deactivate().await?;
        println!(
            "{:>0width$}/{:>0width$}, CLIENT: Now disconnect done.",
            c,
            count,
            width = count_width
        );
        let wc = volume.show_work().await?;
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
        while let Err(e) = volume.activate_with_gen(gen).await {
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
    let mut wtq = WhenToQuit::Count { count: 20 };
    generic_workload(volume, &mut wtq, di, false, false).await?;

    Ok(())
}

/*
 * Generate a random offset and length, and write, flush, then read from
 * that offset/length.  Verify the data is what we expect.
 */
async fn write_flush_read_workload(
    volume: &Volume,
    count: usize,
    di: &mut DiskInfo,
) -> Result<()> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    let count_width = count.to_string().len();
    let block_size = di.volume_info.block_size;
    for c in 1..=count {
        /*
         * Pick a random size (in blocks) for the IO, up to the size of the
         * max IO we allow.
         */
        let size = rng.random_range(1..=di.max_block_io);

        /*
         * Once we have our IO size, decide where the starting offset should
         * be, which is the total possible size minus the randomly chosen
         * IO size.
         */
        let block_max = di.volume_info.total_blocks() - size + 1;
        let block_index = rng.random_range(0..block_max);

        /*
         * Convert offset and length to their byte values.
         */
        let offset = BlockIndex(block_index as u64);

        /*
         * Update the write count for all blocks we plan to write to.
         */
        for i in 0..size {
            di.write_log.update_wc(block_index + i);
        }

        let data = fill_vec(block_index, size, &di.write_log, block_size);

        println!(
            "{:>0width$}/{:>0width$} IO at block {:5}, len:{:7}",
            c,
            count,
            offset.0,
            data.len(),
            width = count_width,
        );
        volume.write(offset, data).await?;

        volume.flush(None).await?;

        let mut data = crucible::Buffer::repeat(255, size, block_size as usize);
        volume.read(offset, &mut data).await?;

        let dl = data.into_bytes();
        match validate_vec(
            dl,
            block_index,
            &mut di.write_log,
            block_size,
            false,
        ) {
            ValidateStatus::Bad | ValidateStatus::InRange => {
                bail!("Error at {}", block_index);
            }
            ValidateStatus::Good => {}
        }
    }

    Ok(())
}

/*
 * Send bursts of work to the demo_workload function.
 * Wait for each burst to finish, pause, then loop.
 */
async fn burst_workload(
    volume: &Volume,
    count: usize,
    demo_count: usize,
    di: &mut DiskInfo,
    verify_out: &Option<PathBuf>,
) -> Result<()> {
    let count_width = count.to_string().len();
    for c in 1..=count {
        demo_workload(volume, demo_count, di).await?;
        let mut wc = volume.show_work().await?;
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
            wc = volume.show_work().await?;
        }

        /*
         * Once everyone is caught up, save the state just in case
         * the user wants to quit at this pause step
         */
        println!();
        if let Some(vo) = &verify_out {
            write_json(vo, &di.write_log, true)?;
            println!("Wrote out file {vo:?} at this time");
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
    volume: &Volume,
    count: usize,
    di: &mut DiskInfo,
) -> Result<()> {
    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    // TODO: Allow user to request r/w/f percentage (how???)
    // We want at least one write, otherwise there will be nothing to
    // repair.
    let mut one_write = false;
    // These help the printlns use the minimum white space
    let count_width = count.to_string().len();
    let total_blocks = di.volume_info.total_blocks();
    let block_width = total_blocks.to_string().len();
    let block_size = di.volume_info.block_size;
    let size_width = (10 * block_size).to_string().len();
    for c in 1..=count {
        let op = rng.random_range(0..10);
        // Make sure the last few commands are not a flush
        if c + 3 < count && op == 0 {
            // flush
            println!(
                "{:>0width$}/{:>0width$} Flush",
                c,
                count,
                width = count_width,
            );
            volume.flush(None).await?;
            // Commit the current write log because we know this flush
            // will make it out on at least two DS, so any writes before this
            // point should also be persistent.
            // Note that we don't want to commit on every write, because
            // those writes might not make it if we have three dirty extents
            // and the one we choose could be the one that does not have
            // the write (no flush, no guarantee of persistence).
            di.write_log.commit();
            // Make sure a write comes next.
            one_write = false;
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to 10
            let size = rng.random_range(1..=10);

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = total_blocks - size + 1;
            let block_index = rng.random_range(0..block_max);

            // Convert offset and length to their byte values.
            let offset = BlockIndex(block_index as u64);

            if !one_write || op <= 4 {
                // Write
                one_write = true;
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    di.write_log.update_wc(block_index + i);
                }

                let data =
                    fill_vec(block_index, size, &di.write_log, block_size);

                print!(
                    "{:>0width$}/{:>0width$} Write \
                    block {:>bw$}  len {:>sw$}  data:",
                    c,
                    count,
                    offset.0,
                    data.len(),
                    width = count_width,
                    bw = block_width,
                    sw = size_width,
                );
                assert_eq!(data[1], di.write_log.get_seed(block_index));
                for i in 0..size {
                    print!("{:>3} ", di.write_log.get_seed(block_index + i));
                }
                println!();

                volume.write(offset, data).await?;
            } else {
                // Read
                let mut data =
                    crucible::Buffer::repeat(255, size, block_size as usize);
                println!(
                    "{:>0width$}/{:>0width$} Read  \
                    block {:>bw$}  len {:>sw$}",
                    c,
                    count,
                    offset.0,
                    data.len(),
                    width = count_width,
                    bw = block_width,
                    sw = size_width,
                );
                volume.read(offset, &mut data).await?;
            }
        }
    }
    volume.show_work().await?;
    Ok(())
}

/*
 * Like the random test, but with IO not as large, and with frequent
 * showing of the internal work queues.  Submit a bunch of random IOs,
 * then watch them complete.
 */
async fn demo_workload(
    volume: &Volume,
    count: usize,
    di: &mut DiskInfo,
) -> Result<()> {
    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_os_rng();

    // Because this workload issues a bunch of IO all at the same time,
    // we can't be sure the order will be preserved for our IOs.
    // We take the state of the volume now as our minimum, and verify
    // that the read at the end of this loop finds some value between
    // what it is now and what it is at the end of test.
    di.write_log.commit();

    let mut read_futures = FuturesOrdered::new();
    let mut write_futures = FuturesOrdered::new();

    // TODO: Let the user select the number of loops
    // TODO: Allow user to request r/w/f percentage (how???)
    for _ in 1..=count {
        let op = rng.random_range(0..10);
        if op == 0 {
            // flush
            let future = volume.flush(None);
            write_futures.push_back(future);
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to 10
            let size = rng.random_range(1..=10);

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = di.volume_info.total_blocks() - size + 1;
            let block_index = rng.random_range(0..block_max);

            // Convert offset and length to their byte values.
            let offset = BlockIndex(block_index as u64);

            if op <= 4 {
                // Write
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    di.write_log.update_wc(block_index + i);
                }

                let data = fill_vec(
                    block_index,
                    size,
                    &di.write_log,
                    di.volume_info.block_size,
                );

                let future = volume.write(offset, data);
                write_futures.push_back(future);
            } else {
                // Read
                let block_size = di.volume_info.block_size as usize;
                let future = {
                    let volume = volume.clone();
                    tokio::spawn(async move {
                        let mut data =
                            crucible::Buffer::repeat(255, size, block_size);
                        volume.read(offset, &mut data).await?;
                        Ok(data)
                    })
                };
                read_futures.push_back(future);
            }
        }
    }
    let mut wc = WQCounts {
        up_count: 0,
        ds_count: 0,
        active_count: 0,
    };
    println!(
        "Submit work and wait for {} jobs to finish",
        write_futures.len() + read_futures.len()
    );
    while let Some(result) = write_futures.next().await {
        result?;
    }

    while let Some(result) = read_futures.next().await {
        let result: Result<Buffer, CrucibleError> = result?;
        let _buf = result?;
    }

    /*
     * Continue loping until all downstairs jobs finish also.
     */
    println!("All submitted jobs completed, waiting for downstairs");
    while wc.up_count + wc.ds_count > 0 {
        wc = volume.show_work().await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
    println!("All downstairs jobs completed.");

    // Commit the current state as the new minimum for any future tests.
    di.write_log.commit();

    Ok(())
}

/*
 * This is a test workload that generates a single write spanning an extent
 * then will try to read the same.  When multiple sub_volumes are present we
 * also issue a Write/Flush/Read that will span the two sub_volumes.
 */
async fn span_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    let mut extent_block_start = 0;
    let last_sub_volume = di.volume_info.volumes.len() - 1;
    let block_size = di.volume_info.block_size;
    for (index, sv) in di.volume_info.volumes.iter().enumerate() {
        // Pick the last block in the first extent
        let extent_size = sv.blocks_per_extent as usize;
        let block_index = extent_block_start + extent_size - 1;

        // Update the counter for the blocks we are about to write.
        di.write_log.update_wc(block_index);
        di.write_log.update_wc(block_index + 1);

        let offset = BlockIndex(block_index as u64);
        let data = fill_vec(block_index, 2, &di.write_log, block_size);

        println!(
            "sub_volume:{} Block:{} Send a write spanning two extents",
            index, block_index
        );
        volume.write(offset, data).await?;

        println!("sub_volume:{index} Send a flush");
        volume.flush(None).await?;

        println!(
            "sub_volume:{} Block:{} Send a read spanning two extents",
            index, block_index
        );
        let mut data = crucible::Buffer::repeat(99, 2, block_size as usize);
        volume.read(offset, &mut data).await?;

        let dl = data.into_bytes();
        match validate_vec(
            dl,
            block_index,
            &mut di.write_log,
            block_size,
            false,
        ) {
            ValidateStatus::Bad | ValidateStatus::InRange => {
                bail!("Span read verify failed");
            }
            ValidateStatus::Good => {}
        }

        // Move to the start of the next extent.
        extent_block_start += extent_size * sv.extent_count as usize;

        // If our sub volume is not the last sub volume, do an IO that will
        // span this one and the next.
        if index < last_sub_volume {
            let block_index = extent_block_start - 1;

            // Update the counter for the blocks we are about to write.
            di.write_log.update_wc(block_index);
            di.write_log.update_wc(block_index + 1);

            let offset = BlockIndex(block_index as u64);
            let data = fill_vec(block_index, 2, &di.write_log, block_size);

            println!(
                "sub_volume:{} Block:{} Send a write spanning two sub_volumes",
                index, block_index
            );
            volume.write(offset, data).await?;

            println!("sub_volume:{index} Send a flush");
            volume.flush(None).await?;

            println!(
                "sub_volume:{} Block:{} Send a read spanning two sub volumes",
                index, block_index
            );
            let mut data = crucible::Buffer::repeat(99, 2, block_size as usize);
            volume.read(offset, &mut data).await?;

            let dl = data.into_bytes();
            match validate_vec(
                dl,
                block_index,
                &mut di.write_log,
                block_size,
                false,
            ) {
                ValidateStatus::Bad | ValidateStatus::InRange => {
                    bail!("Span read verify failed");
                }
                ValidateStatus::Good => {}
            }
        }
    }
    Ok(())
}

/*
 * Write, flush, then read every block in the volume.
 * We wait for each op to finish, so this is all sequential.
 */
async fn big_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    for block_index in 0..di.volume_info.total_blocks() {
        /*
         * Update the write count for all blocks we plan to write to.
         */
        di.write_log.update_wc(block_index);

        let data =
            fill_vec(block_index, 1, &di.write_log, di.volume_info.block_size);
        /*
         * Convert block_index to its byte value.
         */
        let offset = BlockIndex(block_index as u64);

        volume.write(offset, data).await?;

        volume.flush(None).await?;

        let mut data = crucible::Buffer::repeat(
            255,
            1,
            di.volume_info.block_size as usize,
        );
        volume.read(offset, &mut data).await?;

        let dl = data.into_bytes();
        match validate_vec(
            dl,
            block_index,
            &mut di.write_log,
            di.volume_info.block_size,
            false,
        ) {
            ValidateStatus::Bad | ValidateStatus::InRange => {
                bail!("Verify error at block:{}", block_index);
            }
            ValidateStatus::Good => {}
        }
    }

    println!("All IOs sent");
    volume.show_work().await?;

    Ok(())
}

async fn biggest_io_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    /*
     * Based on our protocol, send the biggest IO we can.
     */
    println!("determine blocks for large io");
    let total_blocks = di.volume_info.total_blocks();
    let biggest_io_in_blocks = {
        let crucible_max_io =
            crucible_protocol::CrucibleEncoder::max_io_blocks(
                di.volume_info.block_size as usize,
            )?;

        if crucible_max_io < total_blocks {
            crucible_max_io
        } else {
            println!(
                "Volume total blocks {} smaller than max IO blocks {}",
                total_blocks, crucible_max_io,
            );
            total_blocks
        }
    };

    println!(
        "Using {} as the largest single IO (in blocks)",
        biggest_io_in_blocks
    );
    let mut block_index = 0;
    while block_index < total_blocks {
        let offset = BlockIndex(block_index as u64);

        let next_io_blocks =
            if block_index + biggest_io_in_blocks > total_blocks {
                total_blocks - block_index
            } else {
                biggest_io_in_blocks
            };

        for i in 0..next_io_blocks {
            di.write_log.update_wc(block_index + i);
        }

        let data = fill_vec(
            block_index,
            next_io_blocks,
            &di.write_log,
            di.volume_info.block_size,
        );

        println!(
            "IO at block:{}  size in blocks:{}",
            block_index, next_io_blocks
        );

        volume.write(offset, data).await?;

        block_index += next_io_blocks;
    }

    Ok(())
}

/*
 * A loop that generates a bunch of random reads and writes, increasing the
 * offset each operation.  After 20 are submitted, we wait for all to finish.
 *
 * TODO: Make this test use the global write count, but remember, async.
 */
async fn dep_workload(volume: &Volume, di: &mut DiskInfo) -> Result<()> {
    let total_size = di.volume_info.total_size();
    let final_offset = total_size - di.volume_info.block_size;

    let mut my_offset: u64 = 0;
    for my_count in 1..150 {
        let mut write_futures = FuturesOrdered::new();
        let mut read_futures = FuturesOrdered::new();
        let block_size = di.volume_info.block_size;
        /*
         * Generate some number of operations
         */
        for ioc in 0..200 {
            my_offset = (my_offset + block_size) % final_offset;
            if rand::random_bool(0.5) {
                /*
                 * Generate a write buffer with a locally unique value.
                 */
                let mut data = BytesMut::with_capacity(block_size as usize);
                let seed = ((my_offset % 254) + 1) as u8;
                data.extend(std::iter::repeat_n(seed, block_size as usize));

                println!(
                    "Loop:{} send write {} @ offset:{}  len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );
                let future = volume.write_to_byte_offset(my_offset, data);
                write_futures.push_back(future);
            } else {
                let mut data =
                    crucible::Buffer::repeat(0, 1, block_size as usize);

                println!(
                    "Loop:{} send read  {} @ offset:{} len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );

                let future = {
                    let volume = volume.clone();
                    tokio::spawn(async move {
                        volume
                            .read_from_byte_offset(my_offset, &mut data)
                            .await?;
                        Ok(data)
                    })
                };
                read_futures.push_back(future);
            }
        }

        volume.show_work().await?;

        // The final flush is to help prevent the pause that we get when the
        // last command is a write or read and we have to wait x seconds for the
        // flush check to trigger.
        println!("Loop:{} send a final flush and wait", my_count);
        let flush_future = volume.flush(None);
        write_futures.push_back(flush_future);

        println!(
            "Loop:{} loop over {} futures",
            my_count,
            write_futures.len() + read_futures.len()
        );

        while let Some(result) = write_futures.next().await {
            result?;
        }
        while let Some(result) = read_futures.next().await {
            let result: Result<_, CrucibleError> = result?;
            let _buffer = result?;
        }

        println!("Loop:{} all futures done", my_count);
        volume.show_work().await?;
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
        // Rollover of u8 at 255
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 249);
        assert_eq!(write_log.get_seed(0), 249);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 250);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 251);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 252);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 253);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 254);
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 255);

        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 0);
        // Seed at zero does not mean the counter is zero
        assert!(!write_log.unwritten(0));
        write_log.update_wc(0);
        assert_eq!(write_log.get_seed(0), 1);
    }

    #[test]
    fn test_wl_empty() {
        // No size is empty
        let write_log = WriteLog::new(0);
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
        write_log.set_wc(0, 255);
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
    fn test_wl_commit_range_rollover_range() {
        // validate seed range works if write log seed rolls over and
        // the min max are away from the rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 254);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert!(!write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_range_rollover_min_at() {
        // validate seed range works if write log seed rolls over and
        // the min is at the rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 255);
        write_log.commit();
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert!(!write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_range_rollover_max_at() {
        // validate seed range works if write log seed rolls over and
        // the max is at the rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 253);
        write_log.commit();
        write_log.update_wc(0); // 254
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        assert_eq!(write_log.get_seed(0), 0);
        assert!(!write_log.validate_seed_range(0, 252, false));
        assert!(write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(!write_log.validate_seed_range(0, 1, false));
    }

    #[test]
    fn test_wl_commit_range_update_rollover_below() {
        // validate seed range works if write log seed rolls over when
        // our range value is below the rollover point.
        // Make sure the new range is updated.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 254);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert!(!write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, true));
        // Once we make a new max, the old range is invalid.
        assert!(!write_log.validate_seed_range(0, 255, false));
        assert!(!write_log.validate_seed_range(0, 0, false));
        assert!(!write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_range_update_rollover_above() {
        // validate seed range works if write log seed rolls over when
        // our range value is across the rollover point.
        // Make sure the new range is updated.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 254);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        // Below the range is not okay.
        assert!(!write_log.validate_seed_range(0, 253, false));
        // Pick a "new" range just above the roll over point.
        assert!(write_log.validate_seed_range(0, 0, true));
        // Lower range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        // Anything above is no longer valid.
        assert!(!write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_range_no_update_below_rollover() {
        // validate no change to our expected value when in a
        // rollover situation if the value is below the expected
        // range.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 254);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);

        // Below the range is not okay, and there is no update.
        assert!(!write_log.validate_seed_range(0, 253, true));
        assert_eq!(write_log.count_cur[0], 257);

        // All range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_range_no_update_above_rollover() {
        // validate no change to our expected value when in a
        // rollover situation if the value is above in the expected
        // range.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 254);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        // The actual count continues above 255
        assert_eq!(write_log.count_cur[0], 257);

        // Above the range is not okay, and there is no update.
        assert!(!write_log.validate_seed_range(0, 2, true));
        assert_eq!(write_log.count_cur[0], 257);

        // All range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
    }

    // More rollover tests, but starting at the second rollover point
    #[test]
    fn test_wl_commit_1024_range_rollover_range() {
        // validate seed range works if write log seed rolls over
        // at the second rollover point and the min max are away
        // from the second rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1022);
        write_log.commit();
        write_log.update_wc(0); // 255 or 1023
        write_log.update_wc(0); // 0 or 1024
        write_log.update_wc(0); // 1 or 1025
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        assert!(!write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_1024_range_rollover_min_at() {
        // validate seed range works if write log seed rolls over and
        // the min is at the second rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1023);
        write_log.commit();
        write_log.update_wc(0); // 0 or 1024
        write_log.update_wc(0); // 1 or 1025
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        assert!(!write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_1024_range_rollover_max_at() {
        // validate seed range works if write log seed rolls over and
        // the max is at the rollover point
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1021);
        write_log.commit();
        write_log.update_wc(0); // 254
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        assert_eq!(write_log.get_seed(0), 0);
        assert_eq!(write_log.count_cur[0], 1024);
        assert!(!write_log.validate_seed_range(0, 252, false));
        assert!(write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(!write_log.validate_seed_range(0, 1, false));
    }

    #[test]
    fn test_wl_commit_1024_range_update_rollover_below() {
        // validate seed range works if write log seed rolls over when
        // our range value is below the rollover point.
        // Make sure the new range is updated.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1022);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        assert!(!write_log.validate_seed_range(0, 253, false));
        assert!(write_log.validate_seed_range(0, 254, true));
        // The new cur value goes back to 1022
        assert_eq!(write_log.count_cur[0], 1022);
        // Once we make a new max, the old range is invalid.
        assert!(!write_log.validate_seed_range(0, 255, false));
        assert!(!write_log.validate_seed_range(0, 0, false));
        assert!(!write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_1024_range_update_rollover_above() {
        // validate seed range works if write log seed rolls over when
        // our range value is across the rollover point.
        // Make sure the new range is updated.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1022);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        // Below the range is not okay.
        assert!(!write_log.validate_seed_range(0, 253, false));
        // Pick a "new" range just above the roll over point.
        assert!(write_log.validate_seed_range(0, 0, true));
        assert_eq!(write_log.count_cur[0], 1024);
        // Lower range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        // Anything above is no longer valid.
        assert!(!write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_1024_range_no_update_below_rollover() {
        // validate no change to our expected value when in a
        // rollover situation if the value is below the expected
        // range.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1022);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        // Below the range is not okay, and there is no update.
        assert!(!write_log.validate_seed_range(0, 253, true));
        // All range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
        assert!(!write_log.validate_seed_range(0, 2, false));
    }

    #[test]
    fn test_wl_commit_1024_range_no_update_above_rollover() {
        // validate no change to our expected value when in a
        // rollover situation if the value is above in the expected
        // range.
        let mut write_log = WriteLog::new(10);
        write_log.set_wc(0, 1022);
        write_log.commit();
        write_log.update_wc(0); // 255
        write_log.update_wc(0); // 0
        write_log.update_wc(0); // 1
        assert_eq!(write_log.get_seed(0), 1);
        assert_eq!(write_log.count_cur[0], 1025);
        // Above the range is not okay, and there is no update.
        assert!(!write_log.validate_seed_range(0, 2, true));
        assert_eq!(write_log.count_cur[0], 1025);
        // All range values are still okay.
        assert!(write_log.validate_seed_range(0, 254, false));
        assert!(write_log.validate_seed_range(0, 255, false));
        assert!(write_log.validate_seed_range(0, 0, false));
        assert!(write_log.validate_seed_range(0, 1, false));
    }
    // End rollover range tests

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
}
