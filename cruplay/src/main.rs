// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;
use serde_aux::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Clone, clap::Args)]
struct PlotOpts {
    /// title for plot
    #[clap(short, long)]
    title: Option<String>,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
    /// Plot data
    Plot(PlotOpts),
}

#[derive(Debug, Parser)]
#[clap(about = "replay I/O for crucible")]
pub struct Opts {
    /// trace file
    #[clap(short, long, required = true)]
    filename: String,

    /// disk to process
    #[clap(short, long)]
    disk: Option<String>,

    #[clap(subcommand)]
    subcommand: Command,
}

#[repr(u8)]
#[allow(non_camel_case_types)]
enum RequestFlagBits {
    __REQ_FAILFAST_DEV = 8, /* no driver retries of device errors */
    __REQ_FAILFAST_TRANSPORT, /* no driver retries of transport errors */
    __REQ_FAILFAST_DRIVER,  /* no driver retries of driver errors */
    __REQ_SYNC,             /* request is sync (sync write or read) */
    __REQ_META,             /* metadata io request */
    __REQ_PRIO,             /* boost priority in cfq */
    __REQ_NOMERGE,          /* don't touch this for merging */
    __REQ_IDLE,             /* anticipate more IO after this one */
    __REQ_INTEGRITY,        /* I/O includes block integrity payload */
    __REQ_FUA,              /* forced unit access */
    __REQ_PREFLUSH,         /* request for cache flush */
    __REQ_RAHEAD,           /* read ahead, can fail anytime */
    __REQ_BACKGROUND,       /* background IO */
    __REQ_NOWAIT,           /* Don't wait if request will block */
    __REQ_POLLED,           /* caller polls for completion using bio_poll */
    __REQ_ALLOC_CACHE,      /* allocate IO from cache if available */
    __REQ_SWAP,             /* swap I/O */
    __REQ_DRV,              /* for driver use */
    __REQ_FS_PRIVATE,       /* for file system (submitter) use */

    /*
     * Command specific flags, keep last:
     */
    /* for REQ_OP_WRITE_ZEROES: */
    __REQ_NOUNMAP, /* do not free blocks when zeroing */
    __REQ_NR_BITS, /* stops here */
}

#[derive(Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct RequestFlags(u32);

impl FromStr for RequestFlags {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<RequestFlags, Self::Err> {
        Ok(RequestFlags(parse_int::parse::<u32>(s)?))
    }
}

impl RequestFlags {
    fn is_read(&self) -> bool {
        self.0 & 1 == 0
    }

    fn is_write(&self) -> bool {
        self.0 & 1 == 1
    }

    fn is_flush(&self) -> bool {
        self.0
            & ((1 << (RequestFlagBits::__REQ_PREFLUSH as u8))
                | (1 << (RequestFlagBits::__REQ_FUA as u8)))
            != 0
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Deserialize)]
struct Request {
    ts: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    flags: RequestFlags,
    disk: String,
    sector: u64,
    size: u32,
    tid: u32,
    request: String,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Deserialize)]
struct Response {
    ts: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    flags: RequestFlags,
    request: String,
    latency: u64,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Deserialize)]
#[serde(untagged)]
enum Record {
    Request(Request),
    Response(Response),
}

impl Record {
    fn timestamp(&self) -> u64 {
        match self {
            Record::Request(req) => req.ts,
            Record::Response(resp) => resp.ts,
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct Stats {
    reads: i32,
    writes: i32,
    flushes: i32,
    read_bytes: i64,
    write_bytes: i64,
}

impl Request {
    fn adjust(&self, stats: &mut Stats, mult: i32) {
        if self.flags.is_read() {
            stats.reads += mult;
            stats.read_bytes += (self.size as i64) * mult as i64;
        }

        if self.flags.is_write() {
            stats.writes += mult;
            stats.write_bytes += (self.size as i64) * mult as i64;
        }

        if self.flags.is_flush() {
            stats.flushes += mult;
        }
    }

    #[allow(dead_code)]
    fn abbr(&self) -> String {
        format!(
            "{}{}{}",
            if self.flags.is_read() { "R" } else { "" },
            if self.flags.is_write() { "W" } else { "" },
            if self.flags.is_flush() { "F" } else { "" },
        )
    }

    fn increment(&self, stats: &mut Stats) {
        self.adjust(stats, 1);
    }

    fn decrement(&self, stats: &mut Stats) {
        self.adjust(stats, -1);
    }
}

fn read_file(
    opts: &Opts,
    callback: impl Fn(&BTreeMap<u64, (&Request, &Response, Stats)>) -> Result<()>,
) -> Result<()> {
    let file = File::open(&opts.filename)?;
    let lines = BufReader::new(file).lines();
    let mut records = BTreeMap::new();
    let mut disks = HashSet::new();

    for (lineno, line) in lines.enumerate() {
        let line = line?;

        if line.is_empty() {
            continue;
        }

        let r: Record = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(err) => {
                bail!("line {lineno}: {err}");
            }
        };

        let ts = r.timestamp();

        if let Record::Request(req) = &r {
            disks.insert(req.disk.clone());
        }

        records.insert(ts, r);
    }

    if opts.disk.is_none() && disks.len() > 1 {
        bail!("multiple disks found ({disks:?}); must specify one");
    }

    let mut outstanding: HashMap<&String, (&Request, Stats)> = HashMap::new();
    let mut operations = BTreeMap::new();
    let mut outstanding_by_start = BTreeSet::new();

    let mut stats: Stats = Default::default();
    let mut inflight: Stats = Default::default();

    let mut missing_responses = 0;
    let threshold = 1_000_000_000u64;

    //
    // Now a pass through to connect a request with a response into an
    // operation.
    //
    for record in records.values() {
        match &record {
            Record::Request(req) => {
                if let Some(d) = &opts.disk {
                    if req.disk != *d {
                        continue;
                    }
                }

                if let Some((old, _)) = outstanding.get(&req.request) {
                    //
                    // Unfortunately, despite our best efforts, bpftrace drops
                    // data silently.  We're going to keep track of the drops.
                    //
                    old.decrement(&mut inflight);
                    missing_responses += 1;
                    outstanding_by_start.remove(&(old.ts, &old.request));
                }

                outstanding.insert(&req.request, (req, inflight.clone()));
                outstanding_by_start.insert((req.ts, &req.request));

                req.increment(&mut stats);
                req.increment(&mut inflight);
            }
            Record::Response(response) => {
                if let Some((req, s)) = outstanding.remove(&response.request) {
                    operations.insert(req.ts, (req, response, s));
                    req.decrement(&mut inflight);
                    outstanding_by_start.remove(&(req.ts, &req.request));
                }
            }
        }

        //
        // Now check to see if we should remove some stale I/Os.
        //
        let now = record.timestamp();

        while let Some((oldest, req)) = outstanding_by_start.first() {
            if now - oldest > threshold {
                if let Some((req, _)) = outstanding.remove(req) {
                    req.decrement(&mut inflight);
                    missing_responses += 1;
                } else {
                    panic!("missing request {req}");
                }

                outstanding_by_start.remove(&(*oldest, req));
            } else {
                break;
            }
        }
    }

    if let Some(disk) = &opts.disk {
        if operations.len() == 0 && outstanding.len() == 0 {
            bail!(
                "no trace records correspond to \"{disk}\" \
                (expected one of {disks:?})"
            );
        }
    }

    eprintln!(
        "cruplay: {} reads, {} writes, {} flushes, {} missing responses",
        stats.reads, stats.writes, stats.flushes, missing_responses
    );

    callback(&operations)
}

fn plot(opts: &Opts, plot_opts: &PlotOpts) -> Result<()> {
    read_file(opts, |operations| {
        let start = match operations.first_key_value() {
            Some((ts, _)) => ts,
            None => {
                bail!("no I/O operations found");
            }
        };

        let emit = |out: &mut File,
                    req: &Request,
                    response: &Response,
                    inflight: &Stats| {
            writeln!(
                out,
                "ts={} offset={} sector={} size={} \
                latency={} rq={} wq={} fq={}",
                req.ts,
                req.ts - start,
                req.sector,
                req.size,
                response.latency,
                inflight.reads,
                inflight.writes,
                inflight.flushes
            )
        };

        let filename = &opts.filename;

        let mut reads = File::create(format!("{}.reads", filename))?;
        let mut writes = File::create(format!("{}.writes", filename))?;
        let mut flushes = File::create(format!("{}.flushes", filename))?;

        for (req, response, inflight) in operations.values() {
            if req.flags.is_read() {
                emit(&mut reads, req, response, inflight)?;
            }

            if req.flags.is_write() {
                emit(&mut writes, req, response, inflight)?;
            }

            if req.flags.is_flush() {
                emit(&mut flushes, req, response, inflight)?;
            }
        }

        let gpl_filename = format!("{}.gpl", filename);
        let mut gpl = File::create(&gpl_filename)?;

        let title = match &plot_opts.title {
            Some(str) => str,
            None => &filename,
        };

        writeln!(
            gpl,
            include_str!("./plot.gpl"),
            filename = filename,
            title = title
        )?;

        std::process::Command::new("gnuplot")
            .arg(gpl_filename)
            .output()
            .with_context(|| format!("failed to execute gnuplot"))?;

        Ok(())
    })?;

    Ok(())
}

fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    match opts.subcommand {
        Command::Plot(ref plot_opts) => {
            plot(&opts, &plot_opts)?;
        }
    }

    Ok(())
}
