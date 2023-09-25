// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use clap::Parser;
use serde::Deserialize;
use serde_aux::prelude::*;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Copy, Clone, clap::Subcommand)]
enum Command {
    /// Plot data
    Plot,
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

#[derive(Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct RequestFlags(u32);

impl FromStr for RequestFlags {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<RequestFlags, Self::Err> {
        Ok(RequestFlags(parse_int::parse::<u32>(s)?))
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

fn read_file(opts: &Opts) -> Result<()> {
    let file = File::open(&opts.filename)?;
    let lines = BufReader::new(file).lines();
    let mut records = BTreeMap::new();
    let mut disks = HashSet::new();

    for line in lines {
        let r: Record = serde_json::from_str(&line?)?;

        let ts = r.timestamp();

        if let Record::Request(req) = &r {
            disks.insert(req.disk.clone());
        }

        records.insert(ts, r);
    }

    if opts.disk.is_none() && disks.len() > 1 {
        bail!("multiple disks found ({disks:?}); must specify one");
    }

    let mut total = 0;
    let mut matching = 0;

    for record in records.values() {
        if let Record::Request(req) = record {
            total += 1;

            if let Some(d) = &opts.disk {
                if req.disk != *d {
                    continue;
                }
            }

            matching += 1;
        }
    }

    if let Some(disk) = &opts.disk {
        if matching == 0 {
            bail!(
                "no trace records correspond to \"{disk}\" \
                (expected one of {disks:?})"
            );
        }
    }

    println!("cruplay: {total} total I/O requests found, {matching} matching");

    Ok(())
}

fn main() -> Result<()> {
    let opts: Opts = Opts::parse();
    read_file(&opts)?;
    Ok(())
}
