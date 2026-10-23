// Copyright 2026 Oxide Computer Company

//! crulog - summarize propolis-server logs.
//!
//! Point it at one or more raw (bunyan JSON) propolis-server SMF log
//! files, rotated or not, and it prints the propolis runs, the instance,
//! each disk, and the crucible regions behind each disk.
use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

/// Summarize the instance, disks, and crucible regions found in raw
/// propolis-server log files.
#[derive(Parser)]
#[command(about)]
struct Args {
    /// Print the summary as JSON instead of text.
    #[arg(long)]
    json: bool,

    /// Log files to read.  Rotated logs may be given in any order.
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut inputs = Vec::new();
    for path in &args.files {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading {}", path.display()))?;
        inputs.push((
            path.display().to_string(),
            String::from_utf8_lossy(&bytes).into_owned(),
        ));
    }

    let summary = crulog::summarize(&inputs);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        print!("{}", crulog::render::text(&summary));
    }
    Ok(())
}
