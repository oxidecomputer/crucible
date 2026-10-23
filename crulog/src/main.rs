// Copyright 2026 Oxide Computer Company

//! crulog - summarize propolis-server logs.
//!
//! Point it at one or more raw (bunyan JSON) propolis-server SMF log
//! files, rotated or not, and it prints the propolis runs, the instance,
//! each disk, and the crucible regions behind each disk.
//!
//! `crulog panic` instead reports on a crucible panic: the disk it was
//! on, the IO that caused it, and the history of the extent that IO hit.
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Summarize the instance, disks, and crucible regions found in raw
/// propolis-server log files.
#[derive(Parser)]
#[command(about, args_conflicts_with_subcommands = true)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Print the output as JSON instead of text.
    #[arg(long, global = true)]
    json: bool,

    /// Log files to read.  Rotated logs may be given in any order.
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Report on a crucible panic: its disk, the IO that caused it, and
    /// the history of the extent that IO hit.
    Panic {
        /// Log files to read.  Rotated logs may be given in any order.
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
}

fn read(paths: &[PathBuf]) -> Result<Vec<(String, String)>> {
    let mut inputs = Vec::new();
    for path in paths {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading {}", path.display()))?;
        inputs.push((
            path.display().to_string(),
            String::from_utf8_lossy(&bytes).into_owned(),
        ));
    }
    Ok(inputs)
}

fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        None => {
            let summary = crulog::summarize(&read(&args.files)?);
            if args.json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                print!("{}", crulog::render::text(&summary));
            }
        }
        Some(Command::Panic { files }) => {
            let reports = crulog::history::analyze(&read(files)?);
            if args.json {
                println!("{}", serde_json::to_string_pretty(&reports)?);
            } else {
                print!("{}", crulog::render::panic_report(&reports));
            }
        }
    }
    Ok(())
}
