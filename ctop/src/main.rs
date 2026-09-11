// Copyright 2026 Oxide Computer Company

//! ctop - watch every crucible upstairs on the system.
//!
//! `cmon dtrace` reads the raw dtrace output from stdin, which means
//! running the dtrace script yourself and piping it in.  ctop runs the
//! equivalent command itself and reads its output.
//!
//! This is the plumbing on its own: rows are printed as they arrive.
//! The curses display is built on top of it.

use anyhow::{Context, Result, bail};
use clap::Parser;
use cmon_common::{
    DtraceDisplay, DtraceWrapper, default_display_fields, format_header,
    format_row,
};
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

/// The dtrace command we run when not given one.
///
/// This is `tools/dtrace/upstairs_raw.d` as a one liner, so ctop does
/// not have to find a script file on disk.  The differences from the
/// script: -Z so dtrace waits for a matching probe instead of failing
/// when no upstairs is running yet, and -q to keep dtrace from printing
/// a header of its own.
///
/// dtrace needs privileges, so ctop has to be started with them
/// (`pfexec ctop`).  Without them dtrace exits immediately and says why
/// on stderr.
const DEFAULT_DTRACE_CMD: &str = r#"dtrace -Z -q -x strsize=2k -n 'crucible_upstairs*:::up-status { printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok")); }'"#;

/// How many rows between repeats of the column header.
const HEADER_INTERVAL: usize = 20;

#[derive(Parser, Debug)]
#[clap(name = "ctop", term_width = 80)]
#[clap(
    about = "Monitor crucible upstairs via dtrace",
    long_about = "Monitor crucible upstairs via dtrace.\n\n\
                  Runs a dtrace command and displays the up-status probe \
                  output from every crucible upstairs on the system.  \
                  dtrace requires privileges, so run this as `pfexec ctop`."
)]
struct Args {
    /// Command to run to generate dtrace output
    #[clap(long, default_value = DEFAULT_DTRACE_CMD)]
    dtrace_cmd: String,

    /// Fields to display from the dtrace output
    #[clap(
        short,
        long,
        value_delimiter = ',',
        default_values_t = default_display_fields()
    )]
    #[arg(value_enum)]
    output: Vec<DtraceDisplay>,
}

/// Run `dtrace_cmd` and print a row for each record it produces.
///
/// The command is run through a shell so the quoting in a dtrace one
/// liner survives, and with kill_on_drop so the child does not outlive
/// us.  Let the stderr from DTrace tell us the problem.
///
/// Records for different sessions arrive interleaved, because the probe
/// matches every upstairs on the system.  A job ID only means anything
/// within its own session, so the last one seen is tracked per session
/// and a session's first record has no delta to report.
async fn reader_loop(dtrace_cmd: &str, output: &[DtraceDisplay]) -> Result<()> {
    if dtrace_cmd.trim().is_empty() {
        bail!("empty dtrace command");
    }

    let mut child = Command::new("sh")
        .arg("-c")
        .arg(dtrace_cmd)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start the dtrace command")?;

    let stdout = child
        .stdout
        .take()
        .context("failed to capture the dtrace command's stdout")?;

    let mut lines = BufReader::new(stdout).lines();
    let mut last_job_id: HashMap<String, u64> = HashMap::new();
    let mut count = 0;

    while let Some(line) = lines.next_line().await? {
        // dtrace can emit a blank line, and -Z means we may be running
        // before there is anything to report.
        if line.trim().is_empty() {
            continue;
        }

        let wrapper: DtraceWrapper = match serde_json::from_str(&line) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("skipping unparseable line: {e}");
                continue;
            }
        };

        if count % HEADER_INTERVAL == 0 {
            println!("{}", format_header(output));
        }
        count += 1;

        // insert() hands back this session's previous job ID, or None
        // the first time we see the session, which is exactly when
        // there is no delta to report.
        let job_id = wrapper.status.next_job_id.0;
        let delta = last_job_id
            .insert(wrapper.status.session_id.clone(), job_id)
            .map(|last| job_id.saturating_sub(last));

        println!(
            "{}",
            format_row(wrapper.pid, &wrapper.status, delta, output)
        );
    }

    // stdout closing means the command is finished one way or another.
    let status = child.wait().await.context("waiting for dtrace")?;
    if !status.success() {
        bail!("dtrace command exited ({status})");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    reader_loop(&args.dtrace_cmd, &args.output).await
}
