// Copyright 2022 Oxide Computer Company
use clap::{Parser, Subcommand};
use cmon_common::{
    DtraceDisplay, DtraceWrapper, default_display_fields, format_header,
    format_row,
};
use crucible_control_client::Client;
use std::collections::HashMap;
use std::io::{self, BufRead};
use strum::IntoEnumIterator;
use tokio::time::{Duration, sleep};

/// Connect to crucible control server
#[derive(Parser, Debug)]
#[clap(name = "cmon", term_width = 80)]
#[clap(about = "Crucible monitoring tool", long_about = None)]
struct Args {
    #[clap(subcommand)]
    action: Action,

    /// URL location of the Crucible control server
    #[clap(short, long, default_value = "http://127.0.0.1:7777", action)]
    control: String,

    /// Seconds to wait between displaying data.
    #[clap(short, long, default_value = "5", action)]
    seconds: u64,
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Read from stdin
    Dtrace {
        /// Fields to display from dtrace received input
        #[clap(
            short,
            long,
            value_delimiter = ',',
            default_values_t = default_display_fields(),
        )]
        #[arg(value_enum)]
        output: Vec<DtraceDisplay>,
    },
    /// Decode what options will display what headers.
    DtraceDecode,
    /// Show the current downstairs job queue
    Jobs,
    /// Show the status of various LiveRepair stats
    Repair,
}

// Show the downstairs work queue
async fn show_work_queue(args: Args) {
    let ca = Client::new(&args.control);
    loop {
        clearscreen::clear().unwrap();
        match ca.downstairs_work_queue().await {
            Ok(ji) => {
                println!(
                    "{:>7} {:>6} {:>4} {:>8} {:>6} {:>6} {:>6}",
                    "DS_ID", "TYPE", "BL/E", "ACK", "DS0", "DS1", "DS2"
                );
                for job in ji.jobs.iter() {
                    print!("{:7}", job.id);
                    print!(" {:>6}", job.job_type);
                    print!(" {:>4}", job.num_blocks);
                    print!(" {0:>8}", job.ack_status.to_string());
                    for cid in 0..3 {
                        print!(" {:>6}", job.state[cid]);
                    }
                    println!();
                }
                println!("COMPLETED");
                for job in ji.completed.iter() {
                    print!("{:7}", job.id);
                    print!(" {:>6}", job.job_type);
                    print!(" {:>4}", job.num_blocks);
                    print!(" {0:>8}", job.ack_status.to_string());
                    for cid in 0..3 {
                        print!(" {:>6}", job.state[cid]);
                    }
                    println!();
                }
            }
            Err(e) => {
                println!("Control returned error: {}", e);
            }
        }
        sleep(Duration::from_secs(args.seconds)).await;
    }
}

// Show LiveRepair stats
async fn show_repair_stats(args: Args) {
    let ca = Client::new(&args.control);
    let mut count = 20;
    loop {
        if count == 20 {
            println!(
                "  UJ  DSJ  EL0  EL1  EL2 CONF0 CONF1 CONF2 REPR0 REPR1 REPR2"
            );
            count = 0;
        }
        count += 1;
        match ca.upstairs_fill_info().await {
            Ok(ui) => {
                print!("{:4} {:4}", ui.up_jobs, ui.ds_jobs);
                for cid in 0..3 {
                    if ui.extent_limit[cid].is_some() {
                        print!(" {:4}", ui.extent_limit[cid].unwrap());
                    } else {
                        print!(" None");
                    }
                }
                for cid in 0..3 {
                    print!(" {:5}", ui.extents_confirmed[cid]);
                }
                for cid in 0..3 {
                    print!(" {:5}", ui.extents_repaired[cid]);
                }
                for cid in 0..3 {
                    print!(" {:?}", ui.ds_state[cid]);
                }
                println!();
            }
            Err(e) => {
                println!("Control returned error: {}", e);
            }
        }
        sleep(Duration::from_secs(args.seconds)).await;
    }
}

// Take input from stdin (assumed to be output from the dtrace raw script)
// and print out the fields requested in the output Vec.
//
// The raw script matches every upstairs on the system, so rows for
// different sessions arrive interleaved.  Job IDs are only comparable
// within a session, so the last job ID we saw is tracked per session and
// the first row of any session has no delta to report.
fn dtrace_loop(output: Vec<DtraceDisplay>) {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    let mut count = 0;
    let mut last_job_id: HashMap<String, u64> = HashMap::new();
    loop {
        let mut dtrace_out = String::new();
        match handle.read_line(&mut dtrace_out) {
            // A zero length read means stdin reached EOF; without this
            // the loop spins forever failing to parse an empty line.
            Ok(0) => break,
            Ok(_) => {
                if count == 0 {
                    println!("{}", format_header(&output));
                }
                count = (count + 1) % 20;
                let wrapper: DtraceWrapper =
                    match serde_json::from_str(&dtrace_out) {
                        Ok(a) => a,
                        Err(e) => {
                            println!("Err {:?}", e);
                            continue;
                        }
                    };

                let job_id = wrapper.status.next_job_id.0;
                let delta = last_job_id
                    .insert(wrapper.status.session_id.clone(), job_id)
                    .map(|last| job_id.saturating_sub(last));

                println!(
                    "{}",
                    format_row(wrapper.pid, &wrapper.status, delta, &output)
                );
            }
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }
}

/*
 * Simple tool to connect to a crucible upstairs control http port
 * and report back the results from a upstairs_fill_info command.
 */
#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.action {
        Action::Dtrace { output } => {
            dtrace_loop(output);
        }
        Action::DtraceDecode => {
            for dd in DtraceDisplay::iter() {
                println!("{dd}: {}", format_header(&[dd]));
            }
        }
        Action::Jobs => {
            show_work_queue(args).await;
        }
        Action::Repair => {
            show_repair_stats(args).await;
        }
    }
}
