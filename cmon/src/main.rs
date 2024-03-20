// Copyright 2022 Oxide Computer Company
use clap::{Parser, Subcommand, ValueEnum};
use crucible_control_client::Client;
use crucible_protocol::ClientId;
use std::fmt;
use std::io::{self, BufRead};
use tokio::time::{sleep, Duration};

use crucible::Arg;

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

// The possible fields we will display when receiving DTrace output.
#[derive(Debug, Copy, Clone, ValueEnum)]
enum DtraceDisplay {
    State,
    IoCount,
    UpCount,
    DsCount,
    Reconcile,
    LiveRepair,
    Connected,
    Replaced,
    ExtentRepair,
}

impl fmt::Display for DtraceDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DtraceDisplay::State => write!(f, "state"),
            DtraceDisplay::IoCount => write!(f, "io_count"),
            DtraceDisplay::UpCount => write!(f, "up_count"),
            DtraceDisplay::DsCount => write!(f, "ds_count"),
            DtraceDisplay::Reconcile => write!(f, "reconcile"),
            DtraceDisplay::LiveRepair => write!(f, "live_repair"),
            DtraceDisplay::Connected => write!(f, "connected"),
            DtraceDisplay::Replaced => write!(f, "replaced"),
            DtraceDisplay::ExtentRepair => write!(f, "extent_repair"),
        }
    }
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Read from stdin
    Dtrace {
        /// Fields to display from dtrace received input
        #[clap(short, long, default_value = "io-count")]
        #[arg(value_enum)]
        output: Vec<DtraceDisplay>,
    },
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

// Print out the column headers for the given DtraceDisplay options.
fn print_dtrace_header(dd: &[DtraceDisplay]) {
    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::State => {
                print!(
                    " {:>16} {:>16} {:>16}",
                    "DS_0_STATE", "DS_1_STATE", "DS_2_STATE",
                );
            }
            DtraceDisplay::UpCount => {
                print!(" {:>4}", "UPW");
            }
            DtraceDisplay::DsCount => {
                print!(" {:>4}", "DSW");
            }
            DtraceDisplay::IoCount => {
                print!(" {:4} {:4} {:4}", "NEW0", "NEW1", "NEW2");
                print!(" {:>4} {:>4} {:>4}", "IP0", "IP1", "IP2");
                print!(" {:>4} {:>4} {:>4}", "D0", "D1", "D2");
                print!(" {:>4} {:>4} {:>4}", "S0", "S1", "S2");
                print!(" {:>4} {:>4} {:>4}", "E0", "E1", "E2");
            }
            DtraceDisplay::Reconcile => {
                print!(" {:>4} {:>4}", "REC", "NEED");
            }
            DtraceDisplay::LiveRepair => {
                print!(" {:>4} {:>4} {:>4}", "LRC0", "LRC1", "LRC0");
                print!(" {:>4} {:>4} {:>4}", "LRA0", "LRA1", "LRA2");
            }
            DtraceDisplay::Connected => {
                print!(" {:>4} {:>4} {:>4}", "CON0", "CON1", "CON2");
            }
            DtraceDisplay::Replaced => {
                print!(" {:>4} {:>4} {:>4}", "RPL0", "RPL1", "RPL2");
            }
            DtraceDisplay::ExtentRepair => {
                print!(" {:>4} {:>4} {:>4}", "EXR0", "EXR1", "EXR2");
                print!(" {:>4} {:>4} {:>4}", "EXC0", "EXC1", "EXC2");
            }
        }
    }
    println!();
}

// Print out the values in the dtrace output based on what the DtraceDisplay
// enums are set in the given Vec.
fn print_dtrace_row(d_out: Arg, dd: &[DtraceDisplay]) {
    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::State => {
                print!(
                    " {:>16} {:>16} {:>16}",
                    d_out.ds_state[0].to_string(),
                    d_out.ds_state[1].to_string(),
                    d_out.ds_state[2].to_string(),
                );
            }
            DtraceDisplay::UpCount => {
                print!(" {:4}", d_out.up_count);
            }
            DtraceDisplay::DsCount => {
                print!(" {:4}", d_out.ds_count);
            }
            DtraceDisplay::IoCount => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.new[ClientId::new(0)],
                    d_out.ds_io_count.new[ClientId::new(1)],
                    d_out.ds_io_count.new[ClientId::new(2)],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.error[ClientId::new(0)],
                    d_out.ds_io_count.error[ClientId::new(1)],
                    d_out.ds_io_count.error[ClientId::new(2)],
                );
            }
            DtraceDisplay::Reconcile => {
                print!(
                    " {:4} {:4}",
                    d_out.ds_reconciled, d_out.ds_reconcile_needed
                );
            }
            DtraceDisplay::LiveRepair => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_live_repair_completed[0],
                    d_out.ds_live_repair_completed[1],
                    d_out.ds_live_repair_completed[2],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_live_repair_aborted[0],
                    d_out.ds_live_repair_aborted[1],
                    d_out.ds_live_repair_aborted[2],
                );
            }
            DtraceDisplay::Connected => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_connected[0],
                    d_out.ds_connected[1],
                    d_out.ds_connected[2],
                );
            }
            DtraceDisplay::Replaced => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_replaced[0],
                    d_out.ds_replaced[1],
                    d_out.ds_replaced[2],
                );
            }
            DtraceDisplay::ExtentRepair => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_extents_repaired[0],
                    d_out.ds_extents_repaired[1],
                    d_out.ds_extents_repaired[2],
                );
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_extents_confirmed[0],
                    d_out.ds_extents_confirmed[1],
                    d_out.ds_extents_confirmed[2],
                );
            }
        }
    }
    println!();
}

// Take input from stdin (assumed to be output from the dtrace raw script)
// and print out the fields requested in the output Vec.
fn dtrace_loop(output: Vec<DtraceDisplay>) {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    let mut count = 0;
    loop {
        let mut dtrace_out = String::new();
        match handle.read_line(&mut dtrace_out) {
            Ok(_) => {
                if count == 0 {
                    print_dtrace_header(&output);
                }
                count = (count + 1) % 20;
                let d_out: Arg = match serde_json::from_str(&dtrace_out) {
                    Ok(a) => a,
                    Err(e) => {
                        println!("Err {:?}", e);
                        continue;
                    }
                };
                print_dtrace_row(d_out, &output);
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
        Action::Jobs => {
            show_work_queue(args).await;
        }
        Action::Repair => {
            show_repair_stats(args).await;
        }
    }
}
