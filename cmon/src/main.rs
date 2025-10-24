// Copyright 2022 Oxide Computer Company
use clap::{Parser, Subcommand, ValueEnum};
use crucible_control_client::Client;
use crucible_protocol::ClientId;
use serde::Deserialize;
use std::fmt;
use std::io::{self, BufRead};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio::time::{sleep, Duration};

use crucible::DtraceInfo;

mod ctop;

#[derive(Debug, Deserialize)]
pub struct DtraceWrapper {
    pub pid: u32,
    pub status: DtraceInfo,
}

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
#[derive(Debug, Copy, Clone, ValueEnum, EnumIter)]
pub enum DtraceDisplay {
    Pid,
    Session,
    UpstairsId,
    State,
    IoCount,
    IoSummary,
    UpCount,
    DsCount,
    Reconcile,
    DsReconciled,
    DsReconcileNeeded,
    LiveRepair,
    Connected,
    Replaced,
    ExtentLiveRepair,
    ExtentLimit,
    NextJobId,
    JobDelta,
    DsDelay,
    WriteBytesOut,
    RoLrSkipped,
    // IOStateCount fields (already partially covered by IoCount/IoSummary)
    DsIoInProgress,
    DsIoDone,
    DsIoSkipped,
    DsIoError,
}

impl fmt::Display for DtraceDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DtraceDisplay::Pid => write!(f, "pid"),
            DtraceDisplay::Session => write!(f, "session"),
            DtraceDisplay::UpstairsId => write!(f, "upstairs_id"),
            DtraceDisplay::State => write!(f, "state"),
            DtraceDisplay::IoCount => write!(f, "io_count"),
            DtraceDisplay::IoSummary => write!(f, "io_summary"),
            DtraceDisplay::UpCount => write!(f, "up_count"),
            DtraceDisplay::DsCount => write!(f, "ds_count"),
            DtraceDisplay::Reconcile => write!(f, "reconcile"),
            DtraceDisplay::DsReconciled => write!(f, "ds_reconciled"),
            DtraceDisplay::DsReconcileNeeded => {
                write!(f, "ds_reconcile_needed")
            }
            DtraceDisplay::LiveRepair => write!(f, "live_repair"),
            DtraceDisplay::Connected => write!(f, "connected"),
            DtraceDisplay::Replaced => write!(f, "replaced"),
            DtraceDisplay::ExtentLiveRepair => write!(f, "extent_live_repair"),
            DtraceDisplay::ExtentLimit => write!(f, "extent_under_repair"),
            DtraceDisplay::NextJobId => write!(f, "next_job_id"),
            DtraceDisplay::JobDelta => write!(f, "job_delta"),
            DtraceDisplay::DsDelay => write!(f, "ds_delay"),
            DtraceDisplay::WriteBytesOut => write!(f, "write_bytes_out"),
            DtraceDisplay::RoLrSkipped => write!(f, "ro_lr_skipped"),
            DtraceDisplay::DsIoInProgress => write!(f, "ds_io_in_progress"),
            DtraceDisplay::DsIoDone => write!(f, "ds_io_done"),
            DtraceDisplay::DsIoSkipped => write!(f, "ds_io_skipped"),
            DtraceDisplay::DsIoError => write!(f, "ds_io_error"),
        }
    }
}

#[derive(Debug, Subcommand)]
enum Action {
    /// Read from stdin
    Dtrace {
        /// Fields to display from dtrace received input
        #[clap(short, long, value_delimiter = ',', default_values_t = vec![DtraceDisplay::Pid, DtraceDisplay::Session, DtraceDisplay::State, DtraceDisplay::NextJobId, DtraceDisplay::JobDelta, DtraceDisplay::ExtentLimit, DtraceDisplay::DsReconciled, DtraceDisplay::DsReconcileNeeded])]
        #[arg(value_enum)]
        output: Vec<DtraceDisplay>,
    },
    /// Decode what options will display what headers.
    DtraceDecode,
    /// Show the current downstairs job queue
    Jobs,
    /// Show the status of various LiveRepair stats
    Repair,
    /// Curses-based top-like display of dtrace data
    Ctop {
        /// Command to run to generate dtrace output
        #[clap(
            long,
            default_value = "dtrace -s /opt/oxide/crucible_dtrace/upstairs_raw.d"
        )]
        dtrace_cmd: String,
    },
}

/// Translate what the default DsState string is (that we are getting from DTrace)
/// into a three letter string for printing.
pub fn short_state(dss: &str) -> String {
    match dss {
        "Active" => "ACT".to_string(),
        "WaitQuorum" => "WQ".to_string(),
        "Reconcile" => "REC".to_string(),
        "LiveRepairReady" => "LRR".to_string(),
        "New" => "NEW".to_string(),
        "Faulted" => "FLT".to_string(),
        "Offline" => "OFL".to_string(),
        "Replaced" => "RPL".to_string(),
        "LiveRepair" => "LR".to_string(),
        "Replacing" => "RPC".to_string(),
        "Disabled" => "DIS".to_string(),
        "Deactivated" => "DAV".to_string(),
        "NegotiationFailed" => "NF".to_string(),
        "Fault" => "FLT".to_string(),
        x => x.to_string(),
    }
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
            DtraceDisplay::Pid => {
                print!(" {:>5}", "PID");
            }
            DtraceDisplay::Session => {
                print!(" {:>8}", "SESSION");
            }
            DtraceDisplay::UpstairsId => {
                print!(" {:>8}", "UPSTAIRS");
            }
            DtraceDisplay::State => {
                print!(" {:>3} {:>3} {:>3}", "DS0", "DS1", "DS2",);
            }
            DtraceDisplay::UpCount => {
                print!(" {:>3}", "UPW");
            }
            DtraceDisplay::DsCount => {
                print!(" {:>5}", "DSW");
            }
            DtraceDisplay::IoCount | DtraceDisplay::IoSummary => {
                print!(" {:>5} {:>5} {:>5}", "IP0", "IP1", "IP2");
                print!(" {:>5} {:>5} {:>5}", "D0", "D1", "D2");
                print!(" {:>5} {:>5} {:>5}", "S0", "S1", "S2");

                if matches!(display_item, DtraceDisplay::IoCount) {
                    print!(" {:>4} {:>4} {:>4}", "E0", "E1", "E2");
                }
            }
            DtraceDisplay::Reconcile => {
                print!(" {:>4} {:>4} {:>4}", "REC", "NREC", "AREC");
            }
            DtraceDisplay::DsReconciled => {
                print!(" {:>4}", "ERR");
            }
            DtraceDisplay::DsReconcileNeeded => {
                print!(" {:>4}", "ERN");
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
            DtraceDisplay::ExtentLiveRepair => {
                print!(" {:>4} {:>4} {:>4}", "EXR0", "EXR1", "EXR2");
                print!(" {:>4} {:>4} {:>4}", "EXC0", "EXC1", "EXC2");
            }
            DtraceDisplay::ExtentLimit => {
                print!(" {:>4}", "EXTL");
            }
            DtraceDisplay::NextJobId => {
                print!(" {:>7}", "NEXTJOB");
            }
            DtraceDisplay::JobDelta => {
                print!(" {:>5}", "DELTA");
            }
            DtraceDisplay::DsDelay => {
                print!(" {:>5} {:>5} {:>5}", "DLY0", "DLY1", "DLY2");
            }
            DtraceDisplay::WriteBytesOut => {
                print!(" {:>10}", "WRBYTES");
            }
            DtraceDisplay::RoLrSkipped => {
                print!(" {:>4} {:>4} {:>4}", "RLS0", "RLS1", "RLS2");
            }
            DtraceDisplay::DsIoInProgress => {
                print!(" {:>5} {:>5} {:>5}", "IP0", "IP1", "IP2");
            }
            DtraceDisplay::DsIoDone => {
                print!(" {:>5} {:>5} {:>5}", "D0", "D1", "D2");
            }
            DtraceDisplay::DsIoSkipped => {
                print!(" {:>5} {:>5} {:>5}", "S0", "S1", "S2");
            }
            DtraceDisplay::DsIoError => {
                print!(" {:>4} {:>4} {:>4}", "E0", "E1", "E2");
            }
        }
    }
    println!();
}

// Print out the values in the dtrace output based on what the DtraceDisplay
// enums are set in the given Vec.
fn print_dtrace_row(
    pid: u32,
    d_out: DtraceInfo,
    dd: &[DtraceDisplay],
    last_job_id: &mut u64,
) {
    for display_item in dd.iter() {
        match display_item {
            DtraceDisplay::Pid => {
                print!(" {:>5}", pid);
            }
            DtraceDisplay::Session => {
                let session_short =
                    d_out.session_id.chars().take(8).collect::<String>();
                print!(" {:>8}", session_short);
            }
            DtraceDisplay::UpstairsId => {
                let upstairs_short =
                    d_out.upstairs_id.chars().take(8).collect::<String>();
                print!(" {:>8}", upstairs_short);
            }
            DtraceDisplay::State => {
                print!(
                    " {:>3} {:>3} {:>3}",
                    short_state(&d_out.ds_state[0]),
                    short_state(&d_out.ds_state[1]),
                    short_state(&d_out.ds_state[2]),
                );
            }
            DtraceDisplay::UpCount => {
                print!(" {:3}", d_out.up_count);
            }
            DtraceDisplay::DsCount => {
                print!(" {:5}", d_out.ds_count);
            }
            DtraceDisplay::IoCount | DtraceDisplay::IoSummary => {
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                );
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                );
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                );
                if matches!(display_item, DtraceDisplay::IoCount) {
                    print!(
                        " {:4} {:4} {:4}",
                        d_out.ds_io_count.error[ClientId::new(0)],
                        d_out.ds_io_count.error[ClientId::new(1)],
                        d_out.ds_io_count.error[ClientId::new(2)],
                    );
                }
            }
            DtraceDisplay::Reconcile => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_reconciled,
                    d_out.ds_reconcile_needed,
                    d_out.ds_reconcile_aborted,
                );
            }
            DtraceDisplay::DsReconciled => {
                print!(" {:>4}", d_out.ds_reconciled);
            }
            DtraceDisplay::DsReconcileNeeded => {
                print!(" {:>4}", d_out.ds_reconcile_needed);
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
            DtraceDisplay::ExtentLiveRepair => {
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
            DtraceDisplay::ExtentLimit => {
                print!(" {:4}", d_out.ds_extent_limit);
            }
            DtraceDisplay::NextJobId => {
                print!(" {:>7}", d_out.next_job_id);
            }
            DtraceDisplay::JobDelta => {
                if *last_job_id == 0 {
                    print!(" {:>5}", "---");
                } else {
                    let delta = d_out.next_job_id.0 - *last_job_id;
                    print!(" {:5}", delta);
                }
                *last_job_id = d_out.next_job_id.0;
            }
            DtraceDisplay::DsDelay => {
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_delay_us[0],
                    d_out.ds_delay_us[1],
                    d_out.ds_delay_us[2],
                );
            }
            DtraceDisplay::WriteBytesOut => {
                print!(" {:10}", d_out.write_bytes_out);
            }
            DtraceDisplay::RoLrSkipped => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_ro_lr_skipped[0],
                    d_out.ds_ro_lr_skipped[1],
                    d_out.ds_ro_lr_skipped[2],
                );
            }
            DtraceDisplay::DsIoInProgress => {
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.in_progress[ClientId::new(0)],
                    d_out.ds_io_count.in_progress[ClientId::new(1)],
                    d_out.ds_io_count.in_progress[ClientId::new(2)],
                );
            }
            DtraceDisplay::DsIoDone => {
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.done[ClientId::new(0)],
                    d_out.ds_io_count.done[ClientId::new(1)],
                    d_out.ds_io_count.done[ClientId::new(2)],
                );
            }
            DtraceDisplay::DsIoSkipped => {
                print!(
                    " {:5} {:5} {:5}",
                    d_out.ds_io_count.skipped[ClientId::new(0)],
                    d_out.ds_io_count.skipped[ClientId::new(1)],
                    d_out.ds_io_count.skipped[ClientId::new(2)],
                );
            }
            DtraceDisplay::DsIoError => {
                print!(
                    " {:4} {:4} {:4}",
                    d_out.ds_io_count.error[ClientId::new(0)],
                    d_out.ds_io_count.error[ClientId::new(1)],
                    d_out.ds_io_count.error[ClientId::new(2)],
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
    let mut last_job_id: u64 = 0;
    loop {
        let mut dtrace_out = String::new();
        match handle.read_line(&mut dtrace_out) {
            Ok(_) => {
                if count == 0 {
                    print_dtrace_header(&output);
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
                print_dtrace_row(
                    wrapper.pid,
                    wrapper.status,
                    &output,
                    &mut last_job_id,
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
                print!("{dd}: ");
                print_dtrace_header(&[dd]);
            }
        }
        Action::Jobs => {
            show_work_queue(args).await;
        }
        Action::Repair => {
            show_repair_stats(args).await;
        }
        Action::Ctop { dtrace_cmd } => {
            if let Err(e) = ctop::ctop_loop(dtrace_cmd).await {
                eprintln!("Error running ctop: {}", e);
            }
        }
    }
}
