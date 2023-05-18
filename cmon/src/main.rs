// Copyright 2022 Oxide Computer Company
use clap::{Parser, Subcommand};
use crucible_control_client::Client;
use tokio::time::{sleep, Duration};

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
    /// Show the current downstairs job queue
    Jobs,
    /// Show the status of various OnlineRepair stats
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

// Show OnlineRepair stats
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
                    print!(" {:>4}", ui.ds_short_state[cid]);
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
/*
 * Simple tool to connect to a crucible upstairs control http port
 * and report back the results from a upstairs_fill_info command.
 */
#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.action {
        Action::Jobs => {
            show_work_queue(args).await;
        }
        Action::Repair => {
            show_repair_stats(args).await;
        }
    }
}
