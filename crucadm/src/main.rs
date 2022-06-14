// Copyright 2022 Oxide Computer Company
use clap::Parser;
use crucible_control_client::Client;

/// Connect to crucible control server
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// URL location of the Crucible control server
    #[clap(short, long, default_value = "http://127.0.0.1:9999", action)]
    control: String,
}

/*
 * Simple tool to connect to a crucible upstairs control http port
 * and report back the results from a upstairs_fill_info command.
 */
#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!(
        "Connecting to Crucible Control server at {:?}",
        args.control
    );

    let ca = Client::new(&args.control);
    let ui = ca.upstairs_fill_info().await;
    match ui {
        Ok(ui) => {
            println!();
            println!("Returned: {:?}", ui);
        }
        Err(e) => {
            println!("Control returned error: {}", e);
        }
    }
}
