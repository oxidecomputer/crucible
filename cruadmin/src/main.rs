// Copyright 2022 Oxide Computer Company
use clap::Parser;
use crucible_admin_client::Client;

/// Connect to crucible admin server
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// URL location of the Crucible admin server
    #[clap(short, long, default_value = "http://127.0.0.1:9999")]
    admin: String,
}

/*
 * Simple tool to connect to a crucible upstairs agent http port
 * and report back the results from a upstairs_fill_info command.
 */
#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("Connecting to Crucible Admin server at {:?}", args.admin);

    let ca = Client::new(&args.admin);
    let ui = ca.upstairs_fill_info().await;
    match ui {
        Ok(ui) => {
            println!();
            println!("Returned: {:?}", ui);
        }
        Err(e) => {
            println!("Admin returned error: {}", e);
        }
    }
}
