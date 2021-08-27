use anyhow::{bail, Result};
use std::path::PathBuf;
use structopt::StructOpt;
use std::net::SocketAddr;
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::info;

const PROG: &str = "crucible-agent";

mod server;

#[derive(Debug, StructOpt)]
#[structopt(name = PROG, about = "Crucible zone management agent")]
enum Args {
    Run {
        #[structopt(short = "d", name = "DATA_DIR", parse(try_from_str))]
        data_dir: PathBuf,

        #[structopt(short = "l", name = "IP:PORT", parse(try_from_str))]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args_safe()?;

    match args {
        Args::Run { data_dir, listen } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }.to_logger(PROG)?;

            info!(log, "data directory: {:?}", data_dir);
            info!(log, "listen IP: {:?}", listen);

            server::run_server(&log, listen).await
        }
    }
}
