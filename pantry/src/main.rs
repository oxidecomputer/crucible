// Copyright 2022 Oxide Computer Company

use anyhow::{anyhow, Result};
use clap::Parser;
use std::net::SocketAddr;

use crucible_pantry::*;

#[derive(Debug, Parser)]
#[clap(name = PROG, about = "Crucible volume maintenance agent")]
enum Args {
    Run {
        #[clap(short = 'l', action)]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;

    match args {
        Args::Run { listen } => {
            let (log, pantry) = initialize_pantry()?;

            let (_, join_handle) = server::run_server(&log, listen, &pantry)?;

            join_handle.await?.map_err(|e| anyhow!(e))
        }
    }
}
