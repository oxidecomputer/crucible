// Copyright 2022 Oxide Computer Company

use clap::Parser;
use dsc_client::Client;

use anyhow::Result;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Parser, PartialEq)]
pub enum ClientCommand {
    /// Disable random stopping of downstairs
    DisableRandomStop,
    /// Disable auto restart on the given downstairs client ID
    DisableRestart {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Disable auto restart on all downstairs
    DisableRestartAll,
    /// Enable automatic restart of the given client ID
    EnableRestart {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Enable the random stopping of any downstairs
    EnableRandomStop,
    /// Set the minimum random stop time (in seconds)
    EnableRandomMin {
        #[clap(long, short, action)]
        min: u64,
    },
    /// Set the maximum random stop time (in seconds)
    EnableRandomMax {
        #[clap(long, short, action)]
        max: u64,
    },
    /// Enable auto restart on all downstairs
    EnableRestartAll,
    /// Get PID of the given client ID
    Pid {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Shutdown all downstairs, then shutdown dsc itself.
    Shutdown,
    /// Start the downstairs at the given client ID
    Start {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Start all downstairs
    StartAll,
    /// Get the state of the given client ID
    State {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Stop the downstairs at the given client ID
    Stop {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Stop all the downstairs
    StopAll,
    /// Stop a random downstairs
    StopRand,
}

// Connect to the DSC and run a command.
#[tokio::main]
pub async fn client_main(server: String, cmd: ClientCommand) -> Result<()> {
    let dsc = Client::new(&server);
    match cmd {
        ClientCommand::DisableRandomStop => {
            let _ = dsc.dsc_disable_random_stop().await.unwrap();
        }
        ClientCommand::DisableRestart { cid } => {
            let _ = dsc.dsc_disable_restart(cid).await.unwrap();
        }
        ClientCommand::DisableRestartAll => {
            let _ = dsc.dsc_disable_restart_all().await.unwrap();
        }
        ClientCommand::EnableRandomStop => {
            let _ = dsc.dsc_enable_random_stop().await.unwrap();
        }
        ClientCommand::EnableRandomMin { min } => {
            let _ = dsc.dsc_enable_random_min(min).await.unwrap();
        }
        ClientCommand::EnableRandomMax { max } => {
            let _ = dsc.dsc_enable_random_max(max).await.unwrap();
        }
        ClientCommand::EnableRestart { cid } => {
            let _ = dsc.dsc_enable_restart(cid).await.unwrap();
        }
        ClientCommand::EnableRestartAll => {
            let _ = dsc.dsc_enable_restart_all().await.unwrap();
        }
        ClientCommand::Pid { cid } => {
            let res = dsc.dsc_get_pid(cid).await.unwrap();
            println!("{:?}", res);
        }
        ClientCommand::Shutdown => {
            let _ = dsc.dsc_shutdown().await.unwrap();
        }
        ClientCommand::State { cid } => {
            let res = dsc.dsc_get_ds_state(cid).await.unwrap();
            println!("{:?}", res);
        }
        ClientCommand::Start { cid } => {
            let _ = dsc.dsc_start(cid).await.unwrap();
        }
        ClientCommand::StartAll => {
            let _ = dsc.dsc_start_all().await.unwrap();
        }
        ClientCommand::Stop { cid } => {
            let _ = dsc.dsc_stop(cid).await.unwrap();
        }
        ClientCommand::StopAll => {
            let _ = dsc.dsc_stop_all().await.unwrap();
        }
        ClientCommand::StopRand => {
            let _ = dsc.dsc_stop_rand().await.unwrap();
        }
    }
    Ok(())
}
