// Copyright 2022 Oxide Computer Company

use std::sync::Arc;

use anyhow::Result;
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{o, Logger};

pub const PROG: &str = "crucible-pantry";

pub mod pantry;
pub mod server;

pub async fn initialize_pantry() -> Result<(Logger, Arc<pantry::Pantry>)> {
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    }
    .to_logger(PROG)?;

    let pantry =
        Arc::new(pantry::Pantry::new(log.new(o!("component" => "datafile")))?);

    Ok((log, pantry))
}
