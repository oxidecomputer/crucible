// Copyright 2022 Oxide Computer Company

use anyhow::{anyhow, Result};
use clap::Parser;
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{info, o};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

const PROG: &str = "crucible-pantry";

mod pantry;
mod server;

#[derive(Debug, Parser)]
#[clap(name = PROG, about = "Crucible volume maintenance agent")]
enum Args {
    OpenApi {
        #[clap(short = 'o', action)]
        output: PathBuf,
    },
    Run {
        #[clap(short = 'l', action)]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    /*
     * If any of our async tasks in our runtime panic, then we should exit
     * the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let args = Args::try_parse()?;

    match args {
        Args::OpenApi { output } => {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output)?;
            write_openapi(&mut f)
        }
        Args::Run { listen } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }
            .to_logger(PROG)?;

            info!(log, "listen IP: {:?}", listen);

            let pan = Arc::new(pantry::Pantry::new(
                log.new(o!("component" => "datafile")),
            )?);

            server::run_server(&log, listen, pan).await
        }
    }
}

fn write_openapi<W: Write>(f: &mut W) -> Result<()> {
    let api = server::make_api().map_err(|e| anyhow!(e))?;
    api.openapi("Crucible Pantry", "0.0.0").write(f)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use openapiv3::OpenAPI;

    use crate::write_openapi;

    #[test]
    fn test_crucible_pantry_openapi() {
        let mut raw = Vec::new();
        write_openapi(&mut raw).unwrap();
        let actual = String::from_utf8(raw).unwrap();

        // Make sure the result parses as a valid OpenAPI spec.
        let spec = serde_json::from_str::<OpenAPI>(&actual)
            .expect("output was not valid OpenAPI");

        // Check for lint errors.
        let errors = openapi_lint::validate(&spec);
        assert!(errors.is_empty(), "{}", errors.join("\n\n"));

        expectorate::assert_contents(
            "../openapi/crucible-pantry.json",
            &actual,
        );
    }
}
