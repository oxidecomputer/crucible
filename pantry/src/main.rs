// Copyright 2022 Oxide Computer Company

use anyhow::{anyhow, Result};
use clap::Parser;
use crucible_pantry_api::crucible_pantry_api_mod;
use semver::Version;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

use crucible_pantry::*;

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
            let (log, pantry) = initialize_pantry()?;

            let (_, join_handle) = server::run_server(&log, listen, &pantry)?;

            join_handle.await?.map_err(|e| anyhow!(e))
        }
    }
}

fn write_openapi<W: Write>(f: &mut W) -> Result<()> {
    // TODO: Switch to OpenAPI manager once available.
    let api = crucible_pantry_api_mod::stub_api_description()?;
    api.openapi("Crucible Pantry", Version::new(0, 0, 1))
        .write(f)?;
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
