// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/
//
// Copyright 2025 Oxide Computer Company

use clap::{Parser, Subcommand};

mod external;

#[derive(Debug, Parser)]
struct Xtasks {
    #[command(subcommand)]
    subcommand: XtaskCommands,
}

/// crucible xtask support
#[derive(Debug, Subcommand)]
#[clap(name = "xtask")]
enum XtaskCommands {
    /// run OpenAPI manager
    Openapi(external::External),
}

#[allow(
    clippy::disallowed_macros,
    reason = "using `#[tokio::main]` in xtasks is fine, as they are not \
     deployed in production"
)]
#[tokio::main]
async fn main() {
    let task = Xtasks::parse();
    if let Err(e) = match task.subcommand {
        XtaskCommands::Openapi(external) => external
            .exec_bin("crucible-dropshot-apis", "crucible-dropshot-apis"),
    } {
        eprintln!("failed: {e}");
        std::process::exit(-1);
    }
}
