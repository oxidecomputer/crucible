// Copyright 2021 Oxide Computer Company
use std::net::SocketAddr;

use anyhow::{bail, Result};
use clap::Parser;

use crucible::*;

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use dropshot::TypedBody;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Parser)]
#[clap(about = "volume-side storage component")]
pub struct Opt {
    #[clap(short, long, default_value = "127.0.0.1:9000", action)]
    target: Vec<SocketAddr>,

    #[clap(short, long, action)]
    key: Option<String>,

    #[clap(short, long, default_value = "0", action)]
    gen: u64,

    // TLS options
    #[clap(long, action)]
    cert_pem: Option<String>,
    #[clap(long, action)]
    key_pem: Option<String>,
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    // Start upstairs control http server
    #[clap(long, action)]
    control: Option<SocketAddr>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

/*
 * Crucible needs a runtime as it will create several async tasks to handle
 * adding new IOs, communication with the three downstairs instances, and
 * completing IOs.
 */
#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;
    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: false,
        flush_timeout: None,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
        ..Default::default()
    };

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let (guest, io) = Guest::new(None);

    let _join_handle = up_main(crucible_opts, opt.gen, None, io, None)?;
    println!("Crucible runtime is spawned");

    // Rest server

    let mut cpf = crucible::CruciblePseudoFile::from(guest)?;
    cpf.activate().await?;


    Ok(())
}
