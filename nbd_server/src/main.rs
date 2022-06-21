// Copyright 2021 Oxide Computer Company
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use tokio::runtime::Builder;

use crucible::*;

use nbd::server::{handshake, transmission, Export};
use std::net::{TcpListener, TcpStream as NetTcpStream};

/*
 * NBD server commands translate through the CruciblePseudoFile and turn
 * into Guest work ops.
 */

fn handle_nbd_client<T: crucible::BlockIO>(
    cpf: &mut crucible::CruciblePseudoFile<T>,
    mut stream: NetTcpStream,
) -> Result<()> {
    let e = Export {
        size: cpf.sz(),
        readonly: false,
        ..Default::default()
    };
    handshake(&mut stream, &e)?;
    transmission(&mut stream, cpf)?;
    Ok(())
}

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

fn main() -> Result<()> {
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
        metric_collect: None,
        metric_register: None,
        ..Default::default()
    };

    /*
     * Crucible needs a runtime as it will create several async tasks to
     * handle adding new IOs, communication with the three downstairs
     * instances, and completing IOs.
     */
    let runtime = Builder::new_multi_thread()
        .worker_threads(10)
        .thread_name("crucible-tokio")
        .enable_all()
        .build()
        .unwrap();

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let guest = Arc::new(Guest::new());

    runtime.spawn(up_main(crucible_opts, guest.clone()));
    println!("Crucible runtime is spawned");

    // NBD server

    guest.activate(opt.gen)?;
    let volume = Volume::from_guest(guest)?;
    let mut cpf = crucible::CruciblePseudoFile::from(Arc::new(volume))?;

    let listener = TcpListener::bind("127.0.0.1:10809").unwrap();

    // sent to NBD client during handshake through Export struct
    cpf.activate(opt.gen)?;
    println!("NBD advertised size as {} bytes", cpf.sz());

    for stream in listener.incoming() {
        println!("waiting on nbd traffic");
        match stream {
            Ok(stream) => match handle_nbd_client(&mut cpf, stream) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("handle_nbd_client error: {}", e);
                }
            },
            Err(_) => {
                println!("Error");
            }
        }
    }

    Ok(())
}
