// Copyright 2021 Oxide Computer Company

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use uuid::Uuid;

use crucible::*;

use std::io::{Read, Seek, SeekFrom, Write};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// https://stackoverflow.com/questions/29504514/whats-the-best-way-to-compare-2-vectors-or-strings-element-by-element
fn do_vecs_match<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

#[derive(Debug, Parser)]
#[clap(about = "volume-side storage component")]
pub struct Opt {
    #[clap(short, long, default_value = "127.0.0.1:9000", action)]
    target: Vec<SocketAddr>,

    /*
     * Verify that writes don't extend before or after the actual location.
     */
    #[clap(short, long, action)]
    verify_isolation: bool,

    #[clap(long, action)]
    tracing_endpoint: Option<String>,

    #[clap(short, long, action)]
    key: Option<String>,

    #[clap(short, long, default_value = "0", action)]
    gen: u64,

    /*
     * Number of upstairs to sequentially activate and handoff to
     */
    #[clap(short, long, default_value = "5", action)]
    num_upstairs: usize,

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

    #[clap(long, action)]
    block_size: usize,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;

    if opt.num_upstairs == 0 {
        bail!("Must have non-zero number of upstairs");
    }

    let crucible_opts = CrucibleOpts {
        id: Uuid::new_v4(),
        target: opt.target,
        lossy: false,
        flush_timeout: None,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
        read_only: false,
    };
    let mut generation_number = opt.gen;

    if let Some(tracing_endpoint) = opt.tracing_endpoint {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(tracing_endpoint)
            .with_service_name("crucible-hammer")
            .install_simple()
            .expect("Error initializing Jaeger exporter");

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(telemetry)
            .try_init()
            .expect("Error init tracing subscriber");

        println!("Set up tracing!");
    }

    /*
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    // Create N CruciblePseudoFiles to test activation handoff.
    let mut cpfs: Vec<crucible::CruciblePseudoFile<Guest>> =
        Vec::with_capacity(opt.num_upstairs);

    for _ in 0..opt.num_upstairs {
        /*
         * The structure we use to send work from outside crucible into the
         * Upstairs main task.
         * We create this here instead of inside up_main() so we can use
         * the methods provided by guest to interact with Crucible.
         */
        let guest = Arc::new(Guest::new(opt.block_size));

        tokio::spawn(up_main(
            crucible_opts.clone(),
            opt.gen,
            guest.clone(),
            None,
        )); // XXX increase gen per upstairs
        println!("Crucible runtime is spawned");

        cpfs.push(crucible::CruciblePseudoFile::from(guest)?);
    }

    use rand::Rng;
    let mut rng = rand::thread_rng();

    let rounds = 1500;
    let handoff_amount = rounds / opt.num_upstairs;
    let mut cpf_idx = 0;

    println!("Handing off to CPF {}", cpf_idx);
    cpfs[cpf_idx].activate(generation_number).await?;
    generation_number += 1;
    println!("Handed off to CPF {} {:?}", cpf_idx, cpfs[cpf_idx].uuid());

    if opt.verify_isolation {
        println!("clearing...");

        let cpf = &mut cpfs[0];
        cpf.seek(SeekFrom::Start(0))?;

        let bs = cpf.block_size();
        let sz = cpf.sz();

        for _ in 0..(sz / bs) {
            cpf.write_all(&vec![0; bs as usize])?;
        }
    }

    for idx in 0..rounds {
        let cpf = if idx / handoff_amount != cpf_idx {
            // One last flush
            cpfs[cpf_idx].flush()?;

            cpf_idx = idx / handoff_amount;
            assert!(cpf_idx != 0);

            println!("Handing off to CPF {}", cpf_idx);

            let cpf = &mut cpfs[cpf_idx];
            cpf.activate(generation_number).await?;
            generation_number += 1;

            println!("Handed off to CPF {} {:?}", cpf_idx, cpf.uuid());

            cpf
        } else {
            &mut cpfs[cpf_idx]
        };

        let sz = cpf.sz();

        let mut offset: u64 = rng.gen::<u64>() % sz;
        let mut bsz: usize = rng.gen::<usize>() % 4096;

        while ((offset + bsz as u64) > sz) || (bsz == 0) {
            offset = rng.gen::<u64>() % sz;
            bsz = rng.gen::<usize>() % 4096;
        }

        // println!("testing {}: offset {} sz {}", idx, offset, bsz);

        let vec: Vec<u8> = (0..bsz)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect();

        let mut vec2 = vec![0; bsz];

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.write_all(&vec[..])?;

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.read_exact(&mut vec2[..])?;

        if !do_vecs_match(&vec, &vec2) {
            println!("offset {} bsz {}", offset, bsz);
            println!("vec : {:?}", vec);
            println!("vec2: {:?}", vec2);

            assert_eq!(vec.len(), vec2.len());

            for i in 0..vec.len() {
                if vec[i] != vec2[i] {
                    println!("vec offset {}: {} != {}", i, vec[i], vec2[i]);
                } else {
                    println!("vec offset {} ok", i);
                }
            }

            bail!("vec != vec2");
        }

        if opt.verify_isolation {
            // Read back every byte not written to to make sure it's zero
            cpf.seek(SeekFrom::Start(0))?;

            // read from 0 -> offset
            let mut verify_vec: Vec<u8> = vec![0; offset as usize];
            cpf.read_exact(&mut verify_vec[..])?;

            for i in 0..offset {
                if verify_vec[i as usize] != 0 {
                    bail!("Not isolated! non-zero byte at {}", i);
                }
            }

            // read from (offset + bsz) -> sz
            cpf.seek(SeekFrom::Start(offset + bsz as u64))?;

            let len = sz - (offset + bsz as u64);
            let mut verify_vec: Vec<u8> = vec![0; len as usize];
            cpf.read_exact(&mut verify_vec[..])?;

            for i in 0..len {
                if verify_vec[i as usize] != 0 {
                    bail!(
                        "Not isolated! non-zero byte at {}",
                        (offset + bsz as u64) + i
                    );
                }
            }

            // Once done, zero out the write
            cpf.seek(SeekFrom::Start(offset))?;
            cpf.write_all(&vec![0; bsz])?;
        }
    }

    // One last flush
    cpfs[cpf_idx].flush()?;

    println!("Done ok, waiting on show_work");

    loop {
        let cpf = &mut cpfs[cpf_idx];
        let wc = cpf.show_work().await?;
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }

    Ok(())
}
