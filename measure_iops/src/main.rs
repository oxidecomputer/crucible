// Copyright 2022 Oxide Computer Company

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use rand::Rng;
use tokio::runtime::Builder;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

use crucible::*;

#[derive(Debug, Parser)]
#[clap(about = "measure iops!")]
pub struct Opt {
    // Upstairs options
    #[clap(short, long, default_value = "127.0.0.1:9000", action)]
    target: Vec<SocketAddr>,

    #[clap(short, long, action)]
    key: Option<String>,

    #[clap(short, long, default_value = "0", action)]
    gen: u64,

    #[clap(long, action)]
    cert_pem: Option<String>,

    #[clap(long, action)]
    key_pem: Option<String>,

    #[clap(long, action)]
    root_cert_pem: Option<String>,

    // Tool options
    #[clap(long, default_value = "100", action)]
    samples: usize,

    #[clap(long, action)]
    iop_limit: Option<usize>,

    #[clap(long, action)]
    io_size_in_bytes: Option<usize>,

    #[clap(long, action)]
    io_depth: Option<usize>,

    #[clap(long, action)]
    bw_limit_in_bytes: Option<usize>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

macro_rules! ceiling_div {
    ($a: expr, $b: expr) => {
        ($a + ($b - 1)) / $b
    };
}

fn main() -> Result<()> {
    let opt = opts()?;

    let crucible_opts = CrucibleOpts {
        id: Uuid::new_v4(),
        target: opt.target,
        lossy: false,
        flush_timeout: None,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: None,
        metric_collect: None,
        metric_register: None,
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
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let mut guest = Guest::new();

    if let Some(iop_limit) = opt.iop_limit {
        guest.set_iop_limit(16 * 1024 * 1024, iop_limit);
    }

    if let Some(bw_limit) = opt.bw_limit_in_bytes {
        guest.set_bw_limit(bw_limit);
    }

    let guest = Arc::new(guest);

    runtime.spawn(up_main(crucible_opts, guest.clone()));
    println!("Crucible runtime is spawned");

    guest.activate(opt.gen)?;

    let mut rng = rand::thread_rng();

    let bsz: u64 = guest.query_block_size()?;
    let total_blocks: u64 = guest.query_total_size()? / bsz;

    let io_size = if let Some(io_size_in_bytes) = opt.io_size_in_bytes {
        io_size_in_bytes
    } else {
        bsz as usize
    };

    let io_depth = if let Some(io_depth) = opt.io_depth {
        io_depth
    } else {
        1
    };

    let write_buffers: Vec<Bytes> = (0..io_depth)
        .map(|_| {
            Bytes::from(
                (0..io_size)
                    .map(|_| rng.sample(rand::distributions::Standard))
                    .collect::<Vec<u8>>(),
            )
        })
        .collect();

    let read_buffers: Vec<Buffer> = (0..io_depth)
        .map(|_| Buffer::new(io_size as usize))
        .collect();

    let mut io_operations_sent = 0;
    let mut bw_consumed = 0;
    let mut io_operation_time = Instant::now();
    let mut iops: Vec<f32> = vec![];
    let mut bws: Vec<f32> = vec![];

    'outer: loop {
        let mut waiters = Vec::with_capacity(io_depth);

        for i in 0..io_depth {
            let offset: u64 =
                rng.gen::<u64>() % (total_blocks - io_size as u64 / bsz as u64);

            if rng.gen::<bool>() {
                let waiter = guest.write_to_byte_offset(
                    offset * bsz,
                    write_buffers[i].clone(),
                )?;

                waiters.push(waiter);
            } else {
                let waiter = guest.read_from_byte_offset(
                    offset * bsz,
                    read_buffers[i].clone(),
                )?;

                waiters.push(waiter);
            }
        }

        for mut waiter in waiters {
            waiter.block_wait()?;
            io_operations_sent += ceiling_div!(io_size, 16 * 1024 * 1024);
            bw_consumed += io_size;

            let diff = io_operation_time.elapsed();

            if diff > Duration::from_secs(1) {
                let fractional_seconds: f32 =
                    diff.as_secs() as f32 + (diff.subsec_nanos() as f32 / 1e9);

                iops.push(io_operations_sent as f32 / fractional_seconds);
                bws.push(bw_consumed as f32 / fractional_seconds);

                if iops.len() >= opt.samples {
                    break 'outer;
                }

                io_operations_sent = 0;
                bw_consumed = 0;
                io_operation_time = Instant::now();
            }
        }
    }

    // One last flush
    guest.flush(None)?;

    println!("Done ok, waiting on show_work");

    loop {
        let wc = guest.show_work()?;
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    println!("IOPS: {:?}", iops);
    println!(
        "IOPS mean {} stddev {}",
        statistical::mean(&iops),
        statistical::standard_deviation(&iops, None),
    );

    iops.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "IOPS min {} max {}",
        iops.first().unwrap(),
        iops.last().unwrap(),
    );

    println!("BW: {:?}", bws);
    println!(
        "BW mean {} stddev {}",
        statistical::mean(&bws),
        statistical::standard_deviation(&bws, None),
    );

    bws.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "BW min {} max {}",
        bws.first().unwrap(),
        bws.last().unwrap(),
    );

    Ok(())
}
