// Copyright 2022 Oxide Computer Company

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use rand::Rng;
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::time::{Duration, Instant};

use crucible::*;

#[derive(Debug, StructOpt)]
#[structopt(about = "measure iops!")]
pub struct Opt {
    // Upstairs options
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddr>,

    #[structopt(short, long)]
    key: Option<String>,

    #[structopt(short, long, default_value = "0")]
    gen: u64,

    #[structopt(long)]
    cert_pem: Option<String>,

    #[structopt(long)]
    key_pem: Option<String>,

    #[structopt(long)]
    root_cert_pem: Option<String>,

    // Tool options
    #[structopt(long, default_value = "100")]
    samples: usize,

    #[structopt(long)]
    iop_limit: Option<usize>,

    #[structopt(long)]
    io_size_in_bytes: Option<usize>,

    #[structopt(long)]
    io_depth: Option<usize>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();

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
        target: opt.target,
        lossy: false,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
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
        guest.set_iop_limit(16000, iop_limit);
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

    let write_buffers: Vec<Bytes> =
        (0..io_depth)
            .map(|_|
                Bytes::from(
                    (0..io_size)
                        .map(|_| rng.sample(rand::distributions::Standard))
                        .collect::<Vec<u8>>()
                )
            ).collect();

    let read_buffers: Vec<Buffer> =
        (0..io_depth)
            .map(|_| Buffer::new(io_size as usize))
            .collect();

    let mut io_operations_sent = 0;
    let mut io_operation_time = Instant::now();
    let mut iops: Vec<f32> = vec![];

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
            io_operations_sent += ceiling_div!(io_size, 16000);

            let diff = io_operation_time.elapsed();

            if diff > Duration::from_secs(1) {
                let total_io_operations_sent: f32 = io_operations_sent as f32;

                let fractional_seconds: f32 =
                    diff.as_secs() as f32 + (diff.subsec_nanos() as f32 / 1e9);

                iops.push(total_io_operations_sent / fractional_seconds);

                if iops.len() >= opt.samples {
                    break 'outer;
                }

                io_operations_sent = 0;
                io_operation_time = Instant::now();
            }
        }
    }

    // One last flush
    guest.flush()?;

    println!("Done ok, waiting on show_work");

    loop {
        let wc = guest.show_work()?;
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    iops.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!("IOPS: {:?}", iops);
    println!(
        "IOPS min {} max {} mean {} stddev {}",
        iops.first().unwrap(),
        iops.last().unwrap(),
        statistical::mean(&iops),
        statistical::standard_deviation(&iops, None),
    );

    Ok(())
}
