// Copyright 2023 Oxide Computer Company

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::Parser;
use rand::Rng;
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

    /// Submit all zeroes instead of random data
    #[clap(long, action)]
    all_zeroes: bool,

    /// How long to wait before the auto flush check fires
    #[clap(long, action)]
    flush_timeout: Option<f32>,
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

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;

    let crucible_opts = CrucibleOpts {
        id: Uuid::new_v4(),
        target: opt.target,
        lossy: false,
        flush_timeout: opt.flush_timeout,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: None,
        read_only: false,
    };

    /*
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let mut guest = Guest::new(None);

    if let Some(iop_limit) = opt.iop_limit {
        guest.set_iop_limit(16 * 1024 * 1024, iop_limit);
    }

    if let Some(bw_limit) = opt.bw_limit_in_bytes {
        guest.set_bw_limit(bw_limit);
    }

    let guest = Arc::new(guest);
    let _join_handle =
        up_main(crucible_opts, opt.gen, None, guest.clone(), None)?;
    println!("Crucible runtime is spawned");

    guest.activate().await?;

    let mut rng = rand::thread_rng();

    let bsz: u64 = guest.get_block_size().await?;
    let total_blocks: u64 = guest.total_size().await? / bsz;

    let io_size = if let Some(io_size_in_bytes) = opt.io_size_in_bytes {
        if io_size_in_bytes as u64 % bsz != 0 {
            bail!(
                "invalid io size: {io_size_in_bytes} is not divisible by {bsz}"
            );
        } else {
            io_size_in_bytes
        }
    } else {
        bsz as usize
    };

    let io_depth = if let Some(io_depth) = opt.io_depth {
        io_depth
    } else {
        1
    };

    let mut io_operations_sent = 0;
    let mut bw_consumed = 0;
    let mut measurement_time = Instant::now();
    let mut total_io_time = Duration::ZERO;
    let mut iops: Vec<f32> = vec![];
    let mut bws: Vec<f32> = vec![];

    'outer: loop {
        enum RandomOp {
            Read(u64, Buffer),
            Write(u64, Bytes),
        }

        let mut ops = Vec::with_capacity(io_depth);
        for _ in 0..io_depth {
            let offset: u64 =
                rng.gen::<u64>() % (total_blocks - io_size as u64 / bsz);

            if rng.gen::<bool>() {
                ops.push(RandomOp::Read(
                    offset,
                    Buffer::new(io_size / bsz as usize, bsz as usize),
                ));
            } else {
                let bytes = Bytes::from(if opt.all_zeroes {
                    vec![0u8; io_size]
                } else {
                    (0..io_size)
                        .map(|_| rng.sample(rand::distributions::Standard))
                        .collect::<Vec<u8>>()
                });
                ops.push(RandomOp::Write(offset, bytes));
            }
        }

        let mut futures = Vec::with_capacity(io_depth);

        let io_operation_time = Instant::now();

        for op in ops {
            let guest = guest.clone();
            match op {
                RandomOp::Read(offset, mut buffer) => {
                    futures.push(tokio::spawn(async move {
                        guest
                            .read_from_byte_offset(offset * bsz, &mut buffer)
                            .await
                    }));
                }

                RandomOp::Write(offset, bytes) => {
                    futures.push(tokio::spawn(async move {
                        guest.write_to_byte_offset(offset * bsz, bytes).await
                    }));
                }
            }
        }

        for future in futures {
            future.await??;
        }

        total_io_time += io_operation_time.elapsed();
        io_operations_sent +=
            ceiling_div!(io_size * io_depth, 16 * 1024 * 1024);
        bw_consumed += io_size * io_depth;

        if measurement_time.elapsed() > Duration::from_secs(1) {
            let fractional_seconds: f32 = total_io_time.as_secs() as f32
                + (total_io_time.subsec_nanos() as f32 / 1e9);

            iops.push(io_operations_sent as f32 / fractional_seconds);
            bws.push(bw_consumed as f32 / fractional_seconds);

            if iops.len() >= opt.samples {
                break 'outer;
            }

            io_operations_sent = 0;
            bw_consumed = 0;
            measurement_time = Instant::now();
            total_io_time = Duration::ZERO;
        }
    }

    // One last flush
    guest.flush(None).await?;

    println!("Done ok, waiting on show_work");

    loop {
        let wc = guest.show_work().await?;
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
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
