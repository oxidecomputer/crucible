// Copyright 2023 Oxide Computer Company

use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use crucible_common::Block;
use crucible_protocol::BlockContext;
use crucible_protocol::CrucibleDecoder;
use crucible_protocol::CrucibleEncoder;
use crucible_protocol::EncryptionContext;
use crucible_protocol::JobId;
use crucible_protocol::Message;

use anyhow::Result;
use clap::Parser;
use futures::SinkExt;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[clap(about = "Protocol serialization and deserialization speed test")]
enum Args {
    Dynamometer {
        #[clap(short, long, default_value_t = 512)]
        write_size: usize,

        /// Number of write protocol messages to submit at one time
        #[clap(short, long, default_value_t = 1)]
        num_writes: usize,

        /// Number of write requests per write protocol message
        #[clap(short, long, default_value_t = 1)]
        requests_per_write: usize,

        /// Number of samples to exit for
        #[clap(short, long, default_value_t = 10)]
        samples: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;

    match args {
        Args::Dynamometer {
            write_size,
            num_writes,
            requests_per_write,
            samples,
        } => {
            dynamometer(write_size, num_writes, requests_per_write, samples)
                .await?;
        }
    }

    Ok(())
}

macro_rules! ceiling_div {
    ($a: expr, $b: expr) => {
        ($a + ($b - 1)) / $b
    };
}

async fn dynamometer(
    write_size: usize,
    num_writes: usize,
    requests_per_write: usize,
    samples: usize,
) -> Result<()> {
    // downstairs
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let listener = TcpListener::bind(&bind_addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (sock, _raddr) = listener.accept().await.unwrap();

        let (read, write) = sock.into_split();

        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

        while let Some(m) = fr.next().await.transpose().unwrap() {
            match m {
                Message::Write {
                    upstairs_id,
                    session_id,
                    job_id,
                    ..
                } => {
                    fw.send(Message::WriteAck {
                        upstairs_id,
                        session_id,
                        job_id,
                        result: Ok(()),
                    })
                    .await
                    .unwrap();
                }

                _ => panic!("unknown message"),
            }
        }
    });

    // upstairs
    let sock = TcpSocket::new_v4()?;
    let tcp = sock.connect(local_addr).await?;
    let (read, write) = tcp.into_split();

    let mut fr = FramedRead::new(read, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

    // measurement related
    let mut io_operations_sent = 0;
    let mut bw_consumed = 0;
    let mut measurement_time = Instant::now();
    let mut total_io_time = Duration::ZERO;
    let mut iops: Vec<f32> = vec![];
    let mut bws: Vec<f32> = vec![];

    let upstairs_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let job_id = JobId(12345);

    loop {
        for _ in 0..num_writes {
            let writes = (0..requests_per_write)
                .map(|_| crucible_protocol::Write {
                    eid: 55,
                    offset: Block::new_512(99),
                    data: bytes::Bytes::from(vec![0x55; write_size]),
                    block_context: BlockContext {
                        hash: 1283746189273,
                        encryption_context: Some(EncryptionContext {
                            nonce: vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
                            tag: vec![
                                1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
                            ],
                        }),
                    },
                })
                .collect();

            // Measure the network round trip time only
            let io_operation_time = Instant::now();

            fw.send(Message::Write {
                upstairs_id,
                session_id,
                job_id,
                dependencies: (0..16).map(JobId).collect(),
                writes,
            })
            .await?;

            match fr.next().await.transpose()? {
                Some(m) => match m {
                    Message::WriteAck { .. } => {
                        // ok
                    }

                    _ => panic!("unknown message"),
                },

                None => panic!("disconnected!"),
            }

            total_io_time += io_operation_time.elapsed();
        }

        io_operations_sent += num_writes;
        io_operations_sent +=
            ceiling_div!(num_writes * write_size, 16 * 1024 * 1024);
        bw_consumed += num_writes * write_size;

        if measurement_time.elapsed() > Duration::from_secs(1) {
            let fractional_seconds: f32 = total_io_time.as_secs() as f32
                + (total_io_time.subsec_nanos() as f32 / 1e9);

            iops.push(io_operations_sent as f32 / fractional_seconds);
            bws.push(bw_consumed as f32 / fractional_seconds);

            io_operations_sent = 0;
            bw_consumed = 0;
            measurement_time = Instant::now();
            total_io_time = Duration::ZERO;

            if iops.len() >= samples {
                break;
            }
        }
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
