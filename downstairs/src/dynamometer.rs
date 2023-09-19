// Copyright 2023 Oxide Computer Company
use super::*;

pub async fn dynamometer(mut region: Region) -> Result<()> {
    // TODO: pull into another crate? this is copied from measure-iops tool
    let mut io_operations_sent = 0;
    let mut bw_consumed = 0;
    let mut measurement_time = Instant::now();
    let mut total_io_time = Duration::ZERO;
    let mut iops: Vec<f32> = vec![];
    let mut bws: Vec<f32> = vec![];

    let ddef = region.def();

    // Fill test: write bytes in whole region
    let block = vec![0x1; ddef.block_size() as usize];
    let nonce =
        vec![0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb];
    let tag = vec![
        0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd,
        0xe, 0xf,
    ];
    let hash = integrity_hash(&[&nonce, &tag, &block]);

    for eid in 0..ddef.extent_count() {
        for block_offset in 0..ddef.extent_size().value {
            let block = block.clone();
            let nonce = nonce.clone();
            let tag = tag.clone();

            let io_operation_time = Instant::now();
            region
                .region_write(
                    &[crucible_protocol::Write {
                        eid: eid as u64,
                        offset: Block::new_with_ddef(block_offset, &ddef),
                        data: bytes::Bytes::from(block),
                        block_context: BlockContext {
                            hash,
                            encryption_context: Some(
                                crucible_protocol::EncryptionContext {
                                    nonce,
                                    tag,
                                },
                            ),
                        },
                    }],
                    JobId(1000),
                    false,
                )
                .await?;

            total_io_time += io_operation_time.elapsed();
            io_operations_sent += 1;
            bw_consumed += ddef.block_size();

            if measurement_time.elapsed() > Duration::from_secs(1) {
                let fractional_seconds: f32 = total_io_time.as_secs() as f32
                    + (total_io_time.subsec_nanos() as f32 / 1e9);

                iops.push(io_operations_sent as f32 / fractional_seconds);
                bws.push(bw_consumed as f32 / fractional_seconds);

                io_operations_sent = 0;
                bw_consumed = 0;
                measurement_time = Instant::now();
                total_io_time = Duration::ZERO;
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

    // Random write test

    Ok(())
}
