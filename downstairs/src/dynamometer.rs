// Copyright 2023 Oxide Computer Company
use super::*;

use futures::TryStreamExt;

pub enum DynoFlushConfig {
    FlushPerIops(usize),
    FlushPerBlocks(usize),
    FlushPerMs(Duration),
    None,
}

pub fn dynamometer(
    mut region: Region,
    num_writes: usize,
    samples: usize,
    flush_config: DynoFlushConfig,
) -> Result<()> {
    // TODO: pull into another crate? this is copied from measure-iops tool
    let mut io_operations_sent = 0;
    let mut bw_consumed = 0;
    let mut measurement_time = Instant::now();
    let mut total_io_time = Duration::ZERO;
    let mut iops: Vec<f32> = vec![];
    let mut bws: Vec<f32> = vec![];

    let mut flush_number = 0;
    let mut gen_number = 0;
    let mut flush_time = Instant::now();
    let mut iops_since_last_flush = 0;
    let mut blocks_since_last_flush = 0;

    let ddef = region.def();
    eprintln!("{:?}", ddef);

    // Fill test: write bytes in whole region

    let mut rng = SmallRng::from_entropy();

    'outer: loop {
        let block = (0..ddef.block_size() as usize)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect::<Vec<u8>>();
        let nonce = (0..12)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect::<Vec<u8>>();
        let tag = (0..16)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect::<Vec<u8>>();

        let hash = integrity_hash(&[&nonce, &tag, &block]);

        let block_bytes = bytes::Bytes::from(block.clone());

        for eid in (0..ddef.extent_count()).map(ExtentId) {
            let mut block_offset = 0;
            loop {
                if (block_offset + num_writes as u64)
                    >= ddef.extent_size().value
                {
                    break;
                }

                let nonce = nonce.clone();
                let tag = tag.clone();

                let writes: Vec<_> = (0..num_writes)
                    .map(|i| {
                        let ctx = BlockContext {
                            hash,
                            encryption_context: Some(
                                crucible_protocol::EncryptionContext {
                                    nonce: nonce.as_slice().try_into().unwrap(),
                                    tag: tag.as_slice().try_into().unwrap(),
                                },
                            ),
                        };
                        RegionWriteReq {
                            extent: eid,
                            write: ExtentWrite {
                                offset: BlockOffset(i as u64 + block_offset),
                                data: block_bytes.clone(),
                                block_contexts: vec![ctx],
                            },
                        }
                    })
                    .collect();
                let rw = RegionWrite(writes);

                let io_operation_time = Instant::now();
                region.region_write(&rw, JobId(1000), false)?;

                total_io_time += io_operation_time.elapsed();
                io_operations_sent += num_writes;
                iops_since_last_flush += num_writes;
                blocks_since_last_flush += num_writes;
                bw_consumed += num_writes * ddef.block_size() as usize;

                if measurement_time.elapsed() > Duration::from_secs(1) {
                    let fractional_seconds: f32 = total_io_time.as_secs()
                        as f32
                        + (total_io_time.subsec_nanos() as f32 / 1e9);

                    iops.push(io_operations_sent as f32 / fractional_seconds);
                    bws.push(bw_consumed as f32 / fractional_seconds);

                    io_operations_sent = 0;
                    bw_consumed = 0;
                    measurement_time = Instant::now();
                    total_io_time = Duration::ZERO;

                    if iops.len() >= samples {
                        break 'outer;
                    }
                }

                let needs_flush = match flush_config {
                    DynoFlushConfig::FlushPerIops(value)
                        if iops_since_last_flush > value =>
                    {
                        iops_since_last_flush = 0;
                        true
                    }

                    DynoFlushConfig::FlushPerBlocks(value)
                        if blocks_since_last_flush > value =>
                    {
                        blocks_since_last_flush = 0;
                        true
                    }

                    DynoFlushConfig::FlushPerMs(value)
                        if flush_time.elapsed() > value =>
                    {
                        flush_time = Instant::now();
                        true
                    }

                    _ => false,
                };

                if needs_flush {
                    region.region_flush(
                        flush_number,
                        gen_number,
                        &None, // snapshot_details
                        JobId(1000),
                        None, // extent_limit
                    )?;

                    flush_number += 1;
                    gen_number += 1;
                }

                block_offset += num_writes as u64;
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

pub async fn repair_dynamometer(clone_source: SocketAddr) -> Result<()> {
    let mut bytes_received = 0;
    let mut measurement_time = Instant::now();
    let mut bytes_per_second: Vec<f32> = vec![];

    let url = format!("http://{:?}", clone_source);
    println!("using {url}");
    let repair = repair_client::Client::new(&url);

    let source_def = match repair.get_region_info().await {
        Ok(def) => def.into_inner(),
        Err(e) => {
            bail!("Failed to get source region definition: {e}");
        }
    };

    println!("The source RegionDefinition is: {:?}", source_def);

    let source_ro_mode = match repair.get_region_mode().await {
        Ok(ro) => ro.into_inner(),
        Err(e) => {
            bail!("Failed to get source mode: {e}");
        }
    };

    println!("The source mode is: {:?}", source_ro_mode);
    if !source_ro_mode {
        bail!("Source downstairs is not read only");
    }

    for eid in (0..source_def.extent_count()).map(ExtentId) {
        println!("Repair extent {eid}");

        let mut repair_files = match repair.get_files_for_extent(eid.0).await {
            Ok(f) => f.into_inner(),
            Err(e) => {
                bail!("Failed to get repair files: {:?}", e,);
            }
        };

        repair_files.sort();
        println!("eid:{} Found repair files: {:?}", eid, repair_files);

        let mut stream = match repair
            .get_extent_file(eid.0, repair_client::types::FileType::Data)
            .await
        {
            Ok(rs) => rs,
            Err(e) => {
                bail!("Failed to get extent {} db file: {:?}", eid, e,);
            }
        };

        loop {
            match stream.try_next().await {
                Ok(Some(bytes)) => {
                    bytes_received += bytes.len();

                    let elapsed = measurement_time.elapsed();

                    if elapsed > Duration::from_secs(1) {
                        let fractional_seconds: f32 = elapsed.as_secs() as f32
                            + (elapsed.subsec_nanos() as f32 / 1e9);

                        println!(
                            "bytes per second: {}",
                            bytes_received as f32 / fractional_seconds
                        );
                        bytes_per_second
                            .push(bytes_received as f32 / fractional_seconds);
                        bytes_received = 0;
                        measurement_time = Instant::now();
                    }
                }

                Ok(None) => break,

                Err(e) => {
                    bail!("repair stream error: {:?}", e);
                }
            }
        }
    }

    println!("B/s: {:?}", bytes_per_second);
    println!(
        "B/S mean {} stddev {}",
        statistical::mean(&bytes_per_second),
        statistical::standard_deviation(&bytes_per_second, None),
    );

    bytes_per_second
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "B/s min {} max {}",
        bytes_per_second.first().unwrap(),
        bytes_per_second.last().unwrap(),
    );

    Ok(())
}
