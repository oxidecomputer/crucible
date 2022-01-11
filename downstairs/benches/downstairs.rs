use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use rand::{Rng, RngCore};
use tempfile::tempdir;

use crucible_common::Block;
use crucible_downstairs::Region;

fn new_region_options(block_size: u64) -> crucible_common::RegionOptions {
    let mut region_options: crucible_common::RegionOptions = Default::default();
    region_options.set_block_size(block_size);
    region_options.set_extent_size(Block::new(10, block_size.trailing_zeros()));
    region_options
        .set_uuid("12345678-1111-2222-3333-123456789999".parse().unwrap());
    region_options
}

pub fn downstairs_rw_speed_benchmark<const BS: usize>(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut region =
        Region::create(&dir, new_region_options(BS as u64)).unwrap();
    region.extend(1024).unwrap();

    let ddef = region.def();
    let total_size: usize = ddef.total_size() as usize;
    let num_blocks: usize =
        ddef.extent_size().value as usize * ddef.extent_count() as usize;

    c.bench_function(format!("[{} sectors] region_write", BS).as_str(), |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
                buffer.resize(total_size, 0u8);
                rng.fill_bytes(&mut buffer);

                let mut writes: Vec<crucible_protocol::Write> =
                    Vec::with_capacity(num_blocks);

                for i in 0..num_blocks {
                    let eid: u64 = i as u64 / ddef.extent_size().value;
                    let offset: Block = Block::new(
                        (i as u64) % ddef.extent_size().value,
                        BS.trailing_zeros(),
                    );

                    let data =
                        BytesMut::from(&buffer[(i * BS)..((i + 1) * BS)]);

                    writes.push(crucible_protocol::Write {
                        eid,
                        offset,
                        data: data.freeze(),
                        encryption_context: None,
                    });
                }

                writes
            },
            |writes| {
                region.region_write(&writes).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function(format!("[{} sectors] region_read", BS).as_str(), |b| {
        b.iter_batched(
            || {
                let mut requests: Vec<crucible_protocol::ReadRequest> =
                    Vec::with_capacity(num_blocks);

                for i in 0..num_blocks {
                    let eid: u64 = i as u64 / ddef.extent_size().value;
                    let offset: Block = Block::new(
                        (i as u64) % ddef.extent_size().value,
                        BS.trailing_zeros(),
                    );

                    requests.push(crucible_protocol::ReadRequest {
                        eid,
                        offset,
                        num_blocks: 1,
                    });
                }

                requests
            },
            |requests| {
                let responses = region.region_read(&requests).unwrap();
                for response in responses {
                    assert!(response.encryption_contexts.is_empty());
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function(
        format!("[{} sectors] region_write with nonce and tag", BS).as_str(),
        |b| {
            b.iter_batched(
                || {
                    let mut rng = rand::thread_rng();
                    let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
                    buffer.resize(total_size, 0u8);
                    rng.fill_bytes(&mut buffer);

                    let mut writes: Vec<crucible_protocol::Write> =
                        Vec::with_capacity(num_blocks);

                    for i in 0..num_blocks {
                        let eid: u64 = i as u64 / ddef.extent_size().value;
                        let offset: Block = Block::new(
                            (i as u64) % ddef.extent_size().value,
                            BS.trailing_zeros(),
                        );

                        let data =
                            BytesMut::from(&buffer[(i * BS)..((i + 1) * BS)]);
                        let nonce = Vec::from(rng.gen::<[u8; 12]>());
                        let tag = Vec::from(rng.gen::<[u8; 16]>());

                        writes.push(crucible_protocol::Write {
                            eid,
                            offset,
                            data: data.freeze(),
                            encryption_context: Some(
                                crucible_protocol::EncryptionContext {
                                    nonce,
                                    tag,
                                },
                            ),
                        });
                    }

                    writes
                },
                |writes| {
                    region.region_write(&writes).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    c.bench_function(
        format!("[{} sectors] region_read with nonce and tag", BS).as_str(),
        |b| {
            b.iter_batched(
                || {
                    let mut requests: Vec<crucible_protocol::ReadRequest> =
                        Vec::with_capacity(num_blocks);

                    for i in 0..num_blocks {
                        let eid: u64 = i as u64 / ddef.extent_size().value;
                        let offset: Block = Block::new(
                            (i as u64) % ddef.extent_size().value,
                            BS.trailing_zeros(),
                        );

                        requests.push(crucible_protocol::ReadRequest {
                            eid,
                            offset,
                            num_blocks: 1,
                        });
                    }

                    requests
                },
                |requests| {
                    let responses = region.region_read(&requests).unwrap();
                    for response in responses {
                        assert!(!response.encryption_contexts.is_empty());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );
}

criterion_group!(
    benches,
    downstairs_rw_speed_benchmark<512>,
    downstairs_rw_speed_benchmark<4096>,
);

criterion_main!(benches);
