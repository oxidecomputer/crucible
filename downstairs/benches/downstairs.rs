use criterion::{criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use rand::{Rng, RngCore};
use tempfile::tempdir;

use crucible_common::Block;
use crucible_downstairs::Region;

fn new_region_options() -> crucible_common::RegionOptions {
    let mut region_options: crucible_common::RegionOptions = Default::default();
    let block_size = 512;
    region_options.set_block_size(block_size);
    region_options.set_extent_size(Block::new(10, block_size.trailing_zeros()));
    region_options
        .set_uuid("12345678-1111-2222-3333-123456789999".parse().unwrap());
    region_options
}

pub fn downstairs_rw_speed_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut region = Region::create(&dir, new_region_options()).unwrap();
    region.extend(1024).unwrap();

    let ddef = region.def();
    let total_size: usize = ddef.total_size() as usize;
    let num_blocks: usize =
        ddef.extent_size().value as usize * ddef.extent_count() as usize;

    c.bench_function("region_write", |b| {
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                nonce: None,
                tag: None,
            });
        }

        b.iter(|| {
            region.region_write(&writes).unwrap();
        })
    });

    c.bench_function("region_read", |b| {
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
        }

        b.iter(|| {
            let responses = region.region_read(&requests).unwrap();
            for response in responses {
                assert!(response.nonce.is_none());
            }
        })
    });

    c.bench_function("region_write with nonce and tag", |b| {
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                nonce: Some(Vec::from(rng.gen::<[u8; 12]>())),
                tag: Some(Vec::from(rng.gen::<[u8; 16]>())),
            });
        }

        b.iter(|| {
            region.region_write(&writes).unwrap();
        })
    });

    c.bench_function("region_read with nonce and tag", |b| {
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
        }

        b.iter(|| {
            let responses = region.region_read(&requests).unwrap();

            for response in responses {
                assert!(response.nonce.is_some());
            }
        })
    });
}

criterion_group!(benches, downstairs_rw_speed_benchmark);

criterion_main!(benches);
