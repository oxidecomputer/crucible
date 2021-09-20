// Copyright 2021 Oxide Computer Company
use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use rand::prelude::*;
use rand_chacha::rand_core::SeedableRng;
use structopt::clap::arg_enum;
use structopt::StructOpt;
use tokio::runtime::Builder;

use crucible::*;
use crucible_common::Block;

/*
 * The various tests this program supports.
 */
arg_enum! {
    #[derive(Debug, StructOpt)]
    enum Workload {
        Balloon,
        Big,
        Demo,
        Dep,
        One,
        Rand,
        Span,
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "crucible upstairs test client")]
pub struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,

    #[structopt(short, long, possible_values = &Workload::variants(), default_value = "One", case_insensitive = true)]
    workload: Workload,

    /*
     * This allows the Upstairs to run in a mode where it will not
     * always submit new work to downstairs when it first receives
     * it.  This is for testing dependencies and should not be
     * used in production.  Passing args like this to the upstairs
     * may not be the best way to test, but until we have something
     * better... XXX
     */
    #[structopt(long)]
    lossy: bool,

    /*
     * quit after all crucible work queues are empty.
     */
    #[structopt(short, long)]
    quit: bool,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

/**
 * This is an example Crucible client.
 * Here we make use of the interfaces that Crucible exposes.
 */
fn main() -> Result<()> {
    /*
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let opt = opts()?;

    /*
     * If any of our async tasks in our runtime panic, then we should
     * exit the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: opt.lossy,
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

    std::thread::sleep(std::time::Duration::from_secs(2));

    println!("Wait for a show_work command to finish before sending IO");
    guest.show_work();
    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside
     * (Propolis).  XXX Test code here..
     * runtime.spawn(run_scope(prop_work));
     */

    /*
     * Call the function for the workload option passed from the command line.
     */
    match opt.workload {
        Workload::Balloon => {
            println!("Run balloon test");
            runtime.block_on(balloon_workload(&guest))?;
        }
        Workload::Big => {
            println!("Run big test");
            big_workload(&guest)?;
        }
        Workload::Demo => {
            println!("Run Demo test");
            runtime.block_on(demo_workload(&guest, 10))?;
        }
        Workload::Dep => {
            println!("Run dep test");
            /*
             * Attempted workaround to handle tokio async issues
             * between this and the actual upstairs runtime.  I'm
             * not convinced this actually does any better.
            let dep_runtime = Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("crucible-deptest")
                .enable_all()
                .build()
                .unwrap();
            println!("dep runtime started");
             */
            runtime.block_on(dep_workload(&guest))?;
        }
        Workload::One => {
            println!("One test");
            runtime.block_on(rand_workload(&guest, 1))?;
        }
        Workload::Rand => {
            println!("Run random test");
            runtime.block_on(rand_workload(&guest, 50))?;
        }
        Workload::Span => {
            println!("Span test");
            span_workload(&guest)?;
        }
    }

    println!("Tests done.  All submitted work has been ackd");
    loop {
        let wc = guest.show_work();
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if opt.quit && wc.up_count + wc.ds_count == 0 {
            println!("All crucible jobs finished, exiting program");
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
}

/*
 * Given the write count vec and the index, pick the seed value we use when
 * writing or expect when reading block.
 * TODO: This can be improved on.  The mod at 250 was me leaving some room
 * for some special values, though I may or may not use them.
 */
fn get_seed(index: usize, wc: &[u32]) -> u8 {
    (wc[index] % 250) as u8
}

/*
 * Fill a vec based on the write count at our index.
 * This is fine for an initial fill/verify framework of sorts, but there
 * are many kinds of errors this will not find.  There are also many high
 * performance better coverage kinds of data integrity tests, and the intent
 * here is to balance urgency with rigor in that we can make use of external
 * tests for the more complicated cases, and catch the easy ones here.
 *
 * block_index: What block we started reading from.
 * blocks:      The length of the read in blocks.
 * wc:          The write count vec, indexed by block number.
 * bs:          Crucible's block size.
 */
fn fill_vec(block_index: usize, blocks: usize, wc: &[u32], bs: u64) -> Vec<u8> {
    assert_ne!(blocks, 0);
    assert_ne!(bs, 0);
    /*
     * Each block we are filling the buffer for can have a different
     * seed value.  For multiple block sized writes, we need to create
     * the write buffer with the correct seed value.
     */
    let mut vec: Vec<u8> = Vec::with_capacity(blocks * bs as usize);
    for block_offset in block_index..(block_index + blocks) {
        /*
         * The start of each block contains that blocks index mod 255
         */
        let seed = get_seed(block_offset, wc);
        /*
         * Fill the rest of the buffer with the new write count
         */
        vec.push((block_offset % 255) as u8);
        for _ in 1..bs {
            vec.push(seed);
        }
    }
    vec
}

/*
 * Compare a vec buffer with what we expect to be written for that offset.
 * This assumes you used the fill_vec function (with get_seed) to write
 * the buffer originally.
 *
 * data:        The filled in buffer to be verified.
 * block_index: What block we started reading from.
 * wc:          The write count vec, indexed by block number.
 * bs:          Crucible's block size.
 */
fn validate_vec(
    data: Vec<u8>,
    block_index: usize,
    wc: &[u32],
    bs: u64,
) -> bool {
    let bs = bs as usize;
    assert_eq!(data.len() % bs, 0);
    assert_ne!(data.len(), 0);

    let blocks = data.len() / bs;
    let mut data_offset: usize = 0;
    let mut res = true;
    /*
     * The outer loop walks the buffer by blocks, as each block will have
     * its own unique write count.
     */
    for block_offset in block_index..(block_index + blocks) {
        /*
         * Skip blocks we don't know the expected value of
         */
        if wc[block_offset] == 0 {
            data_offset += bs;
            continue;
        }

        /*
         * First check the initial value to verify it has the block number.
         */
        if data[data_offset] != (block_offset % 255) as u8 {
            let byte_offset = bs as u64 * block_offset as u64;
            println!(
                "BO:{} Offset:{}  Expected: {} != Got: {} Block Index",
                block_offset,
                byte_offset,
                block_offset % 255,
                data[data_offset],
            );
            res = false;
        }

        let seed = get_seed(block_offset, wc);
        for i in 1..bs {
            if data[data_offset + i] != seed {
                let byte_offset = bs as u64 * block_offset as u64;
                println!(
                    "BO:{} Offset:{}  Expected: {} != Got: {}",
                    block_offset,
                    byte_offset + i as u64,
                    seed,
                    data[i],
                );
                res = false;
            }
        }
        data_offset += bs as usize;
    }
    res
}

/*
 * Write then read (and verify) to every possible block, with every size that
 * block can possibly support.
 * I named it balloon because each loop on a block "balloons" from the minimum
 * IO size to the largest possible.
 */
async fn balloon_workload(guest: &Arc<Guest>) -> Result<()> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let block_size = guest.query_block_size();
    let extent_size = guest.query_extent_size();
    let total_size = guest.query_total_size();
    let total_blocks = (total_size / block_size) as usize;

    println!(
        "Balloon workload starting with  es: {:?}  bs:{}  ts:{}  tb:{}",
        extent_size, block_size, total_size, total_blocks
    );

    /*
     * Create an array that tracks the number of writes to each block, so
     * we can know what to expect for reads.
     */
    let mut write_count = vec![0_u32; total_blocks];

    for block_index in 0..total_blocks {
        /*
         * Loop over all the IO sizes (in blocks) that an IO can
         * have, given our starting block and the total number of blocks
         * We always have at least one block, and possibly more.
         */
        for size in 1..=(total_blocks - block_index) {
            /*
             * Update the write count for all blocks we plan to write to.
             */
            for i in 0..size {
                write_count[block_index + i] += 1;
            }

            let vec = fill_vec(block_index, size, &write_count, block_size);
            let data = Bytes::from(vec);
            /*
             * Convert block_index to its byte value.
             */
            let offset =
                Block::new(block_index as u64, block_size.trailing_zeros());

            let mut waiter = guest.write(offset, data);
            waiter.block_wait();

            let mut waiter = guest.flush();
            waiter.block_wait();

            let length: usize = size * block_size as usize;
            let vec: Vec<u8> = vec![255; length];
            let data = crucible::Buffer::from_vec(vec);
            let mut waiter = guest.read(offset, data.clone());
            waiter.block_wait();

            let dl = data.as_vec().to_vec();
            if !validate_vec(dl.clone(), block_index, &write_count, block_size)
            {
                bail!("Error at {}", block_index);
            }
        }
    }

    print_write_count(write_count, total_blocks, extent_size);
    Ok(())
}

/*
 * Print out the contents of the write count vector where each line
 * is an extent and each column is a block.
 */
fn print_write_count(
    write_count: Vec<u32>,
    total_blocks: usize,
    extent_size: Block,
) {
    println!(" Write IO count to each extent");
    println!("###############################");
    for (i, wc) in write_count.iter().enumerate().take(total_blocks) {
        print!("{:5} ", wc);
        if (i + 1) % (extent_size.value as usize) == 0 {
            println!();
        }
    }
}

/*
 * Generate a random offset and length, and write to then read from
 * that offset/length.  Verify the data is what we expect.
 */
async fn rand_workload(guest: &Arc<Guest>, count: u32) -> Result<()> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let block_size = guest.query_block_size();
    let extent_size = guest.query_extent_size();
    let total_size = guest.query_total_size();
    let total_blocks = (total_size / block_size) as usize;

    println!(
        "workload {} loop(s), starting with  es: {:?}  bs:{}  ts:{}  tb:{}",
        count, extent_size, block_size, total_size, total_blocks
    );

    /*
     * Create an array that tracks the number of writes to each block, so
     * we can know what to expect for reads.
     */
    let mut write_count = vec![0_u32; total_blocks];

    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * TODO: Let the user select the number of loops
     */
    for _ in 0..count {
        /*
         * Pick a random size (in blocks) for the IO, up to the size of the
         * entire region.
         */
        let size = rng.gen_range(1..=total_blocks) as usize;

        /*
         * Once we have our IO size, decide where the starting offset should
         * be, which is the total possible size minus the randomly chosen
         * IO size.
         */
        let block_max = total_blocks - size + 1;
        let block_index = rng.gen_range(0..block_max) as usize;

        /*
         * Convert offset and length to their byte values.
         */
        let offset =
            Block::new(block_index as u64, block_size.trailing_zeros());

        /*
         * Update the write count for all blocks we plan to write to.
         */
        for i in 0..size {
            write_count[block_index + i] += 1;
        }

        let vec = fill_vec(block_index, size, &write_count, block_size);
        let data = Bytes::from(vec);

        if count < 10 {
            // When count is small, print out the IO info
            println!("IO at block {}, len:{}", offset.value, data.len());
        }
        let mut waiter = guest.write(offset, data);
        waiter.block_wait();

        let mut waiter = guest.flush();
        waiter.block_wait();

        let length: usize = size * block_size as usize;
        let vec: Vec<u8> = vec![255; length];
        let data = crucible::Buffer::from_vec(vec);
        let mut waiter = guest.read(offset, data.clone());
        waiter.block_wait();

        let dl = data.as_vec().to_vec();
        if !validate_vec(dl.clone(), block_index, &write_count, block_size) {
            bail!("Error at {}", block_index);
        }
    }

    if count >= 10 {
        print_write_count(write_count, total_blocks, extent_size);
    }
    Ok(())
}

/*
 * Like the random test, but with IO not as large, and with frequent
 * showing of the internal work queues.  Submit a bunch of random IOs,
 * then watch them complete.
 */
async fn demo_workload(guest: &Arc<Guest>, count: u32) -> Result<()> {
    /*
     * These query requests have the side effect of preventing the test from
     * starting before the upstairs is ready.
     */
    let block_size = guest.query_block_size();
    let extent_size = guest.query_extent_size();
    let total_size = guest.query_total_size();
    let total_blocks = (total_size / block_size) as usize;

    println!(
        "demo {} loop(s), starting with  es: {:?}  bs:{}  ts:{}  tb:{}",
        count, extent_size, block_size, total_size, total_blocks
    );

    // Create an array that tracks the number of writes to each block, so
    // we can know what to expect for reads.
    let mut write_count = vec![0_u32; total_blocks];

    // TODO: Allow the user to specify a seed here.
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    let mut waiterlist = Vec::new();
    // TODO: Let the user select the number of loops
    for _ in 0..count {
        let op = rng.gen_range(0..3);
        if op == 0 {
            // flush
            let waiter = guest.flush();
            waiterlist.push(waiter);
        } else {
            // Read or Write both need this
            // Pick a random size (in blocks) for the IO, up to 10
            let size = rng.gen_range(1..=10) as usize;

            // Once we have our IO size, decide where the starting offset should
            // be, which is the total possible size minus the randomly chosen
            // IO size.
            let block_max = total_blocks - size + 1;
            let block_index = rng.gen_range(0..block_max) as usize;

            // Convert offset and length to their byte values.
            let offset =
                Block::new(block_index as u64, block_size.trailing_zeros());

            if op == 1 {
                // Write
                // Update the write count for all blocks we plan to write to.
                for i in 0..size {
                    write_count[block_index + i] += 1;
                }

                let vec = fill_vec(block_index, size, &write_count, block_size);
                let data = Bytes::from(vec);

                let waiter = guest.write(offset, data);
                waiterlist.push(waiter);
            } else {
                // Read

                let length: usize = size * block_size as usize;
                let vec: Vec<u8> = vec![255; length];
                let data = crucible::Buffer::from_vec(vec);
                let waiter = guest.read(offset, data.clone());
                waiterlist.push(waiter);
            }
        }
    }
    let mut wc = WQCounts {
        up_count: 0,
        ds_count: 0,
    };
    println!("loop over {} waiters", waiterlist.len());
    for wa in waiterlist.iter_mut() {
        wc = guest.show_work();
        wa.block_wait();
    }
    /*
     * Continue loping until all downstairs jobs finish also.
     */
    println!("All submitted jobs completed, waiting for downstairs");
    while wc.up_count + wc.ds_count > 0 {
        wc = guest.show_work();
        std::thread::sleep(std::time::Duration::from_secs(3));
    }
    println!("All downstairs jobs completed.");

    // TODO: Make a verify the whole volume function, to take the
    // write_count array and then read it all and compare.
    Ok(())
}
/*
 * This is a test workload that generates a single write spanning an extent
 * then will try to read the same.
 * TODO: Compare the buffer we wrote with the buffer we read.
 */
fn span_workload(guest: &Arc<Guest>) -> Result<()> {
    println!("Run span workload");
    let block_size = guest.query_block_size();
    let extent_size = guest.query_extent_size();
    println!("bs: {:?}  es:{}", extent_size, block_size);
    /*
     * Pick the last block in the first extent
     */
    let my_offset = block_size * (extent_size.value - 1);
    let my_length: usize = block_size as usize * 2;

    let mut vec: Vec<u8> = Vec::with_capacity(my_length);
    for seed in 7..9 {
        /*
         * Fill with a unique value for each block
         */
        for _ in 0..block_size {
            vec.push(seed);
        }
    }
    let data = Bytes::from(vec);

    println!("Sending a write spanning two extents");
    let mut waiter = guest.write_to_byte_offset(my_offset, data);
    waiter.block_wait();

    println!("Sending a flush");
    let mut waiter = guest.flush();
    waiter.block_wait();

    let vec: Vec<u8> = vec![99; my_length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Sending a read spanning two extents");
    waiter = guest.read_from_byte_offset(my_offset, data);
    waiter.block_wait();

    Ok(())
}

/*
 * Write, flush, then read every block in the volume.
 * We wait for each op to finish, so this is all sequential.
 */
fn big_workload(guest: &Arc<Guest>) -> Result<()> {
    let block_size = guest.query_block_size();
    let total_size = guest.query_total_size();
    let extent_size = guest.query_extent_size();

    let mut my_offset: u64 = 0;
    let mut cur_block = 0;
    let mut cur_extent = 0;
    while my_offset < total_size {
        /*
         * Generate a write buffer with a locally unique value.
         */
        let seed = (my_offset % 255) as u8;
        let mut vec: Vec<u8> = Vec::with_capacity(block_size as usize);
        for _ in 0..block_size {
            vec.push(seed);
        }
        let data = Bytes::from(vec);

        println!(
            "[{}][{}] send write  offset:{}  len:{}",
            cur_extent,
            cur_block,
            my_offset,
            data.len()
        );
        let mut waiter = guest.write_to_byte_offset(my_offset, data);
        waiter.block_wait();

        /*
         * Pre-populate the read buffer with a known pattern so we can
         * detect if is is not what we expect
         */
        let mut vec: Vec<u8> = Vec::with_capacity(block_size as usize);
        for i in 0..block_size {
            let seed = (i % 255) as u8;
            vec.push(seed);
        }
        let data = crucible::Buffer::from_vec(vec);

        println!(
            "[{}][{}] send read   offset:{}  len:{}",
            cur_extent,
            cur_block,
            my_offset,
            data.len(),
        );
        waiter = guest.read_from_byte_offset(my_offset, data);
        waiter.block_wait();

        my_offset += block_size;
        cur_block += 1;
        if cur_block >= extent_size.value {
            cur_extent += 1;
            cur_block = 0;
            println!("[{}][{}] send flush", cur_extent, cur_block);
            waiter = guest.flush();
            waiter.block_wait();
        }
    }
    println!("All IOs sent");
    std::thread::sleep(std::time::Duration::from_secs(5));
    guest.show_work();

    Ok(())
}

/*
 * A loop that generates a bunch of random reads and writes, increasing the
 * offset each operation.  After 20 are submitted, we wait for all to finish.
 * Use this test and pass the --lossy flag and upstairs will at random skip
 * sending jobs to the downstairs, creating dependencys that it will
 * eventually resolve.
 */
async fn dep_workload(guest: &Arc<Guest>) -> Result<()> {
    let block_size = guest.query_block_size();
    let total_size = guest.query_total_size();
    let final_offset = total_size - block_size;

    let mut my_offset: u64 = 0;
    for my_count in 1..15 {
        let mut waiterlist = Vec::new();

        /*
         * Generate some number of operations
         */
        for ioc in 0..20 {
            my_offset = (my_offset + block_size) % final_offset;
            if random() {
                /*
                 * Generate a write buffer with a locally unique value.
                 */
                let mut vec: Vec<u8> = Vec::with_capacity(block_size as usize);
                let seed = ((my_offset % 254) + 1) as u8;
                for _ in 0..block_size {
                    vec.push(seed);
                }
                let data = Bytes::from(vec);

                println!(
                    "Loop:{} send write {} @ offset:{}  len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );
                let waiter = guest.write_to_byte_offset(my_offset, data);
                waiterlist.push(waiter);
            } else {
                let vec: Vec<u8> = vec![0; block_size as usize];
                let data = crucible::Buffer::from_vec(vec);

                println!(
                    "Loop:{} send read  {} @ offset:{} len:{}",
                    my_count,
                    ioc,
                    my_offset,
                    data.len()
                );
                let waiter = guest.read_from_byte_offset(my_offset, data);
                waiterlist.push(waiter);
            }
        }

        guest.show_work();
        println!("Loop:{} send a final flush and wait", my_count);
        let mut flush_waiter = guest.flush();
        flush_waiter.block_wait();

        println!("Loop:{} loop over {} waiters", my_count, waiterlist.len());
        for wa in waiterlist.iter_mut() {
            wa.block_wait();
        }
        println!("Loop:{} all waiters done", my_count);
        guest.show_work();
    }

    println!("dep test done");
    Ok(())
}

async fn _run_scope(guest: Arc<Guest>) -> Result<()> {
    let scope =
        crucible_scope::Server::new(".scope.upstairs.sock", "upstairs").await?;
    let mut my_offset = 512 * 99;
    scope.wait_for("Send all the IOs").await;
    loop {
        let mut data = BytesMut::with_capacity(512 * 2);
        for seed in 44..46 {
            data.put(&[seed; 512][..]);
        }
        my_offset += 512 * 2;
        scope.wait_for("write 1").await;

        println!("send write 1");
        guest.write_to_byte_offset(my_offset, data.freeze());

        scope.wait_for("show work").await;
        guest.show_work();

        let mut read_offset = 512 * 99;
        const READ_SIZE: usize = 4096;
        for _ in 0..4 {
            let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

            println!("send a read");
            // scope.wait_for("send Read").await;
            guest.read_from_byte_offset(read_offset, data);
            read_offset += READ_SIZE as u64;
            // scope.wait_for("show work").await;
            guest.show_work();
        }

        // scope.wait_for("Flush step").await;
        println!("send flush");
        guest.flush();

        let mut data = BytesMut::with_capacity(512);
        data.put(&[0xbb; 512][..]);

        // scope.wait_for("write 2").await;
        println!("send write 2");
        guest.write_to_byte_offset(my_offset, data.freeze());
        my_offset += 512;
        // scope.wait_for("show work").await;
        guest.show_work();
        //scope.wait_for("at the bottom").await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_compare() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 1;

        let vec = fill_vec(0, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, 0, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_fail() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let vec = fill_vec(0, 1, &write_count, bs);
        write_count[0] = 1;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_fail_buf() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let mut vec = fill_vec(0, 1, &write_count, bs);
        vec[2] = 1;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_fail_block() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        write_count[0] = 2;

        let mut vec = fill_vec(0, 1, &write_count, bs);
        vec[0] = 3;
        assert_eq!(validate_vec(vec, 0, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_1() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] += 1;

        let vec = fill_vec(block_index, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_large() {
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let write_count = vec![1_u32; total_blocks];
        let vec = fill_vec(block_index, total_blocks, &write_count, bs);

        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_large_fail() {
        let bs: u64 = 512;
        let total_blocks = 100;
        let block_index = 0;
        /*
         * Simulate having written to all blocks
         */
        let write_count = vec![1_u32; total_blocks];
        let mut vec = fill_vec(block_index, total_blocks, &write_count, bs);
        let x = vec.len() - 1;
        vec[x] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_span() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let vec = fill_vec(block_index, 3, &write_count, bs);
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), true);
    }

    #[test]
    fn test_read_compare_span_fail() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let mut vec = fill_vec(block_index, 3, &write_count, bs);
        /*
         * Replace the first value in the second block
         */
        vec[(bs + 1) as usize] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_span_fail_2() {
        let bs: u64 = 512;
        let mut write_count = vec![0_u32; 10];
        let block_index = 1;
        write_count[block_index] = 1;
        write_count[block_index + 1] = 2;
        write_count[block_index + 2] = 3;

        let mut vec = fill_vec(block_index, 3, &write_count, bs);
        /*
         * Replace the second value in the second block
         */
        vec[(bs + 2) as usize] = 9;
        assert_eq!(validate_vec(vec, block_index, &write_count, bs), false);
    }

    #[test]
    fn test_read_compare_empty() {
        let bs: u64 = 512;
        let write_count = vec![0_u32; 10];

        let vec = fill_vec(0, 1, &write_count, bs);
        assert_eq!(validate_vec(vec, 0, &write_count, bs), true);
    }
}
