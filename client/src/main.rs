use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use structopt::StructOpt;
use tokio::runtime::Builder;

use crucible::*;

#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
pub struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,

    #[structopt(short, long, default_value = "one")]
    workload: String,
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
    let opt = opts()?;
    let crucible_opts = CrucibleOpts { target: opt.target };

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

    std::thread::sleep(std::time::Duration::from_secs(5));

    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside (Propolis).
     */
    //runtime.spawn(run_scope(prop_work));
    match opt.workload.as_str() {
        "one" => {
            println!("One test");
            run_single_workload(&guest)?;
        }
        "big" => {
            println!("Run big test");
            run_big_workload(&guest)?;
        }
        _ => {
            println!("Unknown workload type {}", opt.workload);
            return Ok(());
        }
    }

    println!("Tests done");
    loop {
        guest.show_work();
        std::thread::sleep(std::time::Duration::from_secs(30));
        println!("\n\n   Loop send flush");
        guest.flush();
    }
}

/*
 * This is a test workload that generates a single write spanning an extent
 * then will try to read the same.
 * TODO: Compare the buffer we wrote with the buffer we read.
 */
fn run_single_workload(guest: &Arc<Guest>) -> Result<()> {
    let block_size = guest.query_block_size();
    let extent_size = guest.query_extent_size();
    println!("bs: {}  es:{}", extent_size, block_size);
    /*
     * Pick the last block in the first extent
     */
    let my_offset = (block_size * extent_size) - block_size;
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
    let mut waiter = guest.write(my_offset, data);
    waiter.block_wait();

    println!("Sending a flush");
    let mut waiter = guest.flush();
    waiter.block_wait();

    let vec: Vec<u8> = vec![99; my_length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Sending a read spanning two extents");
    waiter = guest.read(my_offset, data);
    waiter.block_wait();

    Ok(())
}

/*
 * Write, flush, then read every block in the volume.
 * We wait for each op to finish, so this is all sequential.
 */
fn run_big_workload(guest: &Arc<Guest>) -> Result<()> {
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
        let mut waiter = guest.write(my_offset, data);
        waiter.block_wait();

        println!("[{}][{}] send flush", cur_extent, cur_block);
        waiter = guest.flush();
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
        waiter = guest.read(my_offset, data);
        waiter.block_wait();

        my_offset += block_size;
        cur_block += 1;
        if cur_block >= extent_size {
            cur_extent += 1;
            cur_block = 0;
        }
    }
    println!("All IOs sent");
    std::thread::sleep(std::time::Duration::from_secs(5));
    guest.show_work();

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
        guest.write(my_offset, data.freeze());

        scope.wait_for("show work").await;
        guest.show_work();

        let mut read_offset = 512 * 99;
        const READ_SIZE: usize = 4096;
        for _ in 0..4 {
            let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

            println!("send a read");
            // scope.wait_for("send Read").await;
            guest.read(read_offset, data);
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
        guest.write(my_offset, data.freeze());
        my_offset += 512;
        // scope.wait_for("show work").await;
        guest.show_work();
        //scope.wait_for("at the bottom").await;
    }
}
