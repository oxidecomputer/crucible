// Copyright 2021 Oxide Computer Company
use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use rand::prelude::*;
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

    std::thread::sleep(std::time::Duration::from_secs(5));

    println!("Wait for a show_work command to finish before sending IO");
    guest.show_work();
    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside
     * (Propolis).  XXX Test code here..
     * runtime.spawn(run_scope(prop_work));
     */

    /*
     * Figure out the workload option passed to us on the command line.
     */
    match opt.workload.as_str() {
        "one" => {
            println!("One test");
            single_workload(&guest)?;
        }
        "big" => {
            println!("Run big test");
            big_workload(&guest)?;
        }
        "dep" => {
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
            runtime.block_on(dep_workload(guest))?;
            return Ok(());
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
fn single_workload(guest: &Arc<Guest>) -> Result<()> {
    println!("Run single workload");
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
        let mut waiter = guest.write(my_offset, data);
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
async fn dep_workload(guest: Arc<Guest>) -> Result<()> {
    let block_size = guest.query_block_size();
    let total_size = guest.query_total_size();
    let final_offset = total_size - block_size;

    let mut my_offset: u64 = 0;
    for my_count in 1..15 {
        let mut waiterlist = Vec::new();

        /*
         * Generate some numbero of operations
         */
        for ioc in 0..20 {
            my_offset += block_size % final_offset;
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
                let waiter = guest.write(my_offset, data);
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
                let waiter = guest.read(my_offset, data);
                waiterlist.push(waiter);
            }
        }

        println!("Loop:{} send a final flush and wait", my_count);
        let mut flush_waiter = guest.flush();
        flush_waiter.block_wait();

        println!("Loop:{} loop over {} waiters", my_count, waiterlist.len());
        for wa in waiterlist.iter_mut() {
            wa.block_wait();
        }
        println!("Loop:{} all waiters done", my_count);
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
