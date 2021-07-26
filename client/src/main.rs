use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::runtime::Builder;

use crucible::*;

/**
 * This is an example Crucible client.
 * Here we make use of the interfaces that Crucible exposes.
 */
fn main() -> Result<()> {
    let opt = opts()?;

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

    runtime.spawn(up_main(opt, guest.clone()));
    println!("Crucible runtime is spawned");

    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside (Propolis).
     */
    //runtime.spawn(run_scope(prop_work));

    /*
     * XXX The rest of this is just test code
     */
    std::thread::sleep(std::time::Duration::from_secs(5));
    //_run_big_workload(&guest, 2)?;
    for _ in 0..1000 {
        run_single_workload(&guest)?;
        /*
         * This helps us get around async/non-async issues.
         * Keeing this process busy means some async tasks will never get
         * time to run.  Give a little pause here and let some other
         * tasks go.  Yes, this is a hack.  XXX
         */
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    // show_guest_work(&guest);
    println!("Tests done, wait");
    std::thread::sleep(std::time::Duration::from_secs(5));
    // show_guest_work(&guest);
    println!("Tests done");
    std::thread::sleep(std::time::Duration::from_secs(10));
    println!("all Tests done");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}

/*
 * This is a test workload that generates a write spanning an extent
 * then trys to read the same.
 */
fn run_single_workload(guest: &Arc<Guest>) -> Result<()> {
    let my_offset = 512 * 99;
    let mut data = BytesMut::with_capacity(512 * 2);
    for seed in 4..6 {
        data.put(&[seed; 512][..]);
    }
    let data = data.freeze();
    let wio = BlockOp::Write {
        offset: my_offset,
        data,
    };
    guest.send(wio);

    guest.send(BlockOp::Flush);
    //guest.send(BlockOp::ShowWork);

    let read_offset = my_offset;
    const READ_SIZE: usize = 1024;
    println!("generate a read 1");
    let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

    println!("send read");
    let rio = BlockOp::Read {
        offset: read_offset,
        data,
    };
    guest.send(rio);
    // guest.send(BlockOp::ShowWork);

    println!("Final offset: {}", my_offset);

    Ok(())
}
/*
 * This is basically just a test loop that generates a workload then sends the
 * workload to Crucible.
 */
fn _run_big_workload(guest: &Arc<Guest>, loops: u32) -> Result<()> {
    for _ll in 0..loops {
        let mut my_offset: u64 = 0;
        for olc in 0..10 {
            for lc in 0..100 {
                let seed = (my_offset % 255) as u8;
                let mut data = BytesMut::with_capacity(512);
                data.put(&[seed; 512][..]);
                let data = data.freeze();
                let wio = BlockOp::Write {
                    offset: my_offset,
                    data,
                };
                println!("[{}][{}] send write  offset:{}", olc, lc, my_offset);
                guest.send(wio);

                let read_offset = my_offset;
                const READ_SIZE: usize = 512;
                let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);
                println!(
                    "[{}][{}] send read   offset:{}",
                    olc, lc, read_offset,
                );
                let rio = BlockOp::Read {
                    offset: read_offset,
                    data,
                };
                guest.send(rio);

                println!("[{}][{}] send flush", olc, lc);
                guest.send(BlockOp::Flush);
                // guest.send(BlockOp::ShowWork);
                my_offset += 512;
            }
        }
        println!("Final offset: {}", my_offset);
    }
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
        let data = data.freeze();
        let wio = BlockOp::Write {
            offset: my_offset,
            data,
        };
        my_offset += 512 * 2;
        scope.wait_for("write 1").await;
        println!("send write 1");
        guest.send(wio);
        scope.wait_for("show work").await;
        guest.send(BlockOp::ShowWork);

        let mut read_offset = 512 * 99;
        const READ_SIZE: usize = 4096;
        for _ in 0..4 {
            let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

            println!("send a read");
            let rio = BlockOp::Read {
                offset: read_offset,
                data,
            };
            // scope.wait_for("send Read").await;
            guest.send(rio);
            read_offset += READ_SIZE as u64;
            // scope.wait_for("show work").await;
            guest.send(BlockOp::ShowWork);
        }

        // scope.wait_for("Flush step").await;
        println!("send flush");
        guest.send(BlockOp::Flush);

        let mut data = BytesMut::with_capacity(512);
        data.put(&[0xbb; 512][..]);
        let data = data.freeze();
        let wio = BlockOp::Write {
            offset: my_offset,
            data,
        };
        // scope.wait_for("write 2").await;
        println!("send write 2");
        guest.send(wio);
        my_offset += 512;
        // scope.wait_for("show work").await;
        guest.send(BlockOp::ShowWork);
        //scope.wait_for("at the bottom").await;
    }
}
