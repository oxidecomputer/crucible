use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::runtime::Builder;

use crucible::*;

fn main() -> Result<()> {
    let opt = opts()?;

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
     * the run_scope() function to submit test work.
     */
    let guest = Arc::new(Guest::new());

    runtime.spawn(up_main(opt, guest.clone()));
    println!("runtime is spawned");

    /*
     * The rest of this is just test code
     */
    std::thread::sleep(std::time::Duration::from_secs(1));
    run_single_workload(&guest)?;
    println!("Tests done, wait");
    std::thread::sleep(std::time::Duration::from_secs(5));
    println!("all Tests done");
    Ok(())
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
    println!("send a write");
    guest.send(wio);

    println!("send a flush");
    guest.send(BlockOp::Flush);

    let read_offset = my_offset;
    const READ_SIZE: usize = 1024;
    let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

    println!("send a read");
    let rio = BlockOp::Read {
        offset: read_offset,
        data,
    };
    guest.send(rio);

    Ok(())
}
