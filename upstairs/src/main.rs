// Copyright 2021 Oxide Computer Company
use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{BufMut, BytesMut};
use structopt::StructOpt;
use tokio::runtime::Builder;

use crucible::*;

/*
 * A simple example of using the crucible lib
 */
#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
pub struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,

    #[structopt(short, long)]
    key: Option<String>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

impl Opt {
    /*
     * Use:
     *
     *     let opt = Opt::from_string("-- -t 192.168.1.1:3801 -t 192.168.1.2:3801".to_string()).unwrap();
     *
     */
    pub fn from_string(args: String) -> Result<Opt> {
        let opt: Opt = Opt::from_iter(args.split(' '));

        if opt.target.is_empty() {
            bail!("must specify at least one --target");
        }

        Ok(opt)
    }
}

#[cfg(test)]
mod tests {
    use crate::Opt;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_opt_from_string() {
        let opt = Opt::from_string(
            "-- -t 192.168.1.1:3801 -t 192.168.1.2:3801".to_string(),
        )
        .unwrap();
        assert_eq!(opt.target.is_empty(), false);
        assert_eq!(opt.target.len(), 2);

        assert_eq!(
            opt.target[0],
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 3801)
        );
        assert_eq!(
            opt.target[1],
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 2), 3801)
        );
    }

    #[test]
    fn test_key() {
        let key_bytes =
            base64::decode("9YGqFSwBHCX/IbjstbI1WuUPKOfwrwNAJSFzUN2w4iU=")
                .unwrap();

        let opt = Opt::from_string(
            "-- -t 192.168.1.1:3801 -k 9YGqFSwBHCX/IbjstbI1WuUPKOfwrwNAJSFzUN2w4iU=".to_string(),
        )
        .unwrap();

        if let Some(key) = &opt.key {
            assert_eq!(
                base64::decode(key).expect("base64 decode failed"),
                key_bytes
            );
        } else {
            panic!("bad news");
        }

        let crucible_opts = crucible::CrucibleOpts {
            target: opt.target,
            lossy: false,
            key: opt.key,
        };

        if let Some(key) = crucible_opts.key_bytes() {
            assert_eq!(key_bytes, key);
        } else {
            panic!("bad news");
        }
    }
}

fn main() -> Result<()> {
    let opt = opts()?;
    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: false,
        key: opt.key,
    };

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
    runtime.spawn(up_main(crucible_opts, guest.clone()));
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

    println!("send a write");
    guest.write_to_byte_offset(my_offset, data.freeze());

    println!("send a flush");
    guest.flush();

    let read_offset = my_offset;
    const READ_SIZE: usize = 1024;
    let data = crucible::Buffer::from_slice(&[0x99; READ_SIZE]);

    println!("send a read");
    guest.read_from_byte_offset(read_offset, data);

    Ok(())
}
