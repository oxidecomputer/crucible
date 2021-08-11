#![feature(with_options)]

use std::net::SocketAddrV4;
use std::sync::Arc;

use anyhow::{bail, Result};
use structopt::StructOpt;
use tokio::runtime::Builder;

use crucible::*;

use std::io::{Read, Seek, SeekFrom, Write};

// https://stackoverflow.com/questions/29504514/whats-the-best-way-to-compare-2-vectors-or-strings-element-by-element
fn do_vecs_match<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
pub struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,

    /*
     * Verify that writes don't extend before or after the actual location.
     */
    #[structopt(short, long)]
    verify_isolation: bool,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

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

    let bs = guest.query_block_size() as u64;
    let sz = guest.query_total_size() as u64;
    println!("advertised size as {} bytes ({} byte blocks)", sz, bs);

    let mut cpf = crucible::CruciblePseudoFile::from_guest(guest);

    use rand::Rng;
    let mut rng = rand::thread_rng();

    if opt.verify_isolation {
        println!("clearing...");
        cpf.seek(SeekFrom::Start(0))?;
        for _ in 0..(sz / bs) {
            cpf.write_all(&vec![0; bs as usize])?;
        }
    }

    loop {
        let mut offset: u64 = rng.gen::<u64>() % sz;
        let mut bsz: usize = rng.gen::<usize>() % 4096;

        while ((offset + bsz as u64) > sz) || (bsz == 0) {
            offset = rng.gen::<u64>() % sz;
            bsz = rng.gen::<usize>() % 4096;
        }

        println!("testing: offset {} sz {}", offset, bsz);

        let vec: Vec<u8> = (0..bsz)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect();

        let mut vec2 = vec![0; bsz];

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.write_all(&vec[..])?;

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.read_exact(&mut vec2[..])?;

        if !do_vecs_match(&vec, &vec2) {
            println!("offset {} bsz {}", offset, bsz);
            println!("vec : {:?}", vec);
            println!("vec2: {:?}", vec2);

            assert_eq!(vec.len(), vec2.len());

            for i in 0..vec.len() {
                if vec[i] != vec2[i] {
                    println!("vec offset {}: {} != {}", i, vec[i], vec2[i]);
                } else {
                    println!("vec offset {} ok", i);
                }
            }

            bail!("vec != vec2");
        }

        if opt.verify_isolation {
            // Read back every byte not written to to make sure it's zero
            cpf.seek(SeekFrom::Start(0))?;

            // - read from 0 -> offset
            let mut verify_vec: Vec<u8> = vec![0; offset as usize];
            cpf.read_exact(&mut verify_vec[..])?;

            for i in 0..offset {
                if verify_vec[i as usize] != 0 {
                    bail!("Not isolated! non-zero byte at {}", i);
                }
            }

            // - read from (offset + bsz) -> sz
            cpf.seek(SeekFrom::Start(offset + bsz as u64))?;

            let len = sz - (offset + bsz as u64);
            let mut verify_vec: Vec<u8> = vec![0; len as usize];
            cpf.read_exact(&mut verify_vec[..])?;

            for i in 0..len {
                if verify_vec[i as usize] != 0 {
                    bail!(
                        "Not isolated! non-zero byte at {}",
                        (offset + bsz as u64) + i
                    );
                }
            }

            // Once done, zero out the write
            cpf.seek(SeekFrom::Start(offset))?;
            cpf.write_all(&vec![0; bsz])?;
        }
    }
}
