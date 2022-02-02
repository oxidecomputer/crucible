use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use anyhow::Result;
use rand::Rng;

use crucible::*;

fn test_multiple_sub_volumes_basic_write_read() -> Result<()> {
    // Start two downstairs, both 16k, with different encryption keys, and
    // write out random data, then read back
    let opts_1 = CrucibleOpts {
        target: vec![
            "127.0.0.1:44101".parse()?,
            "127.0.0.1:44102".parse()?,
            "127.0.0.1:44103".parse()?,
        ],
        lossy: false,
        key: Some("e14dkfJpdOzcvtXGT8DrDycQuxZ2NKWd79S/JzQyW3s=".into()),
        ..Default::default()
    };

    let opts_2 = CrucibleOpts {
        target: vec![
            "127.0.0.1:44104".parse()?,
            "127.0.0.1:44105".parse()?,
            "127.0.0.1:44106".parse()?,
        ],
        lossy: false,
        key: Some("JocLYqEI5t/dTkIUhSLn2Me+RxVXQDK1ufZTUN2271I=".into()),
        ..Default::default()
    };

    let mut volume = Volume::new(4096);

    volume.add_subvolume_create_guest(opts_1, 0)?;
    volume.add_subvolume_create_guest(opts_2, 0)?;

    volume.activate(1)?;

    assert_eq!(volume.total_size()?, 32768);

    let mut rng = rand::thread_rng();

    let data: Vec<u8> = (0..32768)
        .map(|_| rng.sample(rand::distributions::Standard))
        .collect();

    // println!("wrote: {:?}", hex::encode(&data));

    let mut waiter =
        volume.write(Block::new_4096(0), Bytes::from(data.clone()))?;
    waiter.block_wait()?;

    let read = Buffer::new(32768);

    let mut waiter = volume.read(Block::new_4096(0), read.clone())?;
    waiter.block_wait()?;

    // println!("read: {:?}", hex::encode(&*read.as_vec()));

    let read_vec = read.as_vec();

    assert_eq!(read_vec.len(), data.len());

    // std::fs::write("data", &data)?;
    // std::fs::write("read_vec", &*read_vec)?;

    if *read_vec != data {
        for i in 0..data.len() {
            if read_vec[i] != data[i] {
                println!("bad offset {}", i);
                panic!("bad read");
            }
        }
    }

    Ok(())
}

fn test_multiple_sub_volumes_hammer() -> Result<()> {
    let opts_1 = CrucibleOpts {
        target: vec![
            "127.0.0.1:44101".parse()?,
            "127.0.0.1:44102".parse()?,
            "127.0.0.1:44103".parse()?,
        ],
        lossy: false,
        key: Some("e14dkfJpdOzcvtXGT8DrDycQuxZ2NKWd79S/JzQyW3s=".into()),
        ..Default::default()
    };

    let opts_2 = CrucibleOpts {
        target: vec![
            "127.0.0.1:44104".parse()?,
            "127.0.0.1:44105".parse()?,
            "127.0.0.1:44106".parse()?,
        ],
        lossy: false,
        key: Some("JocLYqEI5t/dTkIUhSLn2Me+RxVXQDK1ufZTUN2271I=".into()),
        ..Default::default()
    };

    let mut volume = Volume::new(4096);

    volume.add_subvolume_create_guest(opts_1, 0)?;
    volume.add_subvolume_create_guest(opts_2, 0)?;

    let mut cpf = CruciblePseudoFile::from(Arc::new(volume))?;
    let rounds = 1000;

    cpf.activate(1)?;

    let mut rng = rand::thread_rng();

    for _ in 0..rounds {
        let sz = cpf.sz();

        let mut offset: u64 = rng.gen::<u64>() % sz;
        let mut bsz: usize = rng.gen::<usize>() % 4096;

        while ((offset + bsz as u64) > sz) || (bsz == 0) {
            offset = rng.gen::<u64>() % sz;
            bsz = rng.gen::<usize>() % 4096;
        }

        let vec: Vec<u8> = (0..bsz)
            .map(|_| rng.sample(rand::distributions::Standard))
            .collect();

        let mut vec2 = vec![0; bsz];

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.write_all(&vec[..])?;

        cpf.seek(SeekFrom::Start(offset))?;
        cpf.read_exact(&mut vec2[..])?;

        assert_eq!(vec, vec2);
    }

    cpf.flush()?;

    println!("Done ok, waiting on show_work");

    loop {
        let wc = cpf.show_work()?;
        println!("Up:{} ds:{}", wc.up_count, wc.ds_count);
        if wc.up_count + wc.ds_count == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    test_multiple_sub_volumes_basic_write_read()?;
    test_multiple_sub_volumes_hammer()?;

    Ok(())
}
