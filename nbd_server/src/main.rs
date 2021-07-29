use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::runtime::Builder;

use crucible::*;
use crucible_common::{RegionDefinition, RegionOptions};

use nbd::server::{handshake, transmission, Export};
use std::io::Result as IOResult;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream as NetTcpStream};

/*
 * NBD server commands translate through the CruciblePseudoFile and turn
 * into Guest work ops.
 */

struct CruciblePseudoFile {
    guest: Arc<Guest>,
    block_size: usize,
    offset: u64,
    sz: u64,
}

impl CruciblePseudoFile {
    fn _read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let data = crucible::Buffer::from_slice(buf);

        let rio = BlockOp::Read {
            offset: self.offset,
            data: data.clone(),
        };

        let mut waiter = self.guest.send(rio);
        waiter.block_wait();

        // TODO: for block devices, we can't increment offset past the
        // device size but we're supposed to be pretending to be a proper
        // file here
        self.offset += buf.len() as u64;

        // TODO: is there a better way to do this fill?
        {
            let vec = data.as_vec();
            for i in 0..buf.len() {
                buf[i] = vec[i];
            }
        }

        Ok(buf.len())
    }

    fn _write(&mut self, buf: &[u8]) -> IOResult<usize> {
        let mut data = BytesMut::with_capacity(buf.len());
        data.put_slice(buf);

        let wio = BlockOp::Write {
            offset: self.offset,
            data: data.freeze(),
        };

        let mut waiter = self.guest.send(wio);
        waiter.block_wait();

        // TODO: can't increment offset past the device size
        self.offset += buf.len() as u64;

        Ok(buf.len())
    }
}

/*
 * The Read + Write impls here translate arbitrary sized operations into
 * sector size calls for the underlying Crucible API.
 */
impl Read for CruciblePseudoFile {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        assert!((buf.len() % 512) == 0);

        let mut result: usize = 0;

        for i in (0..buf.len()).step_by(512) {
            result += self._read(&mut buf[i..(i + self.block_size)])?;
        }

        Ok(result)
    }
}

impl Write for CruciblePseudoFile {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        assert!((buf.len() % 512) == 0);

        let mut result: usize = 0;

        for i in (0..buf.len()).step_by(512) {
            result += self._write(&buf[i..(i + self.block_size)])?;
        }

        Ok(result)
    }

    fn flush(&mut self) -> IOResult<()> {
        let mut waiter = self.guest.send(BlockOp::Flush);
        waiter.block_wait();

        Ok(())
    }
}

impl Seek for CruciblePseudoFile {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        // TODO: does not check against block device size
        let mut offset: i64 = self.offset as i64;
        match pos {
            SeekFrom::Start(v) => {
                offset = v as i64;
            }
            SeekFrom::Current(v) => {
                // TODO: as checked add?
                offset += v;
            }
            SeekFrom::End(v) => {
                // TODO: as checked add?
                offset += v;
            }
        }

        if offset < 0 {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "offset is negative!"))
        } else {
            // offset >= 0
            self.offset = offset as u64;
            Ok(self.offset)
        }
    }

    fn stream_position(&mut self) -> IOResult<u64> {
        self.seek(SeekFrom::Current(0))
    }
}

fn handle_nbd_client(
    cpf: &mut CruciblePseudoFile,
    mut stream: NetTcpStream,
) -> Result<()> {
    let e = Export {
        size: cpf.sz,
        readonly: false,
        ..Default::default()
    };
    handshake(&mut stream, &e)?;
    transmission(&mut stream, cpf)?;
    Ok(())
}

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

    // TODO: read this from somewhere, instead of defaults
    let mut region =
        RegionDefinition::from_options(&RegionOptions::default()).unwrap();
    region.set_extent_count(10);

    let sz = region.block_size()
        * region.extent_size()
        * (region.extent_count() as u64);
    println!("NBD advertised size as {} bytes", sz);

    // NBD server
    let listener = TcpListener::bind("127.0.0.1:10809").unwrap();
    let mut cpf = CruciblePseudoFile {
        guest,
        block_size: region.block_size() as usize,
        offset: 0,
        sz, // sent to NBD client during handshake through Export struct
    };

    for stream in listener.incoming() {
        println!("waiting on nbd traffic");
        match stream {
            Ok(stream) => match handle_nbd_client(&mut cpf, stream) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("handle_nbd_client error: {}", e);
                }
            },
            Err(_) => {
                println!("Error");
            }
        }
    }

    Ok(())
}
