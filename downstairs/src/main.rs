use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible::*;
use crucible_protocol::*;

use anyhow::{bail, Result};
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

mod region;
use region::Region;

#[derive(Debug, StructOpt)]
#[structopt(about = "disk-side storage component")]
pub struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    #[structopt(short, long, default_value = "9000")]
    port: u16,

    #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
    data: PathBuf,

    #[structopt(short, long = "create")]
    create: bool,

    #[structopt(short, long, parse(from_os_str), name = "FILE")]
    import_path: Option<PathBuf>,
}

/*
 * Parse the command line options and do some sanity checking
 */
fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    /*
     * Make sure we don't clobber an existing region, if we are creating
     * a new one, then it should not exist.
     * In addition, if we are just opening an existing region, then
     * expect to find the files where the should exist and return error
     * if they do not exist.
     */
    if opt.create {
        if opt.data.is_dir() {
            bail!("Directory {:?} already exists, Cannot create", opt.data);
        }
    } else if !opt.data.is_dir() {
        bail!("--data {:?} must exist as a directory", opt.data);
    }

    Ok(opt)
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

fn _downstairs_import(d: &Arc<Downstairs>) -> Result<()> {
    println!("Import to downstairs");

    let (bs, es, _) = d.region.region_def();
    let size_per_extent = bs * es;

    println!("Size per extent is: {}", bs * es);

    let path = "./foo.txt".to_string();
    let mut file = OpenOptions::new().read(true).open(&path)?;

    // let mut data = BytesMut::with_capacity(size_per_extent as usize);
    let mut data = BytesMut::with_capacity(size_per_extent as usize);

    file.read_exact(&mut data).unwrap();
    println!("Got data: {:#?} {}", data, data.len());

    Ok(())
}

fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    import_path: P,
) -> Result<()> {
    /*
     * Import a file in, extending the appropriate number of extents first.
     */
    let file_size = fs::metadata(&import_path)?.len();
    let (block_size, extent_size, _) = region.region_def();

    // TODO: use a ceiling divide for u32 instead?
    let mut extents = (file_size / block_size) / extent_size;
    while (extents * block_size * extent_size) < file_size {
        extents += 1;
    }

    let spe = block_size * extent_size;
    let mut extents_needed = file_size / spe;
    if file_size % spe != 0 {
        extents_needed += 1;
    }
    println!(
        "file_size: {}  spe:{}  e_need:{}",
        file_size, spe, extents_needed
    );
    assert_eq!(extents, extents_needed);

    region.extend(extents as u32)?;

    println!(
        "Importing {:?} to region with {} extents",
        import_path, extents_needed
    );

    let space_per_extent = block_size * extent_size;
    //let mut buffer = Vec::<u8>::with_capacity(block_size as usize);
    let mut buffer = vec![0; block_size as usize];
    buffer.resize(block_size as usize, 0);

    let mut fp = File::open(import_path)?;
    let mut offset: u64 = 0;

    let rm = region.def();
    let mut blocks_copied = 0;
    while let Ok(n) = fp.read(&mut buffer[..]) {
        if n == 0 {
            /*
             * If we read 0 without error, then we are done
             */
            break;
        }
        blocks_copied += 1;
        /*
         * Once it works, switch to nwo way
         */
        let eid = offset / space_per_extent;
        let block_offset = (offset % space_per_extent) / block_size;
        let nwo = extent_from_offset(rm, offset, block_size as usize).unwrap();

        assert_eq!(nwo.len(), 1);
        assert_eq!(nwo[0].0, eid);
        assert_eq!(nwo[0].1, block_offset);

        if n != (block_size as usize) {
            if n != 0 {
                let rest = &buffer[0..n];
                region.region_write(eid, block_offset, rest)?;
            }
            break;
        } else {
            region.region_write(eid, block_offset, &buffer)?;
            offset += n as u64;
        }
    }
    println!(
        "Created {} extents and Copied {} blocks",
        extents_needed, blocks_copied
    );

    Ok(())
}

/*
 * A new IO request has been received.
 *
 * For writes and flushes, we put them on the work queue.
 */
async fn proc_frame(
    d: &Arc<Downstairs>,
    m: &Message,
    fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
) -> Result<()> {
    match m {
        Message::Ruok => fw.send(Message::Imok).await,
        Message::ExtentVersionsPlease => {
            let (bs, es, ec) = d.region.region_def();
            fw.send(Message::ExtentVersions(bs, es, ec, d.region.versions()))
                .await
        }
        Message::Write(rn, eid, dependencies, block_offset, data) => {
            println!(
                "Write       rn:{} eid:{:?} dep:{:?} bo:{:?}",
                rn, eid, dependencies, block_offset
            );

            d.region.region_write(*eid, *block_offset, data)?;
            fw.send(Message::WriteAck(*rn)).await
        }
        Message::Flush(rn, dependencies, flush_number) => {
            println!(
                "flush       rn:{} dep:{:?} fln:{:?}",
                rn, dependencies, flush_number
            );
            d.region
                .region_flush(dependencies.to_vec(), *flush_number)?;
            fw.send(Message::FlushAck(*rn)).await
        }
        Message::ReadRequest(rn, eid, block_offset, blocks) => {
            println!(
                "Read        rn:{} eid:{:?} bo:{:?}",
                rn, eid, block_offset
            );
            /*
             * XXX Some thought will need to be given to where the read
             * data buffer is created, both on this side and the remote.
             * Also, we (I) need to figure out how to read data into an
             * uninitialized buffer.  Until then, we have this workaround.
             *
             * Also, the 512's here should be block_size  XXX for that.
             */
            let mut data = BytesMut::with_capacity(*blocks as usize * 512);
            for _ in 0..*blocks {
                data.put(&[1; 512][..]);
            }
            d.region.region_read(*eid, *block_offset, &mut data)?;
            let data = data.freeze();
            fw.send(Message::ReadResponse(*rn, data.clone())).await
        }
        x => bail!("unexpected frame {:?}", x),
    }
}

async fn proc(d: &Arc<Downstairs>, mut sock: TcpStream) -> Result<()> {
    let (read, write) = sock.split();
    let mut fr = FramedRead::new(read, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(write, CrucibleEncoder::new());

    /*
     * Don't wait more than 5 seconds to hear from the other side.
     * XXX Timeouts, timeouts: always wrong!  Some too short and some too long.
     */
    let mut deadline = deadline_secs(50);
    let mut negotiated = false;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => {
                if !negotiated {
                    bail!("did not negotiate a protocol");
                } else {
                    bail!("inactivity timeout");
                }
            }
            new_read = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                match new_read.transpose()? {
                    None => return Ok(()),
                    Some(Message::HereIAm(version)) => {
                        if negotiated {
                            bail!("negotiated already!");
                        }
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = true;
                        fw.send(Message::YesItsMe(1)).await?;
                    }
                    Some(msg) => {
                        if !negotiated {
                            bail!("expected HereIAm first");
                        }

                        proc_frame(d, &msg, &mut fw).await?;
                        deadline = deadline_secs(50);
                    }
                }
            }
        }
    }
}

struct Downstairs {
    region: Region,
    // completed ringbuf?
    // in_progress ringbuff.
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;

    /*
     * Open or create the region for which we will be responsible.
     */
    let mut region;
    if opt.create {
        println!("Create new region directory");
        region = Region::create(&opt.data, Default::default())?;
        if let Some(import_path) = opt.import_path {
            downstairs_import(&mut region, import_path).unwrap();
            let dep = Vec::new();
            region.region_flush(dep, 1)?;
        } else {
            region.extend(10)?;
        }
    } else {
        println!("Open existing region directory");
        region = Region::open(&opt.data, Default::default())?;
    }

    println!("Startup Extent values: {:?}", region.versions());
    let d = Arc::new(Downstairs { region });

    /*
     * Establish a listen server on the port.
     */
    let listen_on = SocketAddrV4::new(opt.address, opt.port);
    let listener = TcpListener::bind(&listen_on).await?;

    /*
     * We now loop listening for a connection from the Upstairs.
     * When we get one, we then call the proc() function to handle
     * it and wait on that function to finish.  If it does, we loop
     * and wait for another connection.
     *
     * XXX We may want to consider taking special action when an upstairs
     * has gone away, like perhaps flushing all outstanding writes?
     */
    println!("listening on {}", listen_on);
    let mut connections: u64 = 1;
    loop {
        let (sock, raddr) = listener.accept().await?;
        println!(
            "connection from {:?}  connections count:{}",
            raddr, connections
        );

        if let Err(e) = proc(&d, sock).await {
            println!("ERROR: connection({}): {:?}", connections, e);
        } else {
            println!("OK: connection({}): all done", connections);
        }
        connections += 1;
    }
}
