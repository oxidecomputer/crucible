use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible::*;
use crucible_protocol::*;

use anyhow::{bail, Result};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

    #[structopt(short, long)]
    trace_endpoint: Option<String>,

    #[structopt(short, long, parse(from_os_str), name = "OUT_FILE")]
    export_path: Option<PathBuf>,

    #[structopt(short, long, default_value = "0", name = "SKIP")]
    skip: u64,

    /*
     * Number of blocks to export.
     */
    #[structopt(long, default_value = "0", name = "COUNT")]
    count: u64,

    /*
     * The next three opts apply during region creation
     */
    #[structopt(long, default_value = "512")]
    block_size: u64,

    #[structopt(long, default_value = "100")]
    extent_size: u64,

    #[structopt(long, default_value = "15")]
    extent_count: u64,
}

/*
 * Parse the command line options and do some sanity checking
 */
fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
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

    if opt.import_path.is_some() && !opt.create {
        bail!("Can't import without create option");
    }

    Ok(opt)
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

/*
 * Export the contents or partial contents of a Downstairs Region to
 * the file indicated.
 *
 * We will start from the provided start_block.
 * We will stop after "count" blocks are written to the export_path.
 */
fn downstairs_export<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    export_path: P,
    start_block: u64,
    mut count: u64,
) -> Result<()> {
    /*
     * Export an existing downstairs region to a file
     */
    let (block_size, extent_size, extent_count) = region.region_def();
    let space_per_extent = block_size * extent_size;
    assert!(block_size > 0);
    assert!(extent_size > 0);
    assert!(extent_count > 0);
    assert!(space_per_extent > 0);
    let file_size = block_size * extent_size * extent_count as u64;

    if count == 0 {
        count = extent_size * extent_count as u64;
    }

    println!(
        "Export total_size: {}  Extent size:{}  Total Extents:{}",
        file_size, space_per_extent, extent_count
    );
    println!(
        "Exporting from start_block: {}  count:{}",
        start_block, count
    );

    let mut data = BytesMut::with_capacity(block_size as usize);
    data.resize(block_size as usize, 0);

    let mut out_file = File::create(export_path)?;
    let mut blocks_copied = 0;

    //let count = extent_size * extent_count as u64;
    'eid_loop: for eid in 0..extent_count {
        let extent_offset = space_per_extent * eid as u64;
        for block_offset in 0..extent_size {
            if extent_offset + block_offset >= start_block {
                blocks_copied += 1;
                region
                    .region_read(eid as u64, block_offset, &mut data)
                    .unwrap();
                out_file.write_all(&data).unwrap();
                data.resize(block_size as usize, 0);

                if blocks_copied >= count {
                    break 'eid_loop;
                }
            }
        }
    }

    println!("Read and wrote out {} blocks", blocks_copied);

    Ok(())
}

/*
 * Import the contents of a file into a new Region.
 * The total size of the region will be rounded up to the next largest
 * extent multiple.
 */
fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    import_path: P,
) -> Result<()> {
    /*
     * We are only allowing the import on an empty Region, i.e.
     * one that has just been created.  If you want to overwrite
     * an existing region, write more code: TODO
     */
    assert!(region.def().extent_count() == 0);

    /*
     * Get some information about our file and the region defaults
     * and figure out how many extents we will need to import
     * this file.
     */
    let file_size = fs::metadata(&import_path)?.len();
    let (block_size, extent_size, _) = region.region_def();
    let space_per_extent = block_size * extent_size;

    let mut extents_needed = file_size / space_per_extent;
    if file_size % space_per_extent != 0 {
        extents_needed += 1;
    }
    println!(
        "Import file_size: {}  Extent size:{}  Total extents:{}",
        file_size, space_per_extent, extents_needed
    );

    /*
     * Create the number of extents the file will require
     */
    region.extend(extents_needed as u32)?;
    let rm = region.def();

    println!("Importing {:?} to new region", import_path);

    let mut buffer = vec![0; block_size as usize];
    buffer.resize(block_size as usize, 0);

    let mut fp = File::open(import_path)?;
    let mut offset: u64 = 0;
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
         * Use the same function upsairs uses to decide where to put the
         * data based on the LBA offset.
         */
        let nwo = extent_from_offset(rm, offset, block_size as usize).unwrap();
        assert_eq!(nwo.len(), 1);
        let (eid, block_offset, _) = nwo[0];

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

    /*
     * As there is no EOF indication in the downstairs, print the
     * number of total blocks we wrote to so the caller can, if they
     * want, use that to extract just this imported file.
     */
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
        Message::Write(rn, eid, _dependencies, block_offset, data) => {
            d.region.region_write(*eid, *block_offset, data)?;
            fw.send(Message::WriteAck(*rn)).await
        }
        Message::Flush(rn, dependencies, flush_number) => {
            d.region
                .region_flush(dependencies.to_vec(), *flush_number)?;
            fw.send(Message::FlushAck(*rn)).await
        }
        Message::ReadRequest(rn, eid, block_offset, blocks) => {
            /*
             * XXX Some thought will need to be given to where the read
             * data buffer is created, both on this side and the remote.
             * Also, we (I) need to figure out how to read data into an
             * uninitialized buffer. Until then, we have this workaround.
             */
            let (bs, _, _) = d.region.region_def();
            let sz = *blocks as usize * bs as usize;
            let mut data = BytesMut::with_capacity(sz);
            data.resize(sz, 1);

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

    if let Some(endpoint) = opt.trace_endpoint {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(endpoint) // usually port 6831
            .with_service_name("downstairs")
            .install_simple()
            .expect("Error initializing Jaeger exporter");

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(telemetry)
            .try_init()
            .expect("Error init tracing subscriber");
    }

    /*
     * Open or create the region for which we will be responsible.
     */
    let mut region;
    if opt.create {
        let mut region_options: crucible_common::RegionOptions = Default::default();
        region_options.set_block_size(opt.block_size);
        region_options.set_extent_size(opt.extent_size);

        region = Region::create(&opt.data, region_options)?;

        if let Some(ref import_path) = opt.import_path {
            downstairs_import(&mut region, import_path).unwrap();
            /*
             * The region we just created should now have a flush so the
             * new data and inital flush number is written to disk.
             */
            let dep = Vec::new();
            region.region_flush(dep, 1)?;
        } else {
            region.extend(opt.extent_count as u32)?;
        }
    } else {
        region = Region::open(&opt.data, Default::default())?;
    }

    if let Some(export_path) = opt.export_path {
        downstairs_export(&mut region, export_path, opt.skip, opt.count)
            .unwrap();
        return Ok(());
    }

    /*
     * If we imported, then stop now.  It's debatable if this is the typical
     * use case.  If we decide it is not, then we can remove this. XXX
     * The check is down here so we can import/export in the same command
     * if so desired.
     */
    if opt.create && opt.import_path.is_some() {
        println!("Exitng after import");
        return Ok(());
    }
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
