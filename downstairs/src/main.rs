use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    #[structopt(short, long, default_value = "9000")]
    port: u16,

    #[structopt(short, long, parse(from_os_str), name = "DIRECTORY")]
    data: PathBuf,
}

fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if !opt.data.is_dir() {
        bail!("--data {:?} must be a directory", opt.data);
    }

    Ok(opt)
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
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
     * Open the region for which we will be responsible.
     */
    let mut region = Region::open(&opt.data, Default::default())?;
    region.extend(10)?;

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
