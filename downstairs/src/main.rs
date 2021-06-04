use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crucible_protocol::*;

use anyhow::{bail, Result};
use futures::{SinkExt, StreamExt};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

mod disk;
use disk::Disk;

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

async fn proc_frame(
    _id: u64,
    d: &Arc<Downstairs>,
    m: &Message,
    fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
) -> Result<()> {
    match m {
        Message::Ruok => fw.send(Message::Imok).await,
        Message::ExtentVersionsPlease => {
            let (bs, es, ec) = d.disk.disk_def();
            fw.send(Message::ExtentVersions(bs, es, ec, d.disk.versions()))
                .await
        }
        Message::Write(rn, block_offset, data) => {
            d.disk.disk_write(*block_offset, data)?;
            fw.send(Message::WriteAck(*rn)).await
        }
        Message::Flush(rn, dependencies, flush) => {
            println!(
                "Received flush rn:{} dep:{:?} fl:{:?}",
                rn, dependencies, flush
            );
            //let dep = Vec::new();
            d.disk.disk_flush(dependencies.to_vec(), flush.to_vec())?;
            fw.send(Message::FlushAck(*rn)).await
        }
        x => bail!("unexpected frame {:?}", x),
    }
}

async fn proc(id: u64, d: &Arc<Downstairs>, mut sock: TcpStream) -> Result<()> {
    let (r, w) = sock.split();
    let mut fr = FramedRead::new(r, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(w, CrucibleEncoder::new());

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
            f = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                match f.transpose()? {
                    None => return Ok(()),
                    Some(Message::HereIAm(version)) => {
                        if negotiated {
                            bail!("negotiated already!");
                        }
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = true;
                        println!("{}: version {}", id, version);
                        fw.send(Message::YesItsMe(1)).await?;
                    }
                    Some(m) => {
                        if !negotiated {
                            bail!("expected HereIAm first");
                        }

                        proc_frame(id, d, &m, &mut fw).await?;
                        deadline = deadline_secs(50);
                    }
                }
            }
        }
    }
}

struct Downstairs {
    disk: Disk,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;

    /*
     * Open the disk for which we will be responsible.
     */
    let mut disk = Disk::open(&opt.data, Default::default())?;
    disk.extend(10)?;

    println!("Startup Extent values: {:?}", disk.versions());
    let d = Arc::new(Downstairs { disk });

    /*
     * Establish a listen server on the port.
     */
    let listen_on = SocketAddrV4::new(opt.address, opt.port);
    let listener = TcpListener::bind(&listen_on).await?;

    let mut next_id: u64 = 1;
    println!("listening on {}", listen_on);
    loop {
        let id = next_id;
        next_id += 1;

        let (sock, raddr) = listener.accept().await?;
        println!("connection {} from {:?}", id, raddr);

        /*
         * Spawn a task to deal with this connection.
         */
        let d = Arc::clone(&d);
        tokio::spawn(async move {
            if let Err(e) = proc(id, &d, sock).await {
                println!("ERROR: proc({}): {:?}", id, e);
            } else {
                println!("OK: proc({}): all done", id);
            }
        });
    }
}
