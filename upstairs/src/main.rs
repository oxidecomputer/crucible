use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;

use crucible_protocol::*;

use anyhow::{bail, Result};
use futures::{SinkExt, StreamExt};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddrV4>,
}

fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

async fn proc_frame(
    _u: &Arc<Upstairs>,
    m: &Message,
    _fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
) -> Result<()> {
    match m {
        Message::Imok => Ok(()),
        x => bail!("unexpected frame {:?}", x),
    }
}

async fn proc(
    target: &SocketAddrV4,
    input: &mut watch::Receiver<u64>,
    output: &mpsc::Sender<Condition>,
    u: &Arc<Upstairs>,
    mut sock: TcpStream,
    connected: &mut bool,
) -> Result<()> {
    let (r, w) = sock.split();
    let mut fr = FramedRead::new(r, CrucibleDecoder::new());
    let mut fw = FramedWrite::new(w, CrucibleEncoder::new());

    /*
     * As the "client", we must begin the negotiation.
     */
    fw.send(Message::HereIAm(1)).await?;

    /*
     * Don't wait more than 5 seconds to hear from the other side.
     * XXX Timeouts, timeouts: always wrong!  Some too short and some too long.
     */
    let mut deadline = deadline_secs(5);
    let mut negotiated = false;

    /*
     * To keep things alive, initiate a ping any time we have been idle for a
     * second.
     */
    let mut pingat = deadline_secs(1);
    let mut needping = false;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => {
                if !negotiated {
                    bail!("did not negotiate a protocol");
                } else {
                    bail!("inactivity timeout");
                }
            }
            _ = sleep_until(pingat), if needping => {
                fw.send(Message::Ruok).await?;
                needping = false;
            }
            _ = input.changed() => {
                println!("~ ~ ~ {}: request from main thread! ~ ~ ~", target);
            }
            f = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                match f.transpose()? {
                    None => return Ok(()),
                    Some(Message::YesItsMe(version)) => {
                        if negotiated {
                            bail!("negotiated already!");
                        }
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = true;
                        needping = true;
                        *connected = true;
                        output.send(Condition {
                            target: *target,
                            connected: true,
                        }).await
                        .unwrap();

                        println!("{}: version {}", target, version);
                    }
                    Some(m) => {
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }

                        println!("{} --recv--> {:?}", target, m);
                        proc_frame(u, &m, &mut fw).await?;
                        deadline = deadline_secs(5);
                        pingat = deadline_secs(1);
                        needping = true;
                    }
                }
            }
        }
    }
}

async fn looper(
    target: SocketAddrV4,
    mut input: watch::Receiver<u64>,
    output: mpsc::Sender<Condition>,
    u: &Arc<Upstairs>,
) {
    println!("looper start for {:?}", target);

    let mut firstgo = true;
    let mut connected = false;

    'outer: loop {
        if firstgo {
            firstgo = false;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Make connection to host.
         */
        let sock = TcpSocket::new_v4().expect("v4 socket");

        /*
         * Set a connect timeout, and connect to the target:
         */
        println!("connecting to {:?}", target);
        let deadline = tokio::time::sleep_until(deadline_secs(10));
        tokio::pin!(deadline);
        let tcp = sock.connect(target.into());
        tokio::pin!(tcp);

        let tcp: TcpStream = loop {
            tokio::select! {
                _ = &mut deadline => {
                    println!("connect timeout");
                    continue 'outer;
                }
                tcp = &mut tcp => {
                    match tcp {
                        Ok(tcp) => {
                            println!("ok, connected to {:?}", target);
                            break tcp;
                        }
                        Err(e) => {
                            println!("connect to {:?} failure: {:?}",
                                target, e);
                            continue 'outer;
                        }
                    }
                }
            }
        };

        if let Err(e) =
            proc(&target, &mut input, &output, u, tcp, &mut connected).await
        {
            eprintln!("ERROR: {}: proc: {:?}", target, e);
        }

        if connected {
            output
                .send(Condition {
                    target,
                    connected: false,
                })
                .await
                .unwrap();
            connected = false;
        }
    }
}

/*
 * XXX Track scheduled storage work in the central structure.  Have the
 * target management task check for work to do here by changing the value in
 * its watch::channel.  Have the main thread determine that an overflow of
 * work to do backing up in here means we need to do something like mark the
 * target as behind or institute some kind of back pressure, etc.
 */
struct Upstairs {
}

struct Target {
    #[allow(dead_code)]
    target: SocketAddrV4,
    input: watch::Sender<u64>,
}

#[derive(Debug)]
struct Condition {
    target: SocketAddrV4,
    connected: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;

    let u = Arc::new(Upstairs {});

    println!("target list: {:#?}", opt.target);

    /*
     * Use this channel to receive updates on target status from each target
     * management task.
     */
    let (ctx, mut crx) = mpsc::channel::<Condition>(32);

    let mut lastcast = 1;
    let t = opt
        .target
        .iter()
        .map(|t| {
            /*
             * Create the channel that we will use to request that the loop
             * check for work to do in the central structure.
             * XXX Not quite wired up yet.
             */
            let (itx, irx) = watch::channel(lastcast);

            let u = Arc::clone(&u);
            let ctx = ctx.clone();
            let t0 = *t;
            tokio::spawn(async move {
                looper(t0, irx, ctx, &u).await;
            });

            Target {
                target: *t,
                input: itx,
            }
        })
        .collect::<Vec<_>>();

    loop {
        let c = crx.recv().await.unwrap();

        if c.connected {
            println!("{:?} #### CONNECTED ########", c.target);
        } else {
            println!("{:?} #### DISCONNECTED! ####", c.target);
        }

        lastcast += 1;
        t.iter().for_each(|t| t.input.send(lastcast).unwrap());
    }
}
