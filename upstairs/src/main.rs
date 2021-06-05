use std::collections::HashMap;
use std::io::{stdin, stdout, Read, Write};
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crucible_protocol::*;

use anyhow::{bail, Result};
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

/*
 * XXX This is temp, this DiskDefinition needs to live in some common
 * area where both upstairs and downstaris can use it.  To get things
 * to work, the struct here is a copy of what lives in downstairs disk.rs
 * file, which should also be renamed, but that is the subject of another
 * XXX in that file.
 */
#[derive(Debug)]
pub struct DiskDefinition {
    /**
     * The size of each block in bytes.  Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: u64,

    /**
     * How many whole extents comprise this disk?
     */
    extent_count: u32,
}

impl Default for DiskDefinition {
    fn default() -> DiskDefinition {
        DiskDefinition {
            block_size: 0,
            extent_size: 0,
            extent_count: 0,
        }
    }
}

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
    target: &SocketAddrV4,
    _u: &Arc<Upstairs>,
    m: &Message,
    _fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
) -> Result<()> {
    match m {
        Message::Imok => Ok(()),
        Message::WriteAck(rn) => {
            println!("{} Write {} acked", target, rn);
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            Ok(())
        }
        Message::FlushAck(rn) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            println!("{} flush {} acked", target, rn);
            // XXX Clear dirty bit, but we need the job info for that.
            Ok(())
        }
        x => bail!("unexpected frame {:?}", x),
    }
}

// XXX This should move to some common lib.  This will be used when
// the upstairs switches to sending write requests using extent EID and
// block offset.
fn _eid_from_offset(u: &Arc<Upstairs>, offset: u64) -> usize {
    let ddef = u.ddef.lock().unwrap();
    let space_per_extent = ddef.block_size * ddef.extent_size;
    let eid: u64 = offset / space_per_extent;
    eid as usize
}

/*
 * Decide what to do with a downstairs that has just connected and has
 * sent us information about its extents.
 *
 * This will eventually need to message the main thread so it knows
 * when it can starting doing I/O.
 */
fn process_downstairs(
    target: &SocketAddrV4,
    u: &Arc<Upstairs>,
    bs: u64,
    es: u64,
    ec: u32,
    versions: Vec<u64>,
) -> Result<()> {
    println!(
        "{} process: bs:{} es:{} ec:{} versions: {:?}",
        target, bs, es, ec, versions
    );

    let mut v = u.versions.lock().unwrap();
    if v.len() == 0 {
        /*
         * This is the first version list we have, so
         * we will make it the original and compare
         * whatever comes next.
         */
        *v = versions;
        println!("Setting inital Extent versions to {:?}", v);

        /*
         * Create the dirty vector with the same length as our version list.
         * XXX Not used yet.
         */
        let mut d = u.dirty.lock().unwrap();
        *d = vec![false; v.len()];
    } else if v.len() != versions.len() {
        /*
         * I don't think there is much we can do here, the expected number
         * of flush numbers does not match.  Possibly we have grown one but
         * not the rest of the downstairs?
         */
        panic!(
            "Expected downstairs version \
              len:{:?} does not match new \
              downstairs:{:?}",
            v.len(),
            versions.len()
        );
    } else {
        /*
         * We already have a list of versions to compare with.  Make that
         * comparision now against this new list
         */
        let ver_cmp = v.iter().eq(versions.iter());
        if !ver_cmp {
            // XXX Recovery process should start here
            panic!(
                "{} MISMATCH process: v: {:?} != versions: {:?}",
                target, v, versions
            );
        }
    }

    /*
     * XXX Here we have another workaround.  We don't know
     * the disk info until after we connect to each
     * downstairs, but we share the ARC Upstairs before we
     * know what to expect.  For now I'm using zero as an
     * indication that we don't yet know the valid values
     * and non-zero meaning we have at least one downstairs
     * to compare with.  We might want to consider breaking
     * out the static config info into something different
     * that is updated on initial downstairs setup from the
     * structures we use for work submission.
     */
    let mut ddef = u.ddef.lock().unwrap();
    if ddef.block_size == 0 {
        ddef.block_size = bs;
        ddef.extent_size = es;
        ddef.extent_count = ec;
        println!("Global using: bs:{} es:{} ec:{}", bs, es, ec);
    }

    if ddef.block_size != bs
        || ddef.extent_size != es
        || ddef.extent_count != ec
    {
        // XXX Figure out if we can hande this error.  Possibly not.
        panic!("New downstaris disk info mismatch");
    }
    Ok(())
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
    let mut deadline = deadline_secs(50);
    let mut negotiated = false;

    /*
     * To keep things alive, initiate a ping any time we have been idle for a
     * second.
     */
    let mut pingat = deadline_secs(10);
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
                /*
                 * This case is for when the main task has something it
                 * wants us to do.
                 */
                let new_ri: u64;
                let job: IOop;
                {
                    new_ri = *input.borrow();
                }

                /*
                 * We can't hold the hashmap mutex into the await send
                 * below, so make a scope to get our next piece of work
                 * from the hashmap and release the lock when we leave
                 * this scope.
                 */
                {
                    let hm = &*u.work.lock().unwrap();
                    job = match hm.get(&new_ri) {
                        Some(job) => {
                            assert_eq!(new_ri, job.request_id);
                            job.work.clone()
                        }
                        None => {
                            bail!("Missing job hashmap entry {}", new_ri);
                        }
                    };
                }

                match job {
                    IOop::Write{request_id, dependencies, data, offset} => {
                        assert_eq!(request_id, new_ri);
                        /*
                         * XXX Before we write, we should set the dirty bit
                         * {
                         *    let eid = eid_from_offset(u, offset);
                         *    let dirt = u.dirty.lock().unwrap();
                         *}
                         */
                        fw.send(Message::Write(request_id,
                                               offset,
                                               data.clone())).await?;
                    }
                    IOop::Flush{request_id, dependencies, flush_numbers} => {
                        assert_eq!(request_id, new_ri);
                        //let dep = Vec::new();
                        println!("sending flush with dep:{:?} fl:{:?}",
                                dependencies, flush_numbers);
                        fw.send(Message::Flush(request_id,
                                               dependencies.clone(),
                                               flush_numbers.clone())).await?;
                    }
                    _ => {
                        println!("No action for that");
                    }
                }

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
                        /*
                         * XXX Valid version to compare with should come
                         * from main task
                         */
                        if version != 1 {
                            bail!("expected version 1, got {}", version);
                        }
                        negotiated = true;
                        needping = true;
                        println!("{} is version {}", target, version);

                        deadline = deadline_secs(50);

                        /*
                         * Ask for the current version of all extents.
                         */
                        fw.send(Message::ExtentVersionsPlease).await?;
                    }
                    Some(Message::ExtentVersions(bs, es, ec, versions)) => {
                        process_downstairs(target, u, bs, es, ec, versions)?;

                        /*
                         *  XXX I moved this here for now so we can use this
                         * signal to move forward with I/O.  Eventually this
                         * connected being true state should be sent from the
                         * initial YesItsMe case, and we send something else
                         * through a different watcher that tells the main
                         * task the list of versions, or something like that
                         */
                        *connected = true;
                        output.send(Condition {
                            target: *target,
                            connected: true,
                        }).await
                        .unwrap();
                    }
                    Some(m) => {
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }

                        // println!("{} --recv--> {:?}", target, m);
                        proc_frame(&target, u, &m, &mut fw).await?;
                        deadline = deadline_secs(50);
                        pingat = deadline_secs(10);
                        needping = true;
                    }
                }
            }
        }
    }
}

/*
 * This task is responsible for the connection to and traffic between
 * a specific downstairs instance.  In here we handle taking work off of
 * the global work list and performing that work for a specific downstairs.
 */
async fn looper(
    target: SocketAddrV4,
    mut input: watch::Receiver<u64>,
    output: mpsc::Sender<Condition>,
    u: &Arc<Upstairs>,
    client_id: u8,
) {
    println!("{0:?} {1} looper start for {0:?}", target, client_id);

    let mut firstgo = true;
    let mut connected = false;

    'outer: loop {
        if firstgo {
            firstgo = false;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Make connection to this downstairs.
         */
        let sock = TcpSocket::new_v4().expect("v4 socket");

        /*
         * Set a connect timeout, and connect to the target:
         */
        println!("{0} connecting to {0}", target);
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
                            println!("{0} ok, connected to {0}", target);
                            break tcp;
                        }
                        Err(e) => {
                            println!("{0} connect to {0} failure: {1:?}",
                                target, e);
                            continue 'outer;
                        }
                    }
                }
            }
        };

        /*
         * Once we have a connected downstairs, the proc task takes over and
         * handles negiotation and work processing.
         */
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
#[derive(Debug)]
struct Upstairs {
    work: Mutex<HashMap<u64, IOWork>>,
    // Completed jobs ring buffer
    // Last flush ID?
    // The versions vec is not enough to solve a mismatch.  We really need
    // Generation number, flush number, and dirty bit for every extent.
    versions: Mutex<Vec<u64>>,
    dirty: Mutex<Vec<bool>>,
    ddef: Mutex<DiskDefinition>,
}

#[derive(Debug)]
struct IOWork {
    request_id: u64,       // This MUST match our hash index
    completed_by: [u8; 3], // mutex protect me too?? XXX
    work: IOop,
}

#[derive(Debug, Clone)]
pub enum IOop {
    Write {
        request_id: u64,
        dependencies: Vec<u64>, // Other writes that need to come before this
        data: bytes::Bytes,
        offset: u64,
    },
    Read {
        offset: u64,
        length: u32,
    },
    Flush {
        request_id: u64,
        dependencies: Vec<u64>, // List of write requests that must finish first
        flush_numbers: Vec<u64>,
    },
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

    let hm = Mutex::new(HashMap::new());
    let u = Arc::new(Upstairs {
        work: hm,
        versions: Mutex::new(Vec::new()),
        dirty: Mutex::new(Vec::new()),
        ddef: Mutex::new(DiskDefinition::default()),
    });

    println!(
        "#### target list: {:#?} len:{}",
        opt.target,
        opt.target.len()
    );

    /*
     * Use this channel to receive updates on target status from each target
     * management task.
     */
    let (ctx, mut crx) = mpsc::channel::<Condition>(32);

    let mut next_id = 1;
    let mut client_id = 0;
    let t = opt
        .target
        .iter()
        .map(|t| {
            /*
             * Create the channel that we will use to request that the loop
             * check for work to do in the central structure.
             * XXX Not quite wired up yet.
             */
            let (itx, irx) = watch::channel(next_id);

            let u = Arc::clone(&u);
            let ctx = ctx.clone();
            let t0 = *t;
            tokio::spawn(async move {
                looper(t0, irx, ctx, &u, client_id).await;
            });
            client_id += 1;

            Target {
                target: *t,
                input: itx,
            }
        })
        .collect::<Vec<_>>();

    // async tasks need to tell us they are alive, but they also need to
    // tell us the extent list from any attached downstairs.
    // That part is not connected yet.
    let mut connected = 0;
    while connected < opt.target.len() {
        let c = crx.recv().await.unwrap();

        /*
         * XXX NOTES ON INTENDED STATE MACHINE
         *
         * From cold start:
         *
         * When we transition from 0 -> 1 connected Downstairs, nothing happens
         * yet.
         *
         * When we transition from 1 -> 2 connected Downstairs, we must assess
         * the contents of both.  First, the mundane book-keeping: ensure they
         * all have the same block size, extent size, and extent count.  Then,
         * for each extent we get the extent Version and the extent Checksum.
         * If those values are the same on both Downstairs, then we can move on
         * to the next extent.  If they are not the same, the highest Version
         * wins and we replace the contents of the Extent on the other
         * Downstairs; if they ARE the same, it doesn't matter which we select
         * as long as they both end up the same.  Once both Downstairs have an
         * identical set of extents, we are up for WRITES.  It is not
         * anticipated that this will take very long, as there should only be
         * around one flush worth of outstanding data to reconcile.
         *
         * When we transition from 2 -> 3 connected Downstairs, we must perform
         * the same reconciliation, with the added complexity that we are also
         * generally trying to write to the volume.  This process can be
         * incremental, one extent at a time, and it seems likely that we can
         * ourselves just hold writes to that extent while verifying the
         * contents.  If an extent is small, this won't take long.  Writes can
         * start flowing to the synced subset of extents on the 3rd Downstairs
         * as soon as they are synced up -- we should keep a bitmap of which
         * extents are OK on which Downstairs in memory.
         *
         * During regular WRITE operation, we will issue each write to each
         * connected Downstairs.  As soon as two Downstairs have acknowledged
         * it, we can complete in the guest.  A subsequent guest write that
         * overlaps another write that has not yet been acknowledged by all
         * Downstairs will need to "happen after" the first, whether by stalling
         * the second write, or by somehow making it dependent on the first in
         * the protocol request itself.
         *
         * A flush is issued to all Downstairs simultaneously, and all
         * previously issued writes to the Downstairs must be stable on disk
         * (fsync) before the Downstairs completes the flush.  A flush includes
         * a version number, which will be applied to an extents that have been
         * modified since the last flush and itself made stable.  The flush
         * invariant that a disk must expose to the guest is: any write that
         * completed before the flush was issued must be stable on disk; any
         * write that completed after the flush was issued is not stable until
         * another flush.
         *
         * From a warm start:
         *
         * There probably should not be a situation where Upstairs reboots
         * without also rebooting the guest.  As long as Upstairs continues to
         * run, it can remember which I/O requests were in flight when a
         * connection to any particular Downstairs is interrupted.  If that
         * state is dropped on the floor, the guest will need to be told about
         * it somehow -- but we likely don't have a good way to inform the guest
         * of, say, a Virtio Block device malfunction that drops all I/O that
         * was previously inflight, leaving the disk in an indeterminate state.
         */
        if c.connected {
            println!("#### {:?} #### CONNECTED ########", c.target);
            connected += 1;
        } else {
            println!("#### {:?} #### DISCONNECTED! ####", c.target);
            connected -= 1;
        }
    }

    println!("#### Connected all async tasks");
    test_pause();

    create_work(&u, 1)?;
    println!("#### Created a write and a flush, put on work queue");
    test_pause();

    println!("#### next_id {} sending IO work", next_id);
    t.iter().for_each(|t| t.input.send(next_id).unwrap());
    test_pause();

    next_id += 1;
    println!("#### next_id {} sending IO work", next_id);
    t.iter().for_each(|t| t.input.send(next_id).unwrap());
    test_pause();

    create_work(&u, 3)?;
    println!("#### Created another write and flush, put on work queue");
    test_pause();

    next_id += 1;
    println!("#### next_id {} sending IO work", next_id);
    t.iter().for_each(|t| t.input.send(next_id).unwrap());

    test_pause();
    next_id += 1;
    println!("#### next_id {} sending IO work", next_id);
    t.iter().for_each(|t| t.input.send(next_id).unwrap());

    println!();
    println!("#### Main loop will now exit");
    test_pause();

    /*
     * XXX Need to cleanup async tasks and close connections.
     */
    Ok(())
}

/*
 * These are some simple functions to create IO work for testing
 */
fn create_write(ri: u64, offset: u64, data_seed: u8) -> IOWork {
    let mut data = BytesMut::with_capacity(512);
    data.put(&[data_seed; 512][..]);
    let data = data.freeze();

    let awrite = IOop::Write {
        request_id: ri,
        dependencies: Vec::new(), // XXX Coming soon
        data,
        offset,
    };

    IOWork {
        request_id: ri,
        completed_by: [0; 3],
        work: awrite,
    }
}

fn create_flush(ri: u64, fln: Vec<u64>) -> IOWork {
    let flush = IOop::Flush {
        request_id: ri,
        dependencies: Vec::new(), // XXX coming soon
        flush_numbers: fln,
    };

    IOWork {
        request_id: ri,
        completed_by: [0; 3],
        work: flush,
    }
}

fn create_work(u: &Arc<Upstairs>, mut ri: u64) -> Result<()> {
    let mut m = u.work.lock().unwrap();

    let w = create_write(ri, 102400, 0xBB);
    m.insert(ri, w);
    ri += 1;

    let v = u.versions.lock().unwrap();
    let w = create_flush(ri, v.clone());
    m.insert(ri, w);
    Ok(())
}

fn test_pause() {
    let mut stdout = stdout();
    stdout.write_all(b"Press Enter to continue...\n").unwrap();
    stdout.flush().unwrap();
    stdin().read_exact(&mut [0]).unwrap();
}
