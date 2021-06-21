use std::collections::HashMap;
use std::fmt;
use std::io::{stdin, stdout, Read, Write};
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

use crucible_protocol::*;

use anyhow::{bail, Result};
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch};
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
    u: &Arc<Upstairs>,
    m: &Message,
    _fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
    client_id: u8,
) -> Result<()> {
    match m {
        Message::Imok => Ok(()),
        Message::WriteAck(rn) => {
            println!("{} Write rn:{} acked", target, rn);
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            io_completed(u, *rn, client_id)
        }
        Message::FlushAck(rn) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include more so we know which target
            // completed, and know when to ack back to the host
            println!("{} Flush rn:{} acked", target, rn);
            // XXX Clear dirty bit, but we need the job info for that.
            io_completed(u, *rn, client_id)
        }
        Message::ReadResponse(rn, data) => {
            // XXX put this on the completed list for this downstairs
            // We might need to include which target responded
            let l = data.len();
            let mut snip = [0; 4];
            snip[..4].copy_from_slice(&data[0..4]);
            println!("{} Read  rn:{} len:{} data:0x{:x?}", target, rn, l, snip);
            io_completed(u, *rn, client_id)
        }
        x => bail!("unexpected frame {:?}", x),
    }
}

// XXX This should move to some common lib.  This will be used when
// the upstairs needs to translate a read/write offset in to the extent
// eid and the block offset in the extent.
// block offset.
fn _extent_from_offset(u: &Arc<Upstairs>, offset: u64) -> Result<(usize, u64)> {
    let ddef = u.ddef.lock().unwrap();
    let space_per_extent = ddef.block_size * ddef.extent_size;
    let eid: u64 = offset / space_per_extent;
    let block_in_extent: u64 =
        (offset - (eid * space_per_extent)) / ddef.block_size;
    Ok((eid as usize, block_in_extent))
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
        "{} Evaluate new downstairs : bs:{} es:{} ec:{} versions: {:?}",
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
            println!(
                "{} MISMATCH expected: {:?} != new: {:?}",
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
     *
     * 0 should never be a valid block size
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

/*
 * This function is called when the upstairs task is notified that
 * a downstairs has completed an IO request.
 *
 * We do the work here to decide if the IO is completed, or if we
 * are still waiting for other downstairs to respond.  If the IO is
 * completed, then we can add it to the completed ringbuf and take it
 * off of the active hashmap where new and in progress IOs are.
 */
fn io_completed(
    u: &Arc<Upstairs>,
    request_number: u64,
    client_id: u8,
) -> Result<()> {
    let mut hm = u.work.lock().unwrap();
    let job = hm.get_mut(&request_number);
    let mut in_progress = 0;
    match job {
        Some(job) => {
            assert_eq!(request_number, job.request_id);
            // XXX assert job in progress?  Not completed?
            job.state.insert(client_id, IOState::Done);
            for (_, state) in job.state.iter() {
                if *state == IOState::New || *state == IOState::InProgress {
                    in_progress += 1;
                }
            }
        }
        None => {
            bail!("Missing job hashmap entry {}", request_number);
        }
    }
    /*
     * for a read, we can send the results to the caller once we get
     * one answer.
     * For a write or flush, we need at least two "finsihed" states
     * before we can send results to the caller.
     *
     * XXX How do we deal with a write error, or a write that never
     * comes back?  We will need to update states to include an error
     * as a result.
     */
    if in_progress == 0 {
        println!("Remove job {}", request_number);
        hm.remove(&request_number);
        /*
         * We are getting the completed lock while holding hashmap lock.
         * We do need to hold the work HM lock so a request ID will transition
         * from active to completed and not have a window where a completed job
         * has left the work HM but not yet arrived on the completed list where
         * some other actor could go looking for it, such as a flush.
         */
        let mut completed = u.completed.lock().unwrap();
        completed.push(request_number);
    }
    Ok(())
}

/*
 * This fn is for when the main task has added work to the hashmap
 * and the task for a downstairs has received the notification that there
 * is new work to do.
 */
async fn io_send(
    u: &Arc<Upstairs>,
    fw: &mut FramedWrite<WriteHalf<'_>, CrucibleEncoder>,
    client_id: u8,
) -> Result<()> {
    let mut job: IOop;
    let mut new_work: Vec<u64> = Vec::new();

    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     */
    println!("[{}] io_send", client_id);
    {
        let mut hm = u.work.lock().unwrap();
        for (id, job) in hm.iter_mut() {
            let state = job.state.get(&client_id);
            if let Some(IOState::New) = state {
                new_work.push(*id);
            }
        }
    }

    /*
     * Now we have a list of all the job IDs that are new for our client id.
     * Walk this list and process each job, marking it InProgress as we
     * do the work.  We do this in two loops because we can't hold the
     * lock for the hashmap while we do work, and if we release the lock
     * to do work, we would have to start over and look at all jobs in the
     * map to see if they are new.
     *
     * This also allows us to sort the job ids and do them in order they
     * were put into the hashmap, though I don't think that is required.
     */
    new_work.sort();
    if !new_work.is_empty() {
        println!("[{}] new_work_vector: {:?}", client_id, new_work);
    }
    for new_id in new_work.iter() {
        /*
         * We can't hold the hashmap mutex into the await send
         * below, so make a scope to get our next piece of work
         * from the hashmap and release the lock when we leave
         * this scope.
         */
        {
            let mut hm = u.work.lock().unwrap();
            job = match hm.get_mut(&new_id) {
                Some(job) => {
                    assert_eq!(*new_id, job.request_id);
                    assert_eq!(
                        job.state.get(&client_id).unwrap(),
                        &IOState::New
                    );
                    job.state.insert(client_id, IOState::InProgress);
                    job.work.clone()
                }
                None => {
                    bail!("Missing job hashmap entry {}", *new_id);
                }
            };
        }
        match job {
            IOop::Write {
                dependencies,
                eid,
                block_offset,
                data,
            } => {
                /*
                 * XXX Before we write, we should set the dirty bit
                 * {
                 *    let eid = eid_from_offset(u, offset);
                 *    let dirt = u.dirty.lock().unwrap();
                 *}
                 */
                println!(
                    "Write rn:{} eid:{:?} bo:{:?}",
                    *new_id, eid, block_offset
                );
                fw.send(Message::Write(
                    *new_id,
                    eid,
                    block_offset,
                    data.clone(),
                ))
                .await?
            }
            IOop::Flush {
                dependencies,
                flush_numbers,
            } => {
                println!(
                    "Flush rn:{} dep:{:?} fl:{:?}",
                    *new_id, dependencies, flush_numbers
                );
                fw.send(Message::Flush(
                    *new_id,
                    dependencies.clone(),
                    flush_numbers.clone(),
                ))
                .await?
            }
            IOop::Read { eid, block_offset } => {
                println!(
                    "Read  rn:{} eid:{:?} bo:{:?}",
                    *new_id, eid, block_offset
                );
                fw.send(Message::ReadRequest(*new_id, eid, block_offset))
                    .await?
            }
        }
    }
    Ok(())
}

/*
 * Once we have a connection to a downstairs, this task takes over and
 * handles both the initial negotiation and then watches the input for
 * changes, indicating that new work in on the work hashmap.  We will
 * walk the hashmap on the input signal and get any new work for this
 * specific downstairs and mark that job as in progress.
 */
async fn proc(
    target: &SocketAddrV4,
    input: &mut watch::Receiver<u64>,
    output: &mpsc::Sender<Condition>,
    u: &Arc<Upstairs>,
    mut sock: TcpStream,
    connected: &mut bool,
    client_id: u8,
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
        /*
         * XXX Just a thought here, could we send so much input that the
         * select would always have input.changed() and starve out the
         * fr.next() select?  Does this select ever work that way?
         */
        println!("{}[{}] tokio select", target, client_id);

        tokio::select! {
            _ = sleep_until(deadline) => {
                if !negotiated {
                    bail!("did not negotiate a protocol");
                } else {
                    bail!("inactivity timeout");
                }
            }
            _ = sleep_until(pingat), if needping => {
                println!("{}[{}] ping", target, client_id);
                fw.send(Message::Ruok).await?;
                needping = false;
            }
            _ = input.changed() => {
                println!("{}[{}] input changed", target, client_id);
                /*
                 * Something new on the work hashmap.  Go off and figure
                 * out what we need to do.  If there is new work for us then
                 * do that work, marking it as in progress.
                 */
                io_send(u, &mut fw, client_id).await?;
            }
            f = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                println!("{}[{}] frnext", target, client_id);
                match f.transpose()? {
                    None => {
                        println!("{}[{}] None", target, client_id);
                        return Ok(())
                    }
                    Some(Message::YesItsMe(version)) => {
                        println!("{}[{}] yim", target, client_id);
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
                        deadline = deadline_secs(50);

                        /*
                         * Ask for the current version of all extents.
                         */
                        fw.send(Message::ExtentVersionsPlease).await?;
                    }
                    Some(Message::ExtentVersions(bs, es, ec, versions)) => {
                        println!("{}[{}] extv", target, client_id);
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }
                        process_downstairs(target, u, bs, es, ec, versions)?;

                        /*
                         *  XXX I moved this here for now so we can use this
                         * signal to move forward with I/O.  Eventually this
                         * connected being true state should be sent from the
                         * initial YesItsMe case, and we send something else
                         * through a different watcher that tells the main
                         * task the list of versions, or something like that.
                         */

                        /*
                         * If we get here, we are ready to receive IO
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
                        println!("{}[{}] some", target, client_id);

                        proc_frame(&target, u, &m, &mut fw, client_id).await?;
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
        println!("{0}[{1}] connecting to {0}", target, client_id);
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
                            println!("{0}[{1}] ok, connected to {0}",
                                target,
                                client_id);
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
        if let Err(e) = proc(
            &target,
            &mut input,
            &output,
            u,
            tcp,
            &mut connected,
            client_id,
        )
        .await
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
    /*
     * A IOWork struct is added to this hashmap when the upstairs receives
     * a new work request from a client.  See details about the IOWork
     * structure where it is defined.
     */
    work: Mutex<HashMap<u64, IOWork>>,
    // XXX Might need to consider some arrangement between the work and
    // completed lists to prevent deadlocks.  This may require deeper thought
    completed: Mutex<AllocRingBuffer<u64>>,
    // The versions vec is not enough to solve a mismatch.  We really need
    // Generation number, flush number, and dirty bit for every extent
    // when resolving conflicts.
    versions: Mutex<Vec<u64>>,
    dirty: Mutex<Vec<bool>>,
    /*
     * The global description of the downstairs region we are using.
     * This allows us to verify each downstairs is the same, as well as
     * enables us to tranlate an LBA to an extent and block offset.
     */
    ddef: Mutex<DiskDefinition>,
    /*
     * The state of a downstairs connection, based on client ID
     * Ready here indicates it can receive IO.
     */
    downstairs: Mutex<Vec<DownstairsState>>,
}

/*
 * I think we will have more states.  If not, then this should just become
 * a bool.
 */
#[derive(Debug, Clone)]
pub enum DownstairsState {
    NotReady,
    Ready,
}

/*
 * A unit of work that is put into the hashmap.
 */
#[derive(Debug)]
struct IOWork {
    request_id: u64, // This MUST match our hashmap index
    work: IOop,
    /*
     * Hash of work status where key is the downstairs "client id" and the
     * hash value is the current state of the IO request with respect to the
     * upstairs.
     * The length and keys on this hashmap will be used to determine which
     * downstairs will receive the IO request.
     * XXX Determine if it is required for all downstairs to get an entry
     * or if by not putting a downstars in the hash, if that is valid.
     */
    state: HashMap<u8, IOState>,
}

#[derive(Debug, Clone)]
pub enum IOop {
    Write {
        dependencies: Vec<u64>, // Other writes that need to come before this
        eid: u64,
        block_offset: u64,
        data: bytes::Bytes,
    },
    Read {
        eid: u64,
        block_offset: u64,
    },
    Flush {
        dependencies: Vec<u64>, // List of write requests that must finish first
        flush_numbers: Vec<u64>,
    },
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 *
 * New:  A new IO request.
 * InProgress:  The request has been sent to this tasks downstairs.
 * Done:        The response came back from downstairs.
 * Skipped:     The IO request should be ignored.  This situation could be
 *              A read that only needs one downstairs to answer, or we are
 *              doing recovery and we only want a specific downstairs to
 *              do that work.
 * Error:       The IO returned some error.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum IOState {
    New, // XXX Maybe name the Ready, or Available, or something else?
    InProgress,
    Done,
    Skipped,
    Error,
}

impl fmt::Display for IOState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IOState::New => {
                write!(f, " New")
            }
            IOState::InProgress => {
                write!(f, "Sent")
            }
            IOState::Done => {
                write!(f, "Done")
            }
            IOState::Skipped => {
                write!(f, "Skip")
            }
            IOState::Error => {
                write!(f, " Err")
            }
        }
    }
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

fn main() -> Result<()> {

    let opt = opts()?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(20)
        .thread_name("upstairs-main")
        .enable_all()
        .build()
        .unwrap();

    /*
     * This one shows the hang on one task.
     */
    runtime.spawn(up_main(opt));
    println!("runtime is spawned: ");


    /*
     *  This fails the same way.
    runtime.block_on(async {
        println!("This is the task 1");
        tokio::spawn(up_main(opt));
    });
    */

    /* If I switch main to start with tokio runtime, this works

    up_main(opt).await?;

    */
    loop {};
    Ok(())
}

/*
 * Send work to all the targets on this vector.
 * This can be much simpler, but we need to (eventually) take special action
 * when we fail to send a message to a task.
 */
fn send_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        println!("#### send to client {:?}", d_client.target);
        let res = d_client.input.send(val);
        if let Err(e) = res {
            println!("#### error {:#?} sending work to {:?}",
                    e, d_client.target);
            /*
             * XXX
             * Write more code for this error,  If one downstairs
             * never receives a request, it may get picked up on the
             * next request.  However, if the downstairs has gone away,
             * then action will need to be taken, and soon.
             */
        }
    }
}

/*
 * This is the main upstairs task that is responsible for accepting
 * work from propolis (or whomever) and taking that work and converting
 * it into a crucible IO, then sending it on to be processed to the mux
 * portion of crucible.
 */
async fn up_main(opt: Opt) -> Result<()> {

    let hm = Mutex::new(HashMap::new());
    let completed = Mutex::new(AllocRingBuffer::with_capacity(2048));
    let up = Arc::new(Upstairs {
        work: hm,
        completed,
        versions: Mutex::new(Vec::new()),
        dirty: Mutex::new(Vec::new()),
        ddef: Mutex::new(DiskDefinition::default()),
        downstairs: Mutex::new(Vec::with_capacity(opt.target.len())),
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

    let mut client_id = 0;
    let t = opt
        .target
        .iter()
        .map(|t| {
            /*
             * Create the channel that we will use to request that the loop
             * check for work to do in the central structure.
             * XXX Not sure if anyone reads this first value
             */
            let (itx, irx) = watch::channel(0);

            let up = Arc::clone(&up);
            let ctx = ctx.clone();
            let t0 = *t;
            tokio::spawn(async move {
                looper(t0, irx, ctx, &up, client_id).await;
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
    let mut ri = 1;
    loop {
        while connected < opt.target.len() {
            println!("Wait for all tasks to report connected {}/{}",
                connected, opt.target.len());
            let c = crx.recv().await.unwrap();
            if c.connected {
                println!("#### {:?} #### CONNECTED ########", c.target);
                connected += 1;
            } else {
                println!("#### {:?} #### DISCONNECTED! ####", c.target);
                connected -= 1;
            }
        }
        /*
         * To work like this, we need to do stuff here and then go back
         * and watch for clients going away and decide how to take action
         * on that.  I think too much might be done at the individual
         * downstairs level and not enough here in this "mux" task.
         */

        // Can we look at what we have spawned?
        // Can we use the input or crx to tell if something as gone away.
        // Where do we handle more than one mirror going away?
        println!("#### Create test work, put on work queue");
        test_pause();
        ri = create_work(&up, ri, 0x44, 5, 15).unwrap();
        //ri = create_more_work(&up, ri).unwrap(); // job id, data_seed, eid, block_offset
        show_work(&up)?;

        println!("#### ready to submit work one");
        test_pause();
        t.iter().for_each(|t| t.input.send(2).unwrap());
        t.iter().for_each(|t| t.input.send(1).unwrap());
        t.iter().for_each(|t| t.input.send(2).unwrap());
        t.iter().for_each(|t| t.input.send(1).unwrap());
        //send_work(&t, 2);
        println!("#### work submitted, now show work queue");
        test_pause();
        show_work(&up)?;
        test_pause();
    }

    /*
     * XXX Need to cleanup async tasks and close connections.
     */
    Ok(())
}

/*
 * These are some simple functions to create IO work for testing
 */
fn create_write(ri: u64, eid: u64, block_offset: u64, data_seed: u8) -> IOWork {
    let mut data = BytesMut::with_capacity(512);
    data.put(&[data_seed; 512][..]);
    let data = data.freeze();

    let awrite = IOop::Write {
        dependencies: Vec::new(), // XXX Coming soon
        eid,
        block_offset,
        data,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        request_id: ri,
        work: awrite,
        state,
    }
}

fn create_flush(ri: u64, fln: Vec<u64>) -> IOWork {
    let flush = IOop::Flush {
        dependencies: Vec::new(), // XXX coming soon
        flush_numbers: fln,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        request_id: ri,
        work: flush,
        state,
    }
}

fn create_read(ri: u64, eid: u64, block_offset: u64) -> IOWork {
    let aread = IOop::Read { eid, block_offset };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    IOWork {
        request_id: ri,
        work: aread,
        state,
    }
}

/*
 * Debug function to display the work hashmap with status for all three of
 * the clients.
 */
fn show_work(up: &Arc<Upstairs>) -> Result<()> {
    let mut hm = up.work.lock().unwrap();
    for (id, job) in hm.iter_mut() {
        print!("JOB:[{:04}] ", id);
        for cid in 0..3 {
            let state = job.state.get(&cid);
            match state {
                Some(state) => {
                    print!("[{}] state: {}  ", cid, state);
                }
                x => {
                    print!("[{}] unknown state:{:#?}", cid, x);
                }
            }
        }
        println!();
    }
    let done = up.completed.lock().unwrap();
    let done_vec = done.to_vec();
    println!("Done: {:?}", done_vec);
    Ok(())
}

fn create_more_work(up: &Arc<Upstairs>, start_ri: u64) -> Result<u64> {
    let mut hm = up.work.lock().unwrap();
    let mut ri = start_ri;
    let mut seed = 1;

    for eid in 0..10 {
        seed += 1;
        for bo in 0..100 {
            let wr = create_write(ri, eid, bo, seed);
            hm.insert(ri, wr);
            ri += 1;
        }
    }

    let ver = up.versions.lock().unwrap();
    let fl = create_flush(ri, ver.clone());
    hm.insert(ri, fl);
    ri += 1;

    for eid in 0..10 {
        for bo in 0..100 {
            let re = create_read(ri, eid, bo);
            hm.insert(ri, re);
            ri += 1;
        }
    }
    Ok(ri)
}

fn create_work(
    up: &Arc<Upstairs>,
    start_ri: u64,
    seed: u8,
    eid: u64,
    block_offset: u64,
) -> Result<u64> {
    let mut hm = up.work.lock().unwrap();

    let mut ri = start_ri;
    let wr = create_write(ri, eid, block_offset, seed);
    hm.insert(ri, wr);
    ri += 1;

    let ver = up.versions.lock().unwrap();
    let fl = create_flush(ri, ver.clone());
    hm.insert(ri, fl);
    ri += 1;

    let re = create_read(ri, eid, block_offset);
    hm.insert(ri, re);
    ri += 1;
    Ok(ri)
}

fn test_pause() {
    let mut stdout = stdout();
    thread::sleep(Duration::from_secs(1));
    stdout.write_all(b"Press Enter to continue...\n").unwrap();
    stdout.flush().unwrap();
    stdin().read_exact(&mut [0]).unwrap();
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn offset_to_extent() {
        let def = DiskDefinition {
            block_size: 512,
            extent_size: 100,
            extent_count: 10,
        };

        let up = Arc::new(Upstairs {
            work: Mutex::new(HashMap::new()),
            versions: Mutex::new(Vec::new()),
            dirty: Mutex::new(Vec::new()),
            ddef: Mutex::new(def),
        });

        assert_eq!(_extent_from_offset(&up, 0).unwrap(), (0, 0));
        assert_eq!(_extent_from_offset(&up, 512).unwrap(), (0, 1));
        assert_eq!(_extent_from_offset(&up, 1024).unwrap(), (0, 2));
        assert_eq!(_extent_from_offset(&up, 1024 + 512).unwrap(), (0, 3));
        assert_eq!(_extent_from_offset(&up, 51200).unwrap(), (1, 0));
        assert_eq!(_extent_from_offset(&up, 51200 + 512).unwrap(), (1, 1));
        assert_eq!(_extent_from_offset(&up, 51200 + 1024).unwrap(), (1, 2));
        assert_eq!(_extent_from_offset(&up, 102400 - 512).unwrap(), (1, 99));
        assert_eq!(_extent_from_offset(&up, 102400).unwrap(), (2, 0));
    }
}
