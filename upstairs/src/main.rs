use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crucible_protocol::*;

use anyhow::{anyhow, bail, Result};
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use structopt::StructOpt;
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch, Notify};
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
    let mut work = u.work.lock().unwrap();

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
    let counts = work.complete(request_number, client_id)?;

    if counts.active == 0 {
        println!("Remove job {}", request_number);
        work.retire(request_number);
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
    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     */
    println!("[{}] io_send", client_id);
    let mut new_work = u.work.lock().unwrap().new_work(client_id);

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
    } else {
        println!("[{}] new_work_vector is empty", client_id);
    }

    for new_id in new_work.iter() {
        /*
         * We can't hold the hashmap mutex into the await send
         * below, so make a scope to get our next piece of work
         * from the hashmap and release the lock when we leave
         * this scope.
         */
        let job = u.work.lock().unwrap().in_progress(*new_id, client_id);

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
    up: &Arc<Upstairs>,
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
                let iv = *input.borrow();
                println!("{}[{}] wakeup {} from main", target, client_id, iv);
                io_send(up, &mut fw, client_id).await?;
            }
            f = fr.next() => {
                /*
                 * Negotiate protocol before we get into specifics.
                 */
                match f.transpose()? {
                    None => {
                        return Ok(())
                    }
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
                        deadline = deadline_secs(50);

                        /*
                         * Ask for the current version of all extents.
                         */
                        fw.send(Message::ExtentVersionsPlease).await?;
                    }
                    Some(Message::ExtentVersions(bs, es, ec, versions)) => {
                        if !negotiated {
                            bail!("expected YesItsMe first");
                        }
                        process_downstairs(target, up, bs, es, ec, versions)?;

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

                        proc_frame(&target, up, &m, &mut fw, client_id).await?;
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
    up: &Arc<Upstairs>,
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
            up,
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

#[derive(Debug)]
struct Work {
    active: HashMap<u64, IOWork>,
    next_id: u64,
    completed: AllocRingBuffer<u64>,
}

#[derive(Debug, Default)]
struct WorkCounts {
    active: u64,
    done: u64,
}

impl Work {
    /**
     * Assign a new request ID.
     */
    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /**
     * Mark this request as in progress for this client, and return a copy
     * of the details of the request.
     */
    fn in_progress(&mut self, reqid: u64, client_id: u8) -> IOop {
        let job = self.active.get_mut(&reqid).unwrap();
        let oldstate = job.state.insert(client_id, IOState::InProgress);
        assert_eq!(oldstate, Some(IOState::New));
        job.work.clone()
    }

    /**
     * Return a list of request IDs that represent unissued requests for this
     * client.
     */
    fn new_work(&self, client_id: u8) -> Vec<u64> {
        self.active
            .values()
            .filter_map(|job| {
                if let Some(IOState::New) = job.state.get(&client_id) {
                    Some(job.request_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /**
     * Enqueue a new request.
     */
    fn enqueue(&mut self, io: IOWork) {
        self.active.insert(io.request_id, io);
    }

    /**
     * Mark this request as complete for this client.  Returns counts clients
     * for which this request is still active or has been completed already.
     */
    fn complete(&mut self, reqid: u64, client_id: u8) -> Result<WorkCounts> {
        let job = self
            .active
            .get_mut(&reqid)
            .ok_or_else(|| anyhow!("reqid {} is not active", reqid))?;
        let oldstate = job.state.insert(client_id, IOState::Done);
        assert_ne!(oldstate, Some(IOState::Done));

        /*
         * Check to see if all I/O is completed:
         */
        let mut wc: WorkCounts = Default::default();
        for state in job.state.values() {
            match state {
                IOState::New | IOState::InProgress => wc.active += 1,
                IOState::Done | IOState::Skipped | IOState::Error => {
                    wc.done += 1;
                }
            }
        }

        Ok(wc)
    }

    /**
     * This request is now complete on all peers.  Remove it from the active set
     * and mark it in the completed ring buffer.
     */
    fn retire(&mut self, reqid: u64) {
        assert!(!self.completed.contains(&reqid));
        let old = self.active.remove(&reqid);
        assert!(old.is_some());
        self.completed.push(reqid);
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
    work: Mutex<Work>,
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
 * Inspired from Propolis block.rs
 *
 * The following are the operations that Crucible supports from outside callers.
 * We have extended this to cover a bunch of test operations as well.
 * The first three are the supported operations, the other operations
 * tell the upstaris to behave in specific ways.
 */
#[derive(Copy, Clone, Debug)]
pub enum BlockOp {
    Read,
    Write,
    Flush,
    // Begin testing options.
    ReadStep, // Put a read operaion on the upstairs internal work hashmap.
    WriteStep, // Put a write operaion on the upstairs internal work hashmap.
    FlushStep, // Put a flush operaion on the upstairs internal work hashmap.
    Commit,   // Send update to all tasks that there is work on the queue.
    ShowWork, // Show the status of the internal work hashmap and done Vec.
}

/*
 * This is the structure we use to pass work from outside Crucible into the upstairs
 * main task.
 */
#[derive(Debug)]
struct PropWork {
    reqs: Mutex<VecDeque<BlockOp>>,
    notify: Notify,
}

/*
 * These methods are how to add or checking for new work on the PropWork struct
 */
impl PropWork {
    pub fn new() -> PropWork {
        PropWork {
            reqs: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }
    pub fn send(&self, req: BlockOp) {
        self.reqs.lock().unwrap().push_back(req);

        self.notify.notify_one();
    }

    pub async fn recv(&self) -> BlockOp {
        loop {
            if let Some(req) = self.reqs.lock().unwrap().pop_front() {
                return req;
            }
            self.notify.notified().await;
        }
    }
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
        .worker_threads(10)
        .thread_name("crucible-tokio")
        .enable_all()
        .build()
        .unwrap();

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     */
    let prop_work = Arc::new(PropWork::new());

    runtime.spawn(up_main(opt, prop_work.clone()));
    println!("runtime is spawned");

    /*
     * Create the interactive input scope that will generate and send
     * work to the Crucible thread that listens to work from outside (Propolis).
     */
    runtime.spawn(run_scope(prop_work));

    loop {
        /*
         * Sleep forever to avoid spinning in this thread.
         */
        std::thread::sleep(std::time::Duration::from_secs(86400));
    }
}

/*
 * Send work to all the targets on this vector.
 * This can be much simpler, but we need to (eventually) take special action
 * when we fail to send a message to a task.
 */
fn _send_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        println!("#### send to client {:?}", d_client.target);
        let res = d_client.input.send(val);
        if let Err(e) = res {
            println!(
                "#### error {:#?} sending work to {:?}",
                e, d_client.target
            );
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
 * This is basically just a test loop that generates a workload then sends the workload
 * to Crucible.
 */
async fn run_scope(pw: Arc<PropWork>) -> Result<()> {
    let scope =
        crucible_scope::Server::new(".scope.upstairs.sock", "upstairs").await?;
    loop {
        scope.wait_for("create write work, put on work queue").await;
        pw.send(BlockOp::WriteStep);
        scope.wait_for("read step").await;
        pw.send(BlockOp::ReadStep);
        scope.wait_for("flush step").await;
        pw.send(BlockOp::FlushStep);
        scope.wait_for("show step").await;
        pw.send(BlockOp::ShowWork);
        scope.wait_for("commit step").await;
        pw.send(BlockOp::Commit);
        scope.wait_for("show step").await;
        pw.send(BlockOp::ShowWork);
        scope.wait_for("send write ").await;
        pw.send(BlockOp::Write);
        scope.wait_for("send read ").await;
        pw.send(BlockOp::Read);
        scope.wait_for("send read ").await;
        pw.send(BlockOp::Read);
        scope.wait_for("send read ").await;
        pw.send(BlockOp::Read);
        scope.wait_for("show step").await;
        pw.send(BlockOp::ShowWork);
    }
}

/*
 * This task will loop forever and watch the PropWork structure for new IO operations
 * showing up.  When one is detected, the type is checked and the operation is translated
 * into the corresponding upstairs IO type and put on the internal upstairs queue.
 */
async fn up_listen(up: &Arc<Upstairs>, pw: Arc<PropWork>, dst: Vec<Target>) {
    /*
     * XXX Once we move this function to being called after all downstairs are
     * online, we can remove this sleep
     */
    tokio::time::sleep(Duration::from_secs(1)).await;

    /*
     * XXX At the moment we are making up the actual work that upstairs does.  We use the
     * command coming in as a starting point, but then provide our own values to create a
     * real IOop.  Eventually (soon dammit) this should change to actually sending the values
     * that we will (eventually) be receiving from outside instead of making our own.
     */
    let mut lastcast = 1;
    let mut eid = 0;
    let mut block_offset = 0;
    loop {
        let req = pw.recv().await;
        println!("UMT received {:#?}", req);
        match req {
            BlockOp::Read => {
                let mut work = up.work.lock().unwrap();
                let re = create_read(work.next_id(), eid, block_offset);
                work.enqueue(re);
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::Write => {
                let mut work = up.work.lock().unwrap();
                let wr = create_write(work.next_id(), eid, block_offset, 0x55);
                work.enqueue(wr);
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::Flush => {
                let mut work = up.work.lock().unwrap();
                let ver = up.versions.lock().unwrap();
                let fl = create_flush(work.next_id(), ver.clone());
                work.enqueue(fl);
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
            BlockOp::WriteStep => {
                let mut work = up.work.lock().unwrap();
                let wr = create_write(work.next_id(), eid, block_offset, 0x55);
                work.enqueue(wr);
            }
            BlockOp::ReadStep => {
                let mut work = up.work.lock().unwrap();
                let re = create_read(work.next_id(), eid, block_offset);
                work.enqueue(re);
            }
            BlockOp::FlushStep => {
                let mut work = up.work.lock().unwrap();
                let ver = up.versions.lock().unwrap();
                let fl = create_flush(work.next_id(), ver.clone());
                work.enqueue(fl);
            }
            BlockOp::ShowWork => {
                show_work(&up);
            }
            BlockOp::Commit => {
                dst.iter().for_each(|t| t.input.send(lastcast).unwrap());
                lastcast += 1;
            }
        }
        eid = (eid + 1) % 10;
        block_offset = (block_offset + 1) % 100;
    }
}

/*
 * This is the main upstairs task that is responsible for accepting
 * work from propolis (or whomever) and taking that work and converting
 * it into a crucible IO, then sending it on to be processed to the mux
 * portion of crucible.
 */
async fn up_main(opt: Opt, pw: Arc<PropWork>) -> Result<()> {

    let up = Arc::new(Upstairs {
        work: Mutex::new(Work {
            active: HashMap::new(),
            completed: AllocRingBuffer::with_capacity(2048),
            next_id: 1000,
        }),
        versions: Mutex::new(Vec::new()),
        dirty: Mutex::new(Vec::new()),
        ddef: Mutex::new(DiskDefinition::default()),
        downstairs: Mutex::new(Vec::with_capacity(opt.target.len())),
    });

    /*
     * Use this channel to receive updates on target status from each task
     * we create to connect to a downstairs.
     */
    let (ctx, mut crx) = mpsc::channel::<Condition>(32);

    let mut client_id = 0;
    /*
     * Create one downstaris task (dst) for each target in the opt
     * structure that was passed to us.
     */
    let dst = opt
        .target
        .iter()
        .map(|dst| {
            /*
             * Create the channel that we will use to request that the loop
             * check for work to do in the central structure.
             */
            let (itx, irx) = watch::channel(0);

            let up = Arc::clone(&up);
            let ctx = ctx.clone();
            let t0 = *dst;
            tokio::spawn(async move {
                looper(t0, irx, ctx, &up, client_id).await;
            });
            client_id += 1;

            Target {
                target: *dst,
                input: itx,
            }
        })
        .collect::<Vec<_>>();

    /*
     * Create a task to listen for work from outside.
     *
     * The role of this task will be to move work between the outside
     * work queue and the internal Upstairs work queue, as well as send
     * completion messages and/or copy data back to the outside.
     *
     * XXX This needs a little more work.  We should not start to listen
     * to the outside until we know that all our downstairs are ready to
     * take IO operations.
     */
    tokio::spawn(async move {
        up_listen(&up, pw, dst).await;
    });

    // async tasks need to tell us they are alive, but they also need to
    // tell us the extent list from any attached downstairs.
    // That part is not connected yet.
    let mut ds_count = 0u32;
    loop {
        let c = crx.recv().await.unwrap();
        if c.connected {
            ds_count += 1;
            println!(
                "#### {:?} #### CONNECTED ######## {}/{}",
                c.target,
                ds_count,
                opt.target.len()
            );
        } else {
            println!("#### {:?} #### DISCONNECTED! ####", c.target);
            ds_count -= 1;
        }
        /*
         * We need some additional way to indicate that this upstairs is ready
         * to receive work.  Just connecting to n downstairs is not enough,
         * we need to also know that they all have the same data.
         */
    }

    /*
     * XXX Need to cleanup async tasks and close connections.
     */
}

/*
 * A test function to create a write IOWork structure.
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

/*
 * A test function to create a flush IOWork structure.
 */
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

/*
 * A test function to create a read IOWork structure.
 */
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
#[allow(unused_variables)]
fn show_work(up: &Arc<Upstairs>) {
    let mut work = up.work.lock().unwrap();
    for (id, job) in work.active.iter_mut() {
        let job_type = match &job.work {
            IOop::Read { eid, block_offset } => "Read ".to_string(),
            IOop::Write {
                dependencies,
                eid,
                block_offset,
                data,
            } => "Flush".to_string(),
            IOop::Flush {
                dependencies,
                flush_numbers,
            } => "Write".to_string(),
        };
        print!("JOB:[{:04}] {} ", id, job_type);
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
    let done = work.completed.to_vec();
    println!("Done: {:?}", done);
}

fn _create_more_work(up: &Arc<Upstairs>) -> Result<()> {
    let mut seed = 1;

    for eid in 0..10 {
        seed += 1;
        for bo in 0..100 {
            let mut work = up.work.lock().unwrap();
            let wr = create_write(work.next_id(), eid, bo, seed);
            work.enqueue(wr);
        }
    }

    {
        let mut work = up.work.lock().unwrap();
        let ver = up.versions.lock().unwrap();
        let fl = create_flush(work.next_id(), ver.clone());
        work.enqueue(fl);
    }

    for eid in 0..10 {
        for bo in 0..100 {
            let mut work = up.work.lock().unwrap();
            let re = create_read(work.next_id(), eid, bo);
            work.enqueue(re);
        }
    }

    Ok(())
}

fn _create_work(
    up: &Arc<Upstairs>,
    seed: u8,
    eid: u64,
    block_offset: u64,
) -> Result<()> {
    let mut work = up.work.lock().unwrap();

    let wr = create_write(work.next_id(), eid, block_offset, seed);
    work.enqueue(wr);

    let ver = up.versions.lock().unwrap().clone();
    let fl = create_flush(work.next_id(), ver);
    work.enqueue(fl);

    let re = create_read(work.next_id(), eid, block_offset);
    work.enqueue(re);

    Ok(())
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
