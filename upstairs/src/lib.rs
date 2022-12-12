// Copyright 2022 Oxide Computer Company
#![cfg_attr(not(usdt_stable_asm), feature(asm))]
#![cfg_attr(
    all(target_os = "macos", not(usdt_stable_asm_sym)),
    feature(asm_sym)
)]
#![allow(clippy::mutex_atomic)]

use std::clone::Clone;
use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Result as IOResult, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub use crucible_client_types::{CrucibleOpts, VolumeConstructionRequest};
pub use crucible_common::*;
pub use crucible_protocol::*;

use anyhow::{anyhow, bail, Result};
pub use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use oximeter::types::ProducerRegistry;
use rand::prelude::*;
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, warn, Drain, Logger};
use slog_dtrace::{with_drain, ProbeRegistration};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, watch, Mutex, MutexGuard, Notify, RwLock};
use tokio::time::{sleep_until, Instant};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{instrument, span, Level};
use usdt::register_probes;
use uuid::Uuid;

use aes_gcm_siv::aead::{AeadInPlace, NewAead};
use aes_gcm_siv::{Aes256GcmSiv, Key, Nonce, Tag};
use rand_chacha::ChaCha20Rng;

pub mod control;
mod pseudo_file;
mod test;

pub mod volume;
pub use volume::Volume;

pub mod in_memory;
pub use in_memory::InMemoryBlockIO;

pub mod block_io;
pub use block_io::{FileBlockIO, ReqwestBlockIO};

pub mod block_req;
pub(crate) use block_req::{BlockReq, BlockReqWaiter};

mod mend;
pub use mend::{DownstairsMend, ExtentFix, RegionMetadata};
pub use pseudo_file::CruciblePseudoFile;

mod stats;
pub use stats::*;

mod impacted_blocks;
pub use impacted_blocks::*;

use async_trait::async_trait;

/// The BlockIO trait behaves like a physical NVMe disk (or a virtio virtual
/// disk): there is no contract about what order operations that are submitted
/// between flushes are performed in.
#[async_trait]
pub trait BlockIO: Sync {
    async fn activate(&self) -> Result<(), CrucibleError>;

    async fn deactivate(&self) -> Result<(), CrucibleError>;

    async fn query_is_active(&self) -> Result<bool, CrucibleError>;

    // Total bytes of Volume
    async fn total_size(&self) -> Result<u64, CrucibleError>;

    /// Return the block size - this should never change at runtime!
    async fn get_block_size(&self) -> Result<u64, CrucibleError>;

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError>;

    /*
     * `read`, `write`, and `write_unwritten` accept a block offset, and data
     * buffer size must be a multiple of block size.
     */

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<(), CrucibleError>;

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError>;

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError>;

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError>;

    /// Test call that displays the internal job queue on the upstairs, and
    /// returns the guest side and downstairs side job queue depths.
    async fn show_work(&self) -> Result<WQCounts, CrucibleError>;

    // Common methods for BlockIO

    async fn byte_offset_to_block(
        &self,
        offset: u64,
    ) -> Result<Block, CrucibleError> {
        let bs = self.get_block_size().await?;

        if (offset % bs) != 0 {
            crucible_bail!(OffsetUnaligned);
        }

        Ok(Block::new(offset / bs, bs.trailing_zeros()))
    }

    /*
     * `read_from_byte_offset` and `write_to_byte_offset` accept a byte
     * offset, and data must be a multiple of block size.
     */

    async fn read_from_byte_offset(
        &self,
        offset: u64,
        data: Buffer,
    ) -> Result<(), CrucibleError> {
        if !self.query_is_active().await? {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.read(self.byte_offset_to_block(offset).await?, data)
            .await
    }

    async fn write_to_byte_offset(
        &self,
        offset: u64,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        if !self.query_is_active().await? {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.write(self.byte_offset_to_block(offset).await?, data)
            .await
    }

    /// Activate if not active.
    async fn conditional_activate(&self) -> Result<(), CrucibleError> {
        if self.query_is_active().await? {
            return Ok(());
        }

        self.activate().await
    }
}

pub type CrucibleBlockIOFuture<'a> = Pin<
    Box<
        dyn futures::Future<Output = Result<(), CrucibleError>>
            + std::marker::Send
            + 'a,
    >,
>;

/// Await on the results of multiple BlockIO operations
///
/// Using [async_trait] with the BlockIO trait will perform Box::pin on the
/// result of the async operation functions. `join_all` is provided here to
/// consume a list of multiple BlockIO operations' futures and await them all.
#[inline]
pub async fn join_all<'a>(
    iter: impl IntoIterator<Item = CrucibleBlockIOFuture<'a>>,
) -> Result<(), CrucibleError> {
    futures::future::join_all(iter)
        .await
        .into_iter()
        .collect::<Result<Vec<()>, CrucibleError>>()
        .map(|_| ())
}

/// DTrace probes in the upstairs
///
/// up__status: This tracks the state of each of the three downstairs
/// as well as the work queue counts for the upstairs work queue and the
/// downstairs work queue.
///
/// For each read/write/flush, we have a DTrace probe at specific
/// points throughout its path through the upstairs.  Below is the basic
/// order of probes an IO will hit as it works its way through the
/// system.
///
/// volume__*__start: This is when the volume layer has received an
/// IO request and has started work on it.
///
/// gw__*__start: This is when the upstairs has taken work from the
/// `guest` structure and created a new `gw_id` used to track this IO
/// through the system.  At the point of this probe, we have already
/// taken two locks, so it's not the very beginning of an IO, but it is
/// as close as we get after the `gw_id` is created.
///
/// up__to__ds_*_start: (Upstairs__to__Downstairs) At this point we have
/// created the structures to track this IO through the Upstairs and added
/// it to internal work queues, including the work queue for the three
/// downstairs tasks that are responsible for sending IO to each
/// downstairs.  This probe firing does not mean that a downstairs task
/// has received or is acting on the IO yet, it just means the notification
/// has been sent.
///
/// ds__*__io__start: This is when a downstairs task puts an IO on the
/// wire to the actual downstairs that will do the work. This probe has
/// both the job ID and the client ID so we can tell the individual
/// downstairs apart.
///
/// ds__*__io_done: An ACK has been received from a downstairs for an IO
/// sent to it. At the point of this probe the IO has just come off the
/// wire and we have not processed it yet.
///
/// up__to__ds__*__done: (Upstairs__to__Downstairs) This is the point where
/// the upstairs has decided that it has enough data to complete an IO
/// and send an ACK back to the guest.  For a read, this could be the the
/// first IO to respond from the downstairs.  For a write/flush, we have
/// two downstairs that have ACK'd the IO.
///
/// gw__*__done: An IO is completed and the Upstairs has sent the
/// completion notice to the guest.
///
/// reqwest__read__[start|done] a probe covering BlockIO reqwest read
/// requests. These happen if a volume has a read only parent and either
/// there is no sub volume, or the sub volume did not contain any data.
///
/// volume__*__done: An IO is completed at the volume layer.
#[usdt::provider(provider = "crucible_upstairs")]
mod cdt {
    use crate::Arg;
    fn up__status(_: String, arg: Arg) {}
    fn volume__read__start(_: u32, _: Uuid) {}
    fn volume__write__start(_: u32, _: Uuid) {}
    fn volume__writeunwritten__start(_: u32, _: Uuid) {}
    fn volume__flush__start(_: u32, _: Uuid) {}
    fn gw__read__start(_: u64) {}
    fn gw__write__start(_: u64) {}
    fn gw__write__unwritten__start(_: u64) {}
    fn gw__flush__start(_: u64) {}
    fn up__to__ds__read__start(_: u64) {}
    fn up__to__ds__write__start(_: u64) {}
    fn up__to__ds__write__unwritten__start(_: u64) {}
    fn up__to__ds__flush__start(_: u64) {}
    fn ds__read__io__start(_: u64, _: u64) {}
    fn ds__write__io__start(_: u64, _: u64) {}
    fn ds__write__unwritten__io__start(_: u64, _: u64) {}
    fn ds__flush__io__start(_: u64, _: u64) {}
    fn ds__read__io__done(_: u64, _: u64) {}
    fn ds__write__io__done(_: u64, _: u64) {}
    fn ds__write__unwritten__io__done(_: u64, _: u64) {}
    fn ds__flush__io__done(_: u64, _: u64) {}
    fn up__to__ds__read__done(_: u64) {}
    fn up__to__ds__write__done(_: u64) {}
    fn up__to__ds__write__unwritten__done(_: u64) {}
    fn up__to__ds__flush__done(_: u64) {}
    fn gw__read__done(_: u64) {}
    fn gw__write__done(_: u64) {}
    fn gw__write__unwritten__done(_: u64) {}
    fn gw__flush__done(_: u64) {}
    fn reqwest__read__start(_: u32, _: Uuid) {}
    fn reqwest__read__done(_: u32, _: Uuid) {}
    fn volume__read__done(_: u32, _: Uuid) {}
    fn volume__write__done(_: u32, _: Uuid) {}
    fn volume__writeunwritten__done(_: u32, _: Uuid) {}
    fn volume__flush__done(_: u32, _: Uuid) {}
}

pub fn deadline_secs(secs: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

#[instrument]
async fn process_message(
    u: &Arc<Upstairs>,
    m: &Message,
    up_coms: UpComs,
) -> Result<()> {
    let (upstairs_id, session_id, ds_id, result) = match m {
        Message::Imok => return Ok(()),
        Message::WriteAck {
            upstairs_id,
            session_id,
            job_id,
            result,
        } => {
            cdt::ds__write__io__done!(|| (job_id, up_coms.client_id as u64));
            (
                *upstairs_id,
                *session_id,
                *job_id,
                result.clone().map(|_| Vec::new()),
            )
        }
        Message::WriteUnwrittenAck {
            upstairs_id,
            session_id,
            job_id,
            result,
        } => {
            cdt::ds__write__unwritten__io__done!(|| (
                job_id,
                up_coms.client_id as u64
            ));
            (
                *upstairs_id,
                *session_id,
                *job_id,
                result.clone().map(|_| Vec::new()),
            )
        }
        Message::FlushAck {
            upstairs_id,
            session_id,
            job_id,
            result,
        } => {
            cdt::ds__flush__io__done!(|| (job_id, up_coms.client_id as u64));
            (
                *upstairs_id,
                *session_id,
                *job_id,
                result.clone().map(|_| Vec::new()),
            )
        }
        Message::ReadResponse {
            upstairs_id,
            session_id,
            job_id,
            responses,
        } => {
            cdt::ds__read__io__done!(|| (job_id, up_coms.client_id as u64));
            (*upstairs_id, *session_id, *job_id, responses.clone())
        }
        /*
         * For this case, we will (TODO) want to log an error to someone, but
         * I don't think there is anything else we can do.
         */
        x => {
            warn!(
                u.log,
                "{} unexpected frame {:?}, IGNORED", up_coms.client_id, x
            );
            return Ok(());
        }
    };

    if u.uuid != upstairs_id {
        warn!(
            u.log,
            "[{}] u.uuid {:?} != job {} upstairs_id {:?}!",
            up_coms.client_id,
            u.uuid,
            ds_id,
            upstairs_id,
        );

        return Err(CrucibleError::UuidMismatch.into());
    }

    if u.session_id != session_id {
        warn!(
            u.log,
            "[{}] u.session_id {:?} != job {} session_id {:?}!",
            up_coms.client_id,
            u.session_id,
            ds_id,
            session_id,
        );

        return Err(CrucibleError::UuidMismatch.into());
    }

    // Process this operation.  If the processing results in the
    // job being ready to ACK, then send a message on the ds_done
    // channel.  Note that a failed IO still needs to ACK that failure
    // back to the guest.
    if u.process_ds_operation(ds_id, up_coms.client_id, result)
        .await?
    {
        up_coms.ds_done_tx.send(ds_id).await?;
    }

    Ok(())
}

/*
 * This function is called by a worker task after the main task has added
 * work to the hashmap and notified the worker tasks that new work is ready
 * to be serviced. The worker task will walk the hashmap and build a list
 * of new work that it needs to do. It will then iterate through those
 * work items and send them over the wire to this tasks waiting downstairs.
 *
 * V1 flow control, if we have more than X (where X = 100 for now, as we
 * don't know the best value yet, XXX) jobs submitted that we don't have
 * ACKs for, then stop sending more work and let the receive side catch up.
 * We return true if we have more work to do, false if we are all caught up.
 */
#[instrument(skip(fw))]
async fn io_send<WT>(
    u: &Arc<Upstairs>,
    fw: &mut FramedWrite<WT, CrucibleEncoder>,
    client_id: u8,
) -> Result<bool>
where
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    /*
     * Build ourselves a list of all the jobs on the work hashmap that
     * have the job state for our client id in the IOState::New
     *
     * The length of this list (new work for a downstairs) can give us
     * an idea of how that downstairs is doing. If the number of jobs
     * to be submitted is too big (for some value of big) then there is
     * a problem. All sorts of back pressure information can be
     * gathered here. As (for the moment) the same task does both
     * transmit and receive, we can starve the receive side by spending
     * all our time sending work.
     *
     * This XXX is for coming back here and making a better job of
     * flow control.
     */
    let mut new_work = u.downstairs.lock().await.new_work(client_id);

    /*
     * Now we have a list of all the job IDs that are new for our client id.
     * Walk this list and process each job, marking it InProgress as we
     * do the work. We do this in two loops because we can't hold the
     * lock for the hashmap while we do work, and if we release the lock
     * to do work, we would have to start over and look at all jobs in the
     * map to see if they are new.
     *
     * This also allows us to sort the job ids and do them in order they
     * were put into the hashmap, though I don't think that is required.
     */
    new_work.sort_unstable();

    let mut active_count = u.downstairs.lock().await.submitted_work(client_id);
    for new_id in new_work.iter() {
        if active_count >= 100 {
            // Flow control enacted, stop sending work
            return Ok(true);
        }
        /*
         * Walk the list of work to do, update its status as in progress
         * and send the details to our downstairs.
         */
        if u.lossy && random() && random() {
            continue;
        }

        /*
         * If in_progress returns None, it means that this client should
         * be skipped.
         */
        let job = u.downstairs.lock().await.in_progress(*new_id, client_id);
        if job.is_none() {
            continue;
        }

        active_count += 1;
        match job.unwrap() {
            IOop::Write {
                dependencies,
                writes,
            } => {
                cdt::ds__write__io__start!(|| (*new_id, client_id as u64));
                fw.send(Message::Write {
                    upstairs_id: u.uuid,
                    session_id: u.session_id,
                    job_id: *new_id,
                    dependencies: dependencies.clone(),
                    writes: writes.clone(),
                })
                .await?
            }
            IOop::WriteUnwritten {
                dependencies,
                writes,
            } => {
                cdt::ds__write__unwritten__io__start!(|| (
                    *new_id,
                    client_id as u64
                ));
                fw.send(Message::WriteUnwritten {
                    upstairs_id: u.uuid,
                    session_id: u.session_id,
                    job_id: *new_id,
                    dependencies: dependencies.clone(),
                    writes: writes.clone(),
                })
                .await?
            }
            IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
            } => {
                cdt::ds__flush__io__start!(|| (*new_id, client_id as u64));
                fw.send(Message::Flush {
                    upstairs_id: u.uuid,
                    session_id: u.session_id,
                    job_id: *new_id,
                    dependencies: dependencies.clone(),
                    flush_number,
                    gen_number,
                    snapshot_details,
                })
                .await?
            }
            IOop::Read {
                dependencies,
                requests,
            } => {
                cdt::ds__read__io__start!(|| (*new_id, client_id as u64));
                fw.send(Message::ReadRequest {
                    upstairs_id: u.uuid,
                    session_id: u.session_id,
                    job_id: *new_id,
                    dependencies: dependencies.clone(),
                    requests,
                })
                .await?
            }
        }
    }
    Ok(false)
}

async fn proc_stream(
    target: &SocketAddr,
    up: &Arc<Upstairs>,
    stream: WrappedStream,
    connected: &mut bool,
    up_coms: &mut UpComs,
) -> Result<()> {
    match stream {
        WrappedStream::Http(sock) => {
            let (read, write) = sock.into_split();

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            proc(target, up, fr, fw, connected, up_coms).await
        }
        WrappedStream::Https(stream) => {
            let (read, write) = tokio::io::split(stream);

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = FramedWrite::new(write, CrucibleEncoder::new());

            proc(target, up, fr, fw, connected, up_coms).await
        }
    }
}

/*
 * Once we have a connection to a downstairs, this task takes over and
 * handles the initial negotiation.
 *
 * The DsState of this downstairs when it leaves this function will
 * determine if it goes into repair mode, or goes straight to receiving
 * IO from the guest.
 */
async fn proc<RT, WT>(
    target: &SocketAddr,
    up: &Arc<Upstairs>,
    mut fr: FramedRead<RT, CrucibleDecoder>,
    mut fw: FramedWrite<WT, CrucibleEncoder>,
    connected: &mut bool,
    up_coms: &mut UpComs,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    // Clear this Downstair's repair address, and let the YesItsMe set it. This
    // works if this Downstairs is new, reconnecting, or was replaced entirely -
    // the repair address could have changed in any of these cases.
    up.ds_clear_repair_address(up_coms.client_id).await;

    // If this Downstairs is returning from being disconnected, we need to call
    // re_new.
    {
        let mut ds = up.downstairs.lock().await;
        let my_state = ds.ds_state[up_coms.client_id as usize];

        info!(
            up.log,
            "[{}] Proc runs for {} in state {:?}",
            up_coms.client_id,
            target,
            my_state,
        );

        // XXX Move this all to some state check place?
        if my_state != DsState::New
            && my_state != DsState::Disconnected
            && my_state != DsState::Failed
            && my_state != DsState::Offline
        {
            panic!(
                "[{}] failed proc with state {:?}",
                up_coms.client_id, my_state
            );
        }

        /*
         * This is only applicable for a downstairs that is returning from
         * being disconnected. Mark any in progress jobs since the
         * last good flush back to New, as we have reconnected to
         * this downstairs and will need to replay any work that we
         * were holding that we did not flush.
         */
        if my_state == DsState::Offline {
            ds.re_new(up_coms.client_id);
        }
    }

    let mut self_promotion = false;

    /*
     * As the "client", we must begin the negotiation.
     */
    let m = Message::HereIAm {
        version: 1,
        upstairs_id: up.uuid,
        session_id: up.session_id,
        gen: up.get_generation().await,
        read_only: up.read_only,
        encrypted: up.encrypted(),
    };
    fw.send(m).await?;

    /*
     * Used to track where we are in the current negotiation.
     */
    let mut negotiated = 0;

    // XXX figure out what deadlines make sense here
    let mut ping_interval = deadline_secs(5);
    let mut timeout_deadline = deadline_secs(50);

    /*
     * Either we get all the way through the negotiation, or we hit the
     * timeout and exit to retry.
     *
     * XXX There are many ways we can handle this, but as we figure out
     * how the upstairs is notified that a DS is new or moving, or other
     * things, this way will work. We will revisit when we have more info.
     *
     * The negotiation flow starts as follows, with the value of the
     * negotiated variable on the left:
     *
     *          Upstairs             Downstairs
     * 0:        HereIAm(...)  --->
     *                         <---  YesItsMe(...)
     *
     * At this point, a downstairs will wait for a "PromoteToActive" message
     * to be sent to it.  If this is a new upstairs that has not yet
     * connected to a downstairs, then we will wait for the guest to send
     * us this message and pass it down to the downstairs.  If a downstairs
     * is reconnecting after having already been active, then we look at our
     * upstairs guest_io_ready() and, if the upstairs is ready, we send the
     * downstairs the message ourselves that they should promote to active.
     *
     * 1: PromoteToActive(uuid)--->
     *                         <---  YouAreNowActive(uuid)
     *
     * 2:    RegionInfoPlease  --->
     *                         <---  RegionInfo(r)
     *
     *    At this point the upstairs looks to see what state the downstairs
     *    is currently in.  It will be "New", "Disconnected" or "Offline".
     *
     *    For "New" or "Disconnected" it means this downstairs never was
     *    "Active" and we have to go through the full compare of this
     *    downstairs with other downstairs and make sure they are
     *    consistent (This code still TBW).  The New/Disconnected steps
     *    continue here:
     *
     *          Upstairs             Downstairs
     * 4: ExtentVersionsPlease --->
     *                         <---  ExtentVersions(g, v, d)
     *
     *    Now with the extent info, Upstairs calls process_downstairs() and
     *    if no problems, sends connected=true to the up_listen() task,
     *    we set the downstairs to DsState::WaitQuorum and we exit the
     *    while loop.
     *
     *    For the "Offline" state, the downstairs was connected and verified
     *    and after that point the connection was lost.  To handle this
     *    condition we follow these final steps to get this downstairs
     *    working again:
     *
     *          Upstairs             Downstairs
     * 3:       LastFlush(lf)) --->
     *                         <---  LastFlushAck(lf)
     *
     * 5: Now the downstairs is ready to receive replay IOs from the
     *    upstairs. We set the downstairs to DsState::Replay and the while
     *    loop is exited.
     */
    while !(*connected) {
        tokio::select! {
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * XXX Timeouts, timeouts: always wrong! Some too short and some
             * too long.
             * TODO: 50 is too long, but what is the correct value?
             */
            _ = sleep_until(timeout_deadline) => {
                bail!("timed out during negotiation");
            }
            _ = sleep_until(ping_interval) => {
                fw.send(Message::Ruok).await?;
                ping_interval = deadline_secs(5);
            }
            r = up_coms.ds_active_rx.changed(),
                if negotiated == 1 && !self_promotion =>
            {
                match r {
                    Ok(_) => {
                        let gen = up_coms.ds_active_rx.borrow();
                        info!(up.log, "[{}] received activate with gen {:?}",
                            up_coms.client_id, *gen);
                    }
                    Err(e) => {
                        error!(up.log, "[{}] received activate error {:?}",
                            up_coms.client_id, e);
                    }
                }
                /*
                 * Promote self to active when message arrives from the Guest.
                 *
                 * This check must only be done when the proper
                 * negotiation step is reached.  If we check too soon, then
                 * we can be out of order.  We also might have self promoted
                 * if the upstairs has already received the request to
                 * activate and this downstairs was not connected at that
                 * time.
                 */
                info!(
                    up.log,
                    "[{}] client got ds_active_rx, promote! session {}",
                    up_coms.client_id,
                    up.session_id,
                );
                self_promotion = true;
                fw.send(Message::PromoteToActive {
                    upstairs_id: up.uuid,
                    session_id: up.session_id,
                    gen: up.get_generation().await,
                }).await?;
            }
            f = fr.next() => {
                // When the downstairs responds, push the deadlines
                timeout_deadline = deadline_secs(50);
                ping_interval = deadline_secs(5);

                match f.transpose()? {
                    None => {
                        // Downstairs disconnected
                        info!(up.log, "[{}] client hung up", up_coms.client_id);
                        return Ok(())
                    }
                    Some(Message::Imok) => {}
                    Some(Message::ReadOnlyMismatch { expected }) => {
                        // Upstairs will never be able to connect, bail
                        bail!(
                            "downstairs read_only is {}, ours is {}!",
                            expected,
                            up.read_only,
                        );
                    }
                    Some(Message::EncryptedMismatch { expected }) => {
                        // Upstairs will never be able to connect, bail
                        bail!(
                            "downstairs encrypted is {}, ours is {}!",
                            expected,
                            up.encrypted(),
                        );
                    }
                    Some(Message::YesItsMe { version, repair_addr }) => {
                        if negotiated != 0 {
                            bail!("Got version already!");
                        }

                        /*
                         * XXX Valid version to compare with should come
                         * from main task. In the future we will also have
                         * to handle a version mismatch.
                         */
                        if version != 1 {
                            up.ds_transition(
                                up_coms.client_id,
                                DsState::BadVersion
                            ).await;
                            bail!("expected version 1, got {}", version);
                        }

                        negotiated = 1;

                        up.ds_set_repair_address(
                            up_coms.client_id, repair_addr,
                        ).await;

                        /*
                         * We only set guest_io_ready after all three downstairs
                         * have gone active, which means the upstairs did
                         * received a request to go active. Since we won't be
                         * getting another request, we can self promote.
                         */
                        if up.guest_io_ready().await {
                            /*
                             * This could be either a reconnect, or a
                             * downstairs that totally failed and now has to
                             * start over and reconcile again.
                             */
                            info!(
                                up.log,
                                "[{}] upstairs guest_io_ready=TRUE, promote! session {}",
                                up_coms.client_id,
                                up.session_id,
                            );
                            self_promotion = true;
                            fw.send(Message::PromoteToActive {
                                upstairs_id: up.uuid,
                                session_id: up.session_id,
                                gen: up.get_generation().await,
                            }).await?;
                        } else {
                            /*
                             * Transition this Downstairs to WaitActive
                             */
                            up.ds_transition(
                                up_coms.client_id,
                                DsState::WaitActive,
                            ).await;

                            /*
                             * If the guest already requested activate, but
                             * the downstairs went away and then came back, we
                             * can resend the activate ourselves.  We set a
                             * local self_promote so we don't send the
                             * promote to active twice.
                             */
                            if up.is_active_requested().await {
                                info!(
                                    up.log,
                                    "[{}] client is_active_req TRUE, promote! session {}",
                                    up_coms.client_id,
                                    up.session_id,
                                );
                                /*
                                 * If there is anything in the ds_active_rx
                                 * channel, clear it out so we don't later
                                 * check it and confuse it for a new request
                                 */
                                {
                                    up_coms.ds_active_rx.borrow_and_update();
                                }
                                self_promotion = true;
                                fw.send(Message::PromoteToActive {
                                    upstairs_id: up.uuid,
                                    session_id: up.session_id,
                                    gen: up.get_generation().await,
                                }).await?;
                            }
                        }
                    }
                    Some(Message::YouAreNowActive {
                        upstairs_id,
                        session_id,
                        gen,
                    }) => {
                        let match_uuid = up.uuid == upstairs_id;
                        let match_session = up.session_id == session_id;
                        let match_gen = up.get_generation().await == gen;
                        let matches_self =
                            match_uuid && match_session && match_gen;

                        if !matches_self {
                            warn!(
                                up.log,
                                "[{}] YouAreNowActive didn't match self! {} {} {}",
                                up_coms.client_id,
                                if !match_uuid {
                                    format!(
                                        "UUID {:?} != {:?}",
                                        up.uuid, upstairs_id
                                    )
                                } else {
                                    String::new()
                                },
                                if !match_session {
                                    format!(
                                        "session {:?} != {:?}",
                                        up.session_id, session_id
                                    )
                                } else {
                                    String::new()
                                },
                                if !match_gen {
                                    format!(
                                        "gen {:?} != {:?}",
                                        up.get_generation().await,
                                        gen
                                    )
                                } else {
                                    String::new()
                                },
                            );

                            up.ds_transition(
                                up_coms.client_id, DsState::New
                            ).await;
                            if !match_gen {
                                let gen_error = format!(
                                    "Generation requested:{} found:{}",
                                    gen, up.get_generation().await
                                );
                                up.set_inactive(
                                    CrucibleError::GenerationNumberTooLow(
                                        gen_error.clone()
                                    )
                                )
                                .await;
                                return Err(
                                    CrucibleError::GenerationNumberTooLow(
                                        gen_error
                                    )
                                    .into()
                                );
                            } else {
                                up.set_inactive(
                                    CrucibleError::UuidMismatch
                                )
                                .await;
                                return Err(CrucibleError::UuidMismatch.into());
                            }
                        }

                        if negotiated != 1 {
                            bail!(
                                "Received YouAreNowActive out of order! {}",
                                negotiated
                            );
                        }
                        negotiated = 2;
                        fw.send(Message::RegionInfoPlease).await?;

                    }
                    Some(Message::YouAreNoLongerActive {
                        new_upstairs_id,
                        new_session_id,
                        new_gen,
                    }) => {
                        warn!(
                            up.log,
                            "[{}] {} ({}) proc saw YouAreNoLongerActive {:?} {:?} {}",
                            up_coms.client_id,
                            up.uuid,
                            up.session_id,
                            new_upstairs_id,
                            new_session_id,
                            new_gen,
                        );

                        up.ds_transition(up_coms.client_id, DsState::New).await;
                        up.set_inactive(CrucibleError::NoLongerActive).await;

                        // What if the newly active upstairs has the same UUID?
                        if up.uuid == new_upstairs_id {
                            if new_gen > up.get_generation().await {
                                // The next generation of this Upstairs
                                // connected, bail - this generation won't be
                                // able to connect again.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the newly active Upstairs, which shares
                            // our UUID. We shouldn't have received this
                            // message. The downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, same \
                                upstairs uuid and our gen {} >= new gen {}!",
                                up_coms.client_id,
                                up.uuid,
                                up.get_generation().await,
                                new_gen,
                            );
                        } else {
                            // A new upstairs connected
                            if new_gen > up.get_generation().await {
                                // The next generation of another Upstairs
                                // connected.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the old one, and it's a new Upstairs. We
                            // shouldn't have received this message. The
                            // downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, different \
                                upstairs uuid {:?} and our gen {} >= new gen \
                                {}!",
                                up_coms.client_id,
                                up.uuid,
                                new_upstairs_id,
                                up.get_generation().await,
                                new_gen,
                            );
                        }

                    }
                    Some(Message::RegionInfo { region_def }) => {
                        if negotiated != 2 {
                            bail!("Received RegionInfo out of order!");
                        }
                        info!(up.log, "[{}] downstairs client at {} has UUID {}",
                            up_coms.client_id, target, region_def.uuid(),
                        );

                        up.add_ds_region(up_coms.client_id, region_def).await?;

                        let my_state = {
                            let state = &up.downstairs.lock().await.ds_state;
                            state[up_coms.client_id as usize]
                        };
                        if my_state == DsState::Offline {
                            /*
                             * If we are coming from state Offline, then it
                             * means the downstairs has departed then came
                             * back in short enough time that it does not
                             * have to go into full recovery/repair mode.
                             * If we have verified that the UUID and region
                             * info is the same, we can reconnect and let
                             * any outstanding work be replayed to catch
                             * us up.  We do need to tell the downstairs
                             * the last flush ID it had ACKd to us.
                             */
                            let lf = up.last_flush_id(up_coms.client_id).await;
                            info!(
                                up.log,
                                "[{}] send last flush ID to this DS: {}",
                                up_coms.client_id, lf);
                            negotiated = 3;
                            fw.send(Message::LastFlush { last_flush_number: lf }).await?;

                        } else if my_state == DsState::WaitActive {
                            /*
                             * Ask for the current version of all extents.
                             */
                            negotiated = 4;
                            fw.send(Message::ExtentVersionsPlease).await?;

                        } else {
                            /*
                             * TODO: This is the case where a downstairs
                             * failed and was removed and has re-joined
                             * (Hopefully fixed now).  To bring this back,
                             * we need to write code to support combining
                             * a "new" downstairs with two running downstairs
                             * while those downstairs are still taking IO.
                             * Good luck!
                             */
                            panic!("[{}] Write more code. join from state {:?} {} {}",
                                up_coms.client_id,
                                my_state,
                                up.uuid,
                                negotiated,
                            );
                        }
                        up.ds_state_show().await;

                    }
                    Some(Message::LastFlushAck { last_flush_number }) => {
                        if negotiated != 3 {
                            bail!("Received LastFlushAck out of order!");
                        }
                        let my_state = {
                            let state = &up.downstairs.lock().await.ds_state;
                            state[up_coms.client_id as usize]
                        };
                        assert_eq!(my_state, DsState::Offline);
                        info!(
                            up.log,
                            "[{}] replied this last flush ID: {}",
                            up_coms.client_id,
                            last_flush_number,
                        );
                        // Assert now, but this should eventually be an
                        // error and move the downstairs to failed. XXX
                        assert_eq!(
                            up.last_flush_id(up_coms.client_id).await, last_flush_number
                        );
                        up.ds_transition(
                            up_coms.client_id, DsState::Replay
                        ).await;

                        *connected = true;
                        negotiated = 5;
                    },
                    Some(Message::ExtentVersions { gen_numbers, flush_numbers, dirty_bits }) => {
                        if negotiated != 4 {
                            bail!("Received ExtentVersions out of order!");
                        }

                        let my_state = {
                            let state = &up.downstairs.lock().await.ds_state;
                            state[up_coms.client_id as usize]
                        };
                        assert_eq!(my_state, DsState::WaitActive);
                        /*
                         * Record this downstairs region info for later
                         * comparison with the other downstairs in this
                         * region set.
                         */
                        let dsr = RegionMetadata {
                            generation: gen_numbers,
                            flush_numbers: flush_numbers.clone(),
                            dirty: dirty_bits,
                        };

                        up.downstairs
                          .lock()
                          .await
                          .region_metadata
                          .insert(up_coms.client_id, dsr);

                        negotiated = 5;
                        up.ds_transition(
                            up_coms.client_id, DsState::WaitQuorum
                        ).await;
                        //up.ds_state_show().await;

                        *connected = true;
                    }
                    Some(Message::UuidMismatch { expected_id }) => {
                        /*
                         * XXX Our downstairs is returning a different
                         * UUID then we have, can this happen in a case where
                         * we (the upstairs) should continue to accept work?
                         *
                         * Until we know better, we are going to disable
                         * this upstairs, which will stop the other
                         * downstairs from taking and sending any more
                         * IO.
                         */
                        warn!(
                            up.log,
                            "[{}] {} received UuidMismatch, expecting {:?}!",
                            up_coms.client_id, up.uuid, expected_id
                        );
                        up.ds_transition(
                            up_coms.client_id, DsState::Disabled
                        ).await;
                        up.set_inactive(CrucibleError::UuidMismatch).await;
                        if up.uuid == expected_id {
                            /*
                             * Now, this is really going off the rails. Our
                             * downstairs thinks we have a different UUID
                             * and is sending us the UUID of the "new"
                             * downstairs, but that UUID IS our UUID.  So
                             * clearly the downstairs is confused.
                             */
                            bail!(
                                "[{}] {} received bad UuidMismatch, {:?}!",
                                up_coms.client_id, up.uuid, expected_id
                            );
                        }
                        bail!(
                            "[{}] {} received UuidMismatch, expecting {:?}!",
                            up_coms.client_id, up.uuid, expected_id
                        );
                    }
                    Some(m) => {
                        bail!(
                            "[{}] unexpected command {:?} \
                            received in state {:?}",
                            up_coms.client_id, m, up.ds_state(up_coms.client_id).await
                        );
                    }
                }
            }
        }
    }
    /*
     * Tell up_listen task that a downstairs has completed the negotiation
     * and is ready to either rejoin an active upstairs, or participate
     * in the reconciliation.
     */
    if let Err(e) = up_coms
        .ds_status_tx
        .send(Condition {
            target: *target,
            connected: true,
            client_id: up_coms.client_id,
        })
        .await
    {
        bail!(
            "[{}] Failed to send status to main task {:?}",
            up_coms.client_id,
            e
        );
    }

    /*
     * This check is just to make sure we have completed negotiation.
     * But, XXX, this will go away when we redo the state transition code
     * for a downstairs connection.
     */
    assert_eq!(negotiated, 5);
    cmd_loop(up, fr, fw, up_coms).await
}

/*
 * Once we have negotiated a connection to a downstairs, this task takes
 * over and watches the input for changes, indicating that new work in on
 * the work hashmap. We will walk the hashmap on the input signal and get
 * any new work for this specific downstairs and mark that job as in
 * progress.
 *
 * V1 flow control: To enable flow control we have a few things.
 * 1. The boolean more_work variable, that indicates we are in a
 * flow control situation and should check for work to do even if new work
 * has not shown up.
 * 2. A resume timeout that is reset each time we try to do more work but
 * find the sending queue is "full" for some value of full we define in
 * the io_work function.
 * 3. Biased setting for the select loop. We start with looking for work
 * ACK messages before putting more new work on the list, which will
 * enable any downstairs to continue to send completed ACKs.
 *
 * Note that the more_work variable is also used when we have a disconnected
 * downstairs that comes back.  In that situation we also need to take our
 * work queue and resend everything since the last flush that was ACK'd.
 */
async fn cmd_loop<RT, WT>(
    up: &Arc<Upstairs>,
    mut fr: FramedRead<RT, crucible_protocol::CrucibleDecoder>,
    mut fw: FramedWrite<WT, crucible_protocol::CrucibleEncoder>,
    up_coms: &mut UpComs,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    /*
     * We set more_work if we arrive here on a re-connection, this will
     * allow us to replay any outstanding work.  If we don't arrive here
     * on a reconnect, then we have to enter the reconcile loop and
     * do any repairs that might be necessary.
     */
    let mut more_work = up.ds_is_replay(up_coms.client_id).await;
    if !more_work {
        do_reconcile_work(up, &mut fr, &mut fw, up_coms).await?;
    }

    /*
     * To keep things alive, initiate a ping any time we have been idle for
     * 10 seconds.
     *
     * XXX figure out what deadlines make sense here
     */
    let mut more_work_interval = deadline_secs(1);
    let mut ping_interval = deadline_secs(10);
    let mut timeout_deadline = deadline_secs(50);

    /*
     * We create a task that handles messages from the downstairs (usually
     * a result of a message we sent).  This channel is how this task
     * communicates that there is a message to handle.
     */
    let (tx, mut rx) = mpsc::channel::<Message>(100);

    info!(up.log, "[{}] Starts cmd_loop", up_coms.client_id);
    let pm_task = {
        let up_c = up.clone();
        let up_coms_c = up_coms.clone();

        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                /*
                 * TODO: Add a check here to make sure we are
                 * connected and in the proper state before we
                 * accept any commands.
                 *
                 * XXX Check the return code here and do something
                 * about it.  If we fail in process_message, we should
                 * handle it.
                 */
                if let Err(e) =
                    process_message(&up_c, &m, up_coms_c.clone()).await
                {
                    warn!(
                        up_c.log,
                        "[{}] Error processing message: {}",
                        up_coms_c.client_id,
                        e
                    );
                }

                if up_c.ds_deactivate(up_coms_c.client_id).await {
                    bail!("[{}] exits after deactivation", up_coms_c.client_id);
                }
            }
            Ok(())
        })
    };

    tokio::pin!(pm_task);
    loop {
        tokio::select! {
            /*
             * We set biased so the loop will:
             * First make sure the pm task is still running.
             * Second, get and look at messages received from the downstairs.
             *   Some messages we can handle right here, but ACKs from
             *   messages we sent are passed on to the pm task.
             *
             * By handling messages from the downstairs before sending
             * new work, we help to avoid overwhelming the downstairs.
             */
            biased;
            _ = &mut pm_task => {
                bail!("[{}] client work task ended, so we end too",
                    up_coms.client_id);
            }
            f = fr.next() => {
                // When the downstairs responds, push the deadlines
                timeout_deadline = deadline_secs(50);
                ping_interval = deadline_secs(10);

                match f.transpose()? {
                    None => {
                        // Downstairs disconnected
                        warn!(up.log, "[{}] None response", up_coms.client_id);
                        return Ok(())
                    },
                    Some(Message::YouAreNoLongerActive {
                        new_upstairs_id,
                        new_session_id,
                        new_gen,
                    }) => {
                        info!(
                            up.log,
                            "[{}] {} ({}) cmd_loop saw YouAreNoLongerActive {:?} {:?} {}",
                            up_coms.client_id,
                            up.uuid,
                            up.session_id,
                            new_upstairs_id,
                            new_session_id,
                            new_gen,
                        );
                        up.ds_transition(up_coms.client_id, DsState::Disabled).await;
                        up.set_inactive(CrucibleError::NoLongerActive).await;

                        // What if the newly active upstairs has the same UUID?
                        if up.uuid == new_upstairs_id {
                            if new_gen > up.get_generation().await {
                                // The next generation of this Upstairs
                                // connected, bail - this generation won't be
                                // able to connect again.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the newly active Upstairs, which shares
                            // our UUID. We shouldn't have received this
                            // message. The downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, same upstairs \
                                uuid and our gen {} >= new gen {}!",
                                up_coms.client_id,
                                up.uuid,
                                up.get_generation().await,
                                new_gen,
                            );
                        } else {
                            // A new upstairs connected
                            if new_gen > up.get_generation().await {
                                // The next generation of another Upstairs
                                // connected.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the old one, and it's a new Upstairs. We
                            // shouldn't have received this message. The
                            // downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, different \
                                upstairs uuid {:?} and our gen {} >= new gen \
                                {}!",
                                up_coms.client_id,
                                up.uuid,
                                new_upstairs_id,
                                up.get_generation().await,
                                new_gen,
                            );
                        }
                    }
                    Some(Message::UuidMismatch { expected_id }) => {
                        /*
                         * For now, when we get the wrong UUID back from
                         * the downstairs, take ourselves out.
                         * XXX Can we handle the case of a corrupted
                         * UUID?
                         * XXX Can a bad downstairs sending us a bad
                         * UUID be used as a denial of service?
                         */
                        up.ds_transition(up_coms.client_id, DsState::Disabled).await;
                        up.set_inactive(CrucibleError::UuidMismatch).await;
                        bail!(
                            "[{}] received UuidMismatch, expecting {:?}!",
                            up_coms.client_id, expected_id
                        );
                    }
                    Some(m) => {
                        tx.send(m).await?;
                    }
                }
            }
            _ = up_coms.ds_work_rx.changed() => {
                /*
                 * A change here indicates the work hashmap has changed
                 * and we should go look for new work to do. It is possible
                 * that there is no new work but we won't know until we
                 * check.
                 */
                let more =
                    io_send(up, &mut fw, up_coms.client_id).await?;

                if more && !more_work {
                    warn!(up.log, "[{}] flow control start ",
                        up_coms.client_id
                    );

                    more_work = true;
                    more_work_interval = deadline_secs(1);
                }
            }
            _ = sleep_until(more_work_interval), if more_work => {
                warn!(up.log, "[{}] flow control sending more work",
                    up_coms.client_id
                );

                let more = io_send(up, &mut fw, up_coms.client_id).await?;

                if more {
                    more_work = true;
                } else {
                    more_work = false;
                    warn!(up.log, "[{}] flow control end ", up_coms.client_id);
                }

                more_work_interval = deadline_secs(1);
            }
            /*
             * Don't wait more than 50 seconds to hear from the other side.
             * TODO: 50 is too long, but what is the correct value?
             */
            _ = sleep_until(timeout_deadline) => {
                warn!(up.log, "[{}] Downstairs not responding, take offline",
                    up_coms.client_id);
                return Ok(());
            }
            _ = sleep_until(ping_interval) => {
                /*
                 * To keep things alive, initiate a ping any time we have
                 * been idle for (TBD) seconds.
                 */
                fw.send(Message::Ruok).await?;

                if up.lossy {
                    /*
                     * When lossy is set, we don't always send work to a
                     * downstairs when we should. This means we need to,
                     * every now and then, signal the downstairs task to
                     * check and see if we skipped some work earlier.
                     */
                    io_send(up, &mut fw, up_coms.client_id).await?;
                }

                /*
                 * If we had no work in the work queue, and a disconnect
                 * was requested, we won't issue a flush as things are
                 * already flushed.  However, this task will not know
                 * about the disconnect unless we check for it here.  This
                 * check could be better though, as we really only need
                 * to look for the empty queue case.  The other cases
                 * should be handled when the downstairs ack's the flush
                 * generated by the disconnect request.
                 */
                if up.ds_deactivate(up_coms.client_id).await {
                    bail!("[{}] exits ping deactivation", up_coms.client_id);
                }

                ping_interval = deadline_secs(10);
            }
        }
    }
}

/**
 * When the upstairs is trying to transition to active, all downstairs
 * connecting to the upstairs will pass through this function.
 *
 * This function is run by each downstairs task and is responsible for
 * sending reconciliation work to the specific downstairs, and listening for
 * the response.  All downstairs stay in repair mode until the repair queue
 * is empty.
 *
 * If any downstairs disconnect during the repair, we abort the entire
 * operation and require all downstairs to reconnect again and go back
 * through the entire reconciliation process.  Because of this, the repair
 * task and all downstairs tasks need to check state and listen for
 * messages from each other that can indicate a problem.
 */
async fn do_reconcile_work<RT, WT>(
    up: &Arc<Upstairs>,
    fr: &mut FramedRead<RT, crucible_protocol::CrucibleDecoder>,
    fw: &mut FramedWrite<WT, crucible_protocol::CrucibleEncoder>,
    up_coms: &mut UpComs,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    info!(up.log, "[{}] Starts reconcile loop", up_coms.client_id);

    /*
     * We will arrive here (most likely) before the upstairs has
     * determined if we need to reconcile or not.
     * We both listen for messages coming from downstairs, and wait for
     * reconciliation work notification to arrive from the upstairs task
     * responsible for making all downstairs the same.
     */
    let mut ping_interval = deadline_secs(5);
    let mut timeout_deadline = deadline_secs(40);
    loop {
        tokio::select! {
            f = fr.next() => {
                // When the downstairs responds, push the deadlines
                timeout_deadline = deadline_secs(40);
                ping_interval = deadline_secs(5);

                match f.transpose()? {
                    None => {
                        bail!("[{}] None response during repair",
                            up_coms.client_id);
                    },
                    Some(Message::YouAreNoLongerActive {
                        new_upstairs_id,
                        new_session_id,
                        new_gen,
                    }) => {
                        info!(
                            up.log,
                            "[{}] {} ({}) reconcile saw YouAreNoLongerActive {:?} {:?} {}",
                            up_coms.client_id,
                            up.uuid,
                            up.session_id,
                            new_upstairs_id,
                            new_session_id,
                            new_gen,
                        );
                        up.ds_transition(up_coms.client_id, DsState::Disabled).await;

                        // What if the newly active upstairs has the same UUID?
                        if up.uuid == new_upstairs_id {
                            if new_gen > up.get_generation().await {
                                // The next generation of this Upstairs
                                // connected, bail - this generation won't be
                                // able to connect again.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the newly active Upstairs, which shares
                            // our UUID. We shouldn't have received this
                            // message. The downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, same \
                                upstairs uuid and our gen {} >= new gen {}!",
                                up_coms.client_id,
                                up.uuid,
                                up.get_generation().await,
                                new_gen,
                            );
                        } else {
                            // A new upstairs connected
                            if new_gen > up.get_generation().await {
                                // The next generation of another Upstairs
                                // connected.
                                bail!(
                                    CrucibleError::GenerationNumberTooLow(
                                        format!("saw YouAreNoLongerActive with \
                                            larger gen {} than ours {}",
                                            new_gen, up.get_generation().await)
                                    )
                                );
                            }

                            // Here, our generation number is greater than or
                            // equal to the old one, and it's a new Upstairs. We
                            // shouldn't have received this message. The
                            // downstairs is confused.
                            bail!(
                                "[{}] {} bad YouAreNoLongerActive, different \
                                upstairs uuid {:?} and our gen {} >= new gen \
                                {}!",
                                up_coms.client_id,
                                up.uuid,
                                new_upstairs_id,
                                up.get_generation().await,
                                new_gen,
                            );
                        }
                    }
                    Some(Message::UuidMismatch { expected_id }) => {
                        up.ds_transition(up_coms.client_id, DsState::Disabled).await;
                        bail!(
                            "[{}] received UuidMismatch, expecting {:?}!",
                            up_coms.client_id, expected_id
                        );
                    }
                    Some(Message::RepairAckId { repair_id }) => {
                        if up.downstairs.lock().await.rep_done(
                            up_coms.client_id, repair_id
                        ) {
                            up.ds_repair_done_notify(
                                up_coms.client_id,
                                repair_id,
                                &up_coms.ds_reconcile_done_tx,
                            ).await?;
                        }
                    }
                    Some(Message::Imok) => {
                        info!(up.log, "[{}] Received Imok", up_coms.client_id);
                    }
                    Some(Message::ExtentError {
                        repair_id,
                        extent_id,
                        error,
                    }) => {
                        error!(
                            up.log,
                            "[{}] Extent {} error on job {}: {}",
                            up_coms.client_id,
                            extent_id,
                            repair_id,
                            error,
                        );
                        bail!(
                            "[{}] Extent {} error on job {}: {}",
                            up_coms.client_id,
                            extent_id,
                            repair_id,
                            error,
                        );
                    }
                    Some(m) => {
                        panic!(
                            "[{}] In repair, No action for message {:?}",
                            up_coms.client_id, m);
                    }
                }
            }
            _ = up_coms.ds_reconcile_work_rx.changed() => {
                /*
                 * When we have reconcile work to do, a message is sent
                 * on this channel indicating that each downstairs should
                 * either look for new work and/or check to see if the
                 * reconciliation has completed.
                 */
                info!(up.log, "[{}] received reconcile message",
                    up_coms.client_id);

                /*
                 * We use rep_done to indicate this was job where our client
                 * did not have any actual work to send to the downstairs.
                 * It indicates that, we don't need a response from the
                 * downstairs and can go ahead and mark this rep_id as
                 * completed for this client and move forward.
                 */
                let mut rep_done = None;
                let job = up.
                    downstairs
                    .lock()
                    .await
                    .rep_in_progress(up_coms.client_id);
                match job {
                    Some(op) => {
                        info!(up.log, "[{}] client {:?}",
                            up_coms.client_id, op);
                        /*
                         * If there is work to do, check to see if it is
                         * a repair job.  If so, only send that to the actual
                         * clients that need to get it.  The source downstairs
                         * does not get a message for this operation, nor
                         * will a downstairs that matches the source.
                         *
                         * If the work is an extent flush, then only send the
                         * message to the source extent, the other downstairs
                         * must not get a message.
                         */
                        match op {
                            Message::ExtentRepair {
                                repair_id,
                                extent_id: _,
                                source_client_id: _,
                                source_repair_address: _,
                                ref dest_clients,
                            } => {
                                let mut send_repair = false;
                                for d in dest_clients {
                                    if *d == up_coms.client_id {
                                        send_repair = true;
                                        break;
                                    }
                                }
                                if send_repair {
                                    info!(
                                        up.log,
                                        "[{}] Sending repair request {:?}",
                                        up_coms.client_id, repair_id,
                                    );
                                    fw.send(op.clone()).await?;
                                } else {
                                    info!(
                                        up.log,
                                        "[{}] No action required {:?}",
                                        up_coms.client_id, repair_id,
                                    );
                                    rep_done = Some(repair_id);
                                }
                            },
                            Message::ExtentFlush {
                                repair_id,
                                extent_id: _,
                                client_id,
                                flush_number: _,
                                gen_number: _,
                            } => {
                                if up_coms.client_id != client_id {
                                    rep_done = Some(repair_id);
                                } else {
                                    fw.send(op).await?;
                                }
                            },
                            op => {
                                fw.send(op).await?;
                            }
                        }
                    },
                    None => {
                        /*
                         * rep_in_progress will return None for four reasons,
                         * figure out which reason
                         */
                        let st = up.ds_state(up_coms.client_id).await;
                        if st == DsState::Active || st == DsState::Repair {
                            // Option 1: work is done
                            if up.downstairs
                                .lock()
                                .await
                                .reconcile_task_list
                                .is_empty() {
                                info!(
                                    up.log,
                                    "[{}] All repairs completed, exit",
                                    up_coms.client_id
                                );
                                return Ok(());
                            } else {
                                // Option 2: more work, but not yet.
                                assert_eq!(st, DsState::Repair);
                                info!(
                                    up.log,
                                    "[{}] still work to do, just not now",
                                    up_coms.client_id
                                );
                            }
                        } else if st == DsState::FailedRepair {
                            // Option 3: Give up, and reconnect.
                            bail!("[{}] Abort reconcile", up_coms.client_id);

                        } else {
                            // Option 4: wait for other downstairs to show up.
                            info!(
                                up.log,
                                "[{}] Not yet in repair mode",
                                up_coms.client_id
                            );
                            continue;
                        }
                    }
                }
                /*
                 * If rep_done is Some, it means this client had nothing
                 * to send to the downstairs, and we can go ahead and mark
                 * this rep_id as completed, which will trigger sending a
                 * notify if all other downstairs are also complete.
                 */
                if let Some(rep_id) = rep_done {
                    if up.downstairs.lock().await
                        .rep_done(up_coms.client_id, rep_id) {
                        info!(
                            up.log,
                            "[{}] self notify as src for {}",
                            up_coms.client_id,
                            rep_id
                        );
                        up.ds_repair_done_notify(
                            up_coms.client_id,
                            rep_id,
                            &up_coms.ds_reconcile_done_tx,
                        ).await?;
                    }
                }
            }
            _ = sleep_until(timeout_deadline) => {
                bail!("[{}] Downstairs not responding, take offline",
                    up_coms.client_id);
            }
            _ = sleep_until(ping_interval) => {
                /*
                 * To keep things alive, initiate a ping any time we have
                 * been idle for (TBD) seconds.
                 */
                fw.send(Message::Ruok).await?;

                /*
                 * This task will not know about a disconnect request
                 * unless we check for it here.
                 * TODO: This code path is still not connected.
                 */
                if up.ds_deactivate(up_coms.client_id).await {
                    bail!("[{}] exits ping deactivation", up_coms.client_id);
                }

                ping_interval = deadline_secs(10);
            }
        }
    }
}

/**
 * Things that allow the various tasks of Upstairs to communicate
 * with each other.
 */
#[derive(Clone, Debug)]
struct UpComs {
    /**
     * The client ID (a downstairs) who will be using these channels.
     */
    client_id: u8,
    /**
     * This channel is used to receive a notification that new work has
     * (possibly) arrived on the work queue and this client should go
     * see what new work has arrived
     */
    ds_work_rx: watch::Receiver<u64>,
    /**
     * This channel is used to transmit that the state of the connection
     * to this downstairs has changed.  The receiver is the up_listen task.
     */
    ds_status_tx: mpsc::Sender<Condition>,
    /**
     * This channel is used to transmit that an IO request sent by the
     * upstairs to all required downstairs has completed. The receiver is
     * the up_ds_listen() task.
     */
    ds_done_tx: mpsc::Sender<u64>,
    /**
     * This channel is used to notify the proc task that it's time to
     * promote this downstairs to active.
     */
    ds_active_rx: watch::Receiver<u64>,

    /**
     * This channel is used to receive notifications from the upstairs
     * task that handles reconciliation.  A message indicates the
     * downstairs task should look at the work queue and:
     * - Exit repair and go to normal operations.
     * - Exit repair and reset to new (reconciliation aborted).
     * - Do new repair work (if there is new)
     * - Do nothing (if other downstairs are not yet ready)
     *
     * It's possible to receive messages when there is nothing to do.
     */
    ds_reconcile_work_rx: watch::Receiver<u64>,

    /**
     * This channel is used to transmit that a reconcile command issued
     * to all downstairs has completed.  The receiver is the up_listen
     * task.
     */
    ds_reconcile_done_tx: mpsc::Sender<Repair>,
}

#[allow(clippy::large_enum_variant)]
enum WrappedStream {
    Http(tokio::net::TcpStream),
    Https(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

/*
 * This task is responsible for the connection to a specific downstairs
 * instance.  This task will run forever.
 */
async fn looper(
    target: SocketAddr,
    tls_context: Arc<
        tokio::sync::Mutex<Option<crucible_common::x509::TLSContext>>,
    >,
    up: &Arc<Upstairs>,
    mut up_coms: UpComs,
) {
    let mut firstgo = true;
    let mut connected = false;

    let log = up.log.new(o!("looper" => up_coms.client_id.to_string()));
    'outer: loop {
        if firstgo {
            firstgo = false;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /*
         * Make connection to this downstairs.
         */
        let sock = if target.is_ipv4() {
            TcpSocket::new_v4().unwrap()
        } else {
            TcpSocket::new_v6().unwrap()
        };

        /*
         * Set a connect timeout, and connect to the target:
         */
        info!(log, "[{1}] connecting to {0}", target, up_coms.client_id);
        let deadline = tokio::time::sleep_until(deadline_secs(10));
        tokio::pin!(deadline);
        let tcp = sock.connect(target);
        tokio::pin!(tcp);

        let tcp: TcpStream = loop {
            tokio::select! {
                _ = &mut deadline => {
                    info!(log, "connect timeout");
                    continue 'outer;
                }
                tcp = &mut tcp => {
                    match tcp {
                        Ok(tcp) => {
                            info!(
                                log,
                                "[{}] {} looper connected",
                                up_coms.client_id,
                                up.uuid,
                            );
                            break tcp;
                        }
                        Err(_e) => {
                            /*
                            warn!(
                                up.log,
                                "{0} looper connect to {0} failure: {1:?}",
                                target, e);
                            */
                            continue 'outer;
                        }
                    }
                }
            }
        };

        let tcp = {
            let tls_context = tls_context.lock().await;
            if let Some(ref tls_context) = *tls_context {
                // XXX these unwraps are bad!
                let config = tls_context.get_client_config().unwrap();

                let connector =
                    tokio_rustls::TlsConnector::from(Arc::new(config));

                let server_name = tokio_rustls::rustls::ServerName::try_from(
                    format!("downstairs{}", up_coms.client_id).as_str(),
                )
                .unwrap();

                WrappedStream::Https(
                    connector.connect(server_name, tcp).await.unwrap(),
                )
            } else {
                WrappedStream::Http(tcp)
            }
        };

        /*
         * Once we have a connected downstairs, the proc task takes over and
         * handles negotiation and work processing.
         */
        match proc_stream(&target, up, tcp, &mut connected, &mut up_coms).await
        {
            Ok(()) => {
                // XXX figure out what to do here
            }

            Err(e) => {
                error!(log, "{}: proc: {:?}", target, e);

                // XXX proc can return fatal and non-fatal errors, figure out
                // what to do here
            }
        }

        /*
         * If the connection goes down here, we need to know what state we
         * were in to decide what state to transition to.  The ds_missing
         * method will do that for us.
         *
         */
        up.ds_missing(up_coms.client_id).await;

        /*
         * If we are deactivating, then check and see if this downstairs
         * is the final one required to deactivate and if so, switch
         * the upstairs back to initializing.
         */
        up.deactivate_transition_check().await;

        info!(
            log,
            "[{}] {} connection to {} closed",
            up_coms.client_id,
            up.uuid,
            target
        );
        connected = false;
        /*
         * This can fail if we are shutting down and the other side of this
         * task has already ended and we are still trying to message it.
         */
        if let Err(e) = up_coms
            .ds_status_tx
            .send(Condition {
                target,
                connected: false,
                client_id: up_coms.client_id,
            })
            .await
        {
            error!(log, "{} Message to ds_status_tx failed: {:?}", target, e);
        }
    }
}

/*
 * These counts describe the various states that a Downstairs IO can
 * be in.
 */
#[derive(Debug, Default)]
pub struct WorkCounts {
    active: u64,  // New or in flight to downstairs.
    error: u64,   // This IO had an error.
    skipped: u64, // Skipped
    done: u64,    // This IO has completed
}

impl WorkCounts {
    fn completed_ok(&self) -> u64 {
        self.done
    }
}

/*
 * The structure that tracks information about the three downstairs
 * connections as well as the work that each is doing.
 */
#[derive(Debug)]
struct Downstairs {
    /*
     * UUID for each downstairs, index by client ID
     */
    ds_uuid: HashMap<u8, Uuid>,

    /*
     * The IP:Port for repair when contacting the downstairs, hashed by
     * the client index the upstairs gives it.
     */
    ds_repair: HashMap<u8, SocketAddr>,

    /*
     * The state of a downstairs connection, based on client ID
     * Ready here indicates it can receive IO.
     * TODO: When growing to more than one region, should this become
     * a 2d Vec? Index for region, then index for the DS?
     */
    ds_state: Vec<DsState>,
    /*
     * The last flush ID that each downstairs has acked.
     */
    ds_last_flush: Vec<u64>,
    downstairs_errors: HashMap<u8, u64>, // client id -> errors
    ds_active: HashMap<u64, DownstairsIO>,
    next_id: u64,
    completed: AllocRingBuffer<u64>,

    /**
     * On Startup, we collect info from each downstairs region. We use that
     * info to make sure that all three regions in a region set are the
     * same, and if not the same, to decide which data we will consider
     * valid and make the other downstairs contain that same data.
     *
     * We also determine the next flush ID and verify the generation
     * number.
     */
    region_metadata: HashMap<u8, RegionMetadata>,

    /**
     * This holds the current piece of repair work that the three
     * downstairs are working on.  It can be New, InProgress, Skipped,
     * or Done.
     */
    reconcile_current_work: Option<ReconcileIO>,

    /**
     * This queue holds the remaining work required to make all three
     * downstairs in a region set the same.
     */
    reconcile_task_list: VecDeque<ReconcileIO>,

    /**
     * Count of extents repaired and needing repair since the start of
     * this upstairs.
     */
    reconcile_repaired: usize,
    reconcile_repair_needed: usize,

    /**
     * The logger for messages sent from downstairs methods.
     */
    log: Logger,

    /**
     * Counters for the in flight work for the downstairs
     */
    io_state_count: IOStateCount,
}

impl Downstairs {
    fn new(log: Logger) -> Self {
        Self {
            ds_uuid: HashMap::new(),
            ds_repair: HashMap::new(),
            ds_state: vec![DsState::New; 3],
            ds_last_flush: vec![0; 3],
            downstairs_errors: HashMap::new(),
            ds_active: HashMap::new(),
            completed: AllocRingBuffer::with_capacity(2048),
            next_id: 1000,
            region_metadata: HashMap::new(),
            reconcile_current_work: None,
            reconcile_task_list: VecDeque::new(),
            reconcile_repaired: 0,
            reconcile_repair_needed: 0,
            log: log.new(o!("" => "downstairs".to_string())),
            io_state_count: IOStateCount::new(),
        }
    }

    /**
     * Assign a new downstairs ID.
     */
    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /**
     * Mark this request as in progress for this client, and return a copy
     * of the details of the request. If the downstairs client has
     * experienced errors in the past, return None and mark this as
     * Skipped.
     *
     * XXX Better error handling might mean clearing previous downstairs
     * errors, as for all we know it's a new downstairs.
     */
    fn in_progress(&mut self, ds_id: u64, client_id: u8) -> Option<IOop> {
        let job = self.ds_active.get_mut(&ds_id).unwrap();

        let new_state = match &self.downstairs_errors.get(&client_id) {
            Some(_) => IOState::Skipped,
            None => IOState::InProgress,
        };

        let old_state = job.state.insert(client_id, new_state.clone()).unwrap();
        assert_eq!(old_state, IOState::New);
        self.io_state_count.decr(&old_state, client_id);
        self.io_state_count.incr(&new_state, client_id);

        match new_state {
            IOState::Skipped => None,
            IOState::InProgress => Some(job.work.clone()),
            _ => panic!("bad state {} in in_progress!", new_state),
        }
    }

    /**
     * Verify this Downstairs region set is still in a state where
     * reconciliation can continue.
     *
     * We return an error if any downstairs state has changed and
     * we have to abort this whole repair.  We also set any
     * downstairs that believes it is still repairing to failed.
     * This FailedRepair state is an indicator to a downstairs task
     * that it should give up on repairing and close the connection
     * to itself (which will enable the repair once all downstairs are
     * ready).
     */
    fn repair_or_abort(&mut self) -> Result<()> {
        let not_ready = self
            .ds_state
            .iter()
            .filter(|state| **state != DsState::Repair)
            .count();

        if not_ready > 0 {
            /*
             * Something has changed, so abort this repair.
             * Mark any downstairs that have not changed as failed
             * and return error.
             */
            for (i, s) in self.ds_state.iter_mut().enumerate() {
                if *s == DsState::Repair {
                    *s = DsState::FailedRepair;
                    warn!(self.log, "Mark {} as FAILED REPAIR", i);
                }
            }
            info!(self.log, "Clear out existing repair work queue");
            self.reconcile_task_list = VecDeque::new();
            self.reconcile_current_work = None;

            bail!("Invalid DS state, aborting reconcile");
        }
        Ok(())
    }

    /**
     * The caller (a downstairs repair task) is asking for any new work
     * as part of a repair in progress.  If there is work, then we
     * mark that work as InProgress for this client_id and return the
     * work to the caller.
     *
     * We return None in the following situations:
     *
     * 1) We really are done with repair. The caller can verify this by
     *    checking to see if there is still work on the queue.
     * 2) We got an extra notification message (can happen if a downstairs
     *    disconnected and reconnected) and we have already submitted
     *    the current work for this client.  For this the caller should
     *    sit tight and wait for more work.
     * 3) The upstairs reconcile task has detected that a downstairs
     *    has gone away and has given up on this reconcile. This is
     *    indicated by the DsState not being Repair any longer.
     *    The upstairs will retry the repair after all downstairs have
     *    reconnected.
     * 4) This downstairs had #3 above and exited, reconnected, and has
     *    now come back around and is ready to start the repair again, but
     *    the other downstairs are not yet ready, so this downstairs
     *    should just continue waiting for work to show up.
     */
    fn rep_in_progress(&mut self, client_id: u8) -> Option<Message> {
        if self.ds_state[client_id as usize] != DsState::Repair {
            return None;
        }
        if let Some(job) = &mut self.reconcile_current_work {
            let old_state = job.state.insert(client_id, IOState::InProgress);

            /*
             * It is possible in reconnect states that multiple messages
             * will back up on the message notify queue that there is new
             * work for this client to do. Make sure we don't send the the
             * same message twice.
             */
            if old_state != Some(IOState::New) {
                info!(
                    self.log,
                    "[{}] rep_in_progress ignore submitted job {:?}",
                    client_id,
                    job
                );
                return None;
            }
            info!(
                self.log,
                "[{}] rep_in_progress: return {:?}", client_id, job
            );
            Some(job.op.clone())
        } else {
            None
        }
    }

    /**
     * Mark a reconcile work request as done for this client and return
     * true if all work requests are done
     */
    fn rep_done(&mut self, client_id: u8, rep_id: u64) -> bool {
        if let Some(job) = &mut self.reconcile_current_work {
            let old_state = job.state.insert(client_id, IOState::Done).unwrap();
            assert_eq!(old_state, IOState::InProgress);
            assert_eq!(job.id, rep_id);
            let mut done = 0;

            for (_, s) in job.state.iter() {
                if s == &IOState::Done || s == &IOState::Skipped {
                    done += 1;
                }
            }
            done == 3
        } else {
            panic!(
                "[{}] Attempted to complete job {} that does not exist",
                client_id, rep_id
            );
        }
    }

    /*
     * Given a client ID, return the SocketAddr for repair to use.
     */
    fn repair_addr(&mut self, client_id: u8) -> SocketAddr {
        *self.ds_repair.get(&client_id).unwrap()
    }

    /**
     * Take a hashmap with extents we need to fix and convert that to
     * a queue of crucible messages we need to execute to perform the fix.
     *
     * The order of messages in the queue shall be the order they are
     * performed, and no message can start until the previous message
     * has been ack'd by all three downstairs.
     */
    fn convert_rc_to_messages(
        &mut self,
        mut rec_list: HashMap<usize, ExtentFix>,
        max_flush: u64,
        max_gen: u64,
    ) {
        let mut rep_id = 0;
        info!(self.log, "Full repair list: {:?}", rec_list);
        for (ext, ef) in rec_list.drain() {
            /*
             * For each extent needing repair, we put the following
             * tasks on the reconcile task list.
             * Flush (the source) extent with latest gen/flush#.
             * Close extent (on all ds)
             * Send repair command to bad extents
             * Reopen extent.
             */
            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentFlush {
                    repair_id: rep_id,
                    extent_id: ext,
                    client_id: ef.source,
                    flush_number: max_flush,
                    gen_number: max_gen,
                },
            ));
            rep_id += 1;

            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentClose {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id += 1;

            let repair = self.repair_addr(ef.source);
            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentRepair {
                    repair_id: rep_id,
                    extent_id: ext,
                    source_client_id: ef.source,
                    source_repair_address: repair,
                    dest_clients: ef.dest,
                },
            ));
            rep_id += 1;

            self.reconcile_task_list.push_back(ReconcileIO::new(
                rep_id,
                Message::ExtentReopen {
                    repair_id: rep_id,
                    extent_id: ext,
                },
            ));
            rep_id += 1;
        }

        info!(self.log, "Task list: {:?}", self.reconcile_task_list);
    }

    /**
     * We have received a deactivate command from the guest, but we have
     * a downstairs that is offline.  Since we don't know when it might
     * come back, we have to discard all the work it has as we have no
     * longer consider the upstairs active, so the replay will not happen.
     * Mark every job on this downstairs not done as skipped, then take
     * the downstairs out.
     * TODO: This is not completed yet.  The logic for how to ACK the
     * deactivate does not support having any (or all) downstairs offline
     * when there is work in the queue.
     */
    fn ds_deactivate_offline(&mut self, client_id: u8) {
        let mut kvec: Vec<u64> =
            self.ds_active.keys().cloned().collect::<Vec<u64>>();
        kvec.sort_unstable();

        info!(
            self.log,
            "[{}] client skip all {} jobs for deactivate",
            client_id,
            kvec.len(),
        );
        for ds_id in kvec.iter() {
            let job = self.ds_active.get_mut(ds_id).unwrap();

            let state = job.state.get(&client_id).unwrap();

            if *state == IOState::InProgress || *state == IOState::New {
                info!(self.log, "{} change {} to skipped", client_id, ds_id);
                let old_state =
                    job.state.insert(client_id, IOState::Skipped).unwrap();
                self.io_state_count.decr(&old_state, client_id);
                self.io_state_count.incr(&IOState::Skipped, client_id);
            }
        }
    }

    /**
     * We have reconnected to a downstairs. Move every job since the
     * last flush for this client_id back to New, even if we already have
     * an ACK back from the downstairs for this job. We must replay
     * everything since the last flush to guarantee persistence.
     *
     * If the job has already been acked back to the guest, then we don't
     * change that, but we do replay it to the downstairs.
     *
     * The special case we have to handle is a job that is AckReady.
     * In this case, we need to understand if a success from this IO to
     * this downstairs was used to decide that we can send an Ack back to
     * the guest. If it was, then we need to retract that AckReady state,
     * switch the overall job back to NotAcked, and then let the replay
     * happen.
     */
    fn re_new(&mut self, client_id: u8) {
        let lf = self.ds_last_flush[client_id as usize];
        let mut kvec: Vec<u64> =
            self.ds_active.keys().cloned().collect::<Vec<u64>>();
        kvec.sort_unstable();

        info!(
            self.log,
            "[{}] client re-new {} jobs since flush {}",
            client_id,
            kvec.len(),
            lf
        );
        for ds_id in kvec.iter() {
            let is_read = self.is_read(*ds_id).unwrap();
            let wc = self.state_count(*ds_id).unwrap();
            let jobs_completed_ok = wc.completed_ok();

            let job = self.ds_active.get_mut(ds_id).unwrap();

            // We don't need to send anything before our last good flush
            if *ds_id <= lf {
                assert_eq!(Some(&IOState::Done), job.state.get(&client_id));
                continue;
            }

            /*
             * If the job is InProgress or New, then we can just go back
             * to New and no extra work is required.
             * If it's Done, then we need to look further
             */
            if Some(&IOState::Done) == job.state.get(&client_id) {
                /*
                 * If the job is acked, then we are good to go and
                 * we can re-send it downstairs and the upstairs ack
                 * path will handle a downstairs ack for a job that
                 * we already ack'd back to the guest.
                 *
                 * If the job is AckReady, then we need to decide
                 * if this downstairs job was part of what made it AckReady
                 * and if so, we need to undo that AckReady status.
                 */
                if job.ack_status == AckStatus::AckReady {
                    if is_read {
                        if jobs_completed_ok == 1 {
                            info!(self.log, "Remove read data for {}", ds_id);
                            job.data = None;
                            job.ack_status = AckStatus::NotAcked;
                            job.read_response_hashes = Vec::new();
                        }
                    } else {
                        /*
                         * For a write or flush, if we have 3 completed,
                         * then we can leave this job as AckReady, if not,
                         * then we have to undo the AckReady.
                         */
                        if jobs_completed_ok < 3 {
                            info!(
                                self.log,
                                "Remove AckReady for W/F {}", ds_id
                            );
                            job.ack_status = AckStatus::NotAcked;
                        }
                    }
                }
            }
            let old_state = job.state.insert(client_id, IOState::New).unwrap();
            job.replay = true;
            if old_state != IOState::New {
                self.io_state_count.decr(&old_state, client_id);
                self.io_state_count.incr(&IOState::New, client_id);
            }
        }
    }

    /**
     * Return a list of downstairs request IDs that represent unissued
     * requests for this client.
     */
    fn new_work(&self, client_id: u8) -> Vec<u64> {
        self.ds_active
            .values()
            .filter_map(|job| {
                if let Some(IOState::New) = job.state.get(&client_id) {
                    Some(job.ds_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /**
     * Return a count of downstairs request IDs of work we have sent
     * for this client, but don't yet have a response.
     */
    fn submitted_work(&self, client_id: u8) -> usize {
        self.ds_active
            .values()
            .filter(|job| {
                Some(&IOState::InProgress) == job.state.get(&client_id)
            })
            .count()
    }
    /**
     * Build a list of jobs that are ready to be acked.
     */
    fn ackable_work(&mut self) -> Vec<u64> {
        let mut ackable = Vec::new();
        for (ds_id, job) in &self.ds_active {
            if job.ack_status == AckStatus::AckReady {
                ackable.push(*ds_id);
            }
        }
        ackable
    }

    /**
     * Enqueue a new downstairs request.
     */
    fn enqueue(&mut self, io: DownstairsIO) {
        for cid in 0..3 {
            assert_eq!(io.state[&cid], IOState::New);
            self.io_state_count.incr(&IOState::New, cid);
        }
        self.ds_active.insert(io.ds_id, io);
    }

    /**
     * Collect the state of the jobs from each client.
     */
    fn state_count(&mut self, ds_id: u64) -> Result<WorkCounts> {
        /* XXX Should this support invalid ds_ids? */
        let job = self
            .ds_active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;
        Ok(job.state_count())
    }

    fn ack(&mut self, ds_id: u64) {
        /*
         * Move AckReady to Acked.
         */
        let job = self
            .ds_active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))
            .unwrap();

        if job.ack_status != AckStatus::AckReady {
            panic!(
                "Job {} not in proper state to ACK:{:?}",
                ds_id, job.ack_status,
            );
        }
        job.ack_status = AckStatus::Acked;
    }

    fn result(&mut self, ds_id: u64) -> Result<(), CrucibleError> {
        /*
         * If enough downstairs returned an error, then return an error to
         * the Guest
         *
         * Not ok:
         * - 2+ errors for Write/Flush
         * - 3+ errors for Reads
         *
         * TODO: this doesn't tell the Guest what the error(s) were?
         * TODO: Add retries here as well.
         */
        let wc = self.state_count(ds_id).unwrap();

        let job = self
            .ds_active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        /*
         * XXX: this code assumes that 3 downstairs is the max that we'll
         * ever support.
         */
        let bad_job = match &job.work {
            IOop::Read {
                dependencies: _dependencies,
                requests: _,
            } => wc.error == 3,
            IOop::Write {
                dependencies: _dependencies,
                writes: _,
            } => wc.error >= 2,
            IOop::WriteUnwritten {
                dependencies: _dependencies,
                writes: _,
            } => wc.error == 2,
            IOop::Flush {
                dependencies: _dependencies,
                flush_number: _flush_number,
                gen_number: _gen_number,
                snapshot_details: _,
            } => wc.error >= 2,
        };

        if bad_job {
            Err(CrucibleError::IoError(format!(
                "{} out of 3 downstairs returned an error",
                wc.error
            )))
        } else {
            Ok(())
        }
    }

    /*
     * This function does a match on IOop type and updates the oximeter
     * stat and dtrace probe for that operation.
     */
    async fn cdt_gw_work_done(
        &self,
        ds_id: u64,
        gw_id: u64,
        io_size: usize,
        stats: &UpStatOuter,
    ) {
        let job = self
            .ds_active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))
            .unwrap();

        match &job.work {
            IOop::Read {
                dependencies: _,
                requests: _,
            } => {
                cdt::gw__read__done!(|| (gw_id));
                stats.add_read(io_size as i64).await;
            }
            IOop::Write {
                dependencies: _,
                writes: _,
            } => {
                cdt::gw__write__done!(|| (gw_id));
                stats.add_write(io_size as i64).await;
            }
            IOop::WriteUnwritten {
                dependencies: _,
                writes: _,
            } => {
                cdt::gw__write__unwritten__done!(|| (gw_id));
                // We don't include WriteUnwritten operation in the
                // metrics for this guest.
            }
            IOop::Flush {
                dependencies: _,
                flush_number: _,
                gen_number: _,
                snapshot_details: _,
            } => {
                cdt::gw__flush__done!(|| (gw_id));
                stats.add_flush().await;
            }
        }
    }

    /// Returns:
    /// - Ok(Some(valid_hash)) where the integrity hash matches
    /// - Ok(None) where there is no integrity hash in the response and the
    ///   block is all 0
    /// - Err otherwise
    fn validate_unencrypted_read_response(
        response: &mut ReadResponse,
        log: &Logger,
    ) -> Result<Option<u64>, CrucibleError> {
        // check integrity hashes - make sure at least one is correct.
        let mut valid_hash = None;

        if !response.block_contexts.is_empty() {
            let mut successful_hash = false;

            let computed_hash = integrity_hash(&[&response.data[..]]);

            // The most recent hash is probably going to be the right one.
            for context in response.block_contexts.iter().rev() {
                if computed_hash == context.hash {
                    successful_hash = true;
                    valid_hash = Some(context.hash);
                    break;
                }
            }

            if !successful_hash {
                // No integrity hash was correct for this response
                error!(log, "No match computed hash:0x{:x}", computed_hash,);
                for context in response.block_contexts.iter().rev() {
                    error!(log, "No match          hash:0x{:x}", context.hash);
                }
                error!(log, "Data from hash: {:?}", response.data);

                return Err(CrucibleError::HashMismatch);
            }
        } else {
            // No block context(s) in the response!
            //
            // Either this is a read of an unwritten block, or an attacker
            // removed the hashes from the db. Because the Upstairs will perform
            // reconciliation before activating, and because the final step of
            // reconciliation is a flush (which will remove block contexts that
            // do not match with the extent data), we should never expect to see
            // this case unless this is a blank block.
            //
            // XXX if it's not a blank block, we may be under attack?
            assert!(response.data[..].iter().all(|&x| x == 0));
        }

        Ok(valid_hash)
    }

    /// Returns:
    /// - Ok(Some(valid_hash)) for successfully decrypted data
    /// - Ok(None) if there were no block contexts and block was all 0
    /// - Err otherwise
    ///
    /// The return value of this will be stored with the job, and compared
    /// between each read.
    fn validate_encrypted_read_response(
        response: &mut ReadResponse,
        encryption_context: &Arc<EncryptionContext>,
        log: &Logger,
    ) -> Result<Option<u64>, CrucibleError> {
        // XXX because we don't have block generation numbers, an attacker
        // downstairs could:
        //
        // 1) remove encryption context and cause a denial of service, or
        // 2) roll back a block by writing an old data and encryption context
        //
        // check that this read response contains block contexts that contain
        // (at least one) encryption context.

        if response.block_contexts.is_empty() {
            // No block context(s) in the response!
            //
            // Either this is a read of an unwritten block, or an attacker
            // removed the encryption contexts from the db. Because the Upstairs
            // will perform reconciliation before activating, and because the
            // final step of reconciliation is a flush (which will remove block
            // contexts that do not match with the extent data), we should never
            // expect to see this case unless this is a blank block.
            //
            // XXX if it's not a blank block, we may be under attack?
            assert!(response.data[..].iter().all(|&x| x == 0));
            return Ok(None);
        }

        let mut valid_hash = None;

        let mut successful_decryption = false;
        let mut successful_hash = false;

        // Attempt decryption with each encryption context, and fail if all
        // do not work. The most recent encryption context will most likely
        // be the correct one so start there.
        for ctx in response.block_contexts.iter().rev() {
            let block_encryption_ctx =
                if let Some(block_encryption_ctx) = &ctx.encryption_context {
                    block_encryption_ctx
                } else {
                    // this block context is missing an encryption context!
                    // continue to see if another block context has a valid one.
                    //
                    // XXX should this be an error instead?
                    continue;
                };

            // Validate integrity hash before decryption
            let computed_hash = integrity_hash(&[
                &block_encryption_ctx.nonce[..],
                &block_encryption_ctx.tag[..],
                &response.data[..],
            ]);

            if computed_hash == ctx.hash {
                successful_hash = true;
                valid_hash = Some(ctx.hash);

                // Now that the integrity hash was verified, attempt
                // decryption.
                //
                // Note: decrypt_in_place does not overwrite the buffer if
                // it fails, otherwise we would need to copy here. There's a
                // unit test to validate this behaviour.
                let decryption_result = encryption_context.decrypt_in_place(
                    &mut response.data[..],
                    Nonce::from_slice(&block_encryption_ctx.nonce[..]),
                    Tag::from_slice(&block_encryption_ctx.tag[..]),
                );

                if decryption_result.is_ok() {
                    successful_decryption = true;
                    break;
                } else {
                    // Because hashes, nonces, and tags are committed to
                    // disk every time there is a Crucible write, but data
                    // is only committed to disk when there's a Crucible
                    // flush, only one hash + nonce + tag + data combination
                    // will be correct. Due to the fact that nonces are
                    // random for each write, even if the Guest wrote the
                    // same data block 100 times, only one index will be
                    // valid.
                    //
                    // if the computed integrity hash matched but decryption
                    // failed, continue to the next contexts. the current
                    // hashing algorithm (xxHash) is not a cryptographic hash
                    // and is only u64, so collisions are not impossible.
                    warn!(
                        log,
                        "Decryption failed even though integrity hash matched!"
                    );
                }
            }
        }

        if !successful_hash {
            error!(log, "No match for integrity hash");
            for ctx in response.block_contexts.iter() {
                let block_encryption_ctx = if let Some(block_encryption_ctx) =
                    &ctx.encryption_context
                {
                    block_encryption_ctx
                } else {
                    error!(log, "missing encryption context!");
                    continue;
                };

                let computed_hash = integrity_hash(&[
                    &block_encryption_ctx.nonce[..],
                    &block_encryption_ctx.tag[..],
                    &response.data[..],
                ]);
                error!(
                    log,
                    "Expected: 0x{:x} != Computed: 0x{:x}",
                    ctx.hash,
                    computed_hash
                );
            }
            // no hash was correct
            Err(CrucibleError::HashMismatch)
        } else if !successful_decryption {
            // no hash + encryption context combination decrypted this block
            error!(log, "Decryption failed with correct hash");
            Err(CrucibleError::DecryptionError)
        } else {
            // Ok!
            Ok(valid_hash)
        }
    }

    /**
     * Mark this downstairs request as complete for this client. Returns
     * true if this completion is enough that we should message the
     * upstairs task that handles returning completions to the guest.
     *
     * This is where we decide the number of successful completions required
     * before setting the AckReady state on a ds_id, which another upstairs
     * task is looking for to then ACK back to the guest.
     */
    fn process_ds_completion(
        &mut self,
        ds_id: u64,
        client_id: u8,
        responses: Result<Vec<ReadResponse>, CrucibleError>,
        encryption_context: &Option<Arc<EncryptionContext>>,
        up_state: UpState,
    ) -> Result<bool> {
        /*
         * Assume we don't have enough completed jobs, and only change
         * it if we have the exact amount required
         */
        let mut notify_guest = false;
        let deactivate = up_state == UpState::Deactivating;

        /*
         * Get the completed count now,
         * because the job self ref won't let us call state_count once we are
         * using that ref, and the number won't change while we are in
         * this method (you did get the lock first, right??).
         */
        let wc = self.state_count(ds_id)?;
        let mut jobs_completed_ok = wc.completed_ok();

        let job = self
            .ds_active
            .get_mut(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        // Validate integrity hashes and optionally authenticated decryption.
        //
        // With AE, responses can come back that are invalid given an encryption
        // context. Test this here. It will allow us to determine if the
        // decryption is bad and set the job result to error accordingly.
        let mut read_response_hashes = Vec::new();
        let read_data: Result<Vec<ReadResponse>, CrucibleError> =
            if let Some(context) = &encryption_context {
                if let Ok(mut responses) = responses {
                    let vlog = self.log.clone();
                    let result: Result<(), CrucibleError> =
                        responses.iter_mut().try_for_each(|x| {
                            let mh =
                                Downstairs::validate_encrypted_read_response(
                                    x, context, &vlog,
                                )?;
                            read_response_hashes.push(mh);
                            Ok(())
                        });

                    if let Some(error) = result.err() {
                        Err(error)
                    } else {
                        Ok(responses)
                    }
                } else {
                    // The downstairs sent us this error
                    warn!(
                        self.log,
                        "[{}] DS Reports error {:?} on job {}, {:?} EC",
                        client_id,
                        responses,
                        ds_id,
                        job,
                    );
                    // bad responses
                    responses
                }
            } else {
                // no upstairs encryption context
                if let Ok(mut responses) = responses {
                    let vlog = self.log.clone();
                    let result: Result<(), CrucibleError> =
                        responses.iter_mut().try_for_each(|x| {
                            let mh =
                                Downstairs::validate_unencrypted_read_response(
                                    x, &vlog,
                                )?;
                            read_response_hashes.push(mh);
                            Ok(())
                        });

                    if let Some(error) = result.err() {
                        Err(error)
                    } else {
                        Ok(responses)
                    }
                } else {
                    // The downstairs sent us this error
                    warn!(
                        self.log,
                        "[{}] DS Reports error {:?} on job {}, {:?}",
                        client_id,
                        responses,
                        ds_id,
                        job,
                    );
                    // bad responses
                    responses
                }
            };

        let new_state = if let Err(ref e) = read_data {
            warn!(
                self.log,
                "[{}] Reports error {:?} on job {}, {:?}",
                client_id,
                e,
                ds_id,
                job,
            );
            IOState::Error(e.clone())
        } else {
            jobs_completed_ok += 1;
            IOState::Done
        };

        let old_state = job.state.insert(client_id, new_state.clone()).unwrap();
        self.io_state_count.decr(&old_state, client_id);
        self.io_state_count.incr(&new_state, client_id);

        /*
         * Verify the job was InProgress
         */
        if old_state != IOState::InProgress {
            bail!(
                "[{}] job completed while not InProgress: {:?}",
                client_id,
                old_state
            );
        }

        if let IOState::Error(e) = new_state {
            // Some errors can be returned without considering the Downstairs
            // bad. For example, it's still an error if a snapshot exists
            // already but we should not increment downstairs_errors and
            // transition that Downstairs to Failed - that downstairs is still
            // able to serve IO.
            match e {
                CrucibleError::SnapshotExistsAlready(_) => {
                    // pass
                }
                _ => {
                    // Mark this downstairs as bad if this was a write or flush
                    // XXX: reconcilation, retries?
                    // XXX: Errors should be reported to nexus
                    match job.work {
                        IOop::Write {
                            dependencies: _,
                            writes: _,
                        }
                        | IOop::WriteUnwritten {
                            dependencies: _,
                            writes: _,
                        }
                        | IOop::Flush {
                            dependencies: _,
                            flush_number: _,
                            gen_number: _,
                            snapshot_details: _,
                        } => {
                            let errors: u64 =
                                match self.downstairs_errors.get(&client_id) {
                                    Some(v) => *v,
                                    None => 0,
                                };

                            self.downstairs_errors
                                .insert(client_id, errors + 1);
                        }
                        IOop::Read {
                            dependencies: _,
                            requests: _,
                        } => {
                            // It's possible we get a read error if the
                            // downstairs disconnects.  However XXX, someone
                            // should be told about this error.
                            //
                            // Some errors, we need to panic on.
                            match e {
                                CrucibleError::HashMismatch => {
                                    panic!(
                                        "[{}] {} read hash mismatch {:?} {:?}",
                                        client_id, ds_id, e, job
                                    );
                                }
                                CrucibleError::DecryptionError => {
                                    panic!(
                                        "[{}] {} read decrypt error {:?} {:?}",
                                        client_id, ds_id, e, job
                                    );
                                }
                                _ => {
                                    error!(
                                        self.log,
                                        "[{}] {} read error {:?} {:?}",
                                        client_id,
                                        ds_id,
                                        e,
                                        job
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else if job.ack_status == AckStatus::Acked {
            assert_eq!(new_state, IOState::Done);
            /*
             * If this job is already acked, then we don't have much
             * more to do here.  If it's a flush, then we want to be
             * sure to update the last flush for this client.
             */
            match &job.work {
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                    gen_number: _gen_number,
                    snapshot_details: _,
                } => {
                    self.ds_last_flush[client_id as usize] = ds_id;
                }
                IOop::Read {
                    dependencies: _dependencies,
                    requests,
                } => {
                    /*
                     * For a read, make sure the data from a previous read
                     * has the same hash
                     */
                    let read_data: Vec<ReadResponse> = read_data.unwrap();
                    assert!(!read_data.is_empty());
                    if job.read_response_hashes != read_response_hashes {
                        // XXX This error needs to go to Nexus
                        // XXX This will become the "force all downstairs
                        // to stop and refuse to restart" mode.
                        let msg = format!(
                            "[{}] read hash mismatch on id {}\n\
                            Expected {:x?}\n\
                            Computed {:x?}\n\
                            guest_id:{} request:{:?}\n\
                            job state:{:?}",
                            client_id,
                            ds_id,
                            job.read_response_hashes,
                            read_response_hashes,
                            job.guest_id,
                            requests,
                            job.state,
                        );
                        if job.replay {
                            info!(self.log, "REPLAY {}", msg);
                        } else {
                            panic!("{}", msg);
                        }
                    }
                }
                _ => { /* Write and WriteUnwritten IOs have no action here */ }
            }
        } else {
            assert_eq!(new_state, IOState::Done);
            assert_ne!(job.ack_status, AckStatus::Acked);

            let read_data: Vec<ReadResponse> = read_data.unwrap();

            /*
             * Transition this job from Done to AckReady if enough have
             * returned ok.
             */
            match &job.work {
                IOop::Read {
                    dependencies: _dependencies,
                    requests: _,
                } => {
                    assert!(!read_data.is_empty());
                    if jobs_completed_ok == 1 {
                        assert!(job.data.is_none());
                        assert!(job.read_response_hashes.is_empty());
                        job.data = Some(read_data);
                        job.read_response_hashes = read_response_hashes;
                        notify_guest = true;
                        assert_eq!(job.ack_status, AckStatus::NotAcked);
                        job.ack_status = AckStatus::AckReady;
                        cdt::up__to__ds__read__done!(|| job.guest_id);
                    } else {
                        /*
                         * If another job has finished already, we can
                         * compare our read hash to
                         * that and verify they are the same.
                         */
                        if job.read_response_hashes != read_response_hashes {
                            // XXX This error needs to go to Nexus
                            // XXX This will become the "force all downstairs
                            // to stop and refuse to restart" mode.
                            panic!(
                                "[{}] read hash mismatch on {} \n\
                                Expected {:x?}\n\
                                Computed {:x?}\n\
                                job: {:?}",
                                client_id,
                                ds_id,
                                job.read_response_hashes,
                                read_response_hashes,
                                job,
                            );
                        }
                    }
                }
                IOop::Write {
                    dependencies: _,
                    writes: _,
                } => {
                    assert!(read_data.is_empty());
                    if jobs_completed_ok == 2 {
                        notify_guest = true;
                        job.ack_status = AckStatus::AckReady;
                        cdt::up__to__ds__write__done!(|| job.guest_id);
                    }
                }
                IOop::WriteUnwritten {
                    dependencies: _,
                    writes: _,
                } => {
                    assert!(read_data.is_empty());
                    if jobs_completed_ok == 2 {
                        notify_guest = true;
                        job.ack_status = AckStatus::AckReady;
                        cdt::up__to__ds__write__unwritten__done!(
                            || job.guest_id
                        );
                    }
                }
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                    gen_number: _gen_number,
                    snapshot_details: _,
                } => {
                    assert!(read_data.is_empty());
                    /*
                     * If we are deactivating, then we want an ACK from
                     * all three downstairs, not the usual two.
                     * TODO here for handling the case where one (or two,
                     * or three! gasp!) downstairs are Offline.
                     */
                    if (deactivate && jobs_completed_ok == 3)
                        || (!deactivate && jobs_completed_ok == 2)
                    {
                        notify_guest = true;
                        job.ack_status = AckStatus::AckReady;
                        cdt::up__to__ds__flush__done!(|| job.guest_id);
                        if deactivate {
                            info!(
                                self.log,
                                "[{}] deactivate flush {} done",
                                client_id,
                                ds_id
                            );
                        }
                    }
                    self.ds_last_flush[client_id as usize] = ds_id;
                }
            }
        }

        /*
         * If all 3 jobs are done, we can check here to see if we can
         * remove this job from the DS list. If we have completed the ack
         * to the guest, then there will be no more work on this job
         * but messages may still be unprocessed.
         */
        if job.ack_status == AckStatus::Acked {
            self.retire_check(ds_id);
        } else if job.ack_status == AckStatus::NotAcked {
            // If we reach this then the job probably has errors and
            // hasn't acked back yet. We check for NotAcked so we don't
            // double count three done and return true if we already have
            // AckReady set.
            let wc = job.state_count();
            if (wc.error + wc.skipped + wc.done) == 3 {
                notify_guest = true;
                job.ack_status = AckStatus::AckReady;
            }
        }

        Ok(notify_guest)
    }

    /**
     * This request is now complete on all peers, but is is ready to retire?
     * Only when a flush is complete on all three downstairs do we check to
     * see if we can remove jobs. Double check that all write jobs have
     * finished and panic if not.
     *
     * Note we don't retire jobs until all three downstairs have returned
     * from the same flush because the Upstairs replays all jobs since
     * the last flush if a downstairs goes away and then comes back.
     * This includes reads because they can be in the deps list for
     * writes and if they aren't included in replay then the write will
     * never start.
     */
    fn retire_check(&mut self, ds_id: u64) {
        if !self.is_flush(ds_id).unwrap() {
            return;
        }

        // Only a completed flush will remove jobs from the active queue -
        // currently we have to keep everything around for use during replay
        let wc = self.state_count(ds_id).unwrap();
        if (wc.error + wc.skipped + wc.done) == 3 {
            assert!(!self.completed.contains(&ds_id));
            assert_eq!(wc.active, 0);

            // Sort the job list, and retire all the jobs that happened before
            // and including this flush.

            let mut kvec: Vec<u64> = self
                .ds_active
                .keys()
                .cloned()
                .filter(|&x| x <= ds_id)
                .collect::<Vec<u64>>();

            kvec.sort_unstable();

            for id in kvec.iter() {
                // Remove everything before this flush
                assert!(*id <= ds_id);

                // Assert the job is actually done, then complete it
                let wc = self.state_count(*id).unwrap();
                let job = self.ds_active.get(id).unwrap();

                if wc.active > 0 && matches!(job.work, IOop::Read { .. }) {
                    // Flushes do not depend on reads, so there's a special case
                    // where all writes that a flush depends on have completed,
                    // and we're retiring that flush, but there's still an
                    // outstanding read result (one that does not overlap with
                    // any write, or does overlap with a write and depends on
                    // that write).
                    //
                    // Call continue here - some future flush will retire this
                    // read, and in the case of replay we'll correctly replay it
                    // and compare the read result and all that good stuff :)
                    continue;
                }

                assert_eq!(wc.active, 0);
                assert_eq!(wc.error + wc.skipped + wc.done, 3);
                assert!(!self.completed.contains(id));

                let oj = self.ds_active.remove(id).unwrap();
                assert_eq!(oj.ack_status, AckStatus::Acked);
                self.completed.push(*id);
                for cid in 0..3 {
                    let old_state = oj.state.get(&cid).unwrap();
                    self.io_state_count.decr(old_state, cid);
                }
            }
        }
    }

    /**
     * Check if an active job is a flush or not.
     */
    fn is_flush(&self, ds_id: u64) -> Result<bool> {
        let job = self
            .ds_active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        match &job.work {
            IOop::Flush {
                dependencies: _dependencies,
                flush_number: _flush_number,
                gen_number: _gen_number,
                snapshot_details: _,
            } => Ok(true),
            _ => Ok(false),
        }
    }

    /**
     * Check if an active job is a read or not.
     */
    fn is_read(&self, ds_id: u64) -> Result<bool> {
        let job = self
            .ds_active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        match &job.work {
            IOop::Read {
                dependencies: _dependencies,
                requests: _,
            } => Ok(true),
            _ => Ok(false),
        }
    }

    fn client_error(
        &self,
        ds_id: u64,
        client_id: u8,
    ) -> Result<(), CrucibleError> {
        let job = self
            .ds_active
            .get(&ds_id)
            .ok_or_else(|| anyhow!("reqid {} is not active", ds_id))?;

        let state = job
            .state
            .get(&client_id)
            .ok_or_else(|| anyhow!("state for client {} missing", client_id))?;

        if let IOState::Error(e) = state {
            Err(e.clone())
        } else {
            Ok(())
        }
    }
}

/// Implement AES-GCM-SIV encryption
pub struct EncryptionContext {
    cipher: Aes256GcmSiv,
    block_size: usize,
}

impl Debug for EncryptionContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("EncryptionContext")
            .field("block_size", &self.block_size)
            .finish()
    }
}

impl EncryptionContext {
    pub fn new(key: Vec<u8>, block_size: usize) -> EncryptionContext {
        assert!(key.len() == 32);
        let key = Key::from_slice(&key[..]);
        let cipher = Aes256GcmSiv::new(key);

        EncryptionContext { cipher, block_size }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn get_random_nonce(&self) -> Nonce {
        let mut rng = ChaCha20Rng::from_entropy();

        let mut random_iv = Vec::<u8>::with_capacity(12);
        random_iv.resize(12, 1);
        rng.fill_bytes(&mut random_iv);

        Nonce::clone_from_slice(&random_iv)
    }

    pub fn encrypt_in_place(
        &self,
        data: &mut [u8],
    ) -> Result<(Nonce, Tag, u64)> {
        let nonce = self.get_random_nonce();

        let tag = self.cipher.encrypt_in_place_detached(&nonce, b"", data);

        if tag.is_err() {
            bail!("Could not encrypt! {:?}", tag.err());
        }

        let tag = tag.unwrap();

        // Hash [nonce + tag + data] in that order. Perform this after
        // encryption so that the downstairs can verify it without the key.
        let computed_hash = integrity_hash(&[&nonce[..], &tag[..], data]);

        Ok((nonce, tag, computed_hash))
    }

    pub fn decrypt_in_place(
        &self,
        data: &mut [u8],
        nonce: &Nonce,
        tag: &Tag,
    ) -> Result<()> {
        let result =
            self.cipher.decrypt_in_place_detached(nonce, b"", data, tag);

        if result.is_err() {
            bail!("Could not decrypt! {:?}", result.err().unwrap());
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UpState {
    /*
     * The upstairs is just coming online.  We can send IO on behalf of
     * the upstairs, but no IO from the guest.
     */
    Initializing,
    /*
     * The upstairs is online and accepting IO from the guest.
     */
    Active,
    /*
     * Let in flight IO continue, but don't allow any new IO.  This state
     * also means that when a downstairs task has completed all the IO
     * it can, including the final flush, it should reset itself back to
     * new and let the connection to the downstairs close and let the
     * loop to reconnect (looper) happen.
     */
    Deactivating,
}

#[derive(Debug)]
struct UpstairsState {
    active_request: bool,
    up_state: UpState,
    req: Option<BlockReq>,
}

impl UpstairsState {
    pub fn default() -> Self {
        UpstairsState {
            active_request: false,
            up_state: UpState::Initializing,
            req: None,
        }
    }

    /*
     * Setting active means the upstairs has contacted all the necessary
     * downstairs, verified they are consistent (or made them so)
     * and is now ready to receive IO.  Going forward a downstairs
     * that is disconnected can have a slightly different path to
     * re-join than the original compare all downstairs to each other
     * that happens on initial startup. This is because the running
     * upstairs has some state it can use to re-verify a downstairs.
     */
    async fn set_active(&mut self) -> Result<(), CrucibleError> {
        if self.up_state == UpState::Active {
            crucible_bail!(UpstairsAlreadyActive);
        } else if self.up_state == UpState::Deactivating {
            /*
             * We don't support deactivate interruption, so we have to
             * let the currently running deactivation finish before we
             * can accept an activation.
             */
            crucible_bail!(UpstairsDeactivating);
        }
        self.active_request = false;
        self.up_state = UpState::Active;
        let req = self.req.take();
        if let Some(req) = req {
            req.send_ok().await;
        }
        Ok(())
    }
}

/// Describes the region definition an upstairs has received or expects to
/// receive from its downstairs.
#[derive(Clone, Copy, Debug)]
enum RegionDefinitionStatus {
    /// The upstairs has not received any region information from any
    /// downstairs yet. It will accept the first legal region definition it
    /// receives from any downstairs and ensure that all other downstairs
    /// supply the same definition.
    WaitingForDownstairs,

    /// The upstairs expects to receive specific region information from each
    /// downstairs and will reject attempts to connect to a downstairs that
    /// supplies the wrong information.
    ExpectingFromDownstairs(RegionDefinition),

    /// The upstairs has received region information from at least one
    /// downstairs, which subsequent downstairs must match.
    Received(RegionDefinition),
}

impl RegionDefinitionStatus {
    fn get_def(&self) -> Option<RegionDefinition> {
        use RegionDefinitionStatus::*;
        match self {
            WaitingForDownstairs => None,
            ExpectingFromDownstairs(rd) => Some(*rd),
            Received(rd) => Some(*rd),
        }
    }
}

/*
 * XXX Track scheduled storage work in the central structure. Have the
 * target management task check for work to do here by changing the value in
 * its watch::channel. Have the main thread determine that an overflow of
 * work to do backing up in here means we need to do something like mark the
 * target as behind or institute some kind of back pressure, etc.
 */
#[derive(Debug)]
pub struct Upstairs {
    /*
     * Is this Upstairs active, or just attaching inactive?
     */
    active: Mutex<UpstairsState>,

    /*
     * Upstairs UUID
     */
    uuid: Uuid,

    // A unique session ID
    session_id: Uuid,

    /*
     * Upstairs Generation number.
     * Will always increase each time an Upstairs starts.
     */
    generation: Mutex<u64>,

    /*
     * The guest struct keeps track of jobs accepted from the Guest as they
     * progress through crucible. A single job submitted can produce
     * multiple downstairs requests.
     */
    guest: Arc<Guest>,

    /*
     * This Downstairs struct keeps track of information about each
     * downstairs as well as tracking IO operations going between
     * upstairs and downstairs. New work for downstairs is generated
     * inside the upstairs on behalf of IO requests coming from the guest.
     */
    downstairs: Mutex<Downstairs>,

    /*
     * The flush info Vec is only used when first connecting or
     * re-connecting to a downstairs. It is populated with the versions
     * the upstairs considers the "correct". If a downstairs disconnects
     * and then comes back, it has to match or be made to match what was
     * decided as the correct list. This may involve having to refresh
     * the versions vec.
     *
     * The versions vec is not enough to solve a mismatch. We really need
     * Generation number, flush number, and dirty bit for every extent
     * when resolving conflicts.
     *
     * On Startup we determine the highest flush number from all three
     * downstairs. We add one to that and it becomes the next flush
     * number. Flush numbers increment by one each time.
     */
    flush_info: Mutex<FlushInfo>,

    /*
     * The global description of the downstairs region we are using.
     * This allows us to verify each downstairs is the same, as well as
     * enables us to translate an LBA to an extent and block offset.
     */
    ddef: Mutex<RegionDefinitionStatus>,

    /*
     * Optional encryption context - Some if a key was supplied in
     * the CrucibleOpts
     */
    encryption_context: Option<Arc<EncryptionContext>>,

    /*
     * Upstairs keeps all IOs in memory until a flush is ACK'd back from
     * all three downstairs.  If there are IOs we have accepted into the
     * work queue that don't end with a flush, then we set this to indicate
     * that the upstairs may need to issue a flush of its own to be sure
     * that data is pushed to disk.  Note that this is not an indication of
     * an ACK'd flush, just that the last IO command we put on the work
     * queue was not a flush.
     */
    need_flush: Mutex<bool>,

    /*
     * Upstairs stats.
     */
    stats: UpStatOuter,

    /*
     * Does this Upstairs throw random errors?
     */
    lossy: bool,

    /*
     * Operate in read-only mode
     */
    read_only: bool,

    /*
     * Logger used by the upstairs
     */
    log: Logger,
}

impl Upstairs {
    pub fn default() -> Arc<Self> {
        let opts = CrucibleOpts {
            id: Uuid::new_v4(),
            target: vec![],
            lossy: false,
            flush_timeout: None,
            key: None,
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only: false,
        };

        // Register DTrace, and setup slog logging to use it.
        register_probes().unwrap();
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(slog::Level::Info)
            .fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let (drain, registration) = with_drain(drain);
        if let ProbeRegistration::Failed(ref e) = registration {
            panic!("Failed to register probes: {:#?}", e);
        }
        let log = Logger::root(drain.fuse(), o!());

        Self::new(&opts, 0, None, Arc::new(Guest::default()), log)
    }

    pub fn new(
        opt: &CrucibleOpts,
        gen: u64,
        expected_region_def: Option<RegionDefinition>,
        guest: Arc<Guest>,
        log: Logger,
    ) -> Arc<Upstairs> {
        /*
         * XXX Make sure we have three and only three downstairs
         */
        #[cfg(not(test))]
        assert_eq!(opt.target.len(), 3);

        // Create an encryption context if a key is supplied.
        let encryption_context = opt.key_bytes().map(|key| {
            Arc::new(EncryptionContext::new(
                key,
                // XXX: Figure out what to do if no expected region definition
                // was supplied. It would be good to do BlockOp::QueryBlockSize
                // here, but this creates a deadlock. Upstairs::new runs before
                // up_ds_listen in up_main, and up_ds_listen needs to run to
                // answer BlockOp::QueryBlockSize. (Note that the downstairs
                // have not reported in yet, so if no expected definition was
                // supplied no downstairs information is available.)
                expected_region_def
                    .map(|rd| rd.block_size() as usize)
                    .unwrap_or(512),
            ))
        });

        let uuid = opt.id;
        info!(log, "Crucible stats registered with UUID: {}", uuid);
        let stats = UpStatOuter {
            up_stat_wrap: Arc::new(Mutex::new(UpCountStat::new(uuid))),
        };

        let rd_status = match expected_region_def {
            None => RegionDefinitionStatus::WaitingForDownstairs,
            Some(d) => RegionDefinitionStatus::ExpectingFromDownstairs(d),
        };

        let session_id = Uuid::new_v4();
        info!(log, "Crucible {} has session id: {}", uuid, session_id);
        Arc::new(Upstairs {
            active: Mutex::new(UpstairsState::default()),
            uuid,
            session_id: Uuid::new_v4(),
            generation: Mutex::new(gen),
            guest,
            downstairs: Mutex::new(Downstairs::new(log.clone())),
            flush_info: Mutex::new(FlushInfo::new()),
            ddef: Mutex::new(rd_status),
            encryption_context,
            need_flush: Mutex::new(false),
            stats,
            lossy: opt.lossy,
            read_only: opt.read_only,
            log,
        })
    }

    pub fn encrypted(&self) -> bool {
        self.encryption_context.is_some()
    }

    /**
     * Update the counters used by dtrace probes.
     * This one method will update the fields of the
     * up_status counter.
     */
    #[inline]
    async fn stat_update(&self, msg: &str) {
        let up_count = self.up_work_active().await;
        let ds_count = self.ds_work_active().await;
        let ds_state = self.ds_state_copy().await;
        let ds_io_count = self.downstairs.lock().await.io_state_count;

        cdt::up__status!(|| {
            let arg = Arg {
                up_count,
                ds_count,
                ds_state,
                ds_io_count,
            };
            (msg, arg)
        });
    }

    async fn set_generation(&self, new_gen: u64) {
        let mut gen = self.generation.lock().await;
        *gen = new_gen;
        info!(self.log, "Set desired generation to :{}", *gen);
    }

    async fn get_generation(&self) -> u64 {
        *self.generation.lock().await
    }

    /*
     * Setting active means the upstairs has contacted all the necessary
     * downstairs, verified they are consistent (or made them so)
     * and is now ready to receive IO.  Going forward a downstairs
     * that is disconnected can have a slightly different path to
     * re-join than the original compare all downstairs to each other
     * that happens on initial startup. This is because the running
     * upstairs has some state it can use to re-verify a downstairs.
     *
     * Note this method is only called during tests.
     */
    #[cfg(test)]
    async fn set_active(&self) -> Result<(), CrucibleError> {
        let mut active = self.active.lock().await;
        self.stats.add_activation().await;
        active.set_active().await?;
        info!(
            self.log,
            "{} is now active with session: {}", self.uuid, self.session_id
        );
        Ok(())
    }

    /*
     * This is called if the upstairs has determined that something is
     * wrong and it should deactivate itself.
     */
    async fn set_inactive(&self, err: CrucibleError) {
        let mut active = self.active.lock().await;
        active.active_request = false;
        active.up_state = UpState::Initializing;

        // If something is waiting for activation, they can give up now.
        let req = active.req.take();
        if let Some(req) = req {
            req.send_err(err).await;
        }
        info!(
            self.log,
            "{} set inactive, session {}", self.uuid, self.session_id
        );
    }

    /*
     * Set deactivate on the upstairs.
     *
     * For a deactivation to complete, we need to:
     * Stop all incoming IO.
     * Let any outstanding IO finish.
     * Submit a final flush to all downstairs.
     * Wait for that final flush to finish (on all downstairs).
     *
     * We may be able to take some shortcuts if there is no work
     * in the active queue, and our last IO was a flush.
     *
     * We decide what to do while holding the upstairs active state lock.
     * This prevents any work sneaking in after our switch to deactivation,
     * and prevents confusion from a flush already in the work queue from
     * being confused as our deactivation flush.  Since we plan to create a
     * flush with a real guest job ID and use the ACK of that flush as a
     * way to notify the guest that their deactivate request is done,
     * we also need the guest work lock.
     *
     * By creating a real guest job, we can have the guest wait on completion
     * of that job as a way to be sure the final flush has been ack'd from
     * all downstairs (that are present).
     *
     * At the moment, we don't have any timeout on how long we will try
     * to clear the outstanding work and final flush.  We try forever and
     * will only give up if a downstairs goes offline or we finish the
     * work in the queue.
     */
    async fn set_deactivate(&self, req: Option<BlockReq>) -> Result<(), ()> {
        /*
         * We are changing (maybe) the upstairs state, to make
         * sure we don't conflict with any existing flush, we get the
         * guest and downstairs lock at the same time.
         */
        let mut active = self.active.lock().await;
        let gw = self.guest.guest_work.lock().await;
        let mut ds = self.downstairs.lock().await;
        /*
         * Protect us from double deactivation, or deactivation
         * before we are activated.
         *
         * TODO: Support deactivation during initial setup.
         * We don't yet have a way to interrupt a deactivation in progress.
         */
        if active.up_state == UpState::Initializing {
            if let Some(req) = req {
                req.send_err(CrucibleError::UpstairsInactive).await;
            }
            return Err(());
        } else if active.up_state == UpState::Deactivating {
            if let Some(req) = req {
                req.send_err(CrucibleError::UpstairsDeactivating).await;
            }
            return Err(());
        }

        active.active_request = false;
        active.up_state = UpState::Deactivating;
        info!(self.log, "{} set deactivating.", self.uuid);

        /*
         * If any downstairs are currently offline, then we are going
         * to lock the door behind them and not let them back in until
         * all non-offline downstairs have deactivated themselves.
         *
         * However: TODO: This is not done yet.
         */
        let mut offline_ds = Vec::new();
        for (index, state) in ds.ds_state.iter().enumerate() {
            if *state == DsState::Offline {
                offline_ds.push(index as u8);
            }
        }

        /*
         * TODO: Handle deactivation when a downstairs is offline.
         */
        for client_id in offline_ds.iter() {
            ds.ds_deactivate_offline(*client_id);
            panic!("Can't deactivate with downstairs offline (yet)");
        }

        if ds.ds_active.keys().len() == 0 {
            info!(self.log, "No work, no need to flush, return OK");
            if let Some(req) = req {
                req.send_ok().await;
            }
            return Ok(());
        }

        /*
         * Now, create the "final" flush and submit it to all the
         * downstairs queues.
         */
        self.submit_flush_internal(gw, ds, req, None).await
    }

    #[cfg(test)]
    async fn is_deactivating(&self) -> bool {
        self.active.lock().await.up_state == UpState::Deactivating
    }

    /*
     * When a downstairs disconnects, check and see if the guest had
     * requested a deactivation (Upstairs will be in Deactivating state).
     *
     * If so, then see if all the downstairs have deactivated and if so,
     * reset this upstairs back to initializing and be ready for a new
     * activate command from the guest.
     */
    async fn deactivate_transition_check(&self) {
        let mut active = self.active.lock().await;
        if active.up_state == UpState::Deactivating {
            info!(self.log, "deactivate transition checking...");
            let mut ds = self.downstairs.lock().await;
            let mut de_done = true;
            ds.ds_state.iter_mut().for_each(|ds_state| {
                if *ds_state == DsState::New || *ds_state == DsState::WaitActive
                {
                    info!(
                        self.log,
                        "deactivate_transition {:#?} Maybe ", *ds_state
                    );
                } else if *ds_state == DsState::Offline {
                    // TODO: support this
                    panic!("Can't deactivate when a downstairs is offline");
                } else {
                    info!(
                        self.log,
                        "deactivate_transition {:#?} NO", *ds_state
                    );
                    de_done = false;
                }
            });
            if de_done {
                info!(self.log, "All DS in the proper state! -> INIT");
                active.up_state = UpState::Initializing;
            }
        }
    }

    /*
     * Check and see if a downstairs client can deactivate itself, and if
     * it can, then mark it so.
     *
     * Return true if we deactivated this downstairs.
     */
    async fn ds_deactivate(&self, client_id: u8) -> bool {
        let active = self.active.lock().await;
        let up_state = active.up_state;
        /*
         * Only check for deactivate if the guest has requested
         * a deactivate, which will set the up_state to Deactivating.
         */
        if up_state != UpState::Deactivating {
            return false;
        }
        let ds = self.downstairs.lock().await;

        let mut kvec: Vec<u64> =
            ds.ds_active.keys().cloned().collect::<Vec<u64>>();
        if kvec.is_empty() {
            info!(self.log, "[{}] deactivate, no work so YES", client_id);
            self.ds_transition_with_lock(
                ds,
                up_state,
                client_id,
                DsState::Deactivated,
            );
            return true;
        } else {
            kvec.sort_unstable();
            /*
             * The last job must be a flush.  It's possible to get
             * here right after deactivating is set, but before the final
             * flush happens.
             */
            let last_id = kvec.last().unwrap();
            if !ds.is_flush(*last_id).unwrap() {
                info!(
                    self.log,
                    "[{}] deactivate last job {} not flush, NO",
                    client_id,
                    last_id
                );
                return false;
            }
            /*
             * Now count our jobs.  Any job not done or skipped means
             * we are not ready to deactivate.
             */
            for id in kvec.iter() {
                let job = ds.ds_active.get(id).unwrap();
                let state = job.state.get(&client_id).unwrap();
                if state == &IOState::New || state == &IOState::InProgress {
                    info!(
                        self.log,
                        "[{}] deactivate job {} not {:?} flush, NO",
                        client_id,
                        id,
                        state
                    );
                    return false;
                }
            }
        }
        /*
         * To arrive here, we verified our most recent job is a flush, and
         * none of the jobs that are on our active job list are New or
         * InProgress (either error, skipped, or done)
         */
        info!(self.log, "[{}] check deactivate YES", client_id);
        self.ds_transition_with_lock(
            ds,
            up_state,
            client_id,
            DsState::Deactivated,
        );
        true
    }

    /*
     * This just indicates if we will take any more IO from the
     * guest and put it on the work list.  It does not mean we can't
     * finish an IO, just that we can't start any new IO.
     * Don't call this with the downstairs lock held.
     */
    async fn guest_io_ready(&self) -> bool {
        let active = self.active.lock().await;
        matches!(active.up_state, UpState::Active)
    }

    /*
     * The guest has requested this upstairs go active.
     */
    async fn set_active_request(
        &self,
        req: BlockReq,
    ) -> Result<(), CrucibleError> {
        let mut active = self.active.lock().await;
        match active.up_state {
            UpState::Initializing => {
                assert!(!active.active_request);
                assert!(active.req.is_none());
                active.active_request = true;
                active.req = Some(req);
                info!(self.log, "{} active request set", self.uuid);
                Ok(())
            }
            UpState::Deactivating => {
                warn!(
                    self.log,
                    "{} active denied while Deactivating", self.uuid
                );
                req.send_err(CrucibleError::UpstairsDeactivating).await;
                crucible_bail!(UpstairsDeactivating);
            }
            UpState::Active => {
                info!(
                    self.log,
                    "{} Request to activate upstairs already active", self.uuid
                );
                req.send_err(CrucibleError::UpstairsAlreadyActive).await;
                crucible_bail!(UpstairsAlreadyActive);
            }
        }
    }

    /*
     * The request to go active is not longer true
     */
    async fn _clear_active_request(&self) {
        let mut active = self.active.lock().await;
        active.active_request = false;
    }

    /*
     * Has the guest asked this upstairs to go active
     */
    async fn is_active_requested(&self) -> bool {
        self.active.lock().await.active_request
    }

    /*
     * If we are doing a flush, the flush number and the rn number
     * must both go up together. We don't want a lower next_id
     * with a higher flush_number to be possible, as that can introduce
     * dependency deadlock.
     * To also avoid any problems, this method should be called only
     * during the submit_flush method so we know the downstairs and
     * guest_work locks are both held.
     */
    async fn next_flush_id(&self) -> u64 {
        let mut fi = self.flush_info.lock().await;
        fi.get_next_flush()
    }

    async fn last_flush_id(&self, client_id: u8) -> u64 {
        let ds = self.downstairs.lock().await;
        ds.ds_last_flush[client_id as usize]
    }

    async fn set_flush_clear(&self) {
        let mut flush = self.need_flush.lock().await;
        *flush = false;
    }

    async fn set_flush_need(&self) {
        let mut flush = self.need_flush.lock().await;
        *flush = true;
    }

    async fn flush_needed(&self) -> bool {
        if !self.guest_io_ready().await {
            return false;
        }
        *self.need_flush.lock().await
    }

    /*
     * If the sender is empty, it means this flush request came from
     * the upstairs itself. There is no real guest IO behind it.
     *
     * Flushes can optionally take a ZFS snapshot if the snapshot_details
     * parameter is set.
     */
    #[instrument]
    async fn submit_flush(
        &self,
        req: Option<BlockReq>,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), ()> {
        /*
         * Lock first the guest_work struct where this new job will go,
         * then lock the downstairs struct. Once we have both we can proceed
         * to build our flush command.
         */
        let gw = self.guest.guest_work.lock().await;
        let downstairs = self.downstairs.lock().await;

        self.submit_flush_internal(gw, downstairs, req, snapshot_details)
            .await
    }

    async fn submit_flush_internal(
        &self,
        mut gw: MutexGuard<'_, GuestWork>,
        mut downstairs: MutexGuard<'_, Downstairs>,
        req: Option<BlockReq>,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), ()> {
        self.set_flush_clear().await;

        /*
         * Get the next ID for our new guest work job. Note that the flush
         * ID and the next_id are connected here, in that all future writes
         * should be flushed at the next flush ID.
         */
        let gw_id: u64 = gw.next_gw_id();
        let next_id = downstairs.next_id();
        let next_flush = self.next_flush_id().await;
        cdt::gw__flush__start!(|| (gw_id));

        if snapshot_details.is_some() {
            info!(self.log, "flush with snap requested");
        }
        /*
         * To build the dependency list for this flush, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new flush must
         * depend on it.
         *
         * We can safely ignore everything before the last flush, because the
         * last flush will depend on jobs before it. But this flush must
         * depend on the last flush - flush and gen numbers
         * downstairs need to be sequential and the same for each
         * downstairs.
         *
         * This flush does not have to depend on reads as they do not impact
         * downstairs state, but must depend on every write since the last
         * flush.
         */
        let num_jobs = downstairs.ds_active.keys().len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        for job_id in downstairs
            .ds_active
            .keys()
            .sorted()
            .collect::<Vec<&u64>>()
            .iter()
            .rev()
        {
            let job = &downstairs.ds_active[job_id];

            // Depend on the last flush, but then bail out
            if job.work.is_flush() {
                dep.push(**job_id);
                break;
            }

            // Depend on all writes seen
            if job.work.is_write() {
                dep.push(**job_id);
            }
        }

        /*
         * TODO: Walk the list of guest work structs and build the same list
         * and make sure it matches.
         */

        /*
         * Build the flush request, and take note of the request ID that
         * will be assigned to this new piece of work.
         */
        let ddef = self.ddef.lock().await;
        let fl = create_flush(
            next_id,
            dep,
            next_flush,
            gw_id,
            self.get_generation().await,
            snapshot_details,
            ImpactedBlocks::new(ddef.get_def().unwrap()),
        );

        let mut sub = HashMap::new();
        sub.insert(next_id, 0);

        let new_gtos = GtoS::new(sub, Vec::new(), None, HashMap::new(), req);
        gw.active.insert(gw_id, new_gtos);

        downstairs.enqueue(fl);
        cdt::up__to__ds__flush__start!(|| (gw_id));

        Ok(())
    }

    /*
     * When we have a guest write request with offset and buffer, take them
     * and build both the upstairs work guest tracking struct as well as the
     * downstairs work struct. Once both are ready, submit them to the
     * required places.
     *
     * The is_write_unwritten bool indicates if this write is a regular
     * write (false) or a write_unwritten write (true) and allows us to
     * construct the proper IOop to submit to the downstairs.
     */
    #[instrument]
    async fn submit_write(
        &self,
        offset: Block,
        data: Bytes,
        req: Option<BlockReq>,
        is_write_unwritten: bool,
    ) -> Result<(), ()> {
        if !self.guest_io_ready().await {
            if let Some(req) = req {
                req.send_err(CrucibleError::UpstairsInactive).await;
            }
            return Err(());
        }

        if self.read_only {
            if let Some(req) = req {
                req.send_err(CrucibleError::ModifyingReadOnlyRegion).await;
            }
            return Err(());
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let mut downstairs = self.downstairs.lock().await;
        self.set_flush_need().await;

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span two extents, and eventually XXX, two regions.
         */
        let ddef = self.ddef.lock().await;
        let impacted_blocks = extent_from_offset(
            ddef.get_def().unwrap(),
            offset,
            Block::from_bytes(data.len(), &ddef.get_def().unwrap()),
        );

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        if is_write_unwritten {
            cdt::gw__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::gw__write__start!(|| (gw_id));
        }

        /*
         * Now create a downstairs work job for each (eid, bi, len) returned
         * from extent_from_offset
         *
         * Create the list of downstairs request numbers (ds_id) we created
         * on behalf of this guest job.
         */
        let mut sub = HashMap::new();
        let next_id = downstairs.next_id();
        let mut cur_offset: usize = 0;

        /*
         * To build the dependency list for this write, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new write must
         * depend on it.
         *
         * Construct a list of dependencies for this write based on the
         * following rules:
         *
         * - ignore everything that happened before the last flush
         * - writes have to depend on the last flush completing
         * - any overlap of impacted blocks requires a dependency
         *
         * TODO: any overlap of impacted blocks will create a dependency.
         * take this an example (this shows three writes, all to the
         * same block, along with the dependency list for each
         * write):
         *
         *       block
         * op# | 0 1 2 | deps
         * ----|-------------
         *   0 | W     |
         *   1 | W     | 0
         *   2 | W     | 0,1
         *
         * op 2 depends on both op 1 and op 0. if dependencies are transitive
         * with an existing job, it would be nice if those were removed from
         * this job's dependencies.
         */
        let num_jobs = downstairs.ds_active.keys().len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for job_id in downstairs
            .ds_active
            .keys()
            .sorted()
            .collect::<Vec<&u64>>()
            .iter()
            .rev()
        {
            let job = &downstairs.ds_active[job_id];

            // Depend on the last flush, then break - flushes are a barrier for
            // all writes.
            if job.work.is_flush() {
                dep.push(**job_id);
                break;
            }

            // If this job impacts the same blocks as something already active,
            // create a dependency.
            if impacted_blocks.conflicts(&job.impacted_blocks) {
                dep.push(**job_id);
            }
        }

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(impacted_blocks.tuples().len());

        for (eid, bo) in impacted_blocks.tuples() {
            let byte_len: usize = ddef.get_def().unwrap().block_size() as usize;

            let (sub_data, encryption_context, hash) = if let Some(context) =
                &self.encryption_context
            {
                // Encrypt here
                let mut mut_data =
                    data.slice(cur_offset..(cur_offset + byte_len)).to_vec();

                let (nonce, tag, hash) =
                    match context.encrypt_in_place(&mut mut_data[..]) {
                        Err(e) => {
                            if let Some(req) = req {
                                req.send_err(CrucibleError::EncryptionError(
                                    e.to_string(),
                                ))
                                .await;
                            }
                            return Err(());
                        }

                        Ok(v) => v,
                    };

                (
                    Bytes::copy_from_slice(&mut_data),
                    Some(crucible_protocol::EncryptionContext {
                        nonce: Vec::from(nonce.as_slice()),
                        tag: Vec::from(tag.as_slice()),
                    }),
                    hash,
                )
            } else {
                // Unencrypted
                let sub_data = data.slice(cur_offset..(cur_offset + byte_len));
                let hash = integrity_hash(&[&sub_data[..]]);

                (sub_data, None, hash)
            };

            writes.push(crucible_protocol::Write {
                eid,
                offset: bo,
                data: sub_data,
                block_context: BlockContext {
                    hash,
                    encryption_context,
                },
            });

            cur_offset += byte_len;
        }

        let wr = create_write_eob(
            next_id,
            dep.clone(),
            gw_id,
            writes,
            is_write_unwritten,
            impacted_blocks,
        );

        sub.insert(next_id, 0); // XXX does value here matter?

        /*
         * New work created, add to the guest_work HM
         */
        let new_gtos = GtoS::new(sub, Vec::new(), None, HashMap::new(), req);
        {
            gw.active.insert(gw_id, new_gtos);
        }

        downstairs.enqueue(wr);
        if is_write_unwritten {
            cdt::up__to__ds__write__unwritten__start!(|| (gw_id));
        } else {
            cdt::up__to__ds__write__start!(|| (gw_id));
        }

        Ok(())
    }

    /*
     * When we have a guest read request with offset and buffer, take them
     * and build both the upstairs work guest tracking struct as well as the
     * downstairs work struct. Once both are ready, submit them to the
     * required places.
     */
    #[instrument]
    async fn submit_read(
        &self,
        offset: Block,
        data: Buffer,
        req: Option<BlockReq>,
    ) -> Result<(), ()> {
        if !self.guest_io_ready().await {
            if let Some(req) = req {
                req.send_err(CrucibleError::UpstairsInactive).await;
            }
            return Err(());
        }

        /*
         * Get the next ID for the guest work struct we will make at the
         * end. This ID is also put into the IO struct we create that
         * handles the operation(s) on the storage side.
         */
        let mut gw = self.guest.guest_work.lock().await;
        let mut downstairs = self.downstairs.lock().await;
        self.set_flush_need().await;

        /*
         * Given the offset and buffer size, figure out what extent and
         * byte offset that translates into. Keep in mind that an offset
         * and length may span many extents, and eventually, TODO, regions.
         */
        let ddef_state = self.ddef.lock().await;
        let ddef = &ddef_state.get_def().unwrap();
        let impacted_blocks = extent_from_offset(
            *ddef,
            offset,
            Block::from_bytes(data.len(), ddef),
        );

        /*
         * Grab this ID after extent_from_offset: in case of Err we don't
         * want to create a gap in the IDs.
         */
        let gw_id: u64 = gw.next_gw_id();
        cdt::gw__read__start!(|| (gw_id));

        /*
         * Create the tracking info for downstairs request numbers (ds_id) we
         * will create on behalf of this guest job.
         */
        let mut sub = HashMap::new();
        let next_id = downstairs.next_id();

        /*
         * To build the dependency list for this read, iterate from the end
         * of the downstairs work active list in reverse order and
         * check each job in that list to see if this new read must
         * depend on it.
         *
         * Construct a list of dependencies for this read based on the
         * following rules:
         *
         * - reads do not depend on flushes, only writes (because flushes do
         *   not modify data!)
         * - any write with an overlap of impacted blocks requires a
         *   dependency
         */
        let num_jobs = downstairs.ds_active.keys().len();
        let mut dep: Vec<u64> = Vec::with_capacity(num_jobs);

        // Search backwards in the list of active jobs
        for job_id in downstairs
            .ds_active
            .keys()
            .sorted()
            .collect::<Vec<&u64>>()
            .iter()
            .rev()
        {
            let job = &downstairs.ds_active[job_id];

            // If this is a write and it impacts the same blocks as something
            // already active, create a dependency.
            if job.work.is_write()
                && impacted_blocks.conflicts(&job.impacted_blocks)
            {
                dep.push(**job_id);
            }
        }

        /*
         * Now create a downstairs work job for each (eid, bo) returned
         * from extent_from_offset.
         */
        let mut requests: Vec<ReadRequest> =
            Vec::with_capacity(impacted_blocks.len());

        for (eid, bo) in impacted_blocks.tuples() {
            requests.push(ReadRequest { eid, offset: bo });
        }

        sub.insert(next_id, 0); // XXX does this value matter?

        let wr = create_read_eob(
            next_id,
            dep.clone(),
            gw_id,
            requests,
            impacted_blocks,
        );

        /*
         * New work created, add to the guest_work HM. New work must be put
         * on the guest_work active HM first, before it lands on the
         * downstairs lists. We don't want to miss a completion from
         * downstairs.
         */
        assert!(!sub.is_empty());
        let new_gtos =
            GtoS::new(sub, Vec::new(), Some(data), HashMap::new(), req);
        {
            gw.active.insert(gw_id, new_gtos);
        }

        downstairs.enqueue(wr);
        cdt::up__to__ds__read__start!(|| (gw_id));

        Ok(())
    }

    /*
     * Our connection to a downstairs has been lost.  Depending on what
     * state the downstairs was in and what state the upstairs is in
     * will indicate which state this downstairs needs to go to.
     *
     * If we were disconnected because the downstairs decided to kick us
     * out, then we should go back to New.
     */
    async fn ds_missing(&self, client_id: u8) {
        let mut ds = self.downstairs.lock().await;
        let current = ds.ds_state[client_id as usize];
        let new_state = match current {
            DsState::Active => DsState::Offline,
            DsState::Replay => DsState::Offline,
            DsState::Offline => DsState::Offline,
            DsState::Migrating => DsState::Failed,
            DsState::Deactivated => DsState::New,
            DsState::Repair => DsState::New,
            DsState::FailedRepair => DsState::New,
            _ => {
                /*
                 * Any other state means we had not yet enabled this
                 * downstairs to receive IO, so we go to the back of the
                 * line and have to re-verify it again.
                 */
                DsState::Disconnected
            }
        };

        info!(
            self.log,
            "[{}] {} Gone missing, transition from {:?} to {:?}",
            client_id,
            self.uuid,
            current,
            new_state,
        );
        ds.ds_state[client_id as usize] = new_state;
    }

    /*
     * Check and see what state our downstairs is in.  This check
     * determines if we have a returning downstairs to an active
     * upstairs, or we have a new upstairs trying to go active and
     * we need to possibly reconcile the three downstairs.  We return
     * true here if this downstairs can go active and start receiving
     * IOs.  We return false if this downstairs should enter the
     * repair path and reconcile with the other downstairs.
     */
    async fn ds_is_replay(&self, client_id: u8) -> bool {
        let mut ds = self.downstairs.lock().await;
        if ds.ds_state[client_id as usize] == DsState::Replay {
            info!(
                self.log,
                "[{}] {} Transition from Replay to Active",
                client_id,
                self.uuid
            );
            ds.ds_state[client_id as usize] = DsState::Active;
            return true;
        }
        false
    }

    /*
     * Move a single downstairs to this new state.
     */
    async fn ds_transition(&self, client_id: u8, new_state: DsState) {
        let active = self.active.lock().await;
        let up_state = active.up_state;
        let ds = self.downstairs.lock().await;
        drop(active);
        self.ds_transition_with_lock(ds, up_state, client_id, new_state);
    }

    /*
     * This is so we can call a state transition if we already have the
     * ds lock.  Avoids problems with race conditions where dropping
     * the lock and getting it allows for state to change.
     */
    fn ds_transition_with_lock(
        &self,
        mut ds: MutexGuard<'_, Downstairs>,
        up_state: UpState,
        client_id: u8,
        new_state: DsState,
    ) {
        info!(
            self.log,
            "[{}] {} ({}) {:?} {:?} {:?} ds_transition to {:?}",
            client_id,
            self.uuid,
            self.session_id,
            ds.ds_state[0],
            ds.ds_state[1],
            ds.ds_state[2],
            new_state
        );

        let old_state = ds.ds_state[client_id as usize];

        /*
         * Check that this is a valid transition
         */
        match new_state {
            DsState::WaitActive => {
                if old_state == DsState::Offline {
                    if up_state == UpState::Active {
                        panic!(
                            "[{}] {} Bad state change when active {:?} -> {:?}",
                            client_id, self.uuid, old_state, new_state,
                        );
                    }
                } else if old_state != DsState::New
                    && old_state != DsState::Failed
                    && old_state != DsState::Disconnected
                {
                    panic!(
                        "[{}] {} {:#?} Negotiation failed, {:?} -> {:?}",
                        client_id, self.uuid, up_state, old_state, new_state,
                    );
                }
            }
            DsState::WaitQuorum => {
                assert_eq!(old_state, DsState::WaitActive);
            }
            DsState::FailedRepair => {
                assert_eq!(old_state, DsState::Repair);
            }
            DsState::Repair => {
                assert_ne!(up_state, UpState::Active);
                assert_eq!(old_state, DsState::WaitQuorum);
            }
            DsState::Replay => {
                assert_eq!(old_state, DsState::Offline);
                assert_eq!(up_state, UpState::Active);
            }
            DsState::Active => {
                if old_state != DsState::WaitQuorum
                    && old_state != DsState::Repair
                {
                    panic!(
                        "[{}] {} Invalid transition: {:?} -> {:?}",
                        client_id, self.uuid, old_state, new_state
                    );
                }
                /*
                 * Make sure repair happened when the upstairs is inactive.
                 */
                if old_state == DsState::Repair {
                    assert_ne!(up_state, UpState::Active);
                }
            }
            DsState::Deactivated => {
                /*
                 *
                 * We only go deactivated if we were actually active, or
                 * somewhere past active (offline?)
                 * if deactivate is requested before active, the downstairs
                 * state should just go back to NEW and re-require an
                 * activation.
                 */
                assert_ne!(old_state, DsState::New);
                assert_ne!(old_state, DsState::BadVersion);
                assert_ne!(old_state, DsState::BadRegion);
                assert_ne!(old_state, DsState::WaitQuorum);
                assert_ne!(old_state, DsState::WaitActive);
                assert_ne!(old_state, DsState::Repair);
            }
            _ => (),
        }

        if old_state != new_state {
            info!(
                self.log,
                "[{}] Transition from {:?} to {:?}",
                client_id,
                ds.ds_state[client_id as usize],
                new_state,
            );
            ds.ds_state[client_id as usize] = new_state;
        } else {
            panic!("[{}] transition to same state: {:?}", client_id, new_state);
        }
    }

    async fn ds_state(&self, client_id: u8) -> DsState {
        let ds = self.downstairs.lock().await;
        ds.ds_state[client_id as usize]
    }

    /*
     * Build the list of extent indexes that don't match.
     * The downstairs lock must be held when calling this, as we don't
     * want this information changing under us while we are looking
     * at it.
     */
    fn mismatch_list(&self, ds: &Downstairs) -> Option<DownstairsMend> {
        let c0_rec = ds.region_metadata.get(&0).unwrap();
        let c1_rec = ds.region_metadata.get(&1).unwrap();
        let c2_rec = ds.region_metadata.get(&2).unwrap();

        let log = self.log.new(o!("" => "mend".to_string()));
        DownstairsMend::new(c0_rec, c1_rec, c2_rec, log)
    }

    /*
     *  Send a message that indicates the downstairs are ready for the
     *  next repair command.
     */
    async fn ds_repair_done_notify(
        &self,
        client_id: u8,
        rep_id: u64,
        ds_reconcile_done_tx: &mpsc::Sender<Repair>,
    ) -> Result<()> {
        info!(
            self.log,
            "[{}] It's time to notify for {}", client_id, rep_id
        );
        if let Err(e) = ds_reconcile_done_tx
            .send(Repair {
                repair: true,
                client_id,
                rep_id,
            })
            .await
        {
            bail!("[{}] Failed to notify {} {:?}", client_id, rep_id, e);
        }
        Ok(())
    }

    /**
     * Get the next repair message from the repair message queue.
     * Make sure the current message has completed.
     *
     * We return true if we have more work to do (after setting
     * reconcile_current_work to that work.
     *
     * We return false if there is no more work to do.
     *
     * repair_or_abort() will check that all downstairs are in the
     * proper state to continue repairing.
     *
     */
    async fn new_rec_work(&self) -> Result<bool> {
        let mut ds = self.downstairs.lock().await;
        /*
         * Make sure all downstairs are still in the correct
         * state before we put the next piece of work on the
         * list for the downstairs to do.
         */
        ds.repair_or_abort()?;

        ds.reconcile_repair_needed = ds.reconcile_task_list.len();
        if let Some(rio) = ds.reconcile_task_list.pop_front() {
            info!(self.log, "Pop front: {:?}", rio);

            // Assert if not None, then job is all done.
            if let Some(job) = &mut ds.reconcile_current_work {
                let mut done = 0;
                for (_, s) in job.state.iter() {
                    if s == &IOState::Done || s == &IOState::Skipped {
                        done += 1;
                    }
                }
                assert_eq!(done, 3);
            }

            ds.reconcile_current_work = Some(rio);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /**
     * Compare downstairs region metadata and based on the results:
     *
     * Determine the global flush number for this region set.
     * Verify the guest given gen number is highest.
     * Decide if we need repair, and if so create the repair list
     */
    async fn collate_downstairs(&self, ds: &mut Downstairs) -> Result<bool> {
        /*
         * Show some (or all if small) of the info from each region.
         *
         * This loop is load bearing, we use this loop to get the
         * max flush number.  Eventually the max flush will come after
         * we have done any repair we need to do.  Since we don't have
         * that code yet, we are making use of this loop to find our
         * max.
         */
        let mut max_flush = 0;
        let mut max_gen = 0;
        for (cid, rec) in &ds.region_metadata {
            let mf = rec.flush_numbers.iter().max().unwrap() + 1;
            if mf > max_flush {
                max_flush = mf;
            }
            let mg = rec.generation.iter().max().unwrap() + 1;
            if mg > max_gen {
                max_gen = mg;
            }
            if rec.flush_numbers.len() > 12 {
                info!(
                    self.log,
                    "[{}]R flush_numbers[0..12]: {:?}",
                    cid,
                    rec.flush_numbers[0..12].to_vec()
                );
                info!(
                    self.log,
                    "[{}]R generation[0..12]: {:?}",
                    cid,
                    rec.generation[0..12].to_vec()
                );
                info!(
                    self.log,
                    "[{}]R dirty[0..12]: {:?}",
                    cid,
                    rec.dirty[0..12].to_vec()
                );
            } else {
                info!(
                    self.log,
                    "[{}]R  flush_numbers: {:?}", cid, rec.flush_numbers
                );
                info!(self.log, "[{}]R  generation: {:?}", cid, rec.generation);
                info!(self.log, "[{}]R  dirty: {:?}", cid, rec.dirty);
            }
        }

        info!(self.log, "Max found gen is {}", max_gen);
        /*
         * Verify that the generation number that the guest has requested
         * is higher than what we have from the three downstairs.
         */
        let requested_gen = self.get_generation().await;
        if requested_gen == 0 {
            error!(self.log, "generation number should be at least 1");
            bail!("Generation number should be at least 1");
        } else if requested_gen < max_gen {
            /*
             * We refuse to connect. The provided generation number is not
             * high enough to let us connect to these downstairs.
             */
            error!(
                self.log,
                "found/using gen number {}, larger than requested: {}",
                max_gen,
                requested_gen,
            );
            bail!(
                "found/using gen number {}, larger than requested: {}",
                max_gen,
                requested_gen,
            );
        } else {
            info!(
                self.log,
                "Generation requested: {} >= found:{}", requested_gen, max_gen,
            );
        }

        /*
         * Set the next flush ID so we have if we need to repair.
         */
        {
            let mut fi = self.flush_info.lock().await;
            fi.next_flush = max_flush;
        }
        info!(self.log, "Next flush: {}", max_flush);

        /*
         * Determine what extents don't match and what to do
         * about that
         */
        let reconcile_list = self.mismatch_list(ds);
        if let Some(reconcile_list) = reconcile_list {
            /*
             * We transition all the downstairs to needing repair here
             * while we have the downstairs lock.  This will insure that
             * all downstairs enter the repair path.
             */
            ds.ds_state.iter_mut().for_each(|ds_state| {
                info!(self.log, "Transition from {:?} to Repair", *ds_state);
                /*
                 * This is a panic and not an error because we should
                 * not call this method without already verifying the
                 * downstairs are in the proper state.
                 */
                assert_eq!(*ds_state, DsState::WaitQuorum);
                *ds_state = DsState::Repair;
            });

            info!(
                self.log,
                "Found {:?} extents that need repair",
                reconcile_list.mend.len()
            );
            ds.convert_rc_to_messages(reconcile_list.mend, max_flush, max_gen);
            ds.reconcile_repair_needed = ds.reconcile_task_list.len();
            Ok(true)
        } else {
            info!(self.log, "All extents match");
            Ok(false)
        }
    }

    /**
     * This is where an upstairs task will coordinate the actual work of
     * making all downstairs look like each other.  The repair work
     * list has already been constructed.
     *
     * The basic loop here is
     * - take a job from the front of the reconcile_task_list and set
     *   reconcile_current_work to that job.
     * - Send the three downstairs a message saying they should look for
     *   work.
     * - Wait for the third downstairs to finish to ACK the message.
     *
     * In addition to the regular steps above, we have to continually check
     * that none of the downstairs have gone away while we were waiting
     * for an ACK, or before submitting more work.  If we do encounter such
     * a situation, we have to inform the other downstairs that we are
     * aborting this repair, clear the repair work, and return error.
     */
    async fn do_reconciliation(
        &self,
        dst: &[Target],
        lastcast: &mut u64,
        ds_reconcile_done_rx: &mut mpsc::Receiver<Repair>,
        repair_commands: usize,
    ) -> Result<()> {
        let mut completed = 0;
        info!(self.log, "Begin repair with {} commands", repair_commands);
        let repair_start = Instant::now();
        loop {
            /*
             * If we get an error here, all Downstairs have to reset and
             * we should clear the work queue as everything starts over
             * after getting the latest state from each downstairs and
             * building a new repair list.
             */
            let res = self.new_rec_work().await;
            match res {
                Ok(true) => {
                    send_reconcile_work(dst, *lastcast);
                    *lastcast += 1;
                    info!(self.log, "Sent repair work, now wait for resp");
                    let mut progress_check = deadline_secs(5);

                    /*
                     * What to do if a downstairs goes away and never
                     * comes back?  At some point we will either need to
                     * give up, or accept an abort signal from outside to
                     * help us get moving again. TODO!
                     */
                    let mut work_done = false;
                    while !work_done {
                        tokio::select! {
                            c = ds_reconcile_done_rx.recv() => {
                                if let Some(c) = c {
                                    info!(
                                        self.log,
                                        "Completion from [{}] id:{} status:{}",
                                        c.client_id, c.rep_id, c.repair,
                                    );
                                    work_done = true;
                                    completed += 1;
                                    self.downstairs
                                        .lock()
                                        .await
                                        .reconcile_repaired += 1;
                                } else {
                                    info!(
                                        self.log,
                                        "Got None from reconcile_done_rx"
                                    );
                                }
                            }
                            _ = sleep_until(progress_check) => {
                                /*
                                 * This may not be an error.  We check every
                                 * so often just to make sure a downstairs
                                 * did not go away while we were waiting for
                                 * an ACK from that downstairs.
                                 */
                                info!(self.log, "progress_check");
                                progress_check = deadline_secs(5);
                                self.ds_state_show().await;
                                let mut ds = self.downstairs.lock().await;
                                if let Err(e) = ds.repair_or_abort() {
                                    error!(self.log, "Aborting reconcile");
                                    /*
                                     * After abort, we send one last message to
                                     * all the downstairs which will trigger
                                     * any that were waiting for more work
                                     * to also abort.
                                     */
                                    send_reconcile_work(dst, *lastcast);
                                    *lastcast += 1;
                                    bail!("Timeout with {}", e);
                                }
                            }
                        }
                        self.stat_update("repair").await;
                        info!(
                            self.log,
                            "[{}/{}] Repair commands completed",
                            completed,
                            repair_commands,
                        );
                    }
                }
                Ok(false) => {
                    break;
                }
                Err(e) => {
                    error!(self.log, "Aborting reconcile");
                    /*
                     * After aborting, we send one last message to
                     * all the downstairs which will trigger any that
                     * were waiting for more work to abort.
                     */
                    send_reconcile_work(dst, *lastcast);
                    *lastcast += 1;
                    bail!("Error: {}", e);
                }
            }
        }
        let repair_total = repair_start.elapsed();
        let time_f = repair_total.as_secs() as f32
            + (repair_total.subsec_nanos() as f32 / 1e9);

        // An extent repair takes four commands.  To get the number of
        // extents repaired, divide repair_commands by 4
        let repaired = repair_commands / 4;
        let ave = time_f / repaired as f32;
        info!(
            self.log,
            "{} extents repaired in {:5.3} ave:{:6.4}", repaired, time_f, ave,
        );
        self.downstairs.lock().await.reconcile_current_work = None;
        Ok(())
    }

    /**
     * Check and see if there are the required number of downstairs
     * connected to combine the three into a region set.
     *
     * Return false if we are not ready, or if things failed.
     * If we failed, then we will update the DsState for what failed.
     *
     * If we have enough downstairs and we can activate, then we should
     * notify the requestor of activation.
     * If we have enough downstairs and can't activate, then we should
     * also notify the requestor.
     *
     * If we have a problem here, we can't activate the upstairs.
     */
    async fn connect_region_set(
        &self,
        dst: &[Target],
        lastcast: &mut u64,
        ds_reconcile_done_rx: &mut mpsc::Receiver<Repair>,
    ) -> Result<()> {
        /*
         * Reconciliation only happens during initialization.
         * Look at all three downstairs region information collected.
         * Determine the highest flush number and make sure our generation
         * is high enough.
         */
        let need_repair;
        let repair_commands;
        let failed_collate;
        {
            let active = self.active.lock().await;
            if active.up_state != UpState::Initializing {
                return Ok(());
            }
            let mut ds = self.downstairs.lock().await;
            drop(active);
            /*
             * Make sure all downstairs are in the correct state before we
             * proceed.
             */
            let not_ready = ds
                .ds_state
                .iter()
                .filter(|state| **state != DsState::WaitQuorum)
                .count();
            if not_ready > 0 {
                info!(
                    self.log,
                    "Waiting for {} more clients to be ready", not_ready
                );
                return Ok(());
            }

            /*
             * While holding the downstairs lock, we figure out if there is
             * any reconciliation to do, and if so, we build the list of
             * operations that will repair the extents that are not in sync.
             *
             * If we fail to collate, then we need to kick out all the
             * downstairs out, forget any activation requests, and the
             * upstairs goes back to waiting for another activation request.
             */
            match self.collate_downstairs(&mut ds).await {
                Ok(res) => {
                    need_repair = res;
                    failed_collate = false;
                }
                Err(e) => {
                    need_repair = false;
                    failed_collate = true;
                    error!(self.log, "Failed collate with {}", e);
                }
            }
            repair_commands = ds.reconcile_task_list.len();
        }

        if failed_collate {
            // We failed to collate the three downstairs, so we need
            // to reset that activation request, and kick all the downstairs
            // to FailedRepair
            //
            self.set_inactive(CrucibleError::RegionAssembleError).await;
            let _active = self.active.lock().await;
            let mut ds = self.downstairs.lock().await;

            // While collating, downstairs should all be DsState::Repair.
            // As we have released then locked the downstairs, we have to
            // verify that the downstairs are all in the state we expect them
            // to be.  Any change means that downstairs went away, but any
            // downstairs still repairing should be moved to failed repair.
            assert_eq!(ds.ds_active.len(), 0);
            assert_eq!(ds.reconcile_task_list.len(), 0);

            for (i, s) in ds.ds_state.iter_mut().enumerate() {
                if *s == DsState::WaitQuorum {
                    *s = DsState::FailedRepair;
                    warn!(
                        self.log,
                        "Mark {} as FAILED Collate in final check", i
                    );
                } else {
                    warn!(
                        self.log,
                        "downstairs in state {} after failed collate", *s
                    );
                }
            }
        } else if need_repair {
            self.do_reconciliation(
                dst,
                lastcast,
                ds_reconcile_done_rx,
                repair_commands,
            )
            .await?;

            let mut active = self.active.lock().await;
            let mut ds = self.downstairs.lock().await;
            /*
             * Now that we have completed reconciliation, we move all
             * the downstairs to the next state.  If we fail here, it means
             * something interrupted our repair and we have to start over.
             *
             * As we repaired, downstairs should all be DsState::Repair.
             *
             * As we have released the downstairs lock while repairing, we
             * have to verify that the downstairs are all in the state we
             * expect them to be.  Any change means we abort and require
             * all downstairs to reconnect, even if repair work is finished
             * as a disconnected DS does not yet know it is all done with
             * work and could have its state reset to New.
             */

            assert_eq!(ds.ds_active.len(), 0);
            assert_eq!(ds.reconcile_task_list.len(), 0);

            let ready = ds
                .ds_state
                .iter()
                .filter(|s| **s == DsState::Repair)
                .count();

            if ready != 3 {
                /*
                 * Some downstairs was not in the proper state any longer,
                 * so we need to abort this reconciliation and start
                 * everyone over.  Move all the downstairs that thought
                 * they were still repairing to FailedRepair which will
                 * trigger a reconnect.
                 */
                for (i, s) in ds.ds_state.iter_mut().enumerate() {
                    if *s == DsState::Repair {
                        *s = DsState::FailedRepair;
                        warn!(
                            self.log,
                            "Mark {} as FAILED REPAIR in final check", i
                        );
                    }
                }
                /*
                 * We don't exit here, as we want the downstairs to all
                 * be notified there is a need to reset and go back through
                 * repair because someone did not complete it.
                 */
            } else {
                info!(self.log, "All required repair work is now completed");
                info!(
                    self.log,
                    "Set Downstairs and Upstairs active after repairs"
                );
                if active.up_state != UpState::Initializing {
                    bail!("Upstairs in unexpected state while reconciling");
                }

                for s in ds.ds_state.iter_mut() {
                    *s = DsState::Active;
                }
                active.set_active().await?;
                info!(
                    self.log,
                    "{} is now active with session: {}",
                    self.uuid,
                    self.session_id
                );
                self.stats.add_activation().await;
            }
        } else {
            /*
             * No repair was needed, but make sure all DS are in the state
             * we expect them to be.
             */
            let mut active = self.active.lock().await;
            let mut ds = self.downstairs.lock().await;

            let ready = ds
                .ds_state
                .iter()
                .filter(|s| **s == DsState::WaitQuorum)
                .count();

            if ready != 3 {
                bail!("Unexpected Downstairs state after collation.");
            } else {
                info!(self.log, "No repair work was required");
                info!(self.log, "Set Downstairs and Upstairs active");
                if active.up_state != UpState::Initializing {
                    bail!("Upstairs in unexpected state while reconciling");
                }
                for s in ds.ds_state.iter_mut() {
                    *s = DsState::Active;
                }
                active.set_active().await?;
                info!(
                    self.log,
                    "{} is now active with session: {}",
                    self.uuid,
                    self.session_id
                );
                self.stats.add_activation().await;
                info!(self.log, "{} Set Active after no repair", self.uuid);
            }
        }

        /*
         * We have to send a final message that all downstairs who should
         * be waiting in the reconcile task that all repair work is done
         * (even if none was required) and they should proceed to being
         * active and accepting commands on the ds_work_ message channel.
         */
        assert!(self
            .downstairs
            .lock()
            .await
            .reconcile_current_work
            .is_none());

        info!(
            self.log,
            "Notify all downstairs, region set compare is done."
        );
        send_reconcile_work(dst, *lastcast);
        *lastcast += 1;

        Ok(())
    }

    /**
     * Return a copy of the DsState vec.
     * DTraces uses this.
     */
    async fn ds_state_copy(&self) -> Vec<DsState> {
        self.downstairs.lock().await.ds_state.clone()
    }

    /**
     * Return a count of the jobs on the downstairs active list.
     * DTrace uses this.
     */
    async fn ds_work_active(&self) -> u32 {
        self.downstairs.lock().await.ds_active.len() as u32
    }

    /**
     * Return a count of the jobs on the upstairs active list.
     * DTrace uses this.
     */
    async fn up_work_active(&self) -> u32 {
        self.guest.guest_work.lock().await.active.len() as u32
    }

    async fn ds_state_show(&self) {
        let ds = self.downstairs.lock().await;

        let mut state_line = String::new();
        state_line.push_str(&self.uuid.to_string());

        for state in ds.ds_state.iter() {
            state_line.push(' ');
            state_line.push_str(&state.to_string());
        }

        info!(self.log, "{}", state_line);
    }

    /*
     * Move all downstairs to this new state.
     * XXX This may just go away if we don't need it.
     */
    async fn _ds_transition_all(&self, new_state: DsState) {
        let mut ds = self.downstairs.lock().await;

        ds.ds_state.iter_mut().for_each(|ds_state| {
            info!(
                self.log,
                "Transition from {:?} to {:?}", *ds_state, new_state,
            );
            match new_state {
                DsState::Active => {
                    // XXX also possible from Repair
                    assert_eq!(*ds_state, DsState::WaitQuorum);
                    *ds_state = new_state;
                }
                DsState::Deactivated => {
                    *ds_state = new_state;
                }
                _ => {
                    panic!(
                        "Unsupported state transition {:?} -> {:?}",
                        *ds_state, new_state
                    );
                }
            }
        });
    }

    /*
     * Store the downstairs UUID, or compare to what we stored before
     * for a given client ID.  Do a sanity check that this downstairs
     * Region Definition matches the other downstairs.  If we don't have
     * any Region info yet, then use the provided RegionDefinition as
     * the source to compare the other downstairs with.
     */
    async fn add_ds_region(
        &self,
        client_id: u8,
        client_ddef: RegionDefinition,
    ) -> Result<()> {
        info!(self.log, "[{}] Got region def {:?}", client_id, client_ddef);

        if client_ddef.get_encrypted() != self.encryption_context.is_some() {
            bail!("Encryption expectation mismatch!");
        }

        /*
         * TODO(#551) Verify that `client_ddef` makes sense (valid, nonzero
         * block size, etc.)
         */

        /*
         * If this downstairs was previously registered, make sure this
         * connection reports the one the old connection did.
         *
         * XXX The expected per-client UUIDs should eventually be provided
         * when the upstairs stairs. When that happens, they can be
         * verified here.
         */
        let mut ds = self.downstairs.lock().await;
        if let Some(uuid) = ds.ds_uuid.get(&client_id) {
            if *uuid != client_ddef.uuid() {
                panic!(
                    "New client:{} uuid:{}  does not match existing {}",
                    client_id,
                    client_ddef.uuid(),
                    uuid
                );
            } else {
                info!(
                    self.log,
                    "Returning client:{} UUID:{} matches", client_id, uuid
                );
            }
        } else {
            ds.ds_uuid.insert(client_id, client_ddef.uuid());
        }

        let mut ddef = self.ddef.lock().await;

        /*
         * If there is an expected region definition of any kind (either from
         * a previous connection or an expectation that was supplied
         * when this upstairs was created), make sure the new
         * definition matches it.
         *
         * If this upstairs' creator didn't specify any expected values, the
         * first downstairs to connect sets the expected values for the other
         * two.
         */
        if let Some(prev_def) = ddef.get_def() {
            if prev_def.block_size() != client_ddef.block_size()
                || prev_def.extent_size().value
                    != client_ddef.extent_size().value
                || prev_def.extent_size().block_size_in_bytes()
                    != client_ddef.extent_size().block_size_in_bytes()
                || prev_def.extent_count() != client_ddef.extent_count()
            {
                // TODO(#558) Figure out if we can handle this error. Possibly not.
                panic!(
                    "New downstairs region info mismatch {:?} vs. {:?}",
                    *ddef, client_ddef
                );
            }
        }

        *ddef = RegionDefinitionStatus::Received(client_ddef);
        Ok(())
    }

    /*
     * Process a downstairs operation.
     * We have received a response to an IO operation.  Here we take the
     * required action for the upstairs depending on what the operation
     * was and the status it returned.
     *
     * Returns true if the guest should be notified.
     */
    async fn process_ds_operation(
        &self,
        ds_id: u64,
        client_id: u8,
        read_data: Result<Vec<ReadResponse>, CrucibleError>,
    ) -> Result<bool> {
        /*
         * We can't get the upstairs state lock when holding the downstairs
         * lock, but we need to make decisions about this downstairs work
         * knowing the upstairs state.  So,
         *  * get the upstairs state lock,
         *  * get the downstairs lock,
         *  * Store the upstairs state.
         *  * Release the upstairs lock.
         * Since we know the upstairs state can't change out of
         * deactivation without downstairs approval (which comes from
         * this method), we are good to move forward here.
         *
         * If the upstairs state changes to deactivation after we drop the
         * active lock, we don't care because their will be a flush coming
         * to the work queue behind us and we have the downstairs lock.
         */

        let active = self.active.lock().await;
        let mut ds = self.downstairs.lock().await;
        let up_state = active.up_state;
        drop(active);

        /*
         * We can finish the job if the downstairs has gone away, but
         * not if it has gone away then come back, because when it comes
         * back, it will have to replay everything.
         * While the downstairs is away, it's OK to act on the result that
         * we already received, because it may never come back.
         */
        let ds_state = ds.ds_state[client_id as usize];
        if ds_state != DsState::Active && ds_state != DsState::Repair {
            warn!(
                self.log,
                "[{}] {} WARNING finish job {} when downstairs state:{:?}",
                client_id,
                self.uuid,
                ds_id,
                ds_state
            );
        }

        // Mark this ds_id for the client_id as completed.
        let notify_guest = match ds.process_ds_completion(
            ds_id,
            client_id,
            read_data,
            &self.encryption_context,
            up_state,
        ) {
            Err(e) => {
                let job = ds.ds_active.get_mut(&ds_id).unwrap();
                error!(
                    self.log,
                    "[{}] ds_completion error: {:?} j:{} {:?} {:?} ",
                    client_id,
                    e,
                    ds_id,
                    &self.encryption_context,
                    job,
                );
                return Err(e);
            }
            Ok(ng) => ng,
        };

        // Mark this downstairs as bad if this was a write or flush
        if let Err(err) = ds.client_error(ds_id, client_id) {
            if err == CrucibleError::UpstairsInactive {
                error!(
                    self.log,
                    "Saw CrucibleError::UpstairsInactive on client {}!",
                    client_id
                );
                self.ds_transition_with_lock(
                    ds,
                    up_state,
                    client_id,
                    DsState::Disabled,
                );
            } else if err == CrucibleError::DecryptionError {
                error!(
                    self.log,
                    "Authenticated decryption failed from client id {}!",
                    client_id
                );

                // XXX reconciliation needs to occur, but do we trust that
                // Downstairs anymore? One could imagine setting that untrusted
                // here:
                //
                // ds_transition_with_lock( ...  DsState::Untrusted);
            } else if matches!(err, CrucibleError::SnapshotExistsAlready(_)) {
                // skip
            }
            /*
             * After work.complete, it's possible that the job is gone
             * due to a retire check
             */
            else if let Some(job) = ds.ds_active.get_mut(&ds_id) {
                if matches!(
                    job.work,
                    IOop::Write {
                        dependencies: _,
                        writes: _,
                    } | IOop::Flush {
                        dependencies: _,
                        flush_number: _,
                        gen_number: _,
                        snapshot_details: _,
                    } | IOop::WriteUnwritten {
                        dependencies: _,
                        writes: _,
                    }
                ) {
                    self.ds_transition_with_lock(
                        ds,
                        up_state,
                        client_id,
                        DsState::Failed,
                    );
                }
            }
        }

        Ok(notify_guest)
    }

    async fn ds_set_repair_address(&self, client_id: u8, addr: SocketAddr) {
        let mut ds = self.downstairs.lock().await;
        ds.ds_repair.insert(client_id, addr);
    }

    async fn ds_clear_repair_address(&self, client_id: u8) {
        let mut ds = self.downstairs.lock().await;
        ds.ds_repair.remove(&client_id);
    }
}

#[derive(Debug)]
struct FlushInfo {
    /*
     * The next flush number to use when a Flush is issued.
     */
    next_flush: u64,
}

impl FlushInfo {
    pub fn new() -> FlushInfo {
        FlushInfo { next_flush: 0 }
    }
    /*
     * Upstairs flush_info mutex must be held when calling this.
     * In addition, a downstairs request ID should be obtained at the
     * same time the next flush number is obtained, such that any IO that
     * is given a downstairs request number higher than the request number
     * for the flush will happen after this flush, never before.
     */
    fn get_next_flush(&mut self) -> u64 {
        let id = self.next_flush;
        self.next_flush += 1;
        id
    }
}

/*
 * States a downstairs can be in.
 * XXX This very much still under development. Most of these are place
 * holders and the final set of states will change.
 */
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DsState {
    /*
     * New connection
     */
    New,
    /*
     * Incompatible software version reported.
     */
    BadVersion,
    /*
     * Waiting for activation signal.
     */
    WaitActive,
    /*
     * Waiting for the minimum number of downstairs to be present.
     */
    WaitQuorum,
    /*
     * Incompatible region format reported.
     */
    BadRegion,
    /*
     * We were connected, but did not transition all the way to
     * active before the connection went away. Arriving here means the
     * downstairs has to go back through the whole negotiation process.
     */
    Disconnected,
    /*
     * Comparing downstairs for consistency.
     */
    Verifying,
    /*
     * Downstairs are repairing from each other.
     */
    Repair,
    /*
     * Failed when attempting to make consistent.
     */
    FailedRepair,
    /*
     * Ready for and/or currently receiving IO
     */
    Active,
    /*
     * IO attempts to this downstairs are failing at too high of a
     * rate, or it is not able to keep up, or it is having some
     * error such that we can no longer use it.
     */
    Failed,
    /*
     * This downstairs is being migrated to a new location
     */
    Migrating,
    /*
     * This downstairs was active, but is now no longer connected.
     * We may have work for it in memory, so a replay is possible
     * if this downstairs reconnects in time.
     */
    Offline,
    /*
     * This downstairs was offline but is now back online and we are
     * sending it all the I/O it missed when it was unavailable.
     */
    Replay,
    /*
     * A guest requested deactivation, this downstairs has completed all
     * its outstanding work and is now waiting for the upstairs to
     * transition back to initializing.
     */
    Deactivated,
    /*
     * Another Upstairs has connected and is now active.
     */
    Disabled,
}
impl fmt::Display for DsState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DsState::New => {
                write!(f, "New")
            }
            DsState::BadVersion => {
                write!(f, "BadVersion")
            }
            DsState::WaitActive => {
                write!(f, "WaitActive")
            }
            DsState::WaitQuorum => {
                write!(f, "WaitQuorum")
            }
            DsState::BadRegion => {
                write!(f, "BadRegion")
            }
            DsState::Disconnected => {
                write!(f, "Disconnected")
            }
            DsState::Verifying => {
                write!(f, "Verifying")
            }
            DsState::Repair => {
                write!(f, "Repair")
            }
            DsState::FailedRepair => {
                write!(f, "FailedRepair")
            }
            DsState::Active => {
                write!(f, "Active")
            }
            DsState::Failed => {
                write!(f, "Failed")
            }
            DsState::Migrating => {
                write!(f, "Migrating")
            }
            DsState::Offline => {
                write!(f, "Offline")
            }
            DsState::Replay => {
                write!(f, "Replay")
            }
            DsState::Deactivated => {
                write!(f, "Deactivated")
            }
            DsState::Disabled => {
                write!(f, "Disabled")
            }
        }
    }
}

/*
 * A unit of work for downstairs that is put into the hashmap.
 */
#[derive(Debug)]
struct DownstairsIO {
    ds_id: u64,    // This MUST match our hashmap index
    guest_id: u64, // The hahsmap ID from the parent guest work.
    work: IOop,

    /*
     * Hash of work status where key is the downstairs "client id" and the
     * hash value is the current state of the IO request with respect to the
     * upstairs.
     * The length and keys on this hashmap will be used to determine which
     * downstairs will receive the IO request.
     * XXX Determine if it is required for all downstairs to get an entry
     * or if by not putting a downstairs in the hash, if that is valid.
     */
    state: HashMap<u8, IOState>,

    /*
     * Has this been acked to the guest yet?
     */
    ack_status: AckStatus,

    /*
     * Is this a replay job, meaning we may have already sent it
     * once.  At the present, this only matters for reads and for
     * when we are comparing read hashes between the three downstairs.
     */
    replay: bool,

    /*
     * If the operation is a Read, this holds the resulting buffer
     * The hashes vec holds the valid hash(es) for the read.
     */
    data: Option<Vec<ReadResponse>>,
    read_response_hashes: Vec<Option<u64>>,

    impacted_blocks: ImpactedBlocks,
}

impl DownstairsIO {
    fn state_count(&self) -> WorkCounts {
        let mut wc: WorkCounts = Default::default();

        for state in self.state.values() {
            match state {
                IOState::New | IOState::InProgress => wc.active += 1,
                IOState::Error(_) => wc.error += 1,
                IOState::Skipped => wc.skipped += 1,
                IOState::Done => wc.done += 1,
            }
        }

        wc
    }

    /*
     * Return the size of the IO in bytes
     * Depending on the IO (write or read) we have to look in a different
     * location to get the size.
     */
    pub fn io_size(&self) -> usize {
        match &self.work {
            IOop::Write {
                dependencies: _,
                writes,
            } => writes.iter().map(|w| w.data.len()).sum(),
            IOop::WriteUnwritten {
                dependencies: _,
                writes,
            } => writes.iter().map(|w| w.data.len()).sum(),
            IOop::Flush {
                dependencies: _,
                flush_number: _flush_number,
                gen_number: _,
                snapshot_details: _,
            } => 0,
            IOop::Read {
                dependencies: _,
                requests: _,
            } => {
                if self.data.is_some() {
                    let rrs = self.data.as_ref().unwrap();
                    rrs.iter().map(|r| r.data.len()).sum()
                } else {
                    0
                }
            }
        }
    }
}

#[derive(Debug)]
struct ReconcileIO {
    id: u64,
    op: Message,
    state: HashMap<u8, IOState>,
}

impl ReconcileIO {
    fn new(id: u64, op: Message) -> ReconcileIO {
        let mut state = HashMap::new();
        for cl in 0..3 {
            state.insert(cl, IOState::New);
        }
        ReconcileIO { id, op, state }
    }
}
/*
 * Crucible to storage IO operations.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum IOop {
    Write {
        dependencies: Vec<u64>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    WriteUnwritten {
        dependencies: Vec<u64>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    Read {
        dependencies: Vec<u64>, // Jobs that must finish before this
        requests: Vec<ReadRequest>,
    },
    Flush {
        dependencies: Vec<u64>, // Jobs that must finish before this
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
    },
}

impl IOop {
    pub fn deps(&self) -> &Vec<u64> {
        match &self {
            IOop::Write {
                dependencies,
                writes: _,
            } => dependencies,
            IOop::Flush {
                dependencies,
                flush_number: _flush_number,
                gen_number: _,
                snapshot_details: _,
            } => dependencies,
            IOop::Read {
                dependencies,
                requests: _,
            } => dependencies,
            IOop::WriteUnwritten {
                dependencies,
                writes: _,
            } => dependencies,
        }
    }

    pub fn is_read(&self) -> bool {
        matches!(self, IOop::Read { .. })
    }

    pub fn is_write(&self) -> bool {
        matches!(self, IOop::Write { .. } | IOop::WriteUnwritten { .. })
    }

    pub fn is_flush(&self) -> bool {
        matches!(self, IOop::Flush { .. })
    }
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 */
#[derive(Debug, Clone, PartialEq)]
pub enum IOState {
    // A new IO request.
    New,
    // The request has been sent to this tasks downstairs.
    InProgress,
    // The successful response came back from downstairs.
    Done,
    // The IO request should be ignored. Ex: we could be doing recovery and
    // we only want a specific downstairs to do that work.
    Skipped,
    // The IO returned an error.
    Error(CrucibleError),
}

impl fmt::Display for IOState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Make sure to right-align output on 4 characters
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
            IOState::Error(_) => {
                write!(f, " Err")
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize)]
struct IOStateCount {
    new: [u32; 3],
    in_progress: [u32; 3],
    done: [u32; 3],
    skipped: [u32; 3],
    error: [u32; 3],
}

impl IOStateCount {
    fn new() -> IOStateCount {
        IOStateCount {
            new: [0; 3],
            in_progress: [0; 3],
            done: [0; 3],
            skipped: [0; 3],
            error: [0; 3],
        }
    }

    fn show_all(&mut self) {
        println!("   STATES      DS:0   DS:1   DS:2   TOTAL");
        self.show(IOState::New);
        self.show(IOState::InProgress);
        self.show(IOState::Done);
        self.show(IOState::Skipped);
        let e = CrucibleError::GenericError("x".to_string());
        self.show(IOState::Error(e));
    }

    fn show(&mut self, state: IOState) {
        let state_stat;
        match state {
            IOState::New => {
                state_stat = self.new;
                print!("    New        ");
            }
            IOState::InProgress => {
                state_stat = self.in_progress;
                print!("    Sent       ");
            }
            IOState::Done => {
                state_stat = self.done;
                print!("    Done       ");
            }
            IOState::Skipped => {
                state_stat = self.skipped;
                print!("    Skipped    ");
            }
            IOState::Error(_) => {
                state_stat = self.error;
                print!("    Error      ");
            }
        }
        let mut sum = 0;
        for ds_stat in state_stat {
            print!("{:4}   ", ds_stat);
            sum += ds_stat;
        }
        println!("{:4}", sum);
    }

    pub fn incr(&mut self, state: &IOState, cid: u8) {
        assert!(cid < 3);
        let cid = cid as usize;
        match state {
            IOState::New => {
                self.new[cid] += 1;
            }
            IOState::InProgress => {
                self.in_progress[cid] += 1;
            }
            IOState::Done => {
                self.done[cid] += 1;
            }
            IOState::Skipped => {
                self.skipped[cid] += 1;
            }
            IOState::Error(_) => {
                self.error[cid] += 1;
            }
        }
    }

    pub fn decr(&mut self, state: &IOState, cid: u8) {
        assert!(cid < 3);
        let cid = cid as usize;
        match state {
            IOState::New => {
                self.new[cid] -= 1;
            }
            IOState::InProgress => {
                self.in_progress[cid] -= 1;
            }
            IOState::Done => {
                self.done[cid] -= 1;
            }
            IOState::Skipped => {
                self.skipped[cid] -= 1;
            }
            IOState::Error(_) => {
                self.error[cid] -= 1;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AckStatus {
    NotAcked,
    AckReady,
    Acked,
}

impl fmt::Display for AckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Make sure to right-align output on 8 characters to match with
        // show_all_work
        match self {
            AckStatus::NotAcked => {
                write!(f, "{0:>8}", "NotAcked")
            }
            AckStatus::AckReady => {
                write!(f, "{0:>8}", "AckReady")
            }
            AckStatus::Acked => {
                write!(f, "{0:>8}", "Acked")
            }
        }
    }
}

/*
 * Provides a shared Buffer that Read operations will write into.
 *
 * Originally BytesMut was used here, but it didn't guarantee that memory
 * was shared between cloned BytesMut objects. Additionally, we added the
 * idea of ownership and that necessitated another field.
 */
#[derive(Clone, Debug)]
pub struct Buffer {
    len: usize,
    data: Arc<Mutex<Vec<u8>>>,
    owned: Arc<Mutex<Vec<bool>>>,
}

impl Buffer {
    pub fn from_vec(vec: Vec<u8>) -> Buffer {
        let len = vec.len();
        Buffer {
            len,
            data: Arc::new(Mutex::new(vec)),
            owned: Arc::new(Mutex::new(vec![false; len])),
        }
    }

    pub fn new(len: usize) -> Buffer {
        Buffer {
            len,
            data: Arc::new(Mutex::new(vec![0; len])),
            owned: Arc::new(Mutex::new(vec![false; len])),
        }
    }

    pub fn from_slice(buf: &[u8]) -> Buffer {
        let mut vec = Vec::<u8>::with_capacity(buf.len());
        for item in buf {
            vec.push(*item);
        }

        Buffer::from_vec(vec)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub async fn as_vec(&self) -> MutexGuard<'_, Vec<u8>> {
        self.data.lock().await
    }

    pub async fn owned_vec(&self) -> MutexGuard<'_, Vec<bool>> {
        self.owned.lock().await
    }
}

#[tokio::test]
async fn test_buffer_len() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);
}

#[tokio::test]
async fn test_buffer_len_after_clone() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);

    let new_buffer = data.clone();
    assert_eq!(new_buffer.len(), READ_SIZE);
    assert_eq!(data.len(), READ_SIZE);
}

#[tokio::test]
#[should_panic(
    expected = "index out of bounds: the len is 512 but the index is 512"
)]
async fn test_buffer_len_index_overflow() {
    const READ_SIZE: usize = 512;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);

    let mut vec = data.as_vec().await;
    assert_eq!(vec.len(), 512);

    for i in 0..(READ_SIZE + 1) {
        vec[i] = 0x99;
    }
}

#[tokio::test]
async fn test_buffer_len_over_block_size() {
    const READ_SIZE: usize = 600;
    let data = Buffer::from_slice(&[0x99; READ_SIZE]);
    assert_eq!(data.len(), READ_SIZE);
}

/*
 * Inspired from Propolis block.rs
 *
 * The following are the operations that Crucible supports from outside
 * callers. We have extended this to cover a bunch of test operations as
 * well. The first three are the supported operations, the other operations
 * tell the upstairs to behave in specific ways.
 */
#[derive(Debug, Clone)]
pub enum BlockOp {
    Read {
        offset: Block,
        data: Buffer,
    },
    Write {
        offset: Block,
        data: Bytes,
    },
    WriteUnwritten {
        offset: Block,
        data: Bytes,
    },
    Flush {
        snapshot_details: Option<SnapshotDetails>,
    },
    GoActive,
    GoActiveWithGen {
        gen: u64,
    },
    Deactivate,
    // Query ops
    QueryBlockSize {
        data: Arc<Mutex<u64>>,
    },
    QueryTotalSize {
        data: Arc<Mutex<u64>>,
    },
    QueryGuestIOReady {
        data: Arc<Mutex<bool>>,
    },
    QueryUpstairsUuid {
        data: Arc<Mutex<Uuid>>,
    },
    // Begin testing options.
    QueryExtentSize {
        data: Arc<Mutex<Block>>,
    },
    QueryWorkQueue {
        data: Arc<Mutex<WQCounts>>,
    },
    // Send an update to all tasks that there is work on the queue.
    Commit,
    // Show internal work queue, return outstanding IO requests.
    ShowWork {
        data: Arc<Mutex<WQCounts>>,
    },
}

macro_rules! ceiling_div {
    ($a: expr, $b: expr) => {
        ($a + ($b - 1)) / $b
    };
}

impl BlockOp {
    /*
     * Compute number of IO operations represented by this BlockOp, rounding
     * up. For example, if IOP size is 16k:
     *
     *   A read of 8k is 1 IOP
     *   A write of 16k is 1 IOP
     *   A write of 16001b is 2 IOPs
     *   A flush isn't an IOP
     *
     * We are not counting WriteUnwritten ops as IO toward the users IO
     * limits.  Though, if too many volumes are created with scrubbers
     * running, we may have to revisit that.
     */
    pub async fn iops(&self, iop_sz: usize) -> Option<usize> {
        match self {
            BlockOp::Read { offset: _, data } => {
                Some(ceiling_div!(data.len(), iop_sz))
            }
            BlockOp::Write { offset: _, data } => {
                Some(ceiling_div!(data.len(), iop_sz))
            }
            _ => None,
        }
    }

    pub fn consumes_iops(&self) -> bool {
        matches!(
            self,
            BlockOp::Read { offset: _, data: _ }
                | BlockOp::Write { offset: _, data: _ }
        )
    }

    // Return the total size of this BlockOp
    pub async fn sz(&self) -> Option<usize> {
        match self {
            BlockOp::Read { offset: _, data } => Some(data.len()),
            BlockOp::Write { offset: _, data } => Some(data.len()),
            _ => None,
        }
    }
}

#[tokio::test]
async fn test_return_iops() {
    const IOP_SZ: usize = 16000;

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(1),
    };
    assert_eq!(op.iops(IOP_SZ).await.unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(8000),
    };
    assert_eq!(op.iops(IOP_SZ).await.unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(16000),
    };
    assert_eq!(op.iops(IOP_SZ).await.unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(16001),
    };
    assert_eq!(op.iops(IOP_SZ).await.unwrap(), 2);
}

/*
 * This structure is for tracking the underlying storage side operations
 * that map to a single Guest IO request. G to S stands for Guest
 * to Storage.
 *
 * The submitted hashmap is indexed by the request number (ds_id) for the
 * downstairs requests issued on behalf of this request.
 */
#[derive(Debug)]
struct GtoS {
    /*
     * Jobs we have submitted (or will soon submit) to the storage side
     * of the upstairs process to send on to the downstairs.
     * The key for the hashmap is the ds_id number in the hashmap for
     * downstairs work. The value is the buffer size of the operation in
     * blocks.
     */
    submitted: HashMap<u64, u64>,
    completed: Vec<u64>,

    /*
     * This buffer is provided by the guest request. If this is a read,
     * data will be written here.
     */
    guest_buffer: Option<Buffer>,

    /*
     * When we have an IO between the guest and crucible, it's possible
     * it will be broken into two smaller requests if the range happens
     * to cross an extent boundary. This hashmap is a list of those
     * buffers with the key being the downstairs request ID.
     *
     * Data moving in/out of this buffer will be encrypted or decrypted
     * depending on the operation.
     */
    downstairs_buffer: HashMap<u64, Vec<ReadResponse>>,

    /*
     * Notify the caller waiting on the job to finish.
     * This is an Option for the case where we want to send an IO on behalf
     * of the Upstairs (not guest driven). Right now the only case where we
     * need that is to flush data to downstairs when the guest has not sent
     * us a flush in some time.  This allows us to free internal buffers.
     * If the sender is None, we know it's a request from the Upstairs and
     * we don't have to ACK it to anyone.
     */
    req: Option<BlockReq>,
}

impl GtoS {
    pub fn new(
        submitted: HashMap<u64, u64>,
        completed: Vec<u64>,
        guest_buffer: Option<Buffer>,
        downstairs_buffer: HashMap<u64, Vec<ReadResponse>>,
        req: Option<BlockReq>,
    ) -> GtoS {
        GtoS {
            submitted,
            completed,
            guest_buffer,
            downstairs_buffer,
            req,
        }
    }

    /*
     * When all downstairs jobs have completed, and all buffers have been
     * attached to the GtoS struct, we can do the final copy of the data
     * from upstairs memory back to the guest's memory.
     */
    #[instrument]
    async fn transfer(&mut self) {
        if let Some(guest_buffer) = &mut self.guest_buffer {
            self.completed.sort_unstable();
            assert!(!self.completed.is_empty());

            let mut offset = 0;
            let mut vec = guest_buffer.as_vec().await;
            let mut owned_vec = guest_buffer.owned_vec().await;

            for ds_id in self.completed.iter() {
                let responses = self.downstairs_buffer.remove(ds_id).unwrap();

                for response in responses {
                    // Copy over into guest memory.
                    {
                        let _ignored =
                            span!(Level::TRACE, "copy to guest buffer")
                                .entered();

                        for i in &response.data {
                            vec[offset] = *i;
                            owned_vec[offset] =
                                !response.block_contexts.is_empty();
                            offset += 1;
                        }
                    }
                }
            }
        } else {
            /*
             * Should this panic?  If the caller is requesting a transfer,
             * the guest_buffer should exist. If it does not exist, then
             * either there is a real problem, or the operation was a write
             * or flush and why are we requesting a transfer for those.
             */
            panic!("No guest buffer, no copy");
        }
    }

    /*
     * Notify corresponding BlockReqWaiter
     */
    pub async fn notify(self, result: Result<(), CrucibleError>) {
        /*
         * If present, send the result to the guest.  If this is a flush
         * issued on behalf of crucible, then there is no place to send
         * a result to.
         *
         * XXX: If the guest is no longer listening and this returns an
         * error, do we care?  This could happen if the guest has
         * given up because an IO took too long, or other possible
         * guest side reasons.
         */
        if let Some(req) = self.req {
            req.send_result(result).await;
        }
    }
}

/**
 * This structure keeps track of work that Crucible has accepted from the
 * "Guest", aka, Propolis.
 *
 * The active is a hashmap of GtoS structures for all I/Os that are
 * outstanding. Either just created or in progress operations. The key
 * for a new job comes from next_gw_id and should always increment.
 *
 * Once we have decided enough downstairs requests are finished, we remove
 * the entry from the active and add the gw_id to the completed vec.
 *
 * TODO: The completed needs to implement some notify back to the Guest, and
 * it should probably be a ring buffer.
 */
#[derive(Debug)]
struct GuestWork {
    active: HashMap<u64, GtoS>,
    next_gw_id: u64,
    completed: AllocRingBuffer<u64>,
}

impl GuestWork {
    fn next_gw_id(&mut self) -> u64 {
        let id = self.next_gw_id;
        self.next_gw_id += 1;
        id
    }

    /*
     * When the required number of completions for a downstairs
     * ds_id have arrived, we call this method on the parent GuestWork
     * that requested them and include the Option<Bytes> from the IO.
     *
     * If this operation was a read, then we attach the Bytes read to the
     * GtoS struct for later transfer.
     *
     * A single GtoS job may have multiple downstairs jobs it created, so
     * we may not be done yet. When the required number of completions have
     * arrived from all the downstairs jobs we created, then we
     * can move forward with finishing up the guest work operation.
     * This may include moving/decrypting data buffers from completed reads.
     */
    #[instrument]
    async fn gw_ds_complete(
        &mut self,
        gw_id: u64,
        ds_id: u64,
        data: Option<Vec<ReadResponse>>,
        result: Result<(), CrucibleError>,
    ) {
        /*
         * A gw_id that already finished and results were sent back to
         * the guest could still have an outstanding ds_id.
         */
        if let Some(mut gtos_job) = self.active.remove(&gw_id) {
            /*
             * If the ds_id is on the submitted list, then we will take it
             * off and, if it is a read, add the read result
             * buffer to the gtos job structure for later
             * copying.
             */
            if gtos_job.submitted.remove(&ds_id).is_some() {
                if let Some(data) = data {
                    /*
                     * The first read buffer will become the source for the
                     * final response back to the guest. This buffer will be
                     * combined with other buffers if the upstairs request
                     * required multiple jobs.
                     */
                    if gtos_job.downstairs_buffer.insert(ds_id, data).is_some()
                    {
                        /*
                         * Only the first successful read should fill the
                         * slot in the downstairs buffer for a ds_id. If
                         * more than one is trying to, then we have a
                         * problem.
                         */
                        panic!(
                            "gw_id:{} read buffer already present for {}",
                            gw_id, ds_id
                        );
                    }
                }

                gtos_job.completed.push(ds_id);
            } else {
                println!("gw_id:{} ({}) already removed???", gw_id, ds_id);
                assert!(gtos_job.completed.contains(&ds_id));
                panic!(
                    "{} Attempting to complete ds_id {} we already completed",
                    gw_id, ds_id
                );
            }

            /*
             * Copy (if present) read data back to the guest buffer they
             * provided to us, and notify any waiters.
             */
            assert!(gtos_job.submitted.is_empty());
            if result.is_ok() && gtos_job.guest_buffer.is_some() {
                gtos_job.transfer().await;
            }

            gtos_job.notify(result).await;

            self.completed.push(gw_id);
        } else {
            /*
             * XXX This is just so I can see if ever does happen.
             */
            panic!(
                "gw_id {} from removed job {} not on active list",
                gw_id, ds_id
            );
        }
    }
}

/**
 * This is the structure we use to keep track of work passed into crucible
 * from the "Guest".
 *
 * Requests from the guest are put into the reqs VecDeque initially.
 *
 * A task on the Crucible side will receive a notification that a new
 * operation has landed on the reqs queue and will take action:
 *
 * * Pop the request off the reqs queue.
 *
 * * Copy (and optionally encrypt) any data buffers provided to us by the
 *   Guest.
 *
 * * Create one or more downstairs DownstairsIO structures.
 *
 * * Create a GtoS tracking structure with the id's for each downstairs task
 *   and the read result buffer if required.
 *
 * * Add the GtoS struct to the in GuestWork active work hashmap.
 *
 * * Put all the DownstairsIO structures on the downstairs work queue.
 *
 * * Send notification to the upstairs tasks that there is new work.
 *
 * Work here will be added to storage side queues and the responses will
 * be waited on and processed when they arrive.
 *
 * This structure and operations on in handle the translation between
 * outside requests and internal upstairs structures and work queues.
 */
#[derive(Debug)]
pub struct Guest {
    /*
     * New requests from outside go onto this VecDeque. The notify is how
     * the submission task tells the listening task that new work has been
     * added.
     */
    reqs: Mutex<VecDeque<BlockReq>>,
    notify: Notify,

    /*
     * When the crucible listening task has noticed a new IO request, it
     * will pull it from the reqs queue and create an GuestWork struct
     * as well as convert the new IO request into the matching
     * downstairs request(s). Each new GuestWork request will get a
     * unique gw_id, which is also the index for that operation into the
     * hashmap.
     *
     * It is during this process that data will encrypted. For a read, the
     * data is decrypted back to the guest provided buffer after all the
     * required downstairs operations are completed.
     */
    guest_work: Mutex<GuestWork>,

    /*
     * Setting an IOP limit means that the rate at which block reqs are
     * pulled off will be limited. No setting means they are sent right
     * away.
     */
    iop_tokens: Mutex<usize>,
    bytes_per_iop: Option<usize>,
    iop_limit: Option<usize>,

    /*
     * Setting a bandwidth limit will also limit the rate at which block
     * reqs are pulled off the queue.
     */
    bw_tokens: Mutex<usize>, // bytes
    bw_limit: Option<usize>, // bytes per second
}

/*
 * These methods are how to add or checking for new work on the Guest struct
 */
impl Guest {
    pub fn new() -> Guest {
        Guest {
            /*
             * Incoming I/O requests are added to this queue.
             */
            reqs: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            /*
             * The active hashmap is for in-flight I/O operations
             * that we have taken off the incoming queue, but we have not
             * received the response from downstairs.
             * Note that a single IO from outside may have multiple I/O
             * requests that need to finish before we can complete that IO.
             */
            guest_work: Mutex::new(GuestWork {
                active: HashMap::new(), // GtoS
                next_gw_id: 1,
                completed: AllocRingBuffer::with_capacity(2048),
            }),

            iop_tokens: Mutex::new(0),
            bytes_per_iop: None,
            iop_limit: None,

            bw_tokens: Mutex::new(0),
            bw_limit: None,
        }
    }

    pub fn set_iop_limit(&mut self, bytes_per_iop: usize, limit: usize) {
        self.bytes_per_iop = Some(bytes_per_iop);
        self.iop_limit = Some(limit);
    }

    /*
     * Return IOPs per second
     */
    pub fn get_iop_limit(&self) -> Option<usize> {
        self.iop_limit
    }

    pub fn set_bw_limit(&mut self, bytes_per_second: usize) {
        self.bw_limit = Some(bytes_per_second);
    }

    pub fn get_bw_limit(&self) -> Option<usize> {
        self.bw_limit
    }

    /*
     * This is used to submit a new BlockOp IO request to Crucible.
     */
    async fn send(&self, op: BlockOp) -> BlockReqWaiter {
        let (send, recv) = mpsc::channel(1);

        self.reqs.lock().await.push_back(BlockReq::new(op, send));
        self.notify.notify_one();

        BlockReqWaiter::new(recv)
    }

    /*
     * A crucible task will listen for new work using this.
     */
    async fn recv(&self) -> BlockReq {
        loop {
            if let Some(req) = self.consume_req().await {
                return req;
            }

            self.notify.notified().await;
        }
    }

    /*
     * Consume one request off queue if it is under the IOP limit and the BW
     * limit.
     */
    async fn consume_req(&self) -> Option<BlockReq> {
        let mut reqs = self.reqs.lock().await;

        // TODO exposing queue depth here would be a good metric for disk
        // contention

        // Check if no requests are queued
        if reqs.is_empty() {
            return None;
        }

        let req_ref: &BlockReq = reqs.front().unwrap();

        // Check if we can consume right away
        let iop_limit_applies =
            self.iop_limit.is_some() && req_ref.op.consumes_iops();
        let bw_limit_applies =
            self.bw_limit.is_some() && req_ref.op.sz().await.is_some();

        if !iop_limit_applies && !bw_limit_applies {
            return Some(reqs.pop_front().unwrap());
        }

        // Check bandwidth limit before IOP limit, but make sure only to consume
        // tokens if both checks pass!

        let mut bw_check_ok = true;
        let mut iop_check_ok = true;

        // XXX if recv ever is called from multiple threads, token locks must be
        // taken for the whole of the procedure, not multiple times in the below
        // if blocks!

        // When checking tokens vs the limit, do not check by checking if adding
        // the block request's values to the applicable limit: this would create
        // a scenario where a large IO enough would stall the pipeline (see
        // test_impossible_io). Instead, check if the limits are already
        // reached.

        if let Some(bw_limit) = self.bw_limit {
            if req_ref.op.sz().await.is_some() {
                let bw_tokens = self.bw_tokens.lock().await;
                if *bw_tokens >= bw_limit {
                    bw_check_ok = false;
                }
            }
        }

        if let Some(iop_limit) = self.iop_limit {
            let bytes_per_iops = self.bytes_per_iop.unwrap();
            if req_ref.op.iops(bytes_per_iops).await.is_some() {
                let iop_tokens = self.iop_tokens.lock().await;
                if *iop_tokens >= iop_limit {
                    iop_check_ok = false;
                }
            }
        }

        // If both checks pass, consume appropriate resources and return the
        // block req
        if bw_check_ok && iop_check_ok {
            if self.bw_limit.is_some() {
                if let Some(sz) = req_ref.op.sz().await {
                    let mut bw_tokens = self.bw_tokens.lock().await;
                    *bw_tokens += sz;
                }
            }

            if self.iop_limit.is_some() {
                let bytes_per_iops = self.bytes_per_iop.unwrap();
                if let Some(req_iops) = req_ref.op.iops(bytes_per_iops).await {
                    let mut iop_tokens = self.iop_tokens.lock().await;
                    *iop_tokens += req_iops;
                }
            }

            return Some(reqs.pop_front().unwrap());
        }

        // Otherwise, don't consume this block req
        None
    }

    /*
     * IOPs are IO operations per second, so leak tokens to allow that
     * through.
     */
    pub async fn leak_iop_tokens(&self, tokens: usize) {
        let mut iop_tokens = self.iop_tokens.lock().await;

        if tokens > *iop_tokens {
            *iop_tokens = 0;
        } else {
            *iop_tokens -= tokens;
        }

        // Notify to wake up recv now that there may be room.
        self.notify.notify_one();
    }

    // Leak bytes from bandwidth tokens
    pub async fn leak_bw_tokens(&self, bytes: usize) {
        let mut bw_tokens = self.bw_tokens.lock().await;

        if bytes > *bw_tokens {
            *bw_tokens = 0;
        } else {
            *bw_tokens -= bytes;
        }

        // Notify to wake up recv now that there may be room.
        self.notify.notify_one();
    }

    pub async fn query_extent_size(&self) -> Result<Block, CrucibleError> {
        let data = Arc::new(Mutex::new(Block::new(0, 9)));
        let extent_query = BlockOp::QueryExtentSize { data: data.clone() };
        self.send(extent_query).await.wait().await?;

        let result = *data.lock().await;
        Ok(result)
    }

    pub async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        let wc = WQCounts {
            up_count: 0,
            ds_count: 0,
        };

        let data = Arc::new(Mutex::new(wc));
        let qwq = BlockOp::QueryWorkQueue { data: data.clone() };
        self.send(qwq).await.wait().await.unwrap();

        let wc = data.lock().await;
        Ok(*wc)
    }

    pub async fn commit(&self) -> Result<(), CrucibleError> {
        self.send(BlockOp::Commit).await.wait().await.unwrap();
        Ok(())
    }
    // Maybe this can just be a guest specific thing, not a BlockIO
    pub async fn activate_with_gen(
        &self,
        gen: u64,
    ) -> Result<(), CrucibleError> {
        let waiter = self.send(BlockOp::GoActiveWithGen { gen }).await;
        println!("The guest has requested activation with gen:{}", gen);
        waiter.wait().await?;
        println!("The guest has finished waiting for activation with:{}", gen);
        Ok(())
    }
}

#[async_trait]
impl BlockIO for Guest {
    async fn activate(&self) -> Result<(), CrucibleError> {
        let waiter = self.send(BlockOp::GoActive).await;
        println!("The guest has requested activation");
        waiter.wait().await?;
        println!("The guest has finished waiting for activation");
        Ok(())
    }

    /// Disable any more IO from this guest and deactivate the downstairs.
    async fn deactivate(&self) -> Result<(), CrucibleError> {
        let waiter = self.send(BlockOp::Deactivate).await;
        waiter.wait().await?;
        Ok(())
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        let data = Arc::new(Mutex::new(false));
        let active_query = BlockOp::QueryGuestIOReady { data: data.clone() };
        self.send(active_query).await.wait().await?;

        let result = *data.lock().await;
        Ok(result)
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        let data = Arc::new(Mutex::new(0));
        let size_query = BlockOp::QueryTotalSize { data: data.clone() };
        self.send(size_query).await.wait().await?;

        let result = *data.lock().await;
        Ok(result)
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        let data = Arc::new(Mutex::new(0));
        let size_query = BlockOp::QueryBlockSize { data: data.clone() };
        self.send(size_query).await.wait().await?;

        let result = *data.lock().await;
        Ok(result)
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        let data = Arc::new(Mutex::new(Uuid::default()));
        let uuid_query = BlockOp::QueryUpstairsUuid { data: data.clone() };
        self.send(uuid_query).await.wait().await?;

        let result = *data.lock().await;
        Ok(result)
    }

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<(), CrucibleError> {
        let bs = self.get_block_size().await?;

        if (data.len() % bs as usize) != 0 {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() as u64 != bs {
            crucible_bail!(BlockSizeMismatch);
        }

        let rio = BlockOp::Read { offset, data };
        Ok(self.send(rio).await.wait().await?)
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        let bs = self.get_block_size().await?;

        if (data.len() % bs as usize) != 0 {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() as u64 != bs {
            crucible_bail!(BlockSizeMismatch);
        }

        let wio = BlockOp::Write { offset, data };
        Ok(self.send(wio).await.wait().await?)
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        let bs = self.get_block_size().await?;

        if (data.len() % bs as usize) != 0 {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() as u64 != bs {
            crucible_bail!(BlockSizeMismatch);
        }

        let wio = BlockOp::WriteUnwritten { offset, data };
        Ok(self.send(wio).await.wait().await?)
    }

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        Ok(self
            .send(BlockOp::Flush { snapshot_details })
            .await
            .wait()
            .await?)
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        // Note: for this implementation, BlockOp::ShowWork will be sent and
        // processed by the Upstairs even if it isn't active.
        let wc = WQCounts {
            up_count: 0,
            ds_count: 0,
        };

        let data = Arc::new(Mutex::new(wc));
        let sw = BlockOp::ShowWork { data: data.clone() };
        self.send(sw).await.wait().await.unwrap();

        let wc = data.lock().await;
        Ok(*wc)
    }
}

/*
 * Work Queue Counts, for debug ShowWork IO type
 */
#[derive(Debug, Copy, Clone)]
pub struct WQCounts {
    pub up_count: usize,
    pub ds_count: usize,
}

impl Default for Guest {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Repair {
    repair: bool,
    client_id: u8,
    rep_id: u64,
}

/**
 * This struct holds the address of a downstairs, and the message channels
 * used to:
 * Send a message there is new work.
 * Send a message there is an activation request from the guest.
 * Send a message there is repair work to do.
 */
pub struct Target {
    target: SocketAddr,
    ds_work_tx: watch::Sender<u64>,
    ds_active_tx: watch::Sender<u64>,
    ds_reconcile_work_tx: watch::Sender<u64>,
}

#[derive(Debug)]
struct Condition {
    target: SocketAddr,
    connected: bool,
    client_id: u8,
}

/**
 * Send work to all the targets.
 * If a send fails, report an error.
 */
fn send_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        let res = d_client.ds_work_tx.send(val);
        if let Err(e) = res {
            println!(
                "ERROR {:#?} Failed to notify {:?} of work {}",
                e, d_client.target, val,
            );
        }
    }
}

/**
 * Send reconcile work to all the targets.
 * If a send fails, report an error.
 */
fn send_reconcile_work(t: &[Target], val: u64) {
    for d_client in t.iter() {
        let res = d_client.ds_reconcile_work_tx.send(val);
        if let Err(e) = res {
            println!(
                "ERROR {:#?} Failed to notify {:?} of reconcile work {}",
                e, d_client.target, val,
            );
        }
    }
}

/**
 * Send active to all the targets.
 * If a send fails, print an error.
 */
fn send_active(t: &[Target], gen: u64) {
    for d_client in t.iter() {
        // println!("#### send to client {:?}", d_client.target);
        let res = d_client.ds_active_tx.send(gen);
        if let Err(e) = res {
            println!(
                "#### error {:#?} Failed 'active' notification to {:?}",
                e, d_client.target
            );
        }
    }
}

/**
 * We listen on the ds_done channel to know when enough of the downstairs
 * requests for a downstairs work task have finished and it is time to
 * complete any buffer transfers (reads) and then notify the guest that
 * their work has been completed.
 */
async fn up_ds_listen(up: &Arc<Upstairs>, mut ds_done_rx: mpsc::Receiver<u64>) {
    /*
     * Accept _any_ ds_done message, but work on the whole list of ackable
     * work.
     */
    while ds_done_rx.recv().await.is_some() {
        /*
         * XXX Do we need to hold the lock while we process all the
         * completed jobs?  We should be continuing to send message over
         * the ds_done_tx channel, so if new things show up while we
         * process the set of things we know are done now, then the
         * ds_done_rx.recv() should trigger when we loop.
         */
        let mut ack_list = up.downstairs.lock().await.ackable_work();
        /*
         * This needs some sort order.  If we are not acking things in job
         * ID order, then we must use a queue or something that will allow
         * the jobs to be acked in the order they were completed on the
         * downstairs.
         */
        ack_list.sort_unstable();

        let mut gw = up.guest.guest_work.lock().await;
        for ds_id_done in ack_list.iter() {
            let mut ds = up.downstairs.lock().await;

            let done = ds.ds_active.get_mut(ds_id_done).unwrap();
            /*
             * Make sure the job state has not changed since we made the
             * list.
             */
            if done.ack_status != AckStatus::AckReady {
                info!(
                    up.log,
                    "Job {} no longer ready, skip for now", ds_id_done
                );
                continue;
            }

            let gw_id = done.guest_id;
            let ds_id = done.ds_id;
            assert_eq!(*ds_id_done, ds_id);

            let io_size = done.io_size();
            let data = done.data.take();

            ds.ack(ds_id);

            gw.gw_ds_complete(gw_id, ds_id, data, ds.result(ds_id))
                .await;

            ds.cdt_gw_work_done(ds_id, gw_id, io_size, &up.stats).await;

            ds.retire_check(ds_id);
        }
    }
    warn!(up.log, "up_ds_listen loop done");
}

/**
 * The upstairs has received a new IO request from the guest. Here we
 * decide what to for that request.
 * For IO operations, we build the downstairs work and if required split
 * the single IO into multiple IOs to the downstairs. Once we have built
 * the work and updated the upstairs and downstairs work queues, we signal
 * to all the downstairs tasks there is new work for them to do.
 *
 * This function can be called before the upstairs is active, so any
 * operation that requires the upstairs to be active should check that
 * and report an error.
 */
async fn process_new_io(
    up: &Arc<Upstairs>,
    dst: &[Target],
    req: BlockReq,
    lastcast: &mut u64,
) {
    /*
     * If any of the submit_* functions fail to send to the downstairs, they
     * return an error.  These are reported to the Guest.
     */
    match req.op() {
        /*
         * These three options can be handled by this task directly,
         * and don't require the upstairs to be fully online.
         */
        BlockOp::GoActive => {
            /*
             * If we are deactivating, then reject this re-connect and
             * let the deactivate finish.
             */
            if let Err(_e) = up.set_active_request(req).await {
                return;
            }
            // Put the req waiter into the upstairs so we have a hook on
            // who to notify when the answer comes back.
            // We must do this before we tell all the tasks for downstairs.
            let gen: u64 = *up.generation.lock().await;
            send_active(dst, gen);
        }
        BlockOp::GoActiveWithGen { gen } => {
            /*
             * If we are deactivating, then reject this re-connect and
             * let the deactivate finish.
             */
            if let Err(_e) = up.set_active_request(req).await {
                return;
            }
            up.set_generation(gen).await;
            send_active(dst, gen);
        }
        BlockOp::QueryGuestIOReady { data } => {
            *data.lock().await = up.guest_io_ready().await;
            req.send_ok().await;
        }
        BlockOp::QueryUpstairsUuid { data } => {
            *data.lock().await = up.uuid;
            req.send_ok().await;
        }
        /*
         * These options are only functional once the upstairs is
         * active and should not be accepted if we are not active.
         */
        BlockOp::Deactivate => {
            info!(up.log, "Request to deactivate this guest");
            /*
             * First do an initial check to make sure we can deactivate.
             * If we can't then return error right away.  If we don't
             * return error here, then we have started the process to
             * deactivation and need to signal all our downstairs that
             * they (may) have a flush to do.
             */
            if up.set_deactivate(Some(req)).await.is_err() {
                return;
            }

            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::Read { offset, data } => {
            if up.submit_read(offset, data, Some(req)).await.is_err() {
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::Write { offset, data } => {
            if up
                .submit_write(offset, data, Some(req), false)
                .await
                .is_err()
            {
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::WriteUnwritten { offset, data } => {
            if up
                .submit_write(offset, data, Some(req), true)
                .await
                .is_err()
            {
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        BlockOp::Flush { snapshot_details } => {
            /*
             * Submit for read and write both check if the upstairs is
             * ready for guest IO or not.  Because the Upstairs itself can
             * call submit_flush, we have to check here that it is okay
             * to accept IO from the guest before calling a guest requested
             * flush command.
             */
            if !up.guest_io_ready().await {
                req.send_err(CrucibleError::UpstairsInactive).await;
                return;
            }

            if up.submit_flush(Some(req), snapshot_details).await.is_err() {
                return;
            }

            send_work(dst, *lastcast);
            *lastcast += 1;
        }
        // Query ops
        BlockOp::QueryBlockSize { data } => {
            let size = match up.ddef.lock().await.get_def() {
                Some(rd) => rd.block_size(),
                None => {
                    warn!(
                        up.log,
                        "Block size not available (active: {})",
                        up.guest_io_ready().await
                    );
                    req.send_err(CrucibleError::PropertyNotAvailable(
                        "block size".to_string(),
                    ))
                    .await;
                    return;
                }
            };
            *data.lock().await = size;
            req.send_ok().await;
        }
        BlockOp::QueryTotalSize { data } => {
            let size = match up.ddef.lock().await.get_def() {
                Some(rd) => rd.total_size(),
                None => {
                    warn!(
                        up.log,
                        "Total size not available (active: {})",
                        up.guest_io_ready().await
                    );
                    req.send_err(CrucibleError::PropertyNotAvailable(
                        "total size".to_string(),
                    ))
                    .await;
                    return;
                }
            };
            *data.lock().await = size;
            req.send_ok().await;
        }
        // Testing options
        BlockOp::QueryExtentSize { data } => {
            // Yes, test only
            let size = match up.ddef.lock().await.get_def() {
                Some(rd) => rd.extent_size(),
                None => {
                    warn!(
                        up.log,
                        "Extent size not available (active: {})",
                        up.guest_io_ready().await
                    );
                    req.send_err(CrucibleError::PropertyNotAvailable(
                        "extent size".to_string(),
                    ))
                    .await;
                    return;
                }
            };
            *data.lock().await = size;
            req.send_ok().await;
        }
        BlockOp::QueryWorkQueue { data } => {
            // TODO should this first check if the Upstairs is active?
            *data.lock().await = WQCounts {
                up_count: up.guest.guest_work.lock().await.active.len(),
                ds_count: up.downstairs.lock().await.ds_active.len(),
            };
            req.send_ok().await;
        }
        BlockOp::ShowWork { data } => {
            // TODO should this first check if the Upstairs is active?
            *data.lock().await = show_all_work(up).await;
            req.send_ok().await;
        }
        BlockOp::Commit => {
            if !up.guest_io_ready().await {
                req.send_err(CrucibleError::UpstairsInactive).await;
                return;
            }
            send_work(dst, *lastcast);
            *lastcast += 1;
        }
    }
}

/**
 * Stat counters struct used by DTrace
 */
#[derive(Clone, Debug, Serialize)]
pub struct Arg {
    up_count: u32,
    ds_count: u32,
    ds_state: Vec<DsState>,
    ds_io_count: IOStateCount,
}

/**
 * This method is a task that will loop forever and wait for three
 * downstairs to get into the ready state. We are notified of that through
 * the ds_status_rx channel.  Once we have three connections, we then also
 * listen for work requests to come over the guest channel.
 *
 * The ds_reconcile_done channel is used during reconciliation to indicate
 * to this task (in the ds_reconcile() function) that a downstairs has
 * completed a reconcile request.
 *
 * This task drives any reconciliation if necessary.  If Repair is required,
 * it happens in three phases.  Typically an interruption of repair will
 * result in things starting over, but if actual repair work to an extent
 * is completed, that extent won't need to be repaired again.
 *
 * The three phases are:
 *
 * Collect:
 * When a Downstairs connects, the Upstairs collects the gen/flush/dirty
 * (GFD) info from all extents.  This GFD information is stored and the
 * Upstairs waits for all three Downstairs to attach.
 *
 * Compare:
 * In the compare phase, the upstairs will walk the list of all extents
 * and compare the G/F/D from each of the downstairs.  When there is a
 * mismatch between downstairs (The dirty bit counts as a mismatch and will
 * force a repair even if generation and flush numbers agree). For each
 * mismatch, the upstairs determines which downstairs has the extent that
 * should be the source, and which of the other downstairs extents needs
 * repair. This list of mismatches (source, destination(s)) is collected.
 * Once an upstairs has compiled its repair list, it will then generates a
 * sequence of Upstairs ->  Downstairs repair commands to repair each
 * extent that needs to be fixed.  For a given piece of repair work, the
 * commands are:
 * - Send a flush to source extent.
 * - Close extent on all downstairs.
 * - Send repair command to destination extents (with source extent
 *   IP/Port).
 * (See DS-DS Repair)
 * - Reopen all extents.
 *
 * Repair:
 * During repair Each command issued from the upstairs must be completed
 * before the next will be sent. The Upstairs is responsible for walking
 * the repair commands and sending them to the required downstairs, and
 * waiting for them to finish.  The actual repair work for an extent
 * takes place on the downstairs being repaired.
 *
 * Repair (ds to ds)
 * Each downstairs runs a repair server (Dropshot) that listens for
 * repair requests from other downstairs.  A downstairs with an extent
 * that needs repair will contact the source downstairs and request the
 * list of files for an extent, then request each file.  Once all files
 * are local to the downstairs needing repair, it will replace the existing
 * extent files with the new ones.
 */
async fn up_listen(
    up: &Arc<Upstairs>,
    dst: Vec<Target>,
    mut ds_status_rx: mpsc::Receiver<Condition>,
    mut ds_reconcile_done_rx: mpsc::Receiver<Repair>,
    timeout: Option<u32>,
) {
    info!(up.log, "up_listen starts"; "task" => "up_listen");
    info!(up.log, "Wait for all three downstairs to come online");
    let flush_timeout = timeout.unwrap_or(5);
    info!(up.log, "Flush timeout: {}", flush_timeout);
    let mut lastcast = 1;

    /*
     * If this guest was configured with an IOPs or BW limit, one branch of
     * the loop below has to leak tokens. Leak every LEAK_MS
     * milliseconds.
     */
    const LEAK_MS: usize = 1000;

    let leak_tick = tokio::time::Duration::from_millis(LEAK_MS as u64);
    let mut leak_deadline = Instant::now().checked_add(leak_tick).unwrap();

    up.stat_update("start").await;
    let mut flush_check = deadline_secs(flush_timeout.into());
    let mut show_work_interval = deadline_secs(5);
    loop {
        /*
         * Wait for all three downstairs to connect (for each region set).
         * Once we have all three, try to reconcile them.
         * Once all downstairs are reconciled, we can start taking IO.
         */
        tokio::select! {
            c = ds_status_rx.recv() => {
                if let Some(c) = c {
                    info!(
                        up.log,
                        "[{}] {:?} new connection:{:?}",
                        c.client_id, c.target, c.connected,
                    );
                    up.ds_state_show().await;
                    /*
                     * If this just connected, see if we now have enough
                     * downstairs to make a valid region set.
                     */
                    if c.connected {
                        if let Err(e) = up.connect_region_set(
                            &dst,
                            &mut lastcast,
                            &mut ds_reconcile_done_rx,
                        ).await {
                            error!(
                                up.log,
                                "Reconciliation attempt reported error {}",
                                e
                            );
                        }
                    } else {
                        info!(
                            up.log,
                            "[{}] goes offline {}",
                            c.client_id, c.target
                        );
                    }
                } else {
                    /*
                     * This message channel should only return None if we
                     * are in the process of shutting down.  Log a message
                     * here and let the shutdown finish.  If there is a
                     * bug somewhere, at least we are leaving this
                     * breadcrumb behind.
                     */
                    info!(up.log, "up_listen reports status_rx -> None ");
                }
            }
            req = up.guest.recv() => {
                process_new_io(up, &dst, req, &mut lastcast).await;
            }
            _ = sleep_until(leak_deadline) => {
                if let Some(iop_limit) = up.guest.get_iop_limit() {
                    let tokens = iop_limit / (1000 / LEAK_MS);
                    up.guest.leak_iop_tokens(tokens).await;
                }

                if let Some(bw_limit) = up.guest.get_bw_limit() {
                    let tokens = bw_limit / (1000 / LEAK_MS);
                    up.guest.leak_bw_tokens(tokens).await;
                }

                leak_deadline = Instant::now().checked_add(leak_tick).unwrap();
            }
            _ = sleep_until(flush_check) => {
                /*
                 * This must fire every "flush_check" seconds to make sure
                 * we don't leave any work in the work queues longer
                 * than necessary.
                 */
                if up.flush_needed().await {
                    if let Err(e) = up.submit_flush(None, None).await {
                        error!(up.log, "flush send failed:{:?}", e);
                        // XXX What to do here?
                    } else {
                        send_work(&dst, 1);
                    }
                }
                /*
                 * Since this should run every time we check for
                 * a flush, we can also use this to update the dtrace
                 * counters with some regularity.
                 */
                up.stat_update("loop").await;

                flush_check = deadline_secs(flush_timeout.into());
            }
            _ = sleep_until(show_work_interval) => {
                // show_all_work(up).await;
                show_work_interval = deadline_secs(5);
            }
        }
    }
}

/*
 * This is the main upstairs task that starts all the other async tasks. The
 * final step is to call up_listen() which will coordinate the connection to
 * the downstairs and start listening for incoming IO from the guest when the
 * time is ready. It will return Ok with a join handle if every required task
 * was successfully launched, and Err otherwise.
 */
pub async fn up_main(
    opt: CrucibleOpts,
    gen: u64,
    region_def: Option<RegionDefinition>,
    guest: Arc<Guest>,
    producer_registry: Option<ProducerRegistry>,
) -> Result<tokio::task::JoinHandle<()>> {
    register_probes().unwrap();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(slog::Level::Info)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let (drain, registration) = with_drain(drain);
    if let ProbeRegistration::Failed(ref e) = registration {
        panic!("Failed to register probes: {:#?}", e);
    }
    let log = Logger::root(drain.fuse(), o!());
    info!(log, "Upstairs starts");

    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let up = Upstairs::new(&opt, gen, region_def, guest, log);

    /*
     * Use this channel to receive updates on target status from each task
     * we create to connect to a downstairs.
     */
    let (ds_status_tx, ds_status_rx) = mpsc::channel::<Condition>(32);

    /*
     * Use this channel to receive updates on the completion of reconcile
     * work requests.
     */
    let (ds_reconcile_done_tx, ds_reconcile_done_rx) =
        mpsc::channel::<Repair>(32);

    /*
     * Use this channel to indicate in the upstairs that all downstairs
     * operations for a specific request have completed.
     */
    let (ds_done_tx, ds_done_rx) = mpsc::channel(500); // XXX 500?

    /*
     * spawn a task to listen for ds completed work which will then
     * take care of transitioning guest work structs to done.
     */
    let upc = Arc::clone(&up);
    tokio::spawn(async move {
        up_ds_listen(&upc, ds_done_rx).await;
    });

    if let Some(pr) = producer_registry {
        let up_oxc = Arc::clone(&up);
        let ups = up_oxc.stats.clone();
        if let Err(e) = pr.register_producer(ups) {
            error!(up_oxc.log, "Failed to register metric producer: {}", e);
        }
    }

    let tls_context = if let Some(cert_pem_path) = opt.cert_pem {
        let key_pem_path = opt.key_pem.unwrap();
        let root_cert_pem_path = opt.root_cert_pem.unwrap();

        let tls_context = crucible_common::x509::TLSContext::from_paths(
            &cert_pem_path,
            &key_pem_path,
            &root_cert_pem_path,
        )?;

        Some(tls_context)
    } else {
        None
    };
    let tls_context = Arc::new(tokio::sync::Mutex::new(tls_context));

    let mut client_id = 0;
    /*
     * Create one downstairs task (dst) for each target in the opt
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
            let (ds_work_tx, ds_work_rx) = watch::channel(1);
            /*
             * Create the channel used to submit reconcile work to each
             * downstairs (when work is required).
             */
            let (ds_reconcile_work_tx, ds_reconcile_work_rx) =
                watch::channel(1);

            // Notify when it's time to go active.
            let (ds_active_tx, ds_active_rx) = watch::channel(0);

            let up = Arc::clone(&up);
            let t0 = *dst;
            let up_coms = UpComs {
                client_id,
                ds_work_rx,
                ds_status_tx: ds_status_tx.clone(),
                ds_done_tx: ds_done_tx.clone(),
                ds_active_rx,
                ds_reconcile_work_rx,
                ds_reconcile_done_tx: ds_reconcile_done_tx.clone(),
            };
            let tls_context = tls_context.clone();
            tokio::spawn(async move {
                looper(t0, tls_context, &up, up_coms).await;
            });
            client_id += 1;

            Target {
                target: *dst,
                ds_work_tx,
                ds_active_tx,
                ds_reconcile_work_tx,
            }
        })
        .collect::<Vec<_>>();

    // Drop here, otherwise receivers will be kept waiting if looper quits
    drop(ds_done_tx);
    drop(ds_status_tx);
    drop(ds_reconcile_done_tx);

    // If requested, start the control http server on the given address:port
    if let Some(control) = opt.control {
        let upi = Arc::clone(&up);
        tokio::spawn(async move {
            let r = control::start(&upi, control).await;
            info!(upi.log, "Control HTTP task finished with {:?}", r);
        });
    }

    let flush_timeout = opt.flush_timeout;
    let join_handle = tokio::spawn(async move {
        /*
         * The final step is to call this function to wait for our downstairs
         * tasks to connect to their respective downstairs instance.
         * Once connected, we then take work requests from the guest and
         * submit them into the upstairs
         */
        up_listen(&up, dst, ds_status_rx, ds_reconcile_done_rx, flush_timeout)
            .await
    });

    Ok(join_handle)
}

/*
 * Create a write DownstairsIO structure from an EID, and offset, and
 * the data buffer
 *
 * The is_write_unwritten bool indicates if this write is a regular
 * write (false) or a write_unwritten write (true) and allows us to
 * construct the proper IOop to submit to the downstairs.
 */
fn create_write_eob(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    writes: Vec<crucible_protocol::Write>,
    is_write_unwritten: bool,
    impacted_blocks: ImpactedBlocks,
) -> DownstairsIO {
    /*
     * Note to self:  Should the dependency list cover everything since
     * the last flush, or everything that is currently outstanding?
     */
    let awrite = if is_write_unwritten {
        IOop::WriteUnwritten {
            dependencies,
            writes,
        }
    } else {
        IOop::Write {
            dependencies,
            writes,
        }
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }

    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: awrite,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

/*
 * Create a write DownstairsIO structure from an EID, and offset, and the
 * data buffer. Used for converting a guest IO read request into a
 * DownstairsIO that the downstairs can understand.
 */
fn create_read_eob(
    ds_id: u64,
    dependencies: Vec<u64>,
    gw_id: u64,
    requests: Vec<ReadRequest>,
    impacted_blocks: ImpactedBlocks,
) -> DownstairsIO {
    let aread = IOop::Read {
        dependencies,
        requests,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }

    DownstairsIO {
        ds_id,
        guest_id: gw_id,
        work: aread,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

/*
 * Create a flush DownstairsIO structure.
 */
fn create_flush(
    ds_id: u64,
    dependencies: Vec<u64>,
    flush_number: u64,
    guest_id: u64,
    gen_number: u64,
    snapshot_details: Option<SnapshotDetails>,
    impacted_blocks: ImpactedBlocks,
) -> DownstairsIO {
    let flush = IOop::Flush {
        dependencies,
        flush_number,
        gen_number,
        snapshot_details,
    };

    let mut state = HashMap::new();
    for cl in 0..3 {
        state.insert(cl, IOState::New);
    }
    DownstairsIO {
        ds_id,
        guest_id,
        work: flush,
        state,
        ack_status: AckStatus::NotAcked,
        replay: false,
        data: None,
        read_response_hashes: Vec::new(),
        impacted_blocks,
    }
}

/*
 * Debug function to display the work hashmap with status for all three of
 * the clients.
 */
async fn show_all_work(up: &Arc<Upstairs>) -> WQCounts {
    let gior = up.guest_io_ready().await;
    let up_count = up.guest.guest_work.lock().await.active.len();

    let mut ds = up.downstairs.lock().await;
    let mut kvec: Vec<u64> = ds.ds_active.keys().cloned().collect::<Vec<u64>>();
    println!(
        "----------------------------------------------------------------"
    );
    println!(
        " Crucible gen:{} GIO:{} \
        work queues:  Upstairs:{}  downstairs:{}",
        up.get_generation().await,
        gior,
        up_count,
        kvec.len(),
    );
    if kvec.is_empty() {
        if up_count != 0 {
            show_guest_work(&up.guest).await;
        }
    } else {
        println!(
            "{0:>5} {1:>8} {2:>5} {3:>7} {4:>7} {5:>5} {6:>5} {7:>5} {8:>7}",
            "GW_ID",
            "ACK",
            "DSID",
            "TYPE",
            "BLOCKS",
            "DS:0",
            "DS:1",
            "DS:2",
            "REPLAY",
        );

        kvec.sort_unstable();
        for id in kvec.iter() {
            let job = ds.ds_active.get(id).unwrap();
            let ack = job.ack_status;

            let (job_type, num_blocks): (String, usize) = match &job.work {
                IOop::Read {
                    dependencies: _dependencies,
                    requests,
                } => {
                    let job_type = "Read".to_string();
                    let num_blocks = requests.len();
                    (job_type, num_blocks)
                }
                IOop::Write {
                    dependencies: _dependencies,
                    writes,
                } => {
                    let job_type = "Write".to_string();
                    let mut num_blocks = 0;

                    for write in writes {
                        let block_size = write.offset.block_size_in_bytes();
                        num_blocks += write.data.len() / block_size as usize;
                    }

                    (job_type, num_blocks)
                }
                IOop::WriteUnwritten {
                    dependencies: _dependencies,
                    writes,
                } => {
                    let job_type = "WriteU".to_string();
                    let mut num_blocks = 0;

                    for write in writes {
                        let block_size = write.offset.block_size_in_bytes();
                        num_blocks += write.data.len() / block_size as usize;
                    }

                    (job_type, num_blocks)
                }
                IOop::Flush {
                    dependencies: _dependencies,
                    flush_number: _flush_number,
                    gen_number: _gen_number,
                    snapshot_details: _,
                } => {
                    let job_type = "Flush".to_string();
                    (job_type, 0)
                }
            };

            print!(
                "{0:>5} {1:>8} {2:>5} {3:>7} {4:>7}",
                job.guest_id, ack, id, job_type, num_blocks
            );

            for cid in 0..3 {
                let state = job.state.get(&cid);
                match state {
                    Some(state) => {
                        // XXX I have no idea why this is two spaces instead of
                        // one...
                        print!("  {0:>5}", state);
                    }
                    _x => {
                        print!("  {0:>5}", "????");
                    }
                }
            }
            print!(" {0:>6}", job.replay);

            println!();
        }
        ds.io_state_count.show_all();
        print!("Last Flush: ");
        for lf in ds.ds_last_flush.iter() {
            print!("{} ", lf);
        }
        println!();
    }

    let done = ds.completed.to_vec();
    let mut count = 0;
    print!("Downstairs last five completed:");
    for j in done.iter().rev() {
        count += 1;
        print!(" {:4}", j);
        if count > 5 {
            break;
        }
    }
    println!();
    drop(ds);

    let up_done = up.guest.guest_work.lock().await.completed.to_vec();
    print!("Upstairs last five completed:  ");
    let mut count = 0;
    for j in up_done.iter().rev() {
        count += 1;
        print!(" {:4}", j);
        if count > 5 {
            break;
        }
    }
    println!();
    drop(up_done);

    WQCounts {
        up_count,
        ds_count: kvec.len(),
    }
}

/*
 * Debug function to dump the guest work structure.
 * This does a bit while holding the mutex, so don't expect performance
 * to get better when calling it.
 *
 * TODO: make this one big dump, where we include the up.work.active
 * printing for each guest_work. It will be much more dense, but require
 * holding both locks for the duration.
 */
async fn show_guest_work(guest: &Arc<Guest>) -> usize {
    println!("Guest work:  Active and Completed Jobs:");
    let gw = guest.guest_work.lock().await;
    let mut kvec: Vec<u64> = gw.active.keys().cloned().collect::<Vec<u64>>();
    kvec.sort_unstable();
    for id in kvec.iter() {
        let job = gw.active.get(id).unwrap();
        println!(
            "GW_JOB active:[{:04}] S:{:?} C:{:?} ",
            id, job.submitted, job.completed
        );
    }
    let done = gw.completed.to_vec();
    println!("GW_JOB completed count:{:?} ", done.len());
    kvec.len()
}
