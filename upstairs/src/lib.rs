// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]
#![allow(clippy::mutex_atomic)]

use std::clone::Clone;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::{Read as _, Result as IOResult, Seek, SeekFrom, Write as _};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub use crucible_client_types::{CrucibleOpts, VolumeConstructionRequest};
pub use crucible_common::*;
pub use crucible_protocol::*;

use anyhow::{bail, Result};
pub use bytes::{Bytes, BytesMut};
use oximeter::types::ProducerRegistry;
use ringbuffer::{AllocRingBuffer, RingBuffer};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot, Mutex, MutexGuard, Notify, RwLock};
use tokio::time::Instant;
use tracing::{instrument, span, Level};
use usdt::register_probes;
use uuid::Uuid;

use aes_gcm_siv::aead::AeadInPlace;
use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit, Nonce, Tag};

pub mod control;
mod dummy_downstairs_tests;
mod pseudo_file;

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

mod impacted_blocks;
pub use impacted_blocks::*;

mod live_repair;

#[cfg(test)]
mod test;

mod active_jobs;
use active_jobs::ActiveJobs;

use async_trait::async_trait;

mod client;
mod downstairs;
mod upstairs;

// Max number of outstanding IOs between the upstairs and the downstairs
// before we give up and mark that downstairs faulted.
const IO_OUTSTANDING_MAX: usize = 57000;

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

    /// Replace one downstairs with a new one.
    ///
    /// This only make sense for Volume, Subvolume, and Guest, so it is only
    /// implemented for those.
    async fn replace_downstairs(
        &self,
        _id: Uuid,
        _old: SocketAddr,
        _new: SocketAddr,
    ) -> Result<ReplaceResult, CrucibleError> {
        panic!("should never hit here!");
    }

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

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ReplaceResult {
    Started,
    StartedAlready,
    CompletedAlready,
    Missing,
}
impl Debug for ReplaceResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplaceResult::Started => {
                write!(f, "Started")
            }
            ReplaceResult::StartedAlready => {
                write!(f, "StartedAlready")
            }
            ReplaceResult::CompletedAlready => {
                write!(f, "CompletedAlready")
            }
            ReplaceResult::Missing => {
                write!(f, "Missing")
            }
        }
    }
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
    fn ds__ping__sent(_: u64, _: u8) {}
    fn volume__read__start(_: u32, _: Uuid) {}
    fn volume__write__start(_: u32, _: Uuid) {}
    fn volume__writeunwritten__start(_: u32, _: Uuid) {}
    fn volume__flush__start(_: u32, _: Uuid) {}
    fn extent__or__start(_: u64) {}
    fn gw__read__start(_: u64) {}
    fn gw__write__start(_: u64) {}
    fn gw__write__unwritten__start(_: u64) {}
    fn gw__write__deps(_: u64, _: u64) {}
    fn gw__flush__start(_: u64) {}
    fn gw__close__start(_: u64, _: u64) {}
    fn gw__repair__start(_: u64, _: u64) {}
    fn gw__noop__start(_: u64) {}
    fn gw__reopen__start(_: u64, _: u64) {}
    fn up__to__ds__read__start(_: u64) {}
    fn up__to__ds__write__start(_: u64) {}
    fn up__to__ds__write__unwritten__start(_: u64) {}
    fn up__to__ds__flush__start(_: u64) {}
    fn ds__read__io__start(_: u64, _: u8) {}
    fn ds__write__io__start(_: u64, _: u8) {}
    fn ds__write__unwritten__io__start(_: u64, _: u8) {}
    fn ds__flush__io__start(_: u64, _: u8) {}
    fn ds__close__start(_: u64, _: u8, _: usize) {}
    fn ds__repair__start(_: u64, _: u8, _: usize) {}
    fn ds__noop__start(_: u64, _: u8) {}
    fn ds__reopen__start(_: u64, _: u8, _: usize) {}
    fn ds__read__io__done(_: u64, _: u8) {}
    fn ds__write__io__done(_: u64, _: u8) {}
    fn ds__write__unwritten__io__done(_: u64, _: u8) {}
    fn ds__flush__io__done(_: u64, _: u8) {}
    fn ds__close__done(_: u64, _: u8) {}
    fn ds__repair__done(_: u64, _: u8) {}
    fn ds__noop__done(_: u64, _: u8) {}
    fn ds__reopen__done(_: u64, _: u8) {}
    fn up__to__ds__read__done(_: u64) {}
    fn up__to__ds__write__done(_: u64) {}
    fn up__to__ds__write__unwritten__done(_: u64) {}
    fn up__to__ds__flush__done(_: u64) {}
    fn gw__read__done(_: u64) {}
    fn gw__write__done(_: u64) {}
    fn gw__write__unwritten__done(_: u64) {}
    fn gw__flush__done(_: u64) {}
    fn gw__close__done(_: u64, _: usize) {}
    fn gw__repair__done(_: u64, _: usize) {}
    fn gw__noop__done(_: u64) {}
    fn gw__reopen__done(_: u64, _: usize) {}
    fn extent__or__done(_: u64) {}
    fn reqwest__read__start(_: u32, _: Uuid) {}
    fn reqwest__read__done(_: u32, _: Uuid) {}
    fn volume__read__done(_: u32, _: Uuid) {}
    fn volume__write__done(_: u32, _: Uuid) {}
    fn volume__writeunwritten__done(_: u32, _: Uuid) {}
    fn volume__flush__done(_: u32, _: Uuid) {}
}

pub fn deadline_secs(secs: f32) -> Instant {
    Instant::now()
        .checked_add(Duration::from_secs_f32(secs))
        .unwrap()
}

/// Array of data associated with three clients, indexed by `ClientId`
#[derive(Copy, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ClientData<T>([T; 3]);

impl<T> std::ops::Index<ClientId> for ClientData<T> {
    type Output = T;
    fn index(&self, index: ClientId) -> &Self::Output {
        &self.0[index.get() as usize]
    }
}

impl<T> std::ops::IndexMut<ClientId> for ClientData<T> {
    fn index_mut(&mut self, index: ClientId) -> &mut Self::Output {
        &mut self.0[index.get() as usize]
    }
}

impl<T: Clone> ClientData<T> {
    pub fn new(t: T) -> Self {
        Self([t.clone(), t.clone(), t])
    }
}
impl<T> ClientData<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.0.iter_mut()
    }
    /// Inserts a new value, returning the old value
    pub fn insert(&mut self, c: ClientId, mut v: T) -> T {
        std::mem::swap(&mut self[c], &mut v);
        v
    }
}

/// Map of data associated with clients, keyed by `ClientId`
#[derive(Copy, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ClientMap<T>(ClientData<Option<T>>);

impl<T> ClientMap<T> {
    fn new() -> Self {
        Self(ClientData([None, None, None]))
    }
    /// Inserts a new value, returning the old value (or `None`)
    pub fn insert(&mut self, c: ClientId, v: T) -> Option<T> {
        self.0.insert(c, Some(v))
    }
    pub fn iter(&self) -> impl Iterator<Item = (ClientId, &T)> {
        self.0
            .iter()
            .enumerate()
            .flat_map(|(i, v)| v.as_ref().map(|v| (ClientId::new(i as u8), v)))
    }
    pub fn get(&self, c: &ClientId) -> Option<&T> {
        self.0[*c].as_ref()
    }
}

impl<T> std::ops::Index<ClientId> for ClientMap<T> {
    type Output = T;
    fn index(&self, index: ClientId) -> &Self::Output {
        self.get(&index).unwrap()
    }
}

#[allow(clippy::large_enum_variant)]
enum WrappedStream {
    Http(tokio::net::TcpStream),
    Https(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
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

#[derive(Debug, Copy, Clone)]
pub struct ExtentRepairIDs {
    close_id: JobId,
    repair_id: JobId,
    noop_id: JobId,
    reopen_id: JobId,
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
        let key = Key::<Aes256GcmSiv>::from_slice(&key[..]);
        let cipher = Aes256GcmSiv::new(key);

        EncryptionContext { cipher, block_size }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    #[cfg(any(target_os = "illumos", target_os = "macos"))]
    fn get_random_nonce(&self) -> Nonce {
        let mut random_iv: Nonce = aes_gcm_siv::aead::generic_array::arr![u8;
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ];

        // macos' and illumos' libc contain this
        extern "C" {
            pub fn arc4random_buf(buf: *mut libc::c_void, nbytes: libc::size_t);
        }

        unsafe {
            arc4random_buf(random_iv.as_mut_ptr() as *mut libc::c_void, 12)
        }

        random_iv
    }

    #[cfg(not(any(target_os = "illumos", target_os = "macos")))]
    fn get_random_nonce(&self) -> Nonce {
        let mut random_iv: Nonce = aes_gcm_siv::aead::generic_array::arr![u8;
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ];

        let filled = unsafe {
            libc::getrandom(
                random_iv.as_mut_ptr() as *mut libc::c_void,
                12,
                libc::GRND_NONBLOCK,
            )
        };

        assert_eq!(filled, 12);

        random_iv
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

#[allow(clippy::derive_partial_eq_without_eq)]
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
 * States of a downstairs
 *
 * This shows the different states a downstairs can be in from the point of
 * view of the upstairs.
 *
 * Double line paths can only be taken if an upstairs is active and goes to
 * deactivated.
 *
 *                       │
 *                       ▼
 *                       │
 *                  ┌────┴──────┐
 *   ┌───────┐      │           ╞═════◄══════════════════╗
 *   │  Bad  │      │    New    ╞═════◄════════════════╗ ║
 *   │Version├──◄───┤           ├─────◄──────┐         ║ ║
 *   └───────┘      └────┬───┬──┘            │         ║ ║
 *                       ▼   └───►───┐       │         ║ ║
 *                  ┌────┴──────┐    │       │         ║ ║
 *                  │   Wait    │    │       │         ║ ║
 *                  │  Active   ├─►┐ │       │         ║ ║
 *                  └────┬──────┘  │ │  ┌────┴───────┐ ║ ║
 *   ┌───────┐      ┌────┴──────┐  │ └──┤            │ ║ ║
 *   │  Bad  │      │   Wait    │  └────┤Disconnected│ ║ ║
 *   │Region ├──◄───┤  Quorum   ├──►────┤            │ ║ ║
 *   └───────┘      └────┬──────┘       └────┬───────┘ ║ ║
 *               ........▼..........         │         ║ ║
 *  ┌─────────┐  :  ┌────┴──────┐  :         ▲         ║ ║
 *  │ Failed  │  :  │  Repair   │  :         │       ╔═╝ ║
 *  │ Repair  ├─◄───┤           ├──►─────────┘       ║   ║
 *  └─────────┘  :  └────┬──────┘  :                 ║   ║
 *  Not Active   :       │         :                 ▲   ▲  Not Active
 *  .............. . . . │. . . . ...................║...║............
 *  Active               ▼                           ║   ║  Active
 *                  ┌────┴──────┐         ┌──────────╨┐  ║
 *              ┌─►─┤  Active   ├─────►───┤Deactivated│  ║
 *              │   │           │  ┌──────┤           ├─◄──────┐
 *              │   └─┬───┬───┬─┘  │      └───────────┘  ║     │
 *              │     ▼   ▼   ▲    ▲                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   │   ▲  ┌─┘                     ║     │
 *              │     │   │ ┌─┴──┴──┐                    ║     │
 *              │     │   │ │Replay │                    ║     │
 *              │     │   │ │       ├─►─┐                ║     │
 *              │     │   │ └─┬──┬──┘   │                ║     │
 *              │     │   ▼   ▼  ▲      │                ║     │
 *              │     │   │   │  │      │                ▲     │
 *              │     │ ┌─┴───┴──┴──┐   │   ┌────────────╨──┐  │
 *              │     │ │  Offline  │   └─►─┤   Faulted     │  │
 *              │     │ │           ├─────►─┤               │  │
 *              │     │ └───────────┘       └─┬─┬───────┬─┬─┘  │
 *              │     │                       ▲ ▲       ▼ ▲    ▲
 *              │     └───────────►───────────┘ │       │ │    │
 *              │                               │       │ │    │
 *              │                      ┌────────┴─┐   ┌─┴─┴────┴─┐
 *              └──────────────────────┤   Live   ├─◄─┤  Live    │
 *                                     │  Repair  │   │  Repair  │
 *                                     │          │   │  Ready   │
 *                                     └──────────┘   └──────────┘
 *
 *
 *      The downstairs state can go to Disabled from any other state, as that
 *      transition happens when a message is received from the actual
 *      downstairs on the other side of the connection..
 *      The only path back at that point is for the Upstairs (who will self
 *      deactivate when it detects this) is to go back to New and through
 *      the reconcile process.
 *      ┌───────────┐
 *      │ Disabled  │
 *      └───────────┘
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DsState {
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
     * Initial startup, downstairs are repairing from each other.
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
    Faulted,
    /*
     * This downstairs was failed, but has disconnected and now we
     * are ready to repair it.
     */
    LiveRepairReady,
    /*
     * This downstairs is undergoing LiveRepair
     */
    LiveRepair,
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
    /*
     * This downstairs is being replaced, Any active task needs to clear
     * any state and exit.
     */
    Replacing,
    /*
     * The current downstairs tasks have ended and the replacement has
     * begun.
     */
    Replaced,
}
impl std::fmt::Display for DsState {
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
            DsState::Repair => {
                write!(f, "Repair")
            }
            DsState::FailedRepair => {
                write!(f, "FailedRepair")
            }
            DsState::Active => {
                write!(f, "Active")
            }
            DsState::Faulted => {
                write!(f, "Faulted")
            }
            DsState::LiveRepairReady => {
                write!(f, "LiveRepairReady")
            }
            DsState::LiveRepair => {
                write!(f, "LiveRepair")
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
            DsState::Replacing => {
                write!(f, "Replacing")
            }
            DsState::Replaced => {
                write!(f, "Replaced")
            }
        }
    }
}

/*
 * A unit of work for downstairs that is put into the hashmap.
 */
#[derive(Debug, Clone)]
struct DownstairsIO {
    ds_id: JobId, // This MUST match our hashmap index

    guest_id: u64, // The hahsmap ID from the parent guest work.
    work: IOop,

    /// Map of work status, tracked on a per-client basis
    state: ClientData<IOState>,

    /*
     * Has this been acked to the guest yet?
     */
    acked: bool,

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
}

impl DownstairsIO {
    fn state_count(&self) -> WorkCounts {
        let mut wc: WorkCounts = Default::default();

        for state in self.state.iter() {
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
     * We don't consider repair IOs in the size calculation.
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
                extent_limit: _,
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
            IOop::ExtentClose {
                dependencies: _,
                extent: _,
            } => 0,
            IOop::ExtentFlushClose {
                dependencies: _,
                extent: _,
                flush_number: _,
                gen_number: _,
                source_downstairs: _,
                repair_downstairs: _,
            } => 0,
            IOop::ExtentLiveRepair {
                dependencies: _,
                extent: _,
                source_downstairs: _,
                source_repair_address: _,
                repair_downstairs: _,
            } => 0,
            IOop::ExtentLiveReopen {
                dependencies: _,
                extent: _,
            } => 0,
            IOop::ExtentLiveNoOp { dependencies: _ } => 0,
        }
    }

    /*
     * Return a summary of this job in the form of the WorkSummary struct.
     */
    pub fn io_summarize(&self) -> WorkSummary {
        let (job_type, num_blocks, deps) = self.work.ioop_summary();

        let mut state = Vec::with_capacity(3);
        /*
         * Convert the possible job states (and handle the None)
         */
        for cid in ClientId::iter() {
            /*
             * We don't ever expect the job state to return None, but
             * if it does because something else is wrong, I don't want
             * to panic here while trying to debug it.
             */
            let dss = format!("{}", self.state[cid]);
            state.push(dss);
        }

        WorkSummary {
            id: self.ds_id,
            replay: self.replay,
            job_type,
            num_blocks,
            deps,
            ack_status: if self.acked {
                AckStatus::Acked
            } else {
                AckStatus::NotAcked
            },
            state,
        }
    }

    /// Compare this struct's read_response_hashes to the hashes in a list of
    /// ReadResponses, returning false if there is a mismatch.
    #[cfg(test)]
    pub fn check_read_response_hashes(
        &self,
        read_data: &[ReadResponse],
    ) -> bool {
        std::iter::zip(
            self.read_response_hashes.iter(),
            read_data.iter().map(|response| response.first_hash()),
        )
        .all(|(l, r)| *l == r)
    }

    /// Verify that we have enough valid IO results when considering all
    /// downstairs results before we send back success to the guest.
    ///
    /// During normal operations, reads can have two failures or skipps and
    /// still return valid data.
    ///
    /// During normal operations, write, write_unwritten, and flush can have one
    /// error or skip and still return success to the upstairs (though, the
    /// downstairs normally will not return error to the upstairs on W/F).
    ///
    /// For repair, we don't permit any errors, but do allow and handle the
    /// "skipped" case for IOs.  This allows us to recover if we are repairing a
    /// downstairs and one of the valid remaining downstairs goes offline.
    pub fn result(&self) -> Result<(), CrucibleError> {
        /*
         * TODO: this doesn't tell the Guest what the error(s) were?
         */
        let wc = self.state_count();

        let bad_job = match &self.work {
            IOop::Read { .. } => wc.error == 3,
            IOop::Write { .. } => wc.skipped + wc.error > 1,
            IOop::WriteUnwritten { .. } => wc.skipped + wc.error > 1,
            IOop::Flush { .. } => wc.skipped + wc.error > 1,
            IOop::ExtentClose {
                dependencies: _,
                extent,
            } => {
                panic!("Received illegal IOop::ExtentClose: {}", extent);
            }
            IOop::ExtentFlushClose { .. } => wc.error >= 1 || wc.skipped > 1,
            IOop::ExtentLiveRepair { .. } => wc.error >= 1 || wc.skipped > 1,
            IOop::ExtentLiveReopen { .. } => wc.error >= 1 || wc.skipped > 1,
            IOop::ExtentLiveNoOp { .. } => wc.error >= 1 || wc.skipped > 1,
        };

        if bad_job {
            Err(CrucibleError::IoError(format!(
                "{} out of 3 downstairs failed to complete this IO",
                wc.error + wc.skipped,
            )))
        } else {
            Ok(())
        }
    }
}

/**
 * A summary of information from a DownstairsIO struct.
 */
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
struct WorkSummary {
    id: JobId,
    replay: bool,
    job_type: String,
    num_blocks: usize,
    deps: Vec<JobId>,
    ack_status: AckStatus,
    state: Vec<String>,
}

#[derive(Debug)]
struct ReconcileIO {
    id: ReconciliationId,
    op: Message,
    state: ClientData<IOState>,
}

impl ReconcileIO {
    fn new(id: ReconciliationId, op: Message) -> ReconcileIO {
        ReconcileIO {
            id,
            op,
            state: ClientData::new(IOState::New),
        }
    }
}
/*
 * Crucible to storage IO operations.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum IOop {
    Write {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    WriteUnwritten {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: Vec<crucible_protocol::Write>,
    },
    Read {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        requests: Vec<ReadRequest>,
    },
    Flush {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
        extent_limit: Option<usize>,
    },
    /*
     * These operations are for repairing a bad downstairs
     */
    ExtentClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
    },
    ExtentFlushClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
        flush_number: u64,
        gen_number: u64,
        source_downstairs: ClientId,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveRepair {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
        source_downstairs: ClientId,
        source_repair_address: SocketAddr,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveReopen {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
    },
    ExtentLiveNoOp {
        dependencies: Vec<JobId>, // Jobs that must finish before this
    },
}

impl IOop {
    pub fn deps(&self) -> &Vec<JobId> {
        match &self {
            IOop::Write {
                dependencies,
                writes: _,
            } => dependencies,
            IOop::Flush {
                dependencies,
                flush_number: _,
                gen_number: _,
                snapshot_details: _,
                extent_limit: _,
            } => dependencies,
            IOop::Read {
                dependencies,
                requests: _,
            } => dependencies,
            IOop::WriteUnwritten {
                dependencies,
                writes: _,
            } => dependencies,
            IOop::ExtentClose {
                dependencies,
                extent: _,
            } => dependencies,
            IOop::ExtentFlushClose {
                dependencies,
                extent: _,
                flush_number: _,
                gen_number: _,
                source_downstairs: _,
                repair_downstairs: _,
            } => dependencies,
            IOop::ExtentLiveRepair {
                dependencies,
                extent: _,
                source_downstairs: _,
                source_repair_address: _,
                repair_downstairs: _,
            } => dependencies,
            IOop::ExtentLiveReopen {
                dependencies,
                extent: _,
            } => dependencies,
            IOop::ExtentLiveNoOp { dependencies } => dependencies,
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

    /*
     * Report if the IOop is one used during LiveRepair
     */
    pub fn is_repair(&self) -> bool {
        matches!(
            self,
            IOop::ExtentClose { .. }
                | IOop::ExtentFlushClose { .. }
                | IOop::ExtentLiveRepair { .. }
                | IOop::ExtentLiveNoOp { .. }
                | IOop::ExtentLiveReopen { .. }
        )
    }

    /**
     * Take a IOop work operation and just return:
     * A string of the job type.
     * The size of the IO, or extent number if a repair operation.
     * A Vec of the dependencies.
     */
    pub fn ioop_summary(&self) -> (String, usize, Vec<JobId>) {
        let (job_type, num_blocks, deps) = match self {
            IOop::Read {
                dependencies,
                requests,
            } => {
                let job_type = "Read".to_string();
                let num_blocks = requests.len();
                (job_type, num_blocks, dependencies.clone())
            }
            IOop::Write {
                dependencies,
                writes,
            } => {
                let job_type = "Write".to_string();
                let mut num_blocks = 0;

                for write in writes {
                    let block_size = write.offset.block_size_in_bytes();
                    num_blocks += write.data.len() / block_size as usize;
                }
                (job_type, num_blocks, dependencies.clone())
            }
            IOop::WriteUnwritten {
                dependencies,
                writes,
            } => {
                let job_type = "WriteU".to_string();
                let mut num_blocks = 0;

                for write in writes {
                    let block_size = write.offset.block_size_in_bytes();
                    num_blocks += write.data.len() / block_size as usize;
                }
                (job_type, num_blocks, dependencies.clone())
            }
            IOop::Flush {
                dependencies,
                flush_number: _flush_number,
                gen_number: _gen_number,
                snapshot_details: _,
                extent_limit: _,
            } => {
                let job_type = "Flush".to_string();
                (job_type, 0, dependencies.clone())
            }
            IOop::ExtentClose {
                dependencies,
                extent,
            } => {
                let job_type = "EClose".to_string();
                (job_type, *extent, dependencies.clone())
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number: _,
                gen_number: _,
                source_downstairs: _,
                repair_downstairs: _,
            } => {
                let job_type = "FClose".to_string();
                (job_type, *extent, dependencies.clone())
            }
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_downstairs: _,
                source_repair_address: _,
                repair_downstairs: _,
            } => {
                let job_type = "Repair".to_string();
                (job_type, *extent, dependencies.clone())
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                let job_type = "Reopen".to_string();
                (job_type, *extent, dependencies.clone())
            }
            IOop::ExtentLiveNoOp { dependencies } => {
                let job_type = "NoOp".to_string();
                (job_type, 0, dependencies.clone())
            }
        };
        (job_type, num_blocks, deps)
    }

    // We have a downstairs in LiveRepair. Compare the extent IDs for this IO
    // and where we have repaired so far (or reserved dependencies for a
    // repair), and determine if this IO should be sent to the downstairs or not
    // (skipped).
    // Return true if we should send it.
    pub fn send_io_live_repair(&self, extent_limit: Option<u64>) -> bool {
        if let Some(extent_limit) = extent_limit {
            // The extent_limit has been set, so we have repair work in
            // progress.  If our IO touches an extent less than or equal
            // to the extent_limit, then we go ahead and send it.
            //
            // The special case of IOs that span extents repaired and not
            // repaired is handled with dependencies, and IOs should arrive
            // here with those dependencies already set.
            match &self {
                IOop::Write {
                    dependencies: _,
                    writes,
                } => {
                    for write in writes {
                        if write.eid <= extent_limit {
                            return true;
                        }
                    }
                    false
                }
                IOop::WriteUnwritten {
                    dependencies: _,
                    writes,
                } => {
                    for write in writes {
                        if write.eid <= extent_limit {
                            return true;
                        }
                    }
                    false
                }
                IOop::Flush { .. } => {
                    // If we have set extent limit, then we go ahead and
                    // send the flush with the extent_limit in it, and allow
                    // the downstairs to act based on that.
                    true
                }
                IOop::Read {
                    dependencies: _,
                    requests,
                } => {
                    for req in requests {
                        if req.eid <= extent_limit {
                            return true;
                        }
                    }
                    false
                }
                _ => {
                    panic!("Unsupported IO check {:?}", self);
                }
            }
        } else {
            // If we have not set an extent_limit yet all IO should
            // be skipped for this downstairs.
            false
        }
    }
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ClientIOStateCount {
    pub new: u32,
    pub in_progress: u32,
    pub done: u32,
    pub skipped: u32,
    pub error: u32,
}

impl ClientIOStateCount {
    fn new() -> ClientIOStateCount {
        ClientIOStateCount {
            new: 0,
            in_progress: 0,
            done: 0,
            skipped: 0,
            error: 0,
        }
    }

    pub fn incr(&mut self, state: &IOState) {
        *self.get_mut(state) += 1;
    }

    pub fn decr(&mut self, state: &IOState) {
        *self.get_mut(state) -= 1;
    }

    fn get_mut(&mut self, state: &IOState) -> &mut u32 {
        match state {
            IOState::New => &mut self.new,
            IOState::InProgress => &mut self.in_progress,
            IOState::Done => &mut self.done,
            IOState::Skipped => &mut self.skipped,
            IOState::Error(_) => &mut self.error,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct IOStateCount {
    pub new: ClientData<u32>,
    pub in_progress: ClientData<u32>,
    pub done: ClientData<u32>,
    pub skipped: ClientData<u32>,
    pub error: ClientData<u32>,
}

impl IOStateCount {
    fn show_all(&self) {
        println!("   STATES      DS:0   DS:1   DS:2   TOTAL");
        self.show(IOState::New);
        self.show(IOState::InProgress);
        self.show(IOState::Done);
        self.show(IOState::Skipped);
        let e = CrucibleError::GenericError("x".to_string());
        self.show(IOState::Error(e));
    }

    fn get(&self, state: &IOState) -> &ClientData<u32> {
        match state {
            IOState::New => &self.new,
            IOState::InProgress => &self.in_progress,
            IOState::Done => &self.done,
            IOState::Skipped => &self.skipped,
            IOState::Error(_) => &self.error,
        }
    }

    fn show(&self, state: IOState) {
        let state_stat = self.get(&state);
        match state {
            IOState::New => {
                print!("    New        ");
            }
            IOState::InProgress => {
                print!("    Sent       ");
            }
            IOState::Done => {
                print!("    Done       ");
            }
            IOState::Skipped => {
                print!("    Skipped    ");
            }
            IOState::Error(_) => {
                print!("    Error      ");
            }
        }
        let mut sum = 0;
        for cid in ClientId::iter() {
            print!("{:4}   ", state_stat[cid]);
            sum += state_stat[cid];
        }
        println!("{:4}", sum);
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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

    #[allow(clippy::redundant_clone)]
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
    // Management commands
    RepairOp,
    ReplaceDownstairs {
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
        result: Arc<Mutex<ReplaceResult>>,
    },
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
    pub fn iops(&self, iop_sz: usize) -> Option<usize> {
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
    pub fn sz(&self) -> Option<usize> {
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
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(8000),
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(16000),
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(16001),
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 2);
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
     * Jobs we have submitted to the storage side of the upstairs process to
     * send on to the downstairs.  The key is the ds_id number in the hashmap
     * for downstairs work.
     */
    submitted: HashSet<JobId>,
    completed: Vec<JobId>,

    /*
     * These buffers are provided by the guest request. If this is a read,
     * data will be written here.
     */
    guest_buffers: HashMap<JobId, Buffer>,

    /*
     * When we have an IO between the guest and crucible, it's possible
     * it will be broken into two smaller requests if the range happens
     * to cross an extent boundary. This hashmap is a list of those
     * buffers with the key being the downstairs request ID.
     */
    downstairs_responses: HashMap<JobId, Vec<ReadResponse>>,

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
    /// Create a new GtoS object where one Guest IO request maps to one
    /// downstairs operation.
    pub fn new(
        ds_id: JobId,
        guest_buffer: Option<Buffer>,
        req: Option<BlockReq>,
    ) -> GtoS {
        let mut submitted = HashSet::new();
        submitted.insert(ds_id);

        let mut guest_buffers = HashMap::new();
        if let Some(guest_buffer) = guest_buffer {
            guest_buffers.insert(ds_id, guest_buffer);
        }

        GtoS::new_bulk(submitted, guest_buffers, req)
    }

    /// Create a new GtoS object where one Guest IO request maps to many
    /// downstairs operations.
    pub fn new_bulk(
        submitted: HashSet<JobId>,
        guest_buffers: HashMap<JobId, Buffer>,
        req: Option<BlockReq>,
    ) -> GtoS {
        GtoS {
            submitted,
            completed: Vec::new(),
            guest_buffers,
            downstairs_responses: HashMap::new(),
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
        assert!(!self.completed.is_empty());

        for ds_id in &self.completed {
            if let Some(guest_buffer) = self.guest_buffers.get_mut(ds_id) {
                let mut offset = 0;
                let mut vec = guest_buffer.as_vec().await;
                let mut owned_vec = guest_buffer.owned_vec().await;

                /*
                 * Should this panic?  If the caller is requesting a transfer,
                 * the guest_buffer should exist. If it does not exist, then
                 * either there is a real problem, or the operation was a write
                 * or flush and why are we requesting a transfer for those.
                 */
                let responses =
                    self.downstairs_responses.remove(ds_id).unwrap();

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
        }
    }

    /*
     * Notify corresponding BlockReqWaiter
     */
    fn notify(self, result: Result<(), CrucibleError>) {
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
            req.send_result(result);
        }
    }

    pub async fn finalize(mut self, result: Result<(), CrucibleError>) {
        if result.is_ok() {
            self.transfer().await;
        }

        self.notify(result);
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
     * This may include moving data buffers from completed reads.
     */
    #[instrument]
    async fn gw_ds_complete(
        &mut self,
        gw_id: u64,
        ds_id: JobId,
        data: Option<Vec<ReadResponse>>,
        result: Result<(), CrucibleError>,
        log: &Logger,
    ) {
        /*
         * A gw_id that already finished and results were sent back to
         * the guest could still have an outstanding ds_id.
         */
        let gtos_job_done = if let Some(gtos_job) = self.active.get_mut(&gw_id)
        {
            /*
             * If the ds_id is on the submitted list, then we will take it
             * off and, if it is a read, add the read result
             * buffer to the gtos job structure for later
             * copying.
             */
            if gtos_job.submitted.remove(&ds_id) {
                if let Some(data) = data {
                    /*
                     * The first read buffer will become the source for the
                     * final response back to the guest. This buffer will be
                     * combined with other buffers if the upstairs request
                     * required multiple jobs.
                     */
                    if gtos_job
                        .downstairs_responses
                        .insert(ds_id, data)
                        .is_some()
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
                error!(log, "gw_id:{} ({}) already removed???", gw_id, ds_id);
                assert!(gtos_job.completed.contains(&ds_id));
                panic!(
                    "{} Attempting to complete ds_id {} we already completed",
                    gw_id, ds_id
                );
            }

            gtos_job.submitted.is_empty()
        } else {
            /*
             * XXX This is just so I can see if ever does happen.
             */
            panic!("gw_id {} for job {} not on active list", gw_id, ds_id);
        };

        if gtos_job_done {
            if let Some(gtos_job) = self.active.remove(&gw_id) {
                /*
                 * Copy (if present) read data back to the guest buffer they
                 * provided to us, and notify any waiters.
                 */
                gtos_job.finalize(result).await;

                self.completed.push(gw_id);
            } else {
                /*
                 * XXX This is just so I can see if ever does happen.
                 */
                panic!("gw_id {} for remove not on active list", gw_id);
            }
        }
    }
}

impl Default for GuestWork {
    fn default() -> Self {
        Self {
            active: HashMap::new(), // GtoS
            next_gw_id: 1,
            completed: AllocRingBuffer::new(2048),
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
    iop_tokens: std::sync::Mutex<usize>,
    bytes_per_iop: Option<usize>,
    iop_limit: Option<usize>,

    /*
     * Setting a bandwidth limit will also limit the rate at which block
     * reqs are pulled off the queue.
     */
    bw_tokens: std::sync::Mutex<usize>, // bytes
    bw_limit: Option<usize>,            // bytes per second

    /// Local cache for block size
    ///
    /// This is 0 when unpopulated, and non-zero otherwise; storing it locally
    /// saves a round-trip through the `reqs` queue, and using an atomic means
    /// it can be read from a `&self` reference.
    block_size: AtomicU64,

    /// Backpressure is implemented as a delay on host write operations
    backpressure_us: AtomicU64,

    /// Backpressure configuration, as a starting point and max delay
    backpressure_config: BackpressureConfig,

    /// Lock held during backpressure delay
    ///
    /// Without this lock, multiple tasks could submit jobs to the upstairs and
    /// wait in parallel, which defeats the purpose of backpressure (since you
    /// could send arbitrarily many jobs at high speed by sending them from
    /// different tasks).
    backpressure_lock: Mutex<()>,
}

/// Configuration for host-side backpressure
///
/// Backpressure adds an artificial delay to host write messages (which are
/// otherwise acked immediately, before actually being complete).  The delay is
/// varied based on two metrics:
///
/// - number of write bytes outstanding
/// - queue length as a fraction (where 1.0 is full)
///
/// These two metrics are used for quadratic backpressure, picking the larger of
/// the two delays.
#[derive(Copy, Clone, Debug)]
struct BackpressureConfig {
    /// When should backpressure start (in bytes)?
    bytes_start: u64,
    /// Scale for byte-based quadratic backpressure
    bytes_scale: f64,

    /// When should queue-based backpressure start?
    queue_start: f64,
    /// Maximum queue-based delay
    queue_max_delay: Duration,
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
                completed: AllocRingBuffer::new(2048),
            }),

            iop_tokens: std::sync::Mutex::new(0),
            bytes_per_iop: None,
            iop_limit: None,

            bw_tokens: std::sync::Mutex::new(0),
            bw_limit: None,

            block_size: AtomicU64::new(0),

            backpressure_us: AtomicU64::new(0),
            backpressure_config: BackpressureConfig {
                bytes_start: 1024u64.pow(3), // Start at 1 GiB
                bytes_scale: 9.3e-8,         // Delay of 10ms at 2 GiB in-flight
                queue_start: 0.05,
                queue_max_delay: Duration::from_millis(5),
            },
            backpressure_lock: Mutex::new(()),
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
        let (send, recv) = oneshot::channel();

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

    /// Set `self.backpressure_us` based on outstanding IO ratio
    fn set_backpressure(&self, bytes: u64, ratio: f64) {
        // Check to see if the number of outstanding write bytes (between
        // the upstairs and downstairs) is particularly high.  If so,
        // apply some backpressure by delaying host operations, with a
        // quadratically-increasing delay.
        let d1 = (bytes.saturating_sub(self.backpressure_config.bytes_start)
            as f64
            * self.backpressure_config.bytes_scale)
            .powf(2.0) as u64;

        // Compute an alternate delay based on queue length
        let d2 = self
            .backpressure_config
            .queue_max_delay
            .mul_f64(
                ((ratio - self.backpressure_config.queue_start).max(0.0)
                    / (1.0 - self.backpressure_config.queue_start))
                    .powf(2.0),
            )
            .as_micros() as u64;
        self.backpressure_us.store(d1.max(d2), Ordering::SeqCst);
    }

    /*
     * Consume one request off queue if it is under the IOP limit and the BW
     * limit. This function must be cancel safe (due to it being used in a
     * `tokio::select!` arm) so it is split into two parts: the first async part
     * grabs all the necessary tokio Mutexes, and the second sync part does the
     * actual work with the mutex guards.
     */
    async fn consume_req(&self) -> Option<BlockReq> {
        let mut reqs = self.reqs.lock().await;
        let mut bw_tokens = self.bw_tokens.lock().unwrap();
        let mut iop_tokens = self.iop_tokens.lock().unwrap();

        self.consume_req_locked(&mut reqs, &mut bw_tokens, &mut iop_tokens)

        // IMPORTANT: there must be no await points after `consume_req_locked`
        // has popped a BlockReq off the VecDeque! The function could be
        // cancelled and would **drop** that BlockReq as a result.
    }

    fn consume_req_locked(
        &self,
        reqs: &mut VecDeque<BlockReq>,
        bw_tokens: &mut usize,
        iop_tokens: &mut usize,
    ) -> Option<BlockReq> {
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
            self.bw_limit.is_some() && req_ref.op.sz().is_some();

        if !iop_limit_applies && !bw_limit_applies {
            return Some(reqs.pop_front().unwrap());
        }

        // Check bandwidth limit before IOP limit, but make sure only to consume
        // tokens if both checks pass!

        let mut bw_check_ok = true;
        let mut iop_check_ok = true;

        // When checking tokens vs the limit, do not check by checking if adding
        // the block request's values to the applicable limit: this would create
        // a scenario where a large IO enough would stall the pipeline (see
        // test_impossible_io). Instead, check if the limits are already
        // reached.

        if let Some(bw_limit) = self.bw_limit {
            if req_ref.op.sz().is_some() && *bw_tokens >= bw_limit {
                bw_check_ok = false;
            }
        }

        if let Some(iop_limit) = self.iop_limit {
            let bytes_per_iops = self.bytes_per_iop.unwrap();
            if req_ref.op.iops(bytes_per_iops).is_some()
                && *iop_tokens >= iop_limit
            {
                iop_check_ok = false;
            }
        }

        // If both checks pass, consume appropriate resources and return the
        // block req
        if bw_check_ok && iop_check_ok {
            if self.bw_limit.is_some() {
                if let Some(sz) = req_ref.op.sz() {
                    *bw_tokens += sz;
                }
            }

            if self.iop_limit.is_some() {
                let bytes_per_iops = self.bytes_per_iop.unwrap();
                if let Some(req_iops) = req_ref.op.iops(bytes_per_iops) {
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
    pub fn leak_iop_tokens(&self, tokens: usize) {
        let mut iop_tokens = self.iop_tokens.lock().unwrap();

        if tokens > *iop_tokens {
            *iop_tokens = 0;
        } else {
            *iop_tokens -= tokens;
        }

        // Notify to wake up recv now that there may be room.
        self.notify.notify_one();
    }

    // Leak bytes from bandwidth tokens
    pub fn leak_bw_tokens(&self, bytes: usize) {
        let mut bw_tokens = self.bw_tokens.lock().unwrap();

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
            active_count: 0,
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

    async fn backpressure_sleep(&self) {
        let bp =
            Duration::from_micros(self.backpressure_us.load(Ordering::SeqCst));
        if bp > Duration::ZERO {
            let _guard = self.backpressure_lock.lock().await;
            tokio::time::sleep(bp).await;
            drop(_guard);
        }
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
        let bs = self.block_size.load(std::sync::atomic::Ordering::Relaxed);
        if bs == 0 {
            let data = Arc::new(Mutex::new(0));
            let size_query = BlockOp::QueryBlockSize { data: data.clone() };
            self.send(size_query).await.wait().await?;

            let result = *data.lock().await;
            self.block_size
                .store(result, std::sync::atomic::Ordering::Relaxed);
            Ok(result)
        } else {
            Ok(bs)
        }
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

        if data.is_empty() {
            return Ok(());
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

        if data.is_empty() {
            return Ok(());
        }
        let wio = BlockOp::Write { offset, data };

        self.backpressure_sleep().await;
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

        if data.is_empty() {
            return Ok(());
        }
        let wio = BlockOp::WriteUnwritten { offset, data };

        self.backpressure_sleep().await;
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
            active_count: 0,
        };

        let data = Arc::new(Mutex::new(wc));
        let sw = BlockOp::ShowWork { data: data.clone() };
        self.send(sw).await.wait().await.unwrap();

        let wc = data.lock().await;
        Ok(*wc)
    }

    async fn replace_downstairs(
        &self,
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
    ) -> Result<ReplaceResult, CrucibleError> {
        let data = Arc::new(Mutex::new(ReplaceResult::Missing));
        let sw = BlockOp::ReplaceDownstairs {
            id,
            old,
            new,
            result: data.clone(),
        };

        self.send(sw).await.wait().await?;
        let result = data.lock().await;
        Ok(*result)
    }
}

/*
 * Work Queue Counts, for debug ShowWork IO type
 */
#[derive(Debug, Copy, Clone)]
pub struct WQCounts {
    pub up_count: usize,
    pub ds_count: usize,
    pub active_count: usize,
}

impl Default for Guest {
    fn default() -> Self {
        Self::new()
    }
}

/**
 * Stat counters struct used by DTrace
 */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Arg {
    /// Jobs on the upstairs guest work queue.
    pub up_count: u32,
    /// Backpressure value
    pub up_backpressure: u64,
    /// Jobs on the downstairs work queue.
    pub ds_count: u32,
    /// Number of write bytes in flight
    pub write_bytes_out: u64,
    /// State of a downstairs
    pub ds_state: [DsState; 3],
    /// Counters for each state of a downstairs job.
    pub ds_io_count: IOStateCount,
    /// Extents repaired during initial reconciliation.
    pub ds_reconciled: usize,
    /// Extents still needing repair during initial reconciliation.
    pub ds_reconcile_needed: usize,
    /// Times we have completed a LiveRepair on a downstairs.
    pub ds_live_repair_completed: [usize; 3],
    /// Times we aborted a LiveRepair on a downstairs.
    pub ds_live_repair_aborted: [usize; 3],
    /// Times the upstairs has connected to a downstairs.
    pub ds_connected: [usize; 3],
    /// Times this downstairs has been replaced.
    pub ds_replaced: [usize; 3],
    /// Times flow control has been enabled on this downstairs.
    pub ds_flow_control: [usize; 3],
    /// Times we have live repaired an extent on this downstairs.
    pub ds_extents_repaired: [usize; 3],
    /// Times we have live confirmed  an extent on this downstairs.
    pub ds_extents_confirmed: [usize; 3],
    /// Times we skipped repairing a downstairs because we are read_only.
    pub ds_ro_lr_skipped: [usize; 3],
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
    upstairs_log: Option<Logger>,
) -> Result<tokio::task::JoinHandle<()>> {
    register_probes().unwrap();

    let log = match upstairs_log {
        Some(log) => log,
        None => build_logger(),
    };
    info!(log, "Upstairs starts");
    let info = crucible_common::BuildInfo::default();
    info!(log, "Crucible Version: {:#?}", info);
    info!(
        log,
        "Upstairs <-> Downstairs Message Version: {}", CRUCIBLE_MESSAGE_VERSION
    );

    if let Some(rd) = region_def {
        info!(log, "Using region definition {:?}", rd);
    }

    let tls_context = if let Some(cert_pem_path) = &opt.cert_pem {
        let key_pem_path = opt.key_pem.as_ref().unwrap();
        let root_cert_pem_path = opt.root_cert_pem.as_ref().unwrap();

        let tls_context = crucible_common::x509::TLSContext::from_paths(
            cert_pem_path,
            key_pem_path,
            root_cert_pem_path,
        )?;

        Some(Arc::new(tls_context))
    } else {
        None
    };

    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let mut up =
        upstairs::Upstairs::new(&opt, gen, region_def, guest, tls_context, log);

    if let Some(pr) = producer_registry {
        let ups = up.stats.clone();
        if let Err(e) = pr.register_producer(ups) {
            error!(up.log, "Failed to register metric producer: {}", e);
        }
    }

    if let Some(control) = opt.control {
        let log = up.log.new(o!("task" => "control".to_string()));
        let up = crate::control::UpstairsInfo::new(&up);
        tokio::spawn(async move {
            let r = control::start(up, log.clone(), control).await;
            info!(log, "Control HTTP task finished with {:?}", r);
        });
    }

    let join_handle = tokio::spawn(async move { up.run().await });

    Ok(join_handle)
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
