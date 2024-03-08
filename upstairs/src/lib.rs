// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]
#![allow(clippy::mutex_atomic)]

use std::clone::Clone;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::{Read as _, Result as IOResult, Seek, SeekFrom, Write as _};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub use crucible_client_types::{CrucibleOpts, VolumeConstructionRequest};
pub use crucible_common::*;
pub use crucible_protocol::*;

use anyhow::{bail, Result};
pub use bytes::{Bytes, BytesMut};
use oximeter::types::ProducerRegistry;
use ringbuffer::AllocRingBuffer;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, warn, Logger};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::time::Instant;
use tracing::instrument;
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
pub(crate) use block_req::{BlockReq, BlockReqReply, BlockReqWaiter, BlockRes};

mod mend;
pub use mend::{DownstairsMend, ExtentFix, RegionMetadata};
pub use pseudo_file::CruciblePseudoFile;

pub(crate) mod guest;
pub use guest::{Guest, WQCounts};
use guest::{GuestIoHandle, GuestWorkId};

mod stats;

pub use crucible_common::impacted_blocks::*;

mod deferred;
mod live_repair;

#[cfg(test)]
mod test;

mod active_jobs;
use active_jobs::ActiveJobs;

use async_trait::async_trait;

mod client;
mod downstairs;
mod upstairs;
use upstairs::{UpCounters, UpstairsAction};

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
        data: &mut Buffer,
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
        data: &mut Buffer,
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

    /// Checks that the data length is a multiple of block size
    ///
    /// Returns block size on success, since we have to look it up anyways.
    async fn check_data_size(&self, len: usize) -> Result<u64, CrucibleError> {
        let block_size = self.get_block_size().await?;
        if len as u64 % block_size == 0 {
            Ok(block_size)
        } else {
            Err(CrucibleError::DataLenUnaligned)
        }
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
/// up__apply: A count of times the upstairs main loop applies an action.
///
/// up_action_*: These probes record which path is taken when the
/// upstairs apply select is triggered.
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
    fn up__apply(_: u64) {}
    fn up__action_downstairs(_: u64) {}
    fn up__action_guest(_: u64) {}
    fn up__action_deferred_block(_: u64) {}
    fn up__action_deferred_message(_: u64) {}
    fn up__action_leak_check(_: u64) {}
    fn up__action_flush_check(_: u64) {}
    fn up__action_stat_check(_: u64) {}
    fn up__action_repair_check(_: u64) {}
    fn up__action_control_check(_: u64) {}
    fn up__action_noop(_: u64) {}
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
    fn up__block__req__dropped() {}
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
 *  │ Failed  │  :  │ Reconcile │  :         │       ╔═╝ ║
 *  │Reconcile├─◄───┤           ├──►─────────┘       ║   ║
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
    Reconcile,
    /*
     * Failed when attempting to make consistent.
     */
    FailedReconcile,
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
            DsState::Reconcile => {
                write!(f, "Reconcile")
            }
            DsState::FailedReconcile => {
                write!(f, "FailedReconcile")
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

    guest_id: GuestWorkId, // The hashmap ID from the parent guest work.
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
            IOop::Write { data, .. } | IOop::WriteUnwritten { data, .. } => {
                data.io_size_bytes
            }
            IOop::Read { .. } => {
                if self.data.is_some() {
                    let rrs = self.data.as_ref().unwrap();
                    rrs.iter().map(|r| r.data.len()).sum()
                } else {
                    0
                }
            }
            IOop::Flush { .. }
            | IOop::ExtentFlushClose { .. }
            | IOop::ExtentLiveRepair { .. }
            | IOop::ExtentLiveReopen { .. }
            | IOop::ExtentLiveNoOp { .. } => 0,
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

    /// Verify that we have enough valid IO results when considering all
    /// downstairs results before we send back success to the guest.
    ///
    /// During normal operations, reads can have two failures or skips and still
    /// return valid data.
    ///
    /// Writes are acked to the host right away, before hearing back from the
    /// Downstairs (the so-called "fast ack" optimization), so this function is
    /// never called for them.
    ///
    /// During normal operations, write_unwritten and flush can have one error
    /// or skip and still return success to the upstairs (though, the downstairs
    /// normally will not return error to the upstairs on W/F).
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
            IOop::Write { .. }
            | IOop::WriteUnwritten { .. }
            | IOop::Flush { .. } => wc.skipped + wc.error > 1,
            IOop::ExtentFlushClose { .. }
            | IOop::ExtentLiveRepair { .. }
            | IOop::ExtentLiveReopen { .. }
            | IOop::ExtentLiveNoOp { .. } => wc.error >= 1 || wc.skipped > 1,
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

/// Pre-serialized write, to avoid extra memory copies / allocation
///
/// The actual data to be written on the wire is in `self.data`; everything else
/// is metadata used for logging, etc.
#[derive(Debug, Clone, PartialEq)]
pub struct SerializedWrite {
    /// Number of blocks written (used for logging)
    num_blocks: usize,

    /// Extents to be written
    eids: Vec<u64>,

    /// Number of bytes written
    io_size_bytes: usize,

    /// Pre-serialized write data, to avoid extra memcpys
    data: bytes::Bytes,
}

impl SerializedWrite {
    /// Helper function to build a `SerializedWrite` from a list of `Writes`
    ///
    /// This is only used during unit tests; normally, the conversion is
    /// performed during the handling of the write request.
    #[cfg(test)]
    fn from_writes(writes: Vec<Write>) -> Self {
        use bytes::BufMut;

        let out = BytesMut::new();
        let mut w = out.writer();
        bincode::serialize_into(&mut w, &writes).unwrap();

        let mut eids: Vec<_> = writes.iter().map(|w| w.eid).collect();
        eids.dedup();

        let num_blocks = writes.len();
        let io_size_bytes = writes.iter().map(|w| w.data.len()).sum();

        Self {
            num_blocks,
            io_size_bytes,
            eids,
            data: w.into_inner().freeze(),
        }
    }
}

/// Raw message header, which is used for zero-copy serialization
///
/// These variants must contain the same fields as their `Message` equivalents
/// in the same order.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[repr(u16)]
enum RawMessage {
    Write {
        upstairs_id: Uuid,
        session_id: Uuid,
        job_id: JobId,
        dependencies: Vec<JobId>,
    },
    WriteUnwritten {
        upstairs_id: Uuid,
        session_id: Uuid,
        job_id: JobId,
        dependencies: Vec<JobId>,
    },
}

impl crucible_protocol::RawMessageDiscriminant for RawMessage {
    /// Returns the discriminant used by the equivalent `Message`
    ///
    /// This is hard-coded and exhaustively checked by a unit test.
    fn discriminant(&self) -> MessageDiscriminants {
        match self {
            RawMessage::Write { .. } => MessageDiscriminants::Write,
            RawMessage::WriteUnwritten { .. } => {
                MessageDiscriminants::WriteUnwritten
            }
        }
    }
}

/*
 * Crucible to storage IO operations.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
enum IOop {
    Write {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        data: SerializedWrite,
    },
    WriteUnwritten {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        data: SerializedWrite,
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
    ExtentFlushClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: usize,
        flush_number: u64,
        gen_number: u64,
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
    #[cfg(test)]
    fn deps(&self) -> &Vec<JobId> {
        match &self {
            IOop::Write { dependencies, .. }
            | IOop::Flush { dependencies, .. }
            | IOop::Read { dependencies, .. }
            | IOop::WriteUnwritten { dependencies, .. }
            | IOop::ExtentFlushClose { dependencies, .. }
            | IOop::ExtentLiveRepair { dependencies, .. }
            | IOop::ExtentLiveReopen { dependencies, .. }
            | IOop::ExtentLiveNoOp { dependencies } => dependencies,
        }
    }

    /*
     * Report if the IOop is one used during LiveRepair
     */
    fn is_repair(&self) -> bool {
        matches!(
            self,
            IOop::ExtentFlushClose { .. }
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
    fn ioop_summary(&self) -> (String, usize, Vec<JobId>) {
        let (job_type, num_blocks, deps) = match self {
            IOop::Read {
                dependencies,
                requests,
            } => {
                let job_type = "Read".to_string();
                let num_blocks = requests.len();
                (job_type, num_blocks, dependencies.clone())
            }
            IOop::Write { dependencies, data } => {
                let job_type = "Write".to_string();
                (job_type, data.num_blocks, dependencies.clone())
            }
            IOop::WriteUnwritten { dependencies, data } => {
                let job_type = "WriteU".to_string();
                (job_type, data.num_blocks, dependencies.clone())
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
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number: _,
                gen_number: _,
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
    fn send_io_live_repair(&self, extent_limit: Option<u64>) -> bool {
        if let Some(extent_limit) = extent_limit {
            // The extent_limit has been set, so we have repair work in
            // progress.  If our IO touches an extent less than or equal
            // to the extent_limit, then we go ahead and send it.
            //
            // The special case of IOs that span extents repaired and not
            // repaired is handled with dependencies, and IOs should arrive
            // here with those dependencies already set.
            match &self {
                IOop::Write { data, .. }
                | IOop::WriteUnwritten { data, .. } => {
                    data.eids.iter().any(|eid| *eid <= extent_limit)
                }
                IOop::Flush { .. } => {
                    // If we have set extent limit, then we go ahead and
                    // send the flush with the extent_limit in it, and allow
                    // the downstairs to act based on that.
                    true
                }
                IOop::Read { requests, .. } => {
                    requests.iter().any(|req| req.eid <= extent_limit)
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

    /// Returns the number of bytes written or read in this job
    fn job_bytes(&self) -> u64 {
        match &self {
            IOop::Write { data, .. } | IOop::WriteUnwritten { data, .. } => {
                data.io_size_bytes as u64
            }
            IOop::Read { requests, .. } => {
                requests
                    .first()
                    .map(|r| r.offset.block_size_in_bytes())
                    .unwrap_or(0) as u64
                    * requests.len() as u64
            }
            _ => 0,
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
 * Provides a strongly-owned Buffer that Read operations will write into.
 *
 * Originally BytesMut was used here, but it didn't guarantee that memory was
 * shared between cloned BytesMut objects. Additionally, we added the idea of
 * ownership and that necessitated another field.
 *
 * Ownership of a block is defined as true if that block has been written to: we
 * say a block is "owned" if the bytes were written to by something, rather than
 * having been initialized to zero. For an Upstairs, a block is owned if it was
 * returned with a non-zero number of block contexts, encrypted or not. This is
 * important when using authenticated encryption to distinguish between zeroes
 * that the Guest has written and blocks that were zero to begin with.
 *
 * It's safe to set ownership to `true` if there's no persistence of ownership
 * information. Persistence is required otherwise: if a particular `BlockIO`
 * implementation is dropped and recreated, the ownership should not be lost as
 * a result.
 *
 * Because persistence is required, ownership will always come from the
 * Downstairs (or other `BlockIO` implementations that persist ownership
 * information) and be propagated "up".
 *
 * The `Buffer` is a block-oriented data structure: any functions which read or
 * write to it's data must be block-aligned and block sized.  Otherwise, these
 * functions will panic.  Any function which panics also notes those conditions
 * in its docstring.
 */
#[must_use]
#[derive(Debug, PartialEq, Default)]
pub struct Buffer {
    block_size: usize,
    data: BytesMut,

    /// Per-block ownership data, using 0 = false and 1 = true
    ///
    /// `owned.len() == data.len() / block_size`
    owned: BytesMut,
}

impl Buffer {
    pub fn new(block_count: usize, block_size: usize) -> Buffer {
        Self::repeat(0, block_count, block_size)
    }

    /// Builds a new buffer that repeats the given value
    pub fn repeat(v: u8, block_count: usize, block_size: usize) -> Self {
        let len = block_count * block_size;
        let mut data = BytesMut::with_capacity(len);
        data.resize(len, v);
        let mut owned = BytesMut::with_capacity(block_count);
        owned.resize(block_count, 0);
        Buffer {
            block_size,
            data,
            owned,
        }
    }

    pub fn with_capacity(block_count: usize, block_size: usize) -> Buffer {
        let len = block_count * block_size;
        let data = BytesMut::with_capacity(len);
        let owned = BytesMut::with_capacity(block_count);

        Buffer {
            block_size,
            data,
            owned,
        }
    }

    /// Extracts and freezes the underlying `BytesMut` bearing buffered data.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.data.freeze()
    }

    /// Extracts the underlying `BytesMut`
    #[must_use]
    pub fn into_bytes_mut(self) -> BytesMut {
        self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Writes data to the buffer, setting `owned` to `true`
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write(&mut self, offset: usize, data: &[u8]) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() % self.block_size, 0);

        self.data[offset..][..data.len()].copy_from_slice(data);
        self.owned[offset / self.block_size..][..data.len() / self.block_size]
            .fill(1);
    }

    /// Writes data to the buffer where `owned` is true
    ///
    /// If `owned[i]` is `false`, then that block is not written
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    /// - `data` and `owned` must be the same size
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write_if_owned(
        &mut self,
        offset: usize,
        data: &[u8],
        owned: &[bool],
    ) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(data.len() % self.block_size, 0);
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() / self.block_size, owned.len());

        let start_block = offset / self.block_size;
        for (b, chunk) in data.chunks(self.block_size).enumerate() {
            debug_assert_eq!(chunk.len(), self.block_size);
            if owned[b] {
                let block = start_block + b;
                self.owned[block] = 1;
                self.block_mut(block).copy_from_slice(chunk);
            }
        }
    }

    /// Writes a `ReadResponse` into the buffer, setting `owned` to true
    ///
    /// The `ReadResponse` must contain a single block's worth of data.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The response data length must be block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write_read_response(
        &mut self,
        offset: usize,
        response: &ReadResponse,
    ) {
        assert!(offset + response.data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(response.data.len(), self.block_size);
        if !response.block_contexts.is_empty() {
            let block = offset / self.block_size;
            self.owned[block] = 1;
            self.block_mut(block).copy_from_slice(&response.data);
        }
    }

    /// Reads buffer data into the given array
    ///
    /// Only blocks with `self.owned` are changed; other blocks are left
    /// unmodified.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The response data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn read(&self, offset: usize, data: &mut [u8]) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() % self.block_size, 0);

        let start_block = offset / self.block_size;
        for (b, chunk) in data.chunks_mut(self.block_size).enumerate() {
            debug_assert_eq!(chunk.len(), self.block_size);
            let block = start_block + b;
            if self.owned[block] != 0 {
                chunk.copy_from_slice(self.block(block));
            }
        }
    }

    /// Consumes the buffer and returns a `Vec<u8>` object
    ///
    /// This is inefficient and should only be used during testing
    #[cfg(test)]
    pub fn into_vec(self) -> Vec<u8> {
        self.data.into_iter().collect()
    }

    /// Consume and layer buffer contents on top of this one
    ///
    /// The `buffer` argument will be reset to zero size, but preserves its
    /// allocations for reuse.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - Both buffers must have the same block size
    ///
    /// If either of these conditions is not met, the function will panic
    pub(crate) fn eat(&mut self, offset: usize, buffer: &mut Buffer) {
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(self.block_size, buffer.block_size);

        let start_block = offset / self.block_size;
        for (b, (owned, chunk)) in buffer.blocks().enumerate() {
            if owned {
                let block = start_block + b;
                self.owned[block] = 1;
                self.block_mut(block).copy_from_slice(chunk);
            }
        }

        buffer.reset(0, self.block_size);
    }

    pub fn owned_ref(&self) -> &[u8] {
        &self.owned
    }

    pub fn reset(&mut self, block_count: usize, block_size: usize) {
        self.data.clear();
        self.owned.clear();

        let len = block_count * block_size;
        self.data.resize(len, 0u8);
        self.owned.resize(block_count, 0);
        self.block_size = block_size;
    }

    /// Returns a reference to a particular block
    pub fn block(&self, b: usize) -> &[u8] {
        &self.data[b * self.block_size..][..self.block_size]
    }

    /// Returns a mutable reference to a particular block
    pub fn block_mut(&mut self, b: usize) -> &mut [u8] {
        &mut self.data[b * self.block_size..][..self.block_size]
    }

    /// Returns an iterator over `(owned, block)` tuples
    pub fn blocks(&self) -> impl Iterator<Item = (bool, &[u8])> {
        self.owned
            .iter()
            .map(|v| *v != 0)
            .zip(self.data.chunks(self.block_size))
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[test]
fn test_buffer_sane() {
    const BLOCK_SIZE: usize = 512;
    let mut data = Buffer::new(2, BLOCK_SIZE);

    data.write(0, &[99u8; BLOCK_SIZE]);

    let mut read_data = vec![0u8; BLOCK_SIZE];
    data.read(0, &mut read_data);

    assert_eq!(&read_data[..], &data[..BLOCK_SIZE]);
    assert_eq!(&data[..BLOCK_SIZE], &[99u8; BLOCK_SIZE]);
}

#[test]
fn test_buffer_len() {
    const READ_SIZE: usize = 512;
    let data = Buffer::repeat(0x99, 1, READ_SIZE);
    assert_eq!(data.len(), READ_SIZE);
}

#[test]
fn test_buffer_len_over_block_size() {
    const READ_SIZE: usize = 1024;
    let data = Buffer::repeat(0x99, 2, 512);
    assert_eq!(data.len(), READ_SIZE);
}

#[test]
fn test_buffer_writes() {
    const BLOCK_COUNT: usize = 8;
    const BLOCK_SIZE: usize = 64;
    let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE); // 512 bytes

    assert_eq!(&data[..], &vec![0u8; 512]);

    data.write(64, &[1u8; 64]);

    assert_eq!(&data[0..64], &vec![0u8; 64]);
    assert_eq!(&data[64..128], &vec![1u8; 64]);
    assert_eq!(&data[128..], &vec![0u8; 512 - 64 - 64]);

    data.write(128, &[7u8; 128]);

    assert_eq!(&data[0..64], &vec![0u8; 64]);
    assert_eq!(&data[64..128], &vec![1u8; 64]);
    assert_eq!(&data[128..256], &vec![7u8; 128]);
    assert_eq!(&data[256..], &vec![0u8; 256]);
}

#[test]
fn test_buffer_eats() {
    // We use an artificially low BLOCK_SIZE here for ease of alignment
    const BLOCK_COUNT: usize = 8;
    const BLOCK_SIZE: usize = 64;
    let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE); // 512 bytes

    assert_eq!(&data[..], &vec![0u8; 512]);

    let mut buffer = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
    buffer.eat(0, &mut data);

    assert_eq!(&buffer[..], &vec![0u8; 512]);

    let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
    data.write(64, &[1u8; 64]);
    buffer.eat(0, &mut data);

    assert_eq!(&buffer[0..64], &vec![0u8; 64]);
    assert_eq!(&buffer[64..128], &vec![1u8; 64]);
    assert_eq!(&buffer[128..], &vec![0u8; 512 - 64 - 64]);

    let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
    data.write(128, &[7u8; 128]);
    buffer.eat(0, &mut data);

    assert_eq!(&buffer[0..64], &vec![0u8; 64]);
    assert_eq!(&buffer[64..128], &vec![1u8; 64]);
    assert_eq!(&buffer[128..256], &vec![7u8; 128]);
    assert_eq!(&buffer[256..], &vec![0u8; 256]);
}

/*
 * Inspired from Propolis block.rs
 *
 * The following are the operations that Crucible supports from outside
 * callers. We have extended this to cover a bunch of test operations as
 * well. The first three are the supported operations, the other operations
 * tell the upstairs to behave in specific ways.
 */
#[derive(Debug)]
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
        data: Buffer::new(1, 512),
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(8, 512), // 4096 bytes
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(31, 512), // 15872 bytes < 16000
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 1);

    let op = BlockOp::Read {
        offset: Block::new_512(1),
        data: Buffer::new(32, 512), // 16384 bytes > 16000
    };
    assert_eq!(op.iops(IOP_SZ).unwrap(), 2);
}

/**
 * Stat counters struct used by DTrace
 */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Arg {
    /// Jobs on the upstairs guest work queue.
    pub up_count: u32,
    /// Apply loop counter
    pub up_counters: UpCounters,
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
    /// Per-client delay to keep them roughly in sync
    pub ds_delay_us: [usize; 3],
    /// Times we skipped repairing a downstairs because we are read_only.
    pub ds_ro_lr_skipped: [usize; 3],
}

/*
 * This is the main upstairs task that starts all the other async tasks.
 *
 * It will return Ok with a join handle if every required task was successfully
 * launched, and Err otherwise.
 */
pub fn up_main(
    opt: CrucibleOpts,
    gen: u64,
    region_def: Option<RegionDefinition>,
    guest: GuestIoHandle,
    producer_registry: Option<ProducerRegistry>,
) -> Result<tokio::task::JoinHandle<()>> {
    register_probes().unwrap();
    let log = guest.log.clone();

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

    #[cfg(test)]
    let disable_backpressure = guest.is_queue_backpressure_disabled();

    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let mut up =
        upstairs::Upstairs::new(&opt, gen, region_def, guest, tls_context);

    #[cfg(test)]
    if disable_backpressure {
        up.disable_client_backpressure();
    }

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
