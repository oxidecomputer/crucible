// Copyright 2023 Oxide Computer Company
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

pub use crucible_client_types::{
    CrucibleOpts, RegionExtentInfo, ReplaceResult, VolumeConstructionRequest,
};
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
use tracing::instrument;
use usdt::register_probes;
use uuid::Uuid;

use aes_gcm_siv::aead::AeadInPlace;
use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit, Nonce, Tag};

pub mod control;

#[cfg(test)]
mod dummy_downstairs_tests;

mod pseudo_file;

pub mod volume;
pub use volume::{Volume, VolumeBuilder};

pub mod in_memory;
pub use in_memory::InMemoryBlockIO;

pub mod block_io;
pub use block_io::{FileBlockIO, ReqwestBlockIO};

pub mod block_req;
pub(crate) use block_req::{BlockOpWaiter, BlockRes};

mod buffer;
pub use buffer::Buffer; // used in BlockIO::Read, so it must be public
pub(crate) use buffer::UninitializedBuffer; // only used in pub(crate) functions

mod mend;
pub use mend::{DownstairsMend, ExtentFix, RegionMetadata};
pub use pseudo_file::CruciblePseudoFile;

pub(crate) mod guest;
pub use guest::{Guest, WQCounts};
use guest::{GuestBlockRes, GuestIoHandle};

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

mod io_limits;
use io_limits::IOLimitGuard;

/// Max number of write bytes between the upstairs and an offline downstairs
///
/// If we exceed this value, the upstairs will give up and mark the offline
/// downstairs as faulted.
const IO_OUTSTANDING_MAX_BYTES: u64 = 50 * 1024 * 1024; // 50 MiB

/// Max number of outstanding IOs between the upstairs and an offline downstairs
///
/// If we exceed this value, the upstairs will give up and mark that offline
/// downstairs as faulted.
pub const IO_OUTSTANDING_MAX_JOBS: usize = 1000;

/// Maximum of bytes to cache from complete (but un-flushed) IO
///
/// Caching complete jobs allows us to replay them if a Downstairs goes offline
/// them comes back.
const IO_CACHED_MAX_BYTES: u64 = 1024 * 1024 * 64; // 64 MiB

/// Maximum of jobs to cache from complete (but un-flushed) IO
///
/// Caching complete jobs allows us to replay them if a Downstairs goes offline
/// them comes back.
const IO_CACHED_MAX_JOBS: u64 = 10000;

// Re-exports for unit testing
#[cfg(feature = "integration-tests")]
pub mod testing {
    pub const IO_CACHED_MAX_JOBS: u64 = super::IO_CACHED_MAX_JOBS;
}

/// The BlockIO trait behaves like a physical NVMe disk (or a virtio virtual
/// disk): there is no contract about what order operations that are submitted
/// between flushes are performed in.
#[async_trait]
pub trait BlockIO: Sync {
    async fn activate(&self) -> Result<(), CrucibleError>;
    async fn activate_with_gen(&self, gen: u64) -> Result<(), CrucibleError>;

    async fn deactivate(&self) -> Result<(), CrucibleError>;

    async fn query_is_active(&self) -> Result<bool, CrucibleError>;
    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError>;
    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError>;

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
        offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError>;

    async fn write(
        &self,
        offset: BlockIndex,
        data: BytesMut,
    ) -> Result<(), CrucibleError>;

    async fn write_unwritten(
        &self,
        offset: BlockIndex,
        data: BytesMut,
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
    ) -> Result<BlockIndex, CrucibleError> {
        let bs = self.get_block_size().await?;

        if (offset % bs) != 0 {
            crucible_bail!(OffsetUnaligned);
        }

        Ok(BlockIndex(offset / bs))
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
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        if !self.query_is_active().await? {
            return Err(CrucibleError::UpstairsInactive);
        }

        self.write(self.byte_offset_to_block(offset).await?, data)
            .await
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
/// `guest` structure and created a new `ds_id` used to track this IO
/// through the system.  This should be as early as possible.
///
/// up__to__ds_*_start: (Upstairs__to__Downstairs) At this point we have
/// created the structures to track this IO through the Upstairs and added
/// it to internal work queues, including the work queue for the three
/// downstairs tasks that are responsible for sending IO to each
/// downstairs.  This probe firing does not mean that a downstairs task
/// has received or is acting on the IO yet, it just means the notification
/// has been sent.
///
/// ds__*__client__start: This is when a job is sent to the client task
/// who will handle the network transfer
///
/// ds__*__net__start: This is when a downstairs client task puts an IO on
/// the wire to the actual downstairs that will do the work. This probe has
/// both the job ID and the client ID so we can tell the individual
/// downstairs apart.
///
/// ds__*__net__done: An ACK has been received from a downstairs for an IO
/// sent to it. At the point of this probe the IO has just come off the
/// wire and we have not processed it yet.
///
/// ds__*__client__done: This probe indicates a message off the wire has
/// been sent back from the client rx task to the main task and is now being
/// processed.
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
    fn gw__barrier__start(_: u64) {}
    fn gw__close__start(_: u64, _: u32) {}
    fn gw__repair__start(_: u64, _: u32) {}
    fn gw__noop__start(_: u64) {}
    fn gw__reopen__start(_: u64, _: u32) {}
    fn up__to__ds__read__start(_: u64) {}
    fn up__to__ds__write__start(_: u64) {}
    fn up__to__ds__write__unwritten__start(_: u64) {}
    fn up__to__ds__flush__start(_: u64) {}
    fn up__to__ds__barrier__start(_: u64) {}
    fn up__block__req__dropped() {}
    fn ds__read__client__start(_: u64, _: u8) {}
    fn ds__write__client__start(_: u64, _: u8) {}
    fn ds__write__unwritten__client__start(_: u64, _: u8) {}
    fn ds__flush__client__start(_: u64, _: u8) {}
    fn ds__barrier__client__start(_: u64, _: u8) {}
    fn ds__close__start(_: u64, _: u8, _: u32) {}
    fn ds__repair__start(_: u64, _: u8, _: u32) {}
    fn ds__noop__start(_: u64, _: u8) {}
    fn ds__reopen__start(_: u64, _: u8, _: u32) {}
    fn ds__read__net__start(_: u64, _: u8) {}
    fn ds__write__net__start(_: u64, _: u8) {}
    fn ds__write__unwritten__net__start(_: u64, _: u8) {}
    fn ds__flush__net__start(_: u64, _: u8) {}
    fn ds__close__net__start(_: u64, _: u8, _: u32) {}
    fn ds__repair__net__start(_: u64, _: u8, _: u32) {}
    fn ds__noop__net__start(_: u64, _: u8) {}
    fn ds__reopen__net__start(_: u64, _: u8, _: u32) {}
    fn ds__read__net__done(_: u64, _: u8) {}
    fn ds__write__net__done(_: u64, _: u8) {}
    fn ds__write__unwritten__net__done(_: u64, _: u8) {}
    fn ds__flush__net__done(_: u64, _: u8) {}
    fn ds__close__net__done(_: u64, _: u8) {}
    fn ds__read__client__done(_: u64, _: u8) {}
    fn ds__write__client__done(_: u64, _: u8) {}
    fn ds__write__unwritten__client__done(_: u64, _: u8) {}
    fn ds__flush__client__done(_: u64, _: u8) {}
    fn ds__barrier__client__done(_: u64, _: u8) {}
    fn ds__close__done(_: u64, _: u8) {}
    fn ds__repair__done(_: u64, _: u8) {}
    fn ds__noop__done(_: u64, _: u8) {}
    fn ds__reopen__done(_: u64, _: u8) {}
    fn gw__read__done(_: u64) {}
    fn gw__write__done(_: u64) {}
    fn gw__write__unwritten__done(_: u64) {}
    fn gw__flush__done(_: u64) {}
    fn gw__barrier__done(_: u64) {}
    fn gw__close__done(_: u64, _: u32) {}
    fn gw__repair__done(_: u64, _: u32) {}
    fn gw__noop__done(_: u64) {}
    fn gw__reopen__done(_: u64, _: u32) {}
    fn extent__or__done(_: u64) {}
    fn reqwest__read__start(_: u32, _: Uuid) {}
    fn reqwest__read__done(_: u32, _: Uuid) {}
    fn volume__read__done(_: u32, _: Uuid) {}
    fn volume__write__done(_: u32, _: Uuid) {}
    fn volume__writeunwritten__done(_: u32, _: Uuid) {}
    fn volume__flush__done(_: u32, _: Uuid) {}
}

/// Array of data associated with three clients, indexed by `ClientId`
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
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

    /// Builds a `ClientData` from a builder function
    pub fn from_fn<F: FnMut(ClientId) -> T>(mut f: F) -> Self {
        Self([
            f(ClientId::new(0)),
            f(ClientId::new(1)),
            f(ClientId::new(2)),
        ])
    }

    /// Builds a new `ClientData` by applying a function to each item
    pub fn map<U, F: FnMut(T) -> U>(self, f: F) -> ClientData<U> {
        ClientData(self.0.map(f))
    }

    #[cfg(test)]
    pub fn get(&self) -> &[T; 3] {
        &self.0
    }
}

/// Map of data associated with clients, keyed by `ClientId`
#[derive(Copy, Clone, Debug)]
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
    pub fn contains(&self, c: &ClientId) -> bool {
        self.0[*c].is_some()
    }
    pub fn take(&mut self, c: &ClientId) -> Option<T> {
        self.0[*c].take()
    }
}

impl<T> std::ops::Index<ClientId> for ClientMap<T> {
    type Output = T;
    fn index(&self, index: ClientId) -> &Self::Output {
        self.get(&index).unwrap()
    }
}

impl<T> From<ClientData<T>> for ClientMap<T> {
    fn from(c: ClientData<T>) -> Self {
        Self(ClientData(c.0.map(Option::Some)))
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

    pub fn encrypt_in_place(&self, data: &mut [u8]) -> (Nonce, Tag, u64) {
        let nonce = self.get_random_nonce();

        // Encryption is infallible as long as the data and nonce are both below
        // 1 << 36 bytes (A_MAX and P_MAX from RFC8452 § 6).  We encrypt on a
        // per-block basis (max of 1 << 15) with a 12-byte nonce, so these
        // conditions should never fail.
        let tag = match self.cipher.encrypt_in_place_detached(&nonce, b"", data)
        {
            Ok(tag) => tag,
            Err(e) => panic!("Could not encrypt! {e:?}"),
        };

        // Hash [nonce + tag + data] in that order. Perform this after
        // encryption so that the downstairs can verify it without the key.
        let computed_hash = integrity_hash(&[&nonce[..], &tag[..], data]);

        (nonce, tag, computed_hash)
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

/// Write data, containing data from all blocks
#[derive(Debug)]
pub struct RawWrite {
    /// Per-block metadata
    pub blocks: Vec<BlockContext>,
    /// Raw data
    pub data: bytes::BytesMut,
}

impl RawWrite {
    /// Builds a new empty `RawWrite` with the given capacity
    pub fn with_capacity(block_count: usize, block_size: u64) -> Self {
        Self {
            blocks: Vec::with_capacity(block_count),
            data: bytes::BytesMut::with_capacity(
                block_count * block_size as usize,
            ),
        }
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

/// Read response data, containing data from all blocks
///
/// Do not derive `Clone` on this type; it will be expensive and tempting to
/// call by accident!
#[derive(Debug, Default)]
pub(crate) struct RawReadResponse {
    /// Per-block metadata
    pub blocks: Vec<ReadBlockContext>,
    /// Raw data
    pub data: bytes::BytesMut,
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
 *                ┌──┐   ▼
 *             bad│  │   │
 *         version│ ┌▼───┴──────┐
 *                └─┤           ╞═════◄══════════════════╗
 *    ┌─────────────►    New    ╞═════◄════════════════╗ ║
 *    │       ┌─────►           ├─────◄──────┐         ║ ║
 *    │       │     └────┬───┬──┘            │         ║ ║
 *    │       │          ▼   └───►───┐ other │         ║ ║
 *    │    bad│     ┌────┴──────┐    │ failures        ║ ║
 *    │ region│     │   Wait    │    │       ▲         ║ ║
 *    │       │     │  Active   ├─►┐ │       │         ║ ║
 *    │       │     └────┬──────┘  │ │       │         ║ ║
 *    │       │     ┌────┴──────┐  │ └───────┤         ║ ║
 *    │       │     │   Wait    │  └─────────┤         ║ ║
 *    │       └─────┤  Quorum   ├──►─────────┤         ║ ║
 *    │             └────┬──────┘            │         ║ ║
 *    │          ........▼..........         │         ║ ║
 *    │failed    :  ┌────┴──────┐  :         │         ║ ║
 *    │reconcile :  │ Reconcile │  :         │       ╔═╝ ║
 *    └─────────────┤           ├──►─────────┘       ║   ║
 *               :  └────┬──────┘  :                 ║   ║
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
 *              │     │   │   │    │                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   │   │    │                     ║     │
 *              │     │   ▼   ▲    ▲                     ║     │
 *              │     │   │   │    │                     ▲     │
 *              │     │ ┌─┴───┴────┴┐       ┌────────────╨──┐  │
 *              │     │ │  Offline  │       │   Faulted     │  │
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
     * Waiting for activation signal.
     */
    WaitActive,
    /*
     * Waiting for the minimum number of downstairs to be present.
     */
    WaitQuorum,
    /*
     * Initial startup, downstairs are repairing from each other.
     */
    Reconcile,
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
     * This downstairs was active, but is now no longer connected.
     * We may have work for it in memory, so a replay is possible
     * if this downstairs reconnects in time.
     */
    Offline,
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
            DsState::WaitActive => {
                write!(f, "WaitActive")
            }
            DsState::WaitQuorum => {
                write!(f, "WaitQuorum")
            }
            DsState::Reconcile => {
                write!(f, "Reconcile")
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
            DsState::Offline => {
                write!(f, "Offline")
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
#[derive(Debug)]
struct DownstairsIO {
    work: IOop,

    /// Map of work status, tracked on a per-client basis
    state: ClientData<IOState>,

    /// Reply handle to send data back to the guest
    res: Option<GuestBlockRes>,

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

    /// If the operation is a Read, this holds the resulting buffer and hashes
    ///
    /// The buffer _may_ be removed during the transfer to the Guest, to reduce
    /// `memcpy` overhead.  If this occurs, the hashes remain present for
    /// consistency checking with subsequent replies.
    data: Option<RawReadResponse>,

    io_limits: ClientMap<io_limits::ClientIOLimitGuard>,
}

impl DownstairsIO {
    fn state_count(&self) -> WorkCounts {
        let mut wc: WorkCounts = Default::default();

        for state in self.state.iter() {
            match state {
                IOState::InProgress => wc.active += 1,
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
        self.work.job_bytes() as usize
    }

    /*
     * Return a summary of this job in the form of the WorkSummary struct.
     */
    pub fn io_summarize(&self, id: JobId) -> WorkSummary {
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
            id,
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
            IOop::Read { .. } => wc.done == 0,
            IOop::Write { .. }
            | IOop::WriteUnwritten { .. }
            | IOop::Flush { .. }
            | IOop::Barrier { .. } => wc.skipped + wc.error > 1,
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
    state: ClientData<ReconcileIOState>,
}

impl ReconcileIO {
    fn new(id: ReconciliationId, op: Message) -> ReconcileIO {
        ReconcileIO {
            id,
            op,
            state: ClientData::new(ReconcileIOState::New),
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
        /// Jobs that mush finish before this
        dependencies: Vec<JobId>,
        /// Extent in which the write starts
        start_eid: ExtentId,
        /// Relative offset (within that extent) to start writing
        start_offset: BlockOffset,
        /// Per-block context
        blocks: Vec<BlockContext>,
        /// Raw data, tightly packed
        data: bytes::Bytes,
    },
    WriteUnwritten {
        /// Jobs that mush finish before this
        dependencies: Vec<JobId>,
        /// Extent in which the write starts
        start_eid: ExtentId,
        /// Relative offset (within that extent) to start writing
        start_offset: BlockOffset,
        /// Per-block context
        blocks: Vec<BlockContext>,
        /// Raw data, tightly packed
        data: bytes::Bytes,
    },
    Read {
        /// Jobs that must finish before this
        dependencies: Vec<JobId>,
        /// Extent in which the read starts
        start_eid: ExtentId,
        /// Relative offset (within that extent) to start reading
        start_offset: BlockOffset,
        /// Number of blocks to read
        count: u64,
        /// Block size for this region (stored here for convenience)
        block_size: u64,
    },
    Flush {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
        extent_limit: Option<ExtentId>,
    },
    Barrier {
        dependencies: Vec<JobId>, // Jobs that must finish before this
    },
    /*
     * These operations are for repairing a bad downstairs
     */
    ExtentFlushClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
        flush_number: u64,
        gen_number: u64,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveRepair {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
        source_downstairs: ClientId,
        source_repair_address: SocketAddr,
        repair_downstairs: Vec<ClientId>,
    },
    ExtentLiveReopen {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
    },
    ExtentLiveNoOp {
        dependencies: Vec<JobId>, // Jobs that must finish before this
    },
}

impl IOop {
    #[cfg(test)]
    fn deps(&self) -> &[JobId] {
        match &self {
            IOop::Write { dependencies, .. }
            | IOop::Flush { dependencies, .. }
            | IOop::Barrier { dependencies, .. }
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

    /// Returns `true` if the `IOop` is a `Write` or `WriteUnwritten`
    fn is_write(&self) -> bool {
        matches!(self, IOop::Write { .. } | IOop::WriteUnwritten { .. })
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
                count,
                ..
            } => {
                let job_type = "Read".to_string();
                (job_type, *count as usize, dependencies.clone())
            }
            IOop::Write {
                dependencies,
                blocks,
                ..
            } => {
                let job_type = "Write".to_string();
                (job_type, blocks.len(), dependencies.clone())
            }
            IOop::WriteUnwritten {
                dependencies,
                blocks,
                ..
            } => {
                let job_type = "WriteU".to_string();
                (job_type, blocks.len(), dependencies.clone())
            }
            IOop::Flush { dependencies, .. } => {
                let job_type = "Flush".to_string();
                (job_type, 0, dependencies.clone())
            }
            IOop::Barrier { dependencies, .. } => {
                let job_type = "Barrier".to_string();
                (job_type, 0, dependencies.clone())
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                ..
            } => {
                let job_type = "FClose".to_string();
                (job_type, extent.0 as usize, dependencies.clone())
            }
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                ..
            } => {
                let job_type = "Repair".to_string();
                (job_type, extent.0 as usize, dependencies.clone())
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                let job_type = "Reopen".to_string();
                (job_type, extent.0 as usize, dependencies.clone())
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
    fn send_io_live_repair(&self, extent_limit: Option<ExtentId>) -> bool {
        // Always send live-repair IOs
        if matches!(
            self,
            IOop::ExtentLiveReopen { .. }
                | IOop::ExtentFlushClose { .. }
                | IOop::ExtentLiveRepair { .. }
                | IOop::ExtentLiveNoOp { .. }
        ) {
            true
        } else if let Some(extent_limit) = extent_limit {
            // The extent_limit has been set, so we have repair work in
            // progress.  If our IO touches an extent less than or equal
            // to the extent_limit, then we go ahead and send it.
            //
            // The special case of IOs that span extents repaired and not
            // repaired is handled with dependencies, and IOs should arrive
            // here with those dependencies already set.
            match &self {
                IOop::Write { start_eid, .. }
                | IOop::WriteUnwritten { start_eid, .. }
                | IOop::Read { start_eid, .. } => *start_eid <= extent_limit,
                IOop::Flush { .. } => {
                    // If we have set extent limit, then we go ahead and
                    // send the flush with the extent_limit in it, and allow
                    // the downstairs to act based on that.
                    true
                }
                IOop::Barrier { .. } => {
                    // The Barrier IOop doesn't actually touch any extents; it's
                    // purely for dependency management.
                    true
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
                data.len() as u64
            }
            IOop::Read {
                count, block_size, ..
            } => *block_size * *count,
            _ => 0,
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileIOState {
    // A new IO request.
    New,
    // The request has been sent to this tasks downstairs.
    InProgress,
    // The successful response came back from downstairs.
    Done,
    // The IO request should be ignored. Ex: we could be doing recovery and
    // we only want a specific downstairs to do that work.
    Skipped,
}

/*
 * The various states an IO can be in when it is on the work hashmap.
 * There is a state that is unique to each downstairs task we have and
 * they operate independent of each other.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum IOState {
    /// The request has been sent to this tasks downstairs.
    InProgress,
    /// The successful response came back from downstairs.
    Done,
    /// The IO request should be ignored. Ex: we could be doing recovery and
    /// we only want a specific downstairs to do that work.
    Skipped,
    /// The IO returned an error.
    Error(CrucibleError),
}

impl fmt::Display for IOState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Make sure to right-align output on 4 characters
        match self {
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

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct ClientIOStateCount<T = u32> {
    pub in_progress: T,
    pub done: T,
    pub skipped: T,
    pub error: T,
}

impl<T> std::ops::Index<&IOState> for ClientIOStateCount<T> {
    type Output = T;
    fn index(&self, index: &IOState) -> &Self::Output {
        match index {
            IOState::InProgress => &self.in_progress,
            IOState::Done => &self.done,
            IOState::Skipped => &self.skipped,
            IOState::Error(_) => &self.error,
        }
    }
}

impl<T> std::ops::IndexMut<&IOState> for ClientIOStateCount<T> {
    fn index_mut(&mut self, index: &IOState) -> &mut Self::Output {
        match index {
            IOState::InProgress => &mut self.in_progress,
            IOState::Done => &mut self.done,
            IOState::Skipped => &mut self.skipped,
            IOState::Error(_) => &mut self.error,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct IOStateCount {
    pub in_progress: ClientData<u32>,
    pub done: ClientData<u32>,
    pub skipped: ClientData<u32>,
    pub error: ClientData<u32>,
}

impl IOStateCount {
    fn show_all(&self) {
        println!("   STATES      DS:0   DS:1   DS:2   TOTAL");
        self.show(IOState::InProgress);
        self.show(IOState::Done);
        self.show(IOState::Skipped);
        let e = CrucibleError::GenericError("x".to_string());
        self.show(IOState::Error(e));
    }

    fn get(&self, state: &IOState) -> &ClientData<u32> {
        match state {
            IOState::InProgress => &self.in_progress,
            IOState::Done => &self.done,
            IOState::Skipped => &self.skipped,
            IOState::Error(_) => &self.error,
        }
    }

    fn show(&self, state: IOState) {
        let state_stat = self.get(&state);
        match state {
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

/// Operations supported by Crucible
///
/// This is inspired by Propolis's `block.rs`, but is internal to the Crucible
/// upstairs crate (i.e. what you're reading right now).  The outside world
/// uses the [`Guest`] API, which translates into these types internally when
/// passing messages to the [`Upstairs`](crate::upstairs::Upstairs).
#[derive(Debug)]
pub(crate) enum BlockOp {
    Read {
        offset: BlockIndex,
        data: Buffer,
        done: BlockRes<Buffer, (Buffer, CrucibleError)>,
        io_guard: IOLimitGuard,
    },
    Write {
        offset: BlockIndex,
        data: BytesMut,
        done: BlockRes,
        io_guard: IOLimitGuard,
    },
    WriteUnwritten {
        offset: BlockIndex,
        data: BytesMut,
        done: BlockRes,
        io_guard: IOLimitGuard,
    },
    Flush {
        snapshot_details: Option<SnapshotDetails>,
        done: BlockRes,
        io_guard: IOLimitGuard,
    },
    GoActive {
        done: BlockRes,
    },
    GoActiveWithGen {
        gen: u64,
        done: BlockRes,
    },
    Deactivate {
        done: BlockRes,
    },
    // Management commands
    ReplaceDownstairs {
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
        done: BlockRes<ReplaceResult>,
    },
    // Query ops
    QueryBlockSize {
        done: BlockRes<u64>,
    },
    QueryTotalSize {
        done: BlockRes<u64>,
    },
    QueryGuestIOReady {
        done: BlockRes<bool>,
    },
    QueryUpstairsUuid {
        done: BlockRes<Uuid>,
    },
    // Begin testing options.
    QueryExtentInfo {
        done: BlockRes<RegionExtentInfo>,
    },
    QueryWorkQueue {
        done: BlockRes<WQCounts>,
    },
    // Show internal work queue, return outstanding IO requests.
    ShowWork {
        done: BlockRes<WQCounts>,
    },

    #[cfg(test)]
    GetDownstairsState {
        done: BlockRes<ClientData<DsState>>,
    },

    #[cfg(test)]
    FaultDownstairs {
        client_id: ClientId,
        done: BlockRes<()>,
    },
}

/**
 * Stat counters struct used by DTrace
 */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Arg {
    /// Upstairs session UUID
    pub session_id: String,
    /// Jobs on the upstairs guest work queue.
    pub up_count: u32,
    /// Apply loop counter
    pub up_counters: UpCounters,
    /// Next JobID
    pub next_job_id: JobId,
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
    /// Times we failed the initial reconcile.
    pub ds_reconcile_aborted: usize,
    /// Times we have completed a LiveRepair on a downstairs.
    pub ds_live_repair_completed: [usize; 3],
    /// Times we aborted a LiveRepair on a downstairs.
    pub ds_live_repair_aborted: [usize; 3],
    /// Times the upstairs has connected to a downstairs.
    pub ds_connected: [usize; 3],
    /// Times this downstairs has been replaced.
    pub ds_replaced: [usize; 3],
    /// Times we have live repaired an extent on this downstairs.
    pub ds_extents_repaired: [usize; 3],
    /// Times we have live confirmed  an extent on this downstairs.
    pub ds_extents_confirmed: [usize; 3],
    /// If in Live Repair, the current extent we are repairing.
    pub ds_extent_limit: usize,
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
    let disable_backpressure = guest.is_backpressure_disabled();

    /*
     * Build the Upstairs struct that we use to share data between
     * the different async tasks
     */
    let mut up =
        upstairs::Upstairs::new(&opt, gen, region_def, guest, tls_context);

    #[cfg(test)]
    if disable_backpressure {
        up.downstairs.disable_client_backpressure();
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

/// Gets a Nexus client based on any IPv6 address
#[cfg(feature = "notify-nexus")]
pub(crate) async fn get_nexus_client(
    log: &Logger,
    client: reqwest::Client,
    target_addrs: &[SocketAddr],
) -> Option<nexus_client::Client> {
    use internal_dns::resolver::Resolver;
    use internal_dns::ServiceName;
    use std::net::Ipv6Addr;

    // Use any rack internal address for `Resolver::new_from_ip`, as that will
    // use the AZ_PREFIX to find internal DNS servers.
    let mut addr: Option<Ipv6Addr> = None;

    for target_addr in target_addrs {
        match &target_addr {
            SocketAddr::V6(target_addr) => {
                addr = Some(*target_addr.ip());
                break;
            }

            SocketAddr::V4(_) => {
                // This branch is seen if compiling with the `notify-nexus`
                // feature but deploying in an ipv4 environment, usually during
                // development. `Resolver::new_from_ip` only accepts IPv6
                // addresses, so we can't use it to look up an address for the
                // Nexus client.
            }
        }
    }

    let Some(addr) = addr else {
        return None;
    };

    let resolver = match Resolver::new_from_ip(log.clone(), addr) {
        Ok(resolver) => resolver,
        Err(e) => {
            error!(log, "could not make resolver: {e}");
            return None;
        }
    };

    let nexus_address =
        match resolver.lookup_socket_v6(ServiceName::Nexus).await {
            Ok(addr) => addr,
            Err(e) => {
                error!(log, "lookup Nexus address failed: {e}");
                return None;
            }
        };

    Some(nexus_client::Client::new_with_client(
        &format!("http://{}", nexus_address),
        client,
        log.clone(),
    ))
}
