// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]

use futures::lock::Mutex;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crucible_common::{
    build_logger, crucible_bail, impacted_blocks::extent_from_offset,
    integrity_hash, mkdir_for_file, verbose_timeout, Block, CrucibleError,
    ExtentId, RegionDefinition, MAX_BLOCK_SIZE,
};
use crucible_protocol::{
    BlockContext, CrucibleDecoder, JobId, Message, MessageWriter,
    ReconciliationId, SnapshotDetails, CRUCIBLE_MESSAGE_VERSION,
};
use repair_client::Client;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use rand::prelude::*;
use slog::{debug, error, info, o, warn, Logger};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

pub mod admin;
mod dump;
mod dynamometer;
mod extent;
pub mod region;
pub mod repair;
mod stats;

mod extent_inner_raw;
pub(crate) mod extent_inner_raw_common;
mod extent_inner_sqlite;

use extent::ExtentState;
use region::Region;

pub use admin::run_dropshot;
pub use dump::dump_region;
pub use dynamometer::*;
pub use stats::{DsCountStat, DsStatOuter};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
enum IOop {
    Write {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: RegionWrite,
    },
    WriteUnwritten {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        writes: RegionWrite,
    },
    Read {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        requests: RegionReadRequest,
    },
    Flush {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
        extent_limit: Option<ExtentId>,
    },
    /*
     * These operations are for repairing a bad downstairs
     */
    ExtentClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
    },
    ExtentFlushClose {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
        flush_number: u64,
        gen_number: u64,
    },
    ExtentLiveRepair {
        dependencies: Vec<JobId>, // Jobs that must finish before this
        extent: ExtentId,
        source_repair_address: SocketAddr,
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
    fn deps(&self) -> &[JobId] {
        match &self {
            IOop::Write { dependencies, .. }
            | IOop::Flush { dependencies, .. }
            | IOop::Read { dependencies, .. }
            | IOop::WriteUnwritten { dependencies, .. }
            | IOop::ExtentClose { dependencies, .. }
            | IOop::ExtentFlushClose { dependencies, .. }
            | IOop::ExtentLiveRepair { dependencies, .. }
            | IOop::ExtentLiveReopen { dependencies, .. }
            | IOop::ExtentLiveNoOp { dependencies } => dependencies,
        }
    }
}

/// Read request for a particular extent
///
/// `data` should be an uninitializezd borrowed section of the outgoing buffer
/// (i.e. from a [`RegionReadResponse`]), to reduce memory copies.
///
/// Do not derive `Clone` on this type; cloning the object will break the borrow
/// of `data`, so it's rarely the correct choice.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ExtentReadRequest {
    offset: Block,
    /// This buffer should be uninitialized; read size is set by its capacity
    data: BytesMut,
}

/// Ordered list of read requests
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RegionReadRequest(Vec<RegionReadReq>);

/// Inner type for a single read requests
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RegionReadReq {
    extent: ExtentId,
    offset: Block,
    count: NonZeroUsize,
}

impl RegionReadRequest {
    fn new(reqs: &[crucible_protocol::ReadRequest]) -> Self {
        let mut out = Self(vec![]);
        for req in reqs {
            match out.0.last_mut() {
                // Check whether this read request is contiguous and in the same
                // extent as the previous read request
                Some(prev)
                    if prev.extent == req.eid
                        && prev.offset.value + prev.count.get() as u64
                            == req.offset.value =>
                {
                    // If so, enlarge it by one block
                    prev.count = prev.count.saturating_add(1);
                }
                _ => {
                    // Otherwise, begin a new request
                    out.0.push(RegionReadReq {
                        extent: req.eid,
                        offset: req.offset,
                        count: NonZeroUsize::new(1).unwrap(),
                    });
                }
            }
        }
        out
    }

    fn iter(&self) -> impl Iterator<Item = &RegionReadReq> {
        self.0.iter()
    }
}

impl IntoIterator for RegionReadRequest {
    type Item = RegionReadReq;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Read response data, designed to prevent copies and reallocations
///
/// We initialize a read response before performing any reads, then chop up its
/// (allocated but uninitialized) buffer space into a set of
/// `ExtentReadRequest`.  Each `ExtentReadRequest` is transformed into an
/// `ExtentReadResponse` (reusing that chunk of buffer), which are then
/// reassembled into the original `RegionReadResponse`.
///
/// Do not derive `Clone` on this type; it will be expensive and tempting to
/// call by accident!
pub(crate) struct RegionReadResponse {
    blocks: Vec<Vec<BlockContext>>,
    data: BytesMut,
    uninit: BytesMut,
}

impl RegionReadResponse {
    /// Builds a new empty `ReadResponse` with the given capacity
    fn with_capacity(block_count: usize, block_size: u64) -> Self {
        Self {
            blocks: vec![],
            data: BytesMut::new(),
            uninit: BytesMut::with_capacity(block_size as usize * block_count),
        }
    }

    /// Borrow the head of this `ReadResponse`, turning it into a read request
    ///
    /// ## Before
    /// ```text
    /// self.uninit = [ ----------------------------------- ]
    /// ```
    ///
    /// ## After
    /// ```text
    /// self.uninit =             [ ----------------------- ]
    /// req         = [ -------- ]
    /// ```
    ///
    /// Note that the `BytesMut` objects remain uninitialized throughout this
    /// whole procedure (to avoid `memset` calls).  We'll read directly into the
    /// uninitialized request buffer, then call `unsplit` to build it back up.
    ///
    /// # Panics
    /// If the size of the request is larger than our remaining buffer capacity
    fn request(
        &mut self,
        offset: Block,
        block_count: usize,
    ) -> ExtentReadRequest {
        let mut data = self
            .uninit
            .split_off(block_count * offset.block_size_in_bytes() as usize);
        std::mem::swap(&mut data, &mut self.uninit);
        ExtentReadRequest { offset, data }
    }

    /// Merge the given `ExtentReadResponse` into the tail of this response
    ///
    /// ## Before
    /// ```text
    /// self.data = [ -------- ]
    /// r.data    =             [ ----------------------- ]
    /// ```
    ///
    /// ## After
    /// ```text
    /// self.data = [ ----------------------------------- ]
    /// ```
    ///
    /// # Panics
    /// If the buffers cannot be unsplit without a reallocation; this indicates
    /// a logic error in our code.
    fn unsplit(&mut self, r: ExtentReadResponse) {
        // The most common case is for a read to only touch one extent, so we
        // have a fast path for just moving the blocks Vec.
        if self.blocks.is_empty() {
            self.blocks = r.blocks;
        } else {
            self.blocks.extend(r.blocks);
        }
        let prev_ptr = self.data.as_ptr();
        let was_empty = self.data.is_empty();
        self.data.unsplit(r.data);
        assert!(was_empty || prev_ptr == self.data.as_ptr());
    }

    /// Returns hashes for the given response
    ///
    /// This is expensive and should only be used for debugging
    pub fn hashes(&self, i: usize) -> Vec<u64> {
        self.blocks[i].iter().map(|ctx| ctx.hash).collect()
    }

    /// Returns encryption contexts for the given response
    ///
    /// This is expensive and should only be used for debugging
    pub fn encryption_contexts(
        &self,
        i: usize,
    ) -> Vec<Option<&crucible_protocol::EncryptionContext>> {
        self.blocks[i]
            .iter()
            .map(|ctx| ctx.encryption_context.as_ref())
            .collect()
    }
}

/// Read response from a single extent
///
/// Do not derive `Clone` on this type; it will be expensive and tempting to
/// call by accident!
pub(crate) struct ExtentReadResponse {
    blocks: Vec<Vec<BlockContext>>,
    /// At this point, the buffer must be fully initialized
    data: BytesMut,
}

/// Deserialized write operation, contiguous to a single extent
///
/// `data` should be a borrowed section of the received `Message::Write`, to
/// reduce memory copies.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ExtentWrite {
    /// Block offset, relative to the start of this extent
    pub offset: Block,

    /// One context per block to be written
    pub block_contexts: Vec<BlockContext>,

    /// Raw block data to be written
    ///
    /// This must be compatible with `block_contexts`, i.e.
    /// `block_contexts.len() == data.len() / block_size` for this extent's
    /// chosen block size.
    pub data: bytes::Bytes,
}

/// A `RegionWrite` is an ordered list of extent writes
///
/// Each element is a `RegionWriteReq`, i.e. a tuple of `(extent id, write)`.
/// The same extent may appear multiple times in the output list, if the source
/// write has multiple non-contiguous writes to that extent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RegionWrite(Vec<RegionWriteReq>);

/// A single request within a `RegionWrite`
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RegionWriteReq {
    extent: ExtentId,
    write: ExtentWrite,
}

impl RegionWrite {
    /// Destructures into a list of contiguous writes, borrowing the buffer
    ///
    /// Returns an error if `buf.len()` is not an even multiple of
    /// `header.blocks.len()`.
    pub fn new(
        blocks: &[crucible_protocol::WriteBlockMetadata],
        mut buf: bytes::Bytes,
    ) -> Result<Self, CrucibleError> {
        let block_size = if let Some(b) = blocks.first() {
            b.offset.block_size_in_bytes() as usize
        } else {
            return Ok(Self(vec![]));
        };
        if buf.len() % block_size != 0 {
            return Err(CrucibleError::DataLenUnaligned);
        } else if buf.len() / block_size != blocks.len() {
            return Err(CrucibleError::InvalidNumberOfBlocks(format!(
                "got {} contexts but {} blocks",
                blocks.len(),
                buf.len() / block_size
            )));
        }
        let mut out = Self(vec![]);
        for b in blocks {
            if out.can_append(b) {
                out.append_block_context(b.block_context);
            } else {
                out.set_last_data(block_size, &mut buf);
                out.begin_write(*b);
            }
        }
        out.set_last_data(block_size, &mut buf);
        assert!(buf.is_empty());
        Ok(out)
    }

    /// Checks whether the next write can be appended to our existing write
    fn can_append(&self, b: &crucible_protocol::WriteBlockMetadata) -> bool {
        match self.0.last() {
            None => false,
            Some(prev) => {
                prev.extent == b.eid
                    && prev.write.offset.value
                        + prev.write.block_contexts.len() as u64
                        == b.offset.value
            }
        }
    }

    /// Sets the data member of the last write as part of construction
    ///
    /// # Panics
    /// If the last write already has data
    fn set_last_data(&mut self, block_size: usize, buf: &mut Bytes) {
        if let Some(prev) = self.0.last_mut() {
            assert!(prev.write.data.is_empty());
            let data =
                buf.split_to(prev.write.block_contexts.len() * block_size);
            prev.write.data = data;
        }
    }

    /// Appends the given block context to an existing write
    ///
    /// # Panics
    /// If there is no write
    fn append_block_context(&mut self, ctx: BlockContext) {
        self.0.last_mut().unwrap().write.block_contexts.push(ctx);
    }

    /// Begins a new write, guided by the given metadata
    fn begin_write(&mut self, b: crucible_protocol::WriteBlockMetadata) {
        self.0.push(RegionWriteReq {
            extent: b.eid,
            write: ExtentWrite {
                offset: b.offset,
                block_contexts: vec![b.block_context],
                data: bytes::Bytes::default(),
            },
        });
    }

    fn iter(&self) -> impl Iterator<Item = &RegionWriteReq> {
        self.0.iter()
    }
}

impl IntoIterator for RegionWrite {
    type Item = RegionWriteReq;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/*
 * Export the contents or partial contents of a Downstairs Region to
 * the file indicated.
 *
 * We will start from the provided start_block.
 * We will stop after "count" blocks are written to the export_path.
 */
pub async fn downstairs_export<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    export_path: P,
    start_block: u64,
    mut count: u64,
) -> Result<()> {
    /*
     * Export an existing downstairs region to a file
     */
    let (block_size, extent_size, extent_count) = region.region_def();
    let space_per_extent = extent_size.byte_value();
    assert!(block_size > 0);
    assert!(space_per_extent > 0);
    assert!(extent_count > 0);
    assert!(space_per_extent > 0);
    let file_size = space_per_extent * extent_count as u64;

    if count == 0 {
        count = extent_size.value * extent_count as u64;
    }

    println!(
        "Export total_size: {}  Extent size:{}  Total Extents:{}",
        file_size, space_per_extent, extent_count
    );
    println!(
        "Exporting from start_block: {}  count:{}",
        start_block, count
    );

    let mut out_file = File::create(export_path)?;
    let mut blocks_copied = 0;

    'eid_loop: for eid in (0..extent_count).map(ExtentId) {
        let extent_offset = space_per_extent * eid.0 as u64;
        for block_offset in 0..extent_size.value {
            if (extent_offset + block_offset) >= start_block {
                blocks_copied += 1;

                let response = region
                    .region_read(
                        &RegionReadRequest(vec![RegionReadReq {
                            extent: eid,
                            offset: Block::new_with_ddef(
                                block_offset,
                                &region.def(),
                            ),
                            count: NonZeroUsize::new(1).unwrap(),
                        }]),
                        JobId(0),
                    )
                    .await?;

                out_file.write_all(&response.data).unwrap();

                if blocks_copied >= count {
                    break 'eid_loop;
                }
            }
        }
    }

    println!("Read and wrote out {} blocks", blocks_copied);

    Ok(())
}

/*
 * Import the contents of a file into a new Region.
 * The total size of the region will be rounded up to the next largest
 * extent multiple.
 */
pub async fn downstairs_import<P: AsRef<Path> + std::fmt::Debug>(
    region: &mut Region,
    import_path: P,
) -> Result<()> {
    /*
     * Open the file to import and determine how many extents we will need
     * based on the length.
     */
    let mut f = File::open(&import_path)?;
    let file_size = f.metadata()?.len();
    let (_, extent_size, _) = region.region_def();
    let space_per_extent = extent_size.byte_value();

    let mut extents_needed = file_size / space_per_extent;
    if file_size % space_per_extent != 0 {
        extents_needed += 1;
    }
    println!(
        "Import file_size: {}  Extent size: {}  Needed extents: {}",
        file_size, space_per_extent, extents_needed
    );

    if extents_needed > region.def().extent_count().into() {
        /*
         * The file to import would require more extents than we have.
         * Extend the region to fit the file.
         */
        println!("Extending region to fit image");
        region.extend(extents_needed as u32).await?;
    } else {
        println!("Region already large enough for image");
    }

    println!("Importing {:?} to region", import_path);
    let rm = region.def();

    /*
     * We want to read and write large chunks of data, rather than individual
     * blocks, to improve import performance.  The chunk buffer must be a
     * whole number of the largest block size we are able to support.
     */
    const CHUNK_SIZE: usize = 32 * 1024 * 1024;
    assert_eq!(CHUNK_SIZE % MAX_BLOCK_SIZE, 0);

    let mut offset = Block::new_with_ddef(0, &region.def());
    loop {
        let mut buffer = BytesMut::new();
        buffer.resize(CHUNK_SIZE, 0u8);

        /*
         * Read data into the buffer until it is full, or we hit EOF.
         */
        let mut total = 0;
        loop {
            assert!(total <= CHUNK_SIZE);
            if total == CHUNK_SIZE {
                break;
            }

            /*
             * Rust's read guarantees that if it returns Ok(n) then
             * `0 <= n <= buffer.len()`. We have to repeatedly read until our
             * buffer is full.
             */
            let n = f.read(&mut buffer[total..])?;

            if n == 0 {
                /*
                 * We have hit EOF.  Extend the read buffer with zeroes until
                 * it is a multiple of the block size.
                 */
                while !Block::is_valid_byte_size(total, &rm) {
                    buffer[total] = 0;
                    total += 1;
                }
                break;
            }

            total += n;
        }

        if total == 0 {
            /*
             * If we read zero bytes without error, then we are done.
             */
            break;
        }

        // Crop the buffer to exactly our write size
        buffer.resize(total, 0u8);
        let buffer = buffer.freeze();

        /*
         * Use the same function upstairs uses to decide where to put the
         * data based on the LBA offset.
         */
        let nblocks = Block::from_bytes(total, &rm);
        let block_size = region.def().block_size() as usize;
        let blocks: Vec<_> = extent_from_offset(&rm, offset, nblocks)
            .blocks(&rm)
            .zip(
                buffer
                    .chunks(block_size)
                    .map(|chunk| integrity_hash(&[chunk])),
            )
            .map(|((eid, offset), hash)| {
                crucible_protocol::WriteBlockMetadata {
                    eid,
                    offset,
                    block_context: BlockContext {
                        hash,
                        encryption_context: None,
                    },
                }
            })
            .collect();

        let write = RegionWrite::new(&blocks, buffer)?;

        // We have no job ID, so it makes no sense for accounting.
        region.region_write(write, JobId(0), false).await?;
        offset.advance(nblocks);
    }

    /*
     * As there is no EOF indication in the downstairs, print the
     * number of total blocks we wrote to so the caller can, if they
     * want, use that to extract just this imported file.
     */
    println!(
        "Populated {} extents by copying {} bytes ({} blocks)",
        extents_needed,
        offset.byte_value(),
        offset.value,
    );

    Ok(())
}

/*
 * Debug function to dump the work list.
 */
pub fn show_work(ds: &mut Downstairs) {
    let active_upstairs_connections = ds.active_upstairs();
    info!(
        ds.log,
        "Active Upstairs connections: {:?}", active_upstairs_connections
    );

    for upstairs_connection in active_upstairs_connections {
        let work = ds.work(upstairs_connection);

        let mut kvec: Vec<JobId> = work.active.keys().cloned().collect();

        if kvec.is_empty() {
            info!(ds.log, "Crucible Downstairs work queue:  Empty");
        } else {
            info!(ds.log, "Crucible Downstairs work queue:");
            info!(
                ds.log,
                "{:8} {:>7} {:>5} {}", "  JOB_ID", "IO_TYPE", "STATE", "DEPS"
            );
            kvec.sort_unstable();
            for id in kvec.iter() {
                let dsw = work.active.get(id).unwrap();
                let (dsw_type, dep_list) = match &dsw.work {
                    IOop::Read { dependencies, .. } => ("Read", dependencies),
                    IOop::Write { dependencies, .. } => ("Write", dependencies),
                    IOop::Flush { dependencies, .. } => ("Flush", dependencies),
                    IOop::WriteUnwritten { dependencies, .. } => {
                        ("WriteU", dependencies)
                    }
                    IOop::ExtentClose { dependencies, .. } => {
                        ("EClose", dependencies)
                    }
                    IOop::ExtentFlushClose { dependencies, .. } => {
                        ("EFClose", dependencies)
                    }
                    IOop::ExtentLiveRepair { dependencies, .. } => {
                        ("Repair", dependencies)
                    }
                    IOop::ExtentLiveReopen { dependencies, .. } => {
                        ("ReOpen", dependencies)
                    }
                    IOop::ExtentLiveNoOp { dependencies } => {
                        ("NoOp", dependencies)
                    }
                };
                info!(
                    ds.log,
                    "{:8} {:>7}  {:>5} {:?}", id, dsw_type, dsw.state, dep_list,
                );
            }
        }

        info!(ds.log, "Completed work {:?}", work.completed);
        info!(ds.log, "Last flush: {:?}", work.last_flush);
    }
}

// DTrace probes for the downstairs
#[usdt::provider(provider = "crucible_downstairs")]
pub mod cdt {
    fn submit__read__start(_: u64) {}
    fn submit__writeunwritten__start(_: u64) {}
    fn submit__write__start(_: u64) {}
    fn submit__flush__start(_: u64) {}
    fn submit__el__close__start(_: u64) {}
    fn submit__el__flush__close__start(_: u64) {}
    fn submit__el__repair__start(_: u64) {}
    fn submit__el__reopen__start(_: u64) {}
    fn submit__el__noop__start(_: u64) {}
    fn work__start(_: u64) {}
    fn os__read__start(_: u64) {}
    fn os__writeunwritten__start(_: u64) {}
    fn os__write__start(_: u64) {}
    fn os__flush__start(_: u64) {}
    fn work__process(_: u64) {}
    fn os__read__done(_: u64) {}
    fn os__writeunwritten__done(_: u64) {}
    fn os__write__done(_: u64) {}
    fn os__flush__done(_: u64) {}
    fn submit__read__done(_: u64) {}
    fn submit__writeunwritten__done(_: u64) {}
    fn submit__write__done(_: u64) {}
    fn submit__flush__done(_: u64) {}
    fn extent__flush__start(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__done(job_id: u64, extent_id: u32, extent_size: u64) {}
    fn extent__flush__file__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__file__done(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__collect__hashes__start(
        job_id: u64,
        extent_id: u32,
        num_dirty: u64,
    ) {
    }
    fn extent__flush__collect__hashes__done(
        job_id: u64,
        extent_id: u32,
        num_rehashed: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__flush__sqlite__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__get__hashes__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__get__hashes__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__write__sqlite__insert__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__sqlite__insert__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__write__raw__context__insert__start(
        job_id: u64,
        extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__write__raw__context__insert__done(
        _job_id: u64,
        _extent_id: u32,
        extent_size: u64,
    ) {
    }
    fn extent__read__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__done(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__get__contexts__start(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__get__contexts__done(
        job_id: u64,
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn extent__read__file__start(job_id: u64, extent_id: u32, n_blocks: u64) {}
    fn extent__read__file__done(job_id: u64, extent_id: u32, n_blocks: u64) {}

    fn extent__context__truncate__start(n_deletions: u64) {}
    fn extent__context__truncate__done() {}
    fn extent__set__block__contexts__write__count(
        extent_id: u32,
        n_blocks: u64,
    ) {
    }
    fn submit__el__close__done(_: u64) {}
    fn submit__el__repair__done(_: u64) {}
    fn submit__el__done(_: u64) {}
    fn work__done(_: u64) {}
}

/// Spawns tasks for the given stream
async fn proc_stream(
    handle: DownstairsHandle,
    id: ConnectionId,
    stream: WrappedStream,
    log: &Logger,
) -> Result<()> {
    match stream {
        WrappedStream::Http(sock) => {
            let (read, write) = sock.into_split();

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = MessageWriter::new(write);

            proc(handle, id, fr, fw, log).await
        }
        WrappedStream::Https(stream) => {
            let (read, write) = tokio::io::split(stream);

            let fr = FramedRead::new(read, CrucibleDecoder::new());
            let fw = MessageWriter::new(write);

            proc(handle, id, fr, fw, log).await
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UpstairsConnection {
    upstairs_id: Uuid,
    session_id: Uuid,
    gen: u64,
}

/// Unique ID identifying a single connection
///
/// This value is assigned and incremented every time `TcpListener::accept()`
/// receives a new connection.  Note that it is **not** the upstairs or session
/// UUID, which may collide under certain circumstances.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct ConnectionId(u64);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum NegotiationState {
    ConnectedToUpstairs,
    PromotedToActive,
    SentRegionInfo,
}

/// Immutable data shared by every `ConnectionState`
#[derive(Debug)]
struct ConnectionData {
    /// Repair address, guaranteed to be valid by this point
    repair_addr: SocketAddr,

    /// Token used to cancel the IO tasks
    #[allow(unused)]
    cancel: tokio_util::sync::DropGuard,

    /// IO channel to the reply task
    reply_channel_tx: mpsc::UnboundedSender<Message>,
}

impl ConnectionData {
    /// Returns a dummy connection, for use with `std::mem::replace`
    fn dummy() -> Self {
        Self {
            repair_addr: SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                9001,
            ),
            cancel: tokio_util::sync::CancellationToken::new().drop_guard(),
            reply_channel_tx: mpsc::unbounded_channel().0,
        }
    }
}

/// A single active connection
#[derive(Debug)]
struct ActiveConnection {
    data: ConnectionData,
    upstairs_connection: UpstairsConnection,
    work: Work,
}

impl ActiveConnection {
    fn add_work(&mut self, ds_id: JobId, work: IOop) {
        let dsw = DownstairsWork {
            upstairs_connection: self.upstairs_connection,
            ds_id,
            work,
            state: WorkState::New,
        };
        self.work.add_work(ds_id, dsw);
    }

    /// Checks that the message's upstairs and session ID match this connection
    ///
    /// - If everything matches, returns `Ok(true)`
    /// - If there is a mismatch, sends an error reply to the Upstairs and
    ///   returns `Ok(false)`
    ///   - If sending that message fails, returns an error
    fn is_message_valid(&self, m: &Message) -> Result<bool> {
        // Initial check against upstairs and session ID
        let r = match m {
            Message::Write {
                header:
                    crucible_protocol::WriteHeader {
                        upstairs_id,
                        session_id,
                        ..
                    },
                ..
            }
            | Message::WriteUnwritten {
                header:
                    crucible_protocol::WriteHeader {
                        upstairs_id,
                        session_id,
                        ..
                    },
                ..
            }
            | Message::Flush {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ReadRequest {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ExtentLiveClose {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ExtentLiveFlushClose {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ExtentLiveRepair {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ExtentLiveReopen {
                upstairs_id,
                session_id,
                ..
            }
            | Message::ExtentLiveNoOp {
                upstairs_id,
                session_id,
                ..
            } => {
                if self.upstairs_connection.upstairs_id != *upstairs_id {
                    self.data.reply_channel_tx.send(Message::UuidMismatch {
                        expected_id: self.upstairs_connection.upstairs_id,
                    })?;
                    false
                } else if self.upstairs_connection.session_id != *session_id {
                    self.data.reply_channel_tx.send(Message::UuidMismatch {
                        expected_id: self.upstairs_connection.session_id,
                    })?;
                    false
                } else {
                    true
                }
            }
            _ => true,
        };
        Ok(r)
    }
}

#[derive(Debug)]
enum ConnectionState {
    Open(ConnectionData),
    Negotiating {
        negotiated: NegotiationState,
        upstairs_connection: UpstairsConnection,
        data: ConnectionData,
    },
    Running(ActiveConnection),
}

impl ConnectionState {
    fn data(&self) -> &ConnectionData {
        match self {
            ConnectionState::Open(data) => data,
            ConnectionState::Negotiating { data, .. } => data,
            ConnectionState::Running(a) => &a.data,
        }
    }

    fn reply(
        &self,
        msg: Message,
    ) -> Result<(), mpsc::error::SendError<Message>> {
        self.data().reply_channel_tx.send(msg)
    }

    fn upstairs_connection(&self) -> Option<UpstairsConnection> {
        match self {
            ConnectionState::Open(..) => None,
            ConnectionState::Negotiating {
                upstairs_connection,
                ..
            } => Some(*upstairs_connection),
            ConnectionState::Running(a) => Some(a.upstairs_connection),
        }
    }
}

/// Spawn tasks for this Upstairs connection, then return
async fn proc<RT, WT>(
    handle: DownstairsHandle,
    id: ConnectionId,
    fr: FramedRead<RT, CrucibleDecoder>,
    fw: MessageWriter<WT>,
    log: &Logger,
) -> Result<()>
where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send + 'static,
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    // Create tasks for:
    //     Reading data from the socket (including deserialization)
    //     Sending data out on the socket (including serialization)
    //
    // These tasks are relatively dumb; they simply forward data to and from the
    // `Downstairs::run` loop.
    //
    // Tasks are organized as follows, with tasks in `snake_case`
    //
    //   ┌──────────┐           ┌───────────┐
    //   │FramedRead│           │FramedWrite│
    //   └────┬─────┘           └─────▲─────┘
    //        │                       │
    //  ┌─────▼──────┐       ┌────────┴───────┐
    //  │ recv_task  │       │   reply_task   │
    //  └─────┬──────┘       └────────▲───────┘
    //        │                       │
    //        │downstairs       reply │responses,
    //        │handle          channel│invalid frame
    //        │                       │errors, pings
    //        │                       │
    //        └─────────────────┐     │
    //                          │     │
    //   per-connection state   │     │
    //  ======================= │ === │ ===============================
    //   common state      ┌────▼─────┴──────┐
    //                     │ Downstairs::run │
    //                     └─────────────────┘
    //
    // Task lifetimes are handled automatically; there are a variety of possible
    // orderings.
    //
    // ## Error in `recv_task`
    // - `recv_task` will proactively send `Message::ConnectionClosed` to the
    //   `Downstairs`, then exit
    // - The `Downstairs` will drop the `ConnectionState`, causing `reply_task`
    //   to exit because its `mpsc::Receiver` will read `None`
    //
    // ## Error in `reply_task`
    // - `reply_task` will exit upon error
    // - `recv_task` will notice that `reply_task` has stopped, and will send
    //   `Message::ConnectionClosed` to the `Downstairs`, then exit
    //
    // ## Intentional disconnection by the `Downstairs`
    // - The `Downstairs` drops the `ConnectionState
    // - `reply_task` stops as above when its `Receiver` reads `None`
    // - The `ConnectionState` owns a `DropGuard` and `recv_task` polls the
    //   corresponding `CancellationToken`; when dropped, `recv_task` exits.

    // We rely on backpressure to limit total jobs in flight, so these channels
    // can be unbounded.
    let (reply_channel_tx, reply_channel_rx) = mpsc::unbounded_channel();

    // Populate our state in the Downstairs, keyed by our `id`
    let cancel_io = handle.new_connection(id, reply_channel_tx).await?;

    let reply_task = tokio::spawn(reply_task(reply_channel_rx, fw));
    tokio::spawn(recv_task(
        id,
        fr,
        cancel_io,
        reply_task,
        handle,
        log.new(o!("task" => "recv")),
    ));

    // The tasks are in flight and will exit autonomously per above
    Ok(())
}

/// Receives messages from a channel and sends them to a socket
///
/// Returns `Ok(())` if the channel closes cleanly, or an error if the socket
/// write fails in some way.
async fn reply_task<WT>(
    mut reply_channel_rx: mpsc::UnboundedReceiver<Message>,
    mut fw: MessageWriter<WT>,
) -> Result<()>
where
    WT: tokio::io::AsyncWrite
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    while let Some(m) = reply_channel_rx.recv().await {
        fw.send(m).await?;
    }
    Ok(())
}

/// Receives messages from a socket, deserializes them, and passes them along
///
/// This is the most 'upstream' per-connection task, so it also monitors the
/// cancellation token (owned by the Downstairs) and the reply task (which is a
/// `JoinHandle`).  If either of these indicate termination, then `recv_task`
/// exits, sending `ConnectionClosed` to the `Downstairs` if relevant.
async fn recv_task<RT>(
    id: ConnectionId,
    mut fr: FramedRead<RT, CrucibleDecoder>,
    cancel_io: tokio_util::sync::CancellationToken,
    mut reply_task: tokio::task::JoinHandle<Result<()>>,
    handle: DownstairsHandle,
    log: Logger,
) where
    RT: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
{
    // How long we wait before logging a message that we have not heard from
    // the upstairs.
    const TIMEOUT_SECS: f32 = 15.0;
    // How many timeouts will tolerate before we disconnect from the upstairs.
    const TIMEOUT_LIMIT: usize = 3;

    let send_disconnect = || {
        warn!(log, "recv_task sending ConnectionClosed");
        let m = DownstairsRequest::ConnectionClosed { id };
        if let Err(e) = handle.tx.send(m) {
            warn!(log, "failed sending ConnectionClosed to handle: {e:?}");
        }
    };

    loop {
        tokio::select! {
            /*
             * Don't wait more than TIMEOUT_SECS * TIMEOUT_LIMIT seconds to hear
             * from the other side.
             * XXX Timeouts, timeouts: always wrong!  Some too short and
             * some too long.
             */
            _ = verbose_timeout(TIMEOUT_SECS, TIMEOUT_LIMIT, log.clone()) => {
                warn!(log, "inactivity timeout");
                send_disconnect();
                break;
            }

            // Check whether the reply task has stopped of its own accord
            _ = &mut reply_task => {
                warn!(log, "reply_task stopped");

                // It may be theoretically possible for `reply_task` to stop
                // (when `ConnectionState::reply_channel_tx` is dropped) and
                // then for this `select!` to notice it stops **before**
                // noticing that `cancel_io.cancelled()` is true.
                //
                // If that's the case, it would be bad to tell the `Downstairs`
                // that we've disconnected, because it already knows!
                //
                // However, the cancel token is always dropped _before_
                // `reply_channel_tx` in `ConnectionState`, so we can check
                // that case here:
                if !cancel_io.is_cancelled() {
                    send_disconnect();
                }
                break;
            }

            // Check whether the Downstairs has cancelled us
            _ = cancel_io.cancelled() => {
                // We don't inform the Downstairs that the task has stopped,
                // because the Downstairs -- as the holder of the cancel token
                // guard -- is the one that cancelled us!
                info!(log, "got cancelled token; recv_task stopping");
                break;
            }

            // Normal operation: read messages from the socket, deserialize
            // them, and send them to the Downstairs as a tagged message.
            new_read = fr.next() => {
                match new_read {
                    None => {
                        info!(log, "upstairs disconnected");
                        send_disconnect();
                        break;
                    }
                    Some(Err(e)) => {
                        // XXX "unexpected end of file" can occur if upstairs
                        // terminates, we don't yet have a HangUp message
                        warn!(log, "error reading from FramedReader: {e:?}");
                        send_disconnect();
                        break;
                    }
                    Some(Ok(msg)) => {
                        let m = DownstairsRequest::Message { id, msg };
                        if let Err(e) = handle.tx.send(m) {
                            warn!(
                                log,
                                "failed sending message to Downstairs: {e:?}"
                            );
                            break;
                        }
                    },
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct DownstairsBuilder<'a> {
    data: &'a Path,
    read_only: bool,
    lossy: Option<bool>,
    read_errors: Option<bool>,  // Test flag
    write_errors: Option<bool>, // Test flag
    flush_errors: Option<bool>, // Test flag
    backend: Option<Backend>,
    log: Option<Logger>,
}

impl DownstairsBuilder<'_> {
    pub fn set_lossy(&mut self, lossy: bool) -> &mut Self {
        self.lossy = Some(lossy);
        self
    }
    pub fn set_test_errors(
        &mut self,
        read_errors: bool,
        write_errors: bool,
        flush_errors: bool,
    ) -> &mut Self {
        self.read_errors = Some(read_errors);
        self.write_errors = Some(write_errors);
        self.flush_errors = Some(flush_errors);
        self
    }
    pub fn set_backend(&mut self, backend: Backend) -> &mut Self {
        self.backend = Some(backend);
        self
    }
    pub fn set_logger(&mut self, log: Logger) -> &mut Self {
        self.log = Some(log);
        self
    }

    pub async fn build(&mut self) -> Result<Downstairs> {
        let lossy = self.lossy.unwrap_or(false);
        let read_errors = self.read_errors.unwrap_or(false);
        let write_errors = self.write_errors.unwrap_or(false);
        let flush_errors = self.flush_errors.unwrap_or(false);
        let backend = self.backend.unwrap_or(Backend::RawFile);

        let log = match &self.log {
            Some(log) => log.clone(),
            None => build_logger(),
        };

        // Open the region at the provided location.
        let region = Region::open_with_backend(
            self.data,
            Default::default(),
            true,
            self.read_only,
            backend,
            &log,
        )
        .await?;

        let encrypted = region.encrypted();

        let dss = DsStatOuter {
            ds_stat_wrap: Arc::new(std::sync::Mutex::new(DsCountStat::new(
                region.def().uuid(),
            ))),
        };

        info!(log, "UUID: {:?}", region.def().uuid());
        info!(
            log,
            "Blocks per extent:{} Total Extents: {}",
            region.def().extent_size().value,
            region.def().extent_count(),
        );

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        Ok(Downstairs {
            region,
            flags: DownstairsFlags {
                lossy,
                read_errors,
                write_errors,
                flush_errors,
                read_only: self.read_only,
                encrypted,
            },
            active_upstairs: HashMap::new(),
            connection_state: HashMap::new(),
            dss,
            address: None,
            repair_address: None,
            log,
            request_tx,
            request_rx,
            reqwest_client: reqwest::ClientBuilder::new()
                .connect_timeout(std::time::Duration::from_secs(15))
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap(),
        })
    }
}

/// Configuration flags for the Downstairs
#[derive(Debug)]
struct DownstairsFlags {
    lossy: bool,        // Test flag, enables pauses and skipped jobs
    read_errors: bool,  // Test flag
    write_errors: bool, // Test flag
    flush_errors: bool, // Test flag
    read_only: bool,
    encrypted: bool,
}

/*
 * Overall structure for things the downstairs is tracking.
 * This includes the extents and their status as well as the
 * downstairs work queue.
 */
#[derive(Debug)]
pub struct Downstairs {
    pub region: Region,
    flags: DownstairsFlags,

    /// Map from active Upstairs UUID to `ConnectionId`
    ///
    /// Actual data is stored in `self.connection_state`
    active_upstairs: HashMap<Uuid, ConnectionId>,

    dss: DsStatOuter,
    pub address: Option<SocketAddr>,
    pub repair_address: Option<SocketAddr>,
    log: Logger,

    /// Per-connection state
    connection_state: HashMap<ConnectionId, ConnectionState>,

    request_tx: mpsc::UnboundedSender<DownstairsRequest>,
    request_rx: mpsc::UnboundedReceiver<DownstairsRequest>,

    // A reqwest client, to be reused when creating progenitor clients
    pub reqwest_client: reqwest::Client,
}

#[allow(clippy::too_many_arguments)]
impl Downstairs {
    pub fn new_builder(data: &Path, read_only: bool) -> DownstairsBuilder {
        DownstairsBuilder {
            data,
            read_only,
            lossy: Some(false),
            read_errors: Some(false),
            write_errors: Some(false),
            flush_errors: Some(false),
            backend: Some(Backend::default()),
            log: None,
        }
    }

    /// Mutably borrows a particular `ActiveConnection`
    ///
    /// # Panics
    /// If the connection is not present or is not running
    fn active_mut(&mut self, conn_id: ConnectionId) -> &mut ActiveConnection {
        let Some(ConnectionState::Running(a)) =
            self.connection_state.get_mut(&conn_id)
        else {
            panic!("could not get work from connection {conn_id:?}");
        };
        a
    }

    /// Immutably borrows a particular `ActiveConnection`
    ///
    /// # Panics
    /// If the connection is not present or is not running
    fn active(&self, conn_id: ConnectionId) -> &ActiveConnection {
        let Some(ConnectionState::Running(a)) =
            self.connection_state.get(&conn_id)
        else {
            panic!("could not get work from connection {conn_id:?}");
        };
        a
    }

    /// Mutably borrows a connection's `Work`
    ///
    /// # Panics
    /// If the connection is not present or is not running
    fn work_mut(&mut self, conn_id: ConnectionId) -> &mut Work {
        &mut self.active_mut(conn_id).work
    }

    /// Borrow a connection's `Work`
    ///
    /// # Panics
    /// If the connection is not present or is not running
    fn work(&self, conn_id: ConnectionId) -> &Work {
        &self.active(conn_id).work
    }

    /// Given a job ID (and its associated connection), do the work for that IO
    ///
    /// Take a IOop type and (after some error checking), do the work required
    /// for that IOop, storing the result. On completion, construct the
    /// corresponding Crucible Message containing the response to it.  The
    /// caller is responsible for sending that response back to the upstairs.
    ///
    /// This function is nominally infallible, because any errors are encoded as
    /// an error field in the returned `Message`.
    async fn do_work(
        &mut self,
        conn_id: ConnectionId,
        job_id: JobId,
    ) -> Message {
        let job = self.work_mut(conn_id).get_ready_job(job_id);

        assert_eq!(job.ds_id, job_id);
        assert!(self.is_active(job.upstairs_connection));

        match job.work {
            IOop::Read {
                dependencies,
                requests,
            } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let response = if self.flags.read_errors && random() && random()
                {
                    warn!(self.log, "returning error on read!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    // This clone shouldn't be too expensive, since it's only
                    // 32 bytes per extent (and should usually only be 1 extent)
                    self.region.region_read(&requests, job_id).await
                };
                debug!(
                    self.log,
                    "Read      :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    response.is_ok(),
                );

                // We've got one or more block contexts per block returned, but
                // the response format need to be in the form of
                // `ReadResponseBlockMetadata` (which also contains extent and
                // offset); we inject that extra information in this terrible
                // match statement.
                let (blocks, data) = match response {
                    Ok(r) => (
                        Ok(requests
                            .iter()
                            .flat_map(|req| {
                                // Iterate over blocks within this extent.  By
                                // construction, each `ReadRequest` will never
                                // overstep the bounds of the extent.
                                (0..req.count.get()).map(|i| {
                                    (
                                        req.extent,
                                        Block {
                                            value: req.offset.value + i as u64,
                                            shift: req.offset.shift,
                                        },
                                    )
                                })
                            })
                            .zip(r.blocks)
                            .map(|((eid, offset), block_contexts)| {
                                crucible_protocol::ReadResponseBlockMetadata {
                                    eid,
                                    offset,
                                    block_contexts,
                                }
                            })
                            .collect()),
                        r.data,
                    ),
                    Err(e) => (Err(e), Default::default()),
                };

                Message::ReadResponse {
                    header: crucible_protocol::ReadResponseHeader {
                        upstairs_id: job.upstairs_connection.upstairs_id,
                        session_id: job.upstairs_connection.session_id,
                        job_id,
                        blocks,
                    },
                    data,
                }
            }
            IOop::WriteUnwritten { writes, .. } => {
                /*
                 * Any error from an IO should be intercepted here and passed
                 * back to the upstairs.
                 */
                let result = if self.flags.write_errors && random() && random()
                {
                    warn!(self.log, "returning error on writeunwritten!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    // The region_write will handle what happens to each block
                    // based on if they have data or not.
                    self.region.region_write(writes, job_id, true).await
                };

                Message::WriteUnwrittenAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::Write {
                writes,
                dependencies,
            } => {
                let result = if self.flags.write_errors && random() && random()
                {
                    warn!(self.log, "returning error on write!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    self.region.region_write(writes, job_id, false).await
                };
                debug!(
                    self.log,
                    "Write     :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );

                Message::WriteAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::Flush {
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
            } => {
                let result = if self.flags.flush_errors && random() && random()
                {
                    warn!(self.log, "returning error on flush!");
                    Err(CrucibleError::GenericError("test error".to_string()))
                } else {
                    self.region
                        .region_flush(
                            flush_number,
                            gen_number,
                            &snapshot_details,
                            job_id,
                            extent_limit,
                        )
                        .await
                };
                debug!(
                    self.log,
                    "Flush     :{} extent_limit {:?} deps:{:?} res:{} f:{} g:{}",
                    job_id, extent_limit, dependencies, result.is_ok(),
                    flush_number, gen_number,
                );

                Message::FlushAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentClose {
                dependencies,
                extent,
            } => {
                let result = self.region.close_extent(extent).await;
                debug!(
                    self.log,
                    "JustClose :{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Message::ExtentLiveCloseAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }
            }
            IOop::ExtentFlushClose {
                dependencies,
                extent,
                flush_number,
                gen_number,
            } => {
                // If flush fails, return that result.
                // Else, if close fails, return that result.
                // Else, return the f/g/d from the close.
                let result = match self
                    .region
                    .region_flush_extent(
                        extent,
                        gen_number,
                        flush_number,
                        job_id,
                    )
                    .await
                {
                    Err(f_res) => Err(f_res),
                    Ok(_) => self.region.close_extent(extent).await,
                };

                debug!(
                    self.log,
                    "FlushClose:{} extent {} deps:{:?} res:{} f:{} g:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                    flush_number,
                    gen_number,
                );

                Message::ExtentLiveCloseAck {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id: job.ds_id,
                    result,
                }
            }
            IOop::ExtentLiveRepair {
                dependencies,
                extent,
                source_repair_address,
            } => {
                debug!(
                    self.log,
                    "ExtentLiveRepair: extent {} sra:{:?}",
                    extent,
                    source_repair_address
                );
                let result = self
                    .region
                    .repair_extent(
                        self.reqwest_client.clone(),
                        extent,
                        source_repair_address,
                        false,
                    )
                    .await;
                debug!(
                    self.log,
                    "LiveRepair:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );

                Message::ExtentLiveRepairAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentLiveReopen {
                dependencies,
                extent,
            } => {
                let result = self.region.reopen_extent(extent).await;
                debug!(
                    self.log,
                    "LiveReopen:{} extent {} deps:{:?} res:{}",
                    job_id,
                    extent,
                    dependencies,
                    result.is_ok(),
                );
                Message::ExtentLiveAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
            IOop::ExtentLiveNoOp { dependencies } => {
                debug!(self.log, "Work of: LiveNoOp {}", job_id);
                let result = Ok(());
                debug!(
                    self.log,
                    "LiveNoOp  :{} deps:{:?} res:{}",
                    job_id,
                    dependencies,
                    result.is_ok(),
                );
                Message::ExtentLiveAckId {
                    upstairs_id: job.upstairs_connection.upstairs_id,
                    session_id: job.upstairs_connection.session_id,
                    job_id,
                    result,
                }
            }
        }
    }

    /// Helper function to call `complete_work` if the `Message` is available
    #[cfg(test)]
    fn complete_work(
        &mut self,
        conn_id: ConnectionId,
        ds_id: JobId,
        m: Message,
    ) {
        let is_flush = matches!(m, Message::FlushAck { .. });
        self.complete_work_inner(conn_id, ds_id, is_flush)
    }

    /// Completes the given job
    ///
    /// This is a three-step process:
    /// - removing the job from active
    /// - removing the response
    /// - putting the id on the completed list.
    ///
    /// # Panics
    /// If the given connection isn't active, or the job isn't active
    fn complete_work_inner(
        &mut self,
        conn_id: ConnectionId,
        ds_id: JobId,
        is_flush: bool,
    ) {
        let work = self.work_mut(conn_id);
        let prev = work.active.remove(&ds_id);
        assert!(prev.is_some());

        if is_flush {
            work.last_flush = ds_id;
            work.completed = Vec::with_capacity(32);
        } else {
            work.completed.push(ds_id);
        }
    }

    /*
     * After we complete a read/write/flush on a region, update the
     * Oximeter counter for the operation.
     */
    fn complete_work_stat(&mut self, m: &Message, ds_id: JobId) {
        // XXX dss per upstairs connection?
        match m {
            Message::FlushAck { .. } => {
                cdt::submit__flush__done!(|| ds_id.0);
                self.dss.add_flush();
            }
            Message::WriteAck { .. } => {
                cdt::submit__write__done!(|| ds_id.0);
                self.dss.add_write();
            }
            Message::WriteUnwrittenAck { .. } => {
                cdt::submit__writeunwritten__done!(|| ds_id.0);
                self.dss.add_write();
            }
            Message::ReadResponse { .. } => {
                cdt::submit__read__done!(|| ds_id.0);
                self.dss.add_read();
            }
            Message::ExtentLiveCloseAck { .. } => {
                cdt::submit__el__close__done!(|| ds_id.0);
            }
            Message::ExtentLiveRepairAckId { .. } => {
                cdt::submit__el__repair__done!(|| ds_id.0);
            }
            Message::ExtentLiveAckId { .. } => {
                cdt::submit__el__done!(|| ds_id.0);
            }
            _ => (),
        }
    }

    /// Adds a new connection, then sets it to `ConnectionState::Running`
    ///
    /// This skips autonegotiation for ease of unit testing
    #[cfg(test)]
    fn add_fake_connection(
        &mut self,
        upstairs_connection: UpstairsConnection,
        conn_id: ConnectionId,
    ) -> tokio_util::sync::CancellationToken {
        let reply_channel_tx = mpsc::unbounded_channel().0;
        let cancel = self.new_connection(conn_id, reply_channel_tx);

        let prev = self.connection_state.remove(&conn_id);
        let Some(ConnectionState::Open(data)) = prev else {
            panic!();
        };
        self.connection_state.insert(
            conn_id,
            ConnectionState::Running(ActiveConnection {
                data,
                upstairs_connection,
                work: Work::new(),
            }),
        );
        cancel
    }

    async fn promote_to_active(
        &mut self,
        upstairs_connection: UpstairsConnection,
        conn_id: ConnectionId,
    ) -> Result<()> {
        assert!(self.connection_state.contains_key(&conn_id));
        if self.flags.read_only {
            // Multiple active read-only sessions are allowed, but multiple
            // sessions for the same Upstairs UUID are not. Kick out a
            // previously active session for this UUID if one exists. This
            // function is called on a `&mut self`, so we're guaranteed that the
            // isn't adding more work.
            if let Some(active_id) = self
                .active_upstairs
                .remove(&upstairs_connection.upstairs_id)
            {
                self.on_new_connection_replacing(
                    active_id,
                    upstairs_connection,
                );
            } else {
                // There is no current session for this Upstairs UUID.
            }

            // Insert the new session.  There cannot be an entry with the same
            // UUID, because we removed it above.
            let prev = self
                .active_upstairs
                .insert(upstairs_connection.upstairs_id, conn_id);
            assert!(prev.is_none());

            Ok(())
        } else {
            // Only one active read-write session is allowed. Kick out the
            // currently active Upstairs session if one exists.
            let currently_active_upstairs_uuids: Vec<Uuid> =
                self.active_upstairs.keys().copied().collect();

            match currently_active_upstairs_uuids.len() {
                0 => {
                    // No currently active Upstairs sessions
                    self.active_upstairs
                        .insert(upstairs_connection.upstairs_id, conn_id);

                    assert_eq!(self.active_upstairs.len(), 1);

                    // Re-open any closed extents
                    self.region.reopen_all_extents().await?;

                    info!(
                        self.log,
                        "{:?} is now active (read-write)", upstairs_connection,
                    );

                    Ok(())
                }

                1 => {
                    // There is an existing session.  Determine if this new
                    // request to promote to active should move forward or
                    // be blocked.
                    let active_id = self
                        .active_upstairs
                        .get(&currently_active_upstairs_uuids[0])
                        .unwrap();

                    let active_upstairs_connection = self.connection_state
                        [&active_id]
                        .upstairs_connection()
                        .expect("bad ConnectionId for active connection");
                    warn!(
                        self.log,
                        "Attempting RW takeover from {:?} to {:?}",
                        active_upstairs_connection,
                        upstairs_connection,
                    );

                    // Compare the new generaion number to what the existing
                    // connection is and take action based on that.
                    match upstairs_connection
                        .gen
                        .cmp(&active_upstairs_connection.gen)
                    {
                        Ordering::Less => {
                            // If the new connection has a lower generation
                            // number than the current connection, we don't
                            // allow it to take over.
                            bail!(
                                "Current gen {} is > requested gen of {}",
                                active_upstairs_connection.gen,
                                upstairs_connection.gen,
                            );
                        }
                        Ordering::Equal => {
                            // The generation numbers match, the only way we
                            // allow this new connection to take over is if the
                            // upstairs_id and the session_id are the same,
                            // which means the whole structures need to be
                            // identical.
                            if active_upstairs_connection != upstairs_connection
                            {
                                bail!(
                                    "Same gen, but UUIDs {:?} don't match {:?}",
                                    active_upstairs_connection,
                                    upstairs_connection,
                                );
                            }
                        }
                        // The only remaining case is the new generation
                        // number is higher than the existing.
                        Ordering::Greater => {}
                    }

                    // Now that we know we can remove/replace it, go ahead
                    // and take it off the list.
                    let active_id = self
                        .active_upstairs
                        .remove(&currently_active_upstairs_uuids[0])
                        .unwrap();

                    warn!(
                        self.log,
                        "Signaling to {:?} thread that {:?} is being \
                        promoted (read-write)",
                        active_upstairs_connection,
                        upstairs_connection,
                    );

                    self.on_new_connection_replacing(
                        active_id,
                        upstairs_connection,
                    );

                    // Insert the new session
                    let prev = self
                        .active_upstairs
                        .insert(upstairs_connection.upstairs_id, conn_id);
                    assert!(prev.is_none());

                    assert_eq!(self.active_upstairs.len(), 1);

                    // Re-open any closed extents
                    self.region.reopen_all_extents().await?;

                    info!(
                        self.log,
                        "{:?} is now active (read-write)", upstairs_connection,
                    );

                    Ok(())
                }

                _ => {
                    // Panic - we shouldn't be running with more than one
                    // active read-write Upstairs
                    panic!(
                        "More than one currently active upstairs! {:?}",
                        currently_active_upstairs_uuids,
                    );
                }
            }
        }
    }

    fn is_active(&self, connection: UpstairsConnection) -> bool {
        let uuid = connection.upstairs_id;
        if let Some(id) = self.active_upstairs.get(&uuid) {
            self.connection_state[&id].upstairs_connection() == Some(connection)
        } else {
            false
        }
    }

    fn active_upstairs(&self) -> Vec<ConnectionId> {
        self.active_upstairs.values().cloned().collect()
    }

    /// Handle a new message from the upstairs
    ///
    /// If the message is an IO, then put the new IO the work hashmap. If the
    /// message is a repair message, then we handle it right here.
    ///
    /// # Panics
    /// If the given `ConnectionId` does not represent a running connection
    async fn proc_frame(
        &mut self,
        m: Message,
        id: ConnectionId,
    ) -> Result<Option<JobId>> {
        let state = self.active_mut(id);
        if !state.is_message_valid(&m)? {
            return Ok(None);
        }

        // The Upstairs will send Flushes periodically, even in read only mode
        // we have to accept them. But read-only should never accept writes!
        if self.flags.read_only {
            let is_write = matches!(
                m,
                Message::Write { .. }
                    | Message::WriteUnwritten { .. }
                    | Message::ExtentClose { .. }
                    | Message::ExtentLiveFlushClose { .. }
                    | Message::ExtentLiveRepair { .. }
                    | Message::ExtentLiveReopen { .. }
                    | Message::ExtentLiveNoOp { .. }
            );

            if is_write {
                error!(self.log, "read-only but received write {:?}", m);
                bail!(CrucibleError::ModifyingReadOnlyRegion);
            }
        }

        // Reborrow
        let state = self.active_mut(id);
        let r = match m {
            Message::Write { header, data } => {
                cdt::submit__write__start!(|| header.job_id.0);
                let writes = RegionWrite::new(&header.blocks, data)?;

                let new_write = IOop::Write {
                    dependencies: header.dependencies,
                    writes,
                };

                state.add_work(header.job_id, new_write);
                Some(header.job_id)
            }
            Message::Flush {
                job_id,
                dependencies,
                flush_number,
                gen_number,
                snapshot_details,
                extent_limit,
                ..
            } => {
                cdt::submit__flush__start!(|| job_id.0);

                let new_flush = IOop::Flush {
                    dependencies,
                    flush_number,
                    gen_number,
                    snapshot_details,
                    extent_limit,
                };

                state.add_work(job_id, new_flush);
                Some(job_id)
            }
            Message::WriteUnwritten { header, data } => {
                cdt::submit__writeunwritten__start!(|| header.job_id.0);
                let writes = RegionWrite::new(&header.blocks, data)?;

                let new_write = IOop::WriteUnwritten {
                    dependencies: header.dependencies,
                    writes,
                };

                state.add_work(header.job_id, new_write);
                Some(header.job_id)
            }
            Message::ReadRequest {
                job_id,
                dependencies,
                requests,
                ..
            } => {
                cdt::submit__read__start!(|| job_id.0);

                let new_read = IOop::Read {
                    dependencies,
                    requests: RegionReadRequest::new(&requests),
                };

                state.add_work(job_id, new_read);
                Some(job_id)
            }
            // These are for repair while taking live IO
            Message::ExtentLiveClose {
                job_id,
                dependencies,
                extent_id,
                ..
            } => {
                cdt::submit__el__close__start!(|| job_id.0);
                // TODO: Add dtrace probes
                let ext_close = IOop::ExtentClose {
                    dependencies,
                    extent: extent_id,
                };

                state.add_work(job_id, ext_close);
                Some(job_id)
            }
            Message::ExtentLiveFlushClose {
                job_id,
                dependencies,
                extent_id,
                flush_number,
                gen_number,
                ..
            } => {
                cdt::submit__el__flush__close__start!(|| job_id.0);
                // Do both the flush, and then the close
                let new_flush = IOop::ExtentFlushClose {
                    dependencies,
                    extent: extent_id,
                    flush_number,
                    gen_number,
                };

                state.add_work(job_id, new_flush);
                Some(job_id)
            }
            Message::ExtentLiveRepair {
                job_id,
                dependencies,
                extent_id,
                source_repair_address,
                ..
            } => {
                cdt::submit__el__repair__start!(|| job_id.0);
                // Do both the flush, and then the close
                let new_repair = IOop::ExtentLiveRepair {
                    dependencies,
                    extent: extent_id,

                    source_repair_address,
                };

                state.add_work(job_id, new_repair);
                debug!(self.log, "Received ExtentLiveRepair {}", job_id);
                Some(job_id)
            }
            Message::ExtentLiveReopen {
                job_id,
                dependencies,
                extent_id,
                ..
            } => {
                cdt::submit__el__reopen__start!(|| job_id.0);
                let new_open = IOop::ExtentLiveReopen {
                    dependencies,
                    extent: extent_id,
                };

                state.add_work(job_id, new_open);
                Some(job_id)
            }
            Message::ExtentLiveNoOp {
                job_id,
                dependencies,
                ..
            } => {
                cdt::submit__el__noop__start!(|| job_id.0);
                let new_open = IOop::ExtentLiveNoOp { dependencies };

                state.add_work(job_id, new_open);
                debug!(self.log, "Received NoOP {}", job_id);
                Some(job_id)
            }

            // These messages arrive during initial reconciliation.
            Message::ExtentFlush {
                repair_id,
                extent_id,
                client_id: _,
                flush_number,
                gen_number,
            } => {
                let msg = {
                    debug!(
                        self.log,
                        "{} Flush extent {} with f:{} g:{}",
                        repair_id,
                        extent_id,
                        flush_number,
                        gen_number
                    );

                    match self
                        .region
                        .region_flush_extent(
                            extent_id,
                            gen_number,
                            flush_number,
                            repair_id,
                        )
                        .await
                    {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                self.connection_state[&id].reply(msg)?;
                None
            }
            Message::ExtentClose {
                repair_id,
                extent_id,
            } => {
                let msg = {
                    debug!(
                        self.log,
                        "{} Close extent {}", repair_id, extent_id
                    );
                    match self.region.close_extent(extent_id).await {
                        Ok(_) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                self.connection_state[&id].reply(msg)?;
                None
            }
            Message::ExtentRepair {
                repair_id,
                extent_id,
                source_client_id,
                source_repair_address,
                dest_clients,
            } => {
                let msg = {
                    debug!(
                        self.log,
                        "{} Repair extent {} source:[{}] {:?} dest:{:?}",
                        repair_id,
                        extent_id,
                        source_client_id,
                        source_repair_address,
                        dest_clients
                    );
                    match self
                        .region
                        .repair_extent(
                            self.reqwest_client.clone(),
                            extent_id,
                            source_repair_address,
                            false,
                        )
                        .await
                    {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                self.connection_state[&id].reply(msg)?;
                None
            }
            Message::ExtentReopen {
                repair_id,
                extent_id,
            } => {
                let msg = {
                    debug!(
                        self.log,
                        "{} Reopen extent {}", repair_id, extent_id
                    );
                    match self.region.reopen_extent(extent_id).await {
                        Ok(()) => Message::RepairAckId { repair_id },
                        Err(error) => Message::ExtentError {
                            repair_id,
                            extent_id,
                            error,
                        },
                    }
                };
                self.connection_state[&id].reply(msg)?;
                None
            }
            Message::Ruok => {
                self.connection_state[&id].reply(Message::Imok)?;
                None
            }
            x => bail!("unexpected frame {:?}", x),
        };
        Ok(r)
    }

    fn in_progress(
        &mut self,
        conn_id: ConnectionId,
        ds_id: JobId,
    ) -> Option<JobId> {
        // TODO: don't make a new log every time?
        let log = self.log.new(o!("role" => "work".to_string()));
        self.work_mut(conn_id).in_progress(ds_id, log.clone())
    }

    async fn do_work_for(&mut self, conn_id: ConnectionId) -> Result<()> {
        let state = self.active_mut(conn_id);
        let upstairs_connection = state.upstairs_connection;
        assert!(self.is_active(upstairs_connection)); // checked above

        // Reborrow!
        let state = self.active_mut(conn_id);

        /*
         * Build ourselves a list of all the jobs on the work hashmap that
         * are New or DepWait.
         */
        let mut new_work: VecDeque<JobId> =
            state.work.new_work().into_iter().collect();
        // TODO: return a VecDeque directly?
        let is_lossy = self.flags.lossy;

        /*
         * We don't have to do jobs in order, but the dependencies are, at
         * least for now, always going to be in order of job id. `new_work` is
         * sorted before it is returned so this function iterates through jobs
         * in order.
         */
        while let Some(new_id) = new_work.pop_front() {
            if is_lossy && random() && random() {
                // Skip a job that needs to be done, moving it to the back of
                // the list.  This exercises job dependency tracking in the face
                // of arbitrary reordering.
                info!(self.log, "[lossy] skipping {}", new_id);
                new_work.push_back(new_id);
                continue;
            }

            /*
             * If this job is still new, take it and go to work. The
             * in_progress method will only return a job if all
             * dependencies are met.
             */
            let Some(job_id) = self.in_progress(conn_id, new_id) else {
                continue;
            };

            cdt::work__process!(|| job_id.0);
            let m = self.do_work(conn_id, job_id).await;

            if let Some(error) = m.err() {
                self.connection_state[&conn_id].reply(
                    Message::ErrorReport {
                        upstairs_id: upstairs_connection.upstairs_id,
                        session_id: upstairs_connection.session_id,
                        job_id: new_id,
                        error: error.clone(),
                    },
                )?;

                // If the job errored, do not consider it completed.
                // Retry it.
                new_work.push_back(new_id);

                // If this is a repair job, and that repair failed, we
                // can do no more work on this downstairs and should
                // force everything to come down before more work arrives.
                //
                // We have replied to the Upstairs above, which lets the
                // upstairs take action to abort the repair and continue
                // working in some degraded state.
                //
                // If you change this, change how the Upstairs processes
                // ErrorReports!
                if matches!(m, Message::ExtentLiveRepairAckId { .. }) {
                    bail!("Repair has failed, exiting task");
                }
            } else {
                // The job completed successfully, so inform the
                // Upstairs
                self.complete_work_stat(&m, job_id);

                // Notify the upstairs before completing work, which
                // consumes the message (so we'll check whether it's
                // a FlushAck beforehand)
                let is_flush = matches!(m, Message::FlushAck { .. });
                self.connection_state[&conn_id].reply(m)?;
                self.complete_work_inner(conn_id, job_id, is_flush);

                cdt::work__done!(|| job_id.0);
            }
        }
        Ok(())
    }

    /// Returns a handle for making async calls against the Downstairs task
    pub fn handle(&self) -> DownstairsHandle {
        DownstairsHandle {
            tx: self.request_tx.clone(),
        }
    }

    /// Clone the extent files in a region from another running downstairs.
    ///
    /// Use the reconcile/repair extent methods to copy another downstairs.
    /// The source downstairs must have the same RegionDefinition as we do,
    /// and both downstairs must be running in read only mode.
    pub async fn clone_region(&mut self, source: SocketAddr) -> Result<()> {
        let info = crucible_common::BuildInfo::default();
        let log = self.log.new(o!("task" => "clone".to_string()));
        info!(log, "Crucible Version: {}", info);
        info!(
            log,
            "Upstairs <-> Downstairs Message Version: {}",
            CRUCIBLE_MESSAGE_VERSION
        );

        info!(log, "Connecting to {source} to obtain our extent files.");

        let url = format!("http://{:?}", source);
        let repair_server =
            Client::new_with_client(&url, self.reqwest_client.clone());

        let source_def = match repair_server.get_region_info().await {
            Ok(def) => def.into_inner(),
            Err(e) => {
                bail!("Failed to get source region definition: {e}");
            }
        };

        info!(log, "The source RegionDefinition is: {:?}", source_def);

        let source_ro_mode = match repair_server.get_region_mode().await {
            Ok(ro) => ro.into_inner(),
            Err(e) => {
                bail!("Failed to get source mode: {e}");
            }
        };

        info!(log, "The source mode is: {:?}", source_ro_mode);
        if !source_ro_mode {
            bail!("Source downstairs is not read only");
        }

        let my_def = self.region.def();
        info!(log, "my def is {:?}", my_def);

        if let Err(e) = my_def.compatible(source_def) {
            bail!("Incompatible region definitions: {e}");
        }

        if let Err(e) = self.region.close_all_extents().await {
            bail!("Failed to close all extents: {e}");
        }

        for eid in (0..my_def.extent_count()).map(ExtentId) {
            info!(log, "Repair extent {eid}");

            if let Err(e) = self
                .region
                .repair_extent(self.reqwest_client.clone(), eid, source, true)
                .await
            {
                bail!("repair extent {eid} returned: {e}");
            }
        }
        info!(log, "Region has been cloned");

        Ok(())
    }

    fn spawn_runner(ds: Downstairs) -> tokio::task::JoinHandle<()> {
        tokio::spawn(ds.run())
    }

    /// The main Downstairs function!
    ///
    /// This function receives and handles requests from various other tasks,
    /// most notable per-connection IO worker tasks.
    async fn run(mut self) {
        let log = self.log.new(o!("task" => "runner".to_string()));
        while let Some(v) = self.request_rx.recv().await {
            match v {
                DownstairsRequest::ShowWork => {
                    show_work(&mut self);
                }
                DownstairsRequest::IsExtentClosed { eid, done } => {
                    let closed = matches!(
                        self.region.extents[eid.0 as usize],
                        ExtentState::Closed
                    );
                    if done.send(closed).is_err() {
                        warn!(log, "failed to reply to IsExtentClosed");
                    }
                }
                DownstairsRequest::NewConnection {
                    id,
                    reply_channel_tx,
                    done,
                } => {
                    let out = self.new_connection(id, reply_channel_tx);
                    if done.send(out).is_err() {
                        warn!(log, "failed to reply to NewConnection");
                    }
                }
                DownstairsRequest::Message { id, msg } => {
                    self.on_message_for(id, msg).await;
                }
                DownstairsRequest::ConnectionClosed { id } => {
                    // Upstairs disconnected, so discard our local state
                    info!(self.log, "connection closed; disconnection");
                    self.remove_connection(id);
                }
            }
        }
    }

    /// See the comment in the `continue_negotiation()` function (on the
    /// upstairs side) that describes how this negotiation takes place.
    ///
    /// The final step in negotiation (as dictated by the upstairs) is
    /// either LastFlush, or ExtentVersionsPlease.  Once we respond to
    /// that message, we can move forward and start receiving IO from
    /// the upstairs.
    async fn on_negotiation_step(
        &mut self,
        m: Message,
        conn_id: ConnectionId,
    ) -> Result<()> {
        let state = self.connection_state.get_mut(&conn_id).unwrap();
        match m {
            Message::Ruok => {
                if let Err(e) = state.reply(Message::Imok) {
                    bail!("Failed to answer ping: {}", e);
                }
            }
            Message::HereIAm {
                version,
                upstairs_id,
                session_id,
                gen,
                read_only,
                encrypted,
                alternate_versions,
            } => {
                let ConnectionState::Open(data) = state else {
                    bail!("Received connect out of order",);
                };
                info!(
                    self.log,
                    "Connection request from {} with version {}",
                    upstairs_id,
                    version
                );

                // Verify we can communicate with the upstairs.  First
                // check our message version.  If that fails,  check
                // to see if our version is one of the supported
                // versions the upstairs has told us it can support.
                if version != CRUCIBLE_MESSAGE_VERSION {
                    if alternate_versions.contains(&CRUCIBLE_MESSAGE_VERSION) {
                        warn!(
                            self.log,
                            "downstairs and upstairs using different \
                             but compatible versions, Upstairs is {}, \
                             but supports {:?}, downstairs is {}",
                            version,
                            alternate_versions,
                            CRUCIBLE_MESSAGE_VERSION,
                        );
                    } else {
                        let m = Message::VersionMismatch {
                            version: CRUCIBLE_MESSAGE_VERSION,
                        };
                        if let Err(e) = state.reply(m) {
                            warn!(
                                self.log,
                                "Failed to send VersionMismatch: {}", e
                            );
                        }
                        bail!(
                            "Required version {}, Or {:?} got {}",
                            CRUCIBLE_MESSAGE_VERSION,
                            alternate_versions,
                            version,
                        );
                    }
                }

                // Reject an Upstairs negotiation if there is a mismatch
                // of expectation, and terminate the connection - the
                // Upstairs will not be able to successfully negotiate.
                {
                    if self.flags.read_only != read_only {
                        if let Err(e) = state.reply(Message::ReadOnlyMismatch {
                            expected: self.flags.read_only,
                        }) {
                            warn!(
                                self.log,
                                "Failed to send ReadOnlyMismatch: {}", e
                            );
                        }

                        bail!("closing connection due to read-only mismatch");
                    }

                    if self.flags.encrypted != encrypted {
                        if let Err(e) =
                            state.reply(Message::EncryptedMismatch {
                                expected: self.flags.encrypted,
                            })
                        {
                            warn!(
                                self.log,
                                "Failed to send EncryptedMismatch: {}", e
                            );
                        }

                        bail!("closing connection due to encryption mismatch");
                    }
                }

                let upstairs_connection = UpstairsConnection {
                    upstairs_id,
                    session_id,
                    gen,
                };

                // Steal data from the connection state
                let data = std::mem::replace(data, ConnectionData::dummy());
                *state = ConnectionState::Negotiating {
                    negotiated: NegotiationState::ConnectedToUpstairs,
                    upstairs_connection,
                    data,
                };
                info!(
                    self.log,
                    "upstairs {:?} connected, version {}",
                    upstairs_connection,
                    CRUCIBLE_MESSAGE_VERSION
                );

                if let Err(e) = state.reply(Message::YesItsMe {
                    version: CRUCIBLE_MESSAGE_VERSION,
                    repair_addr: state.data().repair_addr,
                }) {
                    bail!("Failed sending YesItsMe: {}", e);
                }
            }
            Message::PromoteToActive {
                upstairs_id,
                session_id,
                gen,
            } => {
                let ConnectionState::Negotiating {
                    negotiated,
                    upstairs_connection,
                    ..
                } = state
                else {
                    bail!("Received activate while not negotiating");
                };
                if *negotiated != NegotiationState::ConnectedToUpstairs {
                    bail!("Received activate out of order {:?}", negotiated);
                }

                // Only allowed to promote or demote self
                let upstairs_connection = *upstairs_connection;
                let matches_self = upstairs_connection.upstairs_id
                    == upstairs_id
                    && upstairs_connection.session_id == session_id;

                if !matches_self {
                    if let Err(e) = state.reply(Message::UuidMismatch {
                        expected_id: upstairs_connection.upstairs_id,
                    }) {
                        warn!(self.log, "Failed sending UuidMismatch: {}", e);
                    }
                    bail!(
                        "Upstairs connection expected \
                         upstairs_id:{} session_id:{}  received \
                         upstairs_id:{} session_id:{}",
                        upstairs_connection.upstairs_id,
                        upstairs_connection.session_id,
                        upstairs_id,
                        session_id
                    );
                } else {
                    if upstairs_connection.gen != gen {
                        warn!(
                            self.log,
                            "warning: generation number at negotiation was {} \
                             and {} at activation, updating",
                            upstairs_connection.gen,
                            gen,
                        );

                        // Reborrow to update `upstairs_connection.gen`
                        let ConnectionState::Negotiating {
                            upstairs_connection,
                            ..
                        } = state
                        else {
                            unreachable!()
                        };
                        upstairs_connection.gen = gen;
                    }

                    self.promote_to_active(upstairs_connection, conn_id)
                        .await?;

                    // reborrow our local state
                    let state =
                        self.connection_state.get_mut(&conn_id).unwrap();

                    let ConnectionState::Negotiating {
                        negotiated,
                        upstairs_connection,
                        ..
                    } = state
                    else {
                        unreachable!();
                    };
                    *negotiated = NegotiationState::PromotedToActive;
                    upstairs_connection.gen = gen;

                    if let Err(e) = state.reply(Message::YouAreNowActive {
                        upstairs_id,
                        session_id,
                        gen,
                    }) {
                        bail!("Failed sending YouAreNewActive: {}", e);
                    }
                }
            }
            Message::RegionInfoPlease => {
                let ConnectionState::Negotiating { negotiated, .. } = state
                else {
                    bail!("Received RegionInfo while not negotiating");
                };
                if *negotiated != NegotiationState::PromotedToActive {
                    bail!("Received RegionInfo out of order {:?}", negotiated);
                }

                *negotiated = NegotiationState::SentRegionInfo;
                let region_def = self.region.def();
                if let Err(e) = state.reply(Message::RegionInfo { region_def })
                {
                    bail!("Failed sending RegionInfo: {}", e);
                }
            }
            Message::LastFlush { last_flush_number } => {
                let ConnectionState::Negotiating {
                    negotiated,
                    upstairs_connection,
                    data,
                } = state
                else {
                    bail!("Received LastFlush while not negotiating");
                };
                if *negotiated != NegotiationState::SentRegionInfo {
                    bail!("Received LastFlush out of order {:?}", negotiated);
                }

                let data = std::mem::replace(data, ConnectionData::dummy());
                let upstairs_connection = *upstairs_connection;
                *state = ConnectionState::Running(ActiveConnection {
                    data,
                    upstairs_connection,
                    work: Work::new(),
                });

                let work = self.work_mut(conn_id);
                work.last_flush = last_flush_number;
                info!(self.log, "Set last flush {}", last_flush_number);

                let state = &self.connection_state[&conn_id]; // reborrow
                if let Err(e) =
                    state.reply(Message::LastFlushAck { last_flush_number })
                {
                    bail!("Failed sending LastFlushAck: {}", e);
                }

                /*
                 * Once this command is sent, we are ready to exit
                 * the loop and move forward with receiving IOs
                 */
                info!(self.log, "Downstairs has completed Negotiation");
            }
            Message::ExtentVersionsPlease => {
                let ConnectionState::Negotiating {
                    negotiated,
                    data,
                    upstairs_connection,
                } = state
                else {
                    bail!("Received ExtentVersions while not negotiating");
                };
                if *negotiated != NegotiationState::SentRegionInfo {
                    bail!(
                        "Received ExtentVersions out of order {:?}",
                        negotiated
                    );
                }

                let data = std::mem::replace(data, ConnectionData::dummy());
                let upstairs_connection = *upstairs_connection;
                *state = ConnectionState::Running(ActiveConnection {
                    data,
                    upstairs_connection,
                    work: Work::new(),
                });

                let meta_info = self.region.meta_info().await?;

                let flush_numbers: Vec<_> =
                    meta_info.iter().map(|m| m.flush_number).collect();
                let gen_numbers: Vec<_> =
                    meta_info.iter().map(|m| m.gen_number).collect();
                let dirty_bits: Vec<_> =
                    meta_info.iter().map(|m| m.dirty).collect();
                if flush_numbers.len() > 12 {
                    info!(
                        self.log,
                        "Current flush_numbers [0..12]: {:?}",
                        &flush_numbers[0..12]
                    );
                } else {
                    info!(
                        self.log,
                        "Current flush_numbers [0..12]: {:?}", flush_numbers
                    );
                }

                if let Err(e) = state.reply(Message::ExtentVersions {
                    gen_numbers,
                    flush_numbers,
                    dirty_bits,
                }) {
                    bail!("Failed sending ExtentVersions: {}", e);
                }

                /*
                 * Once this command is sent, we are ready to exit
                 * the loop and move forward with receiving IOs
                 */
            }
            _msg => {
                warn!(self.log, "Ignored message received during negotiation");
            }
        }
        Ok(())
    }

    /// Binds the given `ConnectionId` to a new connection state
    ///
    /// # Panics
    /// If there is no `repair_address`, or the new `ConnectionId` has already
    /// been installed into the map
    fn new_connection(
        &mut self,
        id: ConnectionId,
        reply_channel_tx: mpsc::UnboundedSender<Message>,
    ) -> tokio_util::sync::CancellationToken {
        // Prepare for the autonegotiation / IO loop
        //
        // We'll create a cancellation token here, store it in a DropGuard, and
        // return a child token for `recv_task` (as the most 'upstream' task)
        // to listen for.
        let token = tokio_util::sync::CancellationToken::new();
        let cancel_child = token.child_token();
        let cancel_guard = token.drop_guard();
        let prev = self.connection_state.insert(
            id,
            ConnectionState::Open(ConnectionData {
                repair_addr: self.repair_address.unwrap(),
                cancel: cancel_guard,
                reply_channel_tx,
            }),
        );
        assert!(prev.is_none());

        cancel_child
    }

    /// Removes the given connection
    ///
    /// The connection is removed from `self.connection_state` and
    /// `self.active_upstairs` (if active), and various log messages are
    /// printed.
    fn remove_connection(&mut self, id: ConnectionId) {
        let Some(state) = self.connection_state.remove(&id) else {
            warn!(
                self.log,
                "called remove_connection on connection {id:?}
                 that is already gone"
            );
            return;
        };
        if let Some(upstairs_connection) = state.upstairs_connection() {
            if let ConnectionState::Running(state) = &state {
                info!(
                    self.log,
                    "upstairs {:?} ({id:?}) removed, {} jobs left",
                    upstairs_connection,
                    state.work.jobs(),
                );
            } else {
                info!(
                    self.log,
                    "upstairs {:?} ({id:?}) removed", upstairs_connection,
                );
            }

            if self
                .active_upstairs
                .remove(&upstairs_connection.upstairs_id)
                .is_some()
            {
                info!(
                    self.log,
                    "upstairs {:?} ({id:?}) was previously active, clearing",
                    upstairs_connection,
                );
            }
        } else {
            info!(self.log, "unknown upstairs ({id:?}) removed");
        }
    }

    /// Handles a single message, either negotiation or doing IO
    async fn on_message_for(&mut self, id: ConnectionId, m: Message) {
        let Some(state) = self.connection_state.get_mut(&id) else {
            warn!(self.log, "got message for disconnected id {id:?}; ignoring");
            return;
        };
        if matches!(state, ConnectionState::Running(..)) {
            match self.proc_frame(m, id).await {
                // If we added work, then do it!
                Ok(Some(new_ds_id)) => {
                    cdt::work__start!(|| new_ds_id.0);
                    if let Err(e) = self.do_work_for(id).await {
                        warn!(
                            self.log,
                            "do_work_for returns error {e:?}; disconnecting"
                        );
                        self.remove_connection(id);
                    }
                }
                // If we handled the job locally, nothing to do
                Ok(None) => (),
                Err(e) => {
                    warn!(
                        self.log,
                        "proc_frame returns error {e:?}; disconnecting"
                    );
                    self.remove_connection(id);
                }
            }
        } else if let Err(e) = self.on_negotiation_step(m, id).await {
            warn!(
                self.log,
                "on_negotiation_step returns error {e:?}, disconnecting"
            );
            self.remove_connection(id);
        }
    }

    /// Called to replace a connection (specified by `id`) with a new connection
    /// (specified by `new_upstairs_connection`).
    ///
    /// The original connection should already have been removed from
    /// `self.active_upstairs`, and will be removed from `self.connection_state`
    /// in this function.
    fn on_new_connection_replacing(
        &mut self,
        id: ConnectionId,
        new_upstairs_connection: UpstairsConnection,
    ) {
        // another upstairs negotiated and went active after
        // this one did
        let state = self.connection_state.get_mut(&id).unwrap();
        warn!(
            self.log,
            "Another upstairs {:?} promoted to active, \
             shutting down connection for {:?}",
            new_upstairs_connection,
            state.upstairs_connection()
        );

        if let ConnectionState::Running(state) = state {
            // Note: in the future, differentiate between new upstairs
            // connecting vs same upstairs reconnecting here.
            //
            // Clear out active jobs, the last flush, and completed
            // information, as that will not be valid any longer.
            //
            // TODO: Really work through this error case
            if state.work.active.keys().len() > 0 {
                warn!(
                    self.log,
                    "Crucible Downstairs promoting {:?} to active, \
                        discarding {} jobs",
                    state.upstairs_connection,
                    state.work.active.keys().len()
                );
            }

            // In the future, we may decide there is some way to continue
            // working on outstanding jobs, or a way to merge. But for now,
            // we just throw out what we have and let the upstairs resend
            // anything to us that it did not get an ACK for.
        }

        if let Err(e) = state.reply(Message::YouAreNoLongerActive {
            new_upstairs_id: new_upstairs_connection.upstairs_id,
            new_session_id: new_upstairs_connection.session_id,
            new_gen: new_upstairs_connection.gen,
        }) {
            warn!(self.log, "Failed sending YouAreNoLongerActive: {e}");
        }

        self.connection_state.remove(&id);
    }
}

enum DownstairsRequest {
    /// Checks whether the given extent is closed
    IsExtentClosed {
        eid: ExtentId,
        done: oneshot::Sender<bool>,
    },

    /// Requests that the Downstairs allocates a new connection for this id
    NewConnection {
        id: ConnectionId,
        reply_channel_tx: mpsc::UnboundedSender<Message>,
        done: oneshot::Sender<tokio_util::sync::CancellationToken>,
    },

    /// Prints downstairs work
    ///
    /// This is fire-and-forget, so there's no oneshot reply channel
    ShowWork,

    /// A message has arrived for the given id
    ///
    /// This is fire-and-forget; if the Downstairs doesn't like the message, it
    /// will kill that IO task through the cancellation handle.
    Message { id: ConnectionId, msg: Message },

    /// The given id's upstream connection has closed
    ConnectionClosed { id: ConnectionId },
}

/// Handle allowing for async calls to the Downstairs task
#[derive(Clone)]
pub struct DownstairsHandle {
    tx: mpsc::UnboundedSender<DownstairsRequest>,
}

impl DownstairsHandle {
    pub async fn is_extent_closed(&self, eid: ExtentId) -> Result<bool> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(DownstairsRequest::IsExtentClosed { eid, done })
            .context("could not send message on channel")?;
        rx.await.context("could not receive result")
    }
    pub fn show_work(&self) -> Result<()> {
        self.tx
            .send(DownstairsRequest::ShowWork)
            .context("could not send message on channel")
    }

    async fn new_connection(
        &self,
        id: ConnectionId,
        reply_channel_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<tokio_util::sync::CancellationToken> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(DownstairsRequest::NewConnection {
                id,
                reply_channel_tx,
                done,
            })
            .context("could not send message on channel")?;
        rx.await.context("could not receive result")
    }
}

/*
 * The structure that tracks downstairs work in progress
 */
#[derive(Debug)]
pub struct Work {
    active: HashMap<JobId, DownstairsWork>,
    outstanding_deps: HashMap<JobId, usize>,

    /*
     * We have to keep track of all IOs that have been issued since
     * our last flush, as that is how we make sure dependencies are
     * respected. The last_flush is the downstairs job ID number (ds_id
     * typically) for the most recent flush.
     */
    last_flush: JobId,
    completed: Vec<JobId>,
}

#[derive(Debug, Clone)]
struct DownstairsWork {
    upstairs_connection: UpstairsConnection,
    ds_id: JobId,
    work: IOop,
    state: WorkState,
}

impl Work {
    fn new() -> Self {
        Work {
            active: HashMap::new(),
            outstanding_deps: HashMap::new(),
            last_flush: JobId(0), // TODO(matt) make this an Option?
            completed: Vec::with_capacity(32),
        }
    }

    fn jobs(&self) -> usize {
        self.active.len()
    }

    /**
     * Return a list of downstairs request IDs that are new or have
     * been waiting for other dependencies to finish.
     */
    fn new_work(&self) -> Vec<JobId> {
        let mut result = Vec::with_capacity(self.active.len());

        for job in self.active.values() {
            if job.state == WorkState::New || job.state == WorkState::DepWait {
                result.push(job.ds_id);
            }
        }

        result.sort_unstable();

        result
    }

    fn add_work(&mut self, ds_id: JobId, dsw: DownstairsWork) {
        self.active.insert(ds_id, dsw);
    }

    #[cfg(test)]
    fn get_job(&self, ds_id: JobId) -> DownstairsWork {
        self.active.get(&ds_id).unwrap().clone()
    }

    /**
     * If the requested job is still new, and the dependencies are all met,
     * return the job ID and the upstairs UUID, moving the state of the job as
     * InProgress. If the dependencies are not met, move the state to DepWait.
     *
     * If this job is not new, then just return None.
     *
     * If the job is InProgress, return itself.
     */
    fn in_progress(&mut self, ds_id: JobId, log: Logger) -> Option<JobId> {
        /*
         * Once we support multiple threads, we can obtain a ds_id that
         * looked valid when we made a list of jobs, but something
         * else moved that job along and now it no longer exists.  We
         * need to handle that case correctly.
         */
        if let Some(job) = self.active.get_mut(&ds_id) {
            if job.state == WorkState::New || job.state == WorkState::DepWait {
                /*
                 * Before we can make this in_progress, we have to check the dep
                 * list if there is one and make sure all dependencies are
                 * completed.
                 */
                let dep_list = job.work.deps();

                /*
                 * See which of our dependencies are met.
                 * XXX Make this better/faster by removing the ones that
                 * are met, so next lap we don't have to check again?  There
                 * may be some debug value to knowing what the dep list was,
                 * so consider that before making this faster.
                 */
                let mut deps_outstanding: Vec<JobId> =
                    Vec::with_capacity(dep_list.len());

                for dep in dep_list.iter() {
                    // The Downstairs currently assumes that all jobs previous
                    // to the last flush have completed, hence this early out.
                    //
                    // Currently `work.completed` is cleared out when
                    // `Downstairs::complete_work` (or `complete` in mod test)
                    // is called with a FlushAck so this early out cannot be
                    // removed unless that is changed too.
                    if dep <= &self.last_flush {
                        continue;
                    }

                    if !self.completed.contains(dep) {
                        deps_outstanding.push(*dep);
                    }
                }

                if !deps_outstanding.is_empty() {
                    let print = if let Some(existing_outstanding_deps) =
                        self.outstanding_deps.get(&ds_id)
                    {
                        *existing_outstanding_deps != deps_outstanding.len()
                    } else {
                        false
                    };

                    if print {
                        warn!(
                            log,
                            "{} job {} for connection {:?} waiting on {} deps",
                            ds_id,
                            job.upstairs_connection.upstairs_id,
                            match &job.work {
                                IOop::Write { .. } => "Write",
                                IOop::WriteUnwritten { .. } => "WriteUnwritten",
                                IOop::Flush { .. } => "Flush",
                                IOop::Read { .. } => "Read",
                                IOop::ExtentClose { .. } => "ECLose",
                                IOop::ExtentFlushClose { .. } => "EFlushCLose",
                                IOop::ExtentLiveRepair { .. } => "ELiveRepair",
                                IOop::ExtentLiveReopen { .. } => "ELiveReopen",
                                IOop::ExtentLiveNoOp { .. } => "NoOp",
                            },
                            deps_outstanding.len(),
                        );
                    }

                    let _ = self
                        .outstanding_deps
                        .insert(ds_id, deps_outstanding.len());

                    /*
                     * If we got here, then the dep is not met.
                     * Set DepWait if not already set.
                     */
                    if job.state == WorkState::New {
                        job.state = WorkState::DepWait;
                    }

                    return None;
                }

                /*
                 * We had no dependencies, or they are all completed, we
                 * can go ahead and work on this job.
                 */
                job.state = WorkState::InProgress;

                Some(job.ds_id)
            } else if job.state == WorkState::InProgress {
                // A previous call of this function put this job in progress, so
                // return idempotently.
                Some(job.ds_id)
            } else {
                /*
                 * job id is not new, we can't run it.
                 */
                None
            }
        } else {
            /*
             * XXX If another upstairs took over, a job ID could be
             * invalid.  Check here to verify that this set of
             * downstairs tasks is no longer active.
             */
            warn!(log, "This ID is no longer a valid job id");
            None
        }
    }

    /// Returns a job that's ready to have the work done
    ///
    /// # Panics
    /// If any part of the job's preconditions aren't met (it's not marked as
    /// `InProgress`, it has unmet dependencies, etc).
    fn get_ready_job(&self, job_id: JobId) -> DownstairsWork {
        let job = self.active.get(&job_id).unwrap();
        assert_eq!(job.state, WorkState::InProgress);
        assert_eq!(job_id, job.ds_id);

        // validate that deps are done
        let dep_list = job.work.deps();
        for dep in dep_list {
            let last_flush_satisfied = dep <= &self.last_flush;
            let complete_satisfied = self.completed.contains(dep);

            assert!(last_flush_satisfied || complete_satisfied);
        }

        job.clone()
    }
}

/*
 * XXX We may not need Done. At the moment all we actually look at is New or
 * InProgress.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq)]
pub enum WorkState {
    New,
    DepWait,
    InProgress,
    Done,
}

impl fmt::Display for WorkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkState::New => {
                write!(f, " New")
            }
            WorkState::DepWait => {
                write!(f, "DepW")
            }
            WorkState::InProgress => {
                write!(f, "In P")
            }
            WorkState::Done => {
                write!(f, "Done")
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WrappedStream {
    Http(tokio::net::TcpStream),
    Https(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

/// On-disk backend for downstairs storage
///
/// Normally, we only allow the most recent backend.  However, for integration
/// tests, it can be useful to create volumes using older backends.
#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub enum Backend {
    #[default]
    RawFile,

    #[cfg(any(test, feature = "integration-tests"))]
    SQLite,
}

pub async fn create_region(
    block_size: u64,
    data: PathBuf,
    extent_size: u64,
    extent_count: u32,
    uuid: Uuid,
    encrypted: bool,
    log: Logger,
) -> Result<Region> {
    create_region_with_backend(
        data,
        Block {
            value: extent_size,
            shift: block_size.trailing_zeros(),
        },
        extent_count,
        uuid,
        encrypted,
        Backend::default(),
        log,
    )
    .await
}

pub async fn create_region_with_backend(
    data: PathBuf,
    extent_size: Block,
    extent_count: u32,
    uuid: Uuid,
    encrypted: bool,
    backend: Backend,
    log: Logger,
) -> Result<Region> {
    /*
     * Create the region options, then the region.
     */
    let mut region_options: crucible_common::RegionOptions = Default::default();
    region_options.set_block_size(extent_size.block_size_in_bytes().into());
    region_options.set_extent_size(extent_size);
    region_options.set_uuid(uuid);
    region_options.set_encrypted(encrypted);

    let mut region =
        Region::create_with_backend(data, region_options, backend, log).await?;
    region.extend(extent_count).await?;

    Ok(region)
}

/// Return `struct` for `start_downstairs`
#[derive(Debug)]
pub struct RunningDownstairs {
    pub join_handle: tokio::task::JoinHandle<Result<()>>,
    pub address: SocketAddr,
    pub repair_address: SocketAddr,
}

/// Returns Ok if everything spawned ok, Err otherwise
///
/// Return Ok(main task join handle) if all the necessary tasks spawned
/// successfully, and Err otherwise.
#[allow(clippy::too_many_arguments)]
pub async fn start_downstairs(
    mut ds: Downstairs,
    address: IpAddr,
    oximeter: Option<SocketAddr>,
    port: u16,
    rport: u16,
    cert_pem: Option<String>,
    key_pem: Option<String>,
    root_cert_pem: Option<String>,
) -> Result<RunningDownstairs> {
    let root_log = ds.log.clone();
    if let Some(oximeter) = oximeter {
        let dss = ds.dss.clone();
        let log = root_log.new(o!("task" => "oximeter".to_string()));

        tokio::spawn(async move {
            let producer_address = SocketAddr::new(address, 0);
            if let Err(e) =
                stats::ox_stats(dss, oximeter, producer_address, &log).await
            {
                error!(log, "ERROR: oximeter failed: {:?}", e);
            } else {
                warn!(log, "OK: oximeter all done");
            }
        });
    }

    // Setup a log for this task
    let log = root_log.new(o!("task" => "main".to_string()));

    let listen_on = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), port),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), port),
    };

    // Establish a listen server on the port.
    let listener = TcpListener::bind(&listen_on).await?;
    let local_addr = listener.local_addr()?;
    ds.address = Some(local_addr);

    let info = crucible_common::BuildInfo::default();
    info!(log, "Crucible Version: {}", info);
    info!(
        log,
        "Upstairs <-> Downstairs Message Version: {}", CRUCIBLE_MESSAGE_VERSION
    );

    let repair_address = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), rport),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), rport),
    };

    let repair_log = root_log.new(o!("task" => "repair".to_string()));
    let repair_listener =
        match repair::repair_main(&ds, repair_address, &repair_log).await {
            Err(e) => {
                // TODO tear down other things if repair server can't be
                // started?
                bail!("got {:?} from repair main", e);
            }

            Ok(socket_addr) => socket_addr,
        };

    ds.repair_address = Some(repair_listener);
    info!(log, "Using repair address: {:?}", repair_listener);

    // Optionally require SSL connections
    let ssl_acceptor = if let Some(cert_pem_path) = cert_pem {
        let key_pem_path = key_pem.unwrap();
        let root_cert_pem_path = root_cert_pem.unwrap();

        let context = crucible_common::x509::TLSContext::from_paths(
            &cert_pem_path,
            &key_pem_path,
            &root_cert_pem_path,
        )?;

        let config = context.get_server_config()?;

        info!(log, "Configured SSL acceptor");

        Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    } else {
        // unencrypted
        info!(log, "No SSL acceptor configured");
        None
    };

    let mut dss = ds.dss.clone(); // shared handle for stats
    let handle = ds.handle(); // handle for passing messages

    // This is where the actual work takes place; owning the Downstairs
    let mut ds_runner = Downstairs::spawn_runner(ds);

    let join_handle = tokio::spawn(async move {
        // We now loop listening for a connection from the Upstairs.
        // When we get one, we call `proc()` to spawn worker tasks, then wait
        // for another connection.
        info!(log, "downstairs listening on {}", listen_on);
        let mut id = ConnectionId(0);
        loop {
            let v = tokio::select! {
                _ = &mut ds_runner => {
                    info!(log, "downstairs runner stopped; exiting");
                    break Ok(());
                }
                v = listener.accept() => {v}
            };
            let (sock, raddr) = v?;

            /*
             * We have a new connection; before we wrap it, set TCP_NODELAY
             * to assure that we don't get Nagle'd.
             */
            sock.set_nodelay(true).expect("could not set TCP_NODELAY");

            let stream: WrappedStream = if let Some(ssl_acceptor) =
                &ssl_acceptor
            {
                let ssl_acceptor = ssl_acceptor.clone();
                WrappedStream::Https(match ssl_acceptor.accept(sock).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            log,
                            "rejecting connection from {:?}: {:?}", raddr, e,
                        );
                        continue;
                    }
                })
            } else {
                WrappedStream::Http(sock)
            };

            info!(log, "accepted connection from {:?}", raddr);

            // Add one to the counter every time we have a connection
            // from an upstairs
            dss.add_connection();
            let task_log = root_log.new(o!("id" => id.0.to_string()));
            let handle = handle.clone();
            if let Err(e) = proc_stream(handle, id, stream, &task_log).await {
                error!(
                    task_log,
                    "connection ({raddr}) failed to spawn tasks: {e:?}",
                );
            } else {
                info!(task_log, "connection ({raddr}): tasks spawned");
            }
            id.0 += 1;
        }
    });

    Ok(RunningDownstairs {
        join_handle,
        address: local_addr,
        repair_address: repair_listener,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use rand_chacha::ChaCha20Rng;
    use std::net::Ipv4Addr;
    use tempfile::{tempdir, TempDir};
    use tokio::net::TcpSocket;

    const DUMMY_REPAIR_SOCKET: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    fn add_work(
        work: &mut Work,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        deps: Vec<JobId>,
        is_flush: bool,
    ) {
        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: if is_flush {
                    IOop::Flush {
                        dependencies: deps,
                        flush_number: 10,
                        gen_number: 0,
                        snapshot_details: None,
                        extent_limit: None,
                    }
                } else {
                    IOop::Read {
                        dependencies: deps,
                        requests: RegionReadRequest(vec![RegionReadReq {
                            extent: ExtentId(1),
                            offset: Block::new_512(1),
                            count: NonZeroUsize::new(1).unwrap(),
                        }]),
                    }
                },
                state: WorkState::New,
            },
        );
    }

    fn add_work_rf(
        work: &mut Work,
        upstairs_connection: UpstairsConnection,
        ds_id: JobId,
        deps: Vec<JobId>,
    ) {
        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: IOop::WriteUnwritten {
                    dependencies: deps,
                    writes: RegionWrite(vec![]),
                },
                state: WorkState::New,
            },
        );
    }

    fn complete(work: &mut Work, ds_id: JobId) {
        let is_flush = {
            let job = work.active.get(&ds_id).unwrap();

            // validate that deps are done
            let dep_list = job.work.deps();
            for dep in dep_list {
                let last_flush_satisfied = dep <= &work.last_flush;
                let complete_satisfied = work.completed.contains(dep);

                assert!(last_flush_satisfied || complete_satisfied);
            }

            matches!(
                job.work,
                IOop::Flush {
                    dependencies: _,
                    flush_number: _,
                    gen_number: _,
                    snapshot_details: _,
                    extent_limit: _,
                }
            )
        };

        let _ = work.active.remove(&ds_id);

        if is_flush {
            work.last_flush = ds_id;
            work.completed = Vec::with_capacity(32);
        } else {
            work.completed.push(ds_id);
        }
    }

    fn test_push_next_jobs(work: &mut Work) -> Vec<JobId> {
        let mut jobs = vec![];
        let mut new_work = work.new_work();

        new_work.sort_unstable();

        for new_id in new_work.iter() {
            let job = work.in_progress(*new_id, csl());
            match job {
                Some(job) => {
                    jobs.push(job);
                }
                None => {
                    continue;
                }
            }
        }

        for job in &jobs {
            assert_eq!(
                work.active.get(job).unwrap().state,
                WorkState::InProgress
            );
        }

        jobs
    }

    fn test_do_work(work: &mut Work, jobs: Vec<JobId>) {
        for job_id in jobs {
            complete(work, job_id);
        }
    }

    #[test]
    fn you_had_one_job() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);

        assert_eq!(work.new_work(), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work).is_empty());
    }

    #[tokio::test]
    async fn test_simple_read() -> Result<()> {
        // Test region create and a read of one block.
        let block_size: u64 = 512;
        let extent_size = 4;

        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let mut ds = Downstairs::new_builder(&path_dir, false)
            .set_logger(csl())
            .build()
            .await?;
        ds.repair_address = Some(DUMMY_REPAIR_SOCKET);

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // Dummy connection id
        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let rio = IOop::Read {
            dependencies: Vec::new(),
            requests: RegionReadRequest(vec![RegionReadReq {
                extent: ExtentId(0),
                offset: Block::new_512(1),
                count: NonZeroUsize::new(1).unwrap(),
            }]),
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        let deps = vec![JobId(1000)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: RegionReadRequest(vec![RegionReadReq {
                extent: ExtentId(1),
                offset: Block::new_512(1),
                count: NonZeroUsize::new(1).unwrap(),
            }]),
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        show_work(&mut ds);

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.active_mut(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        for id in new_work.iter() {
            let ip_id = ds.in_progress(conn_id, *id).unwrap();
            assert_eq!(ip_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(conn_id, *id).await;
            println!("Got m: {:?}", m);
            ds.complete_work(conn_id, *id, m);
        }
        show_work(&mut ds);
        Ok(())
    }

    // Test function to create a simple downstairs with the given block
    // and extent values.  Returns the Downstairs.
    async fn create_test_downstairs(
        block_size: u64,
        extent_size: u64,
        extent_count: u32,
        dir: &TempDir,
    ) -> Result<Downstairs> {
        // create region
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        mkdir_for_file(dir.path())?;
        let mut region = Region::create(dir, region_options, csl()).await?;
        region.extend(extent_count).await?;

        let path_dir = dir.as_ref().to_path_buf();
        let mut ds = Downstairs::new_builder(&path_dir, false)
            .set_logger(csl())
            .build()
            .await?;
        ds.repair_address = Some(DUMMY_REPAIR_SOCKET);

        Ok(ds)
    }

    #[tokio::test]
    async fn test_extent_simple_close_flush_close() -> Result<()> {
        // Test creating these IOops:
        // IOop::ExtentClose
        // IOop::ExtentFlushClose
        // IOop::Read
        // IOop::ExtentLiveNoOp
        // IOop::ExtentLiveReopen
        // Put them on the work queue and verify they flow through the
        // queue as expected, No actual work results are checked.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 10,
        };

        // Dummy ConnectionId
        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: ExtentId(0),
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: ExtentId(1),
            flush_number: 1,
            gen_number: 2,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        let deps = vec![JobId(1000), JobId(1001)];
        let rio = IOop::Read {
            dependencies: deps,
            requests: RegionReadRequest(vec![RegionReadReq {
                extent: ExtentId(2),
                offset: Block::new_512(1),
                count: NonZeroUsize::new(1).unwrap(),
            }]),
        };
        ds.active_mut(conn_id).add_work(JobId(1002), rio);

        let deps = vec![JobId(1000), JobId(1001), JobId(1002)];
        let rio = IOop::ExtentLiveNoOp { dependencies: deps };
        ds.active_mut(conn_id).add_work(JobId(1003), rio);

        let deps = vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)];
        let rio = IOop::ExtentLiveReopen {
            dependencies: deps,
            extent: ExtentId(0),
        };
        ds.active_mut(conn_id).add_work(JobId(1004), rio);

        println!("Before doing work we have:");
        show_work(&mut ds);

        // Now we mimic what happens in the do_work_task()
        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 5);

        for id in new_work.iter() {
            let ip_id = ds.in_progress(conn_id, *id).unwrap();
            assert_eq!(ip_id, *id);
            println!("Do IOop {}", *id);
            let m = ds.do_work(conn_id, *id).await;
            println!("Got m: {:?}", m);
            ds.complete_work(conn_id, *id, m);
        }

        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_new_close_flush_close() -> Result<()> {
        // Test sending IOops for ExtentClose and ExtentFlushClose when we
        // have an unwritten block.  We are verifying here that only the
        // dirty bit is set, as the other values (gen/flush) should remain
        // as if the extent is unwritten.
        // After closing, reopen both extents and verify no issues.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        ds.repair_address = Some(DUMMY_REPAIR_SOCKET);

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        // Dummy ConnectionId
        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let rio = IOop::ExtentClose {
            dependencies: Vec::new(),
            extent: ExtentId(0),
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: ExtentId(1),
            flush_number: 1,
            gen_number: gen,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        // Add the two reopen commands for the two extents we closed.
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1000)],
            extent: ExtentId(0),
        };
        ds.active_mut(conn_id).add_work(JobId(1002), rio);
        let rio = IOop::ExtentLiveReopen {
            dependencies: vec![JobId(1001)],
            extent: ExtentId(1),
        };
        ds.active_mut(conn_id).add_work(JobId(1003), rio);
        show_work(&mut ds);

        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the ExtentClose
        ds.in_progress(conn_id, JobId(1000)).unwrap();
        let m = ds.do_work(conn_id, JobId(1000)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1000), m);

        // Process the ExtentFlushClose
        ds.in_progress(conn_id, JobId(1001)).unwrap();
        let m = ds.do_work(conn_id, JobId(1001)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was unwritten, the close would not have
        // changed the generation number nor flush number, and dirty bit
        // should be false.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1001), m);

        // Process the two ExtentReopen commands
        for id in (1002..=1003).map(JobId) {
            ds.in_progress(conn_id, id).unwrap();
            let m = ds.do_work(conn_id, id).await;
            match m {
                Message::ExtentLiveAckId {
                    upstairs_id,
                    session_id,
                    job_id,
                    ref result,
                } => {
                    assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                    assert_eq!(session_id, upstairs_connection.session_id);
                    assert_eq!(job_id, id);
                    assert!(result.is_ok());
                }
                _ => {
                    panic!("Incorrect message: {:?} for id: {}", m, id);
                }
            }
            ds.complete_work(conn_id, id, m);
        }

        // Nothing should be left on the queue.
        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    // A test function that will return a generic crucible write command
    // for use when building the IOop::Write structure.  The data (of 9's)
    // matches the hash.
    fn create_generic_test_write(eid: ExtentId) -> RegionWrite {
        let data = Bytes::from(vec![9u8; 512]);
        let offset = Block::new_512(1);

        RegionWrite(vec![RegionWriteReq {
            extent: eid,
            write: ExtentWrite {
                offset,
                data,
                block_contexts: vec![BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                }],
            },
        }])
    }

    #[tokio::test]
    async fn test_extent_write_flush_write_close() -> Result<()> {
        // Send IOops for write, flush, write, then ExtentClose
        // We are testing here that the flush will make the metadata for
        // the first write (the gen/flush) persistent.  The 2nd write won't
        // change what is returned by the ExtentClose, as that data
        // was specifically not flushed, but the dirty bit should be set.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let eid = ExtentId(3);

        // Create and add the first write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        // add work for flush 1001
        let rio = IOop::Flush {
            dependencies: vec![],
            flush_number: 3,
            gen_number: gen,
            snapshot_details: None,
            extent_limit: None,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        // Add work for 2nd write 1002
        let writes = create_generic_test_write(eid);

        let rio = IOop::Write {
            dependencies: vec![JobId(1000), JobId(1001)],
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1002), rio);

        // Now close the extent
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000), JobId(1001), JobId(1002)],
            extent: eid,
        };
        ds.active_mut(conn_id).add_work(JobId(1003), rio);

        show_work(&mut ds);

        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the first Write
        ds.in_progress(conn_id, JobId(1000)).unwrap();
        let m = ds.do_work(conn_id, JobId(1000)).await;
        ds.complete_work(conn_id, JobId(1000), m);

        // Process the flush
        ds.in_progress(conn_id, JobId(1001)).unwrap();
        let m = ds.do_work(conn_id, JobId(1001)).await;
        ds.complete_work(conn_id, JobId(1001), m);

        // Process write 2
        ds.in_progress(conn_id, JobId(1002)).unwrap();
        let m = ds.do_work(conn_id, JobId(1002)).await;
        ds.complete_work(conn_id, JobId(1002), m);

        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        ds.in_progress(conn_id, JobId(1003)).unwrap();
        let m = ds.do_work(conn_id, JobId(1003)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 10); // From the flush
                assert_eq!(*f, 3); // From the flush
                assert!(*d); // Dirty should be true.
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1003), m);

        // Nothing should be left on the queue.
        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_close() -> Result<()> {
        // Test sending IOops for Write then ExtentClose.
        // Because we are not sending a flush here, we expect only the
        // dirty bit to be set when we look at the close results.
        // For the write, we do verify that the return contents in the
        // message are as we expect.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let eid = ExtentId(0);

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1000)],
            extent: eid,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        show_work(&mut ds);

        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        ds.in_progress(conn_id, JobId(1000)).unwrap();
        let m = ds.do_work(conn_id, JobId(1000)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1000), m);

        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentClose
        ds.in_progress(conn_id, JobId(1001)).unwrap();
        let m = ds.do_work(conn_id, JobId(1001)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written but not flushed, the close would
        // not have changed the generation number nor flush number But, the
        // dirty bit should be true.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1001), m);

        // Nothing should be left on the queue.
        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_flush_close() -> Result<()> {
        // Test sending IOops for Write then ExtentFlushClose
        // Verify we get the expected results.  Because we will be
        // writing to the extents, we expect to get different results
        // than in the non-writing case.
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;

        // This happens in proc() function.
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        // Dummy ConnectionId
        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let eid = ExtentId(1);

        // Create the write
        let writes = create_generic_test_write(eid);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid,
            flush_number: 3,
            gen_number: gen,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        show_work(&mut ds);

        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 2);

        // Process the Write
        ds.in_progress(conn_id, JobId(1000)).unwrap();
        let m = ds.do_work(conn_id, JobId(1000)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.
        match m {
            Message::WriteAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1000));
                assert!(result.is_ok());
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1000), m);

        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 1);

        // Process the ExtentFlushClose
        ds.in_progress(conn_id, JobId(1001)).unwrap();
        let m = ds.do_work(conn_id, JobId(1001)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1001));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 3);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1001), m);

        // Nothing should be left on the queue.
        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_extent_write_write_flush_close() -> Result<()> {
        // Test sending IOops for two different Writes then an
        // ExtentFlushClose on one extent, and then a ExtentClose
        // on the other extent.
        // Verify that the first extent close returns data including
        // a flush, and the second extent remains dirty (no flush pollution
        // from the flush on the first extent).
        let block_size: u64 = 512;
        let extent_size = 4;
        let dir = tempdir()?;
        let gen = 10;

        let mut ds =
            create_test_downstairs(block_size, extent_size, 5, &dir).await?;
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen,
        };

        let conn_id = ConnectionId(0);
        ds.add_fake_connection(upstairs_connection, conn_id);
        ds.promote_to_active(upstairs_connection, conn_id).await?;

        let eid_one = ExtentId(1);
        let eid_two = ExtentId(2);

        // Create the write for extent 1
        let writes = create_generic_test_write(eid_one);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1000), rio);

        // Create the write for extent 2
        let writes = create_generic_test_write(eid_two);
        let rio = IOop::Write {
            dependencies: Vec::new(),
            writes,
        };
        ds.active_mut(conn_id).add_work(JobId(1001), rio);

        // Flush and close extent 1
        let rio = IOop::ExtentFlushClose {
            dependencies: vec![JobId(1000)],
            extent: eid_one,
            flush_number: 6,
            gen_number: gen,
        };
        ds.active_mut(conn_id).add_work(JobId(1002), rio);

        // Just close extent 2
        let rio = IOop::ExtentClose {
            dependencies: vec![JobId(1001)],
            extent: eid_two,
        };
        ds.active_mut(conn_id).add_work(JobId(1003), rio);

        show_work(&mut ds);

        let new_work = ds.active(conn_id).work.new_work();
        println!("Got new work: {:?}", new_work);
        assert_eq!(new_work.len(), 4);

        // Process the Writes
        for id in (1000..=1001).map(JobId) {
            ds.in_progress(conn_id, id).unwrap();
            let m = ds.do_work(conn_id, id).await;
            ds.complete_work(conn_id, id, m);
        }

        // Process the ExtentFlushClose
        ds.in_progress(conn_id, JobId(1002)).unwrap();
        let m = ds.do_work(conn_id, JobId(1002)).await;
        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1002));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, gen);
                assert_eq!(*f, 6);
                assert!(!*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1002), m);

        // Process the ExtentClose
        ds.in_progress(conn_id, JobId(1003)).unwrap();
        let m = ds.do_work(conn_id, JobId(1003)).await;

        // Verify that we not only have composed the correct ACK, but the
        // result inside that ACK is also what we expect.  In this case
        // because the extent was written, and we sent a "flush and close"
        // the data that we wrote should be committed and our flush should
        // have persisted that data.
        match m {
            Message::ExtentLiveCloseAck {
                upstairs_id,
                session_id,
                job_id,
                ref result,
            } => {
                assert_eq!(upstairs_id, upstairs_connection.upstairs_id);
                assert_eq!(session_id, upstairs_connection.session_id);
                assert_eq!(job_id, JobId(1003));
                let (g, f, d) = result.as_ref().unwrap();
                assert_eq!(*g, 0);
                assert_eq!(*f, 0);
                assert!(*d);
            }
            _ => {
                panic!("Incorrect message: {:?}", m);
            }
        }
        ds.complete_work(conn_id, JobId(1003), m);
        // Nothing should be left on the queue.
        let new_work = ds.active(conn_id).work.new_work();
        assert_eq!(new_work.len(), 0);
        Ok(())
    }

    #[test]
    fn jobs_write_unwritten() {
        // Verify WriteUnwritten jobs move through the work queue
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        add_work_rf(&mut work, upstairs_connection, JobId(1000), vec![]);

        assert_eq!(work.new_work(), vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        assert!(test_push_next_jobs(&mut work).is_empty());
    }

    fn test_misc_work_through_work_queue(ds_id: JobId, ioop: IOop) {
        // Verify that a IOop work request will move through the work queue.
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        work.add_work(
            ds_id,
            DownstairsWork {
                upstairs_connection,
                ds_id,
                work: ioop,
                state: WorkState::New,
            },
        );

        assert_eq!(work.new_work(), vec![ds_id]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![ds_id]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![ds_id]);

        assert!(test_push_next_jobs(&mut work).is_empty());
    }

    #[test]
    fn jobs_extent_close() {
        // Verify ExtentClose jobs move through the work queue
        let eid = ExtentId(1);
        let ioop = IOop::ExtentClose {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_flush_close() {
        // Verify ExtentFlushClose jobs move through the work queue

        let eid = ExtentId(1);
        let ioop = IOop::ExtentFlushClose {
            dependencies: vec![],
            extent: eid,
            flush_number: 1,
            gen_number: 2,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_repair() {
        // Verify ExtentLiveRepair jobs move through the work queue

        let eid = ExtentId(1);
        let source_repair_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let ioop = IOop::ExtentLiveRepair {
            dependencies: vec![],
            extent: eid,
            source_repair_address,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_reopen() {
        // Verify ExtentLiveReopen jobs move through the work queue
        let eid = ExtentId(1);

        let ioop = IOop::ExtentLiveReopen {
            dependencies: vec![],
            extent: eid,
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_extent_live_noop() {
        // Verify ExtentLiveNoOp jobs move through the work queue

        let ioop = IOop::ExtentLiveNoOp {
            dependencies: vec![],
        };
        test_misc_work_through_work_queue(JobId(1000), ioop);
    }

    #[test]
    fn jobs_independent() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add two independent jobs
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(&mut work, upstairs_connection, JobId(1001), vec![], false);

        // new_work returns all new jobs
        assert_eq!(work.new_work(), vec![JobId(1000), JobId(1001)]);

        // should push both, they're independent
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000), JobId(1001)]);

        // new work returns only jobs in new or dep wait
        assert!(work.new_work().is_empty());

        // do work
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);

        assert!(test_push_next_jobs(&mut work).is_empty());
    }

    #[test]
    fn unblock_job() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add two jobs, one blocked on another
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(work.new_work(), vec![JobId(1000), JobId(1001)]);

        // only one is ready to run
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        // new_work returns all new or dep wait jobs
        assert_eq!(work.new_work(), vec![JobId(1001)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
    }

    #[test]
    fn unblock_job_chain() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000), JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_first_is_flush() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain, first is flush
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], true);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1000));
        assert_eq!(work.completed, vec![JobId(1001), JobId(1002)]);
    }

    #[test]
    fn unblock_job_chain_second_is_flush() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other in a chain, second is flush
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            true,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // new_work returns all new or dep wait jobs
        assert_eq!(
            work.new_work(),
            vec![JobId(1000), JobId(1001), JobId(1002)]
        );

        // only one is ready to run at a time

        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        assert_eq!(work.new_work(), vec![JobId(1001), JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        assert_eq!(work.new_work(), vec![JobId(1002)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert!(work.completed.is_empty());
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        assert!(work.new_work().is_empty());

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1001));
        assert_eq!(work.completed, vec![JobId(1002)]);
    }

    #[test]
    fn unblock_job_upstairs_sends_big_deps() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            true,
        );

        // Downstairs is really fast!
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        // Upstairs sends a job with these three in deps, not knowing Downstairs
        // has done the jobs already
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1004),
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1004)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert_eq!(work.completed, vec![JobId(1003), JobId(1004)]);
    }

    #[test]
    fn job_dep_not_satisfied() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            true,
        );

        // Add one that can't run yet
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(2000)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(1002));
        assert!(work.completed.is_empty());

        assert_eq!(work.new_work(), vec![JobId(1003)]);
        assert_eq!(
            work.active.get(&JobId(1003)).unwrap().state,
            WorkState::DepWait
        );
    }

    #[test]
    fn two_job_chains() {
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        // Add another set of jobs blocked on each other
        add_work(&mut work, upstairs_connection, JobId(2000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(2001),
            vec![JobId(2000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(2002),
            vec![JobId(2000), JobId(2001)],
            true,
        );

        // should do each chain in sequence
        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000), JobId(2000)]);
        test_do_work(&mut work, next_jobs);
        assert_eq!(work.completed, vec![JobId(1000), JobId(2000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001), JobId(2001)]);
        test_do_work(&mut work, next_jobs);
        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(2000), JobId(1001), JobId(2001)]
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002), JobId(2002)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(work.last_flush, JobId(2002));
        assert!(work.completed.is_empty());
    }

    #[test]
    fn out_of_order_arrives_after_first_push_next_jobs() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_first_do_work() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[test]
    fn out_of_order_arrives_after_1001_completes() {
        /*
         * Test that jobs arriving out of order still complete.
         */
        let mut work = Work::new();
        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 0,
        };

        // Add three jobs all blocked on each other (missing 1002)
        add_work(&mut work, upstairs_connection, JobId(1000), vec![], false);
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1001),
            vec![JobId(1000)],
            false,
        );
        add_work(
            &mut work,
            upstairs_connection,
            JobId(1003),
            vec![JobId(1000), JobId(1001), JobId(1002)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1000)]);

        test_do_work(&mut work, next_jobs);

        assert_eq!(work.completed, vec![JobId(1000)]);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1001)]);
        test_do_work(&mut work, next_jobs);

        // can't run anything, dep not satisfied
        let next_jobs = test_push_next_jobs(&mut work);
        assert!(next_jobs.is_empty());
        test_do_work(&mut work, next_jobs);

        add_work(
            &mut work,
            upstairs_connection,
            JobId(1002),
            vec![JobId(1000), JobId(1001)],
            false,
        );

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1002)]);
        test_do_work(&mut work, next_jobs);

        let next_jobs = test_push_next_jobs(&mut work);
        assert_eq!(next_jobs, vec![JobId(1003)]);
        test_do_work(&mut work, next_jobs);

        assert_eq!(
            work.completed,
            vec![JobId(1000), JobId(1001), JobId(1002), JobId(1003)]
        );
    }

    #[tokio::test]
    async fn import_test_basic() -> Result<()> {
        /*
         * import + export test where data matches region size
         */

        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_small() -> Result<()> {
        /*
         * import + export test where data is smaller than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 fewer bytes than region size)

        let total_bytes = region.def().total_size() - 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 fewer bytes imported than
        // region size still means the whole region is exported)

        let export_path = tempdir.path().join("exported_data");
        let region_size = region.def().total_size();
        downstairs_export(
            &mut region,
            &export_path,
            0,
            region_size / block_size,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded

        let padding_size = actual.len() - total_bytes;
        assert_eq!(padding_size, 100);

        let mut padding = vec![0; padding_size];
        padding.resize(padding_size, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_too_large() -> Result<()> {
        /*
         * import + export test where data is larger than region size
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file (100 more bytes than region size)

        let total_bytes = region.def().total_size() + 100;
        let mut random_data = vec![0; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // export region to another file (note: 100 more bytes will have caused
        // 10 more extents to be added, but someone running the export command
        // will use the number of blocks copied by the import command)
        assert_eq!(region.def().extent_count(), 11);

        let export_path = tempdir.path().join("exported_data");
        downstairs_export(
            &mut region,
            &export_path,
            0,
            total_bytes / block_size + 1,
        )
        .await?;

        // compare files

        let expected = std::fs::read(random_file_path)?;
        let actual = std::fs::read(export_path)?;

        // assert what was imported is correct

        let total_bytes = total_bytes as usize;
        assert_eq!(expected, actual[0..total_bytes]);

        // assert the rest is zero padded
        // the export only exported the extra block, not the extra extent
        let padding_in_extra_block: usize = 512 - 100;

        let mut padding = vec![0; padding_in_extra_block];
        padding.resize(padding_in_extra_block, 0);
        assert_eq!(actual[total_bytes..], padding);

        Ok(())
    }

    #[tokio::test]
    async fn import_test_basic_read_blocks() -> Result<()> {
        /*
         * import + export test where data matches region size, and read the
         * blocks
         */
        let block_size: u64 = 512;
        let extent_size = 10;

        // create region

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(10).await?;

        // create random file

        let total_bytes = region.def().total_size();
        let mut random_data = vec![0u8; total_bytes as usize];
        random_data.resize(total_bytes as usize, 0u8);

        let mut rng = ChaCha20Rng::from_entropy();
        rng.fill_bytes(&mut random_data);

        // write random_data to file

        let tempdir = tempdir()?;
        mkdir_for_file(tempdir.path())?;

        let random_file_path = tempdir.path().join("random_data");
        let mut random_file = File::create(&random_file_path)?;
        random_file.write_all(&random_data[..])?;

        // import random_data to the region

        downstairs_import(&mut region, &random_file_path).await?;
        region.region_flush(1, 1, &None, JobId(0), None).await?;

        // read block by block
        let mut read_data = Vec::with_capacity(total_bytes as usize);
        for eid in (0..region.def().extent_count()).map(ExtentId) {
            for offset in 0..region.def().extent_size().value {
                let response = region
                    .region_read(
                        &RegionReadRequest(vec![RegionReadReq {
                            extent: eid,
                            offset: Block::new_512(offset),
                            count: NonZeroUsize::new(1).unwrap(),
                        }]),
                        JobId(0),
                    )
                    .await?;

                assert_eq!(response.blocks.len(), 1);

                let hashes = response.hashes(0);
                assert_eq!(hashes.len(), 1);
                assert_eq!(integrity_hash(&[&response.data[..]]), hashes[0],);

                read_data.extend_from_slice(&response.data[..]);
            }
        }

        assert_eq!(random_data, read_data);

        Ok(())
    }

    async fn build_test_downstairs(read_only: bool) -> Result<Downstairs> {
        let block_size: u64 = 512;
        let extent_size = 4;

        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        region_options.set_block_size(block_size);
        region_options.set_extent_size(Block::new(
            extent_size,
            block_size.trailing_zeros(),
        ));
        region_options.set_uuid(Uuid::new_v4());

        let dir = tempdir()?;
        mkdir_for_file(dir.path())?;

        let mut region = Region::create(&dir, region_options, csl()).await?;
        region.extend(2).await?;

        let path_dir = dir.as_ref().to_path_buf();

        let mut ds = Downstairs::new_builder(&path_dir, read_only)
            .set_logger(csl())
            .build()
            .await?;
        ds.repair_address = Some(DUMMY_REPAIR_SOCKET);
        Ok(ds)
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_write() -> Result<()> {
        let mut ds = build_test_downstairs(false).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        ds.add_fake_connection(upstairs_connection, ConnectionId(0));
        ds.promote_to_active(upstairs_connection, ConnectionId(0))
            .await?;

        assert_eq!(ds.active_upstairs().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_one_read_only() -> Result<()> {
        let mut ds = build_test_downstairs(true).await?;

        let upstairs_connection = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        ds.add_fake_connection(upstairs_connection, ConnectionId(0));
        ds.promote_to_active(upstairs_connection, ConnectionId(0))
            .await?;

        assert_eq!(ds.active_upstairs().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with the same gen should be blocked
        let mut ds = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);
        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        let res = ds.promote_to_active(upstairs_connection_2, id2).await;
        assert!(res.is_err());

        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled()); // XXX should we cancel IO task here?

        assert_eq!(ds.active_upstairs().len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_different_uuid_lower_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's different
        // Upstairs) but with a lower gen should be blocked.
        let mut ds = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        println!("ds1: {:?}", ds);

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);
        ds.promote_to_active(upstairs_connection_1, id1).await?;
        println!("\nds2: {:?}\n", ds);

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        let res = ds.promote_to_active(upstairs_connection_2, id2).await;
        assert!(res.is_err());

        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled()); // XXX should we cancel IO task here?

        assert_eq!(ds.active_upstairs().len(), 1);

        // Original connection is still active.
        assert!(ds.is_active(upstairs_connection_1));
        // New connection was blocked.
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_same_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write (where it's the same
        // Upstairs but a different session) will block the "new" connection
        // if it has the same generation number.
        let mut ds = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);

        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        let res = ds.promote_to_active(upstairs_connection_2, id2).await;
        assert!(res.is_err());

        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled()); // XXX should we cancel IO task here?

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(!ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_write_same_uuid_larger_gen(
    ) -> Result<()> {
        // Attempting to activate multiple read-write where it's the same
        // Upstairs, but a different session, and with a larger generation
        // should allow the new connection to take over.
        let mut ds = build_test_downstairs(false).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 2,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);

        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        ds.promote_to_active(upstairs_connection_2, id2).await?;

        assert!(cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_different_uuid(
    ) -> Result<()> {
        // Activating multiple read-only with different Upstairs UUIDs should
        // work.
        let mut ds = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);

        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        ds.promote_to_active(upstairs_connection_2, id2).await?;
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        assert_eq!(ds.active_upstairs().len(), 2);

        assert!(ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_promote_to_active_multi_read_only_same_uuid() -> Result<()> {
        // Activating multiple read-only with the same Upstairs UUID should
        // kick out the other active one.
        let mut ds = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: upstairs_connection_1.upstairs_id,
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);

        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        ds.promote_to_active(upstairs_connection_2, id2).await?;

        assert!(cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        assert_eq!(ds.active_upstairs().len(), 1);

        assert!(!ds.is_active(upstairs_connection_1));
        assert!(ds.is_active(upstairs_connection_2));

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_read_only_no_job_id_collision() -> Result<()> {
        // Two read-only Upstairs shouldn't see each other's jobs
        let mut ds = build_test_downstairs(true).await?;

        let upstairs_connection_1 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let upstairs_connection_2 = UpstairsConnection {
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
        };

        let id1 = ConnectionId(1);
        let id2 = ConnectionId(2);
        let cancel1 = ds.add_fake_connection(upstairs_connection_1, id1);
        let cancel2 = ds.add_fake_connection(upstairs_connection_2, id2);

        ds.promote_to_active(upstairs_connection_1, id1).await?;

        assert_eq!(ds.active_upstairs().len(), 1);
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        ds.promote_to_active(upstairs_connection_2, id2).await?;
        assert!(!cancel1.is_cancelled());
        assert!(!cancel2.is_cancelled());

        assert_eq!(ds.active_upstairs().len(), 2);

        let read_1 = IOop::Read {
            dependencies: Vec::new(),
            requests: RegionReadRequest(vec![RegionReadReq {
                extent: ExtentId(0),
                offset: Block::new_512(1),
                count: NonZeroUsize::new(1).unwrap(),
            }]),
        };
        ds.active_mut(id1).add_work(JobId(1000), read_1.clone());

        let read_2 = IOop::Read {
            dependencies: Vec::new(),
            requests: RegionReadRequest(vec![RegionReadReq {
                extent: ExtentId(1),
                offset: Block::new_512(2),
                count: NonZeroUsize::new(1).unwrap(),
            }]),
        };
        ds.active_mut(id2).add_work(JobId(1000), read_2.clone());

        let work_1 = ds.active(id1).work.new_work();
        let work_2 = ds.active(id2).work.new_work();

        assert_eq!(work_1, work_2);

        let job_1 = ds.active(id1).work.get_job(JobId(1000));
        let job_2 = ds.active(id2).work.get_job(JobId(1000));

        assert_eq!(job_1.upstairs_connection, upstairs_connection_1);
        assert_eq!(job_1.ds_id, JobId(1000));
        assert_eq!(job_1.work, read_1);
        assert_eq!(job_1.state, WorkState::New);

        assert_eq!(job_2.upstairs_connection, upstairs_connection_2);
        assert_eq!(job_2.ds_id, JobId(1000));
        assert_eq!(job_2.work, read_2);
        assert_eq!(job_2.state, WorkState::New);

        Ok(())
    }

    /*
     * Test function that will start up a downstairs (at the provided port)
     * then create a tcp connection to that downstairs, returning the tcp
     * connection to the caller.
     */
    async fn start_ds_and_connect(
        listen_port: u16,
        repair_port: u16,
    ) -> Result<tokio::net::TcpStream> {
        /*
         * Pick some small enough values for what we need.
         */
        let bs = 512;
        let es = 4;
        let ec = 5;
        let dir = tempdir()?;

        let ds = create_test_downstairs(bs, es, ec, &dir).await?;
        let _jh = start_downstairs(
            ds,
            "127.0.0.1".parse().unwrap(),
            None,
            listen_port,
            repair_port,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let sock = TcpSocket::new_v4().unwrap();

        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            listen_port,
        );
        Ok(sock.connect(addr).await.unwrap())
    }

    #[tokio::test]
    async fn test_version_match() -> Result<()> {
        // A simple test to verify that sending the current crucible
        // message version to the downstairs will trigger a response
        // indicating the version is supported.
        let tcp = start_ds_and_connect(5555, 5556).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = MessageWriter::new(write);

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: Vec::new(),
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5556".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_downrev() -> Result<()> {
        // Test that a newer crucible version will result in a message
        // indicating there is a version mismatch.
        let tcp = start_ds_and_connect(5557, 5558).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = MessageWriter::new(write);

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION - 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION - 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_version_uprev_only() -> Result<()> {
        // Test sending only the +1 version to the DS, verify it rejects
        // this version as unsupported.
        let tcp = start_ds_and_connect(5579, 5560).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = MessageWriter::new(write);

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![CRUCIBLE_MESSAGE_VERSION + 1],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::VersionMismatch { version }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_compatible() -> Result<()> {
        // Test sending the +1 version to the DS, but also include the
        // current version on the supported list.  The downstairs should
        // see that and respond with the version it does support.
        let tcp = start_ds_and_connect(5561, 5562).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = MessageWriter::new(write);

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 1,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5562".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_version_uprev_list() -> Result<()> {
        // Test sending an invalid version to the DS, but also include the
        // current version on the supported list, but with several
        // choices.
        let tcp = start_ds_and_connect(5563, 5564).await.unwrap();
        let (read, write) = tcp.into_split();
        let mut fr = FramedRead::new(read, CrucibleDecoder::new());
        let mut fw = MessageWriter::new(write);

        // Our downstairs version is CRUCIBLE_MESSAGE_VERSION
        let m = Message::HereIAm {
            version: CRUCIBLE_MESSAGE_VERSION + 4,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 1,
            read_only: false,
            encrypted: false,
            alternate_versions: vec![
                CRUCIBLE_MESSAGE_VERSION - 1,
                CRUCIBLE_MESSAGE_VERSION,
                CRUCIBLE_MESSAGE_VERSION + 1,
            ],
        };
        fw.send(m).await?;

        let f = fr.next().await.unwrap();

        match f {
            Ok(Message::YesItsMe {
                version,
                repair_addr,
            }) => {
                assert_eq!(version, CRUCIBLE_MESSAGE_VERSION);
                assert_eq!(repair_addr, "127.0.0.1:5564".parse().unwrap());
            }
            x => {
                panic!("Invalid answer from downstairs: {:?}", x);
            }
        }
        Ok(())
    }

    #[test]
    fn decode_write() {
        use crucible_protocol::WriteBlockMetadata;
        let blocks = vec![WriteBlockMetadata {
            eid: ExtentId(0),
            offset: Block::new_512(0),
            block_context: BlockContext {
                hash: 123,
                encryption_context: None,
            },
        }];
        let data = Bytes::from(vec![1u8; 512]);
        let w = RegionWrite::new(&blocks, data).unwrap();
        assert_eq!(w.0.len(), 1);

        // Two contiguous blocks
        let blocks = vec![
            WriteBlockMetadata {
                eid: ExtentId(0),
                offset: Block::new_512(0),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
            WriteBlockMetadata {
                eid: ExtentId(0),
                offset: Block::new_512(1),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
        ];
        let data = Bytes::from(vec![1u8; 512 * 2]);
        let w = RegionWrite::new(&blocks, data).unwrap();
        assert_eq!(w.0.len(), 1);
        assert_eq!(w.0[0].extent, ExtentId(0));
        assert_eq!(w.0[0].write.offset, Block::new_512(0));
        assert_eq!(w.0[0].write.data.len(), 512 * 2);

        // Two non-contiguous blocks
        let blocks = vec![
            WriteBlockMetadata {
                eid: ExtentId(1),
                offset: Block::new_512(0),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
            WriteBlockMetadata {
                eid: ExtentId(2),
                offset: Block::new_512(1),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
        ];
        let data = Bytes::from(vec![1u8; 512 * 2]);
        let w = RegionWrite::new(&blocks, data).unwrap();
        assert_eq!(w.0.len(), 2);
        assert_eq!(w.0[0].extent, ExtentId(1));
        assert_eq!(w.0[0].write.offset, Block::new_512(0));
        assert_eq!(w.0[0].write.data.len(), 512);
        assert_eq!(w.0[1].extent, ExtentId(2));
        assert_eq!(w.0[1].write.offset, Block::new_512(1));
        assert_eq!(w.0[1].write.data.len(), 512);

        // Overlapping writes to the same block
        let blocks = vec![
            WriteBlockMetadata {
                eid: ExtentId(1),
                offset: Block::new_512(3),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
            WriteBlockMetadata {
                eid: ExtentId(1),
                offset: Block::new_512(3),
                block_context: BlockContext {
                    hash: 123,
                    encryption_context: None,
                },
            },
        ];
        let data = Bytes::from(vec![1u8; 512 * 2]);
        let w = RegionWrite::new(&blocks, data).unwrap();
        assert_eq!(w.0.len(), 2);
        assert_eq!(w.0[0].extent, ExtentId(1));
        assert_eq!(w.0[0].write.offset, Block::new_512(3));
        assert_eq!(w.0[0].write.data.len(), 512);
        assert_eq!(w.0[1].extent, ExtentId(1));
        assert_eq!(w.0[1].write.offset, Block::new_512(3));
        assert_eq!(w.0[1].write.data.len(), 512);

        // Mismatched block / data sizes
        let blocks = vec![WriteBlockMetadata {
            eid: ExtentId(1),
            offset: Block::new_512(3),
            block_context: BlockContext {
                hash: 123,
                encryption_context: None,
            },
        }];
        let data = Bytes::from(vec![1u8; 512 * 2]);
        let r = RegionWrite::new(&blocks, data);
        assert!(r.is_err());

        let data = Bytes::from(vec![1u8; 513]);
        let r = RegionWrite::new(&blocks, data);
        assert!(r.is_err());
    }
}
