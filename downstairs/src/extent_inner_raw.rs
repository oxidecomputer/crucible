// Copyright 2023 Oxide Computer Company
use crate::{
    cdt,
    extent::{
        check_input, extent_path, DownstairsBlockContext, ExtentInner,
        EXTENT_META_RAW,
    },
    extent_inner_raw_common::{
        pread_all, pwrite_all, OnDiskMeta, BLOCK_META_SIZE_BYTES,
    },
    integrity_hash, mkdir_for_file,
    region::JobOrReconciliationId,
    Block, BlockContext, CrucibleError, ExtentReadRequest, ExtentReadResponse,
    ExtentWrite, JobId, RegionDefinition,
};
use crucible_common::ExtentId;
use crucible_protocol::ReadBlockContext;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use slog::{error, Logger};

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read};
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;

/// Equivalent to `DownstairsBlockContext`, but without one's own block number
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OnDiskDownstairsBlockContext {
    block_context: BlockContext,
    on_disk_hash: u64,
}

/// Size of backup data
///
/// This must be large enough to fit an `Option<OnDiskDownstairsBlockContext>`
/// serialized using `bincode`.
const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 48;

/// Number of extra syscalls per read / write that triggers defragmentation
const DEFRAGMENT_THRESHOLD: u64 = 3;

/// `RawInner` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file is structured as follows:
/// - Block data, structured as `block_size` × `extent_size`
/// - Block contexts (for encryption).  There are two arrays of context slots,
///   each containing `extent_size` elements (i.e. one slot for each block).
///   Each slot is [`BLOCK_CONTEXT_SLOT_SIZE_BYTES`] in size, so this section of
///   the file is `BLOCK_CONTEXT_SLOT_SIZE_BYTES * extent_size * 2` bytes in
///   total.  The slots contain an `Option<OnDiskDownstairsBlockContext>`,
///   serialized using `bincode`.
/// - Active context slots, stored as a bit-packed array (where 0 is
///   [`ContextSlot::A`] and 1 is [`ContextSlot::B`]).  This array contains
///   `extent_size.div_ceil(8)` bytes.  It is only valid when the `dirty` bit is
///   cleared.  This is an optimization that speeds up opening a clean extent
///   file; otherwise, we would have to rehash every block to find the active
///   context slot.
/// - [`BLOCK_META_SIZE_BYTES`], which contains an [`OnDiskMeta`] serialized
///   using `bincode`.  The first byte of this range is `dirty`, serialized as a
///   `u8` (where `1` is dirty and `0` is clean).
///
/// There are a few considerations that led to this particular ordering:
/// - Active context slots and metadata must be contiguous, because we want to
///   write them atomically when clearing the `dirty` flag
/// - The metadata contains an extent version (currently [`EXTENT_META_RAW`]).
///   We will eventually have multiple raw file formats, so it's convenient to
///   always place the metadata at the end; this lets us deserialize it without
///   knowing anything else about the file, then dispatch based on extent
///   version.
#[derive(Debug)]
pub struct RawInner {
    file: File,

    /// Our extent number
    extent_number: ExtentId,

    /// Extent size, in blocks
    extent_size: Block,

    /// Helper `struct` controlling layout within the file
    layout: RawLayout,

    /// Is the `A` or `B` context slot active, on a per-block basis?
    active_context: Vec<ContextSlot>,

    /// Local cache for the `dirty` value
    ///
    /// This allows us to only write the flag when the value changes
    dirty: bool,

    /// Marks whether the given context slot is dirty
    ///
    /// A dirty context slot has not yet been saved to disk, and must be
    /// synched before being overwritten.
    context_slot_dirty: ContextSlotsDirty,

    /// Total number of extra syscalls due to context slot fragmentation
    extra_syscall_count: u64,

    /// Denominator corresponding to `extra_syscall_count`
    extra_syscall_denominator: u64,
}

/// Data structure containing bitpacked dirty context slot flags
///
/// Each block has two flags, independently tracking the dirty state of context
/// slots A and B.
#[derive(Debug)]
struct ContextSlotsDirty(Vec<u8>);

impl ContextSlotsDirty {
    fn new(block_count: u64) -> Self {
        Self(vec![0u8; block_count as usize])
    }

    /// Checks whether the given `(block, slot)` tuple is dirty
    #[must_use]
    fn get(&self, block: u64, slot: ContextSlot) -> bool {
        let mask = 1 << (slot as usize);
        (self.0[block as usize] & mask) != 0
    }

    /// Sets the given `(block, slot)` tuple as dirty
    fn set(&mut self, block: u64, slot: ContextSlot) {
        self.0[block as usize] |= 1 << slot as usize;
    }

    /// Marks every slot as not dirty
    fn reset(&mut self) {
        self.0.fill(0);
    }
}

#[cfg(test)]
impl std::ops::Index<u64> for ContextSlotsDirty {
    type Output = u8;
    fn index(&self, block: u64) -> &Self::Output {
        &self.0[block as usize]
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ContextSlot {
    A,
    B,
}

impl std::ops::Not for ContextSlot {
    type Output = Self;
    fn not(self) -> Self {
        match self {
            ContextSlot::A => ContextSlot::B,
            ContextSlot::B => ContextSlot::A,
        }
    }
}

impl ExtentInner for RawInner {
    fn flush_number(&self) -> Result<u64, CrucibleError> {
        self.get_metadata().map(|v| v.flush_number)
    }

    fn gen_number(&self) -> Result<u64, CrucibleError> {
        self.get_metadata().map(|v| v.gen_number)
    }

    fn dirty(&self) -> Result<bool, CrucibleError> {
        Ok(self.dirty)
    }

    /// Performs a single write within this extent
    fn write(
        &mut self,
        job_id: JobId,
        write: &ExtentWrite,
        only_write_unwritten: bool,
        _iov_max: usize,
    ) -> Result<(), CrucibleError> {
        check_input(self.extent_size, write.offset, write.data.len())?;
        /*
         * In order to be crash consistent, perform the following steps in
         * order:
         *
         * 1) set the dirty bit
         * 2) for each write:
         *   a) write out encryption context and hashes first
         *   b) write out extent data second
         *
         * If encryption context is written after the extent data, a crash or
         * interruption before extent data is written would potentially leave
         * data on the disk that cannot be decrypted.
         *
         * If hash is written after extent data, same thing - a crash or
         * interruption would leave data on disk that would fail the
         * integrity hash check.
         *
         * Note that writing extent data here does not assume that it is
         * durably on disk - the only guarantee of that is returning
         * ok from fsync. The data is only potentially on disk and
         * this depends on operating system implementation.
         *
         * To minimize the performance hit of sending many transactions to the
         * filesystem, as much as possible is written at the same time. This
         * means multiple loops are required. The steps now look like:
         *
         * 1) set the dirty bit
         * 2) gather and write all encryption contexts + hashes
         * 3) write all extent data
         *
         * If "only_write_unwritten" is true, then we only issue a write for
         * a block if that block has not been written to yet.  Note
         * that we can have a write that is "sparse" if the range of
         * blocks it contains has a mix of written an unwritten
         * blocks.
         *
         * We define a block being written to or not has if that block has
         * `Some(...)` with a matching checksum serialized into a context slot
         * or not. So it is required that a written block has a checksum.
         */

        let num_blocks = write.block_contexts.len() as u64;
        let block_size = self.extent_size.block_size_in_bytes() as u64;
        // If `only_write_written`, we need to skip writing to blocks that
        // already contain data. We'll first query the metadata to see which
        // blocks have hashes
        let mut writes_to_skip = HashSet::new();
        if only_write_unwritten {
            cdt::extent__write__get__hashes__start!(|| {
                (job_id.0, self.extent_number.0, num_blocks)
            });

            // Query hashes for the write range.
            let block_contexts =
                self.get_block_contexts(write.offset.0, num_blocks)?;

            for (i, block_contexts) in block_contexts.iter().enumerate() {
                if block_contexts.is_some() {
                    writes_to_skip.insert(i);
                }
            }

            cdt::extent__write__get__hashes__done!(|| {
                (job_id.0, self.extent_number.0, num_blocks)
            });

            if writes_to_skip.len() == write.block_contexts.len() {
                // Nothing to do
                return Ok(());
            }
        }

        self.set_dirty()?;

        // Write all the context data to the raw file
        //
        // TODO right now we're including the integrity_hash() time in the
        // measured time.  Is it small enough to be ignored?
        cdt::extent__write__raw__context__insert__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // Compute block contexts, then write them to disk
        let block_ctx: Vec<_> = write
            .block_contexts
            .iter()
            .enumerate()
            .filter(|(i, _ctx)| !writes_to_skip.contains(i))
            .map(|(i, ctx)| {
                // TODO it would be nice if we could profile what % of time we're
                // spending on hashes locally vs writing to disk
                let chunk = &write.data[i * block_size as usize..]
                    [..block_size as usize];
                let on_disk_hash = integrity_hash(&[chunk]);

                DownstairsBlockContext {
                    block_context: *ctx,
                    block: write.offset.0 + i as u64,
                    on_disk_hash,
                }
            })
            .collect();

        self.set_block_contexts(&block_ctx)?;

        cdt::extent__write__raw__context__insert__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // PERFORMANCE TODO:
        //
        // Something worth considering for small writes is that, based on
        // my memory of conversations we had with propolis folks about what
        // OSes expect out of an NVMe driver, I believe our contract with the
        // upstairs doesn't require us to have the writes inside the file
        // until after a flush() returns. If that is indeed true, we could
        // buffer a certain amount of writes, only actually writing that
        // buffer when either a flush is issued or the buffer exceeds some
        // set size (based on our memory constraints). This would have
        // benefits on any workload that frequently writes to the same block
        // between flushes, would have benefits for small contiguous writes
        // issued over multiple write commands by letting us batch them into
        // a larger write, and (speculation) may benefit non-contiguous writes
        // by cutting down the number of metadata writes. But, it introduces
        // complexity. The time spent implementing that would probably better be
        // spent switching to aio or something like that.
        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        let r = self.write_inner(write, &writes_to_skip);

        if r.is_err() {
            for i in 0..write.block_contexts.len() {
                if !writes_to_skip.contains(&i) {
                    // Try to recompute the context slot from the file.  If this
                    // fails, then we _really_ can't recover, so bail out
                    // unceremoniously.
                    let block = write.offset.0 + i as u64;
                    self.recompute_slot_from_file(block).unwrap();
                }
            }
        } else {
            // Now that writes have gone through, update active context slots
            for i in 0..write.block_contexts.len() {
                if !writes_to_skip.contains(&i) {
                    // We always write to the inactive slot, so just swap it
                    let block = write.offset.0 as usize + i;
                    self.active_context[block] = !self.active_context[block];
                }
            }
        }

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        Ok(())
    }

    fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
        _iov_max: usize, // unused by raw backend
    ) -> Result<ExtentReadResponse, CrucibleError> {
        let mut buf = req.data;
        let block_size = self.extent_size.block_size_in_bytes() as u64;
        let num_blocks = buf.capacity() as u64 / block_size;
        check_input(self.extent_size, req.offset, buf.capacity())?;

        // Query the block metadata
        cdt::extent__read__get__contexts__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });
        let blocks = self.get_block_contexts_inner(
            req.offset.0,
            num_blocks,
            |ctx, _block| match ctx {
                None => ReadBlockContext::Empty,
                Some(c) => match c.block_context.encryption_context {
                    Some(ctx) => ReadBlockContext::Encrypted { ctx },
                    None => ReadBlockContext::Unencrypted {
                        hash: c.block_context.hash,
                    },
                },
            },
        )?;
        cdt::extent__read__get__contexts__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // To avoid a `memset`, we're reading directly into uninitialized
        // memory in the buffer.  This is fine; we sized the buffer
        // appropriately in advance (and will panic here if we messed up).
        assert!(buf.is_empty());

        // Finally we get to read the actual data. That's why we're here
        cdt::extent__read__file__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // SAFETY: the buffer has sufficient capacity, and this is a valid
        // file descriptor.
        let expected_bytes = buf.capacity();
        let r = unsafe {
            libc::pread(
                self.file.as_raw_fd(),
                buf.spare_capacity_mut().as_mut_ptr() as *mut libc::c_void,
                expected_bytes as libc::size_t,
                req.offset.0 as i64 * block_size as i64,
            )
        };
        // Check against the expected number of bytes.  We could do more
        // robust error handling here (e.g. retrying in a loop), but for
        // now, simply bailing out seems wise.
        let r = nix::errno::Errno::result(r).map(|r| r as usize);
        let num_bytes = r.map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: read failed: {e}",
                self.extent_number
            ))
        })?;
        if num_bytes != expected_bytes {
            return Err(CrucibleError::IoError(format!(
                "extent {}: incomplete read \
                 (expected {expected_bytes}, got {num_bytes})",
                self.extent_number
            )));
        }
        // SAFETY: we just initialized this chunk of the buffer
        unsafe {
            buf.set_len(expected_bytes);
        }

        cdt::extent__read__file__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        Ok(ExtentReadResponse { data: buf, blocks })
    }

    fn pre_flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        cdt::extent__flush__start!(|| {
            (job_id.get(), self.extent_number.0, 0)
        });

        // We put all of our metadata updates into a single write to make this
        // operation atomic.
        self.set_flush_number(new_flush, new_gen)?;

        Ok(())
    }

    fn flush_inner(
        &mut self,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        // Now, we fsync to ensure data is flushed to disk.  It's okay to crash
        // before this point, because setting the flush number is atomic.
        cdt::extent__flush__file__start!(|| {
            (job_id.get(), self.extent_number.0)
        });
        if let Err(e) = self.file.sync_all() {
            /*
             * XXX Retry?  Mark extent as broken?
             */
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure: {e:?}",
                self.extent_number,
            )));
        }
        self.context_slot_dirty.reset();
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number.0)
        });
        Ok(())
    }

    fn post_flush(
        &mut self,
        _new_flush: u64,
        _new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        // Check for fragmentation in the context slots leading to worse
        // performance, and defragment if that's the case.
        let extra_syscalls_per_rw = self
            .extra_syscall_count
            .checked_div(self.extra_syscall_denominator)
            .unwrap_or(0);
        self.extra_syscall_count = 0;
        self.extra_syscall_denominator = 0;
        let r = if extra_syscalls_per_rw > DEFRAGMENT_THRESHOLD {
            self.defragment()
        } else {
            Ok(())
        };

        cdt::extent__flush__done!(|| { (job_id.get(), self.extent_number.0) });

        r
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.set_dirty()?;
        self.set_block_contexts(&[*block_context])?;
        self.active_context[block_context.block as usize] =
            !self.active_context[block_context.block as usize];
        Ok(())
    }

    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>, CrucibleError> {
        RawInner::get_block_contexts(self, block, count)
    }
}

impl RawInner {
    /// Imports context and metadata
    ///
    /// Returns a buffer that must be appended to raw block data to form the
    /// full raw extent file.
    pub fn import(
        file: &mut File,
        def: &RegionDefinition,
        ctxs: Vec<Option<DownstairsBlockContext>>,
        dirty: bool,
        flush_number: u64,
        gen_number: u64,
    ) -> Result<(), CrucibleError> {
        let layout = RawLayout::new(def.extent_size());
        let block_count = layout.block_count() as usize;
        assert_eq!(block_count, def.extent_size().value as usize);
        assert_eq!(block_count, ctxs.len());

        file.set_len(layout.file_size())?;
        layout.write_context_slots_contiguous(
            file,
            0,
            ctxs.iter().map(Option::as_ref),
            ContextSlot::A,
        )?;
        layout.write_context_slots_contiguous(
            file,
            0,
            std::iter::repeat(None).take(block_count),
            ContextSlot::B,
        )?;

        layout.write_active_context_and_metadata(
            file,
            vec![ContextSlot::A; block_count].as_slice(),
            dirty,
            flush_number,
            gen_number,
        )?;

        Ok(())
    }

    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size);
        let size = layout.file_size();

        mkdir_for_file(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // All 0s are fine for everything except extent version in the metadata
        file.set_len(size)?;
        let mut out = Self {
            file,
            dirty: false,
            extent_size,
            layout,
            extent_number,
            active_context: vec![
                ContextSlot::A; // both slots are empty, so this is fine
                def.extent_size().value as usize
            ],
            context_slot_dirty: ContextSlotsDirty::new(extent_size.value),
            extra_syscall_count: 0,
            extra_syscall_denominator: 0,
        };
        // Setting the flush number also writes the extent version, since
        // they're serialized together in the same block.
        out.set_flush_number(0, 0)?;

        // Sync the file to disk, to avoid any questions
        if let Err(e) = out.file.sync_all() {
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure during initial sync: {e}",
                out.extent_number,
            )));
        }
        Ok(out)
    }

    /// Constructs a new `Inner` object from files that already exist on disk
    pub fn open(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size);
        let size = layout.file_size();

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file =
            match OpenOptions::new().read(true).write(!read_only).open(&path) {
                Err(e) => {
                    error!(
                        log,
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                    return Err(CrucibleError::IoError(format!(
                        "extent {extent_number}: open of {path:?} failed: {e}",
                    )));
                }
                Ok(f) => {
                    let cur_size = f.metadata().unwrap().len();
                    if size != cur_size {
                        return Err(CrucibleError::IoError(format!(
                            "extent {extent_number}: file size {cur_size:?} \
                             does not match expected {size:?}",
                        )));
                    }
                    f
                }
            };

        // Just in case, let's be very sure that the file on disk is what it
        // should be
        if !read_only {
            if let Err(e) = file.sync_all() {
                return Err(CrucibleError::IoError(format!(
                    "extent {extent_number}: \
                     fsync 1 failure during initial rehash: {e}",
                )));
            }
        }

        let layout = RawLayout::new(def.extent_size());
        let meta = layout.get_metadata(&file)?;

        // If the file is dirty, then we have to recompute which context slot is
        // active for every block.  This is slow, but can't be avoided; we
        // closed the file without a flush so we can't be confident about the
        // data that was on disk.
        let active_context = if !meta.dirty {
            // Easy case first: if it's **not** dirty, then just assign active
            // slots based on the bitpacked active context buffer from the file.
            layout.get_active_contexts(&file)?
        } else {
            // Otherwise, read block-size chunks and check hashes against
            // both context slots, looking for a match.
            let ctx_a = layout.read_context_slots_contiguous(
                &file,
                0,
                layout.block_count(),
                ContextSlot::A,
            )?;
            let ctx_b = layout.read_context_slots_contiguous(
                &file,
                0,
                layout.block_count(),
                ContextSlot::B,
            )?;

            // Now that we've read the context slot arrays, read file data and
            // figure out which context slot is active.
            let mut file_buffered = BufReader::with_capacity(64 * 1024, &file);
            let mut active_context = vec![];
            let mut buf = vec![0; extent_size.block_size_in_bytes() as usize];
            let mut last_seek_block = 0;
            for (block, (context_a, context_b)) in
                ctx_a.into_iter().zip(ctx_b).enumerate()
            {
                let slot = if context_a.is_none() && context_b.is_none() {
                    // Small optimization: if both context slots are empty, the
                    // block must also be empty (and we don't need to read +
                    // hash it, saving a little time)
                    //
                    // Note that if `context_a == context_b` but they are both
                    // `Some(..)`, we still want to read + hash the block to
                    // make sure that it matches. Otherwise, someone has managed
                    // to corrupt our extent file on disk, which is Bad News.
                    ContextSlot::A
                } else {
                    // Otherwise, we have to compute hashes from the file.
                    if block != last_seek_block {
                        file_buffered.seek_relative(
                            (block - last_seek_block) as i64
                                * extent_size.block_size_in_bytes() as i64,
                        )?;
                    }
                    file_buffered.read_exact(&mut buf)?;
                    last_seek_block = block + 1; // since we just read a block
                    let hash = integrity_hash(&[&buf]);

                    let mut matching_slot = None;
                    let mut empty_slot = None;

                    for slot in [ContextSlot::A, ContextSlot::B] {
                        let context = [context_a, context_b][slot as usize];
                        if let Some(context) = context {
                            if context.on_disk_hash == hash {
                                matching_slot = Some(slot);
                            }
                        } else if empty_slot.is_none() {
                            empty_slot = Some(slot);
                        }
                    }

                    matching_slot.or(empty_slot).ok_or(
                        CrucibleError::MissingContextSlot(block as u64),
                    )?
                };
                active_context.push(slot);
            }
            active_context
        };

        Ok(Self {
            file,
            active_context,
            dirty: meta.dirty,
            extent_number,
            extent_size: def.extent_size(),
            layout: RawLayout::new(def.extent_size()),
            context_slot_dirty: ContextSlotsDirty::new(def.extent_size().value),
            extra_syscall_count: 0,
            extra_syscall_denominator: 0,
        })
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        if !self.dirty {
            self.layout.set_dirty(&self.file)?;
            self.dirty = true;
        }
        Ok(())
    }

    /// Updates `self.active_context[block]` based on data read from the file
    ///
    /// This returns an error if neither context slot matches the block data,
    /// which should never happen (even during error conditions or after a
    /// crash).
    ///
    /// We expect to call this function rarely, so it does not attempt to
    /// minimize the number of syscalls it executes.
    fn recompute_slot_from_file(
        &mut self,
        block: u64,
    ) -> Result<(), CrucibleError> {
        // Read the block data itself:
        let block_size = self.extent_size.block_size_in_bytes();
        let mut buf = vec![0; block_size as usize];
        pread_all(
            self.file.as_fd(),
            &mut buf,
            (block_size as u64 * block) as i64,
        )
        .map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: reading block {block} data failed: {e}",
                self.extent_number
            ))
        })?;
        let hash = integrity_hash(&[&buf]);

        // Then, read the slot data and decide if either slot
        // (1) is present and
        // (2) has a matching hash
        let mut matching_slot = None;
        let mut empty_slot = None;
        for slot in [ContextSlot::A, ContextSlot::B] {
            // Read a single context slot, which is by definition contiguous
            let mut context = self
                .layout
                .read_context_slots_contiguous(&self.file, block, 1, slot)?;
            assert_eq!(context.len(), 1);
            let context = context.pop().unwrap();

            if let Some(context) = context {
                if context.on_disk_hash == hash {
                    matching_slot = Some(slot);
                }
            } else if empty_slot.is_none() {
                empty_slot = Some(slot);
            }
        }
        let value = matching_slot
            .or(empty_slot)
            .ok_or(CrucibleError::MissingContextSlot(block))?;
        self.active_context[block as usize] = value;
        Ok(())
    }

    fn set_block_contexts(
        &mut self,
        block_contexts: &[DownstairsBlockContext],
    ) -> Result<(), CrucibleError> {
        // If any of these block contexts will be overwriting an unsynched
        // context slot, then we insert a sync here.
        let needs_sync = block_contexts.iter().any(|block_context| {
            let block = block_context.block as usize;
            // We'll be writing to the inactive slot
            let slot = !self.active_context[block];
            self.context_slot_dirty.get(block_context.block, slot)
        });
        if needs_sync {
            self.file.sync_all().map_err(|e| {
                CrucibleError::IoError(format!(
                    "extent {}: fsync 1 failure: {e}",
                    self.extent_number,
                ))
            })?;
            self.context_slot_dirty.reset();
        }
        // Mark the to-be-written slots as unsynched on disk
        //
        // It's harmless if we bail out before writing the actual context slot
        // here, because all it will do is force a sync next time this is called
        // (that sync is right above here!)
        for block_context in block_contexts {
            let block = block_context.block as usize;
            let slot = !self.active_context[block];
            self.context_slot_dirty.set(block_context.block, slot);
        }

        let mut start = 0;
        let mut write_count = 0;
        for i in 0..block_contexts.len() {
            if i + 1 == block_contexts.len()
                || block_contexts[i].block + 1 != block_contexts[i + 1].block
            {
                write_count += self.set_block_contexts_contiguous(
                    &block_contexts[start..=i],
                )?;
                start = i + 1;
            }
        }
        cdt::extent__set__block__contexts__write__count!(|| (
            self.extent_number.0,
            write_count,
        ));
        Ok(())
    }

    /// Efficiently sets block contexts in bulk
    ///
    /// Returns the number of writes, for profiling
    ///
    /// # Panics
    /// `block_contexts` must represent a contiguous set of blocks
    fn set_block_contexts_contiguous(
        &mut self,
        block_contexts: &[DownstairsBlockContext],
    ) -> Result<u64, CrucibleError> {
        for (a, b) in block_contexts.iter().zip(block_contexts.iter().skip(1)) {
            assert_eq!(a.block + 1, b.block, "blocks must be contiguous");
        }

        let mut writes = 0u64;
        for (slot, group) in block_contexts
            .iter()
            .chunk_by(|block_context| {
                // We'll be writing to the inactive slot
                !self.active_context[block_context.block as usize]
            })
            .into_iter()
        {
            let mut group = group.peekable();
            let start = group.peek().unwrap().block;
            self.layout.write_context_slots_contiguous(
                &self.file,
                start,
                group.map(Option::Some),
                slot,
            )?;
            writes += 1;
        }
        if let Some(writes) = writes.checked_sub(1) {
            self.extra_syscall_count += writes;
            self.extra_syscall_denominator += 1;
        }

        Ok(writes)
    }

    fn get_metadata(&self) -> Result<OnDiskMeta, CrucibleError> {
        self.layout.get_metadata(&self.file)
    }

    /// Update the flush number, generation number, and clear the dirty bit
    fn set_flush_number(
        &mut self,
        new_flush: u64,
        new_gen: u64,
    ) -> Result<(), CrucibleError> {
        self.layout.write_active_context_and_metadata(
            &self.file,
            &self.active_context,
            false, // dirty
            new_flush,
            new_gen,
        )?;
        self.dirty = false;
        Ok(())
    }

    /// Returns the valid block contexts (or `None`) for the given block range
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>, CrucibleError> {
        self.get_block_contexts_inner(block, count, |ctx, block| {
            ctx.map(|c| DownstairsBlockContext {
                block,
                block_context: c.block_context,
                on_disk_hash: c.on_disk_hash,
            })
        })
    }

    /// Maps a function across block contexts, return a `Vec<T>`
    fn get_block_contexts_inner<F, T>(
        &mut self,
        block: u64,
        count: u64,
        f: F,
    ) -> Result<Vec<T>, CrucibleError>
    where
        F: Fn(Option<OnDiskDownstairsBlockContext>, u64) -> T,
    {
        let mut out = Vec::with_capacity(count as usize);
        let mut reads = 0u64;
        for (slot, group) in (block..block + count)
            .chunk_by(|block| self.active_context[*block as usize])
            .into_iter()
        {
            let mut group = group.peekable();
            let start = *group.peek().unwrap();
            let count = group.count();
            self.layout.read_context_slots_contiguous_inner(
                &self.file,
                start,
                count as u64,
                slot,
                &f,
                &mut out,
            )?;
            reads += 1;
        }
        if let Some(reads) = reads.checked_sub(1) {
            self.extra_syscall_count += reads;
            self.extra_syscall_denominator += 1;
        }

        Ok(out)
    }

    fn write_inner(
        &self,
        write: &ExtentWrite,
        writes_to_skip: &HashSet<usize>,
    ) -> Result<(), CrucibleError> {
        // Perform writes, which may be broken up by skipped blocks
        let block_size = self.extent_size.block_size_in_bytes() as u64;
        for (skip, mut group) in (0..write.block_contexts.len())
            .chunk_by(|i| writes_to_skip.contains(i))
            .into_iter()
        {
            if skip {
                continue;
            }
            let start = group.next().unwrap();
            let count = group.count() + 1;

            let data = &write.data[start * block_size as usize..]
                [..count * block_size as usize];
            let start_block = write.offset.0 + start as u64;

            pwrite_all(
                self.file.as_fd(),
                data,
                (start_block * block_size) as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        }
        Ok(())
    }

    /// Helper function to get a single block context
    ///
    /// This is inefficient and should only be used in unit tests
    #[cfg(test)]
    fn get_block_context(
        &mut self,
        block: u64,
    ) -> Result<Option<DownstairsBlockContext>, CrucibleError> {
        let mut out = self.get_block_contexts(block, 1)?;
        assert_eq!(out.len(), 1);
        Ok(out.pop().unwrap())
    }

    /// Consolidates context slots into either the A or B array
    ///
    /// This must only be run directly after the file is synced to disk
    fn defragment(&mut self) -> Result<(), CrucibleError> {
        // At this point, the active context slots (on a per-block basis)
        // may be scattered across the two arrays:
        //
        // block   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | ... |
        // context | A | A |   |   |   |   | A | A | A | ... | [A array]
        //         |   |   | B | B | B | B |   |   |   | ... | [B array]
        //
        // This can be inefficient, because it means that a write would have to
        // be split into updating multiple regions (instead of a single
        // contiguous write).  As such, if the context slots disagree, we
        // "defragment" them:
        //
        // - Figure out whether A or B is more popular
        // - Copy context data from the less-popular slot to the more-popular
        //
        // In the example above, `A` is more popular so we would perform the
        // following copy:
        //
        // block   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | ... |
        // context | A | A | ^ | ^ | ^ | ^ | A | A | A | ... | [A array]
        //         |   |   | B | B | B | B |   |   |   | ... | [B array]
        //
        // This is safe because it occurs immediately after a flush, so we are
        // dead certain that the active context matches file contents.  This
        // means that we can safely overwrite the old (inactive) context.
        //
        // We track the number of A vs B slots, as well as the range covered by
        // the slots.  It's that range that we'll need to read + write, so we
        // want to pick whichever slot does less work.
        assert!(!self.dirty); // This can only be called after a flush!

        #[derive(Copy, Clone)]
        struct Counter {
            count: usize,
            min_block: u64,
            max_block: u64,
        }
        let mut a_count = Counter {
            count: 0,
            min_block: u64::MAX,
            max_block: 0,
        };
        let mut b_count = a_count;
        for (i, s) in self.active_context.iter().enumerate() {
            let count = match s {
                ContextSlot::A => &mut a_count,
                ContextSlot::B => &mut b_count,
            };
            count.count += 1;
            count.min_block = count.min_block.min(i as u64);
            count.max_block = count.max_block.max(i as u64);
        }
        if a_count.count == 0 || b_count.count == 0 {
            return Ok(());
        }

        let (copy_from, counter) = if a_count.count < b_count.count {
            (ContextSlot::A, a_count)
        } else {
            (ContextSlot::B, b_count)
        };
        assert!(counter.count > 0);
        assert!(counter.max_block >= counter.min_block);
        let num_slots = counter.max_block + 1 - counter.min_block;

        // Read the source context slots from the file
        let source_slots = self.layout.read_context_slots_contiguous(
            &self.file,
            counter.min_block,
            num_slots,
            copy_from,
        )?;
        let mut dest_slots = self.layout.read_context_slots_contiguous(
            &self.file,
            counter.min_block,
            num_slots,
            !copy_from,
        )?;

        // Selectively overwrite dest with source context slots
        for (i, block) in (counter.min_block..=counter.max_block).enumerate() {
            let block = block as usize;
            if self.active_context[block] == copy_from {
                dest_slots[i] = source_slots[i];

                // Mark this slot as unsynched, so that we don't overwrite
                // it later on without a sync
                self.context_slot_dirty.set(block as u64, !copy_from);
            }
        }
        let r = self.layout.write_context_slots_contiguous(
            &self.file,
            counter.min_block,
            dest_slots.iter().map(|v| v.as_ref()),
            !copy_from,
        );

        // If this write failed, then try recomputing every context slot
        // within the unknown range
        if r.is_err() {
            for block in counter.min_block..=counter.max_block {
                self.recompute_slot_from_file(block).unwrap();
            }
        } else {
            for block in counter.min_block..=counter.max_block {
                self.active_context[block as usize] = !copy_from;
            }
            // At this point, the `dirty` bit is not set, but values in
            // `self.active_context` disagree with the active context slot
            // stored in the file.  Normally, this is bad: if we crashed and
            // reloaded the file from disk right at this moment, we'd end up
            // with different values in `self.active_context`.  In this case,
            // though, it's okay: the values that have changed have the **same
            // data** in both context slots, so it would still be a valid state
            // for the file.
        }
        r.map(|_| ())
    }
}

/// Data structure that implements the on-disk layout of a raw extent file
struct RawLayout {
    extent_size: Block,
}

impl std::fmt::Debug for RawLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawLayout")
            .field("extent_size", &self.extent_size)
            .finish()
    }
}

impl RawLayout {
    fn new(extent_size: Block) -> Self {
        RawLayout { extent_size }
    }

    /// Sets the dirty flag in the file true
    ///
    /// This unconditionally writes to the file; to avoid extra syscalls, it
    /// would be wise to cache this at a higher level and only write if it has
    /// changed.
    fn set_dirty(&self, file: &File) -> Result<(), CrucibleError> {
        let offset = self.metadata_offset();
        pwrite_all(file.as_fd(), &[1u8], offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing dirty byte failed: {e}",))
        })?;
        Ok(())
    }

    /// Returns the total size of the raw data file
    ///
    /// This includes block data, context slots, active slot array, and metadata
    fn file_size(&self) -> u64 {
        let block_count = self.block_count();
        self.block_size().checked_mul(block_count).unwrap()
            + BLOCK_META_SIZE_BYTES
            + block_count.div_ceil(8)
            + BLOCK_CONTEXT_SLOT_SIZE_BYTES * block_count * 2
    }

    /// Returns the beginning of supplementary data in the file
    fn supplementary_data_offset(&self) -> u64 {
        self.block_count() * self.block_size()
    }

    /// Returns the byte offset of the given context slot in the file
    ///
    /// Contexts slots are located after block data in the extent file. There
    /// are two context slots arrays, each of which contains one context slot
    /// per block.  We use a ping-pong strategy to ensure that one of them is
    /// always valid (i.e. matching the data in the file).
    fn context_slot_offset(&self, block: u64, slot: ContextSlot) -> u64 {
        self.supplementary_data_offset()
            + (self.block_count() * slot as u64 + block)
                * BLOCK_CONTEXT_SLOT_SIZE_BYTES
    }

    /// Number of blocks in the extent file
    fn block_count(&self) -> u64 {
        self.extent_size.value
    }

    /// Returns the byte offset of the `active_context` bitpacked array
    fn active_context_offset(&self) -> u64 {
        self.supplementary_data_offset()
            + self.block_count() * 2 * BLOCK_CONTEXT_SLOT_SIZE_BYTES
    }

    fn active_context_size(&self) -> u64 {
        self.block_count().div_ceil(8)
    }

    fn metadata_offset(&self) -> u64 {
        self.active_context_offset() + self.active_context_size()
    }

    /// Number of bytes in each block
    fn block_size(&self) -> u64 {
        self.extent_size.block_size_in_bytes() as u64
    }

    fn get_metadata(&self, file: &File) -> Result<OnDiskMeta, CrucibleError> {
        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        let offset = self.metadata_offset();
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("reading metadata failed: {e}"))
        })?;
        let out: OnDiskMeta = bincode::deserialize(&buf)
            .map_err(|e| CrucibleError::BadMetadata(e.to_string()))?;
        Ok(out)
    }

    fn write_context_slots_contiguous<'a, I>(
        &self,
        file: &File,
        block_start: u64,
        iter: I,
        slot: ContextSlot,
    ) -> Result<(), CrucibleError>
    where
        I: Iterator<Item = Option<&'a DownstairsBlockContext>>,
    {
        let mut buf = vec![];

        for block_context in iter {
            let n = buf.len();
            buf.extend([0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize]);
            let d = block_context.map(|b| OnDiskDownstairsBlockContext {
                block_context: b.block_context,
                on_disk_hash: b.on_disk_hash,
            });
            bincode::serialize_into(&mut buf[n..], &d).unwrap();
        }
        let offset = self.context_slot_offset(block_start, slot);
        pwrite_all(file.as_fd(), &buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing context slots failed: {e}"))
        })?;
        Ok(())
    }

    fn read_context_slots_contiguous(
        &self,
        file: &File,
        block_start: u64,
        block_count: u64,
        slot: ContextSlot,
    ) -> Result<Vec<Option<DownstairsBlockContext>>, CrucibleError> {
        let mut out = Vec::with_capacity(block_count as usize);
        self.read_context_slots_contiguous_inner(
            file,
            block_start,
            block_count,
            slot,
            |ctx, block| {
                ctx.map(|c| DownstairsBlockContext {
                    block,
                    block_context: c.block_context,
                    on_disk_hash: c.on_disk_hash,
                })
            },
            &mut out,
        )?;
        Ok(out)
    }

    /// Low-level function to read context slots
    ///
    /// This function takes a generic transform function and writes to a
    /// user-provided array, to minimize allocations.
    fn read_context_slots_contiguous_inner<F, T>(
        &self,
        file: &File,
        block_start: u64,
        block_count: u64,
        slot: ContextSlot,
        f: F,
        out: &mut Vec<T>,
    ) -> Result<(), CrucibleError>
    where
        F: Fn(Option<OnDiskDownstairsBlockContext>, u64) -> T,
    {
        let mut buf =
            vec![0u8; (BLOCK_CONTEXT_SLOT_SIZE_BYTES * block_count) as usize];

        let offset = self.context_slot_offset(block_start, slot);
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("reading context slots failed: {e}"))
        })?;

        for (i, chunk) in buf
            .chunks_exact(BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize)
            .enumerate()
        {
            let ctx: Option<OnDiskDownstairsBlockContext> =
                bincode::deserialize(chunk).map_err(|e| {
                    CrucibleError::BadContextSlot(e.to_string())
                })?;
            let v = f(ctx, block_start + i as u64);
            out.push(v);
        }
        Ok(())
    }

    /// Write out the active context array and metadata section of the file
    ///
    /// This is done in a single write, so it should be atomic.
    ///
    /// # Panics
    /// `active_context.len()` must match `self.block_count()`, and the function
    /// will panic otherwise.
    fn write_active_context_and_metadata(
        &self,
        file: &File,
        active_context: &[ContextSlot],
        dirty: bool,
        flush_number: u64,
        gen_number: u64,
    ) -> Result<(), CrucibleError> {
        assert_eq!(active_context.len(), self.block_count() as usize);

        // Serialize bitpacked active slot values
        let mut buf = vec![];
        for c in active_context.chunks(8) {
            let mut v = 0;
            for (i, slot) in c.iter().enumerate() {
                v |= (*slot as u8) << i;
            }
            buf.push(v);
        }

        let d = OnDiskMeta {
            dirty,
            flush_number,
            gen_number,
            ext_version: EXTENT_META_RAW,
        };
        let mut meta = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta.as_mut_slice(), &d).unwrap();
        buf.extend(meta);

        let offset = self.active_context_offset();

        pwrite_all(file.as_fd(), &buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing metadata failed: {e}"))
        })?;

        Ok(())
    }

    /// Decodes the active contexts from the given file
    ///
    /// The file descriptor offset is not changed by this function
    fn get_active_contexts(
        &self,
        file: &File,
    ) -> Result<Vec<ContextSlot>, CrucibleError> {
        let mut buf = vec![0u8; self.active_context_size() as usize];
        let offset = self.active_context_offset();
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!(
                "could not read active contexts: {e}"
            ))
        })?;

        let mut active_context = vec![];
        for bit in buf
            .iter()
            .flat_map(|b| (0..8).map(move |i| b & (1 << i) == 0))
            .take(self.block_count() as usize)
        {
            // Unpack bits from each byte
            active_context.push(if bit {
                ContextSlot::A
            } else {
                ContextSlot::B
            });
        }
        assert_eq!(active_context.len(), self.block_count() as usize);
        Ok(active_context)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use crucible_common::BlockOffset;
    use crucible_protocol::EncryptionContext;
    use rand::Rng;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[test]
    fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_context(0)?.is_none());
        assert!(inner.get_block_context(1)?.is_none());

        // Set and verify block 0's context
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                    tag: [
                        4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                        19,
                    ],
                }),
                hash: 123,
            },
            block: 0,
            on_disk_hash: 456,
        })?;

        let ctx = inner.get_block_context(0)?.unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        // Block 1 should still be blank
        assert!(inner.get_block_context(1)?.is_none());

        // Set and verify a new context for block 0
        let blob1 = rand::thread_rng().gen::<[u8; 12]>();
        let blob2 = rand::thread_rng().gen::<[u8; 16]>();

        // Set and verify block 0's context
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: blob1,
                    tag: blob2,
                }),
                hash: 1024,
            },
            block: 0,
            on_disk_hash: 65536,
        })?;

        let ctx = inner.get_block_context(0)?.unwrap();

        // Second context was appended
        assert_eq!(ctx.block_context.encryption_context.unwrap().nonce, blob1);
        assert_eq!(ctx.block_context.encryption_context.unwrap().tag, blob2);
        assert_eq!(ctx.block_context.hash, 1024);
        assert_eq!(ctx.on_disk_hash, 65536);

        Ok(())
    }

    #[test]
    fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_context(0)?.is_none());
        assert!(inner.get_block_context(1)?.is_none());

        // Set block 0's and 1's context and dirty flag
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                    tag: [
                        4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                        19,
                    ],
                }),
                hash: 123,
            },
            block: 0,
            on_disk_hash: 456,
        })?;
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    tag: [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                }),
                hash: 9999,
            },
            block: 1,
            on_disk_hash: 1234567890,
        })?;

        // Verify block 0's context
        let ctx = inner.get_block_context(0)?.unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        // Verify block 1's context
        let ctx = inner.get_block_context(1)?.unwrap();

        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctx.block_context.hash, 9999);
        assert_eq!(ctx.on_disk_hash, 1234567890);

        // Return both block 0's and block 1's context, and verify

        let ctxs = inner.get_block_contexts(0, 2)?;
        let ctx = ctxs[0].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        let ctx = ctxs[1].as_ref().unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctx.block_context.encryption_context.unwrap().tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctx.block_context.hash, 9999);
        assert_eq!(ctx.on_disk_hash, 1234567890);

        // Append a whole bunch of block context rows
        for i in 0..10 {
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: rand::thread_rng().gen::<[u8; 12]>(),
                        tag: rand::thread_rng().gen::<[u8; 16]>(),
                    }),
                    hash: rand::thread_rng().gen::<u64>(),
                },
                block: 0,
                on_disk_hash: i,
            })?;
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: rand::thread_rng().gen::<[u8; 12]>(),
                        tag: rand::thread_rng().gen::<[u8; 16]>(),
                    }),
                    hash: rand::thread_rng().gen::<u64>(),
                },
                block: 1,
                on_disk_hash: i,
            })?;
        }

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert!(ctxs[0].is_some());
        assert_eq!(ctxs[0].as_ref().unwrap().on_disk_hash, 9);

        assert!(ctxs[1].is_some());
        assert_eq!(ctxs[1].as_ref().unwrap().on_disk_hash, 9);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_without_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };
        inner.write(JobId(10), &write, false, IOV_MAX_TEST)?;
        let prev_hash = hash;

        // The context should be in place, though we haven't flushed yet

        // Therefore, we expect that write_unwritten to the first block won't
        // do anything.
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = ExtentWrite {
                offset: BlockOffset(0),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(20), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: BlockOffset(0),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(21), read, IOV_MAX_TEST)?;

            // We should get back our old data, because block 0 was written.
            assert_eq!(
                resp.blocks,
                vec![ReadBlockContext::Unencrypted { hash: prev_hash }]
            );
            assert_ne!(resp.data, BytesMut::from(data.as_ref()));
        }

        // But, writing to the second block still should!
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = ExtentWrite {
                offset: BlockOffset(1),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(30), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: BlockOffset(1),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(31), read, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp.blocks,
                vec![ReadBlockContext::Unencrypted { hash }]
            );
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        Ok(())
    }

    #[test]
    fn test_auto_sync() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };

        // Write block 1, so that we can notice when a sync happens.  The write
        // should go to slot B.
        let write1 = ExtentWrite {
            offset: BlockOffset(1),
            ..write.clone()
        };
        inner.write(JobId(10), &write1, false, IOV_MAX_TEST)?;
        assert_eq!(inner.context_slot_dirty[0], 0b00);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[1], 0b10);
        assert_eq!(inner.active_context[1], ContextSlot::B);

        // The context should be written to block 0, slot B
        inner.write(JobId(10), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.context_slot_dirty[0], 0b10);
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[1], 0b10); // unchanged
        assert_eq!(inner.active_context[1], ContextSlot::B); // unchanged

        // The context should be written to block 0, slot A
        inner.write(JobId(11), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.context_slot_dirty[0], 0b11);
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[1], 0b10); // unchanged
        assert_eq!(inner.active_context[1], ContextSlot::B); // unchanged

        // The context should be written to slot B, forcing a sync
        inner.write(JobId(12), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.context_slot_dirty[0], 0b10);
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[1], 0b00);
        assert_eq!(inner.active_context[1], ContextSlot::B); // unchanged

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };
        // The context should be written to slot B
        inner.write(JobId(10), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b10);

        // Flush!  This will mark all slots as synched
        inner.flush(12, 12, JobId(11).into())?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b00);

        // The context should be written to slot A
        inner.write(JobId(11), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[0], 0b01);

        // The context should be written to slot B
        inner.write(JobId(12), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b11);

        // The context should be written to slot A, forcing a sync
        inner.write(JobId(12), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[0], 0b01);

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush_2() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };
        // The context should be written to slot B
        inner.write(JobId(10), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b10);

        // The context should be written to slot A
        inner.write(JobId(11), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[0], 0b11);

        // Flush!  This will mark all slots as synched
        inner.flush(12, 12, JobId(11).into())?;
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[0], 0b00);

        // The context should be written to slot B
        inner.write(JobId(12), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b10);

        // The context should be written to slot A
        inner.write(JobId(12), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::A);
        assert_eq!(inner.context_slot_dirty[0], 0b11);

        // The context should be written to slot B, forcing a sync
        inner.write(JobId(11), &write, false, IOV_MAX_TEST)?;
        assert_eq!(inner.active_context[0], ContextSlot::B);
        assert_eq!(inner.context_slot_dirty[0], 0b10);

        Ok(())
    }

    /// If a write successfully put a context into a context slot, but it never
    /// actually got the data onto the disk, that block should revert back to
    /// being "unwritten". After all, the data never was truly written.
    ///
    /// This test is very similar to test_region_open_removes_partial_writes.
    #[test]
    fn test_reopen_marks_blocks_unwritten_if_data_never_hit_disk() -> Result<()>
    {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Partial write, the data never hits disk, but there's a context
        // in the DB and the dirty flag is set.
        inner.set_dirty_and_block_context(&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: None,
                hash: 1024,
            },
            block: 0,
            on_disk_hash: 65536,
        })?;
        drop(inner);

        // Reopen, which should note that the hash doesn't match on-disk values
        // and decide that block 0 is unwritten.
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Writing to block 0 should succeed with only_write_unwritten
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = ExtentWrite {
                offset: BlockOffset(0),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(30), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: BlockOffset(0),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(31), read, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp.blocks,
                vec![ReadBlockContext::Unencrypted { hash }]
            );
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        Ok(())
    }

    #[test]
    fn test_defragment_full() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write every other block, so that the active context slot alternates
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        for i in 0..5 {
            let write = ExtentWrite {
                offset: BlockOffset(i * 2),
                data: data.clone(),
                block_contexts: vec![BlockContext {
                    encryption_context: None,
                    hash,
                }],
            };
            inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        }

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }
        assert_eq!(inner.extra_syscall_count, 0);
        assert_eq!(inner.extra_syscall_denominator, 5);
        inner.flush(10, 10, JobId(10).into())?;
        assert!(inner.context_slot_dirty.0.iter().all(|v| *v == 0));

        // This should not have changed active context slots!
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }

        // Now, do one big write, which will be forced to bounce between the
        // context slots.
        let data = Bytes::from(vec![0x33; 512 * 10]);
        let block_contexts = data
            .chunks(512)
            .map(|chunk| BlockContext {
                encryption_context: None,
                hash: integrity_hash(&[chunk]),
            })
            .collect();
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts,
        };
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;

        // This write has toggled every single context slot
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 {
                    ContextSlot::A
                } else {
                    ContextSlot::B
                },
                "invalid context slot at {i}",
            );
        }
        assert!(inner.extra_syscall_count > 0);
        assert_eq!(inner.extra_syscall_denominator, 1);

        // Do a flush!  Because we had a bunch of extra syscalls, this should
        // trigger defragmentation; after the flush, every context slot should
        // be in array A.
        inner.flush(11, 11, JobId(11).into())?;

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                ContextSlot::A,
                "invalid context slot at {i}",
            );
        }

        Ok(())
    }

    #[test]
    fn test_defragment_into_b() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write every other block, so that the active context slot alternates
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        for i in 0..3 {
            let write = ExtentWrite {
                offset: BlockOffset(i * 2),
                data: data.clone(),
                block_contexts: vec![BlockContext {
                    encryption_context: None,
                    hash,
                }],
            };
            inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        }
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // B | A | B | A | B | A | A | A | A | A

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }
        assert_eq!(inner.extra_syscall_count, 0);
        assert_eq!(inner.extra_syscall_denominator, 3);
        inner.flush(10, 10, JobId(10).into())?;

        // This should not have changed active context slots!
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }

        // Now, do one big write, which will be forced to bounce between the
        // context slots.
        let data = Bytes::from(vec![0x33; 512 * 10]);
        let block_contexts = data
            .chunks(512)
            .map(|chunk| BlockContext {
                encryption_context: None,
                hash: integrity_hash(&[chunk]),
            })
            .collect();
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts,
        };
        // This write should toggled every single context slot:
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // A | B | A | B | A | B | B | B | B | B
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::A
                } else {
                    ContextSlot::B
                },
                "invalid context slot at {i}",
            );
        }
        assert!(inner.extra_syscall_count > 0);
        assert_eq!(inner.extra_syscall_denominator, 1);

        // Do a flush!  Because we had a bunch of extra syscalls, this should
        // trigger defragmentation; after the flush, every context slot should
        // be in array B (which minimizes copies)
        inner.flush(11, 11, JobId(11).into())?;

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                ContextSlot::B,
                "invalid context slot at {i}",
            );
        }

        Ok(())
    }

    #[test]
    fn test_defragment_into_a() -> Result<()> {
        // Identical to `test_defragment_a`, except that we force the
        // defragmentation to copy into the A slots
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write every other block, so that the active context slot alternates
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        for i in 0..3 {
            let write = ExtentWrite {
                offset: BlockOffset(i * 2),
                data: data.clone(),
                block_contexts: vec![BlockContext {
                    encryption_context: None,
                    hash,
                }],
            };
            inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        }
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // B | A | B | A | B | A | A | A | A | A

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }
        assert_eq!(inner.extra_syscall_count, 0);
        assert_eq!(inner.extra_syscall_denominator, 3);
        inner.flush(10, 10, JobId(10).into())?;

        // This should not have changed active context slots!
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }

        // Now, do two big writes, which will be forced to bounce between the
        // context slots.
        let data = Bytes::from(vec![0x33; 512 * 10]);
        let block_contexts = data
            .chunks(512)
            .map(|chunk| BlockContext {
                encryption_context: None,
                hash: integrity_hash(&[chunk]),
            })
            .collect();
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts,
        };
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        // This write should toggled every single context slot:
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // B | A | B | A | B | A | A | A | A | A
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 4 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                },
                "invalid context slot at {i}",
            );
        }
        assert!(inner.extra_syscall_count > 0);
        assert_eq!(inner.extra_syscall_denominator, 2);

        // Do a flush!  Because we had a bunch of extra syscalls, this should
        // trigger defragmentation; after the flush, every context slot should
        // be in array A (which minimizes copies)
        inner.flush(11, 11, JobId(11).into())?;

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                ContextSlot::A,
                "invalid context slot at {i}",
            );
        }

        Ok(())
    }

    #[test]
    fn test_defragment_not_enough() -> Result<()> {
        // Identical to `test_defragment_a`, except that we force the
        // defragmentation to copy into the A slots
        let dir = tempdir()?;
        let mut inner = RawInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write every other block, so that the active context slot alternates
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        for i in 0..2 {
            let write = ExtentWrite {
                offset: BlockOffset(i * 2),
                data: data.clone(),
                block_contexts: vec![BlockContext {
                    encryption_context: None,
                    hash,
                }],
            };
            inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        }
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // B | A | B | A | A | A | A | A | A | A

        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 2 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }
        assert_eq!(inner.extra_syscall_count, 0);
        assert_eq!(inner.extra_syscall_denominator, 2);
        inner.flush(10, 10, JobId(10).into())?;

        // This should not have changed active context slots!
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 2 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                }
            );
        }

        // Now, do two big writes, which will be forced to bounce between the
        // context slots.
        let data = Bytes::from(vec![0x33; 512 * 10]);
        let block_contexts = data
            .chunks(512)
            .map(|chunk| BlockContext {
                encryption_context: None,
                hash: integrity_hash(&[chunk]),
            })
            .collect();
        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts,
        };
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        inner.write(JobId(30), &write, false, IOV_MAX_TEST)?;
        // This write should toggled every single context slot:
        // 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9
        // --|---|---|---|---|---|---|---|---|---
        // B | A | B | A | A | A | A | A | A | A
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 2 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                },
                "invalid context slot at {i}",
            );
        }
        assert!(inner.extra_syscall_count > 0);
        assert_eq!(inner.extra_syscall_denominator, 2);

        // This write didn't add enough extra syscalls to trigger
        // defragmentation.
        inner.flush(11, 11, JobId(11).into())?;

        // These should be the same!
        for i in 0..10 {
            assert_eq!(
                inner.active_context[i],
                if i % 2 == 0 && i <= 2 {
                    ContextSlot::B
                } else {
                    ContextSlot::A
                },
                "invalid context slot at {i}",
            );
        }

        Ok(())
    }

    #[test]
    fn test_serialized_context_size() {
        let c = OnDiskDownstairsBlockContext {
            block_context: BlockContext {
                hash: u64::MAX,
                encryption_context: Some(EncryptionContext {
                    nonce: [0xFF; 12],
                    tag: [0xFF; 16],
                }),
            },
            on_disk_hash: u64::MAX,
        };
        let mut ctx_buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        bincode::serialize_into(ctx_buf.as_mut_slice(), &Some(c)).unwrap();
    }
}
