// Copyright 2023 Oxide Computer Company
use crate::{
    cdt, crucible_bail,
    extent::{
        check_input, extent_path, DownstairsBlockContext, ExtentInner,
        EXTENT_META_RAW,
    },
    integrity_hash, mkdir_for_file,
    region::{BatchedPwritev, JobOrReconciliationId},
    Block, BlockContext, CrucibleError, JobId, ReadResponse, RegionDefinition,
};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use slog::{error, Logger};

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, IoSliceMut, Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::path::Path;

/// Equivalent to `DownstairsBlockContext`, but without one's own block number
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskDownstairsBlockContext {
    pub block_context: BlockContext,
    pub on_disk_hash: u64,
}

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
///
/// In particular, the `dirty` byte is first, so it's easy to read at a known
/// offset within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,
}

/// Size of backup data
///
/// This must be large enough to contain an `Option<DownstairsBlockContext>`
/// serialized using `bincode`.
pub const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 64;

/// Size of metadata region
///
/// This must be large enough to contain an `OnDiskMeta` serialized using
/// `bincode`.
pub const BLOCK_META_SIZE_BYTES: u64 = 64;

/// `RawInner` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file is structured as follows:
/// - Block data, structured as `block_size` × `extent_size`
/// - [`BLOCK_META_SIZE_BYTES`], which contains an [`OnDiskMeta`] serialized
///   using `bincode`.  The first byte of this range is `dirty`, serialized as a
///   `u8` (where `1` is dirty and `0` is clean).
/// - Block contexts (for encryption).  Each block index (in the range
///   `0..extent_size`) has two context slots; we use a ping-pong strategy when
///   writing to ensure that one slot is always valid.  Each slot is
///   [`BLOCK_CONTEXT_SLOT_SIZE_BYTES`] in size, so this region is
///   `BLOCK_CONTEXT_SLOT_SIZE_BYTES * extent_size * 2` bytes in total.  The
///   slots contain an `Option<OnDiskDownstairsBlockContext>`, serialized using
///   `bincode`.
#[derive(Debug)]
pub struct RawInner {
    file: File,

    /// Our extent number
    extent_number: u32,

    /// Extent size, in blocks
    extent_size: Block,

    /// Is the `A` or `B` context slot active, on a per-block basis?
    active_context: Vec<ContextSlot>,

    /// Local cache for the `dirty` value
    ///
    /// This allows us to only write the flag when the value changes
    dirty: bool,

    /// Monotonically increasing sync index
    ///
    /// This is unrelated to `flush_number` or `gen_number`; it's purely
    /// internal to this data structure and is transient.  We use this value to
    /// determine whether a context slot has been persisted to disk or not.
    sync_index: u64,

    /// The value of `sync_index` when the current value of a context slot was
    /// synched to disk.  When the value of a context slot changes, the value in
    /// this array is set to `self.sync_index + 1` indicates that the slot has
    /// not yet been synched.
    context_slot_synched_at: Vec<[u64; 2]>,
}

#[derive(Copy, Clone, Debug)]
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

    fn write(
        &mut self,
        job_id: JobId,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
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

        // If `only_write_written`, we need to skip writing to blocks that
        // already contain data. We'll first query the metadata to see which
        // blocks have hashes
        let mut writes_to_skip = HashSet::new();
        if only_write_unwritten {
            cdt::extent__write__get__hashes__start!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });
            let mut write_run_start = 0;
            while write_run_start < writes.len() {
                let first_write = writes[write_run_start];

                // Starting from the first write in the potential run, we scan
                // forward until we find a write with a block that isn't
                // contiguous with the request before it. Since we're counting
                // pairs, and the number of pairs is one less than the number of
                // writes, we need to add 1 to get our actual run length.
                let n_contiguous_writes = writes[write_run_start..]
                    .windows(2)
                    .take_while(|wr_pair| {
                        wr_pair[0].offset.value + 1 == wr_pair[1].offset.value
                    })
                    .count()
                    + 1;

                // Query hashes for the write range.
                let block_contexts = self.get_block_contexts(
                    first_write.offset.value,
                    n_contiguous_writes as u64,
                )?;

                for (i, block_contexts) in block_contexts.iter().enumerate() {
                    if block_contexts.is_some() {
                        let _ = writes_to_skip
                            .insert(i as u64 + first_write.offset.value);
                    }
                }

                write_run_start += n_contiguous_writes;
            }
            cdt::extent__write__get__hashes__done!(|| {
                (job_id.0, self.extent_number, writes.len() as u64)
            });

            if writes_to_skip.len() == writes.len() {
                // Nothing to do
                return Ok(());
            }
        }

        self.set_dirty()?;

        // Write all the metadata to the raw file, at the end
        //
        // TODO right now we're including the integrity_hash() time in the
        // measured time.  Is it small enough to be ignored?
        cdt::extent__write__raw__context__insert__start!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        let mut pending_slots = vec![];
        for write in writes {
            if writes_to_skip.contains(&write.offset.value) {
                pending_slots.push(None);
                continue;
            }

            // TODO it would be nice if we could profile what % of time we're
            // spending on hashes locally vs writing to disk
            let on_disk_hash = integrity_hash(&[&write.data[..]]);

            let next_slot =
                self.set_block_context(&DownstairsBlockContext {
                    block_context: write.block_context,
                    block: write.offset.value,
                    on_disk_hash,
                })?;
            pending_slots.push(Some(next_slot));
        }

        cdt::extent__write__raw__context__insert__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
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
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        let r = self.write_inner(writes, &writes_to_skip, iov_max);

        if r.is_err() {
            for write in writes.iter() {
                let block = write.offset.value;
                if !writes_to_skip.contains(&block) {
                    // Try to recompute the context slot from the file.  If this
                    // fails, then we _really_ can't recover, so bail out
                    // unceremoniously.
                    self.recompute_slot_from_file(block).unwrap();
                }
            }
        } else {
            // Now that writes have gone through, update active context slots
            for (write, new_slot) in writes.iter().zip(pending_slots) {
                if let Some(slot) = new_slot {
                    self.active_context[write.offset.value as usize] = slot;
                }
            }
        }

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number, writes.len() as u64)
        });

        Ok(())
    }

    fn read(
        &mut self,
        job_id: JobId,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // This code batches up operations for contiguous regions of
        // ReadRequests, so we can perform larger read syscalls queries. This
        // significantly improves read throughput.

        // Keep track of the index of the first request in any contiguous run
        // of requests. Of course, a "contiguous run" might just be a single
        // request.
        let mut req_run_start = 0;
        let block_size = self.extent_size.block_size_in_bytes();
        while req_run_start < requests.len() {
            let first_req = requests[req_run_start];

            // Starting from the first request in the potential run, we scan
            // forward until we find a request with a block that isn't
            // contiguous with the request before it. Since we're counting
            // pairs, and the number of pairs is one less than the number of
            // requests, we need to add 1 to get our actual run length.
            let mut n_contiguous_requests = 1;

            for request_window in requests[req_run_start..].windows(2) {
                if (request_window[0].offset.value + 1
                    == request_window[1].offset.value)
                    && ((n_contiguous_requests + 1) < iov_max)
                {
                    n_contiguous_requests += 1;
                } else {
                    break;
                }
            }

            // Create our responses and push them into the output. While we're
            // at it, check for overflows.
            let resp_run_start = responses.len();
            let mut iovecs = Vec::with_capacity(n_contiguous_requests);
            for req in requests[req_run_start..][..n_contiguous_requests].iter()
            {
                let resp = ReadResponse::from_request(req, block_size as usize);
                check_input(self.extent_size, req.offset, &resp.data)?;
                responses.push(resp);
            }

            // Create what amounts to an iovec for each response data buffer.
            for resp in
                &mut responses[resp_run_start..][..n_contiguous_requests]
            {
                iovecs.push(IoSliceMut::new(&mut resp.data[..]));
            }

            // Finally we get to read the actual data. That's why we're here
            cdt::extent__read__file__start!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            nix::sys::uio::preadv(
                self.file.as_raw_fd(),
                &mut iovecs,
                first_req.offset.value as i64 * block_size as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            cdt::extent__read__file__done!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            // Query the block metadata
            cdt::extent__read__get__contexts__start!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });
            let block_contexts = self.get_block_contexts(
                first_req.offset.value,
                n_contiguous_requests as u64,
            )?;
            cdt::extent__read__get__contexts__done!(|| {
                (job_id.0, self.extent_number, n_contiguous_requests as u64)
            });

            // Now it's time to put block contexts into the responses.
            // We use into_iter here to move values out of enc_ctxts/hashes,
            // avoiding a clone(). For code consistency, we use iters for the
            // response and data chunks too. These iters will be the same length
            // (equal to n_contiguous_requests) so zipping is fine
            let resp_iter =
                responses[resp_run_start..][..n_contiguous_requests].iter_mut();
            let ctx_iter = block_contexts.into_iter();

            for (resp, r_ctx) in resp_iter.zip(ctx_iter) {
                resp.block_contexts =
                    r_ctx.into_iter().map(|x| x.block_context).collect();
            }

            req_run_start += n_contiguous_requests;
        }

        Ok(())
    }

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        if !self.dirty()? {
            /*
             * If we have made no writes to this extent since the last flush,
             * we do not need to update the extent on disk
             */
            return Ok(());
        }

        // We put all of our metadata updates into a single write to make this
        // operation atomic.
        self.set_flush_number(new_flush, new_gen)?;

        // Now, we fsync to ensure data is flushed to disk.  It's okay to crash
        // before this point, because setting the flush number is atomic.
        cdt::extent__flush__file__start!(|| {
            (job_id.get(), self.extent_number, 0)
        });
        if let Err(e) = self.file.sync_all() {
            /*
             * XXX Retry?  Mark extent as broken?
             */
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure: {:?}",
                self.extent_number,
                e
            );
        }
        self.sync_index += 1;
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number, 0)
        });

        // Finally, reset the file's seek offset to 0
        self.file.seek(SeekFrom::Start(0))?;

        cdt::extent__flush__done!(|| { (job_id.get(), self.extent_number, 0) });

        Ok(())
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.set_dirty()?;
        let new_slot = self.set_block_context(block_context)?;
        self.active_context[block_context.block as usize] = new_slot;
        Ok(())
    }

    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>, CrucibleError> {
        let out = RawInner::get_block_contexts(self, block, count)?;
        Ok(out.into_iter().map(|v| v.into_iter().collect()).collect())
    }
}

impl RawInner {
    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
    ) -> Result<Self> {
        let path = extent_path(dir, extent_number);
        let bcount = def.extent_size().value;
        let size = def.block_size().checked_mul(bcount).unwrap()
            + BLOCK_META_SIZE_BYTES
            + BLOCK_CONTEXT_SLOT_SIZE_BYTES * bcount * 2;

        mkdir_for_file(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // All 0s are fine for everything except extent version in the metadata
        file.set_len(size)?;
        file.seek(SeekFrom::Start(0))?;
        let mut out = Self {
            file,
            dirty: false,
            extent_size: def.extent_size(),
            extent_number,
            active_context: vec![
                ContextSlot::A; // both slots are empty, so this is fine
                def.extent_size().value as usize
            ],
            context_slot_synched_at: vec![
                [0, 0];
                def.extent_size().value as usize
            ],
            sync_index: 0,
        };
        // Setting the flush number also writes the extent version, since
        // they're serialized together in the same block.
        out.set_flush_number(0, 0)?;

        // Sync the file to disk, to avoid any questions
        if let Err(e) = out.file.sync_all() {
            return Err(CrucibleError::IoError(format!(
                "extent {}: fsync 1 failure during initial sync: {e:?}",
                out.extent_number,
            ))
            .into());
        }
        Ok(out)
    }

    /// Constructs a new `Inner` object from files that already exist on disk
    pub fn open(
        path: &Path,
        def: &RegionDefinition,
        extent_number: u32,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self> {
        let extent_size = def.extent_size();
        let bcount = extent_size.value;
        let size = def.block_size().checked_mul(bcount).unwrap()
            + BLOCK_CONTEXT_SLOT_SIZE_BYTES * bcount * 2
            + BLOCK_META_SIZE_BYTES;

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file =
            match OpenOptions::new().read(true).write(!read_only).open(path) {
                Err(e) => {
                    error!(
                        log,
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                    bail!(
                        "Open of {path:?} for extent#{extent_number} \
                         returned: {e}",
                    );
                }
                Ok(f) => {
                    let cur_size = f.metadata().unwrap().len();
                    if size != cur_size {
                        bail!(
                            "File size {size:?} does not match \
                             expected {cur_size:?}",
                        );
                    }
                    f
                }
            };

        // Just in case, let's be very sure that the file on disk is what it
        // should be
        if !read_only {
            if let Err(e) = file.sync_all() {
                return Err(CrucibleError::IoError(format!(
                    "extent {extent_number}:
                 fsync 1 failure during initial rehash: {e:?}",
                ))
                .into());
            }
        }

        // Now, we'll compute which context slots are active in the file!  We
        // start by reading + hashing the file, then compare those hashes
        // against values in the context slots.  This is equivalent to
        // `recompute_slot_from_file`, but reads the file in bulk for
        // efficiency.

        // Buffer the file so we don't spend all day waiting on syscalls
        let mut file_buffered = BufReader::with_capacity(64 * 1024, &file);

        let block_hashes = {
            let mut block_hashes =
                Vec::with_capacity(extent_size.value as usize);

            // Stream the contents of the file and rehash them.
            let mut buf = vec![0; extent_size.block_size_in_bytes() as usize];
            for _block in 0..extent_size.value as usize {
                file_buffered.read_exact(&mut buf)?;
                block_hashes.push(integrity_hash(&[&buf]));
            }
            block_hashes
        };

        let dirty = {
            let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
            file_buffered.read_exact(&mut meta_buf)?;
            match meta_buf[0] {
                0 => false,
                1 => true,
                i => bail!("invalid dirty value: {i}"),
            }
        };

        // Now, read context data from the file and assign slots
        let active_context = {
            let mut buf = vec![0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
            let mut active_context = vec![];
            for (block, hash) in block_hashes.into_iter().enumerate() {
                let mut matching_slot = None;
                let mut empty_slot = None;
                for slot in [ContextSlot::A, ContextSlot::B] {
                    file_buffered.read_exact(&mut buf)?;
                    let context: Option<OnDiskDownstairsBlockContext> =
                        bincode::deserialize(&buf)?;
                    if let Some(context) = context {
                        if context.on_disk_hash == hash {
                            matching_slot = Some(slot);
                        }
                    } else if empty_slot.is_none() {
                        empty_slot = Some(slot);
                    }
                }
                let value = matching_slot.or(empty_slot).ok_or(
                    CrucibleError::IoError(format!(
                        "no slot found for {block}"
                    )),
                )?;
                active_context.push(value);
            }
            active_context
        };

        Ok(Self {
            file,
            // Lazy initialization of which context slot is active
            active_context,
            dirty,
            extent_number,
            extent_size: def.extent_size(),
            context_slot_synched_at: vec![
                [0, 0];
                def.extent_size().value as usize
            ],
            sync_index: 0,
        })
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        if !self.dirty {
            let offset = self.meta_offset();
            nix::sys::uio::pwrite(self.file.as_raw_fd(), &[1u8], offset as i64)
                .map_err(|e| CrucibleError::IoError(e.to_string()))?;
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
        self.file.seek(SeekFrom::Start(block_size as u64 * block))?;
        self.file.read_exact(&mut buf)?;
        let hash = integrity_hash(&[&buf]);

        // Then, read the slot data and decide if either slot
        // (1) is present and
        // (2) has a matching hash
        let mut buf = [0; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        let mut matching_slot = None;
        let mut empty_slot = None;
        for slot in [ContextSlot::A, ContextSlot::B] {
            // TODO: is `pread` faster than seek + read_exact?
            self.file
                .seek(SeekFrom::Start(self.context_slot_offset(block, slot)))?;
            self.file.read_exact(&mut buf)?;
            let context: Option<OnDiskDownstairsBlockContext> =
                bincode::deserialize(&buf).map_err(|e| {
                    CrucibleError::IoError(format!(
                        "context deserialization failed: {e:?}"
                    ))
                })?;
            if let Some(context) = context {
                if context.on_disk_hash == hash {
                    matching_slot = Some(slot);
                }
            } else if empty_slot.is_none() {
                empty_slot = Some(slot);
            }
        }
        let value = matching_slot.or(empty_slot).ok_or(
            CrucibleError::IoError(format!("no slot found for {block}")),
        )?;
        self.active_context[block as usize] = value;
        Ok(())
    }

    /// Writes the inactive block context slot
    ///
    /// Returns the new slot which should be marked as active after the write
    fn set_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<ContextSlot> {
        let block = block_context.block as usize;
        // Select the inactive slot
        let slot = !self.active_context[block];

        // If the context slot that we're about to write into hasn't been
        // synched to disk yet, we must sync it first.  This prevents subtle
        // ordering issues!
        let last_sync = &mut self.context_slot_synched_at[block][slot as usize];
        if *last_sync > self.sync_index {
            assert_eq!(*last_sync, self.sync_index + 1);
            if let Err(e) = self.file.sync_all() {
                return Err(CrucibleError::IoError(format!(
                    "extent {}: fsync 1 failure: {:?}",
                    self.extent_number, e
                ))
                .into());
            }
            self.sync_index += 1;
        }
        // The given slot is about to be newly unsynched, because we're going to
        // write to it below.
        *last_sync = self.sync_index + 1;

        let offset = self.context_slot_offset(block_context.block, slot);

        // Serialize into a local buffer, then write into the inactive slot
        let mut buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        let d = OnDiskDownstairsBlockContext {
            block_context: block_context.block_context,
            on_disk_hash: block_context.on_disk_hash,
        };
        bincode::serialize_into(buf.as_mut_slice(), &Some(d))?;
        nix::sys::uio::pwrite(self.file.as_raw_fd(), &buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

        // Return the just-written slot; it's the caller's responsibility to
        // select it as active once data is written.
        Ok(slot)
    }

    fn get_metadata(&self) -> Result<OnDiskMeta, CrucibleError> {
        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        let offset = self.meta_offset();
        nix::sys::uio::pread(self.file.as_raw_fd(), &mut buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        let out: OnDiskMeta = bincode::deserialize(&buf)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        Ok(out)
    }

    /// Returns the byte offset of the given context slot
    ///
    /// Contexts slots are located after block data in the extent file.  There
    /// are two context slots per block.  We use a ping-pong strategy to ensure
    /// that one of them is always valid (i.e. matching the data in the file).
    fn context_slot_offset(&self, block: u64, slot: ContextSlot) -> u64 {
        self.extent_size.block_size_in_bytes() as u64 * self.extent_size.value
            + BLOCK_META_SIZE_BYTES
            + (block * 2 + slot as u64) * BLOCK_CONTEXT_SLOT_SIZE_BYTES
    }

    /// Returns the byte offset of the metadata region
    ///
    /// The resulting offset points to serialized [`OnDiskMeta`] data.
    fn meta_offset(&self) -> u64 {
        Self::meta_offset_from_extent_size(self.extent_size)
    }

    fn meta_offset_from_extent_size(extent_size: Block) -> u64 {
        extent_size.block_size_in_bytes() as u64 * extent_size.value
    }

    fn get_block_context(
        &mut self,
        block: u64,
    ) -> Result<Option<DownstairsBlockContext>> {
        let slot = self.active_context[block as usize];
        let offset = self.context_slot_offset(block, slot);
        let mut buf = [0u8; BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
        nix::sys::uio::pread(self.file.as_raw_fd(), &mut buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        let out: Option<OnDiskDownstairsBlockContext> =
            bincode::deserialize(&buf)?;
        let out = out.map(|c| DownstairsBlockContext {
            block,
            block_context: c.block_context,
            on_disk_hash: c.on_disk_hash,
        });

        Ok(out)
    }

    /// Update the flush number, generation number, and clear the dirty bit
    fn set_flush_number(&mut self, new_flush: u64, new_gen: u64) -> Result<()> {
        let d = OnDiskMeta {
            dirty: false,
            flush_number: new_flush,
            gen_number: new_gen,
            ext_version: EXTENT_META_RAW,
        };
        // Byte 0 is the dirty byte
        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];

        bincode::serialize_into(buf.as_mut_slice(), &d)?;
        let offset = self.meta_offset();
        nix::sys::uio::pwrite(self.file.as_raw_fd(), &buf, offset as i64)
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        self.dirty = false;

        Ok(())
    }

    /// Returns the valid block contexts (or `None`) for the given block range
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>> {
        let mut out = vec![];
        for i in block..block + count {
            let ctx = self.get_block_context(i)?;
            out.push(ctx);
        }
        Ok(out)
    }

    fn write_inner(
        &self,
        writes: &[&crucible_protocol::Write],
        writes_to_skip: &HashSet<u64>,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        // Now, batch writes into iovecs and use pwritev to write them all out.
        let mut batched_pwritev = BatchedPwritev::new(
            self.file.as_raw_fd(),
            writes.len(),
            self.extent_size.block_size_in_bytes().into(),
            iov_max,
        );

        for write in writes {
            if !writes_to_skip.contains(&write.offset.value) {
                batched_pwritev.add_write(write)?;
            }
        }

        // Write any remaining data
        batched_pwritev.perform_writes()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use crucible_protocol::EncryptionContext;
    use crucible_protocol::ReadRequest;
    use rand::Rng;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[tokio::test]
    async fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_none());
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

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

        let ctxs = inner.get_block_contexts(0, 1)?;
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

        // Block 1 should still be blank
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

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

        let ctxs = inner.get_block_contexts(0, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();

        // Second context was appended
        assert_eq!(ctx.block_context.encryption_context.unwrap().nonce, blob1);
        assert_eq!(ctx.block_context.encryption_context.unwrap().tag, blob2);
        assert_eq!(ctx.block_context.hash, 1024);
        assert_eq!(ctx.on_disk_hash, 65536);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_none());
        assert!(inner.get_block_contexts(1, 1)?[0].is_none());

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
        let ctxs = inner.get_block_contexts(0, 1)?;
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

        // Verify block 1's context
        let ctxs = inner.get_block_contexts(1, 1)?;
        let ctx = ctxs[0].as_ref().unwrap();

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
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;

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
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(20), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            inner.read(JobId(21), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should not get back our data, because block 0 was written.
            assert_ne!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(0),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        // But, writing to the second block still should!
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(1),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(30), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            };
            inner.read(JobId(31), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(1),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        Ok(())
    }

    #[test]
    fn test_auto_sync() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        // The context should be written to slot 0
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 0);

        // The context should be written to slot 1
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 0);

        // The context should be written to slot 0, forcing a sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 1);

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        // The context should be written to slot 0
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 0);

        // Flush, which should bump the sync number (marking slot 0 as synched)
        inner.flush(12, 12, JobId(11).into())?;

        // The context should be written to slot 1
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 1);

        // The context should be written to slot 0
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 1);

        // The context should be written to slot 1, forcing a sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 2);

        Ok(())
    }

    #[test]
    fn test_auto_sync_flush_2() -> Result<()> {
        let dir = tempdir()?;
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = crucible_protocol::Write {
            eid: 0,
            offset: Block::new_512(0),
            data,
            block_context: BlockContext {
                encryption_context: None,
                hash,
            },
        };
        // The context should be written to slot 0
        inner.write(JobId(10), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 0);

        // The context should be written to slot 1
        inner.write(JobId(11), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 0);

        // Flush, which should bump the sync number (marking slot 0 as synched)
        inner.flush(12, 12, JobId(11).into())?;

        // The context should be written to slot 0
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 1);

        // The context should be written to slot 1
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 1);

        // The context should be written to slot 0, forcing a sync
        inner.write(JobId(12), &[&write], false, IOV_MAX_TEST)?;
        assert_eq!(inner.sync_index, 2);

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
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
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
        let mut inner =
            RawInner::create(dir.as_ref(), &new_region_definition(), 0)
                .unwrap();

        // Writing to block 0 should succeed with only_write_unwritten
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.clone(),
                block_context,
            };
            inner.write(JobId(30), &[&write], true, IOV_MAX_TEST)?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            inner.read(JobId(31), &[&read], &mut resp, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(
                resp,
                vec![ReadResponse {
                    eid: 0,
                    offset: Block::new_512(0),
                    data: BytesMut::from(data.as_ref()),
                    block_contexts: vec![block_context]
                }]
            );
        }

        Ok(())
    }
}
