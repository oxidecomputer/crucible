// Copyright 2024 Oxide Computer Company
use crate::{
    cdt,
    extent::{check_input, extent_path, ExtentInner, EXTENT_META_RAW_V2},
    extent_inner_raw_common::{
        pread_all, pwrite_all, OnDiskMeta, BLOCK_META_SIZE_BYTES,
    },
    mkdir_for_file,
    region::JobOrReconciliationId,
    Block, CrucibleError, ExtentReadRequest, ExtentReadResponse, ExtentWrite,
    JobId, RegionDefinition,
};

use crucible_common::{BlockOffset, ExtentId};
use crucible_protocol::ReadBlockContext;
use slog::{error, Logger};

use std::io::{BufReader, Read};
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;
use std::{
    fs::{File, OpenOptions},
    io::IoSlice,
};
use zerocopy::AsBytes;

/// Recordsize for non-ZFS systems (matching the ZFS default)
pub(crate) const DUMMY_RECORDSIZE: u64 = 128 * 1024;

/// Max of serialized block context (which is a [`ReadBlockContext`])
pub(crate) const BLOCK_CONTEXT_SIZE_BYTES: u64 = 32;

/// `RawInnerV2` is a wrapper around a [`std::fs::File`] representing an extent
///
/// The file contains [`extent_size`](RawInnerV2::extent_size) blocks, which I'm
/// writing as `N` for simplicity here.
///
/// # File organization
/// The file is structured in four parts.  Getting specific offsets within the
/// file is implemented in the [`RawLayout`] helper class.
///
/// ## Block data and contexts
/// Block data and per-block contexts are paired up as follows:
/// ```text
/// [ ----- block  ----- | context ] x R
/// [ --- padding to recordsize --- ]
/// [ ----- block  ----- | context ] x R
/// [ --- padding to recordsize --- ]
/// [ ----- block  ----- | context ] x R
/// [ --- padding to recordsize --- ]
/// ```
///
/// where `R` is the recordsize / (block + context size)
///
/// We don't want to split blocks or block-context pairs across ZFS records,
/// because that could make writes non-atomic.
///
/// ## Expected recordsize
/// After the block data, we store a single `u64` representing the expected
/// recordsize when the file was written.  When the file is reopened, we detect
/// if its recordsize has changed, which would be surprising!
///
/// ## Written blocks array
/// The next section of the file contains a bit-packed array indicating whether
/// each block is written (where 0 is unwritten and 1 is written).  It takes up
/// `N.div_ceil(8)` bytes.  It is only valid when the `dirty` bit is cleared.
/// This is an optimization that speeds up opening a clean extent file;
/// otherwise, we would have to read every block to find whether it has been
/// written or not.
///
/// ## File metadata
/// The last [`BLOCK_META_SIZE_BYTES`] in the file contain an [`OnDiskMeta`]
/// serialized using `bincode`.  The first byte of this range is `dirty`,
/// serialized as a `u8` (where `1` is dirty and `0` is clean).
///
/// There are a few considerations that led to this particular ordering:
/// - The written blocks array and metadata must be contiguous, because we want
///   to write them atomically when clearing the `dirty` flag.
/// - We have multiple different raw file formats, but they all place an
///   [`OnDiskMeta`] in the last [`BLOCK_META_SIZE_BYTES`] bytes of the file.
///   This means we can read the metadata and pick the correct extent version.
///
/// # Safety
/// This raw file format relies heavily on ZFS for guarantees.  Specifically, it
/// relies on the following properties:
///
/// - Data will not be corrupted on-disk (enforced by ZFS's checksums)
/// - A single `pwritev` call which only touches a single ZFS record is
///   guaranteed to be all-or-nothing; it cannot be split or partially land.
///
/// The latter is not obvious, and requires reading the ZFS source code; see
/// [https://rfd.shared.oxide.computer/rfd/490#_designing_for_crash_consistency](RFD 492)
/// for a more detailed analysis.
///
/// To enforce all-or-nothing writes, data which must land together cannot span
/// multiple ZFS records.  This means that our file layout is dependent on
/// recordsize (see [`RawLayout`]).  In addition, we store the expected
/// recordsize in the file and check it when reopening.
///
/// Changing the dataset's recordsize (or copying an extent file to a dataset
/// with a different recordsize) will produce an error when loading the extent
/// file.
#[derive(Debug)]
pub struct RawInnerV2 {
    file: File,

    /// Our extent number
    extent_number: ExtentId,

    /// Extent size, in blocks
    extent_size: Block,

    /// Helper `struct` controlling layout within the file
    layout: RawLayout,

    /// Has this block been written?
    block_written: Vec<bool>,

    /// Local cache for the `dirty` value
    ///
    /// This allows us to only write the flag when the value changes
    dirty: bool,
}

impl ExtentInner for RawInnerV2 {
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
        write: &ExtentWrite,
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        check_input(self.extent_size, write.offset, write.data.len())?;
        let n_blocks = write.block_contexts.len();

        let start_block = write.offset;
        let block_size = self.extent_size.block_size_in_bytes();
        let mut block = start_block;

        struct WriteChunk<'a> {
            offset: BlockOffset,
            iovecs: Vec<IoSlice<'a>>,
        }
        let mut iovecs = vec![];

        let ctxs = write
            .block_contexts
            .iter()
            .map(|ctx| {
                let mut buf = [0u8; BLOCK_CONTEXT_SIZE_BYTES as usize];
                let ctx = ReadBlockContext::from(*ctx);
                bincode::serialize_into(buf.as_mut_slice(), &ctx).unwrap();
                buf
            })
            .collect::<Vec<_>>();

        let padding = vec![0u8; self.layout.padding_size() as usize];
        let mut expected_bytes = 0;
        let mut start_new_chunk = true;
        for (data, ctx) in write.data.chunks(block_size as usize).zip(&ctxs) {
            if only_write_unwritten && self.block_written[block.0 as usize] {
                start_new_chunk = true;
                block.0 += 1;
                continue;
            }
            if start_new_chunk {
                iovecs.push(WriteChunk {
                    offset: block,
                    iovecs: vec![],
                });
                start_new_chunk = false
            }
            let vs = &mut iovecs.last_mut().unwrap().iovecs;
            vs.push(IoSlice::new(data));
            vs.push(IoSlice::new(ctx));
            expected_bytes += data.len() + ctx.len();
            if self.layout.has_padding_after(block) {
                vs.push(IoSlice::new(&padding));
                expected_bytes += padding.len();
            }
            block.0 += 1;

            // If the next write would exceed iov_max, set the `start_new_chunk`
            // flag so that we begin a new chunk before pushing the write.
            start_new_chunk |=
                vs.len() + 2 + self.layout.has_padding_after(block) as usize
                    > iov_max;
        }

        if iovecs.is_empty() {
            return Ok(());
        }

        self.set_dirty()?;

        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number.0, n_blocks as u64)
        });

        // Now execute each chunk in a separate `pwritev` call
        let mut total_bytes = 0;
        let mut err = None;
        for c in iovecs {
            let start_pos = self.layout.block_pos(c.offset);
            let r = nix::sys::uio::pwritev(
                self.file.as_fd(),
                &c.iovecs,
                start_pos as i64,
            )
            .map_err(|e| {
                CrucibleError::IoError(format!(
                    "extent {}: write failed: {e}",
                    self.extent_number
                ))
            });
            match r {
                Err(e) => err = Some(e),
                Ok(num_bytes) => total_bytes += num_bytes,
            };
        }

        let r = match err {
            Some(e) => Err(e),
            None if total_bytes != expected_bytes => {
                Err(CrucibleError::IoError(format!(
                    "extent {}: incomplete write \
                    (expected {expected_bytes}, got {total_bytes})",
                    self.extent_number
                )))
            }
            None => Ok(()),
        };

        if r.is_err() {
            for i in 0..n_blocks {
                let block = BlockOffset(write.offset.0 + i as u64);

                // Try to recompute the context slot from the file.  If this
                // fails, then we _really_ can't recover, so bail out
                // unceremoniously.
                self.recompute_block_written_from_file(block).unwrap();
            }
        } else {
            // Now that writes have gone through, mark as written
            self.block_written[(write.offset.0) as usize..][..n_blocks]
                .fill(true);
        }
        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number.0, n_blocks as u64)
        });

        r
    }

    fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
        iov_max: usize,
    ) -> Result<ExtentReadResponse, CrucibleError> {
        let mut buf = req.data;
        let block_size = self.extent_size.block_size_in_bytes() as usize;
        let start_block = req.offset;
        let num_blocks = buf.capacity() / block_size;
        check_input(self.extent_size, start_block, buf.capacity())?;

        let start_pos = self.layout.block_pos(start_block);
        let mut buf_ptr =
            buf.spare_capacity_mut().as_mut_ptr() as *mut libc::c_void;

        let mut ctxs =
            vec![[0u8; BLOCK_CONTEXT_SIZE_BYTES as usize]; num_blocks];
        let mut padding = vec![0u8; self.layout.padding_size() as usize];
        let mut iovecs = Vec::with_capacity(num_blocks * 3);

        let mut block = start_block;
        let mut padding_count = 0;
        for ctx in &mut ctxs {
            iovecs.push(libc::iovec {
                iov_base: buf_ptr,
                iov_len: block_size,
            });
            iovecs.push(libc::iovec {
                iov_base: ctx as *mut _ as *mut _,
                iov_len: BLOCK_CONTEXT_SIZE_BYTES as usize,
            });
            if self.layout.has_padding_after(block) {
                iovecs.push(libc::iovec {
                    iov_base: padding.as_mut_ptr() as *mut _,
                    iov_len: padding.len(),
                });
                padding_count += 1;
            }
            buf_ptr = buf_ptr.wrapping_add(block_size);
            block.0 += 1;
        }

        // How many bytes do we expect `preadv` to return?
        let expected_bytes = num_blocks
            * (block_size + BLOCK_CONTEXT_SIZE_BYTES as usize)
            + padding.len() * padding_count as usize;

        // Finally we get to read the actual data. That's why we're here
        cdt::extent__read__file__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks as u64)
        });
        let mut total_bytes = 0;
        for iov in iovecs.chunks(iov_max) {
            let r = unsafe {
                libc::preadv(
                    self.file.as_raw_fd(),
                    iov.as_ptr(),
                    iov.len() as libc::c_int,
                    start_pos as i64 + total_bytes as i64,
                )
            };

            // Check against the expected number of bytes.  We could do more
            // robust error handling here (e.g. retrying in a loop), but for
            // now, simply bailing out seems wise.
            let r = nix::errno::Errno::result(r).map(|r| r as usize);
            let num_bytes = match r.map_err(|e| {
                CrucibleError::IoError(format!(
                    "extent {}: read failed: {e}",
                    self.extent_number
                ))
            }) {
                Ok(n) => n,
                Err(e) => {
                    // Early exit, so fire the DTrace probe here
                    cdt::extent__read__file__done!(|| {
                        (job_id.0, self.extent_number.0, num_blocks as u64)
                    });
                    return Err(e);
                }
            };
            total_bytes += num_bytes;
        }

        // Fire the DTrace probe to indicate the read is done
        cdt::extent__read__file__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks as u64)
        });

        if total_bytes != expected_bytes as usize {
            return Err(CrucibleError::IoError(format!(
                "extent {}: incomplete read \
                 (expected {expected_bytes}, got {total_bytes})",
                self.extent_number
            )));
        }

        // SAFETY: we just initialized this chunk of the buffer
        unsafe {
            buf.set_len(num_blocks * block_size);
        }

        let blocks = ctxs
            .into_iter()
            .map(|ctx| bincode::deserialize(&ctx))
            .collect::<Result<Vec<ReadBlockContext>, _>>()
            .map_err(|e| CrucibleError::BadContextSlot(e.to_string()))?;

        Ok(ExtentReadResponse { data: buf, blocks })
    }

    fn flush(
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

        // Now, we fsync to ensure data is flushed to disk.  It's okay to crash
        // before this point, because setting the flush number is atomic.
        cdt::extent__flush__file__start!(|| {
            (job_id.get(), self.extent_number.0, 0)
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
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number.0, 0)
        });

        cdt::extent__flush__done!(|| {
            (job_id.get(), self.extent_number.0, 0)
        });

        Ok(())
    }

    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        _block: u64,
        _count: u64,
    ) -> Result<Vec<Option<crate::extent::DownstairsBlockContext>>, CrucibleError>
    {
        panic!("cannot get block contexts outside of a read");
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        _block_context: &crate::extent::DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        panic!("cannot set block contexts outside of a write");
    }
}

impl RawInnerV2 {
    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
        extent_recordsize: u64,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size, extent_recordsize);
        let size = layout.file_size();

        mkdir_for_file(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // All 0s are fine for everything except recordsize and metadata
        file.set_len(size)?;
        layout.write_recordsize(&file, extent_recordsize)?;
        let mut out = Self {
            file,
            dirty: false,
            extent_size,
            block_written: vec![false; def.extent_size().value as usize],
            layout,
            extent_number,
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
        recordsize: u64,
        log: &Logger,
    ) -> Result<Self, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let extent_size = def.extent_size();
        let layout = RawLayout::new(extent_size, recordsize);
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

        let expected_recordsize = layout.get_recordsize(&file)?;
        if expected_recordsize != recordsize {
            return Err(CrucibleError::IoError(format!(
                "recordsize for extent {extent_number} has changed: \
                 expected {expected_recordsize}, \
                 got {recordsize} from filesystem",
            )));
        }

        let meta = layout.get_metadata(&file)?;

        // If the file is dirty, then we have to recompute whether blocks are
        // written or not.  This is slow, but can't be avoided; we closed the
        // file without a flush so we can't be confident about the data that was
        // on disk.
        let block_written = if !meta.dirty {
            // Easy case first: if it's **not** dirty, then just assign active
            // slots based on the bitpacked active context buffer from the file.
            layout.get_block_written_array(&file)?
        } else {
            // Now that we've read the context slot arrays, read file data and
            // figure out which context slot is active.
            let mut file_buffered = BufReader::with_capacity(64 * 1024, &file);
            let mut block_written = vec![];
            for _ in 0..layout.block_count() {
                // Read the variant tag, which is 0 for ReadBlockContext::Empty
                let mut tag = 0u32;
                file_buffered.read_exact(tag.as_bytes_mut())?;
                block_written.push(tag != 0);

                // Skip the bulk data, on to the next block's context slot
                file_buffered
                    .seek_relative(extent_size.block_size_in_bytes() as i64)?;
            }
            block_written
        };

        Ok(Self {
            file,
            dirty: meta.dirty,
            extent_number,
            extent_size: def.extent_size(),
            block_written,
            layout,
        })
    }

    fn set_dirty(&mut self) -> Result<(), CrucibleError> {
        if !self.dirty {
            self.layout.set_dirty(&self.file)?;
            self.dirty = true;
        }
        Ok(())
    }

    /// Updates `self.block_written[block]` based on data read from the file
    ///
    /// Specifically, if the context is written (has a non-zero `tag`), then the
    /// block is guaranteed to be written, because they are always written
    /// together in an atomic operation.
    ///
    /// We expect to call this function rarely, so it does not attempt to
    /// minimize the number of syscalls it executes.
    fn recompute_block_written_from_file(
        &mut self,
        block: BlockOffset,
    ) -> Result<(), CrucibleError> {
        let pos = self.layout.context_slot(block) as i64;
        let mut tag = 0u32;
        pread_all(self.file.as_fd(), tag.as_bytes_mut(), pos).map_err(|e| {
            CrucibleError::IoError(format!(
                "extent {}: reading block {} data failed: {e}",
                self.extent_number, block.0
            ))
        })?;

        self.block_written[block.0 as usize] = tag != 0;
        Ok(())
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
        self.layout.write_block_written_and_metadata(
            &self.file,
            &self.block_written,
            false, // dirty
            new_flush,
            new_gen,
        )?;
        self.dirty = false;
        Ok(())
    }
}

/// Data structure that implements the on-disk layout of a raw extent file
#[derive(Debug)]
struct RawLayout {
    extent_size: Block,
    recordsize: u64,
}

impl RawLayout {
    fn new(extent_size: Block, recordsize: u64) -> Self {
        RawLayout {
            extent_size,
            recordsize,
        }
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
        self.metadata_offset() + BLOCK_META_SIZE_BYTES
    }

    /// Number of blocks in the extent file
    fn block_count(&self) -> u64 {
        self.extent_size.value
    }

    /// Records the position of the 8-byte recordsize field
    fn recordsize_offset(&self) -> u64 {
        let bpr = self.blocks_per_record();
        let bc = self.block_count();

        if bc % bpr == 0 {
            (bc / bpr) * self.recordsize
        } else {
            let record_count = bc / bpr;
            let trailing_blocks = bc - record_count * bpr;

            // Make sure that metadata isn't torn across two records; if that's
            // the case, then snap to the next recordsize boundary
            let start_offset = record_count * self.recordsize
                + trailing_blocks
                    * (self.block_size() + BLOCK_CONTEXT_SIZE_BYTES);
            let end_offset = start_offset
                + 8
                + self.block_written_array_size()
                + BLOCK_META_SIZE_BYTES;

            let start_record = start_offset / self.recordsize;
            let end_record = end_offset / self.recordsize;
            if start_record == end_record {
                start_offset
            } else {
                end_record * self.recordsize
            }
        }
    }

    /// Returns the byte offset of the `block_written` bitpacked array
    fn block_written_array_offset(&self) -> u64 {
        self.recordsize_offset() + std::mem::size_of::<u64>() as u64
    }

    /// Returns the size of the `block_written` bitpacked array, in bytes
    fn block_written_array_size(&self) -> u64 {
        self.block_count().div_ceil(8)
    }

    /// Returns the offset of the metadata chunk of the file
    fn metadata_offset(&self) -> u64 {
        self.block_written_array_offset() + self.block_written_array_size()
    }

    /// Number of bytes in each block
    fn block_size(&self) -> u64 {
        self.extent_size.block_size_in_bytes() as u64
    }

    /// Reads the expected recordsize from the file
    fn get_recordsize(&self, file: &File) -> Result<u64, CrucibleError> {
        let mut v = 0u64;
        let offset = self.recordsize_offset();
        pread_all(file.as_fd(), v.as_bytes_mut(), offset as i64).map_err(
            |e| {
                CrucibleError::IoError(format!(
                    "reading recordsize failed: {e}"
                ))
            },
        )?;
        Ok(v)
    }

    /// Writes the expected recordsize from the file
    fn write_recordsize(
        &self,
        file: &File,
        recordsize: u64,
    ) -> Result<(), CrucibleError> {
        let offset = self.recordsize_offset();
        pwrite_all(file.as_fd(), recordsize.as_bytes(), offset as i64)
            .map_err(|e| {
                CrucibleError::IoError(format!("writing metadata failed: {e}"))
            })?;
        Ok(())
    }

    /// Reads metadata from the file
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

    /// Write out the metadata section of the file
    ///
    /// This is done in a single write, so it should be atomic.
    ///
    /// # Panics
    /// `block_written.len()` must match `self.block_count()`, and the function
    /// will panic otherwise.
    fn write_block_written_and_metadata(
        &self,
        file: &File,
        block_written: &[bool],
        dirty: bool,
        flush_number: u64,
        gen_number: u64,
    ) -> Result<(), CrucibleError> {
        assert_eq!(block_written.len(), self.block_count() as usize);

        let mut buf = vec![];
        for c in block_written.chunks(8) {
            let mut v = 0;
            for (i, w) in c.iter().enumerate() {
                v |= (*w as u8) << i;
            }
            buf.push(v);
        }

        let d = OnDiskMeta {
            dirty,
            flush_number,
            gen_number,
            ext_version: EXTENT_META_RAW_V2,
        };
        let mut meta = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta.as_mut_slice(), &d).unwrap();
        buf.extend(meta);

        let offset = self.block_written_array_offset();

        pwrite_all(file.as_fd(), &buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!("writing metadata failed: {e}"))
        })?;

        Ok(())
    }

    /// Decodes the block written array from the given file
    ///
    /// The file descriptor offset is not changed by this function
    fn get_block_written_array(
        &self,
        file: &File,
    ) -> Result<Vec<bool>, CrucibleError> {
        let mut buf = vec![0u8; self.block_written_array_size() as usize];
        let offset = self.block_written_array_offset();
        pread_all(file.as_fd(), &mut buf, offset as i64).map_err(|e| {
            CrucibleError::IoError(format!(
                "could not read active contexts: {e}"
            ))
        })?;

        let mut block_written = vec![];
        for bit in buf
            .iter()
            .flat_map(|b| (0..8).map(move |i| b & (1 << i)))
            .take(self.block_count() as usize)
        {
            // Unpack bits from each byte
            block_written.push(bit != 0);
        }
        assert_eq!(block_written.len(), self.block_count() as usize);
        Ok(block_written)
    }

    /// Returns the starting point in the file for the given block
    fn block_pos(&self, block: BlockOffset) -> u64 {
        let bpr = self.blocks_per_record();
        let record = block.0 / bpr;
        let block = block.0 % bpr;
        record * self.recordsize
            + block * (self.block_size() + BLOCK_CONTEXT_SIZE_BYTES)
    }

    /// Returns the position of the given block's context
    fn context_slot(&self, block: BlockOffset) -> u64 {
        self.block_pos(block) + self.block_size()
    }

    /// Returns the number of blocks that fit into a ZFS recordsize
    fn blocks_per_record(&self) -> u64 {
        // Each block contains data and a single context slot
        let bytes_per_block = self.block_size() + BLOCK_CONTEXT_SIZE_BYTES;
        self.recordsize / bytes_per_block
    }

    /// Checks whether there is padding after the given block
    fn has_padding_after(&self, block: BlockOffset) -> bool {
        // No padding at the end of the data section
        if block.0 == self.block_count() - 1 {
            return false;
        }
        // Otherwise, there's padding at the end of each block-pair-group
        let bpr = self.blocks_per_record();
        (block.0 % bpr) == bpr - 1
    }

    /// Returns the size of `recordsize` padding
    fn padding_size(&self) -> u64 {
        let bpr = self.blocks_per_record();
        self.recordsize - bpr * (self.block_size() + BLOCK_CONTEXT_SIZE_BYTES)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use crucible_common::{build_logger, integrity_hash};
    use crucible_protocol::BlockContext;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[test]
    fn test_recordsize_change() -> Result<()> {
        let dir = tempdir()?;

        let def = new_region_definition();
        let eid = ExtentId(0);
        let inner =
            RawInnerV2::create(dir.as_ref(), &def, eid, DUMMY_RECORDSIZE)
                .unwrap();
        let recordsize = inner.layout.recordsize;

        // Manually tweak the recordsize in the raw file's on-disk data
        inner
            .layout
            .write_recordsize(&inner.file, recordsize / 2)
            .unwrap();

        // Reopen, which should fail due to a recordsize mismatch
        let reopen = RawInnerV2::open(
            dir.as_ref(),
            &def,
            eid,
            false,
            DUMMY_RECORDSIZE,
            &build_logger(),
        );
        assert!(reopen.is_err());

        Ok(())
    }

    #[test]
    fn test_metadata_position() {
        let layout = RawLayout::new(Block::new(240, 9), DUMMY_RECORDSIZE);
        assert!(layout.file_size() > DUMMY_RECORDSIZE);
        assert!(layout.recordsize_offset() == DUMMY_RECORDSIZE);

        let layout = RawLayout::new(Block::new(230, 9), DUMMY_RECORDSIZE);
        assert!(layout.file_size() < DUMMY_RECORDSIZE);
        assert!(layout.recordsize_offset() < DUMMY_RECORDSIZE);
    }

    #[test]
    fn test_write_unwritten_without_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = RawInnerV2::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
            DUMMY_RECORDSIZE,
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
}
