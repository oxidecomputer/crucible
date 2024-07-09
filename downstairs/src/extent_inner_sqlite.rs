// Copyright 2023 Oxide Computer Company
use crate::{
    cdt,
    extent::{check_input, extent_path, DownstairsBlockContext, ExtentInner},
    extent_inner_raw_common::pwrite_all,
    integrity_hash,
    region::JobOrReconciliationId,
    Block, BlockContext, CrucibleError, ExtentReadRequest, ExtentReadResponse,
    ExtentWrite, JobId, RegionDefinition,
};
use crucible_common::{crucible_bail, ExtentId};
use crucible_protocol::EncryptionContext;

use anyhow::{bail, Result};
use itertools::Itertools;
use rusqlite::{params, Connection, Transaction};
use slog::{error, Logger};

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;

#[derive(Debug)]
pub struct SqliteInner(std::sync::Mutex<SqliteMoreInner>);

impl ExtentInner for SqliteInner {
    fn gen_number(&self) -> Result<u64, CrucibleError> {
        self.0.lock().unwrap().gen_number()
    }
    fn flush_number(&self) -> Result<u64, CrucibleError> {
        self.0.lock().unwrap().flush_number()
    }
    fn dirty(&self) -> Result<bool, CrucibleError> {
        self.0.lock().unwrap().dirty()
    }

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        self.0.lock().unwrap().flush(new_flush, new_gen, job_id)
    }

    fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
        _iov_max: usize, // unused by SQLite backend
    ) -> Result<ExtentReadResponse, CrucibleError> {
        self.0.lock().unwrap().read(job_id, req)
    }

    fn write(
        &mut self,
        job_id: JobId,
        write: &ExtentWrite,
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError> {
        self.0.lock().unwrap().write(
            job_id,
            write,
            only_write_unwritten,
            iov_max,
        )
    }

    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>, CrucibleError> {
        self.0.lock().unwrap().get_block_contexts(block, count)
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.0
            .lock()
            .unwrap()
            .set_dirty_and_block_context(block_context)
    }
}

impl SqliteInner {
    #[cfg(any(test, feature = "integration-tests"))]
    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
    ) -> Result<Self> {
        let i = SqliteMoreInner::create(dir, def, extent_number)?;
        Ok(Self(i.into()))
    }

    pub fn open(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self> {
        let i = SqliteMoreInner::open(dir, def, extent_number, read_only, log)?;
        Ok(Self(i.into()))
    }

    pub fn export_contexts(
        &mut self,
    ) -> Result<Vec<Option<DownstairsBlockContext>>> {
        self.0.lock().unwrap().export_contexts()
    }

    #[cfg(test)]
    pub fn fully_rehash_and_clean_all_stale_contexts(
        &mut self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError> {
        self.0
            .lock()
            .unwrap()
            .fully_rehash_and_clean_all_stale_contexts(force_override_dirty)
    }

    #[cfg(test)]
    fn truncate_encryption_contexts_and_hashes(
        &self,
        extent_block_indexes_and_hashes: &[(usize, u64)],
    ) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .truncate_encryption_contexts_and_hashes(
                extent_block_indexes_and_hashes,
            )
    }
}

#[derive(Debug)]
struct SqliteMoreInner {
    file: File,
    metadb: Connection,

    /// Our extent number
    extent_number: ExtentId,

    /// Extent size, in blocks
    extent_size: Block,

    /// Flag indicating whether the `dirty` bit is set
    ///
    /// This is cached locally to avoid an expensive SQLite operation when it is
    /// already true, and is a `Cell` for interior mutability.
    dirty: std::cell::Cell<bool>,

    /// Set of blocks that have been written since last flush.
    ///
    /// If the hash is known, then it's also recorded here.  It _should_ always
    /// be known, unless the write failed.
    dirty_blocks: BTreeMap<usize, Option<u64>>,
}

impl SqliteMoreInner {
    fn gen_number(&self) -> Result<u64, CrucibleError> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='gen_number'",
        )?;
        let mut gen_number_iter = stmt.query_map([], |row| row.get(0))?;
        let gen_number = gen_number_iter.next().unwrap()?;
        assert!(gen_number_iter.next().is_none());

        Ok(gen_number)
    }

    fn flush_number(&self) -> Result<u64, CrucibleError> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='flush_number'",
        )?;
        let mut flush_number_iter = stmt.query_map([], |row| row.get(0))?;
        let flush_number = flush_number_iter.next().unwrap()?;
        assert!(flush_number_iter.next().is_none());

        Ok(flush_number)
    }

    fn dirty(&self) -> Result<bool, CrucibleError> {
        Ok(self.dirty.get())
    }

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError> {
        // Used for profiling
        let n_dirty_blocks = self.dirty_blocks.len() as u64;

        cdt::extent__flush__start!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });

        /*
         * We must first fsync to get any outstanding data written to disk.
         * This must be done before we update the flush number.
         */
        cdt::extent__flush__file__start!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });
        if let Err(e) = self.file.sync_all() {
            /*
             * XXX Retry?  Mark extent as broken?
             */
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure: {e:?}",
                self.extent_number,
            );
        }
        cdt::extent__flush__file__done!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });

        // Clear old block contexts. In order to be crash consistent, only
        // perform this after the extent fsync is done. For each block
        // written since the last flush, remove all block context rows where
        // the integrity hash does not map the last-written value. This is
        // safe, because we know the process has not crashed since those
        // values were written. When the region is first opened, the entire
        // file is rehashed, since in that case we don't have that luxury.

        cdt::extent__flush__collect__hashes__start!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });

        // Rehash any parts of the file that we *may have written* data to since
        // the last flush.  (If we know that we wrote the data, then we don't
        // bother rehashing)
        let n_rehashed = self.rehash_dirty_blocks()?;

        cdt::extent__flush__collect__hashes__done!(|| {
            (job_id.get(), self.extent_number.0, n_rehashed as u64)
        });

        cdt::extent__flush__sqlite__insert__start!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });

        // We put all of our metadb updates into a single transaction to
        // assure that we have a single sync.
        let tx = self.metadb.unchecked_transaction()?;

        self.truncate_encryption_contexts_and_hashes_with_tx(
            self.dirty_blocks
                .iter()
                .map(|(block, hash)| (*block, hash.unwrap())),
            &tx,
        )?;

        cdt::extent__flush__sqlite__insert__done!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });

        self.set_flush_number(new_flush, new_gen)?;
        tx.commit()?;
        self.dirty_blocks.clear();

        // Finally, reset the file's seek offset to 0
        self.file.seek(SeekFrom::Start(0))?;

        cdt::extent__flush__done!(|| {
            (job_id.get(), self.extent_number.0, n_dirty_blocks)
        });
        Ok(())
    }

    fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
    ) -> Result<ExtentReadResponse, CrucibleError> {
        let mut buf = req.data;

        let block_size = self.extent_size.block_size_in_bytes() as u64;
        let num_blocks = buf.capacity() as u64 / block_size;
        check_input(self.extent_size, req.offset, buf.capacity())?;

        // Query the block metadata
        cdt::extent__read__get__contexts__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });
        let block_contexts =
            self.get_block_contexts(req.offset.value, num_blocks)?;
        cdt::extent__read__get__contexts__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });
        // Convert from DownstairsBlockContext -> BlockContext
        let blocks = block_contexts
            .into_iter()
            .map(|bs| bs.map(|b| b.block_context))
            .collect();

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
                req.offset.value as i64 * block_size as i64,
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
         *   a) write out encryption context first
         *   b) write out hashes second
         *   c) write out extent data third
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
         * To minimize the performance hit of sending many transactions to
         * sqlite, as much as possible is written at the same time. This
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
         * a checksum or not.  So it is required that a written block has
         * a checksum.
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
            // TODO we should consider adding a query that doesnt actually
            // give us back the data, just checks for its presence.
            let block_contexts =
                self.get_block_contexts(write.offset.value, num_blocks)?;

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

        // We do all of our metadb updates in a single transaction to minimize
        // syncs.  (Note that the "unchecked" in the signature merely denotes
        // that we are taking responsibility for assuring that we are not
        // in a nested transaction, accepting that it will fail at run-time
        // if we are.)
        let tx = self.metadb.unchecked_transaction()?;

        self.set_dirty()?;

        // Write all the metadata to the DB
        // TODO right now we're including the integrity_hash() time in the sqlite time. It's small in
        // comparison right now, but worth being aware of when looking at dtrace numbers
        cdt::extent__write__sqlite__insert__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        let mut hashes_to_write =
            Vec::with_capacity(write.block_contexts.len());
        for (i, ctx) in write.block_contexts.iter().enumerate() {
            if writes_to_skip.contains(&i) {
                hashes_to_write.push(None);
                continue;
            }

            // TODO it would be nice if we could profile what % of time we're
            // spending on hashes locally vs sqlite
            let on_disk_hash =
                integrity_hash(&[&write.data[i * block_size as usize..]
                    [..block_size as usize]]);
            let offset = i as u64 + write.offset.value;

            self.set_block_context(&DownstairsBlockContext {
                block_context: *ctx,
                block: offset,
                on_disk_hash,
            })?;

            // Worth some thought: this could happen inside
            // tx_set_block_context, if we passed a reference to dirty_blocks
            // into that function too. This might be nice, since then a block
            // context could never be set without marking the block as dirty.
            // On the other paw, our test suite very much likes the fact that
            // tx_set_block_context doesn't mark blocks as dirty, since it
            // lets us easily test specific edge-cases of the database state.
            // Could make another function that wraps tx_set_block_context
            // and handles this as well.
            self.dirty_blocks.insert(offset as usize, None);
            hashes_to_write.push(Some(on_disk_hash));
        }
        tx.commit()?;

        cdt::extent__write__sqlite__insert__done!(|| {
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
        // by cutting down the number of sqlite transactions. But, it
        // introduces complexity. The time spent implementing that would
        // probably better be spent switching to aio or something like that.
        cdt::extent__write__file__start!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // Perform writes, which may be broken up by skipped blocks
        for (skip, mut group) in (0..write.block_contexts.len())
            .group_by(|i| writes_to_skip.contains(i))
            .into_iter()
        {
            if skip {
                continue;
            }
            let start = group.next().unwrap();
            let count = group.count() + 1;

            let data = &write.data[start * block_size as usize..]
                [..count * block_size as usize];
            let start_block = write.offset.value + start as u64;

            pwrite_all(
                self.file.as_fd(),
                data,
                (start_block * block_size) as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
        }

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.extent_number.0, num_blocks)
        });

        // At this point, we know that the written data for the target blocks
        // must match the integrity hashes calculated above (and stored to
        // SQLite).  We can therefore store pre-computed hash values for these
        // dirty blocks, allowing us to skip rehashing during a flush operation.
        for (i, hash) in (0..num_blocks).zip(&hashes_to_write) {
            let offset = i + write.offset.value;
            if let Some(h) = hash {
                // This overwrites the `None` value written above!
                let prev = self.dirty_blocks.insert(offset as usize, Some(*h));
                assert_eq!(prev, Some(None));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.set_dirty()?;
        self.set_block_context(block_context)?;
        Ok(())
    }

    /// Exports context slots for every block in the file
    fn export_contexts(
        &mut self,
    ) -> Result<Vec<Option<DownstairsBlockContext>>> {
        let out = self.get_block_contexts(0, self.extent_size.value)?;
        Ok(out)
    }

    fn get_block_contexts(
        &self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Option<DownstairsBlockContext>>, CrucibleError> {
        let stmt =
            "SELECT block, hash, nonce, tag, on_disk_hash FROM block_context \
             WHERE block BETWEEN ?1 AND ?2";
        let mut stmt = self.metadb.prepare_cached(stmt)?;

        let stmt_iter =
            stmt.query_map(params![block, block + count - 1], |row| {
                let block_index: u64 = row.get(0)?;
                let hash: i64 = row.get(1)?;
                let nonce: Option<[u8; 12]> = row.get(2)?;
                let tag: Option<[u8; 16]> = row.get(3)?;
                let on_disk_hash: i64 = row.get(4)?;

                Ok((block_index, hash, nonce, tag, on_disk_hash))
            })?;

        let mut results: Vec<Option<DownstairsBlockContext>> =
            vec![None; count as usize];
        let mut known_hashes: Vec<Option<u64>> = vec![None; count as usize];

        let block_size = self.extent_size.block_size_in_bytes();
        let mut buffer = vec![0u8; block_size as usize];
        for row in stmt_iter {
            let (block_index, hash, nonce, tag, on_disk_hash) = row?;

            let encryption_context = if let Some(nonce) = nonce {
                tag.map(|tag| EncryptionContext { nonce, tag })
            } else {
                None
            };

            let ctx = DownstairsBlockContext {
                block_context: BlockContext {
                    hash: hash as u64,
                    encryption_context,
                },
                block: block_index,
                on_disk_hash: on_disk_hash as u64,
            };

            // If we've stored multiple contexts for this block, then select
            // whichever one has a matching hash, or `None` if no contexts have
            // a matching hash.  Otherwise, assume that the single context is
            // correct (without checking, because that's slow).
            let i = (ctx.block - block) as usize;
            if let Some(prev_result) = results[i] {
                // Make sure we have the true hash stored in known_hashes
                // (caching in case we have > 2 rows for this block)
                let hash = if let Some(h) = known_hashes[i] {
                    h
                } else {
                    let r = unsafe {
                        libc::pread(
                            self.file.as_raw_fd(),
                            buffer.as_mut_ptr() as *mut libc::c_void,
                            block_size as libc::size_t,
                            ctx.block as i64 * block_size as i64,
                        )
                    };

                    let r = nix::errno::Errno::result(r).map(|r| r as usize);
                    let num_bytes = r.map_err(|e| {
                        CrucibleError::IoError(format!(
                            "extent {}: read failed: {e}",
                            self.extent_number
                        ))
                    })?;
                    if num_bytes != block_size as usize {
                        return Err(CrucibleError::IoError(format!(
                            "extent {}: incomplete read \
                         (expected {block_size}, got {num_bytes})",
                            self.extent_number
                        )));
                    }
                    let hash = integrity_hash(&[&buffer]);
                    known_hashes[i] = Some(hash);
                    hash
                };

                // Now, compare the on-disk hash with what's stored in the
                // database, to see which row is correct
                if prev_result.on_disk_hash == hash {
                    // Previous result is valid, leave it be
                } else if ctx.on_disk_hash == hash {
                    // New hash is valid
                    results[i] = Some(ctx);
                } else {
                    // Neither hash is valid, so clear it
                    results[i] = None;
                }
            } else if let Some(h) = known_hashes[i] {
                // If we have computed the hash, then only return rows where the
                // hash actually matches.
                if h == ctx.on_disk_hash {
                    results[i] = Some(ctx);
                }
            } else {
                // Otherwise, assume that the hash is valid (because there's
                // only one row, and we don't want to recheck every time).
                results[i] = Some(ctx);
            }
        }

        Ok(results)
    }

    // We should never create a new SQLite-backed extent in production code,
    // because we should be using raw extents everywhere.  However, we'll create
    // them during tests to check that our automatic migration system works.
    #[cfg(any(test, feature = "integration-tests"))]
    fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
    ) -> Result<Self> {
        use crate::{
            extent::{ExtentMeta, EXTENT_META_SQLITE},
            mkdir_for_file,
        };
        let mut path = extent_path(dir, extent_number);
        let bcount = def.extent_size().value;
        let size = def.block_size().checked_mul(bcount).unwrap();

        mkdir_for_file(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        file.set_len(size)?;
        file.seek(SeekFrom::Start(0))?;

        let mut seed = dir.to_path_buf();
        seed.push("seed");
        seed.set_extension("db");
        path.set_extension("db");

        // Instead of creating the sqlite db for every extent, create it only
        // once, and copy from a seed db when creating other extents. This
        // minimizes Region create time.
        let metadb = if Path::new(&seed).exists() {
            std::fs::copy(&seed, &path)?;

            open_sqlite_connection(&path)?
        } else {
            /*
             * Create the metadata db
             */
            let metadb = open_sqlite_connection(&path)?;

            /*
             * Create tables and insert base data
             */
            metadb.execute(
                "CREATE TABLE metadata (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )",
                [],
            )?;

            let meta = ExtentMeta::new(EXTENT_META_SQLITE);

            metadb.execute(
                "INSERT INTO metadata
                (name, value) VALUES (?1, ?2)",
                params!["ext_version", meta.ext_version],
            )?;
            metadb.execute(
                "INSERT INTO metadata
                (name, value) VALUES (?1, ?2)",
                params!["gen_number", meta.gen_number],
            )?;
            metadb.execute(
                "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
                params!["flush_number", meta.flush_number],
            )?;
            metadb.execute(
                "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
                params!["dirty", meta.dirty],
            )?;

            // Within an extent, store a context row for each block.
            //
            // The Upstairs will send either an integrity hash, or an integrity
            // hash along with some encryption context (a nonce and tag).
            //
            // The Downstairs will have to record multiple context rows for each
            // block, because while what is committed to sqlite is durable (due
            // to the write-ahead logging and the fact that we set PRAGMA
            // SYNCHRONOUS), what is written to the extent file is not durable
            // until a flush of that file is performed.
            //
            // Any of the context rows written between flushes could be valid
            // until we call flush and remove context rows where the integrity
            // hash does not match what was actually flushed to disk.

            // in WITHOUT ROWID mode, SQLite arranges the tables on-disk ordered
            // by the primary key. since we're always doing operations on
            // contiguous ranges of blocks, this is great for us. The only catch
            // is that you can actually see worse performance with large rows
            // (not a problem for us).
            //
            // From https://www.sqlite.org/withoutrowid.html:
            // > WITHOUT ROWID tables work best when individual rows are not too
            // > large. A good rule-of-thumb is that the average size of a
            // > single row in a WITHOUT ROWID table should be less than about
            // > 1/20th the size of a database page. That means that rows should
            // > not contain more than about 50 bytes each for a 1KiB page size
            // > or about 200 bytes each for 4KiB page size.
            //
            // The default SQLite page size is 4KiB, per
            // https://sqlite.org/pgszchng2016.html
            //
            // The primary key is also a uniqueness constraint. Because the
            // on_disk_hash is a hash of the data AFTER encryption, we only need
            // (block, on_disk_hash). A duplicate write with a different
            // encryption context necessarily results in a different on disk
            // hash.
            metadb.execute(
                "CREATE TABLE block_context (
                    block INTEGER,
                    hash INTEGER,
                    nonce BLOB,
                    tag BLOB,
                    on_disk_hash INTEGER,
                    PRIMARY KEY (block, on_disk_hash)
                ) WITHOUT ROWID",
                [],
            )?;

            // write out
            metadb.close().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("metadb.close() failed! {}", e.1),
                )
            })?;

            // Save it as DB seed
            std::fs::copy(&path, &seed)?;

            open_sqlite_connection(&path)?
        };

        // The seed DB or default metadata should not have dirty set, but we'll
        // check here for completeness.
        let dirty = Self::get_dirty_from_metadb(&metadb)?;
        assert!(!dirty);

        /*
         * Complete the construction of our new extent
         */
        Ok(Self {
            file,
            dirty: dirty.into(),
            extent_size: def.extent_size(),
            metadb,
            extent_number,
            dirty_blocks: BTreeMap::new(),
        })
    }

    fn open(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: ExtentId,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self> {
        let mut path = extent_path(dir, extent_number);
        let bcount = def.extent_size().value;
        let size = def.block_size().checked_mul(bcount).unwrap();

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

        /*
         * Open a connection to the metadata db
         */
        path.set_extension("db");
        let metadb = match open_sqlite_connection(&path) {
            Err(e) => {
                error!(
                    log,
                    "Error: Open of db file {path:?} for \
                     extent#{extent_number} returned: {e}",
                );
                bail!(
                    "Open of db file {path:?} for extent#{extent_number} \
                     returned: {e}",
                );
            }
            Ok(m) => m,
        };

        let dirty = Self::get_dirty_from_metadb(&metadb)?;

        let mut out = Self {
            file,
            metadb,
            extent_size: def.extent_size(),
            dirty: dirty.into(),
            extent_number,
            dirty_blocks: BTreeMap::new(),
        };
        // Clean out any irrelevant block contexts, which may be present
        // if downstairs crashed between a write() and a flush().
        if !read_only {
            out.fully_rehash_and_clean_all_stale_contexts(false)?;
        }
        Ok(out)
    }

    fn set_dirty(&self) -> Result<()> {
        if !self.dirty.get() {
            let _ = self
                .metadb
                .prepare_cached(
                    "UPDATE metadata SET value=1 WHERE name='dirty'",
                )?
                .execute([])?;
            self.dirty.set(true);
        }
        Ok(())
    }

    fn set_block_context(
        &self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        let stmt =
            "INSERT OR IGNORE INTO block_context (block, hash, nonce, tag, on_disk_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5)";

        let (nonce, tag) = if let Some(encryption_context) =
            &block_context.block_context.encryption_context
        {
            (
                Some(&encryption_context.nonce),
                Some(&encryption_context.tag),
            )
        } else {
            (None, None)
        };

        let rows_affected =
            self.metadb.prepare_cached(stmt)?.execute(params![
                block_context.block,
                block_context.block_context.hash as i64,
                nonce,
                tag,
                block_context.on_disk_hash as i64,
            ])?;

        // We avoid INSERTing duplicate rows, so this should always be 0 or 1.
        assert!(rows_affected <= 1);

        Ok(())
    }

    /// A wrapper around ['truncate_encryption_contexts_and_hashes_with_tx']
    /// that calls it from within a transaction.
    fn truncate_encryption_contexts_and_hashes(
        &self,
        extent_block_indexes_and_hashes: &[(usize, u64)],
    ) -> Result<()> {
        let tx = self.metadb.unchecked_transaction()?;

        self.truncate_encryption_contexts_and_hashes_with_tx(
            extent_block_indexes_and_hashes.iter().cloned(),
            &tx,
        )?;

        tx.commit()?;

        Ok(())
    }
    /// Get rid of all block context rows except those that match the on-disk
    /// hash that is computed after a flush. For best performance, make sure
    /// `extent_block_indexes_and_hashes` is sorted by block number before
    /// calling this function.  Note that this takes an open transaction as
    /// a parameter; if a caller does not have an open transaction, the
    /// unadorned variant should be called instead.
    fn truncate_encryption_contexts_and_hashes_with_tx(
        &self,
        extent_block_indexes_and_hashes: impl Iterator<Item = (usize, u64)>
            + ExactSizeIterator,
        tx: &Transaction,
    ) -> Result<()> {
        let n_blocks = extent_block_indexes_and_hashes.len();
        cdt::extent__context__truncate__start!(|| n_blocks as u64);

        {
            let stmt = "DELETE FROM block_context \
                where block == ?1 and on_disk_hash != ?2";

            let mut stmt = tx.prepare_cached(stmt)?;
            for (block, on_disk_hash) in extent_block_indexes_and_hashes {
                let _rows_affected =
                    stmt.execute(params![block, on_disk_hash as i64])?;
            }
        }

        cdt::extent__context__truncate__done!(|| ());

        Ok(())
    }

    /// Rehashes any block in `self.dirty_blocks` with an unknown hash
    ///
    /// When this function is complete, every value in `self.dirty_blocks`
    /// should be of the form `Some(hash)` (i.e. there should be no `None`
    /// values).
    ///
    /// Returns the number of blocks that needed to be rehashed.
    #[allow(clippy::read_zero_byte_vec)] // see rust-clippy#9274
    fn rehash_dirty_blocks(&mut self) -> Result<usize> {
        let mut buffer = vec![]; // resized lazily if needed
        let mut out = 0;
        let block_size = self.extent_size.block_size_in_bytes() as u64;
        for (block, hash) in self.dirty_blocks.iter_mut() {
            if hash.is_none() {
                buffer.resize(block_size as usize, 0u8);
                self.file
                    .seek(SeekFrom::Start(*block as u64 * block_size))?;
                self.file.read_exact(&mut buffer)?;
                *hash = Some(integrity_hash(&[&buffer]));
                out += 1;
            }
        }
        Ok(out)
    }

    fn get_dirty_from_metadb(metadb: &Connection) -> Result<bool> {
        let mut stmt = metadb
            .prepare_cached("SELECT value FROM metadata where name='dirty'")?;
        let mut dirty_iter = stmt.query_map([], |row| row.get(0))?;
        let dirty = dirty_iter.next().unwrap()?;
        assert!(dirty_iter.next().is_none());
        Ok(dirty)
    }

    /*
     * The flush and generation numbers will be updated at the same time.
     */
    fn set_flush_number(&self, new_flush: u64, new_gen: u64) -> Result<()> {
        let mut stmt = self.metadb.prepare_cached(
            "UPDATE metadata SET value=?1 WHERE name='flush_number'",
        )?;

        let _rows_affected = stmt.execute([new_flush])?;

        let mut stmt = self.metadb.prepare_cached(
            "UPDATE metadata SET value=?1 WHERE name='gen_number'",
        )?;

        let _rows_affected = stmt.execute([new_gen])?;

        /*
         * When we write out the new flush number, the dirty bit should be
         * set back to false.
         */
        let mut stmt = self
            .metadb
            .prepare_cached("UPDATE metadata SET value=0 WHERE name='dirty'")?;
        let _rows_affected = stmt.execute([])?;
        self.dirty.set(false);

        Ok(())
    }

    /// Rehash the entire file. Remove any stored hash/encryption contexts
    /// that do not correlate to data currently stored on disk. This is
    /// primarily when opening an extent after recovering from a crash, since
    /// irrelevant hashes will normally be cleared out during a flush().
    ///
    /// By default this function will only do work if the extent is marked
    /// dirty. Set `force_override_dirty` to `true` to override this behavior.
    /// This override should generally not be necessary, as the dirty flag
    /// is set before any contexts are written.
    fn fully_rehash_and_clean_all_stale_contexts(
        &mut self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError> {
        if !force_override_dirty && !self.dirty()? {
            return Ok(());
        }

        // Just in case, let's be very sure that the file on disk is what it should be
        if let Err(e) = self.file.sync_all() {
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure during full rehash: {e:?}",
                self.extent_number,
            );
        }

        self.file.seek(SeekFrom::Start(0))?;

        // Buffer the file so we dont spend all day waiting on syscalls
        let mut self_file_buffered =
            BufReader::with_capacity(64 * 1024, &self.file);

        // This gets filled one block at a time for hashing
        let mut block =
            vec![0; self.extent_size.block_size_in_bytes() as usize];

        // The vec of hashes that we'll pass off to truncate...()
        let mut extent_block_indexes_and_hashes =
            Vec::with_capacity(self.extent_size.value as usize);

        // Stream the contents of the file and rehash them.
        for i in 0..self.extent_size.value as usize {
            self_file_buffered.read_exact(&mut block)?;
            extent_block_indexes_and_hashes
                .push((i, integrity_hash(&[&block])));
        }

        // NOTE on safety: Unlike BufWriter, BufReader drop() does not have
        // side-effects. There are no unhandled errors here.
        drop(self_file_buffered);

        self.truncate_encryption_contexts_and_hashes(
            &extent_block_indexes_and_hashes,
        )?;

        // Intentionally not clearing the dirty flag - we want to know that the
        // extent is still considered dirty.

        // Let's also clear out the internal map of hashes, in case it has any.
        // This shouldn't be necessary, for two reasons
        // 1. aside from tests, this function is only called before any writes
        //    are issued.
        // 2. this function should always produce results that match the state
        //    of that map _anyway_.
        // But, I never trust a cache, and may as well avoid keeping memory
        // around at least right?
        self.dirty_blocks.clear();

        Ok(())
    }
}

/// Always open sqlite with journaling, and synchronous.
/// Note: these pragma_updates are not durable
fn open_sqlite_connection<P: AsRef<Path>>(path: &P) -> Result<Connection> {
    let metadb = Connection::open(path)?;

    assert!(metadb.is_autocommit());
    metadb.pragma_update(None, "journal_mode", "WAL")?;
    metadb.pragma_update(None, "synchronous", "FULL")?;

    // 16 page * 4KiB page size = 64KiB cache size
    // Value chosen somewhat arbitrarily as a guess at a good starting point.
    // the default is a 2000KiB cache size which was way too large, and we did
    // not see any performance changes moving to a 64KiB cache size. But, this
    // value may be something we want to reduce further, tune, or scale with
    // extent size.
    metadb.pragma_update(None, "cache_size", 16)?;

    // rusqlite provides an LRU Cache (a cache which, when full, evicts the
    // least-recently-used value). This caches prepared statements, allowing
    // us to nullify the cost of parsing and compiling frequently used
    // statements.  I've changed all sqlite queries in Inner to use
    // `prepare_cached` to take advantage of this cache. I have not done this
    // for `prepare_cached` region creation,
    // since it wouldn't be relevant there.

    // The result is a dramatic reduction in CPU time spent parsing queries.
    // Prior to this change, sqlite3Prepare was taking 60% of CPU time
    // during reads and 50% during writes based on dtrace flamegraphs.
    // Afterwards, they don't even show up on the graph. I'm seeing a minimum
    // doubling of actual throughput with `crudd` after this change on my
    // local hardware.

    // However, this does cost some amount of memory per-extent and thus will
    // scale linearly . This cache size I'm setting now (64 statements) is
    // chosen somewhat arbitrarily. If this becomes a significant consumer
    // of memory, we should reduce the size of the LRU, and stop caching the
    // statements in the coldest paths. At present, it doesn't seem to have any
    // meaningful impact on memory usage.

    // We could instead try to pre-generate and hold onto all the statements we
    // need to use ourselves, but I looked into doing that, and basically
    // the code required would be a mess due to lifetimes. We shouldn't do
    // that except as a last resort.

    // Also, if you're curious, the hashing function used is
    // https://docs.rs/ahash/latest/ahash/index.html
    metadb.set_prepared_statement_cache_capacity(64);

    Ok(metadb)
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use rand::Rng;
    use rand::RngCore;
    use tempfile::tempdir;

    const IOV_MAX_TEST: usize = 1000;

    fn new_region_definition() -> RegionDefinition {
        let opt = crate::region::test::new_region_options();
        RegionDefinition::from_options(&opt).unwrap()
    }

    #[test]
    fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
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

        let ctx = inner.get_block_contexts(0, 1)?[0].unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().tag,
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

        let ctx = inner.get_block_contexts(0, 1)?[0];

        // Neither of these hashes is actually valid, so once we have two of
        // them, we'll return None (because we actually check the hash)
        assert!(ctx.is_none());

        // "Flush", so only the rows that match should remain.
        inner.truncate_encryption_contexts_and_hashes(&[(0, 65536)])?;

        let ctx = inner.get_block_contexts(0, 1)?[0].unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().nonce,
            blob1
        );
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().tag,
            blob2
        );
        assert_eq!(ctx.block_context.hash, 1024);
        assert_eq!(ctx.on_disk_hash, 65536);

        Ok(())
    }

    #[test]
    fn duplicate_context_insert() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        assert!(inner.get_block_contexts(0, 1)?[0].is_none());

        // Submit the same contents for block 0 over and over, simulating a user
        // writing the same unencrypted contents to the same offset.
        for _ in 0..10 {
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 123,
                },
                block: 0,
                on_disk_hash: 123,
            })?;
        }

        // Duplicate rows should not be inserted

        let ctxs = inner.get_block_contexts(0, 1)?[0];
        assert!(ctxs.is_some());

        // Truncation should still work

        inner.truncate_encryption_contexts_and_hashes(&[(0, 123)])?;

        let ctxs = inner.get_block_contexts(0, 1)?[0];
        assert!(ctxs.is_some());

        Ok(())
    }

    #[test]
    fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
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
        let ctx = inner.get_block_contexts(0, 1)?[0].unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctx.block_context.hash, 123);
        assert_eq!(ctx.on_disk_hash, 456);

        // Verify block 1's context
        let ctx = inner.get_block_contexts(1, 1)?[0].unwrap();
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctx.block_context.encryption_context.as_ref().unwrap().tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctx.block_context.hash, 9999);
        assert_eq!(ctx.on_disk_hash, 1234567890);

        // Return both block 0's and block 1's context, and verify

        let ctxs = inner.get_block_contexts(0, 2)?;
        assert!(ctxs[0].is_some());
        assert_eq!(
            ctxs[0]
                .unwrap()
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            ctxs[0]
                .unwrap()
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(ctxs[0].unwrap().block_context.hash, 123);
        assert_eq!(ctxs[0].unwrap().on_disk_hash, 456);

        assert!(ctxs[1].is_some());
        assert_eq!(
            ctxs[1]
                .unwrap()
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert_eq!(
            ctxs[1]
                .unwrap()
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            [8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        );
        assert_eq!(ctxs[1].unwrap().block_context.hash, 9999);
        assert_eq!(ctxs[1].unwrap().on_disk_hash, 1234567890);

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

        println!("getting block contexts");
        let ctxs = inner.get_block_contexts(0, 2)?;
        assert!(ctxs[0].is_none());
        assert!(ctxs[1].is_none());

        // "Flush", so only the rows that match the on-disk hash should remain.

        inner.truncate_encryption_contexts_and_hashes(&[(0, 6), (1, 7)])?;

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert!(ctxs[0].is_some());
        assert_eq!(ctxs[0].unwrap().on_disk_hash, 6);

        assert!(ctxs[1].is_some());
        assert_eq!(ctxs[1].unwrap().on_disk_hash, 7);

        Ok(())
    }

    /// It's very important that only_write_unwritten always works correctly.
    /// We should never add contexts for blocks that haven't been written by
    /// an upstairs.
    #[test]
    fn test_fully_rehash_and_clean_does_not_mark_blocks_as_written(
    ) -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        // Write a block, but don't flush.
        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);
        let write = ExtentWrite {
            offset: Block::new_512(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: None,
                hash,
            }],
        };
        inner.write(JobId(10), &write, false, IOV_MAX_TEST)?;

        // We haven't flushed, but this should leave our context in place.
        inner.fully_rehash_and_clean_all_stale_contexts(false)?;

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
                offset: Block::new_512(0),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(20), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: Block::new_512(0),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(21), read, IOV_MAX_TEST)?;

            // We should not get back our data, because block 0 was written.
            assert_ne!(resp.blocks, vec![Some(block_context)]);
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
                offset: Block::new_512(1),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(30), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: Block::new_512(1),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(31), read, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(resp.blocks, vec![Some(block_context)]);
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        Ok(())
    }

    /// If a write successfully put a context into the database, but it never
    /// actually got the data onto the disk, that block should revert back to
    /// being "unwritten". After all, the data never was truly written.
    ///
    /// This test is very similar to test_region_open_removes_partial_writes,
    /// but is distinct in that it call fully_rehash directly, without closing
    /// and re-opening the extent.
    #[test]
    fn test_fully_rehash_marks_blocks_unwritten_if_data_never_hit_disk(
    ) -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
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

        // Run a full rehash, which should clear out that partial write.
        inner.fully_rehash_and_clean_all_stale_contexts(false)?;

        // Writing to block 0 should succeed with only_write_unwritten
        {
            let data = Bytes::from(vec![0x66; 512]);
            let hash = integrity_hash(&[&data[..]]);
            let block_context = BlockContext {
                encryption_context: None,
                hash,
            };
            let write = ExtentWrite {
                offset: Block::new_512(0),
                data: data.clone(),
                block_contexts: vec![block_context],
            };
            inner.write(JobId(30), &write, true, IOV_MAX_TEST)?;

            let read = ExtentReadRequest {
                offset: Block::new_512(0),
                data: BytesMut::with_capacity(512),
            };
            let resp = inner.read(JobId(31), read, IOV_MAX_TEST)?;

            // We should get back our data! Block 1 was never written.
            assert_eq!(resp.blocks, vec![Some(block_context)]);
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        Ok(())
    }

    #[test]
    fn test_dirty_hash_correct() -> Result<()> {
        let dir = tempdir()?;
        let mut inner = SqliteInner::create(
            dir.as_ref(),
            &new_region_definition(),
            ExtentId(0),
        )
        .unwrap();

        let mut data = BytesMut::from(&vec![0u8; 512 * 2] as &[u8]);
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut data);
        let data = data.freeze();

        let block_contexts: Vec<_> = data
            .chunks(512)
            .map(|c| BlockContext {
                encryption_context: None,
                hash: integrity_hash(&[c]),
            })
            .collect();
        let write = ExtentWrite {
            offset: Block::new_512(0),
            data: data.clone(),
            block_contexts: block_contexts.clone(),
        };
        inner.write(JobId(30), &write, true, IOV_MAX_TEST)?;

        let read = || ExtentReadRequest {
            offset: Block::new_512(0),
            data: BytesMut::with_capacity(512 * 2),
        };
        let resp = inner.read(JobId(31), read(), IOV_MAX_TEST)?;
        let expected_blocks: Vec<_> =
            block_contexts.iter().map(|b| Some(*b)).collect();
        assert_eq!(resp.blocks, expected_blocks);
        assert_eq!(resp.data, BytesMut::from(data.as_ref()));

        inner.fully_rehash_and_clean_all_stale_contexts(true)?;
        let resp = inner.read(JobId(31), read(), IOV_MAX_TEST)?;
        assert_eq!(resp.blocks, expected_blocks);
        assert_eq!(resp.data, BytesMut::from(data.as_ref()));

        // Write a new hash, then confirm that we're reading the original
        for block in 0..2 {
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    }),
                    hash: 123,
                },
                block,
                on_disk_hash: 456, // this is wrong!
            })?;
            let resp = inner.read(JobId(31), read(), IOV_MAX_TEST)?;
            assert_eq!(resp.blocks, expected_blocks);
            assert_eq!(resp.data, BytesMut::from(data.as_ref()));
        }

        // Read block 1 and make sure it works as well
        let resp = inner.read(
            JobId(31),
            ExtentReadRequest {
                offset: Block::new_512(1),
                data: BytesMut::with_capacity(512),
            },
            IOV_MAX_TEST,
        )?;
        assert_eq!(resp.blocks, &expected_blocks[1..]);
        assert_eq!(resp.data, &data.as_ref()[512..]);

        Ok(())
    }
}
