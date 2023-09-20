// Copyright 2023 Oxide Computer Company
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::fs::{rename, File, OpenOptions};
use std::io::{BufReader, IoSlice, IoSliceMut, Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use anyhow::{anyhow, bail, Result};
use futures::TryStreamExt;
use nix::unistd::{sysconf, SysconfVar};
use rusqlite::{params, Connection, Transaction};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crucible_common::*;
use crucible_protocol::{EncryptionContext, SnapshotDetails};
use repair_client::types::FileType;
use repair_client::Client;

use std::os::fd::AsRawFd;

use super::*;

#[derive(Debug)]
pub struct Extent {
    number: u32,
    read_only: bool,
    block_size: u64,
    extent_size: Block,
    iov_max: usize,

    /// Inner contains information about the actual extent file that holds the
    /// data, the metadata (stored in the database) about that extent, and the
    /// set of dirty blocks that have been written to since last flush.
    pub inner: Mutex<Inner>,
}

/// BlockContext, with the addition of block index and on_disk_hash
#[derive(Clone)]
pub struct DownstairsBlockContext {
    pub block_context: BlockContext,

    pub block: u64,
    pub on_disk_hash: u64,
}

#[derive(Debug)]
pub struct Inner {
    file: File,
    metadb: Connection,

    /// Set of blocks that have been written since last flush.
    ///
    /// If the hash is known, then it's also recorded here.  It _should_ always
    /// be known, unless the write failed.
    dirty_blocks: BTreeMap<usize, Option<u64>>,
}

/// An extent can be Opened or Closed. If Closed, it is probably being updated
/// out of band. If Opened, then this extent is accepting operations.
#[derive(Debug)]
pub enum ExtentState {
    Opened(Arc<Extent>),
    Closed,
}

impl Inner {
    pub fn gen_number(&self) -> Result<u64> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='gen_number'",
        )?;
        let mut gen_number_iter = stmt.query_map([], |row| row.get(0))?;
        let gen_number = gen_number_iter.next().unwrap()?;
        assert!(gen_number_iter.next().is_none());

        Ok(gen_number)
    }

    pub fn flush_number(&self) -> Result<u64> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='flush_number'",
        )?;
        let mut flush_number_iter = stmt.query_map([], |row| row.get(0))?;
        let flush_number = flush_number_iter.next().unwrap()?;
        assert!(flush_number_iter.next().is_none());

        Ok(flush_number)
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

        Ok(())
    }

    pub fn dirty(&self) -> Result<bool> {
        let mut stmt = self
            .metadb
            .prepare_cached("SELECT value FROM metadata where name='dirty'")?;
        let mut dirty_iter = stmt.query_map([], |row| row.get(0))?;
        let dirty = dirty_iter.next().unwrap()?;
        assert!(dirty_iter.next().is_none());

        Ok(dirty)
    }

    fn set_dirty(&self) -> Result<()> {
        let _ = self
            .metadb
            .prepare_cached("UPDATE metadata SET value=1 WHERE name='dirty'")?
            .execute([])?;
        Ok(())
    }

    /// For a given block range, return all context rows since the last flush.
    /// `get_block_contexts` returns a `Vec<Vec<DownstairsBlockContext>>` of
    /// length equal to `count`. Each `Vec<DownstairsBlockContext>` inside this
    /// parent Vec contains all contexts for a single block.
    fn get_block_contexts(
        &self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>> {
        let stmt =
            "SELECT block, hash, nonce, tag, on_disk_hash FROM block_context \
             WHERE block BETWEEN ?1 AND ?2";
        let mut stmt = self.metadb.prepare_cached(stmt)?;

        let stmt_iter =
            stmt.query_map(params![block, block + count - 1], |row| {
                let block_index: u64 = row.get(0)?;
                let hash: i64 = row.get(1)?;
                let nonce: Option<Vec<u8>> = row.get(2)?;
                let tag: Option<Vec<u8>> = row.get(3)?;
                let on_disk_hash: i64 = row.get(4)?;

                Ok((block_index, hash, nonce, tag, on_disk_hash))
            })?;

        let mut results = Vec::with_capacity(count as usize);
        for _i in 0..count {
            results.push(Vec::new());
        }

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

            results[(ctx.block - block) as usize].push(ctx);
        }

        Ok(results)
    }

    /*
     * Append a block context row.
     */
    pub fn tx_set_block_context(
        tx: &rusqlite::Transaction,
        block_context: &DownstairsBlockContext,
    ) -> Result<()> {
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

        let rows_affected = tx.prepare_cached(stmt)?.execute(params![
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

    #[cfg(test)]
    fn set_block_contexts(
        &mut self,
        block_contexts: &[&DownstairsBlockContext],
    ) -> Result<()> {
        self.set_dirty()?;

        let tx = self.metadb.transaction()?;

        for block_context in block_contexts {
            Self::tx_set_block_context(&tx, block_context)?;
        }

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

    /// A wrapper around ['truncate_encryption_contexts_and_hashes_with_tx']
    /// that calls it from within a transaction.
    fn truncate_encryption_contexts_and_hashes(
        &self,
        extent_block_indexes_and_hashes: impl Iterator<Item = (usize, u64)>
            + ExactSizeIterator,
    ) -> Result<()> {
        let tx = self.metadb.unchecked_transaction()?;

        self.truncate_encryption_contexts_and_hashes_with_tx(
            extent_block_indexes_and_hashes,
            &tx,
        )?;

        tx.commit()?;

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
    fn rehash_dirty_blocks(&mut self, block_size: u64) -> Result<usize> {
        let mut buffer = vec![]; // resized lazily if needed
        let mut out = 0;
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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExtentMeta {
    /**
     * Version information regarding the extent structure.
     * Not currently connected to anything XXX
     */
    pub ext_version: u32,
    /**
     * Increasing value provided from upstairs every time it connects to
     * a downstairs.  Used to help break ties if flash numbers are the same
     * on extents.
     */
    pub gen_number: u64,
    /**
     * Increasing value incremented on every write to an extent.
     * All mirrors of an extent should have the same value.
     */
    pub flush_number: u64,
    /**
     * Used to indicate data was written to disk, but not yet flushed
     * Should be set back to false once data has been flushed.
     */
    pub dirty: bool,
}

impl Default for ExtentMeta {
    fn default() -> ExtentMeta {
        ExtentMeta {
            ext_version: 1,
            gen_number: 0,
            flush_number: 0,
            dirty: false,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum ExtentType {
    Data,
    Db,
    DbShm,
    DbWal,
}

impl fmt::Display for ExtentType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtentType::Data => Ok(()),
            ExtentType::Db => write!(f, "db"),
            ExtentType::DbShm => write!(f, "db-shm"),
            ExtentType::DbWal => write!(f, "db-wal"),
        }
    }
}

/**
 * Take an ExtentType and translate it into the corresponding
 * FileType from the repair client.
 */
impl ExtentType {
    fn to_file_type(&self) -> FileType {
        match self {
            ExtentType::Data => FileType::Data,
            ExtentType::Db => FileType::Db,
            ExtentType::DbShm => FileType::DbShm,
            ExtentType::DbWal => FileType::DbWal,
        }
    }
}

/**
 * Produce the string name of the data file for a given extent number
 */
pub fn extent_file_name(number: u32, extent_type: ExtentType) -> String {
    match extent_type {
        ExtentType::Data => {
            format!("{:03X}", number & 0xFFF)
        }
        ExtentType::Db | ExtentType::DbShm | ExtentType::DbWal => {
            format!("{:03X}.{}", number & 0xFFF, extent_type)
        }
    }
}

/**
 * Produce a PathBuf that refers to the containing directory for extent
 * "number", anchored under "dir".
 */
pub fn extent_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push(format!("{:02X}", (number >> 24) & 0xFF));
    out.push(format!("{:03X}", (number >> 12) & 0xFFF));
    out
}

/**
 * Produce a PathBuf that refers to the backing file for extent "number",
 * anchored under "dir".
 */
pub fn extent_path<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out
}

/**
 * Produce a PathBuf that refers to the copy directory we create for
 * a given extent "number",  This directory will hold the files we
 * transfer from the source downstairs.
 * anchored under "dir".
 */
pub fn copy_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("copy");
    out
}

/**
 * Produce a PathBuf that refers to the replace directory we use for
 * a given extent "number".  This directory is generated (see below) when
 * all the files needed for repairing an extent have been added to the
 * copy directory and we are ready to start replacing the extents files.
 * anchored under "dir".  The actual generation of this directory will
 * be done by renaming the copy directory.
 */
pub fn replace_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("replace");
    out
}

/**
 * Produce a PathBuf that refers to the completed directory we use for
 * a given extent "number".  This directory is generated (see below) when
 * all the files needed for repairing an extent have been copied to their
 * final destination and everything has been flushed.
 * The actual generation of this directory will be done by renaming the
 * replace directory.
 */
pub fn completed_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("completed");
    out
}

fn config_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push("region.json");
    out
}

/**
 * Remove directories associated with repair except for the replace
 * directory. Replace is handled specifically during extent open.
 */
pub fn remove_copy_cleanup_dir<P: AsRef<Path>>(dir: P, eid: u32) -> Result<()> {
    let mut remove_dirs = vec![copy_dir(&dir, eid)];
    remove_dirs.push(completed_dir(&dir, eid));

    for d in remove_dirs {
        if Path::new(&d).exists() {
            std::fs::remove_dir_all(&d)?;
        }
    }
    Ok(())
}

/**
 * Validate a list of sorted repair files.
 * There are either two or four files we expect to find, any more or less
 * and we have a bad list.  No duplicates.
 */
pub fn validate_repair_files(eid: usize, files: &[String]) -> bool {
    let eid = eid as u32;

    let some = vec![
        extent_file_name(eid, ExtentType::Data),
        extent_file_name(eid, ExtentType::Db),
    ];

    let mut all = some.clone();
    all.extend(vec![
        extent_file_name(eid, ExtentType::DbShm),
        extent_file_name(eid, ExtentType::DbWal),
    ]);

    // Either we have some or all.
    files == some || files == all
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

impl Extent {
    fn get_iov_max() -> Result<usize> {
        let i: i64 = sysconf(SysconfVar::IOV_MAX)?
            .ok_or_else(|| anyhow!("IOV_MAX returned None!"))?;
        Ok(i.try_into()?)
    }

    /**
     * Open an existing extent file at the location requested.
     * Read in the metadata from the first block of the file.
     */
    async fn open<P: AsRef<Path>>(
        dir: P,
        def: &RegionDefinition,
        number: u32,
        read_only: bool,
        log: &Logger,
    ) -> Result<Extent> {
        /*
         * Store extent data in files within a directory hierarchy so that
         * there are not too many files in any level of that hierarchy.
         */
        let mut path = extent_path(&dir, number);

        let bcount = def.extent_size().value;
        let size = def.block_size().checked_mul(bcount).unwrap();

        remove_copy_cleanup_dir(&dir, number)?;

        // If the replace directory exists for this extent, then it means
        // a repair was interrupted before it could finish.  We will continue
        // the repair before we open the extent.
        let replace_dir = replace_dir(&dir, number);
        if !read_only && Path::new(&replace_dir).exists() {
            info!(
                log,
                "Extent {} found replacement dir, finishing replacement",
                number
            );
            move_replacement_extent(&dir, number as usize, log)?;
        }

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file = match OpenOptions::new()
            .read(true)
            .write(!read_only)
            .open(&path)
        {
            Err(e) => {
                error!(
                    log,
                    "Open of {:?} for extent#{} returned: {}", path, number, e,
                );
                bail!(
                    "Open of {:?} for extent#{} returned: {}",
                    path,
                    number,
                    e,
                );
            }
            Ok(f) => {
                let cur_size = f.metadata().unwrap().len();
                if size != cur_size {
                    bail!(
                        "File size {:?} does not match expected {:?}",
                        size,
                        cur_size
                    );
                }
                f
            }
        };

        /*
         * Open a connection to the metadata db
         */
        path.set_extension("db");
        let metadb =
            match open_sqlite_connection(&path) {
                Err(e) => {
                    error!(
                    log,
                    "Error: Open of db file {:?} for extent#{} returned: {}",
                    path, number, e
                );
                    bail!(
                        "Open of db file {:?} for extent#{} returned: {}",
                        path,
                        number,
                        e,
                    );
                }
                Ok(m) => m,
            };

        // XXX: schema updates?

        let extent = Extent {
            number,
            read_only,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            iov_max: Extent::get_iov_max()?,
            inner: Mutex::new(Inner {
                file,
                metadb,
                dirty_blocks: BTreeMap::new(),
            }),
        };

        // Clean out any irrelevant block contexts, which may be present if
        // downstairs crashed between a write() and a flush().
        extent
            .fully_rehash_and_clean_all_stale_contexts(false)
            .await?;

        Ok(extent)
    }

    pub async fn dirty(&self) -> bool {
        self.inner.lock().await.dirty().unwrap()
    }

    /**
     * Close an extent and the metadata db files for it.
     */
    pub async fn close(self) -> Result<(u64, u64, bool), CrucibleError> {
        let inner = self.inner.lock().await;

        let gen = inner.gen_number().unwrap();
        let flush = inner.flush_number().unwrap();
        let dirty = inner.dirty().unwrap();

        Ok((gen, flush, dirty))
    }

    /**
     * Create an extent at the location requested.
     * Start off with the default meta data.
     * Note that this function is not safe to run concurrently.
     */
    fn create<P: AsRef<Path>>(
        // Extent
        dir: P,
        def: &RegionDefinition,
        number: u32,
    ) -> Result<Extent> {
        /*
         * Store extent data in files within a directory hierarchy so that
         * there are not too many files in any level of that hierarchy.
         */
        let mut path = extent_path(&dir, number);

        /*
         * Verify there are not existing extent files.
         */
        if Path::new(&path).exists() {
            bail!("Extent file already exists {:?}", path);
        }
        remove_copy_cleanup_dir(&dir, number)?;

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

        let mut seed = dir.as_ref().to_path_buf();
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

            let meta = ExtentMeta::default();

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

        /*
         * Complete the construction of our new extent
         */
        Ok(Extent {
            number,
            read_only: false,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            iov_max: Extent::get_iov_max()?,
            inner: Mutex::new(Inner {
                file,
                metadb,
                dirty_blocks: BTreeMap::new(),
            }),
        })
    }

    /**
     * Create the copy directory for this extent.
     */
    fn create_copy_dir<P: AsRef<Path>>(
        dir: P,
        eid: usize,
    ) -> Result<PathBuf, CrucibleError> {
        let cp = copy_dir(dir, eid as u32);

        /*
         * Verify the copy directory does not exist
         */
        if Path::new(&cp).exists() {
            crucible_bail!(IoError, "Copy directory:{:?} already exists", cp);
        }

        std::fs::create_dir_all(&cp)?;
        Ok(cp)
    }

    /**
     * Create the file that will hold a copy of an extent from a
     * remote downstairs.
     */
    fn create_copy_file(
        mut copy_dir: PathBuf,
        eid: usize,
        extension: Option<ExtentType>,
    ) -> Result<File> {
        // Get the base extent name before we consider the actual Type
        let name = extent_file_name(eid as u32, ExtentType::Data);
        copy_dir.push(name);
        if let Some(extension) = extension {
            let ext = format!("{}", extension);
            copy_dir.set_extension(ext);
        }
        let copy_path = copy_dir;

        if Path::new(&copy_path).exists() {
            bail!("Copy file:{:?} already exists", copy_path);
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&copy_path)?;
        Ok(file)
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    /// Read the real data off underlying storage, and get block metadata. If
    /// an error occurs while processing any of the requests, the state of
    /// `responses` is undefined.
    #[instrument]
    pub async fn read(
        &self,
        job_id: JobId,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
    ) -> Result<(), CrucibleError> {
        cdt::extent__read__start!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        let inner = self.inner.lock().await;

        // This code batches up operations for contiguous regions of
        // ReadRequests, so we can perform larger read syscalls and sqlite
        // queries. This significantly improves read throughput.

        // Keep track of the index of the first request in any contiguous run
        // of requests. Of course, a "contiguous run" might just be a single
        // request.
        let mut req_run_start = 0;
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
                    && ((n_contiguous_requests + 1) < self.iov_max)
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
                let resp =
                    ReadResponse::from_request(req, self.block_size as usize);
                self.check_input(req.offset, &resp.data)?;
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
                (job_id.0, self.number, n_contiguous_requests as u64)
            });

            nix::sys::uio::preadv(
                inner.file.as_raw_fd(),
                &mut iovecs,
                first_req.offset.value as i64 * self.block_size as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            cdt::extent__read__file__done!(|| {
                (job_id.0, self.number, n_contiguous_requests as u64)
            });

            // Query the block metadata
            cdt::extent__read__get__contexts__start!(|| {
                (job_id.0, self.number, n_contiguous_requests as u64)
            });
            let block_contexts = inner.get_block_contexts(
                first_req.offset.value,
                n_contiguous_requests as u64,
            )?;
            cdt::extent__read__get__contexts__done!(|| {
                (job_id.0, self.number, n_contiguous_requests as u64)
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

        cdt::extent__read__done!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        Ok(())
    }

    /**
     * Verify that the requested block offset and size of the buffer
     * will fit within the extent.
     */
    fn check_input(
        &self,
        offset: Block,
        data: &[u8],
    ) -> Result<(), CrucibleError> {
        /*
         * Only accept block sized operations
         */
        if data.len() != self.block_size as usize {
            crucible_bail!(DataLenUnaligned);
        }

        if offset.block_size_in_bytes() != self.block_size as u32 {
            crucible_bail!(BlockSizeMismatch);
        }

        if offset.shift != self.extent_size.shift {
            crucible_bail!(BlockSizeMismatch);
        }

        let total_size = self.block_size * self.extent_size.value;
        let byte_offset = offset.value * self.block_size;

        if (byte_offset + data.len() as u64) > total_size {
            crucible_bail!(OffsetInvalid);
        }

        Ok(())
    }

    #[instrument]
    pub async fn write(
        &self,
        job_id: JobId,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::extent__write__start!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        let mut inner_guard = self.inner.lock().await;
        // I realize this looks like some nonsense but what this is doing is
        // borrowing the inner up-front from the MutexGuard, which will allow
        // us to later disjointly borrow fields. Basically, we're helping the
        // borrow-checker do its job.
        let inner = &mut *inner_guard;

        for write in writes {
            self.check_input(write.offset, &write.data)?;
        }

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

        // If `only_write_written`, we need to skip writing to blocks that
        // already contain data. We'll first query the metadata to see which
        // blocks have hashes
        let mut writes_to_skip = HashSet::new();
        if only_write_unwritten {
            cdt::extent__write__get__hashes__start!(|| {
                (job_id.0, self.number, writes.len() as u64)
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
                // TODO we should consider adding a query that doesnt actually
                // give us back the data, just checks for its presence.
                let block_contexts = inner.get_block_contexts(
                    first_write.offset.value,
                    n_contiguous_writes as u64,
                )?;

                for (i, block_contexts) in block_contexts.iter().enumerate() {
                    if !block_contexts.is_empty() {
                        let _ = writes_to_skip
                            .insert(i as u64 + first_write.offset.value);
                    }
                }

                write_run_start += n_contiguous_writes;
            }
            cdt::extent__write__get__hashes__done!(|| {
                (job_id.0, self.number, writes.len() as u64)
            });

            if writes_to_skip.len() == writes.len() {
                // Nothing to do
                cdt::extent__write__done!(|| {
                    (job_id.0, self.number, writes.len() as u64)
                });
                return Ok(());
            }
        }

        // We do all of our metadb updates in a single transaction to minimize
        // syncs.  (Note that the "unchecked" in the signature merely denotes
        // that we are taking responsibility for assuring that we are not
        // in a nested transaction, accepting that it will fail at run-time
        // if we are.)
        let tx = inner.metadb.unchecked_transaction()?;

        inner.set_dirty()?;

        // Write all the metadata to the DB
        // TODO right now we're including the integrity_hash() time in the sqlite time. It's small in
        // comparison right now, but worth being aware of when looking at dtrace numbers
        cdt::extent__write__sqlite__insert__start!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        let mut hashes_to_write = Vec::with_capacity(writes.len());
        for write in writes {
            if writes_to_skip.contains(&write.offset.value) {
                hashes_to_write.push(None);
                continue;
            }

            // TODO it would be nice if we could profile what % of time we're
            // spending on hashes locally vs sqlite
            let on_disk_hash = integrity_hash(&[&write.data[..]]);

            Inner::tx_set_block_context(
                &tx,
                &DownstairsBlockContext {
                    block_context: write.block_context.clone(),
                    block: write.offset.value,
                    on_disk_hash,
                },
            )?;

            // Worth some thought: this could happen inside
            // tx_set_block_context, if we passed a reference to dirty_blocks
            // into that function too. This might be nice, since then a block
            // context could never be set without marking the block as dirty.
            // On the other paw, our test suite very much likes the fact that
            // tx_set_block_context doesn't mark blocks as dirty, since it
            // lets us easily test specific edge-cases of the database state.
            // Could make another function that wraps tx_set_block_context
            // and handles this as well.
            inner.dirty_blocks.insert(write.offset.value as usize, None);
            hashes_to_write.push(Some(on_disk_hash));
        }
        tx.commit()?;

        cdt::extent__write__sqlite__insert__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
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
            (job_id.0, self.number, writes.len() as u64)
        });

        // Now, batch writes into iovecs and use pwritev to write them all out.
        let mut batched_pwritev = BatchedPwritev::new(
            inner.file.as_raw_fd(),
            writes.len(),
            self.block_size,
            self.iov_max,
        );

        for write in writes {
            let block = write.offset.value;

            // TODO, can this be `only_write_unwritten &&
            // write_to_skip.contains()`?
            if writes_to_skip.contains(&block) {
                debug_assert!(only_write_unwritten);
                batched_pwritev.perform_writes()?;
                continue;
            }

            batched_pwritev.add_write(write)?;
        }

        // Write any remaining data
        batched_pwritev.perform_writes()?;

        // At this point, we know that the written data for the target blocks
        // must match the integrity hashes calculated above (and stored to
        // SQLite).  We can therefore store pre-computed hash values for these
        // dirty blocks, allowing us to skip rehashing during a flush operation.
        for (write, hash) in writes.iter().zip(&hashes_to_write) {
            if let Some(h) = hash {
                // This overwrites the `None` value written above!
                let prev = inner
                    .dirty_blocks
                    .insert(write.offset.value as usize, Some(*h));
                assert_eq!(prev, Some(None));
            }
        }

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        cdt::extent__write__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        Ok(())
    }

    #[instrument]
    pub async fn flush(
        &self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobId,
        log: &Logger,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().await;

        if !inner.dirty()? {
            /*
             * If we have made no writes to this extent since the last flush,
             * we do not need to update the extent on disk
             */
            return Ok(());
        }

        // Read only extents should never have the dirty bit set. If they do,
        // bail
        if self.read_only {
            // XXX Make this a panic?
            error!(log, "read-only extent {} has dirty bit set!", self.number);
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        // Used for profiling
        let n_dirty_blocks = inner.dirty_blocks.len() as u64;

        cdt::extent__flush__start!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

        /*
         * We must first fsync to get any outstanding data written to disk.
         * This must be done before we update the flush number.
         */
        cdt::extent__flush__file__start!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });
        if let Err(e) = inner.file.sync_all() {
            /*
             * XXX Retry?  Mark extent as broken?
             */
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure: {:?}",
                self.number,
                e
            );
        }
        cdt::extent__flush__file__done!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

        // Clear old block contexts. In order to be crash consistent, only
        // perform this after the extent fsync is done. For each block
        // written since the last flush, remove all block context rows where
        // the integrity hash does not map the last-written value. This is
        // safe, because we know the process has not crashed since those
        // values were written. When the region is first opened, the entire
        // file is rehashed, since in that case we don't have that luxury.

        cdt::extent__flush__collect__hashes__start!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

        // Rehash any parts of the file that we *may have written* data to since
        // the last flush.  (If we know that we wrote the data, then we don't
        // bother rehashing)
        let n_rehashed = inner.rehash_dirty_blocks(self.block_size)?;

        cdt::extent__flush__collect__hashes__done!(|| {
            (job_id.0, self.number, n_rehashed as u64)
        });

        cdt::extent__flush__sqlite__insert__start!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

        // We put all of our metadb updates into a single transaction to
        // assure that we have a single sync.
        let tx = inner.metadb.unchecked_transaction()?;

        inner.truncate_encryption_contexts_and_hashes_with_tx(
            inner
                .dirty_blocks
                .iter()
                .map(|(block, hash)| (*block, hash.unwrap())),
            &tx,
        )?;

        cdt::extent__flush__sqlite__insert__done!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

        inner.set_flush_number(new_flush, new_gen)?;
        tx.commit()?;
        inner.dirty_blocks.clear();

        // Finally, reset the file's seek offset to 0
        inner.file.seek(SeekFrom::Start(0))?;

        cdt::extent__flush__done!(|| {
            (job_id.0, self.number, n_dirty_blocks)
        });

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
    #[instrument]
    async fn fully_rehash_and_clean_all_stale_contexts(
        &self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().await;

        if !force_override_dirty && !inner.dirty()? {
            return Ok(());
        }

        // Just in case, let's be very sure that the file on disk is what it should be
        if let Err(e) = inner.file.sync_all() {
            crucible_bail!(
                IoError,
                "extent {}: fsync 1 failure during full rehash: {:?}",
                self.number,
                e
            );
        }

        inner.file.seek(SeekFrom::Start(0))?;

        // Buffer the file so we dont spend all day waiting on syscalls
        let mut inner_file_buffered =
            BufReader::with_capacity(64 * 1024, &inner.file);

        // This gets filled one block at a time for hashing
        let mut block = vec![0; self.block_size as usize];

        // The vec of hashes that we'll pass off to truncate...()
        let mut extent_block_indexes_and_hashes =
            Vec::with_capacity(self.extent_size.value as usize);

        // Stream the contents of the file and rehash them.
        for i in 0..self.extent_size.value as usize {
            inner_file_buffered.read_exact(&mut block)?;
            extent_block_indexes_and_hashes
                .push((i, integrity_hash(&[&block])));
        }

        // NOTE on safety: Unlike BufWriter, BufReader drop() does not have
        // side-effects. There are no unhandled errors here.
        drop(inner_file_buffered);

        inner.truncate_encryption_contexts_and_hashes(
            extent_block_indexes_and_hashes.into_iter(),
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
        inner.dirty_blocks.clear();

        Ok(())
    }
}

/**
 * The main structure describing a region.
 */
#[derive(Debug)]
pub struct Region {
    pub dir: PathBuf,
    def: RegionDefinition,
    pub extents: Vec<Arc<Mutex<ExtentState>>>,

    /// extents which are dirty and need to be flushed. should be true if the
    /// dirty flag in the extent's metadata is set. When an extent is opened, if
    /// it's dirty, it's added to here. When a write is issued to an extent,
    /// it's added to here. If the write doesn't actually make the extent dirty
    /// that's fine, because the extent will NOP during the flush anyway, but
    /// this mainly serves to cut down on the extents we're considering for a
    /// flush in the first place.
    dirty_extents: HashSet<usize>,

    read_only: bool,
    log: Logger,
}

impl Region {
    /// Set the number of open files resource limit to the max. Use the provided
    /// RegionDefinition to check that this Downstairs can open all the files it
    /// needs.
    pub fn set_max_open_files_rlimit(
        log: &Logger,
        def: &RegionDefinition,
    ) -> Result<()> {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 {
            bail!(
                "libc::getrlimit failed with {}",
                std::io::Error::last_os_error()
            );
        }

        let number_of_files_limit = match rlim.rlim_cur.cmp(&rlim.rlim_max) {
            std::cmp::Ordering::Less => {
                info!(
                    log,
                    "raising number of open files limit to from {} to {}",
                    rlim.rlim_cur,
                    rlim.rlim_max,
                );

                rlim.rlim_cur = rlim.rlim_max;

                if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) } != 0 {
                    bail!(
                        "libc::setrlimit failed with {}",
                        std::io::Error::last_os_error()
                    );
                }

                rlim.rlim_max
            }

            Ordering::Equal => {
                info!(
                    log,
                    "current number of open files limit {} is already the maximum",
                    rlim.rlim_cur,
                );

                rlim.rlim_cur
            }

            Ordering::Greater => {
                // wat
                warn!(
                    log,
                    "current number of open files limit {} is already ABOVE THE MAXIMUM {}?",
                    rlim.rlim_cur,
                    rlim.rlim_max,
                );

                rlim.rlim_cur
            }
        };

        // The downstairs needs to open (at minimum) the extent file, the sqlite
        // database, the write-ahead lock, and the sqlite shared memory file for
        // each extent in the region, plus:
        //
        // - the seed database (db + shm + wal)
        // - region.json
        // - stdin, stdout, and stderr
        // - the listen and repair sockets (arbitrarily saying two sockets per
        //   server)
        // - optionally, the stat connection to oximeter
        // - optionally, a control interface
        //
        // If the downstairs cannot open this many files, error here.
        let required_number_of_files = def.extent_count() as u64 * 4 + 13;

        if number_of_files_limit < required_number_of_files {
            bail!("this downstairs cannot open all required files!");
        }

        Ok(())
    }

    /**
     * Create a new region based on the given RegionOptions
     */
    pub async fn create<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        log: Logger,
    ) -> Result<Region> {
        options.validate()?;

        let def = RegionDefinition::from_options(&options).unwrap();

        Self::set_max_open_files_rlimit(&log, &def)?;

        let cp = config_path(dir.as_ref());
        /*
         * If the file exists, then exit now with error.  If the caller
         * wants a new region, they have to delete the old one first.
         */
        if Path::new(&cp).exists() {
            bail!("Config file already exists {:?}", cp);
        }
        mkdir_for_file(&cp)?;

        write_json(&cp, &def, false)?;
        info!(log, "Created new region file {:?}", cp);

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            dirty_extents: HashSet::new(),
            read_only: false,
            log,
        };

        region.open_extents(true).await?;

        Ok(region)
    }

    /**
     * Open an existing region file
     */
    pub async fn open<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        verbose: bool,
        read_only: bool,
        log: &Logger,
    ) -> Result<Region> {
        options.validate()?;

        let cp = config_path(dir.as_ref());

        /*
         * We are expecting to find a region config file and extent files.
         * If we do not, then report error and exit.
         */
        let def: crucible_common::RegionDefinition = match read_json(&cp) {
            Ok(def) => def,
            Err(e) => bail!("Error {:?} opening region config {:?}", e, cp),
        };

        Self::set_max_open_files_rlimit(log, &def)?;

        if verbose {
            info!(log, "Opened existing region file {:?}", cp);
        }

        if def.database_read_version() != crucible_common::DATABASE_READ_VERSION
        {
            bail!(
                "Database read version mismatch, expected {}, got {}",
                crucible_common::DATABASE_READ_VERSION,
                def.database_read_version(),
            );
        }
        info!(log, "Database read version {}", def.database_read_version());

        if def.database_write_version()
            != crucible_common::DATABASE_WRITE_VERSION
        {
            bail!(
                "Database write version mismatch, expected {}, got {}",
                crucible_common::DATABASE_WRITE_VERSION,
                def.database_write_version(),
            );
        }
        info!(
            log,
            "Database write version {}",
            def.database_write_version()
        );

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            dirty_extents: HashSet::new(),
            read_only,
            log: log.clone(),
        };

        region.open_extents(false).await?;

        Ok(region)
    }

    pub fn encrypted(&self) -> bool {
        self.def.get_encrypted()
    }

    async fn get_opened_extent(&self, eid: usize) -> Arc<Extent> {
        match &*self.extents[eid].lock().await {
            ExtentState::Opened(extent) => extent.clone(),
            ExtentState::Closed => {
                panic!("attempting to get closed extent {}", eid)
            }
        }
    }

    /**
     * If our extent_count is higher than the number of populated entries
     * we have in our extents Vec, then open all the new extent files and
     * load their content into the extent Vec.
     *
     * If create is false, we expect the extent files to exist at the
     * expected location and will return error if they are not found.
     *
     * If create is true, we expect to create new extent files, and will
     * return error if the file is already present.
     */
    async fn open_extents(&mut self, create: bool) -> Result<()> {
        let next_eid = self.extents.len() as u32;

        let eid_range = next_eid..self.def.extent_count();
        let mut these_extents = Vec::with_capacity(eid_range.len());

        for eid in eid_range {
            let extent = if create {
                Extent::create(&self.dir, &self.def, eid)?
            } else {
                let extent = Extent::open(
                    &self.dir,
                    &self.def,
                    eid,
                    self.read_only,
                    &self.log,
                )
                .await?;

                if extent.dirty().await {
                    self.dirty_extents.insert(eid as usize);
                }

                extent
            };

            these_extents.push(Arc::new(Mutex::new(ExtentState::Opened(
                Arc::new(extent),
            ))));
        }

        self.extents.extend(these_extents);

        for eid in next_eid..self.def.extent_count() {
            assert_eq!(self.get_opened_extent(eid as usize).await.number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());

        Ok(())
    }

    /**
     * Walk the list of all extents and find any that are not open.
     * Open any extents that are not.
     */
    pub async fn reopen_all_extents(&mut self) -> Result<()> {
        let mut to_open = Vec::new();
        for (i, extent) in self.extents.iter().enumerate() {
            let inner = extent.lock().await;
            if matches!(*inner, ExtentState::Closed) {
                to_open.push(i);
            }
        }

        for eid in to_open {
            self.reopen_extent(eid).await?;
        }

        Ok(())
    }

    /**
     * Re open an extent that was previously closed
     */
    pub async fn reopen_extent(
        &mut self,
        eid: usize,
    ) -> Result<(), CrucibleError> {
        /*
         * Make sure the extent :
         *
         * - is currently closed
         * - matches our eid
         * - is not read-only
         */
        let mut mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        assert!(!self.read_only);

        let new_extent = Extent::open(
            &self.dir,
            &self.def,
            eid as u32,
            self.read_only,
            &self.log,
        )
        .await?;

        if new_extent.dirty().await {
            self.dirty_extents.insert(eid);
        }

        *mg = ExtentState::Opened(Arc::new(new_extent));

        Ok(())
    }

    pub async fn close_extent(
        &self,
        eid: usize,
    ) -> Result<(u64, u64, bool), CrucibleError> {
        let mut extent_state = self.extents[eid].lock().await;

        let open_extent =
            std::mem::replace(&mut *extent_state, ExtentState::Closed);

        match open_extent {
            ExtentState::Opened(extent) => {
                // extent here is Arc<Extent>, and closing only makes sense if
                // the reference count is 1.
                let inner = Arc::into_inner(extent);
                assert!(inner.is_some());
                inner.unwrap().close().await
            }

            ExtentState::Closed => {
                panic!("close on closed extent {}!", eid);
            }
        }
    }

    /**
     * Repair an extent from another downstairs
     *
     * We need to repair an extent in such a way that an interruption
     * at any time can be recovered from.
     *
     * Let us assume we are repairing extent 012
     *  1. Make new 012.copy dir  (extent name plus: .copy)
     *  2. Get all extent files from source side, put in 012.copy directory
     *  3. fsync files we just downloaded
     *  4. Rename 012.copy dir to 012.replace dir
     *  5. fsync extent directory ( 00/000/ where the extent files live)
     *  6. Replace current extent 012 files with copied files of same name
     *    from 012.replace dir
     *  7. Remove any files in extent dir that don't exist in replacing dir
     *     For example, if the replacement extent has 012 and 012.db, but
     *     the current (bad) extent has 012 012.db 012.db-shm
     *     and 012.db-wal, we want to remove the 012.db-shm and 012.db-wal
     *     files when we replace 012 and 012.db with the new versions.
     *  8. fsync files after copying them (new location).
     *  9. fsync containing extent dir
     * 10. Rename 012.replace dir to 012.completed dir.
     * 11. fsync extent dir again (so dir rename is persisted)
     * 12. Delete completed dir.
     * 13. fsync extent dir again (so dir rename is persisted)
     *
     *  This also requires the following behavior on every extent open:
     *   A. If xxx.copy directory found, delete it.
     *   B. If xxx.completed directory found, delete it.
     *   C. If xxx.replace dir found start at step 4 above and continue
     *      on through 6.
     *   D. Only then, open extent.
     */
    pub async fn repair_extent(
        &self,
        eid: usize,
        repair_addr: SocketAddr,
    ) -> Result<(), CrucibleError> {
        // Make sure the extent:
        // is currently closed, matches our eid, is not read-only
        let mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        drop(mg);
        assert!(!self.read_only);

        self.get_extent_copy(eid, repair_addr).await?;

        // Returning from get_extent_copy means we have copied all our
        // files and moved the copy directory to replace directory.
        // Now, replace the current extent files with the replacement ones.
        move_replacement_extent(&self.dir, eid, &self.log)?;

        Ok(())
    }

    /**
     * Connect to the source and pull over all the extent files for the
     * given extent ID.
     * The files are loaded into the copy_dir for the given extent.
     * After all the files have been copied locally, we rename the
     * copy_dir to replace_dir.
     */
    pub async fn get_extent_copy(
        &self,
        eid: usize,
        repair_addr: SocketAddr,
    ) -> Result<(), CrucibleError> {
        // An extent must be closed before we replace its files.
        let mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        drop(mg);

        // Make sure copy, replace, and cleanup directories don't exist yet.
        // We don't need them yet, but if they do exist, then something
        // is wrong.
        let rd = replace_dir(&self.dir, eid as u32);
        if rd.exists() {
            crucible_bail!(
                IoError,
                "Replace directory: {:?} already exists",
                rd,
            );
        }

        let copy_dir = Extent::create_copy_dir(&self.dir, eid)?;
        info!(self.log, "Created copy dir {:?}", copy_dir);

        // XXX TLS someday?  Authentication?
        let url = format!("http://{:?}", repair_addr);
        let repair_server = Client::new(&url);

        let mut repair_files =
            match repair_server.get_files_for_extent(eid as u32).await {
                Ok(f) => f.into_inner(),
                Err(e) => {
                    crucible_bail!(
                        RepairRequestError,
                        "Failed to get repair files: {:?}",
                        e,
                    );
                }
            };

        repair_files.sort();
        info!(
            self.log,
            "eid:{} Found repair files: {:?}", eid, repair_files
        );

        // The repair file list should always contain the extent data
        // file itself, and the .db file (metadata) for that extent.
        // Missing these means the repair will not succeed.
        // Optionally, there could be both .db-shm and .db-wal.
        if !validate_repair_files(eid, &repair_files) {
            crucible_bail!(
                RepairFilesInvalid,
                "Invalid repair file list: {:?}",
                repair_files,
            );
        }

        // First, copy the main extent data file.
        let extent_copy =
            Extent::create_copy_file(copy_dir.clone(), eid, None)?;
        let repair_stream = match repair_server
            .get_extent_file(eid as u32, FileType::Data)
            .await
        {
            Ok(rs) => rs,
            Err(e) => {
                crucible_bail!(
                    RepairRequestError,
                    "Failed to get extent {} data file: {:?}",
                    eid,
                    e,
                );
            }
        };
        save_stream_to_file(extent_copy, repair_stream.into_inner()).await?;

        // The .db file is also required to exist for any valid extent.
        let extent_db = Extent::create_copy_file(
            copy_dir.clone(),
            eid,
            Some(ExtentType::Db),
        )?;
        let repair_stream = match repair_server
            .get_extent_file(eid as u32, FileType::Db)
            .await
        {
            Ok(rs) => rs,
            Err(e) => {
                crucible_bail!(
                    RepairRequestError,
                    "Failed to get extent {} db file: {:?}",
                    eid,
                    e,
                );
            }
        };
        save_stream_to_file(extent_db, repair_stream.into_inner()).await?;

        // These next two are optional.
        for opt_file in &[ExtentType::DbShm, ExtentType::DbWal] {
            let filename = extent_file_name(eid as u32, opt_file.clone());

            if repair_files.contains(&filename) {
                let extent_shm = Extent::create_copy_file(
                    copy_dir.clone(),
                    eid,
                    Some(opt_file.clone()),
                )?;
                let repair_stream = match repair_server
                    .get_extent_file(eid as u32, opt_file.to_file_type())
                    .await
                {
                    Ok(rs) => rs,
                    Err(e) => {
                        crucible_bail!(
                            RepairRequestError,
                            "Failed to get extent {} {} file: {:?}",
                            eid,
                            opt_file,
                            e,
                        );
                    }
                };
                save_stream_to_file(extent_shm, repair_stream.into_inner())
                    .await?;
            }
        }

        // After we have all files: move the repair dir.
        info!(
            self.log,
            "Repair files downloaded, move directory {:?} to {:?}",
            copy_dir,
            rd
        );
        rename(copy_dir.clone(), rd.clone())?;

        // Files are synced in save_stream_to_file(). Now make sure
        // the parent directory containing the repair directory has
        // been synced so that change is persistent.
        let current_dir = extent_dir(&self.dir, eid as u32);

        sync_path(current_dir, &self.log)?;
        Ok(())
    }

    /**
     * if there is a difference between what our actual extent_count is
     * and what is requested, go out and create the new extent files.
     */
    pub async fn extend(&mut self, newsize: u32) -> Result<()> {
        if self.read_only {
            // XXX return CrucibleError instead of anyhow?
            bail!(CrucibleError::ModifyingReadOnlyRegion.to_string());
        }

        if newsize < self.def.extent_count() {
            bail!(
                "will not truncate {} -> {} for now",
                self.def.extent_count(),
                newsize
            );
        }

        if newsize > self.def.extent_count() {
            self.def.set_extent_count(newsize);
            write_json(config_path(&self.dir), &self.def, true)?;
            self.open_extents(true).await?;
        }
        Ok(())
    }

    pub fn region_def(&self) -> (u64, Block, u32) {
        (
            self.def.block_size(),
            self.def.extent_size(),
            self.def.extent_count(),
        )
    }

    pub fn def(&self) -> RegionDefinition {
        self.def
    }

    pub async fn flush_numbers(&self) -> Result<Vec<u64>> {
        let mut result = Vec::with_capacity(self.extents.len());
        for eid in 0..self.extents.len() {
            let extent = self.get_opened_extent(eid).await;
            result.push(extent.inner.lock().await.flush_number()?);
        }

        if result.len() > 12 {
            info!(
                self.log,
                "Current flush_numbers [0..12]: {:?}",
                &result[0..12]
            );
        } else {
            info!(self.log, "Current flush_numbers [0..12]: {:?}", result);
        }

        Ok(result)
    }

    pub async fn gen_numbers(&self) -> Result<Vec<u64>> {
        let mut result = Vec::with_capacity(self.extents.len());
        for eid in 0..self.extents.len() {
            let extent = self.get_opened_extent(eid).await;
            result.push(extent.inner.lock().await.gen_number()?);
        }
        Ok(result)
    }

    pub async fn dirty(&self) -> Result<Vec<bool>> {
        let mut result = Vec::with_capacity(self.extents.len());
        for eid in 0..self.extents.len() {
            let extent = self.get_opened_extent(eid).await;
            result.push(extent.inner.lock().await.dirty()?);
        }
        Ok(result)
    }

    pub fn validate_hashes(
        &self,
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
        for write in writes {
            let computed_hash = if let Some(encryption_context) =
                &write.block_context.encryption_context
            {
                integrity_hash(&[
                    &encryption_context.nonce[..],
                    &encryption_context.tag[..],
                    &write.data[..],
                ])
            } else {
                integrity_hash(&[&write.data[..]])
            };

            if computed_hash != write.block_context.hash {
                error!(self.log, "Failed write hash validation");
                // TODO: print out the extent and block where this failed!!
                crucible_bail!(HashMismatch);
            }
        }

        Ok(())
    }

    #[instrument]
    pub async fn region_write(
        &mut self,
        writes: &[crucible_protocol::Write],
        job_id: JobId,
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        /*
         * Before anything, validate hashes
         */
        self.validate_hashes(writes)?;

        /*
         * Batch writes so they can all be sent to the appropriate extent
         * together.
         */
        let mut batched_writes: HashMap<usize, Vec<&crucible_protocol::Write>> =
            HashMap::new();

        for write in writes {
            let extent_vec = batched_writes
                .entry(write.eid as usize)
                .or_insert_with(Vec::new);
            extent_vec.push(write);
        }

        if only_write_unwritten {
            cdt::os__writeunwritten__start!(|| job_id.0);
        } else {
            cdt::os__write__start!(|| job_id.0);
        }
        for eid in batched_writes.keys() {
            let extent = self.get_opened_extent(*eid).await;
            let writes = batched_writes.get(eid).unwrap();
            extent
                .write(job_id, &writes[..], only_write_unwritten)
                .await?;
        }

        // Mark any extents we sent a write-command to as potentially dirty
        self.dirty_extents.extend(batched_writes.keys());

        if only_write_unwritten {
            cdt::os__writeunwritten__done!(|| job_id.0);
        } else {
            cdt::os__write__done!(|| job_id.0);
        }

        Ok(())
    }

    #[instrument]
    pub async fn region_read(
        &self,
        requests: &[crucible_protocol::ReadRequest],
        job_id: JobId,
    ) -> Result<Vec<crucible_protocol::ReadResponse>, CrucibleError> {
        let mut responses = Vec::with_capacity(requests.len());

        /*
         * Batch reads so they can all be sent to the appropriate extent
         * together.
         *
         * Note: Have to maintain order with reads! The Upstairs expects read
         * responses to be in the same order as read requests, so we can't
         * use a hashmap in the same way that batching writes can.
         */
        let mut eid: Option<u64> = None;
        let mut batched_reads: Vec<&crucible_protocol::ReadRequest> =
            Vec::with_capacity(requests.len());

        cdt::os__read__start!(|| job_id.0);
        for request in requests {
            if let Some(_eid) = eid {
                if request.eid == _eid {
                    batched_reads.push(request);
                } else {
                    let extent = self.get_opened_extent(_eid as usize).await;
                    extent
                        .read(job_id, &batched_reads[..], &mut responses)
                        .await?;

                    eid = Some(request.eid);
                    batched_reads.clear();
                    batched_reads.push(request);
                }
            } else {
                eid = Some(request.eid);
                batched_reads.clear();
                batched_reads.push(request);
            }
        }

        if let Some(_eid) = eid {
            let extent = self.get_opened_extent(_eid as usize).await;
            extent
                .read(job_id, &batched_reads[..], &mut responses)
                .await?;
        }
        cdt::os__read__done!(|| job_id.0);

        Ok(responses)
    }

    /*
     * Send a flush to just the given extent. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub async fn region_flush_extent(
        &self,
        eid: usize,
        gen_number: u64,
        flush_number: u64,
        job_id: JobId, // only used for logging
    ) -> Result<(), CrucibleError> {
        debug!(
            self.log,
            "Flush just extent {} with f:{} and g:{}",
            eid,
            flush_number,
            gen_number
        );

        let extent = self.get_opened_extent(eid).await;
        extent
            .flush(flush_number, gen_number, job_id, &self.log)
            .await?;

        Ok(())
    }

    /*
     * Send a flush to all extents. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub async fn region_flush(
        &mut self,
        flush_number: u64,
        gen_number: u64,
        snapshot_details: &Option<SnapshotDetails>,
        job_id: JobId,
        extent_limit: Option<usize>,
    ) -> Result<(), CrucibleError> {
        // It should be ok to Flush a read-only region, but not take a snapshot.
        // Most likely this read-only region *is* a snapshot, so that's
        // redundant :)
        if self.read_only && snapshot_details.is_some() {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::os__flush__start!(|| job_id.0);

        // Select extents we're going to flush, while respecting the
        // extent_limit if one was provided.
        let dirty_extents: Vec<usize> = match extent_limit {
            None => self.dirty_extents.iter().copied().collect(),
            Some(el) => {
                if el > self.def.extent_count().try_into().unwrap() {
                    crucible_bail!(InvalidExtent);
                }

                self.dirty_extents
                    .iter()
                    .copied()
                    .filter(|x| *x <= el)
                    .collect()
            }
        };

        let extent_count = dirty_extents.len();

        // Spawn parallel tasks for the flush
        let mut join_handles: Vec<JoinHandle<Result<(), CrucibleError>>> =
            Vec::with_capacity(extent_count);

        for eid in &dirty_extents {
            let extent = self.get_opened_extent(*eid).await;
            let log = self.log.clone();
            let jh = tokio::spawn(async move {
                extent.flush(flush_number, gen_number, job_id, &log).await
            });
            join_handles.push(jh);
        }

        // Wait for all flushes to finish - wait until after
        // cdt::os__flush__done to check the results and bail out.
        let mut results = Vec::with_capacity(extent_count);
        for join_handle in join_handles {
            results.push(
                join_handle
                    .await
                    .map_err(|e| CrucibleError::GenericError(e.to_string())),
            );
        }

        cdt::os__flush__done!(|| job_id.0);

        for result in results {
            // If any extent flush failed, then return that as an error. Because
            // the results were all collected above, each extent flush has
            // completed at this point.
            result??;
        }

        // Now everything has succeeded, we can remove these extents from the
        // flush candidates
        match extent_limit {
            None => self.dirty_extents.clear(),
            Some(_) => {
                for eid in &dirty_extents {
                    self.dirty_extents.remove(eid);
                }
            }
        }

        // snapshots currently only work with ZFS
        if cfg!(feature = "zfs_snapshot") {
            if let Some(snapshot_details) = snapshot_details {
                info!(self.log, "Flush and snap request received");
                // Check if the path exists, return an error if it does
                let test_path = format!(
                    "{}/.zfs/snapshot/{}",
                    self.dir.clone().into_os_string().into_string().unwrap(),
                    snapshot_details.snapshot_name,
                );

                if std::path::Path::new(&test_path).is_dir() {
                    crucible_bail!(
                        SnapshotExistsAlready,
                        "{}",
                        snapshot_details.snapshot_name,
                    );
                }

                // Look up dataset name for path (this works with any path, and
                // will return the parent dataset).
                let path =
                    self.dir.clone().into_os_string().into_string().unwrap();

                let dataset_name = std::process::Command::new("zfs")
                    .args(["list", "-pH", "-o", "name", &path])
                    .output()
                    .map_err(|e| {
                        CrucibleError::SnapshotFailed(e.to_string())
                    })?;

                let dataset_name = std::str::from_utf8(&dataset_name.stdout)
                    .map_err(|e| CrucibleError::SnapshotFailed(e.to_string()))?
                    .trim_end(); // remove '\n' from end

                let output = std::process::Command::new("zfs")
                    .args([
                        "snapshot",
                        format!(
                            "{}@{}",
                            dataset_name, snapshot_details.snapshot_name
                        )
                        .as_str(),
                    ])
                    .output()
                    .map_err(|e| {
                        CrucibleError::SnapshotFailed(e.to_string())
                    })?;

                if !output.status.success() {
                    crucible_bail!(
                        SnapshotFailed,
                        "{}",
                        std::str::from_utf8(&output.stderr).map_err(|e| {
                            CrucibleError::GenericError(e.to_string())
                        })?,
                    );
                }
            }
        } else if snapshot_details.is_some() {
            error!(self.log, "Snapshot request received on unsupported binary");
        }
        Ok(())
    }
}

/**
 * Given a path to a directory or file, open it, then fsync it.
 * If the file is already open, then just fsync it yourself.
 */
pub fn sync_path<P: AsRef<Path> + std::fmt::Debug>(
    path: P,
    log: &Logger,
) -> Result<(), CrucibleError> {
    let file = match File::open(&path) {
        Err(e) => {
            crucible_bail!(IoError, "{:?} open fsync failure: {:?}", path, e);
        }
        Ok(f) => f,
    };
    if let Err(e) = file.sync_all() {
        crucible_bail!(IoError, "{:?}: fsync failure: {:?}", path, e);
    }
    debug!(log, "fsync completed for: {:?}", path);

    Ok(())
}

/**
 * Copy the contents of the replacement directory on to the extent
 * files in the extent directory.
 */
pub fn move_replacement_extent<P: AsRef<Path>>(
    region_dir: P,
    eid: usize,
    log: &Logger,
) -> Result<(), CrucibleError> {
    let destination_dir = extent_dir(&region_dir, eid as u32);
    let extent_file_name = extent_file_name(eid as u32, ExtentType::Data);
    let replace_dir = replace_dir(&region_dir, eid as u32);
    let completed_dir = completed_dir(&region_dir, eid as u32);

    assert!(Path::new(&replace_dir).exists());
    assert!(!Path::new(&completed_dir).exists());

    info!(
        log,
        "Copy files from {:?} in {:?}", replace_dir, destination_dir,
    );

    // Setup the original and replacement file names.
    let mut new_file = replace_dir.clone();
    new_file.push(extent_file_name.clone());

    let mut original_file = destination_dir.clone();
    original_file.push(extent_file_name);

    // Copy the new file (the one we copied from the source side) on top
    // of the original file.
    if let Err(e) = std::fs::copy(new_file.clone(), original_file.clone()) {
        crucible_bail!(
            IoError,
            "copy of {:?} to {:?} got: {:?}",
            new_file,
            original_file,
            e
        );
    }
    sync_path(&original_file, log)?;

    new_file.set_extension("db");
    original_file.set_extension("db");
    if let Err(e) = std::fs::copy(new_file.clone(), original_file.clone()) {
        crucible_bail!(
            IoError,
            "copy {:?} to {:?} got: {:?}",
            new_file,
            original_file,
            e
        );
    }
    sync_path(&original_file, log)?;

    // The .db-shm and .db-wal files may or may not exist.  If they don't
    // exist on the source side, then be sure to remove them locally to
    // avoid database corruption from a mismatch between old and new.
    new_file.set_extension("db-shm");
    original_file.set_extension("db-shm");
    if new_file.exists() {
        if let Err(e) = std::fs::copy(new_file.clone(), original_file.clone()) {
            crucible_bail!(
                IoError,
                "copy {:?} to {:?} got: {:?}",
                new_file,
                original_file,
                e
            );
        }
        sync_path(&original_file, log)?;
    } else if original_file.exists() {
        info!(
            log,
            "Remove old file {:?} as there is no replacement",
            original_file.clone()
        );
        std::fs::remove_file(&original_file)?;
    }

    new_file.set_extension("db-wal");
    original_file.set_extension("db-wal");
    if new_file.exists() {
        if let Err(e) = std::fs::copy(new_file.clone(), original_file.clone()) {
            crucible_bail!(
                IoError,
                "copy {:?} to {:?} got: {:?}",
                new_file,
                original_file,
                e
            );
        }
        sync_path(&original_file, log)?;
    } else if original_file.exists() {
        info!(
            log,
            "Remove old file {:?} as there is no replacement",
            original_file.clone()
        );
        std::fs::remove_file(&original_file)?;
    }
    sync_path(&destination_dir, log)?;

    // After we have all files: move the copy dir.
    info!(
        log,
        "Move directory  {:?} to {:?}", replace_dir, completed_dir
    );
    rename(replace_dir, &completed_dir)?;

    sync_path(&destination_dir, log)?;

    std::fs::remove_dir_all(&completed_dir)?;

    sync_path(&destination_dir, log)?;
    Ok(())
}

/**
 * Given:
 *   The stream returned to us from the progenitor endpoint
 *   A local File, already created and opened,
 * Stream the data from the endpoint into the file.
 * When the stream is completed, fsync the file.
 */
pub async fn save_stream_to_file(
    mut file: File,
    mut stream: repair_client::ByteStream,
) -> Result<(), CrucibleError> {
    loop {
        match stream.try_next().await {
            Ok(Some(bytes)) => {
                file.write_all(&bytes)?;
            }
            Ok(None) => break,
            Err(e) => {
                crucible_bail!(
                    RepairStreamError,
                    "repair {:?}: stream error: {:?}",
                    file,
                    e
                );
            }
        }
    }
    if let Err(e) = file.sync_all() {
        crucible_bail!(IoError, "repair {:?}: fsync failure: {:?}", file, e);
    }
    Ok(())
}

struct BatchedPwritevState<'a> {
    byte_offset: u64,
    iovecs: Vec<IoSlice<'a>>,
    next_block_in_run: u64,
}

struct BatchedPwritev<'a> {
    fd: std::os::fd::RawFd,
    capacity: usize,
    state: Option<BatchedPwritevState<'a>>,
    block_size: u64,
    iov_max: usize,
}

impl<'a> BatchedPwritev<'a> {
    pub fn new(
        fd: std::os::fd::RawFd,
        capacity: usize,
        block_size: u64,
        iov_max: usize,
    ) -> Self {
        Self {
            fd,
            capacity,
            state: None,
            block_size,
            iov_max,
        }
    }

    /// Add a write to the batch. If the write would cause the list of iovecs to
    /// be larger than IOV_MAX, then `perform_writes` is called.
    pub fn add_write(
        &mut self,
        write: &'a crucible_protocol::Write,
    ) -> Result<(), CrucibleError> {
        let block = write.offset.value;

        let should_perform_writes = if let Some(state) = &self.state {
            // Is this write contiguous with the last?
            if block == state.next_block_in_run {
                // If so, then add it to the list. Make sure to flush if the
                // total size would become larger than IOV_MAX.
                (state.iovecs.len() + 1) >= self.iov_max
            } else {
                // If not, then flush, and start the state all over.
                true
            }
        } else {
            false
        };

        if should_perform_writes {
            self.perform_writes()?;
        }

        // If perform_writes was called above, then state will be None. If
        // perform_writes was not called, then:
        //
        // - block == state.next_block_in_run, and
        // - (state.iovecs.len() + 1) <= self.iov_max
        //
        // hence the assertion below.
        if let Some(state) = &mut self.state {
            assert_eq!(block, state.next_block_in_run);
            state.iovecs.push(IoSlice::new(&write.data));
            state.next_block_in_run += 1;
        } else {
            // start fresh
            self.state = Some(BatchedPwritevState {
                byte_offset: write.offset.value * self.block_size,
                iovecs: {
                    let mut iovecs = Vec::with_capacity(self.capacity);
                    iovecs.push(IoSlice::new(&write.data));
                    iovecs
                },
                next_block_in_run: block + 1,
            });
        }

        Ok(())
    }

    // Write bytes out to file target
    pub fn perform_writes(&mut self) -> Result<(), CrucibleError> {
        if let Some(state) = &mut self.state {
            assert!(!state.iovecs.is_empty());

            nix::sys::uio::pwritev(
                self.fd,
                &state.iovecs[..],
                state.byte_offset as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;

            self.state = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::rename;
    use std::path::PathBuf;

    use bytes::{BufMut, BytesMut};
    use rand::{Rng, RngCore};
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::dump::dump_region;

    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn new_extent(number: u32) -> Extent {
        let ff = File::open("/dev/null").unwrap();

        let inn = Inner {
            file: ff,
            metadb: Connection::open_in_memory().unwrap(),
            dirty_blocks: BTreeMap::new(),
        };

        /*
         * Note: All the tests expect 512 and 100, so if you change
         * these, then change the tests!
         */
        Extent {
            number,
            read_only: false,
            block_size: 512,
            extent_size: Block::new_512(100),
            iov_max: Extent::get_iov_max().unwrap(),
            inner: Mutex::new(inn),
        }
    }

    static TEST_UUID_STR: &str = "12345678-1111-2222-3333-123456789999";

    fn test_uuid() -> Uuid {
        TEST_UUID_STR.parse().unwrap()
    }

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    fn new_region_options() -> crucible_common::RegionOptions {
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        let block_size = 512;
        region_options.set_block_size(block_size);
        region_options
            .set_extent_size(Block::new(10, block_size.trailing_zeros()));
        region_options.set_uuid(test_uuid());
        region_options
    }

    #[tokio::test]
    async fn region_create_drop_open() -> Result<()> {
        // Create a region, make three extents.
        // Drop the region, then open it.
        let dir = tempdir()?;
        let log = csl();
        let mut region =
            Region::create(&dir, new_region_options(), log.clone()).await?;
        region.extend(3).await?;

        drop(region);

        let _region =
            Region::open(&dir, new_region_options(), true, false, &log).await?;

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_read_version_low() -> Result<()> {
        // Create a region where the read database version is down rev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(0, DATABASE_WRITE_VERSION);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_write_version_low() -> Result<()> {
        // Create a region where the write database version is downrev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());

        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(DATABASE_READ_VERSION, 0);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_read_version_high() -> Result<()> {
        // Create a region where the read database version is too high.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(
            DATABASE_READ_VERSION + 1,
            DATABASE_WRITE_VERSION,
        );
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_write_version_high() -> Result<()> {
        // Create a region where the write database version is too high.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());

        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(
            DATABASE_READ_VERSION,
            DATABASE_WRITE_VERSION + 1,
        );
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn copy_extent_dir() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Remove the copy directory, make sure it goes away.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let cp = copy_dir(&dir, 1);

        assert!(Extent::create_copy_dir(&dir, 1).is_ok());
        assert!(Path::new(&cp).exists());
        assert!(remove_copy_cleanup_dir(&dir, 1).is_ok());
        assert!(!Path::new(&cp).exists());
        Ok(())
    }

    #[tokio::test]
    async fn copy_extent_dir_twice() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Verify a second create will fail.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(3).await.unwrap();

        Extent::create_copy_dir(&dir, 1).unwrap();
        let res = Extent::create_copy_dir(&dir, 1);
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn close_extent() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        let (gen, flush, dirty) = region.close_extent(1).await.unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for a new unwritten extent.
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(!dirty);

        // Make copy directory for this extent
        let cp = Extent::create_copy_dir(&dir, 1)?;
        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Make sure the copy directory is gone
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_one() -> Result<()> {
        // Verify the copy directory is removed if an extent is
        // opened with that directory present.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Extent::create_copy_dir(&dir, 1)?;

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Make sure copy directory was removed
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_two() -> Result<()> {
        // Verify that the completed directory is removed if present
        // when an extent is re-opened.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Extent::create_copy_dir(&dir, 1)?;

        // Step through the replacement dir, but don't do any work.
        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Finish up the fake repair, but leave behind the completed dir.
        let cd = completed_dir(&dir, 1);
        rename(rd.clone(), cd.clone())?;

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay() -> Result<()> {
        // Verify on extent open that a replacement directory will
        // have it's contents replace an extents existing data and
        // metadata files.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Extent::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db-shm");
        dest_path.set_extension("db-shm");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db-wal");
        dest_path.set_extension("db-wal");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Now we have a replace directory, we verify that special
        // action is taken when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay_short() -> Result<()> {
        // test move_replacement_extent(), create a copy dir, populate it
        // and let the reopen do the work.  This time we make sure our
        // copy dir only has extent data and .db files, and not .db-shm
        // nor .db-wal.  Verify these files are delete from the original
        // extent after the reopen has cleaned them up.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Extent::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        println!("cp {:?} to {:?}", source_path, dest_path);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // The close may remove the db-shm and db-wal files, manually
        // create them here, just to verify they are removed after the
        // reopen as they are not included in the files to be recovered
        // and this test exists to verify they will be deleted.
        let mut invalid_db = extent_path(&dir, 1);
        invalid_db.set_extension("db-shm");
        println!("Recreate {:?}", invalid_db);
        std::fs::copy(source_path.clone(), invalid_db.clone())?;
        assert!(Path::new(&invalid_db).exists());

        invalid_db.set_extension("db-wal");
        println!("Recreate {:?}", invalid_db);
        std::fs::copy(source_path.clone(), invalid_db.clone())?;
        assert!(Path::new(&invalid_db).exists());

        // Now we have a replace directory populated and our files to
        // delete are ready.  We verify that special action is taken
        // when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Make sure there is no longer a db-shm and db-wal
        dest_path.set_extension("db-shm");
        assert!(!Path::new(&dest_path).exists());
        dest_path.set_extension("db-wal");
        assert!(!Path::new(&dest_path).exists());

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_no_replay_readonly() -> Result<()> {
        // Verify on a read-only region a replacement directory will
        // be ignored.  This is required by the dump command, as it would
        // be tragic if the command to inspect a region changed that
        // region's contents in the act of inspecting.

        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Make copy directory for this extent
        let _ext_one = region.get_opened_extent(1).await;
        let cp = Extent::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp, rd.clone())?;

        drop(region);

        // Open up the region read_only now.
        let region =
            Region::open(&dir, new_region_options(), false, true, &csl())
                .await?;

        // Verify extent 1 has opened again.
        let _ext_one = region.get_opened_extent(1).await;

        // Make sure repair directory is still present
        assert!(Path::new(&rd).exists());

        Ok(())
    }

    #[test]
    fn validate_repair_files_empty() {
        // No repair files is a failure
        assert!(!validate_repair_files(1, &Vec::new()));
    }

    #[test]
    fn validate_repair_files_good() {
        // This is an expected list of files for an extent
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];

        assert!(validate_repair_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_also_good() {
        // This is also an expected list of files for an extent
        let good_files: Vec<String> =
            vec!["001".to_string(), "001.db".to_string()];
        assert!(validate_repair_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_duplicate() {
        // duplicate file names for extent 2
        let good_files: Vec<String> =
            vec!["002".to_string(), "002".to_string()];
        assert!(!validate_repair_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_duplicate_pair() {
        // duplicate file names for extent 2
        let good_files: Vec<String> = vec![
            "002".to_string(),
            "002".to_string(),
            "002.db".to_string(),
            "002.db".to_string(),
        ];
        assert!(!validate_repair_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_quad_duplicate() {
        // This is an expected list of files for an extent
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-shm".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_offbyon() {
        // Incorrect file names for extent 2
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];

        assert!(!validate_repair_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_too_good() {
        // Duplicate file in list
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_not_good_enough() {
        // We require 2 or 4 files, not 3
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
    }

    #[tokio::test]
    async fn reopen_all_extents() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(5).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Close extent 4
        region.close_extent(4).await.unwrap();
        assert!(matches!(
            *region.extents[4].lock().await,
            ExtentState::Closed
        ));

        // Reopen all extents
        region.reopen_all_extents().await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Verify extent four is valid
        let ext_four = region.get_opened_extent(4).await;

        // Make sure the eid matches
        assert_eq!(ext_four.number, 4);

        Ok(())
    }

    #[tokio::test]
    async fn new_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl()).await;
        Ok(())
    }

    #[tokio::test]
    async fn new_existing_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl()).await;
        let _ = Region::open(&dir, new_region_options(), false, false, &csl())
            .await;
        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn bad_import_region() {
        let _ = Region::open(
            "/tmp/12345678-1111-2222-3333-123456789999/notadir",
            new_region_options(),
            false,
            false,
            &csl(),
        )
        .await
        .unwrap();
    }

    #[test]
    fn extent_io_valid() {
        let ext = new_extent(0);
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        ext.check_input(Block::new_512(0), &data).unwrap();
        ext.check_input(Block::new_512(99), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_large() {
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        let ext = new_extent(0);
        ext.check_input(Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_small() {
        let mut data = BytesMut::with_capacity(511);
        data.put(&[1; 511][..]);

        let ext = new_extent(0);
        ext.check_input(Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_bad_block() {
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        let ext = new_extent(0);
        ext.check_input(Block::new_512(100), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent(0);
        ext.check_input(Block::new_512(99), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent(0);
        ext.check_input(Block::new_512(1), &data).unwrap();
    }

    #[test]
    fn extent_name_basic() {
        assert_eq!(extent_file_name(4, ExtentType::Data), "004");
    }

    #[test]
    fn extent_name_basic_ext() {
        assert_eq!(extent_file_name(4, ExtentType::Db), "004.db");
    }

    #[test]
    fn extent_name_basic_ext_shm() {
        assert_eq!(extent_file_name(4, ExtentType::DbShm), "004.db-shm");
    }

    #[test]
    fn extent_name_basic_ext_wal() {
        assert_eq!(extent_file_name(4, ExtentType::DbWal), "004.db-wal");
    }

    #[test]
    fn extent_name_basic_two() {
        assert_eq!(extent_file_name(10, ExtentType::Data), "00A");
    }

    #[test]
    fn extent_name_basic_three() {
        assert_eq!(extent_file_name(59, ExtentType::Data), "03B");
    }

    #[test]
    fn extent_name_max() {
        assert_eq!(extent_file_name(u32::MAX, ExtentType::Data), "FFF");
    }

    #[test]
    fn extent_name_min() {
        assert_eq!(extent_file_name(u32::MIN, ExtentType::Data), "000");
    }

    #[test]
    fn extent_dir_basic() {
        assert_eq!(extent_dir("/var/region", 4), p("/var/region/00/000/"));
    }

    #[test]
    fn extent_dir_max() {
        assert_eq!(
            extent_dir("/var/region", u32::MAX),
            p("/var/region/FF/FFF")
        );
    }

    #[test]
    fn extent_dir_min() {
        assert_eq!(
            extent_dir("/var/region", u32::MIN),
            p("/var/region/00/000/")
        );
    }

    #[test]
    fn extent_path_min() {
        assert_eq!(
            extent_path("/var/region", u32::MIN),
            p("/var/region/00/000/000")
        );
    }

    #[test]
    fn copy_path_basic() {
        assert_eq!(
            copy_dir("/var/region", 4),
            p("/var/region/00/000/004.copy")
        );
    }

    #[test]
    fn extent_path_three() {
        assert_eq!(extent_path("/var/region", 3), p("/var/region/00/000/003"));
    }

    #[test]
    fn extent_path_mid_hi() {
        assert_eq!(
            extent_path("/var/region", 65536),
            p("/var/region/00/010/000")
        );
    }

    #[test]
    fn extent_path_mid_lo() {
        assert_eq!(
            extent_path("/var/region", 65535),
            p("/var/region/00/00F/FFF")
        );
    }

    #[test]
    fn extent_path_max() {
        assert_eq!(
            extent_path("/var/region", u32::MAX),
            p("/var/region/FF/FFF/FFF")
        );
    }

    #[tokio::test]
    async fn dump_a_region() -> Result<()> {
        /*
         * Create a region, give it actual size
         */
        let dir = tempdir()?;
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(2).await?;

        /*
         * Build the Vec for our region dir
         */
        let dvec = vec![dir.into_path()];

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn dump_two_region() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;
        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        let mut r2 = Region::create(&dir2, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(2).await?;
        r2.extend(2).await?;

        /*
         * Build the Vec for our region dirs
         */
        let mut dvec = Vec::new();
        let pdir = dir.into_path();
        dvec.push(pdir);
        let pdir = dir2.into_path();
        dvec.push(pdir);

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn dump_extent() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;

        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(3).await?;
        let mut r2 = Region::create(&dir2, new_region_options(), csl())
            .await
            .unwrap();
        r2.extend(3).await?;

        /*
         * Build the Vec for our region dirs
         */
        let mut dvec = Vec::new();
        let pdir = dir.into_path();
        dvec.push(pdir);
        let pdir = dir2.into_path();
        dvec.push(pdir);

        /*
         * Dump the region
         */
        dump_region(dvec, Some(2), None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let ext = region.get_opened_extent(0).await;
        let mut inner = ext.inner.lock().await;

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_empty());
        assert!(inner.get_block_contexts(1, 1)?[0].is_empty());

        // Set and verify block 0's context

        inner.set_block_contexts(&[&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: [1, 2, 3].to_vec(),
                    tag: [4, 5, 6, 7].to_vec(),
                }),
                hash: 123,
            },
            block: 0,
            on_disk_hash: 456,
        }])?;

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();

        assert_eq!(ctxs.len(), 1);

        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![1, 2, 3]
        );
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![4, 5, 6, 7]
        );
        assert_eq!(ctxs[0].block_context.hash, 123);
        assert_eq!(ctxs[0].on_disk_hash, 456);

        // Block 1 should still be blank

        assert!(inner.get_block_contexts(1, 1)?[0].is_empty());

        // Set and verify a new context for block 0

        let blob1 = rand::thread_rng().gen::<[u8; 32]>();
        let blob2 = rand::thread_rng().gen::<[u8; 32]>();

        inner.set_block_contexts(&[&DownstairsBlockContext {
            block_context: BlockContext {
                encryption_context: Some(EncryptionContext {
                    nonce: blob1.to_vec(),
                    tag: blob2.to_vec(),
                }),
                hash: 1024,
            },
            block: 0,
            on_disk_hash: 65536,
        }])?;

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();

        assert_eq!(ctxs.len(), 2);

        // First context didn't change
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![1, 2, 3]
        );
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![4, 5, 6, 7]
        );
        assert_eq!(ctxs[0].block_context.hash, 123);
        assert_eq!(ctxs[0].on_disk_hash, 456);

        // Second context was appended
        assert_eq!(
            ctxs[1]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            blob1
        );
        assert_eq!(
            ctxs[1]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            blob2
        );
        assert_eq!(ctxs[1].block_context.hash, 1024);
        assert_eq!(ctxs[1].on_disk_hash, 65536);

        // "Flush", so only the rows that match should remain.
        inner.truncate_encryption_contexts_and_hashes(
            vec![(0, 65536)].into_iter(),
        )?;

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();

        assert_eq!(ctxs.len(), 1);

        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            blob1
        );
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            blob2
        );
        assert_eq!(ctxs[0].block_context.hash, 1024);
        assert_eq!(ctxs[0].on_disk_hash, 65536);

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_context_insert() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let ext = region.get_opened_extent(0).await;
        let mut inner = ext.inner.lock().await;

        assert!(inner.get_block_contexts(0, 1)?[0].is_empty());

        // Submit the same contents for block 0 over and over, simulating a user
        // writing the same unencrypted contents to the same offset.

        for _ in 0..10 {
            inner.set_block_contexts(&[&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 123,
                },
                block: 0,
                on_disk_hash: 123,
            }])?;
        }

        // Duplicate rows should not be inserted

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();
        assert_eq!(ctxs.len(), 1);

        // Truncation should still work

        inner.truncate_encryption_contexts_and_hashes(
            vec![(0, 123)].into_iter(),
        )?;

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();
        assert_eq!(ctxs.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let ext = region.get_opened_extent(0).await;
        let mut inner = ext.inner.lock().await;

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_block_contexts(0, 1)?[0].is_empty());
        assert!(inner.get_block_contexts(1, 1)?[0].is_empty());

        // Set block 0's and 1's context

        inner.set_block_contexts(&[
            &DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: [1, 2, 3].to_vec(),
                        tag: [4, 5, 6, 7].to_vec(),
                    }),
                    hash: 123,
                },
                block: 0,
                on_disk_hash: 456,
            },
            &DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: Some(EncryptionContext {
                        nonce: [4, 5, 6].to_vec(),
                        tag: [8, 9, 0, 1].to_vec(),
                    }),
                    hash: 9999,
                },
                block: 1,
                on_disk_hash: 1234567890,
            },
        ])?;

        // Verify block 0's context

        let ctxs = inner.get_block_contexts(0, 1)?[0].clone();

        assert_eq!(ctxs.len(), 1);

        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![1, 2, 3]
        );
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![4, 5, 6, 7]
        );
        assert_eq!(ctxs[0].block_context.hash, 123);
        assert_eq!(ctxs[0].on_disk_hash, 456);

        // Verify block 1's context

        let ctxs = inner.get_block_contexts(1, 1)?[0].clone();

        assert_eq!(ctxs.len(), 1);

        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![4, 5, 6]
        );
        assert_eq!(
            ctxs[0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![8, 9, 0, 1]
        );
        assert_eq!(ctxs[0].block_context.hash, 9999);
        assert_eq!(ctxs[0].on_disk_hash, 1234567890);

        // Return both block 0's and block 1's context, and verify

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert_eq!(ctxs[0].len(), 1);
        assert_eq!(
            ctxs[0][0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![1, 2, 3]
        );
        assert_eq!(
            ctxs[0][0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![4, 5, 6, 7]
        );
        assert_eq!(ctxs[0][0].block_context.hash, 123);
        assert_eq!(ctxs[0][0].on_disk_hash, 456);

        assert_eq!(ctxs[1].len(), 1);
        assert_eq!(
            ctxs[1][0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .nonce,
            vec![4, 5, 6]
        );
        assert_eq!(
            ctxs[1][0]
                .block_context
                .encryption_context
                .as_ref()
                .unwrap()
                .tag,
            vec![8, 9, 0, 1]
        );
        assert_eq!(ctxs[1][0].block_context.hash, 9999);
        assert_eq!(ctxs[1][0].on_disk_hash, 1234567890);

        // Append a whole bunch of block context rows

        for i in 0..10 {
            inner.set_block_contexts(&[
                &DownstairsBlockContext {
                    block_context: BlockContext {
                        encryption_context: Some(EncryptionContext {
                            nonce: rand::thread_rng()
                                .gen::<[u8; 32]>()
                                .to_vec(),
                            tag: rand::thread_rng().gen::<[u8; 32]>().to_vec(),
                        }),
                        hash: rand::thread_rng().gen::<u64>(),
                    },
                    block: 0,
                    on_disk_hash: i,
                },
                &DownstairsBlockContext {
                    block_context: BlockContext {
                        encryption_context: Some(EncryptionContext {
                            nonce: rand::thread_rng()
                                .gen::<[u8; 32]>()
                                .to_vec(),
                            tag: rand::thread_rng().gen::<[u8; 32]>().to_vec(),
                        }),
                        hash: rand::thread_rng().gen::<u64>(),
                    },
                    block: 1,
                    on_disk_hash: i,
                },
            ])?;
        }

        let ctxs = inner.get_block_contexts(0, 2)?;
        assert_eq!(ctxs[0].len(), 11);
        assert_eq!(ctxs[1].len(), 11);

        // "Flush", so only the rows that match the on-disk hash should remain.

        inner.truncate_encryption_contexts_and_hashes(
            vec![(0, 6), (1, 7)].into_iter(),
        )?;

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert_eq!(ctxs[0].len(), 1);
        assert_eq!(ctxs[0][0].on_disk_hash, 6);

        assert_eq!(ctxs[1].len(), 1);
        assert_eq!(ctxs[1][0].on_disk_hash, 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_big_write() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), false).await?;

        // read data into File, compare what was written to buffer

        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let mut data = std::fs::read(path).expect("Unable to read file");

            read_from_files.append(&mut data);
        }

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read

        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_open_removes_partial_writes() -> Result<()> {
        // Opening a dirty extent should fully rehash the extent to remove any
        // contexts that don't correlate with data on disk. This is necessary
        // for write_unwritten to work after a crash, and to move us into a
        // good state for flushes (flushes only clear out contexts for blocks
        // written since the last flush)
        //
        // Specifically, this test checks for the case where we had a brand new
        // block and a write to that blocks that failed such that only the
        // write's block context was persisted, leaving the data all zeros. In
        // this case, there is no data, so we should remove the invalid block
        // context row.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // A write of some sort only wrote a block context row
        {
            let ext = region.get_opened_extent(0).await;
            let mut inner = ext.inner.lock().await;

            inner.set_block_contexts(&[&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 1024,
                },
                block: 0,
                on_disk_hash: 65536,
            }])?;
        }

        // This should clear out the invalid contexts
        for eid in 0..region.extents.len() {
            region.close_extent(eid).await.unwrap();
        }

        region.reopen_all_extents().await?;

        // Verify no block context rows exist
        {
            let ext = region.get_opened_extent(0).await;
            let inner = ext.inner.lock().await;
            assert!(inner.get_block_contexts(0, 1)?[0].is_empty());
        }

        // Assert write unwritten will write to the first block

        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        region
            .region_write(
                &[crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(0),
                    data,
                    block_context: BlockContext {
                        encryption_context: None,
                        hash,
                    },
                }],
                JobId(124),
                true, // only_write_unwritten
            )
            .await?;

        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest {
                    eid: 0,
                    offset: Block::new_512(0),
                }],
                JobId(125),
            )
            .await?;

        let response = &responses[0];

        assert_eq!(response.data.to_vec(), vec![0x55; 512]);

        Ok(())
    }

    #[tokio::test]
    /// It's very important that only_write_unwritten always works correctly.
    /// We should never add contexts for blocks that haven't been written by
    /// an upstairs.
    async fn test_fully_rehash_and_clean_does_not_mark_blocks_as_written(
    ) -> Result<()> {
        // Make a region. It's empty, and should stay that way
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let ext = region.get_opened_extent(0).await;

        // Write a block, but don't flush.
        {
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
            ext.write(JobId(10), &[&write], false).await?;
        }

        // We haven't flushed, but this should leave our context in place.
        ext.fully_rehash_and_clean_all_stale_contexts(false).await?;

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
                block_context: block_context.clone(),
            };
            ext.write(JobId(20), &[&write], true).await?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            ext.read(JobId(21), &[&read], &mut resp).await?;

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
                block_context: block_context.clone(),
            };
            ext.write(JobId(30), &[&write], true).await?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(1),
            };
            ext.read(JobId(31), &[&read], &mut resp).await?;

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

    /// If a write successfully put a context into the database, but it never
    /// actually got the data onto the disk, that block should revert back to
    /// being "unwritten". After all, the data never was truly written.
    ///
    /// This test is very similar to test_region_open_removes_partial_writes,
    /// but is distinct in that it call fully_rehash directly, without closing
    /// and re-opening the extent.
    #[tokio::test]
    async fn test_fully_rehash_marks_blocks_unwritten_if_data_never_hit_disk(
    ) -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let ext = region.get_opened_extent(0).await;

        // Partial write, the data never hits disk, but there's a context
        // in the DB
        {
            let mut inner = ext.inner.lock().await;
            inner.set_block_contexts(&[&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 1024,
                },
                block: 0,
                on_disk_hash: 65536,
            }])?;
        }

        // Run a full rehash, which should clear out that partial write.
        ext.fully_rehash_and_clean_all_stale_contexts(false).await?;

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
                block_context: block_context.clone(),
            };
            ext.write(JobId(30), &[&write], true).await?;

            let mut resp = Vec::new();
            let read = ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            };
            ext.read(JobId(31), &[&read], &mut resp).await?;

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

    #[tokio::test]
    async fn test_ok_hash_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 5061083712412462836,
                },
            }];

        region.region_write(&writes, JobId(0), false).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_empty() -> Result<()> {
        // Verify that a read fill does write to a block when there is
        // no data written yet.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s (random)
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9's
                },
            }];

        region.region_write(&writes, JobId(0), true).await?;

        // Verify the dirty bit is now set.
        // We know our EID, so we can shortcut to getting the actual extent.
        assert!(region.get_opened_extent(eid as usize).await.dirty().await);

        // Now read back that block, make sure it is updated.
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(0),
            )
            .await?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 1);
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_written() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s (random)
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9's
                },
            }];

        region.region_write(&writes, JobId(0), false).await?;

        // Same block, now try to write something else to it.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 5061083712412462836, // hash for all 1s
                },
            }];
        // Do the write again, but with only_write_unwritten set now.
        region.region_write(&writes, JobId(1), true).await?;

        // Now read back that block, make sure it has the first write
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(2),
            )
            .await?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_written_flush() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.  This time run a flush after the
        // first write.  Verify correct state of dirty bit as well.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await?;

        // Verify the dirty bit is now set.
        assert!(region.get_opened_extent(eid as usize).await.dirty().await);

        // Flush extent with eid, fn, gen
        region
            .region_flush_extent(eid as usize, 1, 1, JobId(1))
            .await?;

        // Verify the dirty bit is no longer set.
        assert!(!region.get_opened_extent(eid as usize).await.dirty().await);

        // Create a new write IO with different data.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 5061083712412462836, // hash for all 1s
                },
            }];

        // Do the write again, but with only_write_unwritten set now.
        region.region_write(&writes, JobId(1), true).await?;

        // Verify the dirty bit is not set.
        assert!(!region.get_opened_extent(eid as usize).await.dirty().await);

        // Read back our block, make sure it has the first write data
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(2),
            )
            .await?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write() -> Result<()> {
        // Do a multi block write where all blocks start new (unwritten)
        // Verify only empty blocks have data.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let mut data = std::fs::read(path).expect("Unable to read file");

            read_from_files.append(&mut data);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_0() -> Result<()> {
        // Do a write to block zero, then do a multi block write with
        // only_write_unwritten set. Verify block zero is the first write, and
        // the remaining blocks have the contents from the multi block fill.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];

        // Now write just one block
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let mut data = std::fs::read(path).expect("Unable to read file");

            read_from_files.append(&mut data);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_1() -> Result<()> {
        // Write to the second block, then do a multi block fill.
        // Verify the second block has the original data we wrote, and all
        // the other blocks have the data from the multi block fill.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);

        // Construct the write for the second block on the first EID.
        let eid = 0;
        let offset = Block::new_512(1);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s,
                },
            }];

        // Now write just to the second block.
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        // send write_unwritten command.
        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(1024).skip(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let mut data = std::fs::read(path).expect("Unable to read file");

            read_from_files.append(&mut data);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_final() -> Result<()> {
        // Do a write to the fourth block, then do a multi block read fill
        // where the last block of the read fill is what we wrote to in
        // our first write.
        // verify the fourth block has the original write, and the first
        // three blocks have the data from the multi block read fill.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(5).await?;

        let ddef = region.def();
        // A bunch of things expect a 512, so let's make it explicit.
        assert_eq!(ddef.block_size(), 512);
        let num_blocks: usize = 4;
        let total_size: usize = ddef.block_size() as usize * num_blocks;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);

        // Construct the write for the second block on the first EID.
        let eid = 0;
        let offset = Block::new_512(3);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];

        // Now write just to the second block.
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill four blocks
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        println!("buffer size:{}", buffer.len());
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        // send only_write_unwritten command.
        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our
        // expected buffer for the final block to have that original data.
        for a_buf in buffer.iter_mut().take(2048).skip(1536) {
            *a_buf = 9;
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            println!("Read eid: {}, {} offset: {:?}", eid, i, offset);
            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
            println!("Read a region, append");
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_sparse() -> Result<()> {
        // Do a multi block write_unwritten where a few different blocks have
        // data. Verify only unwritten blocks get the data.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(4).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"s
        let blocks_to_write = [1, 3, 7, 8, 11, 12, 13];
        for b in blocks_to_write {
            let data = BytesMut::from(&[9u8; 512][..]);
            let eid: u64 = b as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((b as u64) % ddef.extent_size().value);

            // Write a few different blocks
            let writes: Vec<crucible_protocol::Write> =
                vec![crucible_protocol::Write {
                    eid,
                    offset,
                    data: data.freeze(),
                    block_context: BlockContext {
                        encryption_context: Some(
                            crucible_protocol::EncryptionContext {
                                nonce: vec![1, 2, 3],
                                tag: vec![4, 5, 6],
                            },
                        ),
                        hash: 4798852240582462654, // Hash for all 9s
                    },
                }];

            // Now write just one block
            region.region_write(&writes, JobId(0), false).await?;
        }

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // Because we did write_unwritten, the block we already written should
        // still have the data from the first write.  Update our buffer
        // for these blocks to have that original data.
        for b in blocks_to_write {
            let b_start = b * 512;
            let b_end = b_start + 512;
            for a_buf in buffer.iter_mut().take(b_end).skip(b_start) {
                *a_buf = 9;
            }
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    // A test function to return a generic'ish write command.
    // We use the "all 9's data" and checksum.
    fn create_generic_write(
        eid: u64,
        offset: crucible::Block,
    ) -> Vec<crucible_protocol::Write> {
        let data = BytesMut::from(&[9u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];
        writes
    }

    #[tokio::test]
    async fn test_flush_extent_limit_base() {
        // Check that the extent_limit value in region_flush is honored
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(2).await.unwrap();

        // Write to extent 0 block 0 first
        let writes = create_generic_write(0, Block::new_512(0));
        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Now write to extent 1 block 0
        let writes = create_generic_write(1, Block::new_512(0));

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(0).await.dirty().await);
        assert!(region.get_opened_extent(1).await.dirty().await);

        // Call flush, but limit the flush to extent 0
        region
            .region_flush(1, 2, &None, JobId(3), Some(0))
            .await
            .unwrap();

        // Verify the dirty bit is no longer set for 0, but still set
        // for extent 1.
        assert!(!region.get_opened_extent(0).await.dirty().await);
        assert!(region.get_opened_extent(1).await.dirty().await);
    }

    #[tokio::test]
    async fn test_flush_extent_limit_end() {
        // Check that the extent_limit value in region_flush is honored
        // Write to the last block in the extents.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(3).await.unwrap();

        // Write to extent 1 block 9 first
        let writes = create_generic_write(1, Block::new_512(9));
        region.region_write(&writes, JobId(1), true).await.unwrap();

        // Now write to extent 2 block 9
        let writes = create_generic_write(2, Block::new_512(9));
        region.region_write(&writes, JobId(2), true).await.unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(1).await.dirty().await);
        assert!(region.get_opened_extent(2).await.dirty().await);

        // Call flush, but limit the flush to extents < 2
        region
            .region_flush(1, 2, &None, JobId(3), Some(1))
            .await
            .unwrap();

        // Verify the dirty bit is no longer set for 1, but still set
        // for extent 2.
        assert!(!region.get_opened_extent(1).await.dirty().await);
        assert!(region.get_opened_extent(2).await.dirty().await);

        // Now flush with no restrictions.
        region
            .region_flush(1, 2, &None, JobId(3), None)
            .await
            .unwrap();

        // Extent 2 should no longer be dirty
        assert!(!region.get_opened_extent(2).await.dirty().await);
    }

    #[tokio::test]
    async fn test_flush_extent_limit_walk_it_off() {
        // Check that the extent_limit value in region_flush is honored
        // Write to all the extents, then flush them one at a time.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(10).await.unwrap();

        // Write to extents 0 to 9
        let mut job_id = 1;
        for ext in 0..10 {
            let writes = create_generic_write(ext, Block::new_512(5));
            region
                .region_write(&writes, JobId(job_id), true)
                .await
                .unwrap();
            job_id += 1;
        }

        // Verify the dirty bit is now set for all extents.
        for ext in 0..10 {
            assert!(region.get_opened_extent(ext).await.dirty().await);
        }

        // Walk up the extent_limit, verify at each flush extents are
        // flushed.
        for ext in 0..10 {
            println!("Send flush to extent limit {}", ext);
            region
                .region_flush(1, 2, &None, JobId(3), Some(ext))
                .await
                .unwrap();

            // This ext should no longer be dirty.
            println!("extent {} should not be dirty now", ext);
            assert!(!region.get_opened_extent(ext).await.dirty().await);

            // Any extent above the current point should still be dirty.
            for d_ext in ext + 1..10 {
                println!("verify {} still dirty", d_ext);
                assert!(region.get_opened_extent(d_ext).await.dirty().await);
            }
        }
    }

    #[tokio::test]
    async fn test_flush_extent_limit_too_large() {
        // Check that the extent_limit value in region_flush will return
        // an error if the extent_limit is too large.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Call flush with an invalid extent
        assert!(region
            .region_flush(1, 2, &None, JobId(3), Some(2))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_extent_write_flush_close() {
        // Verify that a write then close of an extent will return the
        // expected gen flush and dirty bits for that extent.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Flush extent with eid, fn, gen
        region
            .region_flush_extent(eid as usize, 3, 2, JobId(1))
            .await
            .unwrap();

        // Close extent 0
        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for the write that should be flushed now.
        assert_eq!(gen, 3);
        assert_eq!(flush, 2);
        assert!(!dirty);
    }

    #[tokio::test]
    async fn test_extent_close_reopen_flush_close() {
        // Do several extent open close operations, verifying that the
        // gen/flush/dirty return values are as expected.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Close extent 0 without a flush
        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Because we did not flush yet, this extent should still have
        // the values for an unwritten extent, except for the dirty bit.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Open the extent, then close it again, the values should remain
        // the same as the previous check (testing here that dirty remains
        // dirty).
        region.reopen_extent(eid as usize).await.unwrap();

        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify everything is the same, and dirty is still set.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Reopen, then flush extent with eid, fn, gen
        region.reopen_extent(eid as usize).await.unwrap();
        region
            .region_flush_extent(eid as usize, 4, 9, JobId(1))
            .await
            .unwrap();

        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify after flush that g,f are updated, and that dirty
        // is no longer set.
        assert_eq!(gen, 4);
        assert_eq!(flush, 9);
        assert!(!dirty);
    }

    #[tokio::test]
    /// We need to make sure that a flush will properly adjust the DB hashes
    /// after issuing multiple writes to different disconnected sections of
    /// an extent
    async fn test_flush_after_multiple_disjoint_writes() -> Result<()> {
        let dir = tempdir()?;
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(1024));
        let mut region =
            Region::create(&dir, region_opts, csl()).await.unwrap();
        region.extend(1).await.unwrap();

        // Write some data to 3 different areas
        let ranges = [(0..120), (243..244), (487..903)];

        // We will write these ranges multiple times so that there's multiple
        // hashes in the DB, so we need multiple sets of data.
        let writes: Vec<Vec<Vec<crucible_protocol::Write>>> = (0..3)
            .map(|_| {
                ranges
                    .iter()
                    .map(|range| {
                        range
                            .clone()
                            .map(|idx| {
                                // Generate data for this block
                                let data = thread_rng().gen::<[u8; 512]>();
                                let hash = integrity_hash(&[&data]);
                                crucible_protocol::Write {
                                    eid: 0,
                                    offset: Block::new_512(idx),
                                    data: Bytes::copy_from_slice(&data),
                                    block_context: BlockContext {
                                        hash,
                                        encryption_context: None,
                                    },
                                }
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect();

        // Write all the writes
        for write_iteration in &writes {
            for write_chunk in write_iteration {
                region.region_write(write_chunk, JobId(0), false).await?;
            }
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).await?;

        // We are gonna compare against the last write iteration
        let last_writes = writes.last().unwrap();

        let ext = region.get_opened_extent(0).await;
        let inner = ext.inner.lock().await;

        for (i, range) in ranges.iter().enumerate() {
            // Get the contexts for the range
            let ctxts = inner
                .get_block_contexts(range.start, range.end - range.start)?;

            // Every block should have at most 1 block
            assert_eq!(
                ctxts.iter().map(|block_ctxts| block_ctxts.len()).max(),
                Some(1)
            );

            // Now that we've checked that, flatten out for an easier eq
            let actual_ctxts: Vec<_> = ctxts
                .iter()
                .flatten()
                .map(|downstairs_context| &downstairs_context.block_context)
                .collect();

            // What we expect is the hashes for the last write we did
            let expected_ctxts: Vec<_> = last_writes[i]
                .iter()
                .map(|write| &write.block_context)
                .collect();

            // Check that they're right.
            assert_eq!(expected_ctxts, actual_ctxts);
        }

        Ok(())
    }

    #[tokio::test]
    /// This test ensures that our flush logic works even for full-extent
    /// flushes. That's the case where the set of modified blocks will be full.
    async fn test_big_extent_full_write_and_flush() -> Result<()> {
        let dir = tempdir()?;

        const EXTENT_SIZE: u64 = 4096;
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(EXTENT_SIZE));
        let mut region =
            Region::create(&dir, region_opts, csl()).await.unwrap();
        region.extend(1).await.unwrap();

        // writing the entire region a few times over before the flush.
        let writes: Vec<Vec<crucible_protocol::Write>> = (0..3)
            .map(|_| {
                (0..EXTENT_SIZE)
                    .map(|idx| {
                        // Generate data for this block
                        let data = thread_rng().gen::<[u8; 512]>();
                        let hash = integrity_hash(&[&data]);
                        crucible_protocol::Write {
                            eid: 0,
                            offset: Block::new_512(idx),
                            data: Bytes::copy_from_slice(&data),
                            block_context: BlockContext {
                                hash,
                                encryption_context: None,
                            },
                        }
                    })
                    .collect()
            })
            .collect();

        // Write all the writes
        for write_iteration in &writes {
            region
                .region_write(write_iteration, JobId(0), false)
                .await?;
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).await?;

        // compare against the last write iteration
        let last_writes = writes.last().unwrap();

        let ext = region.get_opened_extent(0).await;
        let inner = ext.inner.lock().await;

        // Get the contexts for the range
        let ctxts = inner.get_block_contexts(0, EXTENT_SIZE)?;

        // Every block should have at most 1 block
        assert_eq!(
            ctxts.iter().map(|block_ctxts| block_ctxts.len()).max(),
            Some(1)
        );

        // Now that we've checked that, flatten out for an easier eq
        let actual_ctxts: Vec<_> = ctxts
            .iter()
            .flatten()
            .map(|downstairs_context| &downstairs_context.block_context)
            .collect();

        // What we expect is the hashes for the last write we did
        let expected_ctxts: Vec<_> = last_writes
            .iter()
            .map(|write| &write.block_context)
            .collect();

        // Check that they're right.
        assert_eq!(expected_ctxts, actual_ctxts);

        Ok(())
    }

    #[tokio::test]
    async fn test_bad_hash_bad() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 2398419238764,
                },
            }];

        let result = region.region_write(&writes, JobId(0), false).await;

        assert!(result.is_err());

        match result.err().unwrap() {
            CrucibleError::HashMismatch => {
                // ok
            }
            _ => {
                panic!("Incorrect error with hash mismatch");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_blank_block_read_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest {
                    eid: 0,
                    offset: Block::new_512(0),
                }],
                JobId(0),
            )
            .await?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 0);
        assert_eq!(responses[0].data[..], [0u8; 512][..]);

        Ok(())
    }

    async fn prepare_random_region(
    ) -> Result<(tempfile::TempDir, Region, Vec<u8>)> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let ddef = region.def();
        let total_size = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Write in random data
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), false).await?;

        Ok((dir, region, buffer))
    }

    #[tokio::test]
    async fn test_read_single_large_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a single large contiguous range
        let requests: Vec<crucible::ReadRequest> = (1..8)
            .map(|i| crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(i),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data[512..(8 * 512)],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_single_large_contiguous_span_extents() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a single large contiguous range that spans
        // multiple extents
        let requests: Vec<crucible::ReadRequest> = (9..28)
            .map(|i| crucible_protocol::ReadRequest {
                eid: i / 10,
                offset: Block::new_512(i % 10),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data[(9 * 512)..(28 * 512)],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_multiple_disjoint_large_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a multiple disjoint large contiguous ranges
        let requests: Vec<crucible::ReadRequest> = vec![
            (1..4)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
            (15..24)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
            (27..28)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
        ]
        .into_iter()
        .flatten()
        .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &[
                &data[512..(4 * 512)],
                &data[(15 * 512)..(24 * 512)],
                &data[(27 * 512)..(28 * 512)],
            ]
            .concat(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_multiple_disjoint_none_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a multiple disjoint non-contiguous ranges
        let requests: Vec<crucible::ReadRequest> = vec![
            crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            },
            crucible_protocol::ReadRequest {
                eid: 1,
                offset: Block::new_512(4),
            },
            crucible_protocol::ReadRequest {
                eid: 1,
                offset: Block::new_512(9),
            },
            crucible_protocol::ReadRequest {
                eid: 2,
                offset: Block::new_512(4),
            },
        ];

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &[
                &data[0..512],
                &data[(14 * 512)..(15 * 512)],
                &data[(19 * 512)..(20 * 512)],
                &data[(24 * 512)..(25 * 512)],
            ]
            .concat(),
        );

        Ok(())
    }

    fn prepare_writes(
        offsets: std::ops::Range<usize>,
        data: &mut [u8],
    ) -> Vec<crucible_protocol::Write> {
        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(offsets.len());
        let mut rng = rand::thread_rng();

        for i in offsets {
            let mut buffer: Vec<u8> = vec![0; 512];
            rng.fill_bytes(&mut buffer);

            // alter data as writes are prepared
            data[(i * 512)..((i + 1) * 512)].copy_from_slice(&buffer[..]);

            let hash = integrity_hash(&[&buffer]);

            writes.push(crucible_protocol::Write {
                eid: (i as u64) / 10,
                offset: Block::new_512((i as u64) % 10),
                data: Bytes::from(buffer),
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        assert!(!writes.is_empty());

        writes
    }

    async fn validate_whole_region(region: &Region, data: &[u8]) -> Result<()> {
        let num_blocks = region.def().extent_size().value
            * region.def().extent_count() as u64;

        let requests: Vec<crucible_protocol::ReadRequest> = (0..num_blocks)
            .map(|i| crucible_protocol::ReadRequest {
                eid: i / 10,
                offset: Block::new_512(i % 10),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(1)).await?;

        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_single_large_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a single large contiguous range
        let writes = prepare_writes(1..8, &mut data);

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_single_large_contiguous_span_extents() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = prepare_writes(9..28, &mut data);

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_multiple_disjoint_large_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a multiple disjoint large contiguous ranges
        let writes = vec![
            prepare_writes(1..4, &mut data),
            prepare_writes(15..24, &mut data),
            prepare_writes(27..28, &mut data),
        ]
        .concat();

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_multiple_disjoint_none_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a multiple disjoint non-contiguous ranges
        let writes = vec![
            prepare_writes(0..1, &mut data),
            prepare_writes(14..15, &mut data),
            prepare_writes(19..20, &mut data),
            prepare_writes(24..25, &mut data),
        ]
        .concat();

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_single_large_contiguous() -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range
        let writes = prepare_writes(1..8, &mut data);

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_single_large_contiguous_span_extents(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = prepare_writes(9..28, &mut data);

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_multiple_disjoint_large_contiguous(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a multiple disjoint large contiguous ranges
        let writes = vec![
            prepare_writes(1..4, &mut data),
            prepare_writes(15..24, &mut data),
            prepare_writes(27..28, &mut data),
        ]
        .concat();

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_multiple_disjoint_none_contiguous(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a multiple disjoint non-contiguous ranges
        let writes = vec![
            prepare_writes(0..1, &mut data),
            prepare_writes(14..15, &mut data),
            prepare_writes(19..20, &mut data),
            prepare_writes(24..25, &mut data),
        ]
        .concat();

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }
}
