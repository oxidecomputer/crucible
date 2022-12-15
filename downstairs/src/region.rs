// Copyright 2021 Oxide Computer Company
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::fs::{File, OpenOptions, rename};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Result};
use futures::TryStreamExt;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::macros::support::Pin;
use tracing::instrument;

use crucible_common::*;
use crucible_protocol::{EncryptionContext, SnapshotDetails};
use repair_client::types::FileType;
use repair_client::Client;

use super::*;

#[derive(Debug)]
pub struct Extent {
    number: u32,
    read_only: bool,
    block_size: u64,
    extent_size: Block,
    /// Inner contains information about the actual extent file that holds
    /// the data, and the metadata (stored in the database) about that
    /// extent.
    ///
    /// If Some(), it means the extent file and database metadata for
    /// it are opened.
    /// If None, it means the extent is currently
    /// closed (and possibly being updated out of band).
    inner: Option<Mutex<Inner>>,
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

    /// Map of block number to on_disk_hash of the block. Stores blocks that
    /// have been written since the last flush.
    dirty_blocks: HashMap<usize, u64>,
}

impl Inner {
    pub fn gen_number(&self) -> Result<u64> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='gen_number'",
        )?;
        let gen_number_iter = stmt.query_map([], |row| row.get(0))?;

        let mut gen_number_values: Vec<u64> = vec![];
        for gen_number_value in gen_number_iter {
            gen_number_values.push(gen_number_value?);
        }

        assert!(gen_number_values.len() == 1);

        Ok(gen_number_values[0])
    }

    pub fn flush_number(&self) -> Result<u64> {
        let mut stmt = self.metadb.prepare_cached(
            "SELECT value FROM metadata where name='flush_number'",
        )?;
        let flush_number_iter = stmt.query_map([], |row| row.get(0))?;

        let mut flush_number_values: Vec<u64> = vec![];
        for flush_number_value in flush_number_iter {
            flush_number_values.push(flush_number_value?);
        }

        assert!(flush_number_values.len() == 1);

        Ok(flush_number_values[0])
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
        let dirty_iter = stmt.query_map([], |row| row.get(0))?;

        let mut dirty_values: Vec<bool> = vec![];
        for dirty_value in dirty_iter {
            dirty_values.push(dirty_value?);
        }

        assert!(dirty_values.len() == 1);

        Ok(dirty_values[0])
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
        // NOTE: "ORDER BY RANDOM()" would be a good --lossy addition here
        let stmt =
            "SELECT block, hash, nonce, tag, on_disk_hash FROM block_context \
             WHERE block BETWEEN ?1 AND ?2 \
             ORDER BY ROWID ASC";
        let mut stmt = self.metadb.prepare_cached(stmt)?;

        let stmt_iter =
            stmt.query_map(params![block, block + count - 1], |row| {
                let block_index: u64 = row.get(0)?;
                let hash: Vec<u8> = row.get(1)?;
                let nonce: Option<Vec<u8>> = row.get(2)?;
                let tag: Option<Vec<u8>> = row.get(3)?;
                let on_disk_hash: Vec<u8> = row.get(4)?;

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
                    hash: u64::from_le_bytes(hash[..].try_into()?),
                    encryption_context,
                },
                block: block_index,
                on_disk_hash: u64::from_le_bytes(on_disk_hash[..].try_into()?),
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
            "INSERT INTO block_context (block, hash, nonce, tag, on_disk_hash) \
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
            block_context.block_context.hash.to_le_bytes(),
            nonce,
            tag,
            block_context.on_disk_hash.to_le_bytes(),
        ])?;

        assert_eq!(rows_affected, 1);

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
    /// calling this function.
    fn truncate_encryption_contexts_and_hashes(
        &mut self,
        extent_block_indexes_and_hashes: Vec<(usize, u64)>,
    ) -> Result<()> {
        let tx = self.metadb.transaction()?;

        let stmt = "DELETE FROM block_context where block == ?1 and on_disk_hash != ?2";
        let mut stmt = tx.prepare_cached(stmt)?;

        let n_blocks = extent_block_indexes_and_hashes.len();

        cdt::extent__context__truncate__start!(|| n_blocks as u64);
        for (block, on_disk_hash) in extent_block_indexes_and_hashes {
            let _rows_affected =
                stmt.execute(params![block, on_disk_hash.to_le_bytes()])?;
        }
        cdt::extent__context__truncate__done!(|| ());

        drop(stmt);

        tx.commit()?;

        Ok(())
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
    let metadb = Connection::open(&path)?;

    assert!(metadb.is_autocommit());
    metadb.pragma_update(None, "journal_mode", &"WAL")?;
    metadb.pragma_update(None, "synchronous", &"FULL")?;

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
    /**
     * Open an existing extent file at the location requested.
     * Read in the metadata from the first block of the file.
     */
    fn open<P: AsRef<Path>>(
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
            inner: Some(Mutex::new(Inner {
                file,
                metadb,
                dirty_blocks: HashMap::new(),
            })),
        };

        // Clean out any irrelevant block contexts, which may be present if downstairs
        // crashed between a write() and a flush().
        extent.fully_rehash_and_clean_all_stale_contexts(false)?;

        Ok(extent)
    }

    /**
     * Close an extent and the metadata db files for it.
     */
    pub fn close(&mut self) -> Result<()> {
        let inner = self.inner.as_ref().unwrap().lock().unwrap();
        drop(inner);
        self.inner = None;
        Ok(())
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

            metadb.execute(
                "CREATE TABLE block_context (
                    block INTEGER,
                    hash BLOB NOT NULL,
                    nonce BLOB,
                    tag BLOB,
                    on_disk_hash BLOB NOT NULL,
                    PRIMARY KEY (block, hash, nonce, tag, on_disk_hash)
                )",
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
            inner: Some(Mutex::new(Inner {
                file,
                metadb,
                dirty_blocks: HashMap::new(),
            })),
        })
    }

    /**
     * Create the copy directory for this extent.
     */
    fn create_copy_dir<P: AsRef<Path>>(
        &self,
        dir: P,
    ) -> Result<PathBuf, CrucibleError> {
        let cp = copy_dir(dir, self.number);

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
        &self,
        mut copy_dir: PathBuf,
        extension: Option<ExtentType>,
    ) -> Result<File> {
        // Get the base extent name before we consider the actual Type
        let name = extent_file_name(self.number, ExtentType::Data);
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

    pub fn inner(&self) -> MutexGuard<Inner> {
        self.inner.as_ref().unwrap().lock().unwrap()
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    /// Read the real data off underlying storage, and get block metadata. If
    /// an error occurs while processing any of the requests, the state of
    /// `responses` is undefined.
    #[instrument]
    pub fn read(
        &self,
        job_id: u64,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
    ) -> Result<(), CrucibleError> {
        cdt::extent__read__start!(|| {
            (job_id, self.number, requests.len() as u64)
        });
        let mut inner = self.inner();

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
            let n_contiguous_requests = requests[req_run_start..]
                .windows(2)
                .take_while(|reqs| {
                    reqs[0].offset.value + 1 == reqs[1].offset.value
                })
                .count()
                + 1;

            // Create our responses and push them into the output. While we're
            // at it, check for overflows
            let resp_run_start = responses.len();
            for req in requests[req_run_start..][..n_contiguous_requests].iter()
            {
                let resp =
                    ReadResponse::from_request(req, self.block_size as usize);
                self.check_input(req.offset, &resp.data)?;
                responses.push(resp);
            }

            // Finally we get to read the actual data. That's why we're here
            cdt::extent__read__file__start!(|| {
                (job_id, self.number, n_contiguous_requests as u64)
            });
            inner.file.seek(SeekFrom::Start(
                first_req.offset.value * self.block_size,
            ))?;
            let mut read_buffer = BytesMut::with_capacity(
                n_contiguous_requests * self.block_size as usize,
            );
            read_buffer.resize(read_buffer.capacity(), 0);
            inner.file.read_exact(&mut read_buffer)?;
            cdt::extent__read__file__done!(|| {
                (job_id, self.number, n_contiguous_requests as u64)
            });

            // Query the block metadata
            cdt::extent__read__get__contexts__start!(|| {
                (job_id, self.number, n_contiguous_requests as u64)
            });
            let block_contexts = inner.get_block_contexts(
                first_req.offset.value,
                n_contiguous_requests as u64,
            )?;
            cdt::extent__read__get__contexts__done!(|| {
                (job_id, self.number, n_contiguous_requests as u64)
            });

            // Now it's time to put everything into the responses.
            // We use into_iter here to move values out of enc_ctxts/hashes,
            // avoiding a clone(). For code consistency, we use iters for the
            // response and data chunks too. These iters will be the same length
            // (equal to n_contiguous_requests) so zipping is fine
            let resp_iter =
                responses[resp_run_start..][..n_contiguous_requests].iter_mut();
            let ctx_iter = block_contexts.into_iter();
            let data_iter = read_buffer.chunks_exact(self.block_size as usize);

            // We could make this a little cleaner if we pulled in itertools and
            // used multizip from that, but i don't think it's worth it.
            for ((resp, r_ctx), r_data) in
            resp_iter.zip(ctx_iter).zip(data_iter)
            {
                // Shove everything into the response
                resp.block_contexts =
                    r_ctx.into_iter().map(|x| x.block_context).collect();

                // XXX if resp.data was Bytes instead of BytesMut we could avoid
                // a copy here and instead assign it to a frozen subslice.
                resp.data.copy_from_slice(r_data);
            }

            req_run_start += n_contiguous_requests;
        }

        cdt::extent__read__done!(|| {
            (job_id, self.number, requests.len() as u64)
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
    pub fn write(
        &self,
        job_id: u64,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::extent__write__start!(|| {
            (job_id, self.number, writes.len() as u64)
        });

        let mut inner = self.inner();
        // I realize this looks like some nonsense but what this is doing is
        // borrowing the inner up-front from the MutexGuard, which will allow
        // us to later disjointly borrow fields. Basically, we're helping the
        // borrow-checker do its job.
        let inner = &mut *inner;

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
                (job_id, self.number, writes.len() as u64)
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
                (job_id, self.number, writes.len() as u64)
            });

            if writes_to_skip.len() == writes.len() {
                // Nothing to do
                cdt::extent__write__done!(|| {
                    (job_id, self.number, writes.len() as u64)
                });
                return Ok(());
            }
        }

        inner.set_dirty()?;

        // Write all the metadata to the DB
        // TODO right now we're including the integrity_hash() time in the sqlite time. It's small in
        // comparison right now, but worth being aware of when looking at dtrace numbers
        cdt::extent__write__sqlite__insert__start!(|| {
            (job_id, self.number, writes.len() as u64)
        });
        let tx = inner.metadb.transaction()?;
        for write in writes {
            if writes_to_skip.contains(&write.offset.value) {
                debug_assert!(only_write_unwritten);
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
            // tx_set_block_context would mark blocks as dirty, since it lets
            // us easily test specific edge-cases of the database state.
            // Could make another function that wraps tx_set_block_context
            // and handles this as well.
            let _ = inner
                .dirty_blocks
                .insert(write.offset.value as usize, on_disk_hash);
        }
        tx.commit()?;

        cdt::extent__write__sqlite__insert__done!(|| {
            (job_id, self.number, writes.len() as u64)
        });

        // Buffer writes for fewer syscalls. The 65536 buffer size here is
        // chosen somewhat arbitrarily.
        let mut write_buffer = [0u8; 65536];

        // PERFORMANCE TODO: While sqlite is the bulk of our performance cost
        // in writes, we're still paying quite the price on small writes
        // here. It would be nice if we could improve that a bit. An easy win
        // would be to replace the seek+write with the syscall that does both
        // in one go. I also wonder whether memory-mapping would be
        // advantageous here.
        //
        // Something else worth considering for small writes is that, based on
        // my memory of conversations we had with propolis folks about what
        // OSes expect out of an NVMe driver, I believe our contract with the
        // upstairs doesn't require us to have the writes inside the file
        // until after a flush() returns. If that is indeed true, we could
        // buffer a certain amount of writes, only actually writing that
        // buffer when either a flush is issued or the buffer exceeds some
        // set size(based on our memory constraints). This would have
        // benefits on any workload that frequently writes to the same block
        // between flushes, would have benefits for small contiguous writes
        // issued over multiple write commands by letting us batch them into
        // a larger write, and(speculation) may benefit non-contiguous writes
        // by cutting down the number of sqlite transactions. But, it
        // introduces complexity.
        cdt::extent__write__file__start!(|| {
            (job_id, self.number, writes.len() as u64)
        });
        let mut bytes_in_run = 0;
        let mut next_block_in_run = u64::MAX;
        for write in writes {
            let block = write.offset.value;
            if writes_to_skip.contains(&block) {
                debug_assert!(only_write_unwritten);
                continue;
            }

            // If the current write isn't contiguous with previous writes,
            // write our buffer to the file and seek.
            if block != next_block_in_run {
                if bytes_in_run > 0 {
                    inner.file.write_all(&write_buffer[..bytes_in_run])?;
                    bytes_in_run = 0;
                }

                // Write any buffered data, and then seek
                inner.file.seek(SeekFrom::Start(block * self.block_size))?;
            }

            // If the write buffer is full, write out to the file
            if write_buffer.len() - bytes_in_run < self.block_size as usize {
                inner.file.write_all(&write_buffer[..bytes_in_run])?;
                bytes_in_run = 0;
            }

            write_buffer[bytes_in_run..][..self.block_size as usize]
                .copy_from_slice(&write.data);
            bytes_in_run += self.block_size as usize;

            next_block_in_run = block + 1;
        }

        // Write any remaining buffered data
        if bytes_in_run > 0 {
            inner.file.write_all(&write_buffer[..bytes_in_run])?;
        }
        cdt::extent__write__file__done!(|| {
            (job_id, self.number, writes.len() as u64)
        });

        cdt::extent__write__done!(|| {
            (job_id, self.number, writes.len() as u64)
        });

        Ok(())
    }

    #[instrument]
    pub fn flush(
        &self,
        new_flush: u64,
        new_gen: u64,
        job_id: u64,
        log: &Logger,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner();

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

        cdt::extent__flush__start!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        /*
         * We must first fsync to get any outstanding data written to disk.
         * This must be done before we update the flush number.
         */
        cdt::extent__flush__file__start!(|| {
            (job_id, self.number, self.extent_size.value)
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
            (job_id, self.number, self.extent_size.value)
        });

        // Clear old block contexts. In order to be crash consistent, only
        // perform this after the extent fsync is done. For each block written
        // since the last flush, remove all block context rows where the integrity
        // hash does not map the last-written value. This is safe, because we
        // know the process has not crashed since those values were written.
        // When the region is first opened, the entire file is rehashed, since
        // in that case we don't have that luxury.

        cdt::extent__flush__collect__hashes__start!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        let mut extent_block_indexes_and_hashes: Vec<(usize, u64)> =
            inner.dirty_blocks.drain().collect();

        // Sorting here is extremely important to get good performance out of
        // truncate_encryption_contexts_and_hashes() later.
        //
        // Benchmarks show that sorting by just block doubles how fast this sort
        // happens compared to a sort_unstable() that considers block and value.
        extent_block_indexes_and_hashes
            .sort_unstable_by(|(l, _), (r, _)| l.cmp(r));

        // We could shrink this to 0, but it's nice not to have to expand the
        // dirty_blocks HashMap on small writes. They have a hard enough time
        // as it is. We don't want our maps to be keeping a lot of memory
        // allocated though. With 16 entries max, even a 32TiB region of
        // 128MiB extents would only be spending ~64MiB of ram on on this,
        // across the entire region. There's potential for tuning here.
        inner.dirty_blocks.shrink_to(16);

        cdt::extent__flush__collect__hashes__done!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        cdt::extent__flush__sqlite__insert__start!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        let extent_block_indexes_and_hashes = inner.dirty_blocks.drain();

        inner.truncate_encryption_contexts_and_hashes(
            extent_block_indexes_and_hashes,
        )?;

        // It'd be nice not to allocate here on small writes. They have a hard
        // enough time as it is. We don't want our maps to be keeping a lot of
        // memory allocated though. With 16 entries max, even a 32TiB region
        // of 128MiB extents would only be spending 64MiB of ram on on this.
        // Potential for tuning here.
        inner.dirty_blocks.shrink_to(16);

        cdt::extent__flush__rehash__done!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        cdt::extent__flush__sqlite__insert__start!(|| {
            (job_id, self.number, self.extent_size.value)
        });
        inner.truncate_encryption_contexts_and_hashes(
            extent_block_indexes_and_hashes,
        )?;
        cdt::extent__flush__sqlite__insert__done!(|| {
            (job_id, self.number, self.extent_size.value)
        });

        // Reset the file's seek offset to 0, and set the flush number and gen
        // number
        inner.file.seek(SeekFrom::Start(0))?;

        inner.set_flush_number(new_flush, new_gen)?;

        cdt::extent__flush__done!(|| {
            (job_id, self.number, self.extent_size.value)
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
    fn fully_rehash_and_clean_all_stale_contexts(
        &self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner();

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

        // Buffer the file so we dont spend all day waiting on syscall
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
            extent_block_indexes_and_hashes,
        )?;

        inner.file.seek(SeekFrom::Start(0))?;

        // XXX should we be clearing the dirty flag here? If we do, should we
        // also increment the generation/flush number like flush() does?
        // We don't have them coming in during the open, so if we updated them
        // we'd need to increment the database values, and who are we to assume
        // we're an authority on the matter here?

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
    pub extents: Vec<Extent>,
    read_only: bool,
    log: Logger,
}

impl Region {
    /**
     * Create a new region based on the given RegionOptions
     */
    pub fn create<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        log: Logger,
    ) -> Result<Region> {
        options.validate()?;

        let cp = config_path(dir.as_ref());
        /*
         * If the file exists, then exit now with error.  If the caller
         * wants a new region, they have to delete the old one first.
         */
        if Path::new(&cp).exists() {
            bail!("Config file already exists {:?}", cp);
        }
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::from_options(&options).unwrap();
        write_json(&cp, &def, false)?;
        info!(log, "Created new region file {:?}", cp);

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            read_only: false,
            log,
        };

        region.open_extents(true)?;

        Ok(region)
    }

    /**
     * Open an existing region file
     */
    pub fn open<P: AsRef<Path>>(
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
        let def = match read_json(&cp) {
            Ok(def) => def,
            Err(e) => bail!("Error {:?} opening region config {:?}", e, cp),
        };

        if verbose {
            info!(log, "Opened existing region file {:?}", cp);
        }

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            read_only,
            log: log.clone(),
        };

        region.open_extents(false)?;

        Ok(region)
    }

    pub fn encrypted(&self) -> bool {
        self.def.get_encrypted()
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
    fn open_extents(&mut self, create: bool) -> Result<()> {
        let next_eid = self.extents.len() as u32;

        let these_extents = (next_eid..self.def.extent_count())
            .into_iter()
            .map(|eid| {
                if create {
                    Extent::create(&self.dir, &self.def, eid)
                } else {
                    Extent::open(
                        &self.dir,
                        &self.def,
                        eid,
                        self.read_only,
                        &self.log,
                    )
                }
            })
            .collect::<Result<Vec<Extent>>>()?;

        self.extents.extend(these_extents);

        for eid in next_eid..self.def.extent_count() {
            assert_eq!(self.extents[eid as usize].number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());

        Ok(())
    }

    /**
     * Walk the list of all extents and find any that are not open.
     * Open any extents that are not.
     */
    pub fn reopen_all_extents(&mut self) -> Result<()> {
        let mut to_open = Vec::new();
        for (i, extent) in self.extents.iter().enumerate() {
            if extent.inner.is_none() {
                to_open.push(i);
            }
        }

        for eid in to_open {
            self.reopen_extent(eid as usize)?;
        }

        Ok(())
    }

    /**
     * Re open an extent that was previously closed
     */
    pub fn reopen_extent(&mut self, eid: usize) -> Result<(), CrucibleError> {
        /*
         * Make sure the extent :
         *
         * - is currently closed
         * - matches our eid
         * - is not read-only
         */
        assert!(self.extents[eid].inner.is_none());
        assert_eq!(self.extents[eid].number, eid as u32);
        assert!(!self.read_only);

        let new_extent = Extent::open(
            &self.dir,
            &self.def,
            eid as u32,
            self.read_only,
            &self.log,
        )?;
        self.extents[eid] = new_extent;
        Ok(())
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
        &mut self,
        eid: usize,
        repair_addr: SocketAddr,
    ) -> Result<(), CrucibleError> {
        // Make sure the extent:
        // is currently closed, matches our eid, is not read-only
        assert!(self.extents[eid].inner.is_none());
        assert_eq!(self.extents[eid].number, eid as u32);
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
        &mut self,
        eid: usize,
        repair_addr: SocketAddr,
    ) -> Result<(), CrucibleError> {
        // An extent must be closed before we replace its files.
        assert!(self.extents[eid].inner.is_none());

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

        let extent = &self.extents[eid];
        let copy_dir = extent.create_copy_dir(&self.dir)?;
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
        let extent_copy = extent.create_copy_file(copy_dir.clone(), None)?;
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
        let extent_db =
            extent.create_copy_file(copy_dir.clone(), Some(ExtentType::Db))?;
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
                let extent_shm = extent.create_copy_file(
                    copy_dir.clone(),
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

        sync_path(&current_dir, &self.log)?;
        Ok(())
    }

    /**
     * if there is a difference between what our actual extent_count is
     * and what is requested, go out and create the new extent files.
     */
    pub fn extend(&mut self, newsize: u32) -> Result<()> {
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
            self.open_extents(true)?;
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

    pub fn flush_numbers(&self) -> Result<Vec<u64>> {
        let mut ver = self
            .extents
            .iter()
            .map(|e| e.inner().flush_number())
            .collect::<Result<Vec<_>>>()?;

        if ver.len() > 12 {
            ver = ver[0..12].to_vec();
        }
        info!(self.log, "Current flush_numbers [0..12]: {:?}", ver);

        self.extents
            .iter()
            .map(|e| e.inner().flush_number())
            .collect::<Result<Vec<_>>>()
    }

    pub fn gen_numbers(&self) -> Result<Vec<u64>> {
        self.extents
            .iter()
            .map(|e| e.inner().gen_number())
            .collect::<Result<Vec<_>>>()
    }

    pub fn dirty(&self) -> Result<Vec<bool>> {
        self.extents
            .iter()
            .map(|e| e.inner().dirty())
            .collect::<Result<Vec<_>>>()
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
                crucible_bail!(HashMismatch);
            }
        }

        Ok(())
    }

    #[instrument]
    pub fn region_write(
        &self,
        writes: &[crucible_protocol::Write],
        job_id: u64,
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
            cdt::os__writeunwritten__start!(|| job_id);
        } else {
            cdt::os__write__start!(|| job_id);
        }
        for eid in batched_writes.keys() {
            let extent = &self.extents[*eid];
            let writes = batched_writes.get(eid).unwrap();
            extent.write(job_id, &writes[..], only_write_unwritten)?;
        }
        if only_write_unwritten {
            cdt::os__writeunwritten__done!(|| job_id);
        } else {
            cdt::os__write__done!(|| job_id);
        }

        Ok(())
    }

    #[instrument]
    pub fn region_read(
        &self,
        requests: &[crucible_protocol::ReadRequest],
        job_id: u64,
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

        cdt::os__read__start!(|| job_id);
        for request in requests {
            if let Some(_eid) = eid {
                if request.eid == _eid {
                    batched_reads.push(request);
                } else {
                    let extent = &self.extents[_eid as usize];
                    extent.read(job_id, &batched_reads[..], &mut responses)?;

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
            let extent = &self.extents[_eid as usize];
            extent.read(job_id, &batched_reads[..], &mut responses)?;
        }
        cdt::os__read__done!(|| job_id);

        Ok(responses)
    }

    /*
     * Send a flush to just the given extent. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub fn region_flush_extent(
        &self,
        eid: usize,
        flush_number: u64,
        gen_number: u64,
        job_id: u64,
    ) -> Result<(), CrucibleError> {
        info!(
            self.log,
            "Flush just extent {} with f:{} and g:{}",
            eid,
            flush_number,
            gen_number
        );

        let extent = &self.extents[eid];
        extent.flush(flush_number, gen_number, 0, &self.log)?;

        Ok(())
    }

    /*
     * Send a flush to all extents. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub fn region_flush(
        &self,
        flush_number: u64,
        gen_number: u64,
        snapshot_details: &Option<SnapshotDetails>,
        job_id: u64,
    ) -> Result<(), CrucibleError> {
        // It should be ok to Flush a read-only region, but not take a snapshot.
        // Most likely this read-only region *is* a snapshot, so that's
        // redundant :)
        if self.read_only && snapshot_details.is_some() {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        // XXX How to we convert between usize and u32 correctly?
        cdt::os__flush__start!(|| job_id);
        for eid in 0..self.def.extent_count() {
            let extent = &self.extents[eid as usize];
            extent.flush(flush_number, gen_number, job_id, &self.log)?;
        }
        cdt::os__flush__done!(|| job_id);

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
    info!(log, "fsync completed for: {:?}", path);

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
    mut stream: Pin<
        Box<
            dyn futures::Stream<
                Item=std::result::Result<crucible::Bytes, reqwest::Error>,
            > + std::marker::Send,
        >,
    >,
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
            dirty_blocks: HashMap::new(),
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
            inner: Some(Mutex::new(inn)),
        }
    }

    static TEST_UUID_STR: &str = "12345678-1111-2222-3333-123456789999";

    fn test_uuid() -> Uuid {
        TEST_UUID_STR.parse().unwrap()
    }

    // Create a simple logger
    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
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

    #[test]
    fn copy_extent_dir() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Remove the copy directory, make sure it goes away.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        let ext_one = &mut region.extents[1];
        let cp = copy_dir(&dir, 1);

        assert!(ext_one.create_copy_dir(&dir).is_ok());
        assert!(Path::new(&cp).exists());
        assert!(remove_copy_cleanup_dir(&dir, 1).is_ok());
        assert!(!Path::new(&cp).exists());
        Ok(())
    }

    #[test]
    fn copy_extent_dir_twice() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Verify a second create will fail.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3).unwrap();

        let ext_one = &mut region.extents[1];
        ext_one.create_copy_dir(&dir).unwrap();
        let res = ext_one.create_copy_dir(&dir);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn close_extent() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Make copy directory for this extent
        let cp = ext_one.create_copy_dir(&dir)?;
        // Reopen extent 1
        region.reopen_extent(1)?;

        // Verify extent one is valid
        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Make sure the copy directory is gone
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[test]
    fn reopen_extent_cleanup_one() -> Result<()> {
        // Verify the copy directory is removed if an extent is
        // opened with that directory present.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Make copy directory for this extent
        let cp = ext_one.create_copy_dir(&dir)?;

        // Reopen extent 1
        region.reopen_extent(1)?;

        // Verify extent one is valid
        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure copy directory was removed
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[test]
    fn reopen_extent_cleanup_two() -> Result<()> {
        // Verify that the completed directory is removed if present
        // when an extent is re-opened.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Make copy directory for this extent
        let cp = ext_one.create_copy_dir(&dir)?;

        // Step through the replacement dir, but don't do any work.
        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Finish up the fake repair, but leave behind the completed dir.
        let cd = completed_dir(&dir, 1);
        rename(rd.clone(), cd.clone())?;

        // Reopen extent 1
        region.reopen_extent(1)?;

        // Verify extent one is valid
        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[test]
    fn reopen_extent_cleanup_replay() -> Result<()> {
        // Verify on extent open that a replacement directory will
        // have it's contents replace an extents existing data and
        // metadata files.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Make copy directory for this extent
        let cp = ext_one.create_copy_dir(&dir)?;

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
        region.reopen_extent(1)?;

        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[test]
    fn reopen_extent_cleanup_replay_short() -> Result<()> {
        // test move_replacement_extent(), create a copy dir, populate it
        // and let the reopen do the work.  This time we make sure our
        // copy dir only has extent data and .db files, and not .db-shm
        // nor .db-wal.  Verify these files are delete from the original
        // extent after the reopen has cleaned them up.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Make copy directory for this extent
        let cp = ext_one.create_copy_dir(&dir)?;

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
        region.reopen_extent(1)?;

        // Make sure there is no longer a db-shm and db-wal
        dest_path.set_extension("db-shm");
        assert!(!Path::new(&dest_path).exists());
        dest_path.set_extension("db-wal");
        assert!(!Path::new(&dest_path).exists());

        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[test]
    fn reopen_extent_no_replay_readonly() -> Result<()> {
        // Verify on a read-only region a replacement directory will
        // be ignored.  This is required by the dump command, as it would
        // be tragic if the command to inspect a region changed that
        // region's contents in the act of inspecting.

        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

        // Make copy directory for this extent
        let ext_one = &mut region.extents[1];
        let cp = ext_one.create_copy_dir(&dir)?;

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
        let mut region =
            Region::open(&dir, new_region_options(), false, true, &csl())?;

        // Verify extent 1 has opened again.
        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

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

    #[test]
    fn reopen_all_extents() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(5)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(ext_one.inner.is_none());

        // Close extent 4
        let ext_four = &mut region.extents[4];
        ext_four.close()?;
        assert!(ext_four.inner.is_none());

        // Reopen all extents
        region.reopen_all_extents()?;

        // Verify extent one is valid
        let ext_one = &mut region.extents[1];
        assert!(ext_one.inner.is_some());

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Verify extent four is valid
        let ext_four = &mut region.extents[4];
        assert!(ext_four.inner.is_some());

        // Make sure the eid matches
        assert_eq!(ext_four.number, 4);
        Ok(())
    }

    #[test]
    fn new_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl());
        Ok(())
    }

    #[test]
    fn new_existing_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl());
        let _ = Region::open(&dir, new_region_options(), false, false, &csl());
        Ok(())
    }

    #[test]
    #[should_panic]
    fn bad_import_region() {
        let _ = Region::open(
            &"/tmp/12345678-1111-2222-3333-123456789999/notadir",
            new_region_options(),
            false,
            false,
            &csl(),
        )
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

    #[test]
    fn dump_a_region() -> Result<()> {
        /*
         * Create a region, give it actual size
         */
        let dir = tempdir()?;
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        r1.extend(2)?;

        /*
         * Build the Vec for our region dir
         */
        let dvec = vec![dir.into_path()];

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl())?;

        Ok(())
    }

    #[test]
    fn dump_two_region() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;
        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        let mut r2 =
            Region::create(&dir2, new_region_options(), csl()).unwrap();
        r1.extend(2)?;
        r2.extend(2)?;

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
        dump_region(dvec, None, None, false, false, csl())?;

        Ok(())
    }

    #[test]
    fn dump_extent() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;

        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        r1.extend(3)?;
        let mut r2 =
            Region::create(&dir2, new_region_options(), csl()).unwrap();
        r2.extend(3)?;

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
        dump_region(dvec, Some(2), None, false, false, csl())?;

        Ok(())
    }

    #[test]
    fn block_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

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
        inner.truncate_encryption_contexts_and_hashes(vec![(0, 65536)])?;

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

    #[test]
    fn multiple_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

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

        inner.truncate_encryption_contexts_and_hashes(vec![(0, 6), (1, 7)])?;

        let ctxs = inner.get_block_contexts(0, 2)?;

        assert_eq!(ctxs[0].len(), 1);
        assert_eq!(ctxs[0][0].on_disk_hash, 6);

        assert_eq!(ctxs[1].len(), 1);
        assert_eq!(ctxs[1][0].on_disk_hash, 7);

        Ok(())
    }

    #[test]
    fn test_big_write() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

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

        region.region_write(&writes, 0, false)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_flush_removes_partial_writes() -> Result<()> {
        // Validate that incorrect context rows are removed by a full rehash,
        // so that opening an extent will get rid of partial writes' block
        // context rows. This is necessary for write_unwritten to work after
        // a crash.
        //
        // Specifically, this test checks for the case where we had a brand new
        // block and a write to that blocks that failed such that only the
        // write's block context was persisted, leaving the data all zeros. In
        // this case, there is no data, so we should remove the invalid block
        // context row.

        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

        // A write of some sort only wrote a block context row

        {
            let ext = &region.extents[0];
            let mut inner = ext.inner();
            inner.set_block_contexts(&[&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 1024,
                },
                block: 0,
                on_disk_hash: 65536,
            }])?;
        }

        // This should clear out the invalid context
        for extent in &mut region.extents {
            extent.close()?;
        }
        region.reopen_all_extents()?;

        // Verify no block context rows exist

        {
            let ext = &region.extents[0];
            let inner = ext.inner();
            assert!(inner.get_block_contexts(0, 1)?[0].is_empty());
        }

        // Assert write unwritten will write to the first block

        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        region.region_write(
            &[crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            }],
            124,
            true, // only_write_unwritten
        )?;

        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            }],
            125,
        )?;

        let response = &responses[0];

        assert_eq!(response.data.to_vec(), vec![0x55; 512]);

        Ok(())
    }

    #[test]
    fn test_ok_hash_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

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

        region.region_write(&writes, 0, false)?;

        Ok(())
    }

    #[test]
    fn test_write_unwritten_when_empty() -> Result<()> {
        // Verify that a read fill does write to a block when there is
        // no data written yet.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

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

        region.region_write(&writes, 0, true)?;

        // Verify the dirty bit is now set.
        // We know our EID, so we can shortcut to getting the actual extent.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert!(dirty);

        // Now read back that block, make sure it is updated.
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest { eid, offset }],
            0,
        )?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 1);
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_when_written() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

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

        region.region_write(&writes, 0, false)?;

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
        region.region_write(&writes, 1, true)?;

        // Now read back that block, make sure it has the first write
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest { eid, offset }],
            2,
        )?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_when_written_flush() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.  This time run a flush after the
        // first write.  Verify correct state of dirty bit as well.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

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

        region.region_write(&writes, 0, true)?;

        // Verify the dirty bit is now set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert!(dirty);

        // Flush extent with eid, fn, gen, job_id.
        region.region_flush_extent(eid as usize, 1, 1, 1)?;

        // Verify the dirty bit is no longer set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert!(!dirty);

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
        region.region_write(&writes, 1, true)?;

        // Verify the dirty bit is not set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert!(!dirty);

        // Read back our block, make sure it has the first write data
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest { eid, offset }],
            2,
        )?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_big_write() -> Result<()> {
        // Do a multi block write where all blocks start new (unwritten)
        // Verify only empty blocks have data.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

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

        region.region_write(&writes, 0, true)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_big_write_partial_0() -> Result<()> {
        // Do a write to block zero, then do a multi block write with
        // only_write_unwritten set. Verify block zero is the first write, and
        // the remaining blocks have the contents from the multi block fill.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

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
        region.region_write(&writes, 0, false)?;

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

        region.region_write(&writes, 0, true)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_big_write_partial_1() -> Result<()> {
        // Write to the second block, then do a multi block fill.
        // Verify the second block has the original data we wrote, and all
        // the other blocks have the data from the multi block fill.

        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3)?;

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
        region.region_write(&writes, 0, false)?;

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
        region.region_write(&writes, 0, true)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_big_write_partial_final() -> Result<()> {
        // Do a write to the fourth block, then do a multi block read fill
        // where the last block of the read fill is what we wrote to in
        // our first write.
        // verify the fourth block has the original write, and the first
        // three blocks have the data from the multi block read fill.

        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(5)?;

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
        region.region_write(&writes, 0, false)?;

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
        region.region_write(&writes, 0, true)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
            println!("Read a region, append");
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_write_unwritten_big_write_partial_sparse() -> Result<()> {
        // Do a multi block write_unwritten where a few different blocks have
        // data. Verify only unwritten blocks get the data.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(4)?;

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
            region.region_write(&writes, 0, false)?;
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

        region.region_write(&writes, 0, true)?;

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

        let responses = region.region_read(&requests, 0)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[test]
    fn test_bad_hash_bad() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

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

        let result = region.region_write(&writes, 0, false);

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

    #[test]
    fn test_blank_block_read_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(1)?;

        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            }],
            0,
        )?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 0);
        assert_eq!(responses[0].data[..], [0u8; 512][..]);

        Ok(())
    }
}
