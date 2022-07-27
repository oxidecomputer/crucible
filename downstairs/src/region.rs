// Copyright 2021 Oxide Computer Company
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::fs::{rename, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Result};
use crucible_common::*;
use crucible_protocol::{EncryptionContext, SnapshotDetails};
use futures::TryStreamExt;
use repair_client::types::FileType;
use repair_client::Client;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::macros::support::Pin;
use tracing::instrument;

use super::*;

#[derive(Debug)]
pub struct Extent {
    number: u32,
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

#[derive(Debug)]
pub struct Inner {
    file: File,
    metadb: Connection,
}

impl Inner {
    pub fn gen_number(&self) -> Result<u64> {
        let mut stmt = self
            .metadb
            .prepare("SELECT value FROM metadata where name='gen_number'")?;
        let gen_number_iter = stmt.query_map([], |row| row.get(0))?;

        let mut gen_number_values: Vec<u64> = vec![];
        for gen_number_value in gen_number_iter {
            gen_number_values.push(gen_number_value?);
        }

        assert!(gen_number_values.len() == 1);

        Ok(gen_number_values[0])
    }

    pub fn flush_number(&self) -> Result<u64> {
        let mut stmt = self
            .metadb
            .prepare("SELECT value FROM metadata where name='flush_number'")?;
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
        let mut stmt = self.metadb.prepare(
            "UPDATE metadata SET value=?1 WHERE name='flush_number'",
        )?;

        let _rows_affected = stmt.execute(params![new_flush])?;

        let mut stmt = self
            .metadb
            .prepare("UPDATE metadata SET value=?1 WHERE name='gen_number'")?;

        let _rows_affected = stmt.execute(params![new_gen])?;

        /*
         * When we write out the new flush number, the dirty bit should be
         * set back to false.
         */
        let _rows_affected = self
            .metadb
            .execute("UPDATE metadata SET value=0 WHERE name='dirty'", [])?;

        Ok(())
    }

    pub fn dirty(&self) -> Result<bool> {
        let mut stmt = self
            .metadb
            .prepare("SELECT value FROM metadata where name='dirty'")?;
        let dirty_iter = stmt.query_map([], |row| row.get(0))?;

        let mut dirty_values: Vec<bool> = vec![];
        for dirty_value in dirty_iter {
            dirty_values.push(dirty_value?);
        }

        assert!(dirty_values.len() == 1);

        Ok(dirty_values[0])
    }

    fn set_dirty(&self) -> Result<()> {
        let _rows_affected = self
            .metadb
            .execute("UPDATE metadata SET value=1 WHERE name='dirty'", [])?;
        Ok(())
    }

    /*
     * For a given block, return all encryption contexts since last flush.
     * Order so latest is last.
     */
    fn get_encryption_contexts(
        &self,
        block: u64,
    ) -> Result<Vec<EncryptionContext>> {
        // NOTE: "ORDER BY RANDOM()" would be a good --lossy addition here
        let stmt = vec![
            "SELECT nonce, tag FROM encryption_context where block=?1",
            "ORDER BY counter ASC",
        ]
        .join(" ");

        let mut stmt = self.metadb.prepare(&stmt)?;

        let stmt_iter = stmt
            .query_map(params![block], |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut results = Vec::new();

        for row in stmt_iter {
            let (nonce, tag) = row?;
            results.push(EncryptionContext { nonce, tag });
        }

        Ok(results)
    }

    /*
     * For a given block, return all hashes since last flush. Order so latest
     * is last.
     */
    pub fn get_hashes(&self, block: u64) -> Result<Vec<u64>> {
        // NOTE: "ORDER BY RANDOM()" would be a good --lossy addition here
        let stmt = vec![
            "SELECT hash FROM integrity_hashes where block=?1",
            "ORDER BY counter ASC",
        ]
        .join(" ");

        let mut stmt = self.metadb.prepare(&stmt)?;

        let stmt_iter = stmt.query_map(params![block], |row| row.get(0))?;

        let mut results = Vec::new();

        for row in stmt_iter {
            let hash: Vec<u8> = row?;
            assert_eq!(hash.len(), 8);

            results.push(u64::from_le_bytes(hash[..].try_into()?));
        }

        Ok(results)
    }

    /*
     * Given a (block, nonce, tag), append an encryption context row.
     *
     * For the params, keep a list of references so that copying is
     * minimized.
     */
    pub fn tx_set_encryption_context(
        tx: &rusqlite::Transaction,
        encryption_context_params: &(u64, &EncryptionContext),
    ) -> Result<()> {
        let (block, encryption_context) = encryption_context_params;

        let stmt: String = vec![
            "INSERT INTO encryption_context".to_string(),
            "(counter, block, nonce, tag) values".to_string(),
            format!(
                "({}, {}, X'{}', X'{}')",
                /*
                 * Auto-increment counter based on what's in the db
                 */
                vec![
                    "(SELECT IFNULL(MAX(counter),0) + 1".to_string(),
                    format!("from encryption_context WHERE block={})", block),
                ]
                .join(" "),
                block,
                hex::encode(&encryption_context.nonce),
                hex::encode(&encryption_context.tag),
            ),
        ]
        .join(" ");

        let rows_affected = tx.execute(&stmt, [])?;
        assert_eq!(rows_affected, 1);

        Ok(())
    }

    pub fn metadb_transaction(&mut self) -> Result<rusqlite::Transaction> {
        Ok(self.metadb.transaction()?)
    }

    #[cfg(test)]
    fn set_encryption_context(
        &mut self,
        encryption_context_params: &[(u64, &EncryptionContext)],
    ) -> Result<()> {
        let tx = self.metadb.transaction()?;

        for tuple in encryption_context_params {
            Self::tx_set_encryption_context(&tx, &tuple)?;
        }

        tx.commit()?;

        Ok(())
    }

    /*
     * Given a (block, hash), append a hash row.
     */
    pub fn tx_set_hash(
        tx: &rusqlite::Transaction,
        hash_params: &(u64, u64),
    ) -> Result<()> {
        let (block, hash) = hash_params;

        let stmt: String = vec![
            "INSERT INTO integrity_hashes".to_string(),
            "(counter, block, hash) values".to_string(),
            format!(
                "({}, {}, X'{}')",
                /*
                 * Auto-increment counter based on what's in the db
                 */
                vec![
                    "(SELECT IFNULL(MAX(counter),0) + 1".to_string(),
                    format!("from integrity_hashes WHERE block={})", block),
                ]
                .join(" "),
                block,
                hex::encode(&hash.to_le_bytes())
            ),
        ]
        .join(" ");

        let rows_affected = tx.execute(&stmt, [])?;
        assert_eq!(rows_affected, 1);

        Ok(())
    }

    #[cfg(test)]
    pub fn set_hashes(&mut self, hash_params: &[(u64, u64)]) -> Result<()> {
        let tx = self.metadb.transaction()?;

        for tuple in hash_params {
            Self::tx_set_hash(&tx, tuple)?;
        }

        tx.commit()?;

        Ok(())
    }

    /*
     * Get rid of all but most recent encryption context and hash for each
     * block.
     */
    fn truncate_encryption_contexts_and_hashes(&mut self) -> Result<()> {
        let tx = self.metadb.transaction()?;

        // Clear encryption context
        let stmt: String = vec![
            "DELETE FROM encryption_context WHERE ROWID not in",
            "(select ROWID from",
            "(select ROWID,block,MAX(counter)",
            "from encryption_context group by block)",
            ");",
        ]
        .join(" ");

        let _rows_affected = tx.execute(&stmt, [])?;

        let _rows_affected =
            tx.execute("UPDATE encryption_context SET counter = 0", [])?;

        // Clear integrity hash
        let stmt: String = vec![
            "DELETE FROM integrity_hashes WHERE ROWID not in",
            "(select ROWID from",
            "(select ROWID,block,MAX(counter)",
            "from integrity_hashes group by block)",
            ");",
        ]
        .join(" ");

        let _rows_affected = tx.execute(&stmt, [])?;

        let _rows_affected =
            tx.execute("UPDATE integrity_hashes SET counter = 0", [])?;

        tx.commit()?;

        Ok(())
    }

    /*
     * In order to unit test truncate_encryption_contexts_and_hashes, return
     * blocks and counters.
     */
    #[cfg(test)]
    fn get_blocks_and_counters_for_encryption_context(
        &mut self,
    ) -> Result<Vec<(u64, u64)>> {
        let mut stmt = self
            .metadb
            .prepare(&"SELECT block, counter FROM encryption_context")?;

        let stmt_iter =
            stmt.query_map(params![], |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut results = Vec::new();

        for row in stmt_iter {
            results.push(row?);
        }

        Ok(results)
    }

    #[cfg(test)]
    fn get_blocks_and_counters_for_hashes(
        &mut self,
    ) -> Result<Vec<(u64, u64)>> {
        let mut stmt = self
            .metadb
            .prepare(&"SELECT block, counter FROM integrity_hashes")?;

        let stmt_iter =
            stmt.query_map(params![], |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut results = Vec::new();

        for row in stmt_iter {
            results.push(row?);
        }

        Ok(results)
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
    out.set_extension("copy".to_string());
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
    out.set_extension("replace".to_string());
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
    out.set_extension("completed".to_string());
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
            println!("Deleting dir: {:?}", d);
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
    println!("validate {} with {:?}", eid, files);
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
            println!(
                "Extent {} found replacement dir, finishing replacement",
                number
            );
            move_replacement_extent(&dir, number as usize)?;
        }

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file =
            match OpenOptions::new().read(true).write(!read_only).open(&path) {
                Err(e) => {
                    println!(
                        "Error: Open of {:?} for extent#{} returned: {}",
                        path, number, e,
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
        let metadb = match open_sqlite_connection(&path) {
            Err(e) => {
                println!(
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

        Ok(Extent {
            number,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Some(Mutex::new(Inner { file, metadb })),
        })
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

            metadb.execute(
                "CREATE TABLE encryption_context (
                    counter INTEGER,
                    block INTEGER,
                    nonce BLOB NOT NULL,
                    tag BLOB NOT NULL,
                    PRIMARY KEY (block, counter)
                )",
                [],
            )?;

            metadb.execute(
                "CREATE TABLE integrity_hashes (
                    counter INTEGER,
                    block INTEGER,
                    hash BLOB NOT NULL,
                    PRIMARY KEY (block, counter)
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
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Some(Mutex::new(Inner { file, metadb })),
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

        println!("Create copy dir {:?}", cp);
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

    #[instrument]
    pub fn read(
        &self,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner();

        for request in requests {
            let mut response = crucible_protocol::ReadResponse::from_request(
                request,
                self.block_size as usize,
            );

            self.check_input(request.offset, &response.data)?;

            let byte_offset = request.offset.value * self.block_size;

            inner.file.seek(SeekFrom::Start(byte_offset))?;

            /*
             * XXX This read_exact only works because we have filled our
             * buffer with data ahead of time.  If we want to use
             * an uninitialized buffer, then we need a different
             * read or type for the destination
             */
            inner.file.read_exact(&mut response.data)?;

            response.encryption_contexts =
                inner.get_encryption_contexts(request.offset.value)?;

            response.hashes = inner.get_hashes(request.offset.value)?;

            responses.push(response);
        }

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
        writes: &[&crucible_protocol::Write],
        read_fill: bool,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner();

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
         * If "read_fill" is true, then we only issue a write for a
         * block if that block has not been written to yet.  Note that we
         * can have a write that is "sparse" if the range of blocks it
         * contains has a mix of written an unwritten blocks.
         *
         * We define a block being written to or not has if that block has
         * a checksum or not.  So it is required that a written block has
         * a checksum.
         */

        let mut writes_to_skip: Vec<u64> = Vec::new();
        if read_fill {
            for write in writes {
                if !inner.get_hashes(write.offset.value).unwrap().is_empty() {
                    writes_to_skip.push(write.offset.value);
                }
            }
        }
        if read_fill && writes_to_skip.len() == writes.len() {
            // For read fill, if the list of blocks to skip is the same
            // length as the number of blocks in the write list, then we
            // have no work to do here.
            return Ok(());
        }

        // We know we have at least one block to write.
        inner.set_dirty()?;

        let tx = inner.metadb_transaction()?;
        for write in writes {
            // Since we only add to writes_to_skip if this is a read_fill,
            // this will be empty if read_fill is false, so we don't need to
            // check again here for read_fill being true.
            if writes_to_skip.contains(&write.offset.value) {
                assert!(read_fill);
                continue;
            }
            if let Some(encryption_context) = &write.encryption_context {
                Inner::tx_set_encryption_context(
                    &tx,
                    &(write.offset.value, encryption_context),
                )?;
            }

            Inner::tx_set_hash(&tx, &(write.offset.value, write.hash))?;
        }
        tx.commit()?;

        for write in writes {
            if writes_to_skip.contains(&write.offset.value) {
                println!("skip this write");
                assert!(read_fill);
                continue;
            }
            let byte_offset = write.offset.value * self.block_size;

            inner.file.seek(SeekFrom::Start(byte_offset))?;
            inner.file.write_all(&write.data)?;
        }

        Ok(())
    }

    #[instrument]
    pub fn flush_block(
        &self,
        new_flush: u64,
        new_gen: u64,
        job_id: u64,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner();

        if !inner.dirty()? {
            /*
             * If we have made no writes to this extent since the last flush,
             * we do not need to update the extent on disk
             */
            return Ok(());
        }

        /*
         * We must first fsync to get any outstanding data written to disk.
         * This must be done before we update the flush number.
         */
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

        /*
         * Clear old encryption contexts and hashes. In order to be crash
         * consistent, only perform this after the extent fsync is done.
         */
        inner.truncate_encryption_contexts_and_hashes()?;

        inner.file.seek(SeekFrom::Start(0))?;

        inner.set_flush_number(new_flush, new_gen)?;

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
}

impl Region {
    /**
     * Create a new region based on the given RegionOptions
     */
    pub fn create<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
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
        println!("Created new region file {:?}", cp);

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            read_only: false,
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
            println!("Opened existing region file {:?}", cp);
        }

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            read_only,
        };

        region.open_extents(false)?;

        Ok(region)
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
                    Extent::open(&self.dir, &self.def, eid, self.read_only)
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
        assert!(!self.extents[eid].inner.is_some());
        assert_eq!(self.extents[eid].number, eid as u32);
        assert!(!self.read_only);

        let new_extent =
            Extent::open(&self.dir, &self.def, eid as u32, self.read_only)?;
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
        move_replacement_extent(&self.dir, eid)?;

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
        println!("Found repair files: {:?}", repair_files);

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
        println!(
            "Repair files downloaded, move directory {:?} to {:?}",
            copy_dir, rd
        );
        rename(copy_dir.clone(), rd.clone())?;

        // Files are synced in save_stream_to_file(). Now make sure
        // the parent directory containing the repair directory has
        // been synced so that change is persistent.
        let current_dir = extent_dir(&self.dir, eid as u32);

        sync_path(&current_dir)?;
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
        println!("Current flush_numbers [0..12]: {:?}", ver);

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
            let computed_hash =
                if let Some(encryption_context) = &write.encryption_context {
                    integrity_hash(&[
                        &encryption_context.nonce[..],
                        &encryption_context.tag[..],
                        &write.data[..],
                    ])
                } else {
                    integrity_hash(&[&write.data[..]])
                };

            if computed_hash != write.hash {
                println!("Failed write hash validation {}", computed_hash);
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
        read_fill: bool,
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

        if read_fill {
            cdt::os__readfill__start!(|| job_id);
        } else {
            cdt::os__write__start!(|| job_id);
        }
        for eid in batched_writes.keys() {
            let extent = &self.extents[*eid];
            let writes = batched_writes.get(eid).unwrap();
            extent.write(&writes[..], read_fill)?;
        }
        if read_fill {
            cdt::os__readfill__done!(|| job_id);
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
                    extent.read(&batched_reads[..], &mut responses)?;

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
            extent.read(&batched_reads[..], &mut responses)?;
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
        println!(
            "Flush just extent {} with f:{} and g:{}",
            eid, flush_number, gen_number
        );

        let extent = &self.extents[eid];
        extent.flush_block(flush_number, gen_number, 0)?;

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
            extent.flush_block(flush_number, gen_number, job_id)?;
        }
        cdt::os__flush__done!(|| job_id);

        // snapshots currently only work with ZFS
        if cfg!(feature = "zfs_snapshot") {
            if let Some(snapshot_details) = snapshot_details {
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
    println!("fsync completed for: {:?}", path);

    Ok(())
}

/**
 * Copy the contents of the replacement directory on to the extent
 * files in the extent directory.
 */
pub fn move_replacement_extent<P: AsRef<Path>>(
    region_dir: P,
    eid: usize,
) -> Result<(), CrucibleError> {
    let destination_dir = extent_dir(&region_dir, eid as u32);
    let extent_file_name = extent_file_name(eid as u32, ExtentType::Data);
    let replace_dir = replace_dir(&region_dir, eid as u32);
    let completed_dir = completed_dir(&region_dir, eid as u32);

    assert!(Path::new(&replace_dir).exists());
    assert!(!Path::new(&completed_dir).exists());

    println!("Copy files from {:?} in {:?}", replace_dir, destination_dir,);

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
    sync_path(&original_file)?;

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
    sync_path(&original_file)?;

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
        sync_path(&original_file)?;
    } else if original_file.exists() {
        println!(
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
        sync_path(&original_file)?;
    } else if original_file.exists() {
        println!(
            "Remove old file {:?} as there is no replacement",
            original_file.clone()
        );
        std::fs::remove_file(&original_file)?;
    }
    sync_path(&destination_dir)?;

    // After we have all files: move the copy dir.
    println!("Move directory  {:?} to {:?}", replace_dir, completed_dir);
    rename(replace_dir, &completed_dir)?;

    sync_path(&destination_dir)?;

    std::fs::remove_dir_all(&completed_dir)?;

    sync_path(&destination_dir)?;
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
                    Item = std::result::Result<crucible::Bytes, reqwest::Error>,
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
        println!("Failed to fsync repair file: {:?}", e);
        crucible_bail!(IoError, "repair {:?}: fsync failure: {:?}", file, e);
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dump::dump_region;
    use bytes::{BufMut, BytesMut};
    use rand::{Rng, RngCore};
    use std::fs::rename;
    use std::path::PathBuf;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn new_extent(number: u32) -> Extent {
        let ff = File::open("/dev/null").unwrap();

        let inn = Inner {
            file: ff,
            metadb: Connection::open_in_memory().unwrap(),
        };

        /*
         * Note: All the tests expect 512 and 100, so if you change
         * these, then change the tests!
         */
        Extent {
            number,
            block_size: 512,
            extent_size: Block::new_512(100),
            inner: Some(Mutex::new(inn)),
        }
    }

    static TEST_UUID_STR: &str = "12345678-1111-2222-3333-123456789999";

    fn test_uuid() -> Uuid {
        TEST_UUID_STR.parse().unwrap()
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
        let mut region = Region::create(&dir, new_region_options())?;
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
        let mut region = Region::create(&dir, new_region_options()).unwrap();
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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

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
        let mut region = Region::create(&dir, new_region_options())?;
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
        rename(cp.clone(), rd.clone())?;

        drop(region);

        // Open up the region read_only now.
        let mut region = Region::open(&dir, new_region_options(), false, true)?;

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
        assert_eq!(validate_repair_files(1, &Vec::new()), false);
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
        assert_eq!(validate_repair_files(2, &good_files), false);
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
        assert_eq!(validate_repair_files(2, &good_files), false);
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
        assert_eq!(validate_repair_files(1, &good_files), false);
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

        assert_eq!(validate_repair_files(2, &good_files), false);
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
        assert_eq!(validate_repair_files(1, &good_files), false);
    }

    #[test]
    fn validate_repair_files_not_good_enough() {
        // We require 2 or 4 files, not 3
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-wal".to_string(),
        ];
        assert_eq!(validate_repair_files(1, &good_files), false);
    }

    #[test]
    fn reopen_all_extents() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(5)?;

        // Close extent 1
        let ext_one = &mut region.extents[1];
        ext_one.close()?;
        assert!(!ext_one.inner.is_some());

        // Close extent 4
        let ext_four = &mut region.extents[4];
        ext_four.close()?;
        assert!(!ext_four.inner.is_some());

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
        let _ = Region::create(&dir, new_region_options());
        Ok(())
    }

    #[test]
    fn new_existing_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options());
        let _ = Region::open(&dir, new_region_options(), false, false);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn bad_import_region() -> () {
        let _ = Region::open(
            &"/tmp/12345678-1111-2222-3333-123456789999/notadir",
            new_region_options(),
            false,
            false,
        )
        .unwrap();
        ()
    }

    #[test]
    fn extent_io_valid() {
        let ext = new_extent(0);
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        assert_eq!((), ext.check_input(Block::new_512(0), &data).unwrap());
        assert_eq!((), ext.check_input(Block::new_512(99), &data).unwrap());
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
        assert_eq!((), ext.check_input(Block::new_512(1), &data).unwrap());
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
        let mut r1 = Region::create(&dir, new_region_options()).unwrap();
        r1.extend(2)?;

        /*
         * Build the Vec for our region dir
         */
        let pdir = dir.into_path();
        let mut dvec = Vec::new();
        dvec.push(pdir);

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false)?;

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
        let mut r1 = Region::create(&dir, new_region_options()).unwrap();
        let mut r2 = Region::create(&dir2, new_region_options()).unwrap();
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
        dump_region(dvec, None, None, false, false)?;

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
        let mut r1 = Region::create(&dir, new_region_options()).unwrap();
        r1.extend(3)?;
        let mut r2 = Region::create(&dir2, new_region_options()).unwrap();
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
        dump_region(dvec, Some(2), None, false, false)?;

        Ok(())
    }

    #[test]
    fn encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_encryption_contexts(0)?.is_empty());
        assert!(inner.get_encryption_contexts(1)?.is_empty());

        // Set and verify block 0's context

        inner.set_encryption_context(&[(
            0,
            &EncryptionContext {
                nonce: [1, 2, 3].to_vec(),
                tag: [4, 5, 6, 7].to_vec(),
            },
        )])?;

        let ctxs = inner.get_encryption_contexts(0)?;

        assert_eq!(ctxs.len(), 1);

        assert_eq!(ctxs[0].nonce, vec![1, 2, 3]);
        assert_eq!(ctxs[0].tag, vec![4, 5, 6, 7]);

        // Block 1 should still be blank

        assert!(inner.get_encryption_contexts(1)?.is_empty());

        // Set and verify a new context for block 0

        let blob1 = rand::thread_rng().gen::<[u8; 32]>();
        let blob2 = rand::thread_rng().gen::<[u8; 32]>();

        inner.set_encryption_context(&[(
            0,
            &EncryptionContext {
                nonce: blob1.to_vec(),
                tag: blob2.to_vec(),
            },
        )])?;

        let ctxs = inner.get_encryption_contexts(0)?;

        assert_eq!(ctxs.len(), 2);

        assert_eq!(ctxs[0].nonce, vec![1, 2, 3]);
        assert_eq!(ctxs[0].tag, vec![4, 5, 6, 7]);

        assert_eq!(ctxs[1].nonce, blob1);
        assert_eq!(ctxs[1].tag, blob2);

        // "Flush", so only the latest should remain.
        inner.truncate_encryption_contexts_and_hashes()?;

        let ctxs = inner.get_encryption_contexts(0)?;

        assert_eq!(ctxs.len(), 1);

        assert_eq!(ctxs[0].nonce, blob1);
        assert_eq!(ctxs[0].tag, blob2);

        // Assert counters were reset to zero
        for (_block, counter) in
            inner.get_blocks_and_counters_for_encryption_context()?
        {
            assert_eq!(counter, 0);
        }

        Ok(())
    }

    #[test]
    fn multiple_encryption_context() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

        // Encryption context for blocks 0 and 1 should start blank

        assert!(inner.get_encryption_contexts(0)?.is_empty());
        assert!(inner.get_encryption_contexts(1)?.is_empty());

        // Set and verify block 0's and 1's context

        inner.set_encryption_context(&[
            (
                0,
                &EncryptionContext {
                    nonce: [1, 2, 3].to_vec(),
                    tag: [4, 5, 6, 7].to_vec(),
                },
            ),
            (
                1,
                &EncryptionContext {
                    nonce: [4, 5, 6].to_vec(),
                    tag: [8, 9, 0, 1].to_vec(),
                },
            ),
        ])?;

        let ctxs = inner.get_encryption_contexts(0)?;

        assert_eq!(ctxs.len(), 1);

        assert_eq!(ctxs[0].nonce, vec![1, 2, 3]);
        assert_eq!(ctxs[0].tag, vec![4, 5, 6, 7]);

        let ctxs = inner.get_encryption_contexts(1)?;

        assert_eq!(ctxs.len(), 1);

        assert_eq!(ctxs[0].nonce, vec![4, 5, 6]);
        assert_eq!(ctxs[0].tag, vec![8, 9, 0, 1]);

        Ok(())
    }

    #[test]
    fn hashes() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

        // Hashes for blocks 0 and 1 should start blank

        assert!(inner.get_hashes(0)?.is_empty());
        assert!(inner.get_hashes(1)?.is_empty());

        // Set and verify block 0's hash

        inner.set_hashes(&[(0, 23874612987634)])?;

        let hashes = inner.get_hashes(0)?;

        assert_eq!(hashes.len(), 1);

        assert_eq!(hashes[0], 23874612987634);

        // Block 1 should still be blank

        assert!(inner.get_hashes(1)?.is_empty());

        // Set and verify a new hash for block 0

        let blob1 = rand::thread_rng().gen::<u64>();

        inner.set_hashes(&[(0, blob1)])?;

        let hashes = inner.get_hashes(0)?;

        assert_eq!(hashes.len(), 2);

        assert_eq!(hashes[0], 23874612987634);
        assert_eq!(hashes[1], blob1);

        // "Flush", so only the latest should remain.
        inner.truncate_encryption_contexts_and_hashes()?;

        let hashes = inner.get_hashes(0)?;

        assert_eq!(hashes.len(), 1);

        assert_eq!(hashes[0], blob1);

        // Assert counters were reset to zero
        for (_block, counter) in inner.get_blocks_and_counters_for_hashes()? {
            assert_eq!(counter, 0);
        }

        Ok(())
    }

    #[test]
    fn multiple_hashes() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let ext = &region.extents[0];
        let mut inner = ext.inner();

        // Hashes for blocks 0 and 1 should start blank

        assert!(inner.get_hashes(0)?.is_empty());
        assert!(inner.get_hashes(1)?.is_empty());

        // Set and verify block 0's and 1's context

        inner
            .set_hashes(&[(0, 0xbd1f97574fa0c3f4), (1, 0xa040b75cd3c96fff)])?;

        let hashes = inner.get_hashes(0)?;

        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], 0xbd1f97574fa0c3f4);

        let hashes = inner.get_hashes(1)?;

        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], 0xa040b75cd3c96fff);

        Ok(())
    }

    #[test]
    fn test_big_write() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
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

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
    fn test_ok_hash_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 5061083712412462836,
            }];

        region.region_write(&writes, 0, false)?;

        Ok(())
    }

    #[test]
    fn test_read_fill_when_empty() -> Result<()> {
        // Verify that a read fill does write to a block when there is
        // no data written yet.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9's
            }];

        region.region_write(&writes, 0, true)?;

        // Verify the dirty bit is now set.
        // We know our EID, so we can shortcut to getting the actual extent.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert_eq!(dirty, true);

        // Now read back that block, make sure it is updated.
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            }],
            0,
        )?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes.len(), 1);
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_read_fill_when_written() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9s
            }];

        region.region_write(&writes, 0, false)?;

        // Same block, now try to write something else to it.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 5061083712412462836, // hash for all 1s
            }];
        // Do the write again, but with read_fill set now.
        region.region_write(&writes, 1, true)?;

        // Now read back that block, make sure it has the first write
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            }],
            2,
        )?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes.len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_read_fill_when_written_flush() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.  This time run a flush after the
        // first write.  Verify correct state of dirty bit as well.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9s
            }];

        region.region_write(&writes, 0, true)?;

        // Verify the dirty bit is now set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert_eq!(dirty, true);
        drop(dirty);

        // Flush extent with eid, fn, gen, job_id.
        region.region_flush_extent(eid as usize, 1, 1, 1)?;

        // Verify the dirty bit is no longer set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert_eq!(dirty, false);
        drop(dirty);

        // Create a new write IO with different data.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 5061083712412462836, // hash for all 1s
            }];

        // Do the write again, but with read_fill set now.
        region.region_write(&writes, 1, true)?;

        // Verify the dirty bit is not set.
        let inner = &region.extents[eid as usize].inner.as_ref().unwrap();
        let dirty = inner.lock().unwrap().dirty().unwrap();
        assert_eq!(dirty, false);

        // Read back our block, make sure it has the first write data
        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            }],
            2,
        )?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes.len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[test]
    fn test_read_fill_big_write() -> Result<()> {
        // Do a multi block write where all blocks start new (unwritten)
        // Verify only empty blocks have data.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(3)?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
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

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
    fn test_read_fill_big_write_partial_0() -> Result<()> {
        // Do a write to block zero, then do a multi block read_fill.
        // verify block zero is the first write, and the remaining blocks
        // have the contents from the multi block fill.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9s
            }];

        // Now write just one block
        region.region_write(&writes, 0, false)?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
            });
        }

        region.region_write(&writes, 0, true)?;

        // Because we did read_fill, the block we already written should
        // still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for i in 0..512 {
            buffer[i] = 9;
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

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
    fn test_read_fill_big_write_partial_1() -> Result<()> {
        // Write to the second block, then do a multi block fill.
        // Verify the second block has the original data we wrote, and all
        // the other blocks have the data from the multi block fill.

        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9s
            }];

        // Now write just to the second block.
        region.region_write(&writes, 0, false)?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
            });
        }

        // send read_fill command.
        region.region_write(&writes, 0, true)?;

        // Because we did read_fill, the block we already written should
        // still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for i in 512..1024 {
            buffer[i] = 9;
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

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
    fn test_read_fill_big_write_partial_final() -> Result<()> {
        // Do a write to the fourth block, then do a multi block read fill
        // where the last block of the read fill is what we wrote to in
        // our first write.
        // verify the fourth block has the original write, and the first
        // three blocks have the data from the multi block read fill.

        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 4798852240582462654, // Hash for all 9s
            }];

        // Now write just to the second block.
        region.region_write(&writes, 0, false)?;

        // Now use region_write to fill four blocks
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
            });
        }

        // send read_fill command.
        region.region_write(&writes, 0, true)?;

        // Because we did read_fill, the block we already written should
        // still have the data from the first write.  Update our expected
        // buffer for the final block to have that original data.
        for i in 1536..2048 {
            buffer[i] = 9;
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            println!("Read eid: {}, {} offset: {:?}", eid, i, offset);
            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
    fn test_read_fill_big_write_partial_sparse() -> Result<()> {
        // Do a multi block read_fill where a few different blocks have
        // data. Verify only unwritten blocks get the data.
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
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
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: vec![1, 2, 3],
                            tag: vec![4, 5, 6],
                        },
                    ),
                    hash: 4798852240582462654, // Hash for all 9s
                }];

            // Now write just one block
            region.region_write(&writes, 0, false)?;
        }

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = Vec::with_capacity(total_size);
        buffer.resize(total_size, 0u8);
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
                encryption_context: None,
                hash,
            });
        }

        region.region_write(&writes, 0, true)?;

        // Because we did read_fill, the block we already written should
        // still have the data from the first write.  Update our buffer
        // for these blocks to have that original data.
        for b in blocks_to_write {
            let b_start = b * 512;
            let b_end = b_start + 512;
            for i in b_start..b_end {
                buffer[i] = 9;
            }
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest {
                eid,
                offset,
                num_blocks: 1,
            });
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
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: vec![1, 2, 3],
                        tag: vec![4, 5, 6],
                    },
                ),
                hash: 2398419238764,
            }];

        let result = region.region_write(&writes, 0, false);

        assert!(result.is_err());

        match result.err().unwrap() {
            CrucibleError::HashMismatch => {
                // ok
            }
            _ => {
                assert!(false);
            }
        }

        Ok(())
    }

    #[test]
    fn test_blank_block_read_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options())?;
        region.extend(1)?;

        let responses = region.region_read(
            &[crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
                num_blocks: 1,
            }],
            0,
        )?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes.len(), 0);
        assert_eq!(responses[0].data[..], [0u8; 512][..]);

        Ok(())
    }
}
