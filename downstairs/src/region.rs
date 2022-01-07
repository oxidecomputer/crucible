// Copyright 2021 Oxide Computer Company
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Result};
use crucible_common::*;
use crucible_protocol::EncryptionContext;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Debug)]
pub struct Extent {
    number: u32,
    block_size: u64,
    extent_size: Block,
    inner: Mutex<Inner>,
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
     * For a given block, return all encryption contexts since last fsync.
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
     * Given a list of (block, nonce, tag), append encryption context rows.
     *
     * For the params, keep a list of references so that copying is
     * minimized.
     */
    fn set_encryption_context(
        &mut self,
        encryption_context_params: &[(u64, &EncryptionContext)],
    ) -> Result<()> {
        let mut stmts: Vec<String> =
            Vec::with_capacity(encryption_context_params.len());

        for (block, encryption_context) in encryption_context_params {
            let stmt: String = vec![format!(
                "({}, {}, X'{}', X'{}')",
                /*
                 * Auto-increment counter based on what's in the db
                 */
                vec![
                    "(SELECT IFNULL(MAX(counter),0) + 1".to_string(),
                    format!("from encryption_context WHERE block={})", block,),
                ]
                .join(" "),
                block,
                hex::encode(&encryption_context.nonce),
                hex::encode(&encryption_context.tag),
            )]
            .join(" ");

            stmts.push(stmt);
        }

        let mut stmt: String = vec![
            "INSERT INTO encryption_context".to_string(),
            "(counter, block, nonce, tag) values".to_string(),
        ]
        .join(" ");
        stmt.push_str(&stmts.join(","));

        let rows_affected = self.metadb.execute(&stmt, [])?;

        assert_eq!(rows_affected, encryption_context_params.len());

        Ok(())
    }

    /*
     * Get rid of all but most recent encryption context for each block.
     */
    fn truncate_encryption_contexts(&mut self) -> Result<()> {
        let stmt: String = vec![
            "DELETE FROM encryption_context WHERE ROWID not in",
            "(select ROWID from",
            "(select ROWID,block,MAX(counter)",
            "from encryption_context group by block)",
            ");",
            "UPDATE encryption_context SET counter = 0 WHERE block = ?1",
        ]
        .join(" ");

        let _rows_affected = self.metadb.execute(&stmt, [])?;

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
     * Not currently connected to anything XXX
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

/**
 * Produce a PathBuf that refers to the backing file for extent "number",
 * anchored under "dir".
 */
fn extent_path<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push(format!("{:02X}", (number >> 24) & 0xFF));
    out.push(format!("{:03X}", (number >> 12) & 0xFFF));
    out.push(format!("{:03X}", number & 0xFFF));
    out
}

fn config_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push("region.json");
    out
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
    ) -> Result<Extent> {
        /*
         * Store extent data in files within a directory hierarchy so that
         * there are not too many files in any level of that hierarchy.
         */
        let mut path = extent_path(dir, number);

        let bcount = def.extent_size().value;
        let size = def.block_size().checked_mul(bcount).unwrap();

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Err(e) => {
                bail!("Error: e {} No extent file found for {:?}", e, path);
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
        let metadb = Connection::open(&path)?;
        assert!(metadb.is_autocommit());
        metadb.pragma_update(None, "journal_mode", &"WAL")?;
        metadb.pragma_update(None, "synchronous", &"FULL")?;

        // XXX: schema updates?

        Ok(Extent {
            number,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Mutex::new(Inner { file, metadb }),
        })
    }

    /**
     * Create an extent at the location requested.
     * Start off with the default meta data.
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
        let mut path = extent_path(dir, number);

        /*
         * Verify there are not existing extent files.
         */
        if Path::new(&path).exists() {
            bail!("Extent file already exists {:?}", path);
        }

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

        /*
         * Create the metadata db
         */
        path.set_extension("db");
        let metadb = Connection::open(&path)?;
        assert!(metadb.is_autocommit());
        metadb.pragma_update(None, "journal_mode", &"WAL")?;
        metadb.pragma_update(None, "synchronous", &"FULL")?;

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

        /*
         * Complete the construction of our new extent
         */
        Ok(Extent {
            number,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Mutex::new(Inner { file, metadb }),
        })
    }

    pub fn inner(&self) -> MutexGuard<Inner> {
        self.inner.lock().unwrap()
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
        let mut inner = self.inner.lock().unwrap();

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

            responses.push(response);
        }

        Ok(())
    }

    /**
     * Verify that the requested block offset and size of the buffer
     * will fit within the extent.
     *
     * Note that the checks here do take into account that the first block
     * is the metadata block.
     */
    fn check_input(
        &self,
        offset: Block,
        data: &[u8],
    ) -> Result<(), CrucibleError> {
        if (data.len() % self.block_size as usize) != 0 {
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
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().unwrap();

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
         *   b) write out extent data
         *
         * If encryption context is written after the extent data, a crash or
         * interruption before extent data is written would potentially leave
         * data on the disk that cannot be decrypted.
         *
         * Note that writing extent data here does not assume that it is
         * durably on disk - the only guarantee of that is returning
         * ok from fsync. The data is only potentially on disk and
         * this depends on operating system implementation.
         *
         * To minimize the performance hit of sending many transactions to
         * sqlite, all encryption contexts are written at the same time. This
         * means two loops are required. The steps now look like:
         *
         * 1) set the dirty bit
         * 2) gather and write all encryption contexts
         * 3) write all extent data
         */

        inner.set_dirty()?;

        let mut encryption_context_params: Vec<(u64, &EncryptionContext)> =
            Vec::with_capacity(writes.len());

        for write in writes {
            if let Some(encryption_context) = &write.encryption_context {
                encryption_context_params
                    .push((write.offset.value, encryption_context));
            }
        }

        if !encryption_context_params.is_empty() {
            inner.set_encryption_context(&encryption_context_params)?;
        }

        for write in writes {
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
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().unwrap();

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
        if unsafe { fsync(inner.file.as_raw_fd()) } == -1 {
            let e = std::io::Error::last_os_error();
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
         * Clear old encryption contexts. In order to be crash consistent,
         * only perform this after the extent fsync is done.
         */
        inner.truncate_encryption_contexts()?;

        inner.file.seek(SeekFrom::Start(0))?;

        inner.set_flush_number(new_flush, new_gen)?;

        Ok(())
    }
}

extern "C" {
    fn fsync(fildes: i32) -> i32;
}

/**
 * The main structure describing a region.
 */
#[derive(Debug)]
pub struct Region {
    dir: PathBuf,
    def: RegionDefinition,
    pub extents: Vec<Extent>,
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
        for eid in next_eid..self.def.extent_count() {
            let new_extent: Extent;
            if create {
                new_extent = Extent::create(&self.dir, &self.def, eid)?;
            } else {
                new_extent = Extent::open(&self.dir, &self.def, eid)?;
            }
            self.extents.push(new_extent);
            assert_eq!(self.extents[eid as usize].number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());
        Ok(())
    }

    /**
     * if there is a difference between what our actual extent_count is
     * and what is requested, go out and create the new extent files.
     */
    pub fn extend(&mut self, newsize: u32) -> Result<()> {
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

    #[instrument]
    pub fn single_block_region_write(
        &self,
        eid: u64,
        offset: Block,
        data: bytes::Bytes,
        nonce: Option<Vec<u8>>,
        tag: Option<Vec<u8>>,
    ) -> Result<(), CrucibleError> {
        let encryption_context = if nonce.is_some() && tag.is_some() {
            Some(EncryptionContext {
                nonce: nonce.as_ref().unwrap().to_vec(),
                tag: tag.as_ref().unwrap().to_vec(),
            })
        } else {
            None
        };

        self.region_write(&[crucible_protocol::Write {
            eid,
            offset,
            data,
            encryption_context,
        }])
    }

    #[instrument]
    pub fn region_write(
        &self,
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
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

        for eid in batched_writes.keys() {
            let extent = &self.extents[*eid];
            let writes = batched_writes.get(eid).unwrap();
            extent.write(&writes[..])?;
        }

        Ok(())
    }

    #[instrument]
    pub fn single_block_region_read(
        &self,
        request: crucible_protocol::ReadRequest,
    ) -> Result<crucible_protocol::ReadResponse, CrucibleError> {
        let mut responses = self.region_read(&[request])?;
        let response = responses.pop().unwrap();
        drop(responses);
        Ok(response)
    }

    #[instrument]
    pub fn region_read(
        &self,
        requests: &[crucible_protocol::ReadRequest],
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

        Ok(responses)
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
    ) -> Result<(), CrucibleError> {
        // XXX How to we convert between usize and u32 correctly?
        for eid in 0..self.def.extent_count() {
            let extent = &self.extents[eid as usize];
            extent.flush_block(flush_number, gen_number)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dump::dump_region;
    use bytes::{BufMut, BytesMut};
    use rand::{Rng, RngCore};
    use std::path::PathBuf;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn new_extent() -> Extent {
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
            number: 0,
            block_size: 512,
            extent_size: Block::new_512(100),
            inner: Mutex::new(inn),
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
    fn new_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options());
        Ok(())
    }

    #[test]
    fn new_existing_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options());
        let _ = Region::open(&dir, new_region_options(), false);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn bad_import_region() -> () {
        let _ = Region::open(
            &"/tmp/12345678-1111-2222-3333-123456789999/notadir",
            new_region_options(),
            false,
        )
        .unwrap();
        ()
    }

    #[test]
    fn extent_io_valid() {
        let ext = new_extent();
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        assert_eq!((), ext.check_input(Block::new_512(0), &data).unwrap());
        assert_eq!((), ext.check_input(Block::new_512(99), &data).unwrap());
    }

    #[test]
    fn extent_io_valid2() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent();

        assert_eq!((), ext.check_input(Block::new_512(0), &data).unwrap());
        assert_eq!((), ext.check_input(Block::new_512(97), &data).unwrap());
    }

    #[test]
    fn extent_io_valid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent();
        ext.check_input(Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_large() {
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        let ext = new_extent();
        ext.check_input(Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_small() {
        let mut data = BytesMut::with_capacity(511);
        data.put(&[1; 511][..]);

        let ext = new_extent();
        ext.check_input(Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_bad_block() {
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        let ext = new_extent();
        ext.check_input(Block::new_512(100), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent();
        ext.check_input(Block::new_512(99), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent();
        assert_eq!((), ext.check_input(Block::new_512(1), &data).unwrap());
    }

    #[test]
    fn extent_path_min() {
        assert_eq!(
            extent_path("/var/region", u32::MIN),
            p("/var/region/00/000/000")
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
        dump_region(dvec, None, false)?;

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
        dump_region(dvec, None, false)?;

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
        dump_region(dvec, Some(2), false)?;

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
        inner.truncate_encryption_contexts()?;

        let ctxs = inner.get_encryption_contexts(0)?;

        assert_eq!(ctxs.len(), 1);

        assert_eq!(ctxs[0].nonce, blob1);
        assert_eq!(ctxs[0].tag, blob2);

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

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                encryption_context: None,
            });
        }

        region.region_write(&writes)?;

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

        let responses = region.region_read(&requests)?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }
}
