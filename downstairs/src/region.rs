// Copyright 2021 Oxide Computer Company
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Result};
use bytes::BytesMut;
use crucible_common::*;
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

    fn _set_gen_number(&self, new_gen: u64) -> Result<()> {
        let mut stmt = self
            .metadb
            .prepare("UPDATE metadata SET value=?1 WHERE name='gen_number'")?;

        let _rows_affected = stmt.execute(params![new_gen])?;

        Ok(())
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

    fn set_flush_number(&self, new_flush: u64) -> Result<()> {
        let mut stmt = self.metadb.prepare(
            "UPDATE metadata SET value=?1 WHERE name='flush_number'",
        )?;

        let _rows_affected = stmt.execute(params![new_flush])?;

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
        offset: Block,
        data: &mut BytesMut,
    ) -> Result<(), CrucibleError> {
        self.check_input(offset, data)?;

        let byte_offset = offset.value * self.block_size;

        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(byte_offset))?;

        /*
         * XXX This read_exact only works because we have filled our buffer
         * with data ahead of time.  If we want to use an uninitialized
         * buffer, then we need a different read or type for the destination
         */
        inner.file.read_exact(data)?;

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
        offset: Block,
        data: &[u8],
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().unwrap();

        self.check_input(offset, data)?;

        inner.set_dirty()?;

        let byte_offset = offset.value * self.block_size;

        inner.file.seek(SeekFrom::Start(byte_offset))?;
        inner.file.write_all(data)?;
        inner.file.flush()?;

        Ok(())
    }

    #[instrument]
    pub fn flush_block(&self, new_flush: u64) -> Result<(), CrucibleError> {
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

        inner.file.seek(SeekFrom::Start(0))?;

        inner.set_flush_number(new_flush)?;

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
    ) -> Result<(), CrucibleError> {
        self.region_write(&[crucible_protocol::Write { eid, offset, data }])
    }

    #[instrument]
    pub fn region_write(
        &self,
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
        for write in writes {
            let extent = &self.extents[write.eid as usize];
            extent.write(write.offset, &write.data)?;
        }
        Ok(())
    }

    #[instrument]
    pub fn single_block_region_read(
        &self,
        request: crucible_protocol::ReadRequest,
    ) -> Result<BytesMut, CrucibleError> {
        let block_size = self.def.block_size() as usize;

        let mut data = BytesMut::with_capacity(block_size);
        data.resize(block_size, 1);

        let mut responses = vec![(request, data)];
        self.region_read(&mut responses)?;

        let (_, data) = responses.pop().unwrap();
        drop(responses);

        Ok(data)
    }

    #[instrument]
    pub fn region_read(
        &self,
        responses: &mut Vec<(crucible_protocol::ReadRequest, BytesMut)>,
    ) -> Result<(), CrucibleError> {
        for (request, data) in responses {
            let extent = &self.extents[request.eid as usize];
            extent.read(request.offset, data)?;
        }
        Ok(())
    }

    /*
     * Send a flush to all extents. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub fn region_flush(&self, flush_number: u64) -> Result<(), CrucibleError> {
        // XXX How to we convert between usize and u32 correctly?
        for eid in 0..self.def.extent_count() {
            let extent = &self.extents[eid as usize];
            extent.flush_block(flush_number)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::extent_path;
    use super::*;
    use crate::dump::dump_region;
    use bytes::BufMut;
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
        dump_region(dvec, None)?;

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
        dump_region(dvec, None)?;

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
        dump_region(dvec, Some(2))?;

        Ok(())
    }
}
