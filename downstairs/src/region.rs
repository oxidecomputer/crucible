use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Result};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crucible_common::*;

use tracing::instrument;

#[derive(Debug)]
pub struct Extent {
    number: u32,
    block_size: u64,
    extent_size: u64,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
pub struct Inner {
    file: File,
    meta: ExtentMeta,
}

/*
 * Warning, changing this struct will change what is written to and expected
 * from the physical storage device.
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ExtentMeta {
    ext_version: u32, // XXX Not currently connected to anything.
    /**
     * Increasing value provided from upstairs every time it connects to
     * a downstairs.  Used to help break ties if flash numbers are the same
     * on extents.
     */
    gen: u64, // XXX Not currently connected to anything.
    /**
     * Increasing value incremented on every write to an extent.
     * All mirrors of an extent should have the same value.
     */
    flush_number: u64,
    /**
     * Used to indicate data was written to disk, but not yet flushed
     * Should be set back to false once data has been flushed.
     */
    dirty: bool,
}

impl Default for ExtentMeta {
    fn default() -> ExtentMeta {
        ExtentMeta {
            ext_version: 1,
            gen: 0,
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
         * Store extent data in files within a directory hierarchy so that there
         * are not too many files in any level of that hierarchy.
         */
        let path = extent_path(dir, number);

        /*
         * Leave a block at the beginning of the file as a place to scribble
         * metadata.
         */
        let bcount = def.extent_size().checked_add(1).unwrap();
        let size = def.block_size().checked_mul(bcount).unwrap();

        /*
         * Open the extent file and verify the size is as we expect.
         */
        let mut file =
            match OpenOptions::new().read(true).write(true).open(&path) {
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
         * Read in the metadata from the beginning of the extent file.
         */
        let size = bincode::serialized_size(&ExtentMeta::default()).unwrap();
        let mut encoded: Vec<u8> = vec![0; size as usize];
        file.read_exact(&mut encoded)?;
        let buf: ExtentMeta = bincode::deserialize(&encoded[..]).unwrap();

        Ok(Extent {
            number,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Mutex::new(Inner {
                file,
                meta: ExtentMeta {
                    ext_version: buf.ext_version,
                    gen: buf.gen,
                    flush_number: buf.flush_number,
                    dirty: false,
                },
            }),
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
         * Store extent data in files within a directory hierarchy so that there
         * are not too many files in any level of that hierarchy.
         */
        let path = extent_path(dir, number);

        /*
         * Verify there are not existing extent files.
         */
        if Path::new(&path).exists() {
            bail!("Extent file already exists {:?}", path);
        }

        /*
         * Leave a block at the beginning of the file as a place to scribble
         * metadata.
         */
        let bcount = def.extent_size().checked_add(1).unwrap();
        let size = def.block_size().checked_mul(bcount).unwrap();

        mkdir_for_file(&path)?;
        /*
         */
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        file.set_len(size)?;
        file.seek(SeekFrom::Start(0))?;

        /*
         * Write out the new inner.meta structure for this new extent.
         */
        let meta = ExtentMeta::default();
        let encoded: Vec<u8> = bincode::serialize(&meta).unwrap();
        file.write_all(&encoded)?;
        file.flush()?;

        /*
         * Complete the construction of our new extent
         */
        Ok(Extent {
            number,
            block_size: def.block_size(),
            extent_size: def.extent_size(),
            inner: Mutex::new(Inner { file, meta }),
        })
    }

    fn flush_number(&self) -> u64 {
        self.inner.lock().unwrap().meta.flush_number
    }

    #[instrument]
    pub fn read(&self, byte_offset: u64, data: &mut BytesMut) -> Result<()> {
        self.check_input(byte_offset, data)?;

        /*
         * Skip metadata block
         */
        let file_offset = byte_offset + self.block_size;

        let mut inner = self.inner.lock().unwrap();
        inner.file.seek(SeekFrom::Start(file_offset))?;

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
    fn check_input(&self, byte_offset: u64, data: &[u8]) -> Result<()> {
        let total_size = self.block_size * self.extent_size;

        if (byte_offset + data.len() as u64) > total_size {
            bail!(
                "byte offset {} + data.len() {} is past end of extent {} ({})",
                byte_offset,
                data.len(),
                self.extent_size,
                total_size,
            );
        }

        Ok(())
    }

    #[instrument]
    pub fn write(&self, byte_offset: u64, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        self.check_input(byte_offset, data)?;

        if !inner.meta.dirty {
            inner.file.seek(SeekFrom::Start(0))?;
            let encoded: Vec<u8> = bincode::serialize(&inner.meta).unwrap();
            inner.file.write_all(&encoded)?;
            inner.file.flush()?;

            if unsafe { fsync(inner.file.as_raw_fd()) } == -1 {
                let e = std::io::Error::last_os_error();
                /*
                 * XXX Retry?  Mark extent as broken?
                 */
                bail!("extent {}: fsync 2 failure: {:?}", self.number, e);
            }

            inner.meta.dirty = true;
        }

        /*
         * Skip metadata block
         */
        let file_offset = byte_offset + self.block_size;

        inner.file.seek(SeekFrom::Start(file_offset))?;
        inner.file.write_all(data)?;
        inner.file.flush()?;

        Ok(())
    }

    #[instrument]
    pub fn flush_block(&self, new_flush: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        if !inner.meta.dirty {
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
            bail!("extent {}: fsync 1 failure: {:?}", self.number, e);
        }

        inner.file.seek(SeekFrom::Start(0))?;

        /*
         * When we write out the new flush number, the dirty bit in the file
         * should be set back to false
         * We create a new ExtentMeta structure and write that out first,
         * before we update our internal structure.  This gives us a
         * chance to recover if the write or fsync fail.
         */
        let new_meta = ExtentMeta {
            ext_version: inner.meta.ext_version,
            gen: inner.meta.gen,
            flush_number: new_flush,
            dirty: false,
        };

        let encoded: Vec<u8> = bincode::serialize(&new_meta).unwrap();
        inner.file.write_all(&encoded)?;
        inner.file.flush()?;

        /*
         * Fsync the metadata update
         */
        if unsafe { fsync(inner.file.as_raw_fd()) } == -1 {
            let e = std::io::Error::last_os_error();
            /*
             * XXX Retry?  Mark extent as broken?
             */
            bail!("extent {}: fsync 2 failure: {:?}", self.number, e);
        }

        inner.meta = new_meta;

        Ok(())
    }
}

extern "C" {
    fn fsync(fildes: i32) -> i32;
}

#[derive(Debug)]
pub struct Region {
    dir: PathBuf,
    def: RegionDefinition,
    extents: Vec<Extent>,
}

impl Region {
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
        if bincode::serialized_size(&ExtentMeta::default()).unwrap()
            > def.block_size()
        {
            bail!(
                "Extent metadata will not fit in block size {}",
                def.block_size()
            );
        }
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

    pub fn open<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
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

        println!("Opened existing region file {:?}", cp);
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

    pub fn region_def(&self) -> (u64, u64, u32) {
        (
            self.def.block_size(),
            self.def.extent_size(),
            self.def.extent_count(),
        )
    }
    pub fn def(&self) -> RegionDefinition {
        self.def
    }

    pub fn versions(&self) -> Vec<u64> {
        let mut ver = self
            .extents
            .iter()
            .map(|e| e.flush_number())
            .collect::<Vec<_>>();

        if ver.len() > 12 {
            ver = ver[0..12].to_vec();
        }
        println!("Current flush_numbers [0..12]: {:?}", ver);

        self.extents
            .iter()
            .map(|e| e.flush_number())
            .collect::<Vec<_>>()
    }

    #[instrument]
    pub fn region_write(
        &self,
        eid: u64,
        byte_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let extent = &self.extents[eid as usize];
        extent.write(byte_offset, data)?;
        Ok(())
    }

    #[instrument]
    pub fn region_read(
        &self,
        eid: u64,
        byte_offset: u64,
        data: &mut BytesMut,
    ) -> Result<()> {
        let extent = &self.extents[eid as usize];
        extent.read(byte_offset, data)?;
        Ok(())
    }

    /*
     * Send a flush to all extents.  The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub fn region_flush(
        &self,
        _dep: Vec<u64>,
        flush_number: u64,
    ) -> Result<()> {
        // XXX How to we convert between usize and u32 correctly?
        for eid in 0..self.def.extent_count() {
            // We will need to pull out the value from dep that
            // each extent needs to use for flush number
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
    use crate::Opt;
    use bytes::BufMut;
    use std::fs::remove_dir_all;
    use std::path::PathBuf;
    use structopt::StructOpt;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn new_extent() -> Extent {
        let ff = File::open("/dev/null").unwrap();
        let em = ExtentMeta::default();

        let inn = Inner { file: ff, meta: em };

        /*
         * Note:  All the tests expect 512 and 100, so if you change
         * these, then change the tests!
         */
        Extent {
            number: 0,
            block_size: 512,
            extent_size: 100,
            inner: Mutex::new(inn),
        }
    }

    pub fn opt_from_string(args: String) -> Result<Opt> {
        let opt: Opt = Opt::from_iter(args.split(' '));
        Ok(opt)
    }

    pub fn test_cleanup() {
        if Path::new("/tmp/ds_test").exists() {
            remove_dir_all("/tmp/ds_test").unwrap();
        }
    }

    #[test]
    fn new_region() -> Result<()> {
        test_cleanup();
        let my_arg = "-- -c -p 3801 -d /tmp/ds_test/1".to_string();
        let opt = opt_from_string(my_arg).unwrap();
        //let opt = Opt::from_string(my_arg).unwrap();
        let _ = Region::create(&opt.data, Default::default()).unwrap();
        remove_dir_all("/tmp/ds_test/1").unwrap();
        Ok(())
    }
    #[test]
    fn new_existing_region() -> Result<()> {
        test_cleanup();
        let my_arg = "-- -c -p 3801 -d /tmp/ds_test/2".to_string();
        let opt = opt_from_string(my_arg).unwrap();
        let _ = Region::create(&opt.data, Default::default()).unwrap();
        let _ = Region::open(&opt.data, Default::default());
        remove_dir_all("/tmp/ds_test/2").unwrap();
        Ok(())
    }
    #[test]
    #[should_panic]
    fn bad_import_region() -> () {
        test_cleanup();
        let my_arg = "-- -c -p 3801 -d /tmp/ds_test/3".to_string();
        let opt = opt_from_string(my_arg).unwrap();
        let _ = Region::open(&opt.data, Default::default()).unwrap();
        remove_dir_all("/tmp/ds_test/3").unwrap();
        ()
    }

    #[test]
    fn extent_io_valid() {
        let ext = new_extent();
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        assert_eq!((), ext.check_input(0, &data).unwrap());
        assert_eq!((), ext.check_input(99, &data).unwrap());
    }

    #[test]
    fn extent_io_valid2() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent();

        assert_eq!((), ext.check_input(0, &data).unwrap());
        assert_eq!((), ext.check_input(97 * 512, &data).unwrap());
    }

    #[test]
    fn extent_io_valid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent();
        ext.check_input(0, &data).unwrap();
    }

    #[test]
    fn extent_io_invalid_size() {
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        let ext = new_extent();
        ext.check_input(0, &data).unwrap();
    }

    #[test]
    fn extent_io_invalid_size_small() {
        let mut data = BytesMut::with_capacity(511);
        data.put(&[1; 511][..]);

        let ext = new_extent();
        ext.check_input(0, &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_bad_block() {
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        let ext = new_extent();
        ext.check_input(100 * 512, &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent();
        ext.check_input(99 * 512, &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent();
        assert_eq!((), ext.check_input(1, &data).unwrap());
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
}
