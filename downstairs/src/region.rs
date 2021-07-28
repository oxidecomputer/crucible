use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Result};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crucible_common::*;

pub struct Extent {
    number: u32,
    block_size: u64,
    extent_size: u64,
    inner: Mutex<Inner>,
}

pub struct Inner {
    file: File,
    meta: ExtentMeta,
}

/*
 * Warning, changing this struct will change what is written to and expected
 * from the physical storage device.
 */
#[derive(Deserialize, Serialize)]
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
         * XXX Assert somewhere (compile time?)  that struct ExtentMeta
         * size is always < one block?
         */
        let bcount = def.extent_size().checked_add(1).unwrap();
        let size = def.block_size().checked_mul(bcount).unwrap();

        mkdir_for_file(&path)?;
        /*
         * If the extent file exists, just open it and move forward.
         * If it does not, then create it and set to zero the flush number.
         * If the extent should have existed, we rely on the upper layers to
         * check the flush number and restore missing contents.
         */
        let mut file =
            match OpenOptions::new().read(true).write(true).open(&path) {
                Err(_e) => {
                    /*
                     * XXX Should we check or log the error here?
                     * We should know if it's expected to find a file and we
                     * do not.  Also might want to identify other possible
                     * errors like bad permissions, out of space, etc.
                     */
                    let mut new_file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&path)?;

                    new_file.set_len(size)?;
                    new_file.seek(SeekFrom::Start(0))?;
                    new_file
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

        let size = bincode::serialized_size(&ExtentMeta::default()).unwrap();
        let mut encoded: Vec<u8> = vec![0; size as usize];
        file.read_exact(&mut encoded)?;
        let buf: ExtentMeta = bincode::deserialize(&encoded[..]).unwrap();

        /*
         * Read the flush number from the first block:
         */

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

    fn flush_number(&self) -> u64 {
        self.inner.lock().unwrap().meta.flush_number
    }

    pub fn read_block(
        &self,
        block_offset: u64,
        data: &mut BytesMut,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        self.check_input(block_offset, data)?;

        /*
         * Skip metadata block:
         */
        let block_offset = block_offset.checked_add(1).unwrap();

        /*
         * Calculate offset in file:
         */
        let file_offset = self.block_size.checked_mul(block_offset).unwrap();

        inner.file.seek(SeekFrom::Start(file_offset))?;
        /*
         * XXX This read_exact only works because we have filled our buffer
         * with data ahead of time.  If we want to use an uninitialized
         * buffer, then we need a different read or type for the destination
         */
        inner.file.read_exact(data)?;

        Ok(())
    }

    /*
     * Verify that the requested block offset and size of the buffer
     * will fit within the extent.
     *
     * Note that the checks here do take into account that the first block
     * is the metadata block.
     */
    fn check_input(&self, block_offset: u64, data: &[u8]) -> Result<()> {
        if block_offset >= self.extent_size {
            bail!(
                "block offset {} is past end of extent {}",
                block_offset,
                self.extent_size
            );
        }
        if data.len() < self.block_size as usize {
            bail!(
                "buffer {} is less than block size {}",
                data.len(),
                self.block_size
            );
        }

        let rem = data.len() % self.block_size as usize;
        if rem != 0 {
            bail!(
                "buffer {} is not mutliple of block size {}",
                data.len(),
                self.block_size
            );
        }

        let data_blocks = data.len() as u64 / self.block_size as u64;
        if data_blocks + block_offset > self.extent_size {
            bail!(
                "Start block offset + blocks {} is > extent size {}",
                data_blocks + block_offset,
                self.extent_size
            );
        }

        Ok(())
    }

    pub fn write_block(&self, block_offset: u64, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        self.check_input(block_offset, data)?;

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
         * Skip metadata block:
         */
        let block_offset = block_offset.checked_add(1).unwrap();

        /*
         * Calculate offset in file:
         */
        let file_offset = self.block_size.checked_mul(block_offset).unwrap();

        inner.file.seek(SeekFrom::Start(file_offset))?;
        inner.file.write_all(data)?;
        inner.file.flush()?;

        Ok(())
    }

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

pub struct Region {
    dir: PathBuf,
    def: RegionDefinition,
    extents: Vec<Extent>,
}

impl Region {
    pub fn open<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
    ) -> Result<Region> {
        options.validate()?;

        let cp = config_path(dir.as_ref());
        let def = if let Some(def) = read_json_maybe(&cp)? {
            println!("opened existing region file {:?}", cp);
            def
        } else {
            let def = RegionDefinition::from_options(&options)?;
            write_json(&cp, &def, false)?;
            println!("created new region file {:?}", cp);
            def
        };

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
        };

        region.open_extents()?;

        Ok(region)
    }

    fn open_extents(&mut self) -> Result<()> {
        let next_eid = self.extents.len() as u32;
        for eid in next_eid..self.def.extent_count() {
            self.extents.push(Extent::open(&self.dir, &self.def, eid)?);
            assert_eq!(self.extents[eid as usize].number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());
        Ok(())
    }

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
            self.open_extents()?;
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

    pub fn versions(&self) -> Vec<u64> {
        println!(
            "Current flush_numbers: {:?}",
            self.extents
                .iter()
                .map(|e| e.flush_number())
                .collect::<Vec<_>>()
        );

        self.extents
            .iter()
            .map(|e| e.flush_number())
            .collect::<Vec<_>>()
    }

    pub fn region_write(
        &self,
        eid: u64,
        block_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let extent = &self.extents[eid as usize];
        extent.write_block(block_offset, data)?;
        Ok(())
    }

    pub fn region_read(
        &self,
        eid: u64,
        block_offset: u64,
        data: &mut BytesMut,
    ) -> Result<()> {
        let extent = &self.extents[eid as usize];
        extent.read_block(block_offset, data)?;
        Ok(())
    }

    /*
     * Send a flush to all extents.  The provided flush number is
     * what an extent should use if a flush is required.
     */
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
    use bytes::BufMut;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    fn new_extent() -> Extent {
        let ff = File::open("/dev/null").unwrap();
        let em = ExtentMeta::default();

        let inn = Inner { file: ff, meta: em };

        /*
         * Note:  All the tests expext 512 and 100, so if you change
         * these, then change the tests!
         */
        Extent {
            number: 0,
            block_size: 512,
            extent_size: 100,
            inner: Mutex::new(inn),
        }
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
        assert_eq!((), ext.check_input(98, &data).unwrap());
    }

    #[test]
    fn extent_io_valid_large() {
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        let ext = new_extent();
        assert_eq!((), ext.check_input(0, &data).unwrap());
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_size() {
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        let ext = new_extent();
        ext.check_input(0, &data).unwrap();
    }

    #[test]
    #[should_panic]
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
        ext.check_input(100, &data).unwrap();
    }
    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        let ext = new_extent();
        assert_eq!((), ext.check_input(99, &data).unwrap());
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
