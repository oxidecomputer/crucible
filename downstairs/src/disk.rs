use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Result};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crucible_common::*;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct DiskDefinition {
    /**
     * The size of each block in bytes.  Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: u64,

    /**
     * How many whole extents comprise this disk?
     */
    extent_count: u32,
}

impl DiskDefinition {
    fn from_options(opts: &DiskOptions) -> Result<Self> {
        opts.validate()?;
        Ok(DiskDefinition {
            block_size: opts.block_size,
            extent_size: opts.extent_size,
            extent_count: 0,
        })
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct DiskOptions {
    /**
     * The size of each block in bytes.  Must be a power of 2, minimum 512.
     */
    block_size: u64,

    /**
     * How many blocks should appear in each extent?
     */
    extent_size: u64,
}

impl DiskOptions {
    fn validate(&self) -> Result<()> {
        if !self.block_size.is_power_of_two() {
            bail!("block size must be a power of two, not {}", self.block_size);
        }

        if self.block_size < 512 {
            bail!("minimum block size is 512 bytes, not {}", self.block_size);
        }

        if self.extent_size < 1 {
            bail!("extent size must be at least 1 block");
        }

        let bs = self.extent_size.saturating_mul(self.block_size);
        if bs > 10 * 1024 * 1024 {
            /*
             * For now, make sure we don't accidentally try to use a gigantic
             * extent.
             */
            bail!(
                "extent size {} x {} bytes = {}MB, bigger than 10MB",
                self.extent_size,
                self.block_size,
                bs / 1024 / 1024
            );
        }

        Ok(())
    }
}

impl Default for DiskOptions {
    fn default() -> Self {
        DiskOptions {
            block_size: 512,  /* XXX bigger? */
            extent_size: 100, /* XXX bigger? */
        }
    }
}

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
    out.push("disk.json");
    out
}

impl Extent {
    fn open<P: AsRef<Path>>(
        dir: P,
        def: &DiskDefinition,
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
        let bcount = def.extent_size.checked_add(1).unwrap();
        let size = def.block_size.checked_mul(bcount).unwrap();

        mkdir_for_file(&path)?;
        /*
         * If the extent file exists, just open it and move forward.
         * If it does not, then create it and set zero flush number.
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
            block_size: def.block_size,
            extent_size: def.extent_size,
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

        if block_offset > self.extent_size {
            bail!("block offset {} is past end of extent", block_offset);
        }
        if data.capacity() != self.block_size as usize {
            bail!(
                "block size {}, buffer is {}",
                self.block_size,
                data.capacity()
            );
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
        /*
         * XXX This read_exact only works because we have filled our buffer
         * with data ahead of time.  If we want to use an uninitialized
         * buffer, then we need a different read or type for the destination
         */
        inner.file.read_exact(data)?;

        println!(
            "read_block  eid:{} b_offset:{} f_offset:{} len:{} data:{:?}",
            self.number,
            block_offset,
            file_offset,
            data.len(),
            data
        );
        Ok(())
    }

    pub fn write_block(&self, block_offset: u64, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        if block_offset > self.extent_size {
            bail!("block offset {} is past end of extent", block_offset);
        }
        if data.len() != self.block_size as usize {
            bail!("block size {}, buffer is {}", self.block_size, data.len());
        }

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

        println!(
            "write_block eid:{} b_offset:{} f_offset:{}",
            self.number, block_offset, file_offset
        );

        Ok(())
    }

    pub fn flush(&self, new_flush: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        /*
         * XXX Remove this when upstairs starts sending us the correct
         * flush number for this extent.  For now we are still tracking
         * and updating flush numbers all downstairs.
         */
        if inner.meta.flush_number != new_flush {
            println!(
                "E[{}] Ignore provided fn:{} use our own:{}",
                self.number, new_flush, inner.meta.flush_number
            );
        }

        if !inner.meta.dirty {
            /*
             * If we have made no writes to this extent since the last flush
             * AND, our flush numbers match, we do not need to update the
             * extent on disk
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
            // XXX flush_number: new_flush,
            flush_number: inner.meta.flush_number + 1,
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
        println!("flush extent:{} with fn:{}", self.number, new_flush);
        Ok(())
    }
}

extern "C" {
    fn fsync(fildes: i32) -> i32;
}

// XXX This name and all things "disk" probably need a new name.
pub struct Disk {
    dir: PathBuf,
    def: DiskDefinition,
    extents: Vec<Extent>,
}

impl Disk {
    pub fn open<P: AsRef<Path>>(dir: P, options: DiskOptions) -> Result<Disk> {
        options.validate()?;

        let cp = config_path(dir.as_ref());
        let def = if let Some(def) = read_json_maybe(&cp)? {
            println!("opened existing disk file {:?}", cp);
            def
        } else {
            let def = DiskDefinition::from_options(&options)?;
            write_json(&cp, &def, false)?;
            println!("created new disk file {:?}", cp);
            def
        };

        /*
         * Open every extent that presently exists.
         */
        let mut disk = Disk {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
        };

        disk.open_extents()?;

        Ok(disk)
    }

    fn open_extents(&mut self) -> Result<()> {
        let next_eid = self.extents.len() as u32;
        for eid in next_eid..self.def.extent_count {
            self.extents.push(Extent::open(&self.dir, &self.def, eid)?);
            assert_eq!(self.extents[eid as usize].number, eid);
        }
        assert_eq!(self.def.extent_count as usize, self.extents.len());
        Ok(())
    }

    pub fn extend(&mut self, newsize: u32) -> Result<()> {
        if newsize < self.def.extent_count {
            bail!(
                "will not truncate {} -> {} for now",
                self.def.extent_count,
                newsize
            );
        }

        if newsize > self.def.extent_count {
            self.def.extent_count = newsize;
            write_json(config_path(&self.dir), &self.def, true)?;
            self.open_extents()?;
        }
        Ok(())
    }

    pub fn disk_def(&self) -> (u64, u64, u32) {
        (
            self.def.block_size,
            self.def.extent_size,
            self.def.extent_count,
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

    /*
     * Figure out the Extent that holds an offset, then the new offset
     * specific to that extent.
     *
     * XXX Can we assert offset will always be multiple of block size?
     */
    pub fn extent_from_offset(&self, offset: u64) -> Result<(usize, u64)> {
        let space_per_extent = self.def.block_size * self.def.extent_size;
        let eid: u64 = offset / space_per_extent;
        let block_in_extent: u64 = (offset - (eid * space_per_extent)) / 512;
        Ok((eid as usize, block_in_extent))
    }

    /*
     * XXX This logic will move up into the upstairs. The downstairs will
     * just get the extent EID and the block offset in that extent.
     */
    pub fn disk_write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let (eid, newoff) = self.extent_from_offset(offset).unwrap();
        let extent = &self.extents[eid as usize];
        println!("disk_write of {}, eid:{}, newoff:{}", offset, eid, newoff);
        extent.write_block(newoff, data)?;
        Ok(())
    }

    pub fn disk_read(
        &self,
        eid: u64,
        block_offset: u64,
        data: &mut BytesMut,
    ) -> Result<()> {
        /*
         * XXX Handle the case where the read length can span more than
         * one extent.  Soon!  This means we will also need to handle the
         * case where a read covers two extents.  So we will need to
         * recombine buffers from two extent.read_block() calls.
         */
        let extent = &self.extents[eid as usize];
        extent.read_block(block_offset, data)?;
        Ok(())
    }
    /*
     * Perhaps this can be a list of extents that need flush instead of
     * sending the whole Vec.  Something to think about XXX
     */
    pub fn disk_flush(
        &self,
        dep: Vec<u64>,
        flush_numbers: Vec<u64>,
    ) -> Result<()> {
        if flush_numbers.len()
            != usize::try_from(self.def.extent_count).unwrap()
        {
            bail!(
                "extent count {} does not match flush vector count {}",
                self.def.extent_count,
                dep.len()
            );
        }

        // XXX How to we convert between usize and u32 correctly?
        for eid in 0..self.def.extent_count {
            // We will need to pull out the value from dep that
            // each extent needs to use for flush number
            let extent = &self.extents[eid as usize];
            extent.flush(flush_numbers[eid as usize])?;
        }
        println!("Flush on {} extents", self.def.extent_count);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::extent_path;
    use super::*;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn extent_offset() {
        let def = DiskDefinition {
            block_size: 512,
            extent_size: 100,
            extent_count: 10,
        };
        let disk = Disk {
            dir: p("/dev/null"),
            def,
            extents: Vec::new(),
        };
        assert_eq!(disk.extent_from_offset(0).unwrap(), (0, 0));
        assert_eq!(disk.extent_from_offset(512).unwrap(), (0, 1));
        assert_eq!(disk.extent_from_offset(1024).unwrap(), (0, 2));
        assert_eq!(disk.extent_from_offset(1024 + 512).unwrap(), (0, 3));
        assert_eq!(disk.extent_from_offset(51200).unwrap(), (1, 0));
        assert_eq!(disk.extent_from_offset(51200 + 512).unwrap(), (1, 1));
        assert_eq!(disk.extent_from_offset(51200 + 1024).unwrap(), (1, 2));
        assert_eq!(disk.extent_from_offset(102400 - 512).unwrap(), (1, 99));
        assert_eq!(disk.extent_from_offset(102400).unwrap(), (2, 0));
    }

    #[test]
    fn extent_path_min() {
        assert_eq!(
            extent_path("/var/disk", u32::MIN),
            p("/var/disk/00/000/000")
        );
    }

    #[test]
    fn extent_path_three() {
        assert_eq!(extent_path("/var/disk", 3), p("/var/disk/00/000/003"));
    }

    #[test]
    fn extent_path_mid_hi() {
        assert_eq!(extent_path("/var/disk", 65536), p("/var/disk/00/010/000"));
    }

    #[test]
    fn extent_path_mid_lo() {
        assert_eq!(extent_path("/var/disk", 65535), p("/var/disk/00/00F/FFF"));
    }

    #[test]
    fn extent_path_max() {
        assert_eq!(
            extent_path("/var/disk", u32::MAX),
            p("/var/disk/FF/FFF/FFF")
        );
    }
}
