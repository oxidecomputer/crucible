use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Result};
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
    flush_number: u64,
    dirty: bool,
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
         */
        let bcount = def.extent_size.checked_add(1).unwrap();
        let size = def.block_size.checked_mul(bcount).unwrap();

        mkdir_for_file(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        file.set_len(size)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let flush_number = u64::from_le_bytes(buf);

        /*
         * Read the flush number from the first block:
         */

        Ok(Extent {
            number,
            block_size: def.block_size,
            extent_size: def.extent_size,
            inner: Mutex::new(Inner {
                file,
                flush_number,
                dirty: false,
            }),
        })
    }

    fn flush_number(&self) -> u64 {
        self.inner.lock().unwrap().flush_number
    }

    pub fn read_block(&self, block_offset: u64, data: &mut [u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = true;

        if block_offset > self.extent_size {
            bail!("block offset {} is past end of extent", block_offset);
        }
        if data.len() != self.block_size as usize {
            bail!("block size {}, buffer is {}", self.block_size, data.len());
        }

        /*
         * Skip metadata block:
         */
        let block_offset = block_offset.checked_add(1).unwrap();

        /*
         * Calculate offset in file:
         */
        let file_offset = self.block_size.checked_mul(block_offset).unwrap();

        inner.file.seek(SeekFrom::Start(file_offset));
        inner.file.read_exact(data)?;

        Ok(())
    }

    pub fn write_block(&self, block_offset: u64, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = true;

        if block_offset > self.extent_size {
            bail!("block offset {} is past end of extent", block_offset);
        }
        if data.len() != self.block_size as usize {
            bail!("block size {}, buffer is {}", self.block_size, data.len());
        }

        /*
         * Skip metadata block:
         */
        let block_offset = block_offset.checked_add(1).unwrap();

        /*
         * Calculate offset in file:
         */
        let file_offset = self.block_size.checked_mul(block_offset).unwrap();

        inner.file.seek(SeekFrom::Start(file_offset));
        inner.file.write_all(data)?;
        inner.file.flush()?;

        Ok(())
    }

    pub fn flush(&self, new_version: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        if !inner.dirty {
            /*
             * If we have made no writes to this extent since the last flush, we
             * do not need to update it on disk.
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

        inner.file.seek(SeekFrom::Start(0));
        let buf = new_version.to_le_bytes();
        inner.file.write_all(&buf)?;
        inner.file.flush()?;

        if unsafe { fsync(inner.file.as_raw_fd()) } == -1 {
            let e = std::io::Error::last_os_error();
            /*
             * XXX Retry?  Mark extent as broken?
             */
            bail!("extent {}: fsync 2 failure: {:?}", self.number, e);
        }

        inner.flush_number = new_version;
        inner.dirty = false;
        Ok(())
    }
}

extern "C" {
    fn fsync(fildes: i32) -> i32;
}

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

        self.def.extent_count = newsize;
        write_json(config_path(&self.dir), &self.def, true)?;

        self.open_extents()?;
        Ok(())
    }

    pub fn versions(&self) -> Vec<u64> {
        self.extents
            .iter()
            .map(|e| e.flush_number())
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod test {
    use super::extent_path;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
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
