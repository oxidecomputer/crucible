use crate::{extent::extent_path, CrucibleError};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::AsFd;
use std::path::Path;

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
///
/// In particular, the `dirty` byte is first, so it's easy to read at a known
/// offset within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,
}

impl OnDiskMeta {
    /// Looks up the version tag
    ///
    /// Across all of our raw file formats, `OnDiskMeta` is guaranteed to be
    /// placed at the end of the file in a `BLOCK_META_SIZE_BYTES`-length chunk,
    /// so we can get a tag without knowing anything else about the file.
    pub fn get_version_tag(
        dir: &Path,
        extent_number: u32,
    ) -> Result<u32, CrucibleError> {
        let path = extent_path(dir, extent_number);
        let mut f = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&path)
            .map_err(|e| {
                CrucibleError::IoError(format!(
                    "extent {extent_number}: open of {path:?} failed: {e}",
                ))
            })?;

        let mut buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        f.seek(SeekFrom::End(-(BLOCK_META_SIZE_BYTES as i64)))?;
        f.read_exact(&mut buf)?;
        let meta: OnDiskMeta = bincode::deserialize(&buf)
            .map_err(|e| CrucibleError::BadMetadata(e.to_string()))?;
        Ok(meta.ext_version)
    }
}

/// Size of metadata region
///
/// This must be large enough to contain an `OnDiskMeta` serialized using
/// `bincode`.
pub(super) const BLOCK_META_SIZE_BYTES: u64 = 32;

/// Call `pread` repeatedly to read an entire buffer
///
/// Quoth the standard,
///
/// > The value returned may be less than nbyte if the number of bytes left in
/// > the file is less than nbyte, if the read() request was interrupted by a
/// > signal, or if the file is a pipe or FIFO or special file and has fewer
/// > than nbyte bytes immediately available for reading. For example, a read()
/// > from a file associated with a terminal may return one typed line of data.
///
/// We don't have to worry about most of these conditions, but it may be
/// possible for Crucible to be interrupted by a signal, so let's play it safe.
pub(super) fn pread_all<F: AsFd + Copy>(
    fd: F,
    mut buf: &mut [u8],
    mut offset: i64,
) -> Result<(), nix::errno::Errno> {
    while !buf.is_empty() {
        let n = nix::sys::uio::pread(fd, buf, offset)?;
        offset += n as i64;
        buf = &mut buf[n..];
    }
    Ok(())
}

/// Call `pwrite` repeatedly to write an entire buffer
///
/// See details for why this is necessary in [`pread_all`]
pub(super) fn pwrite_all<F: AsFd + Copy>(
    fd: F,
    mut buf: &[u8],
    mut offset: i64,
) -> Result<(), nix::errno::Errno> {
    while !buf.is_empty() {
        let n = nix::sys::uio::pwrite(fd, buf, offset)?;
        offset += n as i64;
        buf = &buf[n..];
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn on_disk_meta_serialized_size() {
        let m = OnDiskMeta {
            dirty: true,
            gen_number: u64::MAX,
            flush_number: u64::MAX,
            ext_version: u32::MAX,
        };
        let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta_buf.as_mut_slice(), &Some(m)).unwrap();
    }
}
