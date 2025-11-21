use std::os::fd::AsFd;

// Re-export from `crucible_raw_extent`
pub use crucible_raw_extent::{BLOCK_META_SIZE_BYTES, OnDiskMeta};

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
            bonus_sync_count: u32::MAX,
            defrag_count: u32::MAX,
        };
        let mut meta_buf = [0u8; BLOCK_META_SIZE_BYTES as usize];
        bincode::serialize_into(meta_buf.as_mut_slice(), &Some(m)).unwrap();
    }
}
