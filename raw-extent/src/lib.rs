//! Data and on-disk structures for raw extent files
use crucible_common::{CrucibleError, ExtentId};
use crucible_protocol::BlockContext;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

/// Equivalent to `ExtentMeta`, but ordered for efficient on-disk serialization
///
/// In particular, the `dirty` byte is first, so it's easy to read at a known
/// offset within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,

    // Extra data added for debugging purposes
    pub bonus_sync_count: u32,
    pub defrag_count: u32,
}

impl OnDiskMeta {
    /// Looks up the version tag
    ///
    /// Across all of our raw file formats, `OnDiskMeta` is guaranteed to be
    /// placed at the end of the file in a `BLOCK_META_SIZE_BYTES`-length chunk,
    /// so we can get a tag without knowing anything else about the file.
    pub fn get_version_tag(
        path: &Path,
        extent_number: ExtentId,
    ) -> Result<u32, CrucibleError> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(false)
            .open(path)
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
pub const BLOCK_META_SIZE_BYTES: u64 = 32;

/// Equivalent to `DownstairsBlockContext`, but without one's own block number
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub struct OnDiskDownstairsBlockContext {
    pub block_context: BlockContext,
    pub on_disk_hash: u64,
    pub flush_id: u16,
}

/// Size of backup data
///
/// This must be large enough to fit an `Option<OnDiskDownstairsBlockContext>`
/// serialized using `bincode`.
pub const BLOCK_CONTEXT_SLOT_SIZE_BYTES: u64 = 48;
