// Copyright 2023 Oxide Computer Company
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::File;

use anyhow::{anyhow, bail, Result};
use nix::unistd::{sysconf, SysconfVar};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::region::JobOrReconciliationId;
use crucible_common::*;
use repair_client::types::FileType;

use super::*;

#[derive(Debug)]
pub struct Extent {
    pub number: ExtentId,
    read_only: bool,
    iov_max: usize,

    /// Inner contains information about the actual extent file that holds the
    /// data, the metadata about that extent, and the set of dirty blocks that
    /// have been written to since last flush.  We use dynamic dispatch here to
    /// support multiple extent implementations.
    inner: Box<dyn ExtentInner + Send + Sync>,
}

pub(crate) trait ExtentInner: Send + Sync + Debug {
    fn gen_number(&self) -> Result<u64, CrucibleError>;
    fn flush_number(&self) -> Result<u64, CrucibleError>;
    fn dirty(&self) -> Result<bool, CrucibleError>;

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError>;

    fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
        iov_max: usize,
    ) -> Result<ExtentReadResponse, CrucibleError>;

    fn write(
        &mut self,
        job_id: JobId,
        write: &ExtentWrite,
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError>;

    /// Reads zero or more context slots for each block in the given range
    ///
    /// # Panics
    /// If an extent implementation cannot get block contexts separately from a
    /// read, this is allowed to panic.
    #[cfg(test)]
    fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>, CrucibleError>;

    /// Sets the dirty flag and updates a block context
    ///
    /// This should only be called from test functions, where we want to
    /// manually modify block contexts and test associated behavior
    ///
    /// # Panics
    /// If an extent implementation cannot set block contexts separately from a
    /// write, this is allowed to panic.
    #[cfg(test)]
    fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError>;
}

/// BlockContext, with the addition of block index and on_disk_hash
#[derive(Copy, Clone)]
pub struct DownstairsBlockContext {
    pub block_context: BlockContext,

    pub block: u64,
    pub on_disk_hash: u64,
}

/// An extent can be Opened or Closed. If Closed, it is probably being updated
/// out of band. If Opened, then this extent is accepting operations.
#[derive(Debug)]
pub enum ExtentState {
    Opened(Extent),
    Closed,
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

/// Deprecated extent version for SQLite-backed metadata
#[cfg(any(test, feature = "integration-tests"))]
#[allow(dead_code)]
pub const EXTENT_META_SQLITE: u32 = 1;

/// Extent version for raw-file-backed metadata
///
/// See [`extent_inner_raw::RawInner`] for the implementation.
pub const EXTENT_META_RAW: u32 = 2;

impl ExtentMeta {
    pub fn new(ext_version: u32) -> ExtentMeta {
        ExtentMeta {
            ext_version,
            gen_number: 0,
            flush_number: 0,
            dirty: false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum ExtentType {
    Data,
}

/**
 * Take an ExtentType and translate it into the corresponding
 * FileType from the repair client.
 */
impl ExtentType {
    pub fn to_file_type(self) -> FileType {
        match self {
            ExtentType::Data => FileType::Data,
        }
    }
}

/**
 * Produce the string name of the data file for a given extent number
 */
pub fn extent_file_name(number: ExtentId, extent_type: ExtentType) -> String {
    match extent_type {
        ExtentType::Data => {
            format!("{:03X}", number.0 & 0xFFF)
        }
    }
}

/**
 * Produce a PathBuf that refers to the containing directory for extent
 * "number", anchored under "dir".
 */
pub fn extent_dir<P: AsRef<Path>>(dir: P, number: ExtentId) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push(format!("{:02X}", (number.0 >> 24) & 0xFF));
    out.push(format!("{:03X}", (number.0 >> 12) & 0xFFF));
    out
}

/**
 * Produce a PathBuf that refers to the backing file for extent "number",
 * anchored under "dir".
 */
pub fn extent_path<P: AsRef<Path>>(dir: P, number: ExtentId) -> PathBuf {
    extent_dir(dir, number).join(extent_file_name(number, ExtentType::Data))
}

/**
 * Produce a PathBuf that refers to the copy directory we create for
 * a given extent "number",  This directory will hold the files we
 * transfer from the source downstairs.
 * anchored under "dir".
 */
pub fn copy_dir<P: AsRef<Path>>(dir: P, number: ExtentId) -> PathBuf {
    extent_dir(dir, number)
        .join(extent_file_name(number, ExtentType::Data))
        .with_extension("copy")
}

/**
 * Produce a PathBuf that refers to the replace directory we use for
 * a given extent "number".  This directory is generated (see below) when
 * all the files needed for repairing an extent have been added to the
 * copy directory and we are ready to start replacing the extents files.
 * anchored under "dir".  The actual generation of this directory will
 * be done by renaming the copy directory.
 */
pub fn replace_dir<P: AsRef<Path>>(dir: P, number: ExtentId) -> PathBuf {
    extent_dir(dir, number)
        .join(extent_file_name(number, ExtentType::Data))
        .with_extension("replace")
}

/**
 * Produce a PathBuf that refers to the completed directory we use for
 * a given extent "number".  This directory is generated (see below) when
 * all the files needed for repairing an extent have been copied to their
 * final destination and everything has been flushed.
 * The actual generation of this directory will be done by renaming the
 * replace directory.
 */
pub fn completed_dir<P: AsRef<Path>>(dir: P, number: ExtentId) -> PathBuf {
    extent_dir(dir, number)
        .join(extent_file_name(number, ExtentType::Data))
        .with_extension("completed")
}

/**
 * Remove directories associated with repair except for the replace
 * directory. Replace is handled specifically during extent open.
 */
pub fn remove_copy_cleanup_dir<P: AsRef<Path>>(
    dir: P,
    eid: ExtentId,
) -> Result<()> {
    for f in [copy_dir, completed_dir] {
        let d = f(&dir, eid);
        if Path::new(&d).exists() {
            std::fs::remove_dir_all(&d)?;
        }
    }
    Ok(())
}

impl Extent {
    fn get_iov_max() -> Result<usize> {
        let i: i64 = sysconf(SysconfVar::IOV_MAX)?
            .ok_or_else(|| anyhow!("IOV_MAX returned None!"))?;
        Ok(i.try_into()?)
    }

    /**
     * Open an existing extent file at the location requested.
     */
    pub fn open(
        dir: &Path,
        def: &RegionDefinition,
        number: ExtentId,
        read_only: bool,
        log: &Logger,
    ) -> Result<Extent> {
        /*
         * Store extent data in files within a directory hierarchy so that
         * there are not too many files in any level of that hierarchy.
         */
        let path = extent_path(dir, number);

        // If there was an unfinished copy, then clean it up here (??)
        remove_copy_cleanup_dir(dir, number)?;

        // If the replace directory exists for this extent, then it means
        // a repair was interrupted before it could finish.  We will continue
        // the repair before we open the extent.
        let replace_dir = replace_dir(dir, number);
        if !read_only && Path::new(&replace_dir).exists() {
            info!(
                log,
                "Extent {} found replacement dir, finishing replacement",
                number
            );
            move_replacement_extent(dir, number, log)?;
        }

        if path.with_extension("db").exists() {
            bail!("SQLite extents are no longer supported");
        }

        // Pick the format for the downstairs files.
        let inner: Box<dyn ExtentInner + Send + Sync> = {
            match extent_inner_raw_common::OnDiskMeta::get_version_tag(
                dir, number,
            )? {
                EXTENT_META_RAW => Box::new(extent_inner_raw::RawInner::open(
                    dir, def, number, read_only, log,
                )?),
                i => {
                    return Err(CrucibleError::IoError(format!(
                        "raw extent {number} has unknown tag {i}"
                    ))
                    .into())
                }
            }
        };

        let extent = Extent {
            number,
            read_only,
            iov_max: Extent::get_iov_max()?,
            inner,
        };

        Ok(extent)
    }

    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn dirty(&self) -> bool {
        self.inner.dirty().unwrap()
    }

    /**
     * Close an extent and the metadata db files for it.
     */
    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn close(self) -> Result<(u64, u64, bool), CrucibleError> {
        let gen = self.inner.gen_number().unwrap();
        let flush = self.inner.flush_number().unwrap();
        let dirty = self.inner.dirty().unwrap();

        Ok((gen, flush, dirty))
    }

    /**
     * Create an extent at the location requested.
     * Start off with the default meta data.
     * Note that this function is not safe to run concurrently.
     */
    pub fn create(
        dir: &Path,
        def: &RegionDefinition,
        number: ExtentId,
        backend: Backend,
    ) -> Result<Extent> {
        /*
         * Store extent data in files within a directory hierarchy so that
         * there are not too many files in any level of that hierarchy.
         */
        {
            let path = extent_path(dir, number);

            /*
             * Verify there are not existing extent files.
             */
            if Path::new(&path).exists() {
                bail!("Extent file already exists {:?}", path);
            }
        }
        remove_copy_cleanup_dir(dir, number)?;

        let inner: Box<dyn ExtentInner + Send + Sync> = match backend {
            Backend::RawFile => {
                Box::new(extent_inner_raw::RawInner::create(dir, def, number)?)
            }
        };

        /*
         * Complete the construction of our new extent
         */
        Ok(Extent {
            number,
            read_only: false,
            iov_max: Extent::get_iov_max()?,
            inner,
        })
    }

    pub fn number(&self) -> ExtentId {
        self.number
    }

    /// Read the data and metadata off underlying storage
    ///
    /// The size of the read is determined by the `capacity` of the (allocated
    /// but uninitialized) buffer in `req`.
    ///
    /// If an error occurs while processing any of the requests, an error is
    /// returned.  Otherwise, the value in `ExtentReadResponse::data` is
    /// guaranteed to be fully initialized and of the requested length.
    #[instrument]
    pub async fn read(
        &mut self,
        job_id: JobId,
        req: ExtentReadRequest,
    ) -> Result<ExtentReadResponse, CrucibleError> {
        let num_blocks =
            (req.data.len() / req.offset.block_size_in_bytes() as usize) as u64;
        cdt::extent__read__start!(|| (job_id.0, self.number.0, num_blocks));

        let out = self.inner.read(job_id, req, self.iov_max)?;
        cdt::extent__read__done!(|| (job_id.0, self.number.0, num_blocks));

        Ok(out)
    }

    #[instrument]
    pub async fn write(
        &mut self,
        job_id: JobId,
        write: ExtentWrite,
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        let num_blocks = write.block_contexts.len() as u64;
        cdt::extent__write__start!(|| (job_id.0, self.number.0, num_blocks));

        self.inner
            .write(job_id, &write, only_write_unwritten, self.iov_max)?;

        cdt::extent__write__done!(|| (job_id.0, self.number.0, num_blocks));

        Ok(())
    }

    #[instrument]
    pub(crate) async fn flush<I: Into<JobOrReconciliationId> + Debug>(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        id: I, // only used for logging
        log: &Logger,
    ) -> Result<(), CrucibleError> {
        let job_id: JobOrReconciliationId = id.into();

        if !self.inner.dirty()? {
            /*
             * If we have made no writes to this extent since the last flush,
             * we do not need to update the extent on disk
             */
            return Ok(());
        }

        // Read only extents should never have the dirty bit set. If they do,
        // bail
        if self.read_only {
            // XXX Make this a panic?
            error!(log, "read-only extent {} has dirty bit set!", self.number);
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        self.inner.flush(new_flush, new_gen, job_id)
    }

    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn get_meta_info(&self) -> ExtentMeta {
        ExtentMeta {
            ext_version: 0,
            gen_number: self.inner.gen_number().unwrap(),
            flush_number: self.inner.flush_number().unwrap(),
            dirty: self.inner.dirty().unwrap(),
        }
    }

    /// Sets the dirty flag and a single block context
    ///
    /// # Panics
    /// If the inner extent implementation does not support setting block
    /// contexts separately from a write operation
    #[cfg(test)]
    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.inner.set_dirty_and_block_context(block_context)
    }

    /// Gets zero or more block contexts for each block in the given range
    ///
    /// # Panics
    /// If the inner extent implementation does not support getting block
    /// contexts separately from a read operation
    #[cfg(test)]
    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn get_block_contexts(
        &mut self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>, CrucibleError> {
        self.inner.get_block_contexts(block, count)
    }
}

/**
 * Copy the contents of the replacement directory on to the extent
 * files in the extent directory.
 */
pub(crate) fn move_replacement_extent<P: AsRef<Path>>(
    region_dir: P,
    eid: ExtentId,
    log: &Logger,
) -> Result<(), CrucibleError> {
    let destination_dir = extent_dir(&region_dir, eid);
    let extent_file_name = extent_file_name(eid, ExtentType::Data);
    let replace_dir = replace_dir(&region_dir, eid);
    let completed_dir = completed_dir(&region_dir, eid);

    assert!(Path::new(&replace_dir).exists());
    assert!(!Path::new(&completed_dir).exists());

    info!(
        log,
        "Copy files from {:?} in {:?}", replace_dir, destination_dir,
    );

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
    sync_path(&original_file, log)?;
    sync_path(&destination_dir, log)?;

    // After we have all files: move the copy dir.
    info!(
        log,
        "Move directory  {:?} to {:?}", replace_dir, completed_dir
    );
    std::fs::rename(replace_dir, &completed_dir)?;

    sync_path(&destination_dir, log)?;

    std::fs::remove_dir_all(&completed_dir)?;

    sync_path(&destination_dir, log)?;
    Ok(())
}

/**
 * Given a path to a directory or file, open it, then fsync it.
 * If the file is already open, then just fsync it yourself.
 */
pub fn sync_path<P: AsRef<Path> + std::fmt::Debug>(
    path: P,
    log: &Logger,
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
    debug!(log, "fsync completed for: {:?}", path);

    Ok(())
}

/// Verify that the requested block offset and size of the buffer
/// will fit within the extent.
pub(crate) fn check_input(
    extent_size: Block,
    offset: Block,
    data_len: usize,
) -> Result<(), CrucibleError> {
    let block_size = extent_size.block_size_in_bytes() as u64;
    /*
     * Only accept block-aligned operations
     */
    if data_len % block_size as usize != 0 {
        crucible_bail!(DataLenUnaligned);
    }

    if offset.block_size_in_bytes() != block_size as u32 {
        crucible_bail!(BlockSizeMismatch);
    }

    if offset.shift != extent_size.shift {
        crucible_bail!(BlockSizeMismatch);
    }

    let total_size = block_size * extent_size.value;
    let byte_offset = offset.value * block_size;

    if (byte_offset + data_len as u64) > total_size {
        crucible_bail!(OffsetInvalid);
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn extent_io_valid() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        check_input(extent_size, Block::new_512(0), data.len()).unwrap();
        check_input(extent_size, Block::new_512(99), data.len()).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_large() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        check_input(extent_size, Block::new_512(0), data.len()).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_small() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(511);
        data.put(&[1; 511][..]);

        check_input(extent_size, Block::new_512(0), data.len()).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_bad_block() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        check_input(extent_size, Block::new_512(100), data.len()).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        check_input(extent_size, Block::new_512(99), data.len()).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_large() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        check_input(extent_size, Block::new_512(1), data.len()).unwrap();
    }

    #[test]
    fn extent_name_basic() {
        assert_eq!(extent_file_name(ExtentId(4), ExtentType::Data), "004");
    }

    #[test]
    fn extent_name_basic_two() {
        assert_eq!(extent_file_name(ExtentId(10), ExtentType::Data), "00A");
    }

    #[test]
    fn extent_name_basic_three() {
        assert_eq!(extent_file_name(ExtentId(59), ExtentType::Data), "03B");
    }

    #[test]
    fn extent_name_max() {
        assert_eq!(
            extent_file_name(ExtentId(u32::MAX), ExtentType::Data),
            "FFF"
        );
    }

    #[test]
    fn extent_name_min() {
        assert_eq!(
            extent_file_name(ExtentId(u32::MIN), ExtentType::Data),
            "000"
        );
    }

    #[test]
    fn extent_dir_basic() {
        assert_eq!(
            extent_dir("/var/region", ExtentId(4)),
            PathBuf::from("/var/region/00/000/")
        );
    }

    #[test]
    fn extent_dir_max() {
        assert_eq!(
            extent_dir("/var/region", ExtentId(u32::MAX)),
            PathBuf::from("/var/region/FF/FFF")
        );
    }

    #[test]
    fn extent_dir_min() {
        assert_eq!(
            extent_dir("/var/region", ExtentId(u32::MIN)),
            PathBuf::from("/var/region/00/000/")
        );
    }

    #[test]
    fn extent_path_min() {
        assert_eq!(
            extent_path("/var/region", ExtentId(u32::MIN)),
            PathBuf::from("/var/region/00/000/000")
        );
    }

    #[test]
    fn extent_path_three() {
        assert_eq!(
            extent_path("/var/region", ExtentId(3)),
            PathBuf::from("/var/region/00/000/003")
        );
    }

    #[test]
    fn extent_path_mid_hi() {
        assert_eq!(
            extent_path("/var/region", ExtentId(65536)),
            PathBuf::from("/var/region/00/010/000")
        );
    }

    #[test]
    fn extent_path_mid_lo() {
        assert_eq!(
            extent_path("/var/region", ExtentId::from(65535)),
            PathBuf::from("/var/region/00/00F/FFF")
        );
    }

    #[test]
    fn extent_path_max() {
        assert_eq!(
            extent_path("/var/region", ExtentId::from(u32::MAX)),
            PathBuf::from("/var/region/FF/FFF/FFF")
        );
    }
}
