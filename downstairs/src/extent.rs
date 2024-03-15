// Copyright 2023 Oxide Computer Company
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};

use anyhow::{anyhow, bail, Context, Result};
use nix::unistd::{sysconf, SysconfVar};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::region::JobOrReconciliationId;
use crucible_common::*;
use crucible_protocol::RawReadResponse;
use repair_client::types::FileType;

use super::*;

#[derive(Debug)]
pub struct Extent {
    pub number: u32,
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

    fn read_into(
        &mut self,
        job_id: JobId,
        requests: &[crucible_protocol::ReadRequest],
        out: &mut RawReadResponse,
    ) -> Result<(), CrucibleError>;

    /// Performs a read then destructures into a `Vec<ReadResponse>`
    #[cfg(test)]
    fn read(
        &mut self,
        job_id: JobId,
        requests: &[crucible_protocol::ReadRequest],
    ) -> Result<RawReadResponse, CrucibleError> {
        let block_size = requests[0].offset.block_size_in_bytes();
        let mut out =
            RawReadResponse::with_capacity(requests.len(), block_size as u64);
        if !requests.is_empty() {
            self.read_into(job_id, requests, &mut out)?;
        }
        Ok(out)
    }

    fn write(
        &mut self,
        job_id: JobId,
        writes: &[crucible_protocol::Write],
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<(), CrucibleError>;

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

/// Extent version for SQLite-backed metadata
///
/// See [`extent_inner_sqlite::SqliteInner`] for the implementation.
///
/// This is no longer used when creating new extents, but we support opening
/// existing SQLite-based extents because snapshot images are on read-only
/// volumes, so we can't migrate them.
#[cfg(any(test, feature = "integration-tests"))]
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
    Db,
    DbShm,
    DbWal,
}

impl fmt::Display for ExtentType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtentType::Data => Ok(()),
            ExtentType::Db => write!(f, "db"),
            ExtentType::DbShm => write!(f, "db-shm"),
            ExtentType::DbWal => write!(f, "db-wal"),
        }
    }
}

/**
 * Take an ExtentType and translate it into the corresponding
 * FileType from the repair client.
 */
impl ExtentType {
    pub fn to_file_type(self) -> FileType {
        match self {
            ExtentType::Data => FileType::Data,
            ExtentType::Db => FileType::Db,
            ExtentType::DbShm => FileType::DbShm,
            ExtentType::DbWal => FileType::DbWal,
        }
    }
}

/**
 * Produce the string name of the data file for a given extent number
 */
pub fn extent_file_name(number: u32, extent_type: ExtentType) -> String {
    match extent_type {
        ExtentType::Data => {
            format!("{:03X}", number & 0xFFF)
        }
        ExtentType::Db | ExtentType::DbShm | ExtentType::DbWal => {
            format!("{:03X}.{}", number & 0xFFF, extent_type)
        }
    }
}

/**
 * Produce a PathBuf that refers to the containing directory for extent
 * "number", anchored under "dir".
 */
pub fn extent_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push(format!("{:02X}", (number >> 24) & 0xFF));
    out.push(format!("{:03X}", (number >> 12) & 0xFFF));
    out
}

/**
 * Produce a PathBuf that refers to the backing file for extent "number",
 * anchored under "dir".
 */
pub fn extent_path<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    extent_dir(dir, number).join(extent_file_name(number, ExtentType::Data))
}

/**
 * Produce a PathBuf that refers to the copy directory we create for
 * a given extent "number",  This directory will hold the files we
 * transfer from the source downstairs.
 * anchored under "dir".
 */
pub fn copy_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
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
pub fn replace_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
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
pub fn completed_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    extent_dir(dir, number)
        .join(extent_file_name(number, ExtentType::Data))
        .with_extension("completed")
}

/**
 * Remove directories associated with repair except for the replace
 * directory. Replace is handled specifically during extent open.
 */
pub fn remove_copy_cleanup_dir<P: AsRef<Path>>(dir: P, eid: u32) -> Result<()> {
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
        number: u32,
        read_only: bool,
        backend: Backend,
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
        //
        // Note that `move_replacement_extent` must support migrating old
        // (SQLite-based) extents and new (raw file) extents, because it's
        // possible that an old extent replacement crashed right before we
        // updated Crucible.
        let replace_dir = replace_dir(dir, number);
        if !read_only && Path::new(&replace_dir).exists() {
            info!(
                log,
                "Extent {} found replacement dir, finishing replacement",
                number
            );
            move_replacement_extent(dir, number as usize, log, false)?;
        }

        // We will migrate every read-write extent with a SQLite file present.
        //
        // We use the presence of the .db file as a marker to whether the extent
        // data file has been migrated.
        //
        // Remember, the goal is to turn 2-4 files (extent data, .db, .db-wal,
        // db-shm) into a single file containing
        //
        //  - The extent data (same as before, at the beginning of the file)
        //  - Metadata and encryption context slots (after extent data in the
        //  - same file)
        //
        // Here's the procedure:
        //
        //  - If the .db file is present
        //      - Truncate it to the length of the extent data. This is a no-op
        //        normally, but lets us recover from a partial migration (e.g.
        //        if we wrote the raw context but crashed before deleting the
        //        .db file)
        //      - Open the extent using our existing SQLite extent code
        //      - Using standard extent APIs, find the metadata and encryption
        //        context for each block. Append this to the existing data file.
        //      - Close the (SQLite) extent
        //      - Open the extent data file in append mode, and append the new
        //        raw metadata + block contexts to the end of the file.
        //      - Close the extent data file and fsync it to disk
        //      - Delete the .db file
        //      - fsync the containing directory (to make the deletion durable;
        //        not sure if this is necessary)
        //  - At this point, the .db file is not present and the extent data
        //    file represents a raw extent that has been persisted to disk
        //      - If the .db-wal file is present, remove it
        //      - If the .db-shm file is present, remove it
        //
        //  It's safe to fail at any point in this procedure; the next time
        //  `Extent::open` is called, we will restart the migration (if we
        //  haven't gotten to deleting the `.db` file).
        let mut has_sqlite = path.with_extension("db").exists();
        let force_sqlite_backend = match backend {
            Backend::RawFile => false,
            #[cfg(any(test, feature = "integration-tests"))]
            Backend::SQLite => true,
        };
        let should_migrate = has_sqlite && !read_only && !force_sqlite_backend;
        if should_migrate {
            info!(log, "Migrating extent {number}");
            // Truncate the file to the length of the extent data
            {
                let f =
                    OpenOptions::new().read(true).write(true).open(&path)?;
                f.set_len(def.extent_size().value * def.block_size())?;
            }

            // Compute supplemental data from the SQLite extent
            let mut inner = extent_inner_sqlite::SqliteInner::open(
                dir, def, number, read_only, log,
            )?;
            let ctxs = inner.export_contexts()?;
            let dirty = inner.dirty()?;
            let flush_number = inner.flush_number()?;
            let gen_number = inner.gen_number()?;
            drop(inner);

            // Reopen the file and import those changes
            {
                let mut f =
                    OpenOptions::new().read(true).write(true).open(&path)?;
                extent_inner_raw::RawInner::import(
                    &mut f,
                    def,
                    ctxs,
                    dirty,
                    flush_number,
                    gen_number,
                )?;
                f.sync_all()
                    .with_context(|| format!("{path:?}: fsync failure"))?;
            }

            // Remove the .db file, because our extent is now a raw extent and
            // has been persisted to disk.
            std::fs::remove_file(path.with_extension("db"))?;

            // Make that removal persistent by synching the parent directory
            sync_path(extent_dir(dir, number), log)?;

            // We have now migrated from SQLite to raw file extents!
            has_sqlite = false;
            info!(log, "Done migrating extent {number}");
        }

        // Clean up spare SQLite files if this is a raw file extent.  These
        // deletions don't need to be durable, because we're not using their
        // presence / absence for anything meaningful.
        if !has_sqlite && !read_only {
            for ext in ["db-shm", "db-wal"] {
                let f = path.with_extension(ext);
                if f.exists() {
                    std::fs::remove_file(f)?;
                }
            }
        }

        // Pick the format for the downstairs files.  In most cases, we will be
        // using the raw extent format, but for older read-only snapshots that
        // were constructed using the SQLite backend, we have to keep them
        // as-is.
        let inner: Box<dyn ExtentInner + Send + Sync> = {
            if has_sqlite {
                assert!(read_only || force_sqlite_backend);
                let inner = extent_inner_sqlite::SqliteInner::open(
                    dir, def, number, read_only, log,
                )?;
                Box::new(inner)
            } else {
                let inner = extent_inner_raw::RawInner::open(
                    dir, def, number, read_only, log,
                )?;
                Box::new(inner)
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
        number: u32,
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
            #[cfg(any(test, feature = "integration-tests"))]
            Backend::SQLite => Box::new(
                extent_inner_sqlite::SqliteInner::create(dir, def, number)?,
            ),
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

    pub fn number(&self) -> u32 {
        self.number
    }

    /// Read the real data off underlying storage, and get block metadata. If
    /// an error occurs while processing any of the requests, the state of
    /// `responses` is undefined.
    #[instrument]
    pub async fn read_into(
        &mut self,
        job_id: JobId,
        requests: &[crucible_protocol::ReadRequest],
        out: &mut RawReadResponse,
    ) -> Result<(), CrucibleError> {
        cdt::extent__read__start!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        self.inner.read_into(job_id, requests, out)?;
        cdt::extent__read__done!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        Ok(())
    }

    #[instrument]
    pub async fn write(
        &mut self,
        job_id: JobId,
        writes: &[crucible_protocol::Write],
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::extent__write__start!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        self.inner
            .write(job_id, writes, only_write_unwritten, self.iov_max)?;

        cdt::extent__write__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

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

    #[cfg(test)]
    #[allow(clippy::unused_async)] // this will be async again in the future
    pub async fn set_dirty_and_block_context(
        &mut self,
        block_context: &DownstairsBlockContext,
    ) -> Result<(), CrucibleError> {
        self.inner.set_dirty_and_block_context(block_context)
    }

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
    eid: usize,
    log: &Logger,
    clone: bool,
) -> Result<(), CrucibleError> {
    let destination_dir = extent_dir(&region_dir, eid as u32);
    let extent_file_name = extent_file_name(eid as u32, ExtentType::Data);
    let replace_dir = replace_dir(&region_dir, eid as u32);
    let completed_dir = completed_dir(&region_dir, eid as u32);

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

    // We distinguish between SQLite-backend and raw-file extents based on the
    // presence of the `.db` file.  We should never do extent repair across
    // different extent formats; it must be SQLite-to-SQLite or raw-to-raw.
    //
    // It is uncommon to perform extent repair on SQLite-backed extents at all,
    // because they are automatically migrated into raw file extents or
    // read-only.  However, it must be supported for two cases:
    //
    // - If there was an unfinished replacement, we must finish that replacement
    //   before migrating from SQLite -> raw file backend, which happens
    //   automatically later in startup.
    // - We use this same code path to perform clones of read-only regions,
    //   which may be SQLite-backed (and will never migrate to raw files,
    //   because they are read only).  This is only the case when the `clone`
    //   argument is `true`.
    //
    // In the first case, we know that we are repairing an SQLite-based extent
    // because the target (original) extent includes a `.db` file.
    //
    // In the second case, the target (original) extent is not present, so we
    // check whether the new files include a `.db` file.

    new_file.set_extension("db");
    original_file.set_extension("db");

    if original_file.exists() || (new_file.exists() && clone) {
        if let Err(e) = std::fs::copy(new_file.clone(), original_file.clone()) {
            crucible_bail!(
                IoError,
                "copy {:?} to {:?} got: {:?}",
                new_file,
                original_file,
                e
            );
        }
        sync_path(&original_file, log)?;

        // The .db-shm and .db-wal files may or may not exist.  If they don't
        // exist on the source side, then be sure to remove them locally to
        // avoid database corruption from a mismatch between old and new.
        new_file.set_extension("db-shm");
        original_file.set_extension("db-shm");
        if new_file.exists() {
            if let Err(e) =
                std::fs::copy(new_file.clone(), original_file.clone())
            {
                crucible_bail!(
                    IoError,
                    "copy {:?} to {:?} got: {:?}",
                    new_file,
                    original_file,
                    e
                );
            }
            sync_path(&original_file, log)?;
        } else if original_file.exists() {
            // If we are cloning, then our new region will have been
            // created with Backend::RawFile, and we should have no SQLite
            // files.
            assert!(!clone);
            info!(
                log,
                "Remove old file {:?} as there is no replacement",
                original_file.clone()
            );
            std::fs::remove_file(&original_file)?;
        }

        new_file.set_extension("db-wal");
        original_file.set_extension("db-wal");
        if new_file.exists() {
            if let Err(e) =
                std::fs::copy(new_file.clone(), original_file.clone())
            {
                crucible_bail!(
                    IoError,
                    "copy {:?} to {:?} got: {:?}",
                    new_file,
                    original_file,
                    e
                );
            }
            sync_path(&original_file, log)?;
        } else if original_file.exists() {
            // If we are cloning, then our new region will have been
            // created with Backend::RawFile, and we should have no SQLite
            // files.
            assert!(!clone);
            info!(
                log,
                "Remove old file {:?} as there is no replacement",
                original_file.clone()
            );
            std::fs::remove_file(&original_file)?;
        }
    }
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
    data: &[u8],
) -> Result<(), CrucibleError> {
    let block_size = extent_size.block_size_in_bytes() as u64;
    /*
     * Only accept block sized operations
     */
    if data.len() % block_size as usize != 0 {
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

    if (byte_offset + data.len() as u64) > total_size {
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

        check_input(extent_size, Block::new_512(0), &data).unwrap();
        check_input(extent_size, Block::new_512(99), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_large() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(513);
        data.put(&[1; 513][..]);

        check_input(extent_size, Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_non_aligned_small() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(511);
        data.put(&[1; 511][..]);

        check_input(extent_size, Block::new_512(0), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_bad_block() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(512);
        data.put(&[1; 512][..]);

        check_input(extent_size, Block::new_512(100), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_block_buf() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(1024);
        data.put(&[1; 1024][..]);

        check_input(extent_size, Block::new_512(99), &data).unwrap();
    }

    #[test]
    #[should_panic]
    fn extent_io_invalid_large() {
        let extent_size = Block::new_512(100);
        let mut data = BytesMut::with_capacity(512 * 100);
        data.put(&[1; 512 * 100][..]);

        check_input(extent_size, Block::new_512(1), &data).unwrap();
    }

    #[test]
    fn extent_name_basic() {
        assert_eq!(extent_file_name(4, ExtentType::Data), "004");
    }

    #[test]
    fn extent_name_basic_ext() {
        assert_eq!(extent_file_name(4, ExtentType::Db), "004.db");
    }

    #[test]
    fn extent_name_basic_ext_shm() {
        assert_eq!(extent_file_name(4, ExtentType::DbShm), "004.db-shm");
    }

    #[test]
    fn extent_name_basic_ext_wal() {
        assert_eq!(extent_file_name(4, ExtentType::DbWal), "004.db-wal");
    }

    #[test]
    fn extent_name_basic_two() {
        assert_eq!(extent_file_name(10, ExtentType::Data), "00A");
    }

    #[test]
    fn extent_name_basic_three() {
        assert_eq!(extent_file_name(59, ExtentType::Data), "03B");
    }

    #[test]
    fn extent_name_max() {
        assert_eq!(extent_file_name(u32::MAX, ExtentType::Data), "FFF");
    }

    #[test]
    fn extent_name_min() {
        assert_eq!(extent_file_name(u32::MIN, ExtentType::Data), "000");
    }

    #[test]
    fn extent_dir_basic() {
        assert_eq!(
            extent_dir("/var/region", 4),
            PathBuf::from("/var/region/00/000/")
        );
    }

    #[test]
    fn extent_dir_max() {
        assert_eq!(
            extent_dir("/var/region", u32::MAX),
            PathBuf::from("/var/region/FF/FFF")
        );
    }

    #[test]
    fn extent_dir_min() {
        assert_eq!(
            extent_dir("/var/region", u32::MIN),
            PathBuf::from("/var/region/00/000/")
        );
    }

    #[test]
    fn extent_path_min() {
        assert_eq!(
            extent_path("/var/region", u32::MIN),
            PathBuf::from("/var/region/00/000/000")
        );
    }

    #[test]
    fn extent_path_three() {
        assert_eq!(
            extent_path("/var/region", 3),
            PathBuf::from("/var/region/00/000/003")
        );
    }

    #[test]
    fn extent_path_mid_hi() {
        assert_eq!(
            extent_path("/var/region", 65536),
            PathBuf::from("/var/region/00/010/000")
        );
    }

    #[test]
    fn extent_path_mid_lo() {
        assert_eq!(
            extent_path("/var/region", 65535),
            PathBuf::from("/var/region/00/00F/FFF")
        );
    }

    #[test]
    fn extent_path_max() {
        assert_eq!(
            extent_path("/var/region", u32::MAX),
            PathBuf::from("/var/region/FF/FFF/FFF")
        );
    }
}
