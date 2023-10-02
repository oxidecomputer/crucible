// Copyright 2023 Oxide Computer Company
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::fs::{rename, File, OpenOptions};
use tokio::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use nix::unistd::{sysconf, SysconfVar};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crucible_common::*;
use repair_client::types::FileType;

use super::*;

#[derive(Debug)]
pub struct Extent {
    pub number: u32,
    read_only: bool,
    iov_max: usize,

    /// Inner contains information about the actual extent file that holds the
    /// data, the metadata (stored in the database) about that extent, and the
    /// set of dirty blocks that have been written to since last flush.
    inner: Mutex<Box<dyn ExtentInner>>,
}

pub(crate) trait ExtentInner: Send + Debug {
    /// Create an extent at the location requested.
    /// Start off with the default meta data.
    /// Note that this function is not safe to run concurrently.
    fn create(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Opens an existing extent at the given path
    fn open(
        dir: &Path,
        def: &RegionDefinition,
        extent_number: u32,
        read_only: bool,
        log: &Logger,
    ) -> Result<Self>
    where
        Self: Sized;

    fn gen_number(&self) -> Result<u64>;
    fn flush_number(&self) -> Result<u64>;
    fn dirty(&self) -> Result<bool>;

    fn set_dirty(&self) -> Result<()>;
    fn get_block_contexts(
        &self,
        block: u64,
        count: u64,
    ) -> Result<Vec<Vec<DownstairsBlockContext>>>;

    fn set_block_context(
        &self,
        block_context: &DownstairsBlockContext,
    ) -> Result<()>;

    fn flush(
        &mut self,
        new_flush: u64,
        new_gen: u64,
        job_id: JobOrReconciliationId,
    ) -> Result<(), CrucibleError>;

    fn read(
        &self,
        job_id: JobId,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
        iov_max: usize,
    ) -> Result<(), CrucibleError>;

    fn write(
        &mut self,
        job_id: JobId,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
        iov_max: usize,
    ) -> Result<()>;

    fn truncate_encryption_contexts_and_hashes(
        &self,
        extent_block_indexes_and_hashes: &[(usize, u64)],
    ) -> Result<()>;

    fn fully_rehash_and_clean_all_stale_contexts(
        &mut self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError>;
}

/// BlockContext, with the addition of block index and on_disk_hash
#[derive(Clone)]
pub struct DownstairsBlockContext {
    pub block_context: BlockContext,

    pub block: u64,
    pub on_disk_hash: u64,
}

/// Wrapper type for either a job or reconciliation ID
///
/// This is useful for debug logging / DTrace probes, and not much else
#[derive(Copy, Clone, Debug)]
pub(crate) enum JobOrReconciliationId {
    JobId(JobId),
    ReconciliationId(ReconciliationId),
}

impl JobOrReconciliationId {
    pub fn get(self) -> u64 {
        match self {
            Self::JobId(i) => i.0,
            Self::ReconciliationId(i) => i.0,
        }
    }
}

impl From<JobId> for JobOrReconciliationId {
    fn from(i: JobId) -> Self {
        Self::JobId(i)
    }
}

impl From<ReconciliationId> for JobOrReconciliationId {
    fn from(i: ReconciliationId) -> Self {
        Self::ReconciliationId(i)
    }
}

/// An extent can be Opened or Closed. If Closed, it is probably being updated
/// out of band. If Opened, then this extent is accepting operations.
#[derive(Debug)]
pub enum ExtentState {
    Opened(Arc<Extent>),
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

#[derive(Debug, Clone)]
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
    pub fn to_file_type(&self) -> FileType {
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
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out
}

/**
 * Produce a PathBuf that refers to the copy directory we create for
 * a given extent "number",  This directory will hold the files we
 * transfer from the source downstairs.
 * anchored under "dir".
 */
pub fn copy_dir<P: AsRef<Path>>(dir: P, number: u32) -> PathBuf {
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("copy");
    out
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
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("replace");
    out
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
    let mut out = extent_dir(dir, number);
    out.push(extent_file_name(number, ExtentType::Data));
    out.set_extension("completed");
    out
}

/**
 * Remove directories associated with repair except for the replace
 * directory. Replace is handled specifically during extent open.
 */
pub fn remove_copy_cleanup_dir<P: AsRef<Path>>(dir: P, eid: u32) -> Result<()> {
    let mut remove_dirs = vec![copy_dir(&dir, eid)];
    remove_dirs.push(completed_dir(&dir, eid));

    for d in remove_dirs {
        if Path::new(&d).exists() {
            std::fs::remove_dir_all(&d)?;
        }
    }
    Ok(())
}

/**
 * Validate a list of sorted repair files.
 * There are either two or four files we expect to find, any more or less
 * and we have a bad list.  No duplicates.
 */
pub fn validate_repair_files(eid: usize, files: &[String]) -> bool {
    let eid = eid as u32;

    let some = vec![
        extent_file_name(eid, ExtentType::Data),
        extent_file_name(eid, ExtentType::Db),
    ];

    let mut all = some.clone();
    all.extend(vec![
        extent_file_name(eid, ExtentType::DbShm),
        extent_file_name(eid, ExtentType::DbWal),
    ]);

    // Either we have some or all.
    files == some || files == all
}

impl Extent {
    pub(crate) fn get_iov_max() -> Result<usize> {
        let i: i64 = sysconf(SysconfVar::IOV_MAX)?
            .ok_or_else(|| anyhow!("IOV_MAX returned None!"))?;
        Ok(i.try_into()?)
    }

    /**
     * Open an existing extent file at the location requested.
     * Read in the metadata from the first block of the file.
     */
    pub fn open(
        dir: &Path,
        def: &RegionDefinition,
        number: u32,
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
            move_replacement_extent(dir, number as usize, log)?;
        }

        // Pick the format for the downstairs files
        //
        // Right now, this only supports SQLite-flavored Downstairs
        let inner = {
            let mut sqlite_path = path.clone();
            sqlite_path.set_extension("db");
            if sqlite_path.exists() {
                let mut inner = extent_inner_sqlite::SqliteInner::open(
                    &path, def, number, read_only, log,
                )?;
                // Clean out any irrelevant block contexts, which may be present
                // if downstairs crashed between a write() and a flush().
                inner.fully_rehash_and_clean_all_stale_contexts(false)?;
                Box::new(inner)
            } else {
                panic!("no SQLite file present at {sqlite_path:?}");
            }
        };

        // XXX: schema updates?

        let extent = Extent {
            number,
            read_only,
            iov_max: Extent::get_iov_max()?,
            inner: Mutex::new(inner),
        };

        Ok(extent)
    }

    pub async fn dirty(&self) -> bool {
        self.inner.lock().await.dirty().unwrap()
    }

    /**
     * Close an extent and the metadata db files for it.
     */
    pub async fn close(self) -> Result<(u64, u64, bool), CrucibleError> {
        let inner = self.inner.lock().await;

        let gen = inner.gen_number().unwrap();
        let flush = inner.flush_number().unwrap();
        let dirty = inner.dirty().unwrap();

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

        let inner = extent_inner_sqlite::SqliteInner::create(dir, def, number)?;

        /*
         * Complete the construction of our new extent
         */
        Ok(Extent {
            number,
            read_only: false,
            iov_max: Extent::get_iov_max()?,
            inner: Mutex::new(Box::new(inner)),
        })
    }

    /**
     * Create the copy directory for this extent.
     */
    pub fn create_copy_dir<P: AsRef<Path>>(
        dir: P,
        eid: usize,
    ) -> Result<PathBuf, CrucibleError> {
        let cp = copy_dir(dir, eid as u32);

        /*
         * Verify the copy directory does not exist
         */
        if Path::new(&cp).exists() {
            crucible_bail!(IoError, "Copy directory:{:?} already exists", cp);
        }

        std::fs::create_dir_all(&cp)?;
        Ok(cp)
    }

    /**
     * Create the file that will hold a copy of an extent from a
     * remote downstairs.
     */
    pub fn create_copy_file(
        mut copy_dir: PathBuf,
        eid: usize,
        extension: Option<ExtentType>,
    ) -> Result<File> {
        // Get the base extent name before we consider the actual Type
        let name = extent_file_name(eid as u32, ExtentType::Data);
        copy_dir.push(name);
        if let Some(extension) = extension {
            let ext = format!("{}", extension);
            copy_dir.set_extension(ext);
        }
        let copy_path = copy_dir;

        if Path::new(&copy_path).exists() {
            bail!("Copy file:{:?} already exists", copy_path);
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&copy_path)?;
        Ok(file)
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    /// Read the real data off underlying storage, and get block metadata. If
    /// an error occurs while processing any of the requests, the state of
    /// `responses` is undefined.
    #[instrument]
    pub async fn read(
        &self,
        job_id: JobId,
        requests: &[&crucible_protocol::ReadRequest],
        responses: &mut Vec<crucible_protocol::ReadResponse>,
    ) -> Result<(), CrucibleError> {
        cdt::extent__read__start!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        let inner = self.inner.lock().await;

        inner.read(job_id, requests, responses, self.iov_max)?;

        cdt::extent__read__done!(|| {
            (job_id.0, self.number, requests.len() as u64)
        });

        Ok(())
    }

    #[instrument]
    pub async fn write(
        &self,
        job_id: JobId,
        writes: &[&crucible_protocol::Write],
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::extent__write__start!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        let mut inner_guard = self.inner.lock().await;
        // I realize this looks like some nonsense but what this is doing is
        // borrowing the inner up-front from the MutexGuard, which will allow
        // us to later disjointly borrow fields. Basically, we're helping the
        // borrow-checker do its job.
        let inner = &mut *inner_guard;
        inner.write(job_id, writes, only_write_unwritten, self.iov_max)?;

        cdt::extent__write__file__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        cdt::extent__write__done!(|| {
            (job_id.0, self.number, writes.len() as u64)
        });

        Ok(())
    }

    #[instrument]
    pub(crate) async fn flush<I: Into<JobOrReconciliationId> + Debug>(
        &self,
        new_flush: u64,
        new_gen: u64,
        id: I, // only used for logging
        log: &Logger,
    ) -> Result<(), CrucibleError> {
        let job_id: JobOrReconciliationId = id.into();
        let mut inner = self.inner.lock().await;

        if !inner.dirty()? {
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

        inner.flush(new_flush, new_gen, job_id)
    }

    /// Rehash the entire file. Remove any stored hash/encryption contexts
    /// that do not correlate to data currently stored on disk. This is
    /// primarily when opening an extent after recovering from a crash, since
    /// irrelevant hashes will normally be cleared out during a flush().
    ///
    /// By default this function will only do work if the extent is marked
    /// dirty. Set `force_override_dirty` to `true` to override this behavior.
    /// This override should generally not be necessary, as the dirty flag
    /// is set before any contexts are written.
    #[instrument]
    pub async fn fully_rehash_and_clean_all_stale_contexts(
        &self,
        force_override_dirty: bool,
    ) -> Result<(), CrucibleError> {
        let mut inner = self.inner.lock().await;

        if !force_override_dirty && !inner.dirty()? {
            return Ok(());
        }
        inner.fully_rehash_and_clean_all_stale_contexts(force_override_dirty)
    }

    pub async fn get_meta_info(&self) -> ExtentMeta {
        let inner = self.inner.lock().await;
        ExtentMeta {
            ext_version: 0,
            gen_number: inner.gen_number().unwrap(),
            flush_number: inner.flush_number().unwrap(),
            dirty: inner.dirty().unwrap(),
        }
    }

    pub(crate) async fn lock(
        &self,
    ) -> tokio::sync::MutexGuard<Box<dyn ExtentInner>> {
        self.inner.lock().await
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

    new_file.set_extension("db");
    original_file.set_extension("db");
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
    } else if original_file.exists() {
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
    } else if original_file.exists() {
        info!(
            log,
            "Remove old file {:?} as there is no replacement",
            original_file.clone()
        );
        std::fs::remove_file(&original_file)?;
    }
    sync_path(&destination_dir, log)?;

    // After we have all files: move the copy dir.
    info!(
        log,
        "Move directory  {:?} to {:?}", replace_dir, completed_dir
    );
    rename(replace_dir, &completed_dir)?;

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
