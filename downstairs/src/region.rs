// Copyright 2023 Oxide Computer Company
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::{rename, File, OpenOptions};
use std::io::{IoSlice, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use anyhow::{bail, Result};
use futures::TryStreamExt;

use tracing::instrument;

use crucible_common::*;
use crucible_protocol::SnapshotDetails;
use repair_client::Client;

use super::*;
use crate::extent::{
    copy_dir, extent_dir, extent_file_name, move_replacement_extent,
    replace_dir, sync_path, Extent, ExtentMeta, ExtentState, ExtentType,
};

/**
 * Validate the repair file.
 *
 * There is only one repair file: the raw file itself (which also contains
 * structured context and metadata at the end).
 */
pub fn validate_repair_files(eid: usize, files: &[String]) -> bool {
    files == [extent_file_name(eid as u32, ExtentType::Data)]
}

/// Validate the possible files during a clone.
///
/// During a clone of a downstairs region, we have one, two or four
/// possible files we expect to see.
pub fn validate_clone_files(eid: usize, files: &[String]) -> bool {
    let one = vec![extent_file_name(eid as u32, ExtentType::Data)];

    let mut some = one.clone();
    some.extend(vec![extent_file_name(eid as u32, ExtentType::Db)]);

    let mut all = some.clone();
    all.extend(vec![
        extent_file_name(eid as u32, ExtentType::DbShm),
        extent_file_name(eid as u32, ExtentType::DbWal),
    ]);

    // For replacement, we require one, some, or all
    files == one || files == some || files == all
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

/**
 * The main structure describing a region.
 */
#[derive(Debug)]
pub struct Region {
    pub dir: PathBuf,
    def: RegionDefinition,
    pub extents: Vec<Arc<Mutex<ExtentState>>>,

    /// Extents which are dirty and need to be flushed. Should be true if the
    /// dirty flag in the extent's metadata is set. When an extent is opened, if
    /// it's dirty, it's added to here. When a write is issued to an extent,
    /// it's added to here. If the write doesn't actually make the extent dirty
    /// that's fine, because the extent will NOP during the flush anyway, but
    /// this mainly serves to cut down on the extents we're considering for a
    /// flush in the first place.
    dirty_extents: HashSet<usize>,

    read_only: bool,
    log: Logger,

    /// Select the backend to use when creating and opening extents
    ///
    /// The SQLite backend is only allowed in tests; all new extents must be raw
    /// files
    backend: Backend,
}

impl Region {
    /// Set the number of open files resource limit to the max. Use the provided
    /// RegionDefinition to check that this Downstairs can open all the files it
    /// needs.
    pub fn set_max_open_files_rlimit(
        log: &Logger,
        def: &RegionDefinition,
    ) -> Result<()> {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 {
            bail!(
                "libc::getrlimit failed with {}",
                std::io::Error::last_os_error()
            );
        }

        let number_of_files_limit = match rlim.rlim_cur.cmp(&rlim.rlim_max) {
            std::cmp::Ordering::Less => {
                debug!(
                    log,
                    "raising number of open files limit to from {} to {}",
                    rlim.rlim_cur,
                    rlim.rlim_max,
                );

                rlim.rlim_cur = rlim.rlim_max;

                if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) } != 0 {
                    bail!(
                        "libc::setrlimit failed with {}",
                        std::io::Error::last_os_error()
                    );
                }

                rlim.rlim_max
            }

            Ordering::Equal => {
                debug!(
                    log,
                    "current number of open files limit {} is already the maximum",
                    rlim.rlim_cur,
                );

                rlim.rlim_cur
            }

            Ordering::Greater => {
                // wat
                warn!(
                    log,
                    "current number of open files limit {} is already ABOVE THE MAXIMUM {}?",
                    rlim.rlim_cur,
                    rlim.rlim_max,
                );

                rlim.rlim_cur
            }
        };

        // The downstairs needs to open (at minimum) the extent file, the sqlite
        // database, the write-ahead lock, and the sqlite shared memory file for
        // each extent in the region, plus:
        //
        // - the seed database (db + shm + wal)
        // - region.json
        // - stdin, stdout, and stderr
        // - the listen and repair sockets (arbitrarily saying two sockets per
        //   server)
        // - optionally, the stat connection to oximeter
        // - optionally, a control interface
        //
        // If the downstairs cannot open this many files, error here.
        let required_number_of_files = def.extent_count() as u64 * 4 + 13;

        if number_of_files_limit < required_number_of_files {
            bail!("this downstairs cannot open all required files!");
        }

        Ok(())
    }

    /**
     * Create a new region based on the given RegionOptions
     */
    pub async fn create<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        log: Logger,
    ) -> Result<Region> {
        Self::create_with_backend(dir, options, Backend::RawFile, log).await
    }

    pub async fn create_with_backend<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        backend: Backend,
        log: Logger,
    ) -> Result<Region> {
        options.validate()?;

        let def = RegionDefinition::from_options(&options).unwrap();

        Self::set_max_open_files_rlimit(&log, &def)?;

        let cp = config_path(dir.as_ref());
        /*
         * If the file exists, then exit now with error.  If the caller
         * wants a new region, they have to delete the old one first.
         */
        if Path::new(&cp).exists() {
            bail!("Config file already exists {:?}", cp);
        }
        mkdir_for_file(&cp)?;

        write_json(&cp, &def, false)?;
        info!(log, "Created new region file {:?}", cp);

        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            dirty_extents: HashSet::new(),
            read_only: false,
            log,
            backend,
        };
        region.open_extents(true).await?;
        Ok(region)
    }

    /**
     * Open an existing region file
     */
    pub async fn open<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        verbose: bool,
        read_only: bool,
        log: &Logger,
    ) -> Result<Region> {
        Self::open_with_backend(
            dir,
            options,
            verbose,
            read_only,
            Backend::RawFile,
            log,
        )
        .await
    }

    pub async fn open_with_backend<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
        verbose: bool,
        read_only: bool,
        backend: Backend,
        log: &Logger,
    ) -> Result<Region> {
        options.validate()?;

        let cp = config_path(&dir);

        /*
         * We are expecting to find a region config file and extent files.
         * If we do not, then report error and exit.
         */
        let def: crucible_common::RegionDefinition = match read_json(&cp) {
            Ok(def) => def,
            Err(e) => bail!("Error {:?} opening region config {:?}", e, cp),
        };

        Self::set_max_open_files_rlimit(log, &def)?;

        if verbose {
            info!(log, "Opened existing region file {:?}", cp);
        }

        if def.database_read_version() != crucible_common::DATABASE_READ_VERSION
        {
            bail!(
                "Database read version mismatch, expected {}, got {}",
                crucible_common::DATABASE_READ_VERSION,
                def.database_read_version(),
            );
        }
        if verbose {
            info!(log, "Database read version {}", def.database_read_version());
        }

        if def.database_write_version()
            != crucible_common::DATABASE_WRITE_VERSION
        {
            bail!(
                "Database write version mismatch, expected {}, got {}",
                crucible_common::DATABASE_WRITE_VERSION,
                def.database_write_version(),
            );
        }
        if verbose {
            info!(
                log,
                "Database write version {}",
                def.database_write_version()
            );
        }

        /*
         * Open every extent that presently exists.
         */
        let mut region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            dirty_extents: HashSet::new(),
            read_only,
            log: log.clone(),
            backend,
        };

        region.open_extents(false).await?;

        Ok(region)
    }

    pub fn encrypted(&self) -> bool {
        self.def.get_encrypted()
    }

    async fn get_opened_extent(&self, eid: usize) -> Arc<Extent> {
        match &*self.extents[eid].lock().await {
            ExtentState::Opened(extent) => extent.clone(),
            ExtentState::Closed => {
                panic!("attempting to get closed extent {}", eid)
            }
        }
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
    async fn open_extents(&mut self, create: bool) -> Result<()> {
        let next_eid = self.extents.len() as u32;

        let eid_range = next_eid..self.def.extent_count();
        let mut these_extents = Vec::with_capacity(eid_range.len());

        for eid in eid_range {
            let extent = if create {
                Extent::create(&self.dir, &self.def, eid, self.backend)?
            } else {
                let extent = Extent::open(
                    &self.dir,
                    &self.def,
                    eid,
                    self.read_only,
                    self.backend,
                    &self.log,
                )?;

                if extent.dirty().await {
                    self.dirty_extents.insert(eid as usize);
                }

                extent
            };

            these_extents.push(Arc::new(Mutex::new(ExtentState::Opened(
                Arc::new(extent),
            ))));
        }

        self.extents.extend(these_extents);

        for eid in next_eid..self.def.extent_count() {
            assert_eq!(self.get_opened_extent(eid as usize).await.number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());

        Ok(())
    }

    /// Walk the list of extents and close each one.
    ///
    /// If we fail to close an extent, we exit right away, leaving the
    /// remaining extents alone.
    pub async fn close_all_extents(&mut self) -> Result<()> {
        for eid in 0..self.def.extent_count() as usize {
            if let Err(e) = self.close_extent(eid).await {
                bail!("Failed closing extent {eid} with {e}");
            }
        }

        Ok(())
    }

    /**
     * Walk the list of all extents and find any that are not open.
     * Open any extents that are not.
     */
    pub async fn reopen_all_extents(&mut self) -> Result<()> {
        let mut to_open = Vec::new();
        for (i, extent) in self.extents.iter().enumerate() {
            let inner = extent.lock().await;
            if matches!(*inner, ExtentState::Closed) {
                to_open.push(i);
            }
        }

        for eid in to_open {
            self.reopen_extent(eid).await?;
        }

        Ok(())
    }

    /**
     * Re open an extent that was previously closed
     */
    pub async fn reopen_extent(
        &mut self,
        eid: usize,
    ) -> Result<(), CrucibleError> {
        /*
         * Make sure the extent :
         *
         * - is currently closed
         * - matches our eid
         * - is not read-only
         */
        let mut mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        assert!(!self.read_only);

        let new_extent = Extent::open(
            &self.dir,
            &self.def,
            eid as u32,
            self.read_only,
            Backend::RawFile,
            &self.log,
        )?;

        if new_extent.dirty().await {
            self.dirty_extents.insert(eid);
        }

        *mg = ExtentState::Opened(Arc::new(new_extent));

        Ok(())
    }

    pub async fn close_extent(
        &self,
        eid: usize,
    ) -> Result<(u64, u64, bool), CrucibleError> {
        let mut extent_state = self.extents[eid].lock().await;

        let open_extent =
            std::mem::replace(&mut *extent_state, ExtentState::Closed);

        match open_extent {
            ExtentState::Opened(extent) => {
                // extent here is Arc<Extent>, and closing only makes sense if
                // the reference count is 1.
                let inner = Arc::into_inner(extent);
                assert!(inner.is_some());
                inner.unwrap().close().await
            }

            ExtentState::Closed => {
                panic!("close on closed extent {}!", eid);
            }
        }
    }

    /**
     * Repair an extent from another downstairs
     *
     * We need to repair an extent in such a way that an interruption
     * at any time can be recovered from.
     *
     * Let us assume we are repairing extent 012
     *  1. Make new 012.copy dir  (extent name plus: .copy)
     *  2. Get all extent files from source side, put in 012.copy directory
     *  3. fsync files we just downloaded
     *  4. Rename 012.copy dir to 012.replace dir
     *  5. fsync extent directory ( 00/000/ where the extent files live)
     *  6. Replace current extent 012 files with copied files of same name
     *    from 012.replace dir
     *  7. Remove any files in extent dir that don't exist in replacing dir
     *     For example, if the replacement extent has 012 and 012.db, but
     *     the current (bad) extent has 012 012.db 012.db-shm
     *     and 012.db-wal, we want to remove the 012.db-shm and 012.db-wal
     *     files when we replace 012 and 012.db with the new versions.
     *  8. fsync files after copying them (new location).
     *  9. fsync containing extent dir
     * 10. Rename 012.replace dir to 012.completed dir.
     * 11. fsync extent dir again (so dir rename is persisted)
     * 12. Delete completed dir.
     * 13. fsync extent dir again (so dir rename is persisted)
     *
     *  This also requires the following behavior on every extent open:
     *   A. If xxx.copy directory found, delete it.
     *   B. If xxx.completed directory found, delete it.
     *   C. If xxx.replace dir found start at step 4 above and continue
     *      on through 6.
     *   D. Only then, open extent.
     *
     *   If clone == true, then we are replacing our extent files
     *   with ones from a remote downstairs.  We make a few different
     *   decisions as that remote downstairs might have a different
     *   backend than our region was created with.
     */
    pub async fn repair_extent(
        &self,
        eid: usize,
        repair_addr: SocketAddr,
        clone: bool,
    ) -> Result<(), CrucibleError> {
        // Make sure the extent:
        // is currently closed, matches our eid
        let mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        drop(mg);

        // If this is not a clone, then we must not be read_only
        if !clone {
            assert!(!self.read_only);
        }

        self.get_extent_copy(eid, repair_addr, clone).await?;

        // Returning from get_extent_copy means we have copied all our
        // files and moved the copy directory to replace directory.
        // Now, replace the current extent files with the replacement ones.
        move_replacement_extent(&self.dir, eid, &self.log, clone)?;

        Ok(())
    }

    /**
     * Connect to the source and pull over all the extent files for the
     * given extent ID.
     * The files are loaded into the copy_dir for the given extent.
     * After all the files have been copied locally, we rename the
     * copy_dir to replace_dir.
     */
    pub async fn get_extent_copy(
        &self,
        eid: usize,
        repair_addr: SocketAddr,
        clone: bool,
    ) -> Result<(), CrucibleError> {
        // An extent must be closed before we replace its files.
        let mg = self.extents[eid].lock().await;
        assert!(matches!(*mg, ExtentState::Closed));
        drop(mg);

        // Make sure copy, replace, and cleanup directories don't exist yet.
        // We don't need them yet, but if they do exist, then something
        // is wrong.
        let rd = replace_dir(&self.dir, eid as u32);
        if rd.exists() {
            crucible_bail!(
                IoError,
                "Replace directory: {:?} already exists",
                rd,
            );
        }

        let copy_dir = Self::create_copy_dir(&self.dir, eid)?;
        info!(self.log, "Created copy dir {:?}", copy_dir);

        // XXX TLS someday?  Authentication?
        let url = format!("http://{:?}", repair_addr);
        let repair_server = Client::new(&url);

        let mut repair_files =
            match repair_server.get_files_for_extent(eid as u32).await {
                Ok(f) => f.into_inner(),
                Err(e) => {
                    crucible_bail!(
                        RepairRequestError,
                        "Failed to get repair files: {:?}",
                        e,
                    );
                }
            };

        repair_files.sort();
        info!(
            self.log,
            "eid:{} Found repair files: {:?}", eid, repair_files
        );

        // Depending on if this is a clone or not, we have a different
        // set of files we expect to find.
        if clone {
            if !validate_clone_files(eid, &repair_files) {
                crucible_bail!(
                    RepairFilesInvalid,
                    "Invalid clone file list: {:?}",
                    repair_files,
                );
            }
        } else if !validate_repair_files(eid, &repair_files) {
            crucible_bail!(
                RepairFilesInvalid,
                "Invalid repair file list: {:?}",
                repair_files,
            );
        }

        // Replace local files with their remote copies.
        // If we are replacing our region with one from an older version
        // that contained SQLite files, then we need to copy those files
        // over as well.
        let mut count = 0;
        for opt_file in &[
            ExtentType::Data,
            ExtentType::Db,
            ExtentType::DbShm,
            ExtentType::DbWal,
        ] {
            let filename = extent_file_name(eid as u32, *opt_file);

            if !repair_files.contains(&filename) {
                continue;
            }
            let local_file =
                Self::create_copy_file(copy_dir.clone(), eid, *opt_file)?;
            let repair_stream = match repair_server
                .get_extent_file(eid as u32, opt_file.to_file_type())
                .await
            {
                Ok(rs) => rs,
                Err(e) => {
                    crucible_bail!(
                        RepairRequestError,
                        "Failed to get extent {} {} file: {:?}",
                        eid,
                        opt_file,
                        e,
                    );
                }
            };
            save_stream_to_file(local_file, repair_stream.into_inner()).await?;
            count += 1;
        }

        // After we have all files: move the repair dir.
        info!(
            self.log,
            "{count} repair files downloaded, move directory {:?} to {:?}",
            copy_dir,
            rd
        );
        rename(copy_dir.clone(), rd.clone())?;

        // Files are synced in save_stream_to_file(). Now make sure
        // the parent directory containing the repair directory has
        // been synced so that change is persistent.
        let current_dir = extent_dir(&self.dir, eid as u32);

        sync_path(current_dir, &self.log)?;
        Ok(())
    }

    /**
     * if there is a difference between what our actual extent_count is
     * and what is requested, go out and create the new extent files.
     */
    pub async fn extend(&mut self, newsize: u32) -> Result<()> {
        if self.read_only {
            // XXX return CrucibleError instead of anyhow?
            bail!(CrucibleError::ModifyingReadOnlyRegion.to_string());
        }

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
            self.open_extents(true).await?;
        }
        Ok(())
    }

    pub fn region_def(&self) -> (u64, Block, u32) {
        (
            self.def.block_size(),
            self.def.extent_size(),
            self.def.extent_count(),
        )
    }

    pub fn def(&self) -> RegionDefinition {
        self.def
    }

    pub async fn meta_info(&self) -> Result<Vec<ExtentMeta>> {
        let mut result = Vec::with_capacity(self.extents.len());
        for eid in 0..self.extents.len() {
            let extent = self.get_opened_extent(eid).await;
            result.push(extent.get_meta_info().await)
        }
        Ok(result)
    }

    pub fn validate_hashes(
        &self,
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
        for write in writes {
            let computed_hash = if let Some(encryption_context) =
                &write.block_context.encryption_context
            {
                integrity_hash(&[
                    &encryption_context.nonce[..],
                    &encryption_context.tag[..],
                    &write.data[..],
                ])
            } else {
                integrity_hash(&[&write.data[..]])
            };

            if computed_hash != write.block_context.hash {
                error!(self.log, "Failed write hash validation");
                // TODO: print out the extent and block where this failed!!
                crucible_bail!(HashMismatch);
            }
        }

        Ok(())
    }

    #[instrument]
    pub async fn region_write(
        &mut self,
        writes: &[crucible_protocol::Write],
        job_id: JobId,
        only_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        if self.read_only {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        /*
         * Before anything, validate hashes
         */
        self.validate_hashes(writes)?;

        /*
         * Batch writes so they can all be sent to the appropriate extent
         * together.
         */
        let mut batched_writes: HashMap<usize, Vec<crucible_protocol::Write>> =
            HashMap::new();

        for write in writes {
            let extent_vec =
                batched_writes.entry(write.eid as usize).or_default();
            extent_vec.push(write.clone());
        }

        if only_write_unwritten {
            cdt::os__writeunwritten__start!(|| job_id.0);
        } else {
            cdt::os__write__start!(|| job_id.0);
        }
        for eid in batched_writes.keys() {
            let extent = self.get_opened_extent(*eid).await;
            let writes = batched_writes.get(eid).unwrap();
            extent
                .write(job_id, &writes[..], only_write_unwritten)
                .await?;
        }

        // Mark any extents we sent a write-command to as potentially dirty
        self.dirty_extents.extend(batched_writes.keys());

        if only_write_unwritten {
            cdt::os__writeunwritten__done!(|| job_id.0);
        } else {
            cdt::os__write__done!(|| job_id.0);
        }

        Ok(())
    }

    #[instrument]
    pub async fn region_read(
        &self,
        requests: &[crucible_protocol::ReadRequest],
        job_id: JobId,
    ) -> Result<Vec<crucible_protocol::ReadResponse>, CrucibleError> {
        let mut responses = Vec::with_capacity(requests.len());

        /*
         * Batch reads so they can all be sent to the appropriate extent
         * together.
         *
         * Note: Have to maintain order with reads! The Upstairs expects read
         * responses to be in the same order as read requests, so we can't
         * use a hashmap in the same way that batching writes can.
         */
        let mut eid: Option<u64> = None;
        let mut batched_reads = Vec::with_capacity(requests.len());

        cdt::os__read__start!(|| job_id.0);
        for request in requests {
            if let Some(_eid) = eid {
                if request.eid == _eid {
                    batched_reads.push(request.clone());
                } else {
                    let extent = self.get_opened_extent(_eid as usize).await;
                    responses
                        .extend(extent.read(job_id, &batched_reads[..]).await?);

                    eid = Some(request.eid);
                    batched_reads.clear();
                    batched_reads.push(request.clone());
                }
            } else {
                eid = Some(request.eid);
                batched_reads.clear();
                batched_reads.push(request.clone());
            }
        }

        if let Some(_eid) = eid {
            let extent = self.get_opened_extent(_eid as usize).await;
            responses.extend(extent.read(job_id, &batched_reads[..]).await?);
        }
        cdt::os__read__done!(|| job_id.0);

        Ok(responses)
    }

    /*
     * Send a flush to just the given extent. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub(crate) async fn region_flush_extent<
        I: Into<JobOrReconciliationId> + Debug,
    >(
        &self,
        eid: usize,
        gen_number: u64,
        flush_number: u64,
        job_id: I, // only used for logging
    ) -> Result<(), CrucibleError> {
        debug!(
            self.log,
            "Flush just extent {} with f:{} and g:{}",
            eid,
            flush_number,
            gen_number
        );

        let extent = self.get_opened_extent(eid).await;
        extent
            .flush(flush_number, gen_number, job_id, &self.log)
            .await?;

        Ok(())
    }

    /*
     * Send a flush to all extents. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub async fn region_flush(
        &mut self,
        flush_number: u64,
        gen_number: u64,
        snapshot_details: &Option<SnapshotDetails>,
        job_id: JobId,
        extent_limit: Option<usize>,
    ) -> Result<(), CrucibleError> {
        // It should be ok to Flush a read-only region, but not take a snapshot.
        // Most likely this read-only region *is* a snapshot, so that's
        // redundant :)
        if self.read_only && snapshot_details.is_some() {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::os__flush__start!(|| job_id.0);

        // Select extents we're going to flush, while respecting the
        // extent_limit if one was provided.
        let dirty_extents: Vec<usize> = match extent_limit {
            None => self.dirty_extents.iter().copied().collect(),
            Some(el) => {
                if el > self.def.extent_count().try_into().unwrap() {
                    crucible_bail!(InvalidExtent);
                }

                self.dirty_extents
                    .iter()
                    .copied()
                    .filter(|x| *x <= el)
                    .collect()
            }
        };

        let extent_count = dirty_extents.len();

        // Spawn parallel tasks for the flush
        let mut join_handles: Vec<JoinHandle<Result<(), CrucibleError>>> =
            Vec::with_capacity(extent_count);

        for eid in &dirty_extents {
            let extent = self.get_opened_extent(*eid).await;
            let log = self.log.clone();
            let jh = tokio::spawn(async move {
                extent.flush(flush_number, gen_number, job_id, &log).await
            });
            join_handles.push(jh);
        }

        // Wait for all flushes to finish - wait until after
        // cdt::os__flush__done to check the results and bail out.
        let mut results = Vec::with_capacity(extent_count);
        for join_handle in join_handles {
            results.push(
                join_handle
                    .await
                    .map_err(|e| CrucibleError::GenericError(e.to_string())),
            );
        }

        cdt::os__flush__done!(|| job_id.0);

        for result in results {
            // If any extent flush failed, then return that as an error. Because
            // the results were all collected above, each extent flush has
            // completed at this point.
            result??;
        }

        // Now everything has succeeded, we can remove these extents from the
        // flush candidates
        match extent_limit {
            None => self.dirty_extents.clear(),
            Some(_) => {
                for eid in &dirty_extents {
                    self.dirty_extents.remove(eid);
                }
            }
        }

        // snapshots currently only work with ZFS
        if cfg!(feature = "zfs_snapshot") {
            if let Some(snapshot_details) = snapshot_details {
                info!(self.log, "Flush and snap request received");
                // Check if the path exists, return an error if it does
                let test_path = format!(
                    "{}/.zfs/snapshot/{}",
                    self.dir.clone().into_os_string().into_string().unwrap(),
                    snapshot_details.snapshot_name,
                );

                if std::path::Path::new(&test_path).is_dir() {
                    crucible_bail!(
                        SnapshotExistsAlready,
                        "{}",
                        snapshot_details.snapshot_name,
                    );
                }

                // Look up dataset name for path (this works with any path, and
                // will return the parent dataset).
                let path =
                    self.dir.clone().into_os_string().into_string().unwrap();

                let dataset_name = std::process::Command::new("zfs")
                    .args(["list", "-pH", "-o", "name", &path])
                    .output()
                    .map_err(|e| {
                        CrucibleError::SnapshotFailed(e.to_string())
                    })?;

                let dataset_name = std::str::from_utf8(&dataset_name.stdout)
                    .map_err(|e| CrucibleError::SnapshotFailed(e.to_string()))?
                    .trim_end(); // remove '\n' from end

                let output = std::process::Command::new("zfs")
                    .args([
                        "snapshot",
                        format!(
                            "{}@{}",
                            dataset_name, snapshot_details.snapshot_name
                        )
                        .as_str(),
                    ])
                    .output()
                    .map_err(|e| {
                        CrucibleError::SnapshotFailed(e.to_string())
                    })?;

                if !output.status.success() {
                    crucible_bail!(
                        SnapshotFailed,
                        "{}",
                        std::str::from_utf8(&output.stderr).map_err(|e| {
                            CrucibleError::GenericError(e.to_string())
                        })?,
                    );
                }
            }
        } else if snapshot_details.is_some() {
            error!(self.log, "Snapshot request received on unsupported binary");
        }
        Ok(())
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
        extent_type: ExtentType,
    ) -> Result<File> {
        // Get the base extent name before we consider the actual Type
        let name = extent_file_name(eid as u32, ExtentType::Data);
        copy_dir.push(name);

        let ext = format!("{}", extent_type);
        copy_dir.set_extension(ext);
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
}

/**
 * Given:
 *   The stream returned to us from the progenitor endpoint
 *   A local File, already created and opened,
 * Stream the data from the endpoint into the file.
 * When the stream is completed, fsync the file.
 */
pub async fn save_stream_to_file(
    mut file: File,
    mut stream: repair_client::ByteStream,
) -> Result<(), CrucibleError> {
    loop {
        match stream.try_next().await {
            Ok(Some(bytes)) => {
                file.write_all(&bytes)?;
            }
            Ok(None) => break,
            Err(e) => {
                crucible_bail!(
                    RepairStreamError,
                    "repair {:?}: stream error: {:?}",
                    file,
                    e
                );
            }
        }
    }
    if let Err(e) = file.sync_all() {
        crucible_bail!(IoError, "repair {:?}: fsync failure: {:?}", file, e);
    }
    Ok(())
}

struct BatchedPwritevState<'a> {
    byte_offset: u64,
    iovecs: Vec<IoSlice<'a>>,
    next_block_in_run: u64,
    expected_bytes: usize,
}

pub(crate) struct BatchedPwritev<'a> {
    fd: std::os::fd::BorrowedFd<'a>,
    capacity: usize,
    state: Option<BatchedPwritevState<'a>>,
    block_size: u64,
    iov_max: usize,
}

impl<'a> BatchedPwritev<'a> {
    pub fn new(
        fd: std::os::fd::BorrowedFd<'a>,
        capacity: usize,
        block_size: u64,
        iov_max: usize,
    ) -> Self {
        Self {
            fd,
            capacity,
            state: None,
            block_size,
            iov_max,
        }
    }

    /// Add a write to the batch. If the write would cause the list of iovecs to
    /// be larger than IOV_MAX, then `perform_writes` is called.
    pub fn add_write(
        &mut self,
        write: &'a crucible_protocol::Write,
    ) -> Result<(), CrucibleError> {
        let block = write.offset.value;

        let should_perform_writes = if let Some(state) = &self.state {
            // Is this write contiguous with the last?
            if block == state.next_block_in_run {
                // If so, then add it to the list. Make sure to flush if the
                // total size would become larger than IOV_MAX.
                (state.iovecs.len() + 1) >= self.iov_max
            } else {
                // If not, then flush, and start the state all over.
                true
            }
        } else {
            false
        };

        if should_perform_writes {
            self.perform_writes()?;
        }

        // If perform_writes was called above, then state will be None. If
        // perform_writes was not called, then:
        //
        // - block == state.next_block_in_run, and
        // - (state.iovecs.len() + 1) <= self.iov_max
        //
        // hence the assertion below.
        if let Some(state) = &mut self.state {
            assert_eq!(block, state.next_block_in_run);
            state.iovecs.push(IoSlice::new(&write.data));
            state.next_block_in_run += 1;
            state.expected_bytes += write.data.len();
        } else {
            // start fresh
            self.state = Some(BatchedPwritevState {
                byte_offset: write.offset.value * self.block_size,
                iovecs: {
                    let mut iovecs = Vec::with_capacity(self.capacity);
                    iovecs.push(IoSlice::new(&write.data));
                    iovecs
                },
                expected_bytes: write.data.len(),
                next_block_in_run: block + 1,
            });
        }

        Ok(())
    }

    // Write bytes out to file target
    pub fn perform_writes(&mut self) -> Result<(), CrucibleError> {
        if let Some(state) = &mut self.state {
            assert!(!state.iovecs.is_empty());

            let n = nix::sys::uio::pwritev(
                self.fd,
                &state.iovecs[..],
                state.byte_offset as i64,
            )
            .map_err(|e| CrucibleError::IoError(e.to_string()))?;
            if n != state.expected_bytes {
                return Err(CrucibleError::IoError(format!(
                    "pwritev incomplete (expected {}, got {n} bytes)",
                    state.expected_bytes
                )));
            }

            self.state = None;
        }

        Ok(())
    }
}

pub fn config_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push("region.json");
    out
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::rename;
    use std::path::PathBuf;

    use rand::{Rng, RngCore};
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::dump::dump_region;
    use crate::extent::{
        completed_dir, copy_dir, extent_path, remove_copy_cleanup_dir,
        DownstairsBlockContext,
    };

    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    static TEST_UUID_STR: &str = "12345678-1111-2222-3333-123456789999";

    fn test_uuid() -> Uuid {
        TEST_UUID_STR.parse().unwrap()
    }

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    pub fn new_region_options() -> crucible_common::RegionOptions {
        let mut region_options: crucible_common::RegionOptions =
            Default::default();
        let block_size = 512;
        region_options.set_block_size(block_size);
        region_options
            .set_extent_size(Block::new(10, block_size.trailing_zeros()));
        region_options.set_uuid(test_uuid());
        region_options
    }

    #[tokio::test]
    async fn region_create_drop_open() -> Result<()> {
        // Create a region, make three extents.
        // Drop the region, then open it.
        let dir = tempdir()?;
        let log = csl();
        let mut region =
            Region::create(&dir, new_region_options(), log.clone()).await?;
        region.extend(3).await?;

        drop(region);

        let _region =
            Region::open(&dir, new_region_options(), true, false, &log).await?;

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_read_version_low() -> Result<()> {
        // Create a region where the read database version is down rev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(0, DATABASE_WRITE_VERSION);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_write_version_low() -> Result<()> {
        // Create a region where the write database version is downrev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());

        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(DATABASE_READ_VERSION, 0);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_read_version_high() -> Result<()> {
        // Create a region where the read database version is too high.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(
            DATABASE_READ_VERSION + 1,
            DATABASE_WRITE_VERSION,
        );
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn region_bad_database_write_version_high() -> Result<()> {
        // Create a region where the write database version is too high.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());

        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(
            DATABASE_READ_VERSION,
            DATABASE_WRITE_VERSION + 1,
        );
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, new_region_options(), true, false, &csl())
            .await
            .unwrap_err();

        Ok(())
    }

    #[tokio::test]
    async fn copy_extent_dir() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Remove the copy directory, make sure it goes away.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let cp = copy_dir(&dir, 1);

        assert!(Region::create_copy_dir(&dir, 1).is_ok());
        assert!(Path::new(&cp).exists());
        assert!(remove_copy_cleanup_dir(&dir, 1).is_ok());
        assert!(!Path::new(&cp).exists());
        Ok(())
    }

    #[tokio::test]
    async fn copy_extent_dir_twice() -> Result<()> {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Verify a second create will fail.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(3).await.unwrap();

        Region::create_copy_dir(&dir, 1).unwrap();
        let res = Region::create_copy_dir(&dir, 1);
        assert!(res.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn close_extent() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        let (gen, flush, dirty) = region.close_extent(1).await.unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for a new unwritten extent.
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(!dirty);

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;
        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Make sure the copy directory is gone
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_one() -> Result<()> {
        // Verify the copy directory is removed if an extent is
        // opened with that directory present.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Make sure copy directory was removed
        assert!(!Path::new(&cp).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_two() -> Result<()> {
        // Verify that the completed directory is removed if present
        // when an extent is re-opened.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // Step through the replacement dir, but don't do any work.
        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Finish up the fake repair, but leave behind the completed dir.
        let cd = completed_dir(&dir, 1);
        rename(rd.clone(), cd.clone())?;

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Verify extent one is valid
        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay() -> Result<()> {
        // Verify on extent open that a replacement directory will
        // have it's contents replace an extents existing data and
        // metadata files.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Now we have a replace directory, we verify that special
        // action is taken when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay_sqlite() -> Result<()> {
        // Verify on extent open that a replacement directory will
        // have it's contents replace an extents existing data and
        // metadata files.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db-shm");
        dest_path.set_extension("db-shm");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db-wal");
        dest_path.set_extension("db-wal");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Now we have a replace directory, we verify that special
        // action is taken when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay_short() -> Result<()> {
        // test move_replacement_extent(), create a copy dir, populate it
        // and let the reopen do the work.  This time we make sure our
        // copy dir only has the extent data file.

        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        println!("cp {:?} to {:?}", source_path, dest_path);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // Now we have a replace directory populated and our files to
        // delete are ready.  We verify that special action is taken
        // when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_cleanup_replay_short_sqlite() -> Result<()> {
        // test move_replacement_extent(), create a copy dir, populate it
        // and let the reopen do the work.  This time we make sure our
        // copy dir only has extent data and .db files, and not .db-shm
        // nor .db-wal.  Verify these files are delete from the original
        // extent after the reopen has cleaned them up.
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        println!("cp {:?} to {:?}", source_path, dest_path);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp.clone(), rd.clone())?;

        // The close may remove the db-shm and db-wal files, manually
        // create them here, just to verify they are removed after the
        // reopen as they are not included in the files to be recovered
        // and this test exists to verify they will be deleted.
        let mut invalid_db = extent_path(&dir, 1);
        invalid_db.set_extension("db-shm");
        println!("Recreate {:?}", invalid_db);
        std::fs::copy(source_path.clone(), invalid_db.clone())?;
        assert!(Path::new(&invalid_db).exists());

        invalid_db.set_extension("db-wal");
        println!("Recreate {:?}", invalid_db);
        std::fs::copy(source_path.clone(), invalid_db.clone())?;
        assert!(Path::new(&invalid_db).exists());

        // Now we have a replace directory populated and our files to
        // delete are ready.  We verify that special action is taken
        // when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(1).await?;

        // Make sure there is no longer a db-shm and db-wal
        dest_path.set_extension("db-shm");
        assert!(!Path::new(&dest_path).exists());
        dest_path.set_extension("db-wal");
        assert!(!Path::new(&dest_path).exists());

        let _ext_one = region.get_opened_extent(1).await;

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, 1);
        assert!(!Path::new(&cd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_no_replay_readonly_sqlite() -> Result<()> {
        // Verify on a read-only region a replacement directory will
        // be ignored.  This is required by the dump command, as it would
        // be tragic if the command to inspect a region changed that
        // region's contents in the act of inspecting.

        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;

        // Make copy directory for this extent
        let _ext_one = region.get_opened_extent(1).await;
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let mut source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        source_path.set_extension("db");
        dest_path.set_extension("db");
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp, rd.clone())?;

        drop(region);

        // Open up the region read_only now.
        let region =
            Region::open(&dir, new_region_options(), false, true, &csl())
                .await?;

        // Verify extent 1 has opened again.
        let _ext_one = region.get_opened_extent(1).await;

        // Make sure repair directory is still present
        assert!(Path::new(&rd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_no_replay_readonly() -> Result<()> {
        // Verify on a read-only region a replacement directory will
        // be ignored.  This is required by the dump command, as it would
        // be tragic if the command to inspect a region changed that
        // region's contents in the act of inspecting.

        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        // Make copy directory for this extent
        let _ext_one = region.get_opened_extent(1).await;
        let cp = Region::create_copy_dir(&dir, 1)?;

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(1, ExtentType::Data);
        let source_path = extent_path(&dir, 0);
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone())?;

        let rd = replace_dir(&dir, 1);
        rename(cp, rd.clone())?;

        drop(region);

        // Open up the region read_only now.
        let region =
            Region::open(&dir, new_region_options(), false, true, &csl())
                .await?;

        // Verify extent 1 has opened again.
        let _ext_one = region.get_opened_extent(1).await;

        // Make sure repair directory is still present
        assert!(Path::new(&rd).exists());

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_partial_migration() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;
        let ddef = region.def();

        region
            .region_write(
                &[
                    crucible_protocol::Write {
                        eid: 1,
                        offset: Block::new_512(0),
                        data: Bytes::from(vec![1u8; 512]),
                        block_context: BlockContext {
                            encryption_context: None,
                            hash: 8717892996238908351, // hash for all 1s
                        },
                    },
                    crucible_protocol::Write {
                        eid: 2,
                        offset: Block::new_512(0),
                        data: Bytes::from(vec![2u8; 512]),
                        block_context: BlockContext {
                            encryption_context: None,
                            hash: 2192425179333611943, // hash for all 2s
                        },
                    },
                ],
                JobId(0),
                false,
            )
            .await?;
        drop(region);

        // Manually calculate the migration from extent 1
        let extent_file = extent_path(&dir, 1);
        let mut inner = extent_inner_sqlite::SqliteInner::open(
            dir.path(),
            &ddef,
            1,
            false,
            &log,
        )?;
        use crate::extent::ExtentInner;
        let ctxs = inner.export_contexts()?;
        let dirty = inner.dirty()?;
        let flush_number = inner.flush_number()?;
        let gen_number = inner.gen_number()?;
        drop(inner);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&extent_file)?;
        println!("about to call `import` on {:?}", extent_file);
        extent_inner_raw::RawInner::import(
            &mut file,
            &ddef,
            ctxs,
            dirty,
            flush_number,
            gen_number,
        )?;
        println!("dooone");
        // At this point, we have manually written the file, but have not
        // deleted the `.db` on disk.  As such, migration should restart when
        // the extent is reopened.

        let region =
            Region::open(&dir, new_region_options(), true, false, &log).await?;
        let out = region
            .region_read(
                &[
                    ReadRequest {
                        eid: 1,
                        offset: Block::new_512(0),
                    },
                    ReadRequest {
                        eid: 2,
                        offset: Block::new_512(0),
                    },
                ],
                JobId(0),
            )
            .await?;
        assert_eq!(out[0].data.as_ref(), [1; 512]);
        assert_eq!(out[1].data.as_ref(), [2; 512]);

        Ok(())
    }

    #[tokio::test]
    async fn reopen_extent_partial_migration_corrupt() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;
        let ddef = region.def();

        // Make some writes, which we'll check after migration
        region
            .region_write(
                &[
                    crucible_protocol::Write {
                        eid: 1,
                        offset: Block::new_512(0),
                        data: Bytes::from(vec![1u8; 512]),
                        block_context: BlockContext {
                            encryption_context: None,
                            hash: 8717892996238908351, // hash for all 1s
                        },
                    },
                    crucible_protocol::Write {
                        eid: 2,
                        offset: Block::new_512(0),
                        data: Bytes::from(vec![2u8; 512]),
                        block_context: BlockContext {
                            encryption_context: None,
                            hash: 2192425179333611943, // hash for all 2s
                        },
                    },
                ],
                JobId(0),
                false,
            )
            .await?;
        drop(region);

        // Manually calculate the migration from extent 1, but deliberately mess
        // with the context values (simulating a migration that didn't manage to
        // write everything to disk).
        let extent_file = extent_path(&dir, 1);
        let mut inner = extent_inner_sqlite::SqliteInner::open(
            dir.path(),
            &ddef,
            1,
            false,
            &log,
        )?;
        use crate::extent::ExtentInner;
        let ctxs = inner.export_contexts()?.into_iter().map(|_| None).collect();
        let dirty = inner.dirty()?;
        let flush_number = inner.flush_number()?;
        let gen_number = inner.gen_number()?;
        drop(inner);

        // Stage the corrupted migration
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&extent_file)?;
        extent_inner_raw::RawInner::import(
            &mut file,
            &ddef,
            ctxs,
            dirty,
            flush_number,
            gen_number,
        )?;
        // At this point, we have manually written the file, but have not
        // deleted the `.db` on disk.  As such, migration should restart when
        // the extent is reopened, and we should recover from corruption.

        let region =
            Region::open(&dir, new_region_options(), true, false, &log).await?;
        let out = region
            .region_read(
                &[
                    ReadRequest {
                        eid: 1,
                        offset: Block::new_512(0),
                    },
                    ReadRequest {
                        eid: 2,
                        offset: Block::new_512(0),
                    },
                ],
                JobId(0),
            )
            .await?;
        assert_eq!(out[0].data.as_ref(), [1; 512]);
        assert_eq!(out[1].data.as_ref(), [2; 512]);

        Ok(())
    }

    // wrapper to send to sub-tests for true/false here
    #[test]
    fn validate_repair_files_empty() {
        // No repair files is a failure
        assert!(!validate_repair_files(1, &Vec::new()));
        assert!(!validate_clone_files(1, &Vec::new()));
    }

    #[test]
    fn validate_repair_files_good() {
        // This is an expected list of files for an extent
        let good_files: Vec<String> = vec!["001".to_string()];

        assert!(validate_repair_files(1, &good_files));
        assert!(validate_clone_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_duplicate() {
        // duplicate file names for extent 2
        let good_files: Vec<String> =
            vec!["002".to_string(), "002".to_string()];
        assert!(!validate_repair_files(2, &good_files));
        assert!(!validate_clone_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_offbyon_data() {
        // Incorrect file names for extent 2
        let good_files: Vec<String> = vec!["001".to_string()];

        assert!(!validate_repair_files(2, &good_files));
        assert!(!validate_clone_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_db() {
        // db file only exists on replacement
        let good_files: Vec<String> =
            vec!["001".to_string(), "001.db".to_string()];

        assert!(!validate_repair_files(1, &good_files));
        // Valid for replacement
        assert!(validate_clone_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_db_more() {
        // This list can only exist for replacement
        let many_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];

        assert!(!validate_repair_files(1, &many_files));
        assert!(validate_clone_files(1, &many_files));
    }

    #[test]
    fn validate_repair_files_duplicate_pair() {
        // duplicate file names for extent 2
        let good_files: Vec<String> = vec![
            "002".to_string(),
            "002".to_string(),
            "002.db".to_string(),
            "002.db".to_string(),
        ];
        assert!(!validate_repair_files(2, &good_files));
        assert!(!validate_clone_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_quad_duplicate() {
        // Duplicate db-shm file
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-shm".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
        assert!(!validate_clone_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_offbyon_all() {
        // Incorrect file names for extent 2
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(2, &good_files));
        assert!(!validate_clone_files(2, &good_files));
    }

    #[test]
    fn validate_repair_files_too_good() {
        // Duplicate data file in list
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001".to_string(),
            "001.db".to_string(),
            "001.db-shm".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
        assert!(!validate_clone_files(1, &good_files));
    }

    #[test]
    fn validate_repair_files_not_good_enough() {
        // Replacement requires 1, 2 or 4 files, not 3
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(1, &good_files));
        assert!(!validate_clone_files(1, &good_files));
    }

    #[tokio::test]
    async fn reopen_all_extents() -> Result<()> {
        // Create the region, make three extents
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(5).await?;

        // Close extent 1
        region.close_extent(1).await.unwrap();
        assert!(matches!(
            *region.extents[1].lock().await,
            ExtentState::Closed
        ));

        // Close extent 4
        region.close_extent(4).await.unwrap();
        assert!(matches!(
            *region.extents[4].lock().await,
            ExtentState::Closed
        ));

        // Reopen all extents
        region.reopen_all_extents().await?;

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(1).await;

        // Make sure the eid matches
        assert_eq!(ext_one.number, 1);

        // Verify extent four is valid
        let ext_four = region.get_opened_extent(4).await;

        // Make sure the eid matches
        assert_eq!(ext_four.number, 4);

        Ok(())
    }

    #[tokio::test]
    async fn new_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl()).await;
        Ok(())
    }

    #[tokio::test]
    async fn new_existing_region() -> Result<()> {
        let dir = tempdir()?;
        let _ = Region::create(&dir, new_region_options(), csl()).await;
        let _ = Region::open(&dir, new_region_options(), false, false, &csl())
            .await;
        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn bad_import_region() {
        let _ = Region::open(
            "/tmp/12345678-1111-2222-3333-123456789999/notadir",
            new_region_options(),
            false,
            false,
            &csl(),
        )
        .await
        .unwrap();
    }

    #[test]
    fn copy_path_basic() {
        assert_eq!(
            copy_dir("/var/region", 4),
            p("/var/region/00/000/004.copy")
        );
    }

    #[tokio::test]
    async fn dump_a_region() -> Result<()> {
        /*
         * Create a region, give it actual size
         */
        let dir = tempdir()?;
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(2).await?;

        /*
         * Build the Vec for our region dir
         */
        let dvec = vec![dir.into_path()];

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn dump_two_region() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;
        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        let mut r2 = Region::create(&dir2, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(2).await?;
        r2.extend(2).await?;

        /*
         * Build the Vec for our region dirs
         */
        let mut dvec = Vec::new();
        let pdir = dir.into_path();
        dvec.push(pdir);
        let pdir = dir2.into_path();
        dvec.push(pdir);

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn dump_extent() -> Result<()> {
        /*
         * Create our temp dirs
         */
        let dir = tempdir()?;
        let dir2 = tempdir()?;

        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        r1.extend(3).await?;
        let mut r2 = Region::create(&dir2, new_region_options(), csl())
            .await
            .unwrap();
        r2.extend(3).await?;

        /*
         * Build the Vec for our region dirs
         */
        let mut dvec = Vec::new();
        let pdir = dir.into_path();
        dvec.push(pdir);
        let pdir = dir2.into_path();
        dvec.push(pdir);

        /*
         * Dump the region
         */
        dump_region(dvec, Some(2), None, false, false, csl()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_big_write() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), false).await?;

        // read data into File, compare what was written to buffer

        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        let extent_data_size =
            (ddef.extent_size().value * ddef.block_size()) as usize;
        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let data = std::fs::read(path).expect("Unable to read file");

            read_from_files.extend(&data[..extent_data_size]);
        }

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read

        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_big_write_migrate() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create_with_backend(
            &dir,
            new_region_options(),
            Backend::SQLite,
            csl(),
        )
        .await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), false).await?;
        for i in 0..3 {
            region.region_flush_extent(i, 10, 15, JobId(21)).await?;
        }
        let meta = region.get_opened_extent(0).await.get_meta_info().await;
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);
        drop(region);

        // Open the region as read-only, which doesn't trigger a migration
        let region =
            Region::open(&dir, new_region_options(), true, true, &log).await?;
        let meta = region.get_opened_extent(0).await.get_meta_info().await;
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);

        // Assert that the .db files still exist
        for i in 0..3 {
            assert!(extent_dir(&dir, i)
                .join(extent_file_name(i, ExtentType::Db))
                .exists());
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let read_from_region: Vec<u8> = region
            .region_read(&requests, JobId(0))
            .await?
            .into_iter()
            .flat_map(|i| i.data.to_vec())
            .collect();

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);
        drop(region);

        // Open the region as read-write, which **does** trigger a migration
        let region =
            Region::open(&dir, new_region_options(), true, false, &log).await?;
        let meta = region.get_opened_extent(0).await.get_meta_info().await;
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);

        // Assert that the .db files have been deleted during the migration
        for i in 0..3 {
            assert!(!extent_dir(&dir, i)
                .join(extent_file_name(i, ExtentType::Db))
                .exists());
        }
        let read_from_region: Vec<u8> = region
            .region_read(&requests, JobId(0))
            .await?
            .into_iter()
            .flat_map(|i| i.data.to_vec())
            .collect();

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);
        drop(region);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_open_removes_partial_writes() -> Result<()> {
        // Opening a dirty extent should fully rehash the extent to remove any
        // contexts that don't correlate with data on disk. This is necessary
        // for write_unwritten to work after a crash, and to move us into a
        // good state for flushes (flushes only clear out contexts for blocks
        // written since the last flush)
        //
        // Specifically, this test checks for the case where we had a brand new
        // block and a write to that blocks that failed such that only the
        // write's block context was persisted, leaving the data all zeros. In
        // this case, there is no data, so we should remove the invalid block
        // context row.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // A write of some sort only wrote a block context row and dirty flag
        {
            let ext = region.get_opened_extent(0).await;
            let mut inner = ext.lock().await;
            inner.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 1024,
                },
                block: 0,
                on_disk_hash: 65536,
            })?;
        }

        // This should clear out the invalid contexts
        for eid in 0..region.extents.len() {
            region.close_extent(eid).await.unwrap();
        }

        region.reopen_all_extents().await?;

        // Verify no block context rows exist
        {
            let ext = region.get_opened_extent(0).await;
            let mut inner = ext.lock().await;
            assert!(inner.get_block_contexts(0, 1)?[0].is_empty());
        }

        // Assert write unwritten will write to the first block

        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        region
            .region_write(
                &[crucible_protocol::Write {
                    eid: 0,
                    offset: Block::new_512(0),
                    data,
                    block_context: BlockContext {
                        encryption_context: None,
                        hash,
                    },
                }],
                JobId(124),
                true, // only_write_unwritten
            )
            .await?;

        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest {
                    eid: 0,
                    offset: Block::new_512(0),
                }],
                JobId(125),
            )
            .await?;

        let response = &responses[0];

        assert_eq!(response.data.to_vec(), vec![0x55; 512]);

        Ok(())
    }

    #[tokio::test]
    async fn test_ok_hash_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 9163319254371683066,
                },
            }];

        region.region_write(&writes, JobId(0), false).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_empty() -> Result<()> {
        // Verify that a read fill does write to a block when there is
        // no data written yet.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s (random)
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9's
                },
            }];

        region.region_write(&writes, JobId(0), true).await?;

        // Verify the dirty bit is now set.
        // We know our EID, so we can shortcut to getting the actual extent.
        assert!(region.get_opened_extent(eid as usize).await.dirty().await);

        // Now read back that block, make sure it is updated.
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(0),
            )
            .await?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 1);
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_written() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s (random)
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9's
                },
            }];

        region.region_write(&writes, JobId(0), false).await?;

        // Same block, now try to write something else to it.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 9163319254371683066, // hash for all 1s
                },
            }];
        // Do the write again, but with only_write_unwritten set now.
        region.region_write(&writes, JobId(1), true).await?;

        // Now read back that block, make sure it has the first write
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(2),
            )
            .await?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_when_written_flush() -> Result<()> {
        // Verify that a read fill does not write to the block when
        // there is data written already.  This time run a flush after the
        // first write.  Verify correct state of dirty bit as well.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await?;

        // Verify the dirty bit is now set.
        assert!(region.get_opened_extent(eid as usize).await.dirty().await);

        // Flush extent with eid, fn, gen
        region
            .region_flush_extent(eid as usize, 1, 1, JobId(1))
            .await?;

        // Verify the dirty bit is no longer set.
        assert!(!region.get_opened_extent(eid as usize).await.dirty().await);

        // Create a new write IO with different data.
        let data = BytesMut::from(&[1u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 9163319254371683066, // hash for all 1s
                },
            }];

        // Do the write again, but with only_write_unwritten set now.
        region.region_write(&writes, JobId(1), true).await?;

        // Verify the dirty bit is not set.
        assert!(!region.get_opened_extent(eid as usize).await.dirty().await);

        // Read back our block, make sure it has the first write data
        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest { eid, offset }],
                JobId(2),
            )
            .await?;

        // We should still have one response.
        assert_eq!(responses.len(), 1);
        // Hash should be just 1
        assert_eq!(responses[0].hashes().len(), 1);
        // Data should match first write
        assert_eq!(responses[0].data[..], [9u8; 512][..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write() -> Result<()> {
        // Do a multi block write where all blocks start new (unwritten)
        // Verify only empty blocks have data.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region

        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        let extent_data_size =
            (ddef.extent_size().value * ddef.block_size()) as usize;
        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let data = std::fs::read(path).expect("Unable to read file");

            read_from_files.extend(&data[..extent_data_size]);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_0() -> Result<()> {
        // Do a write to block zero, then do a multi block write with
        // only_write_unwritten set. Verify block zero is the first write, and
        // the remaining blocks have the contents from the multi block fill.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];

        // Now write just one block
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        let extent_data_size =
            (ddef.extent_size().value * ddef.block_size()) as usize;
        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let data = std::fs::read(path).expect("Unable to read file");

            read_from_files.extend(&data[..extent_data_size]);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_1() -> Result<()> {
        // Write to the second block, then do a multi block fill.
        // Verify the second block has the original data we wrote, and all
        // the other blocks have the data from the multi block fill.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(3).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);

        // Construct the write for the second block on the first EID.
        let eid = 0;
        let offset = Block::new_512(1);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s,
                },
            }];

        // Now write just to the second block.
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        // send write_unwritten command.
        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(1024).skip(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let mut read_from_files: Vec<u8> = Vec::with_capacity(total_size);

        let extent_data_size =
            (ddef.extent_size().value * ddef.block_size()) as usize;
        for i in 0..ddef.extent_count() {
            let path = extent_path(&dir, i);
            let data = std::fs::read(path).expect("Unable to read file");

            read_from_files.extend(&data[..extent_data_size]);
        }

        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_final() -> Result<()> {
        // Do a write to the fourth block, then do a multi block read fill
        // where the last block of the read fill is what we wrote to in
        // our first write.
        // verify the fourth block has the original write, and the first
        // three blocks have the data from the multi block read fill.

        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(5).await?;

        let ddef = region.def();
        // A bunch of things expect a 512, so let's make it explicit.
        assert_eq!(ddef.block_size(), 512);
        let num_blocks: usize = 4;
        let total_size: usize = ddef.block_size() as usize * num_blocks;

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);

        // Construct the write for the second block on the first EID.
        let eid = 0;
        let offset = Block::new_512(3);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];

        // Now write just to the second block.
        region.region_write(&writes, JobId(0), false).await?;

        // Now use region_write to fill four blocks
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        println!("buffer size:{}", buffer.len());
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        // send only_write_unwritten command.
        region.region_write(&writes, JobId(0), true).await?;

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our
        // expected buffer for the final block to have that original data.
        for a_buf in buffer.iter_mut().take(2048).skip(1536) {
            *a_buf = 9;
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            println!("Read eid: {}, {} offset: {:?}", eid, i, offset);
            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
            println!("Read a region, append");
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_unwritten_big_write_partial_sparse() -> Result<()> {
        // Do a multi block write_unwritten where a few different blocks have
        // data. Verify only unwritten blocks get the data.
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(4).await?;

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Fill a buffer with "9"s
        let blocks_to_write = [1, 3, 7, 8, 11, 12, 13];
        for b in blocks_to_write {
            let data = BytesMut::from(&[9u8; 512][..]);
            let eid: u64 = b as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((b as u64) % ddef.extent_size().value);

            // Write a few different blocks
            let writes: Vec<crucible_protocol::Write> =
                vec![crucible_protocol::Write {
                    eid,
                    offset,
                    data: data.freeze(),
                    block_context: BlockContext {
                        encryption_context: Some(
                            crucible_protocol::EncryptionContext {
                                nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                                tag: [
                                    4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                    16, 17, 18, 19,
                                ],
                            },
                        ),
                        hash: 14137680576404864188, // Hash for all 9s
                    },
                }];

            // Now write just one block
            region.region_write(&writes, JobId(0), false).await?;
        }

        // Now use region_write to fill entire region
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), true).await?;

        // Because we did write_unwritten, the block we already written should
        // still have the data from the first write.  Update our buffer
        // for these blocks to have that original data.
        for b in blocks_to_write {
            let b_start = b * 512;
            let b_end = b_start + 512;
            for a_buf in buffer.iter_mut().take(b_end).skip(b_start) {
                *a_buf = 9;
            }
        }

        // read all using region_read
        let mut requests: Vec<crucible_protocol::ReadRequest> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            requests.push(crucible_protocol::ReadRequest { eid, offset });
        }

        let responses = region.region_read(&requests, JobId(0)).await?;

        let mut read_from_region: Vec<u8> = Vec::with_capacity(total_size);

        for response in &responses {
            read_from_region.append(&mut response.data.to_vec());
        }

        assert_eq!(buffer, read_from_region);

        Ok(())
    }

    // A test function to return a generic'ish write command.
    // We use the "all 9's data" and checksum.
    fn create_generic_write(
        eid: u64,
        offset: crucible::Block,
    ) -> Vec<crucible_protocol::Write> {
        let data = BytesMut::from(&[9u8; 512][..]);
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];
        writes
    }

    #[tokio::test]
    async fn test_flush_extent_limit_base() {
        // Check that the extent_limit value in region_flush is honored
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(2).await.unwrap();

        // Write to extent 0 block 0 first
        let writes = create_generic_write(0, Block::new_512(0));
        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Now write to extent 1 block 0
        let writes = create_generic_write(1, Block::new_512(0));

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(0).await.dirty().await);
        assert!(region.get_opened_extent(1).await.dirty().await);

        // Call flush, but limit the flush to extent 0
        region
            .region_flush(1, 2, &None, JobId(3), Some(0))
            .await
            .unwrap();

        // Verify the dirty bit is no longer set for 0, but still set
        // for extent 1.
        assert!(!region.get_opened_extent(0).await.dirty().await);
        assert!(region.get_opened_extent(1).await.dirty().await);
    }

    #[tokio::test]
    async fn test_flush_extent_limit_end() {
        // Check that the extent_limit value in region_flush is honored
        // Write to the last block in the extents.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(3).await.unwrap();

        // Write to extent 1 block 9 first
        let writes = create_generic_write(1, Block::new_512(9));
        region.region_write(&writes, JobId(1), true).await.unwrap();

        // Now write to extent 2 block 9
        let writes = create_generic_write(2, Block::new_512(9));
        region.region_write(&writes, JobId(2), true).await.unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(1).await.dirty().await);
        assert!(region.get_opened_extent(2).await.dirty().await);

        // Call flush, but limit the flush to extents < 2
        region
            .region_flush(1, 2, &None, JobId(3), Some(1))
            .await
            .unwrap();

        // Verify the dirty bit is no longer set for 1, but still set
        // for extent 2.
        assert!(!region.get_opened_extent(1).await.dirty().await);
        assert!(region.get_opened_extent(2).await.dirty().await);

        // Now flush with no restrictions.
        region
            .region_flush(1, 2, &None, JobId(3), None)
            .await
            .unwrap();

        // Extent 2 should no longer be dirty
        assert!(!region.get_opened_extent(2).await.dirty().await);
    }

    #[tokio::test]
    async fn test_flush_extent_limit_walk_it_off() {
        // Check that the extent_limit value in region_flush is honored
        // Write to all the extents, then flush them one at a time.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(10).await.unwrap();

        // Write to extents 0 to 9
        let mut job_id = 1;
        for ext in 0..10 {
            let writes = create_generic_write(ext, Block::new_512(5));
            region
                .region_write(&writes, JobId(job_id), true)
                .await
                .unwrap();
            job_id += 1;
        }

        // Verify the dirty bit is now set for all extents.
        for ext in 0..10 {
            assert!(region.get_opened_extent(ext).await.dirty().await);
        }

        // Walk up the extent_limit, verify at each flush extents are
        // flushed.
        for ext in 0..10 {
            println!("Send flush to extent limit {}", ext);
            region
                .region_flush(1, 2, &None, JobId(3), Some(ext))
                .await
                .unwrap();

            // This ext should no longer be dirty.
            println!("extent {} should not be dirty now", ext);
            assert!(!region.get_opened_extent(ext).await.dirty().await);

            // Any extent above the current point should still be dirty.
            for d_ext in ext + 1..10 {
                println!("verify {} still dirty", d_ext);
                assert!(region.get_opened_extent(d_ext).await.dirty().await);
            }
        }
    }

    #[tokio::test]
    async fn test_flush_extent_limit_too_large() {
        // Check that the extent_limit value in region_flush will return
        // an error if the extent_limit is too large.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Call flush with an invalid extent
        assert!(region
            .region_flush(1, 2, &None, JobId(3), Some(2))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_extent_write_flush_close() {
        // Verify that a write then close of an extent will return the
        // expected gen flush and dirty bits for that extent.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Flush extent with eid, fn, gen
        region
            .region_flush_extent(eid as usize, 3, 2, JobId(1))
            .await
            .unwrap();

        // Close extent 0
        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for the write that should be flushed now.
        assert_eq!(gen, 3);
        assert_eq!(flush, 2);
        assert!(!dirty);
    }

    #[tokio::test]
    async fn test_extent_close_reopen_flush_close() {
        // Do several extent open close operations, verifying that the
        // gen/flush/dirty return values are as expected.
        let dir = tempdir().unwrap();
        let mut region = Region::create(&dir, new_region_options(), csl())
            .await
            .unwrap();
        region.extend(1).await.unwrap();

        // Fill a buffer with "9"'s
        let data = BytesMut::from(&[9u8; 512][..]);
        let eid = 0;
        let offset = Block::new_512(0);

        // Write the block
        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid,
                offset,
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 14137680576404864188, // Hash for all 9s
                },
            }];

        region.region_write(&writes, JobId(0), true).await.unwrap();

        // Close extent 0 without a flush
        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Because we did not flush yet, this extent should still have
        // the values for an unwritten extent, except for the dirty bit.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Open the extent, then close it again, the values should remain
        // the same as the previous check (testing here that dirty remains
        // dirty).
        region.reopen_extent(eid as usize).await.unwrap();

        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify everything is the same, and dirty is still set.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Reopen, then flush extent with eid, fn, gen
        region.reopen_extent(eid as usize).await.unwrap();
        region
            .region_flush_extent(eid as usize, 4, 9, JobId(1))
            .await
            .unwrap();

        let (gen, flush, dirty) =
            region.close_extent(eid as usize).await.unwrap();

        // Verify after flush that g,f are updated, and that dirty
        // is no longer set.
        assert_eq!(gen, 4);
        assert_eq!(flush, 9);
        assert!(!dirty);
    }

    #[tokio::test]
    /// We need to make sure that a flush will properly adjust the DB hashes
    /// after issuing multiple writes to different disconnected sections of
    /// an extent
    async fn test_flush_after_multiple_disjoint_writes() -> Result<()> {
        let dir = tempdir()?;
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(1024));
        let mut region =
            Region::create(&dir, region_opts, csl()).await.unwrap();
        region.extend(1).await.unwrap();

        // Write some data to 3 different areas
        let ranges = [(0..120), (243..244), (487..903)];

        // We will write these ranges multiple times so that there's multiple
        // hashes in the DB, so we need multiple sets of data.
        let writes: Vec<Vec<Vec<crucible_protocol::Write>>> = (0..3)
            .map(|_| {
                ranges
                    .iter()
                    .map(|range| {
                        range
                            .clone()
                            .map(|idx| {
                                // Generate data for this block
                                let data = thread_rng().gen::<[u8; 512]>();
                                let hash = integrity_hash(&[&data]);
                                crucible_protocol::Write {
                                    eid: 0,
                                    offset: Block::new_512(idx),
                                    data: Bytes::copy_from_slice(&data),
                                    block_context: BlockContext {
                                        hash,
                                        encryption_context: None,
                                    },
                                }
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect();

        // Write all the writes
        for write_iteration in &writes {
            for write_chunk in write_iteration {
                region.region_write(write_chunk, JobId(0), false).await?;
            }
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).await?;

        // We are gonna compare against the last write iteration
        let last_writes = writes.last().unwrap();

        let ext = region.get_opened_extent(0).await;
        let mut inner = ext.lock().await;

        for (i, range) in ranges.iter().enumerate() {
            // Get the contexts for the range
            let ctxts = inner
                .get_block_contexts(range.start, range.end - range.start)?;

            // Every block should have at most 1 block
            assert_eq!(
                ctxts.iter().map(|block_ctxts| block_ctxts.len()).max(),
                Some(1)
            );

            // Now that we've checked that, flatten out for an easier eq
            let actual_ctxts: Vec<_> = ctxts
                .iter()
                .flatten()
                .map(|downstairs_context| &downstairs_context.block_context)
                .collect();

            // What we expect is the hashes for the last write we did
            let expected_ctxts: Vec<_> = last_writes[i]
                .iter()
                .map(|write| &write.block_context)
                .collect();

            // Check that they're right.
            assert_eq!(expected_ctxts, actual_ctxts);
        }

        Ok(())
    }

    #[tokio::test]
    /// This test ensures that our flush logic works even for full-extent
    /// flushes. That's the case where the set of modified blocks will be full.
    async fn test_big_extent_full_write_and_flush() -> Result<()> {
        let dir = tempdir()?;

        const EXTENT_SIZE: u64 = 4096;
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(EXTENT_SIZE));
        let mut region =
            Region::create(&dir, region_opts, csl()).await.unwrap();
        region.extend(1).await.unwrap();

        // writing the entire region a few times over before the flush.
        let writes: Vec<Vec<crucible_protocol::Write>> = (0..3)
            .map(|_| {
                (0..EXTENT_SIZE)
                    .map(|idx| {
                        // Generate data for this block
                        let data = thread_rng().gen::<[u8; 512]>();
                        let hash = integrity_hash(&[&data]);
                        crucible_protocol::Write {
                            eid: 0,
                            offset: Block::new_512(idx),
                            data: Bytes::copy_from_slice(&data),
                            block_context: BlockContext {
                                hash,
                                encryption_context: None,
                            },
                        }
                    })
                    .collect()
            })
            .collect();

        // Write all the writes
        for write_iteration in &writes {
            region
                .region_write(write_iteration, JobId(0), false)
                .await?;
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).await?;

        // compare against the last write iteration
        let last_writes = writes.last().unwrap();

        let ext = region.get_opened_extent(0).await;
        let mut inner = ext.lock().await;

        // Get the contexts for the range
        let ctxts = inner.get_block_contexts(0, EXTENT_SIZE)?;

        // Every block should have at most 1 block
        assert_eq!(
            ctxts.iter().map(|block_ctxts| block_ctxts.len()).max(),
            Some(1)
        );

        // Now that we've checked that, flatten out for an easier eq
        let actual_ctxts: Vec<_> = ctxts
            .iter()
            .flatten()
            .map(|downstairs_context| &downstairs_context.block_context)
            .collect();

        // What we expect is the hashes for the last write we did
        let expected_ctxts: Vec<_> = last_writes
            .iter()
            .map(|write| &write.block_context)
            .collect();

        // Check that they're right.
        assert_eq!(expected_ctxts, actual_ctxts);

        Ok(())
    }

    #[tokio::test]
    async fn test_bad_hash_bad() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let data = BytesMut::from(&[1u8; 512][..]);

        let writes: Vec<crucible_protocol::Write> =
            vec![crucible_protocol::Write {
                eid: 0,
                offset: Block::new_512(0),
                data: data.freeze(),
                block_context: BlockContext {
                    encryption_context: Some(
                        crucible_protocol::EncryptionContext {
                            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                            tag: [
                                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                17, 18, 19,
                            ],
                        },
                    ),
                    hash: 2398419238764,
                },
            }];

        let result = region.region_write(&writes, JobId(0), false).await;

        assert!(result.is_err());

        match result.err().unwrap() {
            CrucibleError::HashMismatch => {
                // ok
            }
            _ => {
                panic!("Incorrect error with hash mismatch");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_blank_block_read_ok() -> Result<()> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;
        region.extend(1).await?;

        let responses = region
            .region_read(
                &[crucible_protocol::ReadRequest {
                    eid: 0,
                    offset: Block::new_512(0),
                }],
                JobId(0),
            )
            .await?;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].hashes().len(), 0);
        assert_eq!(responses[0].data[..], [0u8; 512][..]);

        Ok(())
    }

    async fn prepare_random_region(
    ) -> Result<(tempfile::TempDir, Region, Vec<u8>)> {
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let ddef = region.def();
        let total_size = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Write in random data
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let eid: u64 = i as u64 / ddef.extent_size().value;
            let offset: Block =
                Block::new_512((i as u64) % ddef.extent_size().value);

            let data = BytesMut::from(&buffer[(i * 512)..((i + 1) * 512)]);
            let data = data.freeze();
            let hash = integrity_hash(&[&data[..]]);

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data,
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        region.region_write(&writes, JobId(0), false).await?;

        Ok((dir, region, buffer))
    }

    #[tokio::test]
    async fn test_read_single_large_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a single large contiguous range
        let requests: Vec<crucible::ReadRequest> = (1..8)
            .map(|i| crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(i),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data[512..(8 * 512)],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_single_large_contiguous_span_extents() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a single large contiguous range that spans
        // multiple extents
        let requests: Vec<crucible::ReadRequest> = (9..28)
            .map(|i| crucible_protocol::ReadRequest {
                eid: i / 10,
                offset: Block::new_512(i % 10),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data[(9 * 512)..(28 * 512)],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_multiple_disjoint_large_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a multiple disjoint large contiguous ranges
        let requests: Vec<crucible::ReadRequest> = vec![
            (1..4)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
            (15..24)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
            (27..28)
                .map(|i| crucible_protocol::ReadRequest {
                    eid: i / 10,
                    offset: Block::new_512(i % 10),
                })
                .collect::<Vec<crucible::ReadRequest>>(),
        ]
        .into_iter()
        .flatten()
        .collect();

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &[
                &data[512..(4 * 512)],
                &data[(15 * 512)..(24 * 512)],
                &data[(27 * 512)..(28 * 512)],
            ]
            .concat(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_multiple_disjoint_none_contiguous() -> Result<()> {
        let (_dir, region, data) = prepare_random_region().await?;

        // Call region_read with a multiple disjoint non-contiguous ranges
        let requests: Vec<crucible::ReadRequest> = vec![
            crucible_protocol::ReadRequest {
                eid: 0,
                offset: Block::new_512(0),
            },
            crucible_protocol::ReadRequest {
                eid: 1,
                offset: Block::new_512(4),
            },
            crucible_protocol::ReadRequest {
                eid: 1,
                offset: Block::new_512(9),
            },
            crucible_protocol::ReadRequest {
                eid: 2,
                offset: Block::new_512(4),
            },
        ];

        let responses = region.region_read(&requests, JobId(0)).await?;

        // Validate returned data
        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &[
                &data[0..512],
                &data[(14 * 512)..(15 * 512)],
                &data[(19 * 512)..(20 * 512)],
                &data[(24 * 512)..(25 * 512)],
            ]
            .concat(),
        );

        Ok(())
    }

    fn prepare_writes(
        offsets: std::ops::Range<usize>,
        data: &mut [u8],
    ) -> Vec<crucible_protocol::Write> {
        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(offsets.len());
        let mut rng = rand::thread_rng();

        for i in offsets {
            let mut buffer: Vec<u8> = vec![0; 512];
            rng.fill_bytes(&mut buffer);

            // alter data as writes are prepared
            data[(i * 512)..((i + 1) * 512)].copy_from_slice(&buffer[..]);

            let hash = integrity_hash(&[&buffer]);

            writes.push(crucible_protocol::Write {
                eid: (i as u64) / 10,
                offset: Block::new_512((i as u64) % 10),
                data: Bytes::from(buffer),
                block_context: BlockContext {
                    encryption_context: None,
                    hash,
                },
            });
        }

        assert!(!writes.is_empty());

        writes
    }

    async fn validate_whole_region(region: &Region, data: &[u8]) -> Result<()> {
        let num_blocks = region.def().extent_size().value
            * region.def().extent_count() as u64;

        let requests: Vec<crucible_protocol::ReadRequest> = (0..num_blocks)
            .map(|i| crucible_protocol::ReadRequest {
                eid: i / 10,
                offset: Block::new_512(i % 10),
            })
            .collect();

        let responses = region.region_read(&requests, JobId(1)).await?;

        assert_eq!(
            &responses
                .iter()
                .flat_map(|resp| resp.data.to_vec())
                .collect::<Vec<u8>>(),
            &data,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_single_large_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a single large contiguous range
        let writes = prepare_writes(1..8, &mut data);

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_single_large_contiguous_span_extents() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = prepare_writes(9..28, &mut data);

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_multiple_disjoint_large_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a multiple disjoint large contiguous ranges
        let writes = [
            prepare_writes(1..4, &mut data),
            prepare_writes(15..24, &mut data),
            prepare_writes(27..28, &mut data),
        ]
        .concat();

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_multiple_disjoint_none_contiguous() -> Result<()> {
        let (_dir, mut region, mut data) = prepare_random_region().await?;

        // Call region_write with a multiple disjoint non-contiguous ranges
        let writes = [
            prepare_writes(0..1, &mut data),
            prepare_writes(14..15, &mut data),
            prepare_writes(19..20, &mut data),
            prepare_writes(24..25, &mut data),
        ]
        .concat();

        region.region_write(&writes, JobId(0), false).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_single_large_contiguous() -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range
        let writes = prepare_writes(1..8, &mut data);

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_single_large_contiguous_span_extents(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = prepare_writes(9..28, &mut data);

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_multiple_disjoint_large_contiguous(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a multiple disjoint large contiguous ranges
        let writes = [
            prepare_writes(1..4, &mut data),
            prepare_writes(15..24, &mut data),
            prepare_writes(27..28, &mut data),
        ]
        .concat();

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }

    #[tokio::test]
    async fn test_write_unwritten_multiple_disjoint_none_contiguous(
    ) -> Result<()> {
        // Create a blank region
        let dir = tempdir()?;
        let mut region =
            Region::create(&dir, new_region_options(), csl()).await?;

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3).await?;

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a multiple disjoint non-contiguous ranges
        let writes = [
            prepare_writes(0..1, &mut data),
            prepare_writes(14..15, &mut data),
            prepare_writes(19..20, &mut data),
            prepare_writes(24..25, &mut data),
        ]
        .concat();

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).await?;

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&region, &data).await
    }
}
