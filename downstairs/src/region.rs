use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::fs::{rename, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use futures::TryStreamExt;

use tracing::instrument;

use crucible_common::*;
use crucible_protocol::SnapshotDetails;
use repair_client::Client;

/// Number of worker threads in the Rayon thread pool
///
/// This is only used when synching extents to disk; we spawn separate Rayon
/// tasks to call `fdsync` on each extent in parallel.  The value is picked
/// somewhat arbitrarily as a number that doesn't seem to affect performance,
/// but is significantly lower than the default of "number of system threads"
/// (i.e. 128 for the Gimlet server sleds).
const WORKER_POOL_SIZE: usize = 8;

/// Size above which reads and writes should be done in the Tokio blocking pool
///
/// This is chosen somewhat arbitrarily.
const MIN_BLOCKING_SIZE: usize = 64 * 1024; // 64 KiB

use super::*;
use crate::extent::{
    copy_dir, extent_dir, extent_file_name, move_replacement_extent,
    replace_dir, sync_path, Extent, ExtentMeta, ExtentState, ExtentType,
};

/// Validate files for a repair or clone operation
///
/// There is only one repair file: the raw file itself (which also contains
/// structured context and metadata at the end).
pub fn validate_repair_files(eid: ExtentId, files: &[String]) -> bool {
    files == [extent_file_name(eid, ExtentType::Data)]
}

/// Validate the possible files during a clone.
///
/// During a clone of a downstairs region, we have one, two or four
/// possible files we expect to see.
pub fn validate_clone_files(eid: ExtentId, files: &[String]) -> bool {
    let one = vec![extent_file_name(eid, ExtentType::Data)];

    let mut some = one.clone();
    some.extend(vec![extent_file_name(eid, ExtentType::Db)]);

    let mut all = some.clone();
    all.extend(vec![
        extent_file_name(eid, ExtentType::DbShm),
        extent_file_name(eid, ExtentType::DbWal),
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
    pub extents: Vec<ExtentState>,

    /// Extents which are dirty and need to be flushed. Should be true if the
    /// dirty flag in the extent's metadata is set. When an extent is opened, if
    /// it's dirty, it's added to here. When a write is issued to an extent,
    /// it's added to here. If the write doesn't actually make the extent dirty
    /// that's fine, because the extent will NOP during the flush anyway, but
    /// this mainly serves to cut down on the extents we're considering for a
    /// flush in the first place.
    dirty_extents: HashSet<ExtentId>,

    read_only: bool,
    log: Logger,

    /// Thread pool for doing long-running CPU work outside the Tokio runtime
    pool: rayon::ThreadPool,
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

        // The downstairs needs to open (at minimum) all the extent files --
        // which is one file per extent for raw extents and up to 4 for
        // SQLite-backed extents -- along with
        //
        // - the seed database (for SQLite, db + shm + wal)
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
    pub fn create<P: AsRef<Path>>(
        dir: P,
        options: RegionOptions,
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

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(WORKER_POOL_SIZE)
            .build()?;

        let region = Region {
            dir: dir.as_ref().to_path_buf(),
            def,
            extents: Vec::new(),
            dirty_extents: HashSet::new(),
            read_only: false,
            log,
            pool,
        };
        assert_eq!(region.def.extent_count(), 0);
        Ok(region)
    }

    /**
     * Open an existing region file
     */
    pub fn open<P: AsRef<Path>>(
        dir: P,
        verbose: bool,
        read_only: bool,
        log: &Logger,
    ) -> Result<Region> {
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

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(WORKER_POOL_SIZE)
            .build()?;

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
            pool,
        };

        region.open_extents()?;

        Ok(region)
    }

    pub fn encrypted(&self) -> bool {
        self.def.get_encrypted()
    }

    fn get_opened_extent_mut(&mut self, eid: ExtentId) -> &mut Extent {
        match &mut self.extents[eid.0 as usize] {
            ExtentState::Opened(extent) => extent,
            ExtentState::Closed => {
                panic!("attempting to get closed extent {}", eid)
            }
        }
    }

    fn get_opened_extent(&self, eid: ExtentId) -> &Extent {
        match &self.extents[eid.0 as usize] {
            ExtentState::Opened(extent) => extent,
            ExtentState::Closed => {
                panic!("attempting to get closed extent {}", eid)
            }
        }
    }

    /// If our `extent_count` is higher than the number of populated entries
    /// we have in our extents Vec, then open all the new extent files and
    /// load their content into the extent Vec.
    ///
    /// Returns an error if extent files are missing.
    fn open_extents(&mut self) -> Result<()> {
        let next_eid = self.extents.len() as u32;

        let eid_range = next_eid..self.def.extent_count();
        for eid in eid_range.map(ExtentId) {
            let extent = Extent::open(
                &self.dir,
                &self.def,
                eid,
                self.read_only,
                &self.log,
            )?;

            if extent.dirty() {
                self.dirty_extents.insert(eid);
            }
            self.extents.push(ExtentState::Opened(extent));
        }
        self.check_extents();

        Ok(())
    }

    /// Creates `self.extent_count` extent files and opens them
    fn create_extents(&mut self, backend: Backend) -> Result<()> {
        let next_eid = self.extents.len() as u32;

        let eid_range = next_eid..self.def.extent_count();

        for eid in eid_range.map(ExtentId) {
            let extent = Extent::create(&self.dir, &self.def, eid, backend)?;
            self.extents.push(ExtentState::Opened(extent));
        }
        self.check_extents();

        Ok(())
    }

    /// Checks that all extents are open and have the correct `number`
    ///
    /// # Panics
    /// If any extent is not open, has an incorrect `number`, or we haven't
    /// opened `self.def.extent_count()` extents.
    fn check_extents(&self) {
        for eid in (0..self.def.extent_count()).map(ExtentId) {
            assert_eq!(self.get_opened_extent(eid).number, eid);
        }
        assert_eq!(self.def.extent_count() as usize, self.extents.len());
    }

    /// Walk the list of extents and close each one.
    ///
    /// If we fail to close an extent, we exit right away, leaving the
    /// remaining extents alone.
    pub fn close_all_extents(&mut self) -> Result<()> {
        for eid in (0..self.def.extent_count()).map(ExtentId) {
            if let Err(e) = self.close_extent(eid) {
                bail!("Failed closing extent {eid} with {e}");
            }
        }

        Ok(())
    }

    /**
     * Walk the list of all extents and find any that are not open.
     * Open any extents that are not.
     */
    pub fn reopen_all_extents(&mut self) -> Result<()> {
        let mut to_open = Vec::new();
        for (i, extent) in self.extents.iter().enumerate() {
            if matches!(extent, ExtentState::Closed) {
                to_open.push(ExtentId(i as u32));
            }
        }

        for eid in to_open {
            self.reopen_extent(eid)?;
        }

        Ok(())
    }

    /**
     * Re open an extent that was previously closed
     */
    pub fn reopen_extent(
        &mut self,
        eid: ExtentId,
    ) -> Result<(), CrucibleError> {
        /*
         * Make sure the extent :
         *
         * - is currently closed
         * - matches our eid
         * - is not read-only
         */
        let mg = &mut self.extents[eid.0 as usize];
        assert!(matches!(mg, ExtentState::Closed));
        assert!(!self.read_only);

        let new_extent =
            Extent::open(&self.dir, &self.def, eid, self.read_only, &self.log)?;

        if new_extent.dirty() {
            self.dirty_extents.insert(eid);
        }

        *mg = ExtentState::Opened(new_extent);

        Ok(())
    }

    pub fn close_extent(
        &mut self,
        eid: ExtentId,
    ) -> Result<(u64, u64, bool), CrucibleError> {
        let extent_state = &mut self.extents[eid.0 as usize];

        let open_extent = std::mem::replace(extent_state, ExtentState::Closed);

        match open_extent {
            ExtentState::Opened(extent) => extent.close(),

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
     *  2. Get all extent files from source side, put in 012.copy directory.
     *     Verify after the copy completes that the remote side still has
     *     the extent closed (or the region is read only).
     *  3. fsync files we just downloaded
     *  4. Rename 012.copy dir to 012.replace dir
     *  5. fsync extent directory ( 00/000/ where the extent files live)
     *  6. Replace current extent 012 files with copied files of same name
     *     from 012.replace dir
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
        client: reqwest::Client,
        eid: ExtentId,
        repair_addr: SocketAddr,
        clone: bool,
    ) -> Result<(), CrucibleError> {
        // Make sure the extent:
        // is currently closed, matches our eid, is not read-only
        assert!(matches!(self.extents[eid.0 as usize], ExtentState::Closed));

        // If this is not a clone, then we must not be read_only
        if !clone {
            assert!(!self.read_only);
        }

        self.get_extent_copy(client, eid, repair_addr, clone)
            .await?;

        // Returning from get_extent_copy means we have copied all our
        // files and moved the copy directory to replace directory.
        // Now, replace the current extent files with the replacement ones.
        move_replacement_extent(&self.dir, eid, clone, &self.log)?;

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
        client: reqwest::Client,
        eid: ExtentId,
        repair_addr: SocketAddr,
        clone: bool,
    ) -> Result<(), CrucibleError> {
        // An extent must be closed before we replace its files.
        assert!(matches!(self.extents[eid.0 as usize], ExtentState::Closed));

        // Make sure copy, replace, and cleanup directories don't exist yet.
        // We don't need them yet, but if they do exist, then something
        // is wrong.
        let rd = replace_dir(&self.dir, eid);
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
        let repair_server = Client::new_with_client(&url, client);

        let mut repair_files =
            match repair_server.get_files_for_extent(eid.0).await {
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
        //
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
            let filename = extent_file_name(eid, *opt_file);

            if !repair_files.contains(&filename) {
                continue;
            }
            let local_file =
                Self::create_copy_file(copy_dir.clone(), eid, *opt_file)?;
            let repair_stream = match repair_server
                .get_extent_file(eid.0, opt_file.to_file_type())
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

        // We have copied over the extent.  Verify that the remote side
        // still believes that it is valid for repair so we know we have
        // received a valid copy.
        info!(self.log, "Verify extent {eid} still ready for copy");
        let rc = match repair_server.extent_repair_ready(eid.0).await {
            Ok(rc) => rc.into_inner(),
            Err(e) => {
                crucible_bail!(
                    RepairRequestError,
                    "Failed to verify extent is still valid for repair {:?}",
                    e,
                );
            }
        };

        if !rc {
            warn!(self.log, "The repair of {eid} is being aborted.");
            crucible_bail!(
                RepairRequestError,
                "Extent {eid} on remote repair server is in incorrect state",
            );
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
        let current_dir = extent_dir(&self.dir, eid);

        sync_path(current_dir, &self.log)?;
        Ok(())
    }

    /**
     * if there is a difference between what our actual extent_count is
     * and what is requested, go out and create the new extent files.
     */
    pub fn extend(&mut self, newsize: u32, backend: Backend) -> Result<()> {
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
            self.create_extents(backend)?;
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

    pub fn meta_info(&self) -> Result<Vec<ExtentMeta>> {
        let mut result = Vec::with_capacity(self.extents.len());
        for eid in (0..self.extents.len()).map(|i| ExtentId(i as u32)) {
            let extent = self.get_opened_extent(eid);
            result.push(extent.get_meta_info())
        }
        Ok(result)
    }

    /// Checks that the hashes are valid for all of the input writes
    ///
    /// # Panics
    /// If any write is structurally invalid, i.e. having a different number of
    /// blocks and block contexts.
    fn validate_hashes(
        &self,
        write: &RegionWrite,
    ) -> Result<(), CrucibleError> {
        let block_size = self.def().block_size() as usize;
        for req in write.iter() {
            // TODO do some of `check_input` here instead of panicking?
            let w = &req.write;
            if w.data.len() / block_size != w.block_contexts.len() {
                panic!("invalid write; block count must match context count");
            }
            for (block, ctx) in w.data.chunks(block_size).zip(&w.block_contexts)
            {
                let computed_hash =
                    if let Some(encryption_context) = &ctx.encryption_context {
                        integrity_hash(&[
                            &encryption_context.nonce[..],
                            &encryption_context.tag[..],
                            &block,
                        ])
                    } else {
                        integrity_hash(&[&block])
                    };

                if computed_hash != ctx.hash {
                    error!(self.log, "Failed write hash validation");
                    // TODO: print out the extent and block where this failed!!
                    crucible_bail!(HashMismatch);
                }
            }
        }

        Ok(())
    }

    #[instrument]
    pub fn region_write(
        &mut self,
        writes: &RegionWrite,
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

        if only_write_unwritten {
            cdt::os__writeunwritten__start!(|| job_id.0);
        } else {
            cdt::os__write__start!(|| job_id.0);
        }
        for req in writes.iter() {
            // Mark any extents we sent a write-command to as potentially dirty
            self.dirty_extents.insert(req.extent);

            let extent = self.get_opened_extent_mut(req.extent);
            if req.write.data.len() > MIN_BLOCKING_SIZE {
                run_blocking(|| {
                    extent.write(job_id, &req.write, only_write_unwritten)
                })
            } else {
                extent.write(job_id, &req.write, only_write_unwritten)
            }?;
        }

        if only_write_unwritten {
            cdt::os__writeunwritten__done!(|| job_id.0);
        } else {
            cdt::os__write__done!(|| job_id.0);
        }

        Ok(())
    }

    #[instrument]
    pub fn region_read(
        &mut self,
        req: &RegionReadRequest,
        job_id: JobId,
    ) -> Result<RegionReadResponse, CrucibleError> {
        let mut response = RegionReadResponse::with_capacity(
            req.iter().map(|req| req.count.get()).sum(),
            self.def.block_size(),
        );

        cdt::os__read__start!(|| job_id.0);
        for req in req.iter() {
            let extent = req.extent;
            let req = response.request(req.offset, req.count.get(), &self.def);
            let extent = self.get_opened_extent_mut(extent);

            // Run sufficiently large reads in the blocking pool
            let out = if req.data.capacity() > MIN_BLOCKING_SIZE {
                run_blocking(|| extent.read(job_id, req))
            } else {
                extent.read(job_id, req)
            }?;

            // Note that we only call `unsplit` here if `Extent::read` returned
            // `Ok(..)` (indicating that the data is fully populated); this
            // means that the preconditions for `RegionReadResponse::unsplit`
            // are met and it will not panic.
            response.unsplit(out);
        }
        cdt::os__read__done!(|| job_id.0);

        Ok(response)
    }

    /*
     * Send a flush to just the given extent. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub(crate) fn region_flush_extent<
        I: Into<JobOrReconciliationId> + Debug,
    >(
        &mut self,
        eid: ExtentId,
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

        let log = self.log.clone();
        let extent = self.get_opened_extent_mut(eid);
        extent.flush(flush_number, gen_number, job_id, &log)?;

        Ok(())
    }

    /*
     * Send a flush to all extents. The provided flush number is
     * what an extent should use if a flush is required.
     */
    #[instrument]
    pub fn region_flush(
        &mut self,
        flush_number: u64,
        gen_number: u64,
        snapshot_details: &Option<SnapshotDetails>,
        job_id: JobId,
        extent_limit: Option<ExtentId>,
    ) -> Result<(), CrucibleError> {
        // It should be ok to Flush a read-only region, but not take a snapshot.
        // Most likely this read-only region *is* a snapshot, so that's
        // redundant :)
        if self.read_only && snapshot_details.is_some() {
            crucible_bail!(ModifyingReadOnlyRegion);
        }

        cdt::os__flush__start!(|| job_id.0);

        // Select extents we're going to flush, while respecting the
        // extent_limit if one was provided.  This must be ordered, because
        // we're going to walk through the extent slice.
        let dirty_extents: BTreeSet<ExtentId> = match extent_limit {
            None => self.dirty_extents.iter().copied().collect(),
            Some(el) => {
                if el > ExtentId::from(self.def.extent_count()) {
                    crucible_bail!(InvalidExtent);
                }

                self.dirty_extents
                    .iter()
                    .copied()
                    .filter(|x| *x <= el)
                    .collect()
            }
        };

        // This is a bit sneaky: we want to perform each flush in a separate
        // task for *parallelism*, but can't call `self.get_opened_extent_mut`
        // multiple times.  In addition, we can't use the tokio thread pool to
        // spawn a task, because that requires a 'static lifetime, which we
        // can't get from a borrowed Extent.
        //
        // We'll combine two tricks to work around the issue:
        // - Do the work in the rayon thread pool instead of using tokio tasks
        // - Carefully walk self.extents.as_mut_slice() to mutably borrow
        //   multiple at the same time.
        let mut results = vec![Ok(()); dirty_extents.len()];
        let log = self.log.clone();
        run_blocking(|| {
            let mut slice_start = 0;
            let mut slice = self.extents.as_mut_slice();
            self.pool.scope(|s| {
                for (eid, r) in dirty_extents.iter().zip(results.iter_mut()) {
                    let next = eid.0 - slice_start;
                    slice = &mut slice[next as usize..];
                    let (extent, rest) = slice.split_first_mut().unwrap();
                    let ExtentState::Opened(extent) = extent else {
                        panic!("can't flush closed extent");
                    };
                    slice = rest;
                    slice_start += next + 1;
                    let log = log.clone();
                    s.spawn(move |_| {
                        *r =
                            extent.flush(flush_number, gen_number, job_id, &log)
                    });
                }
            })
        });

        cdt::os__flush__done!(|| job_id.0);

        for result in results {
            // If any extent flush failed, then return that as an error. Because
            // the results were all collected above, each extent flush has
            // completed at this point.
            result?;
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
        eid: ExtentId,
    ) -> Result<PathBuf, CrucibleError> {
        let cp = copy_dir(dir, eid);

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
        eid: ExtentId,
        extent_type: ExtentType,
    ) -> Result<File> {
        // Get the base extent name before we consider the actual Type
        let name = extent_file_name(eid, ExtentType::Data);
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

    pub fn read_only(&self) -> bool {
        self.read_only
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

pub fn config_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut out = dir.as_ref().to_path_buf();
    out.push("region.json");
    out
}

#[cfg(test)]
pub(crate) mod test {
    use bytes::Bytes;
    use itertools::Itertools;
    use std::fs::rename;
    use std::path::PathBuf;

    use rand::RngCore;
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

    fn region_create_drop_open(backend: Backend) {
        // Create a region, make three extents.
        // Drop the region, then open it.
        let dir = tempdir().unwrap();
        let log = csl();
        let mut region =
            Region::create(&dir, new_region_options(), log.clone()).unwrap();
        region.extend(3, backend).unwrap();

        drop(region);

        let _region = Region::open(&dir, true, false, &log).unwrap();
    }

    #[test]
    fn region_bad_database_read_version_low() -> Result<()> {
        // Create a region where the read database version is down rev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());
        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(0, DATABASE_WRITE_VERSION);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, true, false, &csl()).unwrap_err();

        Ok(())
    }

    #[test]
    fn region_bad_database_write_version_low() -> Result<()> {
        // Create a region where the write database version is downrev.
        let dir = tempdir()?;
        let cp = config_path(dir.as_ref());
        assert!(!Path::new(&cp).exists());

        mkdir_for_file(&cp)?;

        let def = RegionDefinition::test_default(DATABASE_READ_VERSION, 0);
        write_json(&cp, &def, false)?;

        // Verify that the open returns an error
        Region::open(&dir, true, false, &csl()).unwrap_err();

        Ok(())
    }

    #[test]
    fn region_bad_database_read_version_high() -> Result<()> {
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
        Region::open(&dir, true, false, &csl()).unwrap_err();

        Ok(())
    }

    #[test]
    fn region_bad_database_write_version_high() -> Result<()> {
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
        Region::open(&dir, true, false, &csl()).unwrap_err();

        Ok(())
    }

    fn copy_extent_dir(backend: Backend) {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Remove the copy directory, make sure it goes away.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        let cp = copy_dir(&dir, ExtentId(1));

        assert!(Region::create_copy_dir(&dir, ExtentId(1)).is_ok());
        assert!(Path::new(&cp).exists());
        assert!(remove_copy_cleanup_dir(&dir, ExtentId(1)).is_ok());
        assert!(!Path::new(&cp).exists());
    }

    fn copy_extent_dir_twice(backend: Backend) {
        // Create the region, make three extents
        // Create the copy directory, make sure it exists.
        // Verify a second create will fail.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        Region::create_copy_dir(&dir, ExtentId(1)).unwrap();
        let res = Region::create_copy_dir(&dir, ExtentId(1));
        assert!(res.is_err());
    }

    fn close_extent(backend: Backend) {
        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Close extent 1
        let (gen, flush, dirty) = region.close_extent(ExtentId(1)).unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for a new unwritten extent.
        assert!(matches!(region.extents[1], ExtentState::Closed));
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(!dirty);

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();
        // Reopen extent 1
        region.reopen_extent(ExtentId(1)).unwrap();

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure the eid matches
        assert_eq!(ext_one.number, ExtentId(1));

        // Make sure the copy directory is gone
        assert!(!Path::new(&cp).exists());
    }

    fn reopen_extent_cleanup_one(backend: Backend) {
        // Verify the copy directory is removed if an extent is
        // opened with that directory present.
        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Close extent 1
        region.close_extent(ExtentId(1)).unwrap();
        assert!(matches!(region.extents[1], ExtentState::Closed));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();

        // Reopen extent 1
        region.reopen_extent(ExtentId(1)).unwrap();

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure the eid matches
        assert_eq!(ext_one.number, ExtentId(1));

        // Make sure copy directory was removed
        assert!(!Path::new(&cp).exists());
    }

    fn reopen_extent_cleanup_two(backend: Backend) {
        // Verify that the completed directory is removed if present
        // when an extent is re-opened.
        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Close extent 1
        region.close_extent(ExtentId(1)).unwrap();
        assert!(matches!(region.extents[1], ExtentState::Closed));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();

        // Step through the replacement dir, but don't do any work.
        let rd = replace_dir(&dir, ExtentId(1));
        rename(cp.clone(), rd.clone()).unwrap();

        // Finish up the fake repair, but leave behind the completed dir.
        let cd = completed_dir(&dir, ExtentId(1));
        rename(rd.clone(), cd.clone()).unwrap();

        // Reopen extent 1
        region.reopen_extent(ExtentId(1)).unwrap();

        // Verify extent one is valid
        let _ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());
        assert!(!Path::new(&cd).exists());
    }

    fn reopen_extent_cleanup_replay(backend: Backend) {
        // Verify on extent open that a replacement directory will
        // have it's contents replace an extents existing data and
        // metadata files.
        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Close extent 1
        region.close_extent(ExtentId(1)).unwrap();
        assert!(matches!(region.extents[1], ExtentState::Closed));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(ExtentId(1), ExtentType::Data);
        let mut source_path = extent_path(&dir, ExtentId(0));
        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();

        if backend == Backend::SQLite {
            source_path.set_extension("db");
            dest_path.set_extension("db");
            std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();

            source_path.set_extension("db-shm");
            dest_path.set_extension("db-shm");
            std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();

            source_path.set_extension("db-wal");
            dest_path.set_extension("db-wal");
            std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();
        }

        let rd = replace_dir(&dir, ExtentId(1));
        rename(cp.clone(), rd.clone()).unwrap();

        // Now we have a replace directory, we verify that special
        // action is taken when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(ExtentId(1)).unwrap();

        let _ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, ExtentId(1));
        assert!(!Path::new(&cd).exists());
    }

    fn reopen_extent_cleanup_replay_short(backend: Backend) {
        // test move_replacement_extent(), create a copy dir, populate it
        // and let the reopen do the work.  This time we make sure our
        // copy dir only has the extent data file.

        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Close extent 1
        region.close_extent(ExtentId(1)).unwrap();
        assert!(matches!(region.extents[1], ExtentState::Closed));

        // Make copy directory for this extent
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(ExtentId(1), ExtentType::Data);
        let mut source_path = extent_path(&dir, ExtentId(0));

        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        println!("cp {:?} to {:?}", source_path, dest_path);
        std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();

        if backend == Backend::SQLite {
            source_path.set_extension("db");
            dest_path.set_extension("db");
            std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();
        }

        let rd = replace_dir(&dir, ExtentId(1));
        rename(cp.clone(), rd.clone()).unwrap();

        if backend == Backend::SQLite {
            // The close may remove the db-shm and db-wal files, manually
            // create them here, just to verify they are removed after the
            // reopen as they are not included in the files to be recovered
            // and this test exists to verify they will be deleted.
            let mut invalid_db = extent_path(&dir, ExtentId(1));
            invalid_db.set_extension("db-shm");
            println!("Recreate {:?}", invalid_db);
            std::fs::copy(source_path.clone(), invalid_db.clone()).unwrap();
            assert!(Path::new(&invalid_db).exists());

            invalid_db.set_extension("db-wal");
            println!("Recreate {:?}", invalid_db);
            std::fs::copy(source_path.clone(), invalid_db.clone()).unwrap();
            assert!(Path::new(&invalid_db).exists());
        }

        // Now we have a replace directory populated and our files to
        // delete are ready.  We verify that special action is taken
        // when we (re)open the extent.

        // Reopen extent 1
        region.reopen_extent(ExtentId(1)).unwrap();

        let _ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure all repair directories are gone
        assert!(!Path::new(&cp).exists());
        assert!(!Path::new(&rd).exists());

        // The reopen should have replayed the repair, renamed, then
        // deleted this directory.
        let cd = completed_dir(&dir, ExtentId(1));
        assert!(!Path::new(&cd).exists());
    }

    fn reopen_extent_no_replay_readonly(backend: Backend) {
        // Verify on a read-only region a replacement directory will
        // be ignored.  This is required by the dump command, as it would
        // be tragic if the command to inspect a region changed that
        // region's contents in the act of inspecting.

        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Make copy directory for this extent
        let _ext_one = region.get_opened_extent(ExtentId(1));
        let cp = Region::create_copy_dir(&dir, ExtentId(1)).unwrap();

        // We are simulating the copy of files from the "source" repair
        // extent by copying the files from extent zero into the copy
        // directory.
        let dest_name = extent_file_name(ExtentId(1), ExtentType::Data);
        let mut source_path = extent_path(&dir, ExtentId(0));

        let mut dest_path = cp.clone();
        dest_path.push(dest_name);
        std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();

        if backend == Backend::SQLite {
            source_path.set_extension("db");
            dest_path.set_extension("db");
            std::fs::copy(source_path.clone(), dest_path.clone()).unwrap();
        }

        let rd = replace_dir(&dir, ExtentId(1));
        rename(cp, rd.clone()).unwrap();

        drop(region);

        // Open up the region read_only now.
        let region = Region::open(&dir, false, true, &csl()).unwrap();

        // Verify extent 1 has opened again.
        let _ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure repair directory is still present
        assert!(Path::new(&rd).exists());
    }

    #[tokio::test]
    async fn reopen_extent_partial_migration() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::SQLite)?;
        let ddef = region.def();

        region.region_write(
            &RegionWrite(vec![
                RegionWriteReq {
                    extent: ExtentId(1),
                    write: ExtentWrite {
                        offset: BlockOffset(0),
                        data: Bytes::from(vec![1u8; 512]),
                        block_contexts: vec![BlockContext {
                            encryption_context: None,
                            hash: 8717892996238908351, // hash for all 1s
                        }],
                    },
                },
                RegionWriteReq {
                    extent: ExtentId(2),
                    write: ExtentWrite {
                        offset: BlockOffset(0),
                        data: Bytes::from(vec![2u8; 512]),
                        block_contexts: vec![BlockContext {
                            encryption_context: None,
                            hash: 2192425179333611943, // hash for all 2s
                        }],
                    },
                },
            ]),
            JobId(0),
            false,
        )?;
        drop(region);

        // Manually calculate the migration from extent 1
        let extent_file = extent_path(&dir, ExtentId(1));
        let mut inner = extent_inner_sqlite::SqliteInner::open(
            dir.path(),
            &ddef,
            ExtentId(1),
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

        let mut region = Region::open(&dir, true, false, &log)?;
        let out = region.region_read(
            &RegionReadRequest(vec![
                RegionReadReq {
                    extent: ExtentId(1),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                },
                RegionReadReq {
                    extent: ExtentId(2),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                },
            ]),
            JobId(0),
        )?;
        assert_eq!(&out.data[..512], [1; 512]);
        assert_eq!(&out.data[512..], [2; 512]);

        Ok(())
    }

    #[test]
    fn reopen_extent_partial_migration_corrupt() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::SQLite)?;
        let ddef = region.def();

        // Make some writes, which we'll check after migration
        region.region_write(
            &RegionWrite(vec![
                RegionWriteReq {
                    extent: ExtentId(1),
                    write: ExtentWrite {
                        offset: BlockOffset(0),
                        data: Bytes::from(vec![1u8; 512]),
                        block_contexts: vec![BlockContext {
                            encryption_context: None,
                            hash: 8717892996238908351, // hash for all 1s
                        }],
                    },
                },
                RegionWriteReq {
                    extent: ExtentId(2),
                    write: ExtentWrite {
                        offset: BlockOffset(0),
                        data: Bytes::from(vec![2u8; 512]),
                        block_contexts: vec![BlockContext {
                            encryption_context: None,
                            hash: 2192425179333611943, // hash for all 2s
                        }],
                    },
                },
            ]),
            JobId(0),
            false,
        )?;
        drop(region);

        // Manually calculate the migration from extent 1, but deliberately mess
        // with the context values (simulating a migration that didn't manage to
        // write everything to disk).
        let extent_file = extent_path(&dir, ExtentId(1));
        let mut inner = extent_inner_sqlite::SqliteInner::open(
            dir.path(),
            &ddef,
            ExtentId(1),
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
            .open(extent_file)?;
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

        let mut region = Region::open(&dir, true, false, &log)?;
        let out = region.region_read(
            &RegionReadRequest(vec![
                RegionReadReq {
                    extent: ExtentId(1),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                },
                RegionReadReq {
                    extent: ExtentId(2),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                },
            ]),
            JobId(0),
        )?;
        assert_eq!(&out.data[..512], [1; 512]);
        assert_eq!(&out.data[512..], [2; 512]);

        Ok(())
    }

    // wrapper to send to sub-tests for true/false here
    #[test]
    fn validate_repair_files_empty() {
        // No repair files is a failure
        assert!(!validate_repair_files(ExtentId(1), &Vec::new()));
        assert!(!validate_clone_files(ExtentId(1), &Vec::new()));
    }

    #[test]
    fn validate_repair_files_good() {
        // This is an expected list of files for an extent
        let good_files: Vec<String> = vec!["001".to_string()];

        assert!(validate_repair_files(ExtentId(1), &good_files));
        assert!(validate_clone_files(ExtentId(1), &good_files));
    }

    #[test]
    fn validate_repair_files_duplicate() {
        // duplicate file names for extent 2
        let good_files: Vec<String> =
            vec!["002".to_string(), "002".to_string()];
        assert!(!validate_repair_files(ExtentId(2), &good_files));
        assert!(!validate_clone_files(ExtentId(2), &good_files));
    }

    #[test]
    fn validate_repair_files_offbyon_data() {
        // Incorrect file names for extent 2
        let good_files: Vec<String> = vec!["001".to_string()];
        assert!(!validate_repair_files(ExtentId(2), &good_files));
        assert!(!validate_clone_files(ExtentId(2), &good_files));
    }

    #[test]
    fn validate_repair_files_db() {
        // db file only exists on replacement
        let good_files: Vec<String> =
            vec!["001".to_string(), "001.db".to_string()];

        assert!(!validate_repair_files(ExtentId(1), &good_files));
        // Valid for replacement
        assert!(validate_clone_files(ExtentId(1), &good_files));
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

        assert!(!validate_repair_files(ExtentId(1), &many_files));
        assert!(validate_clone_files(ExtentId(1), &many_files));
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
        assert!(!validate_repair_files(ExtentId(2), &good_files));
        assert!(!validate_clone_files(ExtentId(2), &good_files));
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
        assert!(!validate_repair_files(ExtentId(1), &good_files));
        assert!(!validate_clone_files(ExtentId(1), &good_files));
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
        assert!(!validate_repair_files(ExtentId(2), &good_files));
        assert!(!validate_clone_files(ExtentId(2), &good_files));
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
        assert!(!validate_repair_files(ExtentId(1), &good_files));
        assert!(!validate_clone_files(ExtentId(1), &good_files));
    }

    #[test]
    fn validate_repair_files_not_good_enough() {
        // Replacement requires 1, 2 or 4 files, not 3
        let good_files: Vec<String> = vec![
            "001".to_string(),
            "001.db".to_string(),
            "001.db-wal".to_string(),
        ];
        assert!(!validate_repair_files(ExtentId(1), &good_files));
        assert!(!validate_clone_files(ExtentId(1), &good_files));
    }

    fn reopen_all_extents(backend: Backend) {
        // Create the region, make three extents
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(5, backend).unwrap();

        // Close extent 1
        region.close_extent(ExtentId(1)).unwrap();
        assert!(matches!(region.extents[1], ExtentState::Closed));

        // Close extent 4
        region.close_extent(ExtentId(4)).unwrap();
        assert!(matches!(region.extents[4], ExtentState::Closed));

        // Reopen all extents
        region.reopen_all_extents().unwrap();

        // Verify extent one is valid
        let ext_one = region.get_opened_extent(ExtentId(1));

        // Make sure the eid matches
        assert_eq!(ext_one.number, ExtentId(1));

        // Verify extent four is valid
        let ext_four = region.get_opened_extent(ExtentId(4));

        // Make sure the eid matches
        assert_eq!(ext_four.number, ExtentId(4));
    }

    fn new_region(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut r = Region::create(&dir, new_region_options(), csl()).unwrap();
        r.extend(10, backend).unwrap();
    }

    fn new_existing_region(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut r = Region::create(&dir, new_region_options(), csl()).unwrap();
        r.extend(10, backend).unwrap();
        let _ = Region::open(&dir, false, false, &csl());
    }

    #[test]
    #[should_panic]
    fn bad_import_region() {
        let _ = Region::open(
            "/tmp/12345678-1111-2222-3333-123456789999/notadir",
            false,
            false,
            &csl(),
        )
        .unwrap();
    }

    #[test]
    fn copy_path_basic() {
        assert_eq!(
            copy_dir("/var/region", ExtentId(4)),
            p("/var/region/00/000/004.copy")
        );
    }

    fn dump_a_region(backend: Backend) {
        /*
         * Create a region, give it actual size
         */
        let dir = tempdir().unwrap();
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        r1.extend(2, backend).unwrap();

        /*
         * Build the Vec for our region dir
         */
        let dvec = vec![dir.into_path()];

        /*
         * Dump the region
         */
        dump_region(dvec, None, None, false, false, csl()).unwrap();
    }

    fn dump_two_region(backend: Backend) {
        /*
         * Create our temp dirs
         */
        let dir = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        let mut r2 =
            Region::create(&dir2, new_region_options(), csl()).unwrap();
        r1.extend(2, backend).unwrap();
        r2.extend(2, backend).unwrap();

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
        dump_region(dvec, None, None, false, false, csl()).unwrap();
    }

    fn dump_extent(backend: Backend) {
        /*
         * Create our temp dirs
         */
        let dir = tempdir().unwrap();
        let dir2 = tempdir().unwrap();

        /*
         * Create the regions, give them some actual size
         */
        let mut r1 = Region::create(&dir, new_region_options(), csl()).unwrap();
        r1.extend(3, backend).unwrap();
        let mut r2 =
            Region::create(&dir2, new_region_options(), csl()).unwrap();
        r2.extend(3, backend).unwrap();

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
        dump_region(dvec, Some(ExtentId(2)), None, false, false, csl())
            .unwrap();
    }

    /// Read block data from raw files on disk
    fn read_file_data(
        ddef: RegionDefinition,
        dir: &Path,
        backend: Backend,
    ) -> Vec<u8> {
        let mut out = vec![];
        let extent_data_size =
            (ddef.extent_size().value * ddef.block_size()) as usize;
        for i in (0..ddef.extent_count()).map(ExtentId) {
            match backend {
                Backend::RawFile | Backend::SQLite => {
                    let path = extent_path(dir, i);
                    let data =
                        std::fs::read(path).expect("Unable to read file");

                    out.extend(&data[..extent_data_size]);
                }
            }
        }
        out
    }

    fn region_write_all(
        region: &mut Region,
        ddef: &RegionDefinition,
        only_write_unwritten: bool,
    ) -> Vec<u8> {
        let total_size: usize = ddef.total_size() as usize;
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes = vec![];

        let bytes_per_extent = ddef.extent_size().value * ddef.block_size();
        for eid in (0..ddef.extent_count()).map(ExtentId) {
            let offset = BlockOffset(0);

            let data = Bytes::from(
                buffer[(eid.0 as u64 * bytes_per_extent) as usize..]
                    [..bytes_per_extent as usize]
                    .to_vec(),
            );

            let block_contexts = data
                .chunks(512)
                .map(|chunk| BlockContext {
                    encryption_context: None,
                    hash: integrity_hash(&[chunk]),
                })
                .collect();

            writes.push(RegionWriteReq {
                extent: eid,
                write: ExtentWrite {
                    offset,
                    data,
                    block_contexts,
                },
            });
        }

        region
            .region_write(&RegionWrite(writes), JobId(0), only_write_unwritten)
            .unwrap();
        buffer
    }

    fn test_big_write(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        let ddef = region.def();
        let num_blocks = ddef.extent_size().value * ddef.extent_count() as u64;

        // use region_write_all to fill the entire region
        let buffer = region_write_all(&mut region, &ddef, false);

        // read data into File, compare what was written to buffer
        let read_from_files = read_file_data(ddef, dir.path(), backend);

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer.len(), responses.data.len());
        assert_eq!(buffer, responses.data);
    }

    #[test]
    fn test_big_write_migrate() -> Result<()> {
        let log = csl();
        let dir = tempdir()?;
        let mut region = Region::create(&dir, new_region_options(), csl())?;
        region.extend(3, Backend::SQLite)?;

        let ddef = region.def();
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // use region_write to fill region
        let buffer = region_write_all(&mut region, &ddef, false);

        for i in (0..3).map(ExtentId) {
            region.region_flush_extent(i, 10, 15, JobId(21))?;
        }
        let meta = region.get_opened_extent(ExtentId(0)).get_meta_info();
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);
        drop(region);

        // Open the region as read-only, which doesn't trigger a migration
        let mut region = Region::open(&dir, true, true, &log)?;
        let meta = region.get_opened_extent(ExtentId(0)).get_meta_info();
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);

        // Assert that the .db files still exist
        for i in (0..3).map(ExtentId) {
            assert!(extent_dir(&dir, i)
                .join(extent_file_name(i, ExtentType::Db))
                .exists());
        }

        // read all using region_read
        let req =
            RegionReadRequest::new(BlockIndex(0), num_blocks as u64, &ddef);
        let read_from_region = region.region_read(&req, JobId(0))?.data;

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);
        drop(region);

        // Open the region as read-write, which **does** trigger a migration
        let mut region = Region::open(&dir, true, false, &log)?;
        let meta = region.get_opened_extent(ExtentId(0)).get_meta_info();
        assert_eq!(meta.gen_number, 10);
        assert_eq!(meta.flush_number, 15);

        // Assert that the .db files have been deleted during the migration
        for i in (0..3).map(ExtentId) {
            assert!(!extent_dir(&dir, i)
                .join(extent_file_name(i, ExtentType::Db))
                .exists());
        }
        let read_from_region = region.region_read(&req, JobId(0))?.data;

        assert_eq!(buffer.len(), read_from_region.len());
        assert_eq!(buffer, read_from_region);
        drop(region);

        Ok(())
    }

    fn test_region_open_removes_partial_writes(backend: Backend) {
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

        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // A write of some sort only wrote a block context row and dirty flag
        {
            let ext = region.get_opened_extent_mut(ExtentId(0));
            ext.set_dirty_and_block_context(&DownstairsBlockContext {
                block_context: BlockContext {
                    encryption_context: None,
                    hash: 1024,
                },
                block: 0,
                on_disk_hash: 65536,
            })
            .unwrap();
        }

        // This should clear out the invalid contexts
        for eid in (0..region.extents.len()).map(|e| ExtentId(e as u32)) {
            region.close_extent(eid).unwrap();
        }

        region.reopen_all_extents().unwrap();

        // Verify no block context rows exist
        {
            let ext = region.get_opened_extent_mut(ExtentId(0));
            assert!(ext.get_block_contexts(0, 1).unwrap()[0].is_none());
        }

        // Assert write unwritten will write to the first block

        let data = Bytes::from(vec![0x55; 512]);
        let hash = integrity_hash(&[&data[..]]);

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq {
                    extent: ExtentId(0),
                    write: ExtentWrite {
                        offset: BlockOffset(0),
                        data,
                        block_contexts: vec![BlockContext {
                            encryption_context: None,
                            hash,
                        }],
                    },
                }]),
                JobId(124),
                true, // only_write_unwritten
            )
            .unwrap();

        let responses = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: ExtentId(0),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(125),
            )
            .unwrap();

        assert_eq!(responses.data, vec![0x55; 512]);
    }

    fn test_ok_hash_ok(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        let data = Bytes::from(vec![1u8; 512]);

        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 9163319254371683066,
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq {
                    extent: ExtentId(0),
                    write,
                }]),
                JobId(0),
                false,
            )
            .unwrap();
    }

    fn test_write_unwritten_when_empty(backend: Backend) {
        // Verify that a read fill does write to a block when there is
        // no data written yet.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Fill a buffer with "9"'s (random)
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9's
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                true,
            )
            .unwrap();

        // Verify the dirty bit is now set.
        // We know our EID, so we can shortcut to getting the actual extent.
        assert!(region.get_opened_extent(eid).dirty());

        // Now read back that block, make sure it is updated.
        let responses = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: eid,
                    offset,
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(0),
            )
            .unwrap();

        assert_eq!(responses.blocks.len(), 1);
        assert!(responses.hashes(0).is_none());
        assert!(responses.encryption_contexts(0).is_some());
        assert_eq!(responses.data[..], [9u8; 512][..]);
    }

    fn test_write_unwritten_when_written(backend: Backend) {
        // Verify that a read fill does not write to the block when
        // there is data written already.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Fill a buffer with "9"'s (random)
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        // Write the block
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9's
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                false,
            )
            .unwrap();

        // Same block, now try to write something else to it.
        let data = Bytes::from(vec![1u8; 512]);
        let ctx = crucible_protocol::EncryptionContext {
            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            tag: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        };
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(ctx),
                hash: 9163319254371683066, // hash for all 1s
            }],
        };
        // Do the write again, but with only_write_unwritten set now.
        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(1),
                true,
            )
            .unwrap();

        // Now read back that block, make sure it has the first write
        let responses = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: eid,
                    offset,
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(2),
            )
            .unwrap();

        // We should still have one response.
        assert_eq!(responses.blocks.len(), 1);
        // We don't return the hash, because we've got encryption
        assert!(responses.hashes(0).is_none());
        assert_eq!(responses.encryption_contexts(0), Some(ctx));
        // Data should match first write
        assert_eq!(responses.data[..], [9u8; 512][..]);
    }

    fn test_write_unwritten_when_written_flush(backend: Backend) {
        // Verify that a read fill does not write to the block when
        // there is data written already.  This time run a flush after the
        // first write.  Verify correct state of dirty bit as well.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        // Write the block
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                true,
            )
            .unwrap();

        // Verify the dirty bit is now set.
        assert!(region.get_opened_extent(eid).dirty());

        // Flush extent with eid, fn, gen
        region.region_flush_extent(eid, 1, 1, JobId(1)).unwrap();

        // Verify the dirty bit is no longer set.
        assert!(!region.get_opened_extent(eid).dirty());

        // Create a new write IO with different data.
        let data = Bytes::from(vec![1u8; 512]);
        let ctx = crucible_protocol::EncryptionContext {
            nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            tag: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        };
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(ctx),
                hash: 9163319254371683066, // hash for all 1s
            }],
        };

        // Do the write again, but with only_write_unwritten set now.
        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(1),
                true,
            )
            .unwrap();

        // Verify the dirty bit is not set.
        assert!(!region.get_opened_extent(eid).dirty());

        // Read back our block, make sure it has the first write data
        let responses = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: eid,
                    offset,
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(2),
            )
            .unwrap();

        // We should still have one response.
        assert_eq!(responses.blocks.len(), 1);
        // We don't return the hash, because we have encryption
        assert!(responses.hashes(0).is_none());
        assert_eq!(responses.encryption_contexts(0), Some(ctx));
        // Data should match first write
        assert_eq!(responses.data[..], [9u8; 512][..]);
    }

    fn test_write_unwritten_big_write(backend: Backend) {
        // Do a multi block write where all blocks start new (unwritten)
        // Verify only empty blocks have data.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        let ddef = region.def();
        let num_blocks = ddef.extent_size().value * ddef.extent_count() as u64;

        // use region_write_all to fill region with write_unwritten = true
        let buffer = region_write_all(&mut region, &ddef, true);

        // read data into File, compare what was written to buffer
        let read_from_files = read_file_data(ddef, dir.path(), backend);

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer, responses.data);
    }

    fn test_write_unwritten_big_write_partial_0(backend: Backend) {
        // Do a write to block zero, then do a multi block write with
        // only_write_unwritten set. Verify block zero is the first write, and
        // the remaining blocks have the contents from the multi block fill.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks = ddef.extent_size().value * ddef.extent_count() as u64;

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        // Write the block
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            }],
        };

        // Now write just one block
        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                false,
            )
            .unwrap();

        // use region_write_all to fill region with only_write_unwritten = true
        let mut buffer = region_write_all(&mut region, &ddef, true);

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let read_from_files = read_file_data(ddef, dir.path(), backend);

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer, responses.data);
    }

    fn test_write_unwritten_big_write_partial_1(backend: Backend) {
        // Write to the second block, then do a multi block fill.
        // Verify the second block has the original data we wrote, and all
        // the other blocks have the data from the multi block fill.

        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        let ddef = region.def();
        let num_blocks = ddef.extent_size().value * ddef.extent_count() as u64;

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);

        // Construct the write for the second block on the first EID.
        let eid = ExtentId(0);
        let offset = BlockOffset(1);
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s,
            }],
        };

        // Now write just to the second block.
        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                false,
            )
            .unwrap();

        // Use region_write_all to fill entire region, write_unwritten = true
        let mut buffer = region_write_all(&mut region, &ddef, true);

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our buffer
        // for the first block to have that original data.
        for a_buf in buffer.iter_mut().take(1024).skip(512) {
            *a_buf = 9;
        }

        // read data into File, compare what was written to buffer
        let read_from_files = read_file_data(ddef, dir.path(), backend);

        assert_eq!(buffer.len(), read_from_files.len());
        assert_eq!(buffer, read_from_files);

        // read all using region_read
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer, responses.data);
    }

    fn test_write_unwritten_big_write_partial_final(backend: Backend) {
        // Do a write to the fourth block, then do a multi block read fill
        // where the last block of the read fill is what we wrote to in
        // our first write.
        // verify the fourth block has the original write, and the first
        // three blocks have the data from the multi block read fill.

        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(5, backend).unwrap();

        let ddef = region.def();
        // A bunch of things expect a 512, so let's make it explicit.
        assert_eq!(ddef.block_size(), 512);
        let num_blocks = 4;
        let total_size = ddef.block_size() as usize * num_blocks as usize;

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);

        // Construct the write for the second block on the first EID.
        let eid = ExtentId(0);
        let offset = BlockOffset(3);
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            }],
        };

        // Now write just to the second block.
        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                false,
            )
            .unwrap();

        // Now use region_write to fill four blocks
        //
        // They're all within the same extent, so we just need one ExtentWrite
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        println!("buffer size:{}", buffer.len());
        rng.fill_bytes(&mut buffer);

        let block_contexts: Vec<_> = buffer
            .chunks(512)
            .map(|chunk| BlockContext {
                hash: integrity_hash(&[chunk]),
                encryption_context: None,
            })
            .collect();

        // send only_write_unwritten command.
        region
            .region_write(
                &RegionWrite::new(
                    BlockIndex(0),
                    &block_contexts,
                    Bytes::from(buffer.as_slice().to_vec()),
                    &ddef,
                )
                .unwrap(),
                JobId(0),
                true,
            )
            .unwrap();

        // Because we set only_write_unwritten, the block we already written
        // should still have the data from the first write.  Update our
        // expected buffer for the final block to have that original data.
        for a_buf in buffer.iter_mut().take(2048).skip(1536) {
            *a_buf = 9;
        }

        // read all using region_read
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer, responses.data);
    }

    fn test_write_unwritten_big_write_partial_sparse(backend: Backend) {
        // Do a multi block write_unwritten where a few different blocks have
        // data. Verify only unwritten blocks get the data.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(4, backend).unwrap();

        let ddef = region.def();
        let total_size: usize = ddef.total_size() as usize;
        println!("Total size: {}", total_size);
        let num_blocks = ddef.extent_size().value * ddef.extent_count() as u64;

        // Fill a buffer with "9"s
        let blocks_to_write = [1, 3, 7, 8, 11, 12, 13];
        for b in blocks_to_write {
            let data = Bytes::from(vec![9u8; 512]);
            let eid = ExtentId((b as u64 / ddef.extent_size().value) as u32);
            let offset = BlockOffset((b as u64) % ddef.extent_size().value);

            // Write a few different blocks
            let write = ExtentWrite {
                offset,
                data,
                block_contexts: vec![BlockContext {
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
                }],
            };

            // Now write just one block
            region
                .region_write(
                    &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                    JobId(0),
                    false,
                )
                .unwrap();
        }

        // Use region_write_all to fill entire region, with only_write_unwritten
        let mut buffer = region_write_all(&mut region, &ddef, true);

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
        let req = RegionReadRequest::new(BlockIndex(0), num_blocks, &ddef);
        let responses = region.region_read(&req, JobId(0)).unwrap();

        assert_eq!(buffer, responses.data);
    }

    // A test function to return a generic'ish write command.
    // We use the "all 9's data" and checksum.
    fn create_generic_write(eid: ExtentId, offset: BlockOffset) -> RegionWrite {
        let data = Bytes::from(vec![9u8; 512]);
        RegionWrite(vec![RegionWriteReq {
            extent: eid,
            write: ExtentWrite {
                offset,
                data,
                block_contexts: vec![BlockContext {
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
                }],
            },
        }])
    }

    fn test_flush_extent_limit_base(backend: Backend) {
        // Check that the extent_limit value in region_flush is honored
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(2, backend).unwrap();

        // Write to extent 0 block 0 first
        let writes = create_generic_write(ExtentId(0), BlockOffset(0));
        region.region_write(&writes, JobId(0), true).unwrap();

        // Now write to extent 1 block 0
        let writes = create_generic_write(ExtentId(1), BlockOffset(0));

        region.region_write(&writes, JobId(0), true).unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(ExtentId(0)).dirty());
        assert!(region.get_opened_extent(ExtentId(1)).dirty());

        // Call flush, but limit the flush to extent 0
        region
            .region_flush(1, 2, &None, JobId(3), Some(ExtentId(0)))
            .unwrap();

        // Verify the dirty bit is no longer set for 0, but still set
        // for extent 1.
        assert!(!region.get_opened_extent(ExtentId(0)).dirty());
        assert!(region.get_opened_extent(ExtentId(1)).dirty());
    }

    fn test_flush_extent_limit_end(backend: Backend) {
        // Check that the extent_limit value in region_flush is honored
        // Write to the last block in the extents.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(3, backend).unwrap();

        // Write to extent 1 block 9 first
        let writes = create_generic_write(ExtentId(1), BlockOffset(9));
        region.region_write(&writes, JobId(1), true).unwrap();

        // Now write to extent 2 block 9
        let writes = create_generic_write(ExtentId(2), BlockOffset(9));
        region.region_write(&writes, JobId(2), true).unwrap();

        // Verify the dirty bit is now set for both extents.
        assert!(region.get_opened_extent(ExtentId(1)).dirty());
        assert!(region.get_opened_extent(ExtentId(2)).dirty());

        // Call flush, but limit the flush to extents < 2
        region
            .region_flush(1, 2, &None, JobId(3), Some(ExtentId(1)))
            .unwrap();

        // Verify the dirty bit is no longer set for 1, but still set
        // for extent 2.
        assert!(!region.get_opened_extent(ExtentId(1)).dirty());
        assert!(region.get_opened_extent(ExtentId(2)).dirty());

        // Now flush with no restrictions.
        region.region_flush(1, 2, &None, JobId(3), None).unwrap();

        // Extent 2 should no longer be dirty
        assert!(!region.get_opened_extent(ExtentId(2)).dirty());
    }

    fn test_flush_extent_limit_walk_it_off(backend: Backend) {
        // Check that the extent_limit value in region_flush is honored
        // Write to all the extents, then flush them one at a time.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(10, backend).unwrap();

        // Write to extents 0 to 9
        let mut job_id = 1;
        for ext in (0..10).map(ExtentId) {
            let writes = create_generic_write(ext, BlockOffset(5));
            region.region_write(&writes, JobId(job_id), true).unwrap();
            job_id += 1;
        }

        // Verify the dirty bit is now set for all extents.
        for ext in (0..10).map(ExtentId) {
            assert!(region.get_opened_extent(ext).dirty());
        }

        // Walk up the extent_limit, verify at each flush extents are
        // flushed.
        for ext in (0..10).map(ExtentId) {
            println!("Send flush to extent limit {}", ext);
            region
                .region_flush(1, 2, &None, JobId(3), Some(ext))
                .unwrap();

            // This ext should no longer be dirty.
            println!("extent {} should not be dirty now", ext);
            assert!(!region.get_opened_extent(ext).dirty());

            // Any extent above the current point should still be dirty.
            for d_ext in (ext.0 + 1..10).map(ExtentId) {
                println!("verify {} still dirty", d_ext);
                assert!(region.get_opened_extent(d_ext).dirty());
            }
        }
    }

    fn test_flush_extent_limit_too_large(backend: Backend) {
        // Check that the extent_limit value in region_flush will return
        // an error if the extent_limit is too large.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Call flush with an invalid extent
        assert!(region
            .region_flush(1, 2, &None, JobId(3), Some(ExtentId(2)))
            .is_err());
    }

    fn test_extent_write_flush_close(backend: Backend) {
        // Verify that a write then close of an extent will return the
        // expected gen flush and dirty bits for that extent.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        // Write the block
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                true,
            )
            .unwrap();

        // Flush extent with eid, fn, gen
        region.region_flush_extent(eid, 3, 2, JobId(1)).unwrap();

        // Close extent 0
        let (gen, flush, dirty) = region.close_extent(eid).unwrap();

        // Verify inner is gone, and we returned the expected gen, flush
        // and dirty values for the write that should be flushed now.
        assert_eq!(gen, 3);
        assert_eq!(flush, 2);
        assert!(!dirty);
    }

    fn test_extent_close_reopen_flush_close(backend: Backend) {
        // Do several extent open close operations, verifying that the
        // gen/flush/dirty return values are as expected.
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Fill a buffer with "9"'s
        let data = Bytes::from(vec![9u8; 512]);
        let eid = ExtentId(0);
        let offset = BlockOffset(0);

        // Write the block
        let write = ExtentWrite {
            offset,
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 14137680576404864188, // Hash for all 9s
            }],
        };

        region
            .region_write(
                &RegionWrite(vec![RegionWriteReq { extent: eid, write }]),
                JobId(0),
                true,
            )
            .unwrap();

        // Close extent 0 without a flush
        let (gen, flush, dirty) = region.close_extent(eid).unwrap();

        // Because we did not flush yet, this extent should still have
        // the values for an unwritten extent, except for the dirty bit.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Open the extent, then close it again, the values should remain
        // the same as the previous check (testing here that dirty remains
        // dirty).
        region.reopen_extent(eid).unwrap();

        let (gen, flush, dirty) = region.close_extent(eid).unwrap();

        // Verify everything is the same, and dirty is still set.
        assert_eq!(gen, 0);
        assert_eq!(flush, 0);
        assert!(dirty);

        // Reopen, then flush extent with eid, fn, gen
        region.reopen_extent(eid).unwrap();
        region.region_flush_extent(eid, 4, 9, JobId(1)).unwrap();

        let (gen, flush, dirty) = region.close_extent(eid).unwrap();

        // Verify after flush that g,f are updated, and that dirty
        // is no longer set.
        assert_eq!(gen, 4);
        assert_eq!(flush, 9);
        assert!(!dirty);
    }

    /// We need to make sure that a flush will properly adjust the DB hashes
    /// after issuing multiple writes to different disconnected sections of
    /// an extent
    fn test_flush_after_multiple_disjoint_writes(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(1024));
        let mut region = Region::create(&dir, region_opts, csl()).unwrap();
        region.extend(1, backend).unwrap();

        // Write some data to 3 different areas
        let ranges = [(0..120), (243..244), (487..903)];

        // We will write these ranges multiple times so that there's multiple
        // hashes in the DB, so we need multiple sets of data.
        let writes: Vec<RegionWrite> = (0..3)
            .map(|_| {
                let writes = ranges
                    .iter()
                    .map(|range| {
                        let n = range.clone().count();
                        let mut rng = rand::thread_rng();
                        let mut data: Vec<u8> = vec![0; 512 * n];
                        rng.fill_bytes(&mut data);

                        let block_contexts = data
                            .chunks(512)
                            .map(|chunk| BlockContext {
                                hash: integrity_hash(&[chunk]),
                                encryption_context: None,
                            })
                            .collect();
                        RegionWriteReq {
                            extent: ExtentId(0),
                            write: ExtentWrite {
                                offset: BlockOffset(range.start),
                                data: Bytes::from(data.as_slice().to_vec()),
                                block_contexts,
                            },
                        }
                    })
                    .collect();
                RegionWrite(writes)
            })
            .collect();

        // Write all the writes
        for w in &writes {
            region.region_write(w, JobId(0), false).unwrap();
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).unwrap();

        // We are gonna compare against the last write iteration
        let last_writes = writes.last().unwrap();

        let ext = region.get_opened_extent_mut(ExtentId(0));

        for (i, range) in ranges.iter().enumerate() {
            let req = ExtentReadRequest {
                offset: BlockOffset(range.start),
                data: BytesMut::with_capacity(
                    512 * (range.end - range.start) as usize,
                ),
            };
            let resp = ext.read(JobId(i as u64), req).unwrap();

            // Now that we've checked that, flatten out for an easier eq
            let actual_ctxts = resp.blocks.clone();

            // What we expect is the hashes for the last write we did
            let expected_ctxts: Vec<_> = last_writes.0[i]
                .write
                .block_contexts
                .iter()
                .map(|b| match b.encryption_context {
                    Some(ctx) => ReadBlockContext::Encrypted { ctx },
                    None => ReadBlockContext::Unencrypted { hash: b.hash },
                })
                .collect();

            // Check that they're right.
            assert_eq!(expected_ctxts, actual_ctxts);
        }
    }

    /// This test ensures that our flush logic works even for full-extent
    /// flushes. That's the case where the set of modified blocks will be full.
    fn test_big_extent_full_write_and_flush(backend: Backend) {
        let dir = tempdir().unwrap();

        const EXTENT_SIZE: u64 = 4096;
        let mut region_opts = new_region_options();
        region_opts.set_extent_size(Block::new_512(EXTENT_SIZE));
        let mut region = Region::create(&dir, region_opts, csl()).unwrap();
        region.extend(1, backend).unwrap();
        eprintln!("created region");

        // writing the entire region a few times over before the flush.
        let writes: Vec<ExtentWrite> = (0..3)
            .map(|_| {
                let mut data = vec![0u8; 512 * EXTENT_SIZE as usize];
                rand::thread_rng().fill_bytes(&mut data);
                let block_contexts = data
                    .chunks(512)
                    .map(|chunk| BlockContext {
                        hash: integrity_hash(&[chunk]),
                        encryption_context: None,
                    })
                    .collect();
                ExtentWrite {
                    offset: BlockOffset(0),
                    data: Bytes::from(data.as_slice().to_vec()),
                    block_contexts,
                }
            })
            .collect();

        // Write all the writes
        for w in &writes {
            region
                .region_write(
                    &RegionWrite(vec![RegionWriteReq {
                        extent: ExtentId(0),
                        write: w.clone(),
                    }]),
                    JobId(0),
                    false,
                )
                .unwrap();
        }

        // Flush
        region.region_flush(1, 2, &None, JobId(3), None).unwrap();

        // compare against the last write iteration
        let last_write = writes.last().unwrap();

        let ext = region.get_opened_extent_mut(ExtentId(0));

        // Get the contexts for the range
        let req = ExtentReadRequest {
            offset: BlockOffset(0),
            data: BytesMut::with_capacity(512 * EXTENT_SIZE as usize),
        };
        let out = ext.read(JobId(0), req).unwrap();

        // Now that we've checked that, flatten out for an easier eq
        let actual_ctxts = out.blocks.clone();

        // What we expect is the hashes for the last write we did
        let expected_ctxts: Vec<_> = last_write
            .block_contexts
            .iter()
            .map(|b| match b.encryption_context {
                Some(ctx) => ReadBlockContext::Encrypted { ctx },
                None => ReadBlockContext::Unencrypted { hash: b.hash },
            })
            .collect();

        // What we expect is the hashes for the last write we did
        // Check that they're right.
        assert_eq!(expected_ctxts, actual_ctxts);
    }

    fn test_bad_hash_bad(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        let data = Bytes::from(vec![1u8; 512]);

        let write = ExtentWrite {
            offset: BlockOffset(0),
            data,
            block_contexts: vec![BlockContext {
                encryption_context: Some(
                    crucible_protocol::EncryptionContext {
                        nonce: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        tag: [
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                            18, 19,
                        ],
                    },
                ),
                hash: 2398419238764,
            }],
        };

        let result = region.region_write(
            &RegionWrite(vec![RegionWriteReq {
                extent: ExtentId(0),
                write,
            }]),
            JobId(0),
            false,
        );

        assert!(result.is_err());

        match result.err().unwrap() {
            CrucibleError::HashMismatch => {
                // ok
            }
            _ => {
                panic!("Incorrect error with hash mismatch");
            }
        }
    }

    fn test_blank_block_read_ok(backend: Backend) {
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();
        region.extend(1, backend).unwrap();

        let responses = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: ExtentId(0),
                    offset: BlockOffset(0),
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(0),
            )
            .unwrap();

        assert_eq!(responses.blocks.len(), 1);
        assert!(responses.hashes(0).is_none());
        assert!(responses.encryption_contexts(0).is_none());
        assert_eq!(responses.data[..], [0u8; 512][..]);
    }

    fn prepare_random_region(
        backend: Backend,
    ) -> (tempfile::TempDir, Region, Vec<u8>) {
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();

        // Create 3 extents, each size 10 blocks
        const EXTENT_COUNT: u32 = 3;
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(EXTENT_COUNT, backend).unwrap();

        let ddef = region.def();
        let total_size = ddef.total_size() as usize;
        let num_blocks: usize =
            ddef.extent_size().value as usize * ddef.extent_count() as usize;

        // Write in random data
        let mut rng = rand::thread_rng();
        let mut buffer: Vec<u8> = vec![0; total_size];
        rng.fill_bytes(&mut buffer);

        let mut writes = Vec::with_capacity(num_blocks);

        for i in 0..EXTENT_COUNT {
            let eid = ExtentId::from(i);
            let data = Bytes::from(
                buffer[(i as usize * 10 * 512)..][..512 * 10].to_vec(),
            );
            let block_contexts = data
                .chunks(512)
                .map(|chunk| BlockContext {
                    encryption_context: None,
                    hash: integrity_hash(&[chunk]),
                })
                .collect();

            writes.push(RegionWriteReq {
                extent: eid,
                write: ExtentWrite {
                    offset: BlockOffset(0),
                    data,
                    block_contexts,
                },
            });
        }

        region
            .region_write(&RegionWrite(writes), JobId(0), false)
            .unwrap();

        (dir, region, buffer)
    }

    fn test_read_single_large_contiguous(backend: Backend) {
        let (_dir, mut region, data) = prepare_random_region(backend);

        // Call region_read with a single large contiguous range
        let req = RegionReadRequest::new(BlockIndex(1), 7, &region.def());
        let responses = region.region_read(&req, JobId(0)).unwrap();

        // Validate returned data
        assert_eq!(responses.data, &data[512..(8 * 512)],);
    }

    fn test_read_single_large_contiguous_span_extents(backend: Backend) {
        let (_dir, mut region, data) = prepare_random_region(backend);

        // Call region_read with a single large contiguous range that spans
        // multiple extents
        let req = RegionReadRequest::new(BlockIndex(9), 19, &region.def());
        let responses = region.region_read(&req, JobId(0)).unwrap();

        // Validate returned data
        assert_eq!(&responses.data, &data[(9 * 512)..(28 * 512)],);
    }

    fn prepare_writes(
        offsets: std::ops::Range<usize>,
        data: &mut [u8],
    ) -> Vec<RegionWriteReq> {
        let mut writes = vec![];
        let mut rng = rand::thread_rng();

        // Offsets are given as blocks; we convert to extents here
        const EXTENT_SIZE: usize = 10; // hard-coded default
        for (eid, mut group) in offsets
            .group_by(|o| ExtentId((*o / EXTENT_SIZE) as u32))
            .into_iter()
        {
            let start = group.next().unwrap();
            let n = group.count() + 1;
            let mut buffer = vec![0; n * 512];
            rng.fill_bytes(&mut buffer);

            let block_contexts = buffer
                .chunks(512)
                .map(|chunk| BlockContext {
                    encryption_context: None,
                    hash: integrity_hash(&[chunk]),
                })
                .collect();

            // alter data as writes are prepared
            data[start * 512..][..buffer.len()].copy_from_slice(&buffer);

            writes.push(RegionWriteReq {
                extent: eid,
                write: ExtentWrite {
                    offset: BlockOffset((start as u64) % (EXTENT_SIZE as u64)),
                    data: Bytes::from(buffer),
                    block_contexts,
                },
            });
        }
        assert!(!writes.is_empty());

        writes
    }

    fn validate_whole_region(region: &mut Region, data: &[u8]) {
        let num_blocks = region.def().extent_size().value
            * region.def().extent_count() as u64;

        let req =
            RegionReadRequest::new(BlockIndex(0), num_blocks, &region.def());
        let responses = region.region_read(&req, JobId(1)).unwrap();

        assert_eq!(&responses.data, &data,);
    }

    fn test_write_single_large_contiguous(backend: Backend) {
        let (_dir, mut region, mut data) = prepare_random_region(backend);

        // Call region_write with a single large contiguous range
        let writes = RegionWrite(prepare_writes(1..8, &mut data));

        region.region_write(&writes, JobId(0), false).unwrap();

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&mut region, &data);
    }

    fn test_write_single_large_contiguous_span_extents(backend: Backend) {
        let (_dir, mut region, mut data) = prepare_random_region(backend);

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = RegionWrite(prepare_writes(9..28, &mut data));

        region.region_write(&writes, JobId(0), false).unwrap();

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&mut region, &data);
    }

    fn test_write_unwritten_single_large_contiguous(backend: Backend) {
        // Create a blank region
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3, backend).unwrap();

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range
        let writes = RegionWrite(prepare_writes(1..8, &mut data));

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).unwrap();

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&mut region, &data);
    }

    fn test_write_unwritten_single_large_contiguous_span_extents(
        backend: Backend,
    ) {
        // Create a blank region
        let dir = tempdir().unwrap();
        let mut region =
            Region::create(&dir, new_region_options(), csl()).unwrap();

        // Create 3 extents, each size 10 blocks
        assert_eq!(region.def().extent_size().value, 10);
        region.extend(3, backend).unwrap();

        let mut data: Vec<u8> = vec![0; region.def().total_size() as usize];

        // Call region_write with a single large contiguous range that spans
        // multiple extents
        let writes = RegionWrite(prepare_writes(9..28, &mut data));

        // write_unwritten = true
        region.region_write(&writes, JobId(0), true).unwrap();

        // Validate written data by reading everything back and comparing with
        // data buffer
        validate_whole_region(&mut region, &data);
    }

    /// Macro defining the full region test suite
    ///
    /// Functions in the test suite should take a `b: Backend` parameter and
    /// panic on an error (i.e. returning `()`).
    ///
    /// Add new functions here to ensure that they're tested for every backend!
    macro_rules! region_test_suite {
        ($b:ident) => {
            region_test_suite!(
                $b,
                region_create_drop_open,
                copy_extent_dir,
                copy_extent_dir_twice,
                close_extent,
                reopen_extent_cleanup_one,
                reopen_extent_cleanup_two,
                reopen_extent_cleanup_replay,
                reopen_extent_cleanup_replay_short,
                reopen_extent_no_replay_readonly,
                reopen_all_extents,
                new_region,
                new_existing_region,
                dump_a_region,
                dump_two_region,
                dump_extent,
                test_big_write,
                test_region_open_removes_partial_writes,
                test_ok_hash_ok,
                test_write_unwritten_when_empty,
                test_write_unwritten_when_written,
                test_write_unwritten_when_written_flush,
                test_write_unwritten_big_write,
                test_write_unwritten_big_write_partial_0,
                test_write_unwritten_big_write_partial_1,
                test_write_unwritten_big_write_partial_final,
                test_write_unwritten_big_write_partial_sparse,
                test_flush_extent_limit_base,
                test_flush_extent_limit_end,
                test_flush_extent_limit_walk_it_off,
                test_flush_extent_limit_too_large,
                test_extent_write_flush_close,
                test_extent_close_reopen_flush_close,
                test_flush_after_multiple_disjoint_writes,
                test_big_extent_full_write_and_flush,
                test_bad_hash_bad,
                test_blank_block_read_ok,
                test_read_single_large_contiguous,
                test_write_single_large_contiguous,
                test_write_single_large_contiguous_span_extents,
                test_write_unwritten_single_large_contiguous,
                test_write_unwritten_single_large_contiguous_span_extents,
                test_read_single_large_contiguous_span_extents
            );
        };

        ($b:ident, $($fs:ident),+) => {
        $(
            #[test]
            fn $fs() {
                super::$fs(Backend::$b)
            }
         )+};
    }

    mod raw_file {
        use super::*;
        region_test_suite!(RawFile);
    }
    mod sqlite {
        use super::*;
        region_test_suite!(SQLite);
    }
}
