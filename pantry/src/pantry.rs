// Copyright 2022 Oxide Computer Company

use std::collections::BTreeMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use dropshot::HttpError;
use sha2::Digest;
use sha2::Sha256;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crucible::BlockIO;
use crucible::SnapshotDetails;
use crucible::Volume;
use crucible::VolumeConstructionRequest;
use crucible_common::crucible_bail;
use crucible_common::CrucibleError;

use crate::server::ExpectedDigest;

pub struct PantryEntry {
    log: Logger,
    volume: Volume,
    volume_construction_request: VolumeConstructionRequest,
}

/// Retry a request in the face of network weather
async fn retry_until_known_result<F, Fut>(
    log: &Logger,
    mut func: F,
) -> Result<reqwest::Response, reqwest::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    let mut last_error = None;

    // Retry for a maximum of 60 times
    for _ in 0..60 {
        let result = func().await;
        match result {
            Ok(v) => {
                return Ok(v);
            }

            Err(e) => {
                if e.is_timeout() {
                    info!(log, "request failed due to timeout, sleeping");
                    last_error = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                } else if matches!(
                    e.status(),
                    Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
                        | Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
                ) {
                    info!(
                        log,
                        "request failed with status {}, sleeping",
                        e.status().unwrap()
                    );
                    last_error = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }

    error!(log, "gave up after 60 retries");

    match last_error {
        Some(e) => Err(e),

        None => {
            panic!("60 retries but last_error was not set?");
        }
    }
}

impl PantryEntry {
    pub const MAX_CHUNK_SIZE: usize = 512 * 1024;

    pub async fn import_from_url(
        &self,
        url: String,
        expected_digest: Option<ExpectedDigest>,
    ) -> Result<(), CrucibleError> {
        // Construct a reqwest client that
        //
        // 1) times out after 10 seconds if a connection can't be made
        // 2) times out if the connection + chunk download takes over 60 seconds
        //
        // Now, `MAX_CHUNK_SIZE / 60s ~= 8.5kb/s`. If the connection you're downloading from is
        // that slow, then the pantry won't work, sorry!
        let connect_timeout = std::time::Duration::from_secs(10);
        let total_timeout =
            std::time::Duration::from_secs(60) + connect_timeout;
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(connect_timeout)
            .timeout(total_timeout)
            .build()
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        // Validate the URL can be reached, and grab the content length
        let response = retry_until_known_result(&self.log, {
            let client = client.clone();
            let url = url.clone();
            move || client.head(&url).send()
        })
        .await
        .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        if !response.status().is_success() {
            crucible_bail!(
                GenericError,
                "querying url returned: {}",
                response.status()
            );
        }

        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .ok_or("no content length!")
            .map_err(|e| anyhow!(e))?;

        let request_total_size = u64::from_str(
            content_length
                .to_str()
                .map_err(|e| CrucibleError::GenericError(e.to_string()))?,
        )
        .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        // check volume size
        let volume_total_size = self.volume.total_size().await?;
        if request_total_size > volume_total_size {
            crucible_bail!(
                InvalidNumberOfBlocks,
                "volume size {} smaller than size {} at url {}",
                volume_total_size,
                request_total_size,
                url,
            );
        }

        // import chunks into the volume, optionally hashing the bytes for later
        // matching against the expected digest
        let mut hasher = if let Some(ref expected_digest) = expected_digest {
            match expected_digest {
                ExpectedDigest::Sha256(_) => Some(Sha256::new()),
            }
        } else {
            None
        };

        let volume_block_size = self.volume.get_block_size().await?;
        for chunk in (0..request_total_size).step_by(Self::MAX_CHUNK_SIZE) {
            let start = chunk;
            let end = std::cmp::min(
                start + Self::MAX_CHUNK_SIZE as u64,
                request_total_size,
            );

            let response = retry_until_known_result(&self.log, {
                let client = client.clone();
                let url = url.clone();
                move || {
                    client
                        .get(&url)
                        .header(
                            reqwest::header::RANGE,
                            format!("bytes={}-{}", start, end - 1),
                        )
                        .send()
                }
            })
            .await
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

            let content_length = response
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .ok_or("no content length!")
                .map_err(|e| anyhow!(e))?;

            let content_length = u64::from_str(
                content_length
                    .to_str()
                    .map_err(|e| CrucibleError::GenericError(e.to_string()))?,
            )
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

            if content_length != (end - start) {
                // the remote web server didn't honour the RANGE header!
                crucible_bail!(
                    GenericError,
                    "RANGE header bytes={}-{}, content length returned is {}!",
                    start,
                    end - 1,
                    content_length,
                );
            }

            assert!(content_length <= Self::MAX_CHUNK_SIZE as u64);
            assert!(content_length % volume_block_size == 0);

            let bytes = response
                .bytes()
                .await
                .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

            if let Some(ref mut hasher) = hasher {
                hasher.update(&bytes);
            }

            self.volume.write_to_byte_offset(start, bytes).await?;
        }

        // flush

        self.volume.flush(None).await?;

        if let Some(hasher) = hasher {
            let digest = hex::encode(hasher.finalize());

            match expected_digest.unwrap() {
                ExpectedDigest::Sha256(expected_digest) => {
                    if expected_digest != digest {
                        crucible_bail!(
                            GenericError,
                            "sha256 digest mismatch! expected {}, saw {}",
                            expected_digest,
                            digest,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn snapshot(
        &self,
        snapshot_id: String,
    ) -> Result<(), CrucibleError> {
        self.volume
            .flush(Some(SnapshotDetails {
                snapshot_name: snapshot_id,
            }))
            .await
    }

    pub async fn bulk_write(
        &self,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), CrucibleError> {
        if data.len() > Self::MAX_CHUNK_SIZE {
            crucible_bail!(
                InvalidNumberOfBlocks,
                "data len {} over max chunk size {}!",
                data.len(),
                Self::MAX_CHUNK_SIZE,
            );
        }

        self.volume.write_to_byte_offset(offset, data.into()).await
    }

    pub async fn bulk_read(
        &self,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, CrucibleError> {
        if size > Self::MAX_CHUNK_SIZE {
            crucible_bail!(
                InvalidNumberOfBlocks,
                "request len {} over max chunk size {}!",
                size,
                Self::MAX_CHUNK_SIZE,
            );
        }

        let buffer = crucible::Buffer::new(size);

        self.volume
            .read_from_byte_offset(offset, buffer.clone())
            .await?;

        Ok(buffer.into_vec().unwrap())
    }

    pub async fn scrub(&self) -> Result<(), CrucibleError> {
        self.volume.scrub(None, None).await
    }

    pub async fn validate(
        &self,
        expected_digest: ExpectedDigest,
        size_to_validate: Option<u64>,
    ) -> Result<(), CrucibleError> {
        let mut hasher = match expected_digest {
            ExpectedDigest::Sha256(_) => Sha256::new(),
        };

        let size_to_validate =
            size_to_validate.unwrap_or(self.volume.total_size().await?);

        let block_size = self.volume.get_block_size().await?;
        if (size_to_validate % block_size) != 0 {
            crucible_bail!(
                InvalidNumberOfBlocks,
                "size to validate {} not divisible by block size {}!",
                size_to_validate,
                block_size,
            );
        }

        for chunk in (0..size_to_validate).step_by(Self::MAX_CHUNK_SIZE) {
            let start = chunk;
            let end = std::cmp::min(
                start + Self::MAX_CHUNK_SIZE as u64,
                size_to_validate,
            );

            let data = crucible::Buffer::new((end - start) as usize);

            self.volume
                .read_from_byte_offset(start, data.clone())
                .await?;

            hasher.update(&data.into_vec().unwrap())
        }

        let digest = hex::encode(hasher.finalize());

        match expected_digest {
            ExpectedDigest::Sha256(expected_digest) => {
                if expected_digest != digest {
                    crucible_bail!(
                        GenericError,
                        "sha256 digest mismatch! expected {}, saw {}",
                        expected_digest,
                        digest,
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn detach(&self) -> Result<(), CrucibleError> {
        self.volume.flush(None).await?;
        self.volume.deactivate().await?;
        Ok(())
    }
}

/// Pantry stores opened Volumes in-memory
pub struct Pantry {
    pub log: Logger,

    /// Store a Volume Construction Request and Volume, indexed by id. Use this
    /// Mutex -> Arc<Mutex> structure in order for multiple requests to act on
    /// multiple PantryEntry objects at the same time.
    entries: Mutex<BTreeMap<String, Arc<PantryEntry>>>,

    /// Pantry can run background jobs on Volumes, and currently running jobs
    /// are stored here.
    jobs: Mutex<BTreeMap<String, JoinHandle<Result<(), CrucibleError>>>>,
}

impl Pantry {
    pub fn new(log: Logger) -> Result<Pantry> {
        Ok(Pantry {
            log,
            entries: Mutex::new(BTreeMap::default()),
            jobs: Mutex::new(BTreeMap::default()),
        })
    }

    pub async fn attach(
        &self,
        volume_id: String,
        volume_construction_request: VolumeConstructionRequest,
    ) -> Result<(), CrucibleError> {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(&volume_id) {
            // This function must be idempotent for the same inputs. If an entry
            // at this ID exists already, compare the existing volume
            // construction request, and return either Ok or conflict
            if entry.volume_construction_request == volume_construction_request
            {
                info!(
                    self.log,
                    "volume {} already an entry, and has same volume \
                    construction request, returning OK",
                    volume_id,
                );

                return Ok(());
            } else {
                error!(
                    self.log,
                    "volume {} already an entry, but has different volume \
                    construction request, bailing!",
                    volume_id,
                );

                crucible_bail!(
                    Unsupported,
                    "Existing entry for {} with different volume construction \
                    request!",
                    volume_id,
                );
            }
        }

        // If no entry exists, then add one
        info!(
            self.log,
            "no entry exists for volume {}, constructing...", volume_id
        );

        let volume = Volume::construct(
            volume_construction_request.clone(),
            None,
            self.log.clone(),
        )
        .await?;

        info!(self.log, "volume {} constructed ok", volume_id);

        volume.activate().await?;

        info!(self.log, "volume {} activated ok", volume_id);

        entries.insert(
            volume_id.clone(),
            Arc::new(PantryEntry {
                log: self.log.new(o!("volume" => volume_id.clone())),
                volume,
                volume_construction_request,
            }),
        );

        info!(self.log, "volume {} constructed and inserted ok", volume_id);

        Ok(())
    }

    pub async fn entry(
        &self,
        volume_id: String,
    ) -> Result<Arc<PantryEntry>, HttpError> {
        let entries = self.entries.lock().await;
        match entries.get(&volume_id) {
            Some(entry) => {
                let entry = entry.clone();
                drop(entries);
                Ok(entry)
            }

            None => {
                error!(self.log, "volume {} not in pantry", volume_id);

                Err(HttpError::for_not_found(None, volume_id))
            }
        }
    }

    pub async fn is_job_finished(
        &self,
        job_id: String,
    ) -> Result<bool, HttpError> {
        let jobs = self.jobs.lock().await;
        match jobs.get(&job_id) {
            Some(join_handle) => Ok(join_handle.is_finished()),

            None => {
                error!(self.log, "job {} not a pantry job", job_id);

                Err(HttpError::for_not_found(None, job_id.to_string()))
            }
        }
    }

    pub async fn get_job_result(
        &self,
        job_id: String,
    ) -> Result<Result<()>, HttpError> {
        let mut jobs = self.jobs.lock().await;

        // Remove the job from the list of jobs, then await on the join handle.
        // If this errors, then the job has failed in some way, so don't leave
        // it in the list of jobs.
        match jobs.remove(&job_id) {
            Some(join_handle) => {
                let result: Result<(), CrucibleError> =
                    join_handle.await.map_err(|e| {
                        HttpError::for_internal_error(e.to_string())
                    })?;

                jobs.remove(&job_id);

                if let Err(e) = &result {
                    error!(self.log, "job {} failed with {}", job_id, e);
                }

                Ok(result.map_err(|e| e.into()))
            }

            None => {
                error!(self.log, "job {} not a pantry job", job_id);

                Err(HttpError::for_not_found(None, job_id.to_string()))
            }
        }
    }

    pub async fn import_from_url(
        &self,
        volume_id: String,
        url: String,
        expected_digest: Option<ExpectedDigest>,
    ) -> Result<String, HttpError> {
        let entry = self.entry(volume_id).await?;
        let entry = entry.clone();

        let join_handle = tokio::spawn(async move {
            entry.import_from_url(url, expected_digest).await
        });

        let mut jobs = self.jobs.lock().await;
        let job_id = Uuid::new_v4().to_string();
        jobs.insert(job_id.clone(), join_handle);

        Ok(job_id)
    }

    pub async fn snapshot(
        &self,
        volume_id: String,
        snapshot_id: String,
    ) -> Result<(), HttpError> {
        let entry = self.entry(volume_id).await?;
        entry.snapshot(snapshot_id).await.map_err(|e| e.into())
    }

    pub async fn bulk_write(
        &self,
        volume_id: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), HttpError> {
        let entry = self.entry(volume_id).await?;
        entry.bulk_write(offset, data).await.map_err(|e| e.into())
    }

    pub async fn bulk_read(
        &self,
        volume_id: String,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, HttpError> {
        let entry = self.entry(volume_id).await?;
        entry.bulk_read(offset, size).await.map_err(|e| e.into())
    }

    pub async fn scrub(&self, volume_id: String) -> Result<String, HttpError> {
        let entry = self.entry(volume_id).await?;
        let entry = entry.clone();

        let join_handle = tokio::spawn(async move { entry.scrub().await });

        let mut jobs = self.jobs.lock().await;
        let job_id = Uuid::new_v4().to_string();
        jobs.insert(job_id.clone(), join_handle);

        Ok(job_id)
    }

    pub async fn validate(
        &self,
        volume_id: String,
        expected_digest: ExpectedDigest,
        size_to_verify: Option<u64>,
    ) -> Result<String, HttpError> {
        let entry = self.entry(volume_id).await?;
        let entry = entry.clone();

        let join_handle = tokio::spawn(async move {
            entry.validate(expected_digest, size_to_verify).await
        });

        let mut jobs = self.jobs.lock().await;
        let job_id = Uuid::new_v4().to_string();
        jobs.insert(job_id.clone(), join_handle);

        Ok(job_id)
    }

    /// Remove an entry from the pantry, and detach it. If detach fails, the
    /// entry is still gone but this function will return an error.
    pub async fn detach(&self, volume_id: String) -> Result<(), CrucibleError> {
        let mut entries = self.entries.lock().await;

        info!(self.log, "detach removing entry for volume {}", volume_id);

        match entries.remove(&volume_id) {
            Some(entry) => {
                info!(self.log, "detaching volume {}", volume_id);
                entry.detach().await?;
                drop(entry);
            }

            None => {
                info!(
                    self.log,
                    "detach did nothing, no entry for volume {}", volume_id
                );
            }
        }

        Ok(())
    }
}
