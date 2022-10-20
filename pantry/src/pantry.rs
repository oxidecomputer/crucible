// Copyright 2022 Oxide Computer Company

use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use dropshot::HttpError;
use sha2::Digest;
use sha2::Sha256;
use slog::info;
use slog::warn;
use slog::Logger;
use tokio::sync::Mutex;

use crucible::Block;
use crucible::BlockIO;
use crucible::SnapshotDetails;
use crucible::Volume;
use crucible::VolumeConstructionRequest;

use crate::server::ExpectedDigest;

pub struct PantryEntry {
    volume: Volume,
    volume_construction_request: VolumeConstructionRequest,
}

impl PantryEntry {
    pub const MAX_CHUNK_SIZE: usize = 512 * 1024;

    pub async fn import_from_url(
        &self,
        url: String,
        expected_digest: Option<ExpectedDigest>,
    ) -> Result<()> {
        // validate the URL can be reached, and grab the content length
        let dur = std::time::Duration::from_secs(5);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;

        let response = client.head(&url).send().await?;

        if !response.status().is_success() {
            bail!("querying url returned: {}", response.status());
        }

        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .ok_or("no content length!")
            .map_err(|e| anyhow!(e))?;

        let request_total_size = usize::from_str(content_length.to_str()?)?;

        // check volume size
        let volume_total_size = self.volume.total_size().await?;
        if request_total_size > volume_total_size as usize {
            bail!(
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
            let end =
                std::cmp::min(start + Self::MAX_CHUNK_SIZE, request_total_size);

            let response = client
                .get(&url)
                .header(
                    reqwest::header::RANGE,
                    format!("bytes={}-{}", start, end - 1),
                )
                .send()
                .await?;

            let content_length = response
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .ok_or("no content length!")
                .map_err(|e| anyhow!(e))?;

            let content_length = usize::from_str(content_length.to_str()?)?;

            assert_eq!(content_length, end - start);
            assert!(content_length <= Self::MAX_CHUNK_SIZE);
            assert!(content_length % volume_block_size as usize == 0);

            let bytes = response.bytes().await?;

            if let Some(ref mut hasher) = hasher {
                hasher.update(&bytes);
            }

            self.volume
                .write(
                    Block::new(
                        start as u64,
                        volume_block_size.trailing_zeros(),
                    ),
                    bytes,
                )
                .await?;
        }

        // flush

        self.volume.flush(None).await?;

        if let Some(hasher) = hasher {
            let digest = hex::encode(hasher.finalize());

            match expected_digest.unwrap() {
                ExpectedDigest::Sha256(expected_digest) => {
                    if expected_digest != digest {
                        bail!(
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

    pub async fn snapshot(&self, snapshot_id: String) -> Result<()> {
        self.volume
            .flush(Some(SnapshotDetails {
                snapshot_name: snapshot_id,
            }))
            .await?;

        Ok(())
    }

    pub async fn bulk_write(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        if data.len() > Self::MAX_CHUNK_SIZE {
            bail!(
                "data len {} over max chunk size{}!",
                data.len(),
                Self::MAX_CHUNK_SIZE
            );
        }

        self.volume
            .write_to_byte_offset(offset, data.into())
            .await?;

        Ok(())
    }
}

/// Pantry stores opened Volumes in-memory
pub struct Pantry {
    pub log: Logger,

    /// Store a Volume Construction Request and Volume, indexed by id.
    entries: Mutex<BTreeMap<String, PantryEntry>>,
}

impl Pantry {
    pub fn new(log: Logger) -> Result<Pantry> {
        Ok(Pantry {
            log,
            entries: Mutex::new(BTreeMap::default()),
        })
    }

    pub async fn attach(
        &self,
        volume_id: String,
        volume_construction_request: VolumeConstructionRequest,
    ) -> Result<()> {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(&volume_id) {
            // This function must be idempotent for the same inputs. If an entry
            // at this ID exists already, compare the existing volume
            // construction request, and return either Ok or conflict
            if entry.volume_construction_request == volume_construction_request
            {
                info!(self.log, "volume {} already an entry, and has same volume construction request, returning OK", volume_id);
                return Ok(());
            } else {
                warn!(self.log, "volume {} already an entry, but has different volume construction request, bailing!", volume_id);
                bail!("Existing entry for {} with different volume construction request!", volume_id);
            }
        }

        // If no entry exists, then add one
        info!(
            self.log,
            "no entry exists for volume {}, constructing...", volume_id
        );

        let volume =
            Volume::construct(volume_construction_request.clone(), None)
                .await?;

        info!(self.log, "volume {} constructed ok", volume_id);

        // XXX activation number going away?
        volume.activate(0).await?;

        info!(self.log, "volume {} activated ok", volume_id);

        entries.insert(
            volume_id.clone(),
            PantryEntry {
                volume,
                volume_construction_request,
            },
        );

        info!(self.log, "volume {} constructed and inserted ok", volume_id);

        Ok(())
    }

    pub async fn import_from_url(
        &self,
        volume_id: String,
        url: String,
        expected_digest: Option<ExpectedDigest>,
    ) -> Result<(), HttpError> {
        let entries = self.entries.lock().await;
        match entries.get(&volume_id) {
            Some(entry) => {
                entry.import_from_url(url, expected_digest).await.map_err(
                    |e| HttpError::for_internal_error(e.to_string()),
                )?;

                Ok(())
            }

            None => {
                warn!(
                    self.log,
                    "attempting to import_from_url for non-existent {}",
                    volume_id,
                );

                Err(HttpError::for_not_found(None, volume_id))
            }
        }
    }

    pub async fn snapshot(
        &self,
        volume_id: String,
        snapshot_id: String,
    ) -> Result<(), HttpError> {
        let entries = self.entries.lock().await;
        match entries.get(&volume_id) {
            Some(entry) => {
                entry.snapshot(snapshot_id).await.map_err(|e| {
                    HttpError::for_internal_error(e.to_string())
                })?;

                Ok(())
            }

            None => {
                warn!(
                    self.log,
                    "attempting to snapshot for non-existent {}", volume_id,
                );

                Err(HttpError::for_not_found(None, volume_id))
            }
        }
    }

    pub async fn bulk_write(
        &self,
        volume_id: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), HttpError> {
        let entries = self.entries.lock().await;
        match entries.get(&volume_id) {
            Some(entry) => {
                entry.bulk_write(offset, data).await.map_err(|e| {
                    HttpError::for_internal_error(e.to_string())
                })?;

                Ok(())
            }

            None => {
                warn!(
                    self.log,
                    "attempting to bulk_write for non-existent {}", volume_id,
                );

                Err(HttpError::for_not_found(None, volume_id))
            }
        }
    }

    pub async fn detach(&self, volume_id: String) -> Result<()> {
        let mut entries = self.entries.lock().await;
        match entries.get(&volume_id) {
            Some(entry) => {
                // Attempt a flush. If this errors, return that to the caller as
                // an internal error. If it succeeds, remove the entry.
                info!(
                    self.log,
                    "detach calling flush for volume {}", volume_id
                );
                entry.volume.flush(None).await?;

                info!(
                    self.log,
                    "detach removing entry for volume {}", volume_id
                );
                entries.remove(&volume_id);
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
