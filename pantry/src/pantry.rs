// Copyright 2022 Oxide Computer Company

use std::collections::BTreeMap;

use anyhow::bail;
use anyhow::Result;
use slog::info;
use slog::warn;
use slog::Logger;
use tokio::sync::Mutex;

use crucible::BlockIO;
use crucible::Volume;
use crucible::VolumeConstructionRequest;

pub struct PantryEntry {
    volume: Volume,
    volume_construction_request: VolumeConstructionRequest,
}

/// Pantry stores opened Volumes in-memory
pub struct Pantry {
    log: Logger,

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
        info!(self.log, "constructing volume {}", volume_id);

        let volume =
            Volume::construct(volume_construction_request.clone(), None)
                .await?;

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
