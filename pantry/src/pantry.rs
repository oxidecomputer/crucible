use anyhow::{bail, Result};
use crucible::{BlockIO, Volume, VolumeConstructionRequest};
use slog::{error, info, Logger};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::block_in_place;

pub struct Attachment {
    req: VolumeConstructionRequest,
    gen: u64,
    volume: Option<Volume>,
    failed: bool,
    activated: bool,
}

pub struct Pantry {
    log: Logger,
    attachments: Mutex<HashMap<String, Arc<Mutex<Attachment>>>>,
}

impl Pantry {
    pub fn new(log: Logger) -> Result<Pantry> {
        Ok(Pantry {
            log,
            attachments: Default::default(),
        })
    }

    pub async fn background_task(&self) -> Result<()> {
        let ids = self
            .attachments
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for id in ids {
            if let Some(a) = self.attachments.lock().await.get_mut(&id) {
                let mut a = a.lock().await;

                if a.volume.is_none() && !a.failed {
                    info!(self.log, "constructing volume for {:?}", id);
                    match block_in_place(|| Volume::construct(a.req.clone())) {
                        Ok(v) => {
                            info!(self.log, "construction ok for {:?}", id);
                            a.volume = Some(v);
                        }
                        Err(e) => {
                            error!(
                                self.log,
                                "construction failed for {:?}: {:?}", id, e
                            );
                            a.failed = true;
                            continue;
                        }
                    }
                }

                if let Some(v) = &a.volume {
                    if !a.activated && !a.failed {
                        info!(self.log, "activating {:?} at gen {}", id, a.gen);
                        if let Err(e) = block_in_place(|| v.activate(a.gen)) {
                            error!(
                                self.log,
                                "activation failed for {:?}: {:?}", id, e
                            );
                            a.failed = true;
                        } else {
                            info!(
                                self.log,
                                "activated {:?} at gen {}", id, a.gen
                            );
                            a.activated = true;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn attach(
        &self,
        id: String,
        req: VolumeConstructionRequest,
        gen: u64,
    ) -> Result<()> {
        let mut a = self.attachments.lock().await;
        if let Some(a) = a.get(&id) {
            let a = a.lock().await;
            if a.req == req && a.gen == gen {
                Ok(())
            } else {
                bail!("mismatch for attach of ID {:?}", id);
            }
        } else {
            a.insert(
                id,
                Arc::new(Mutex::new(Attachment {
                    req,
                    gen,
                    volume: None,
                    failed: false,
                    activated: false,
                })),
            );
            Ok(())
        }
    }
}
