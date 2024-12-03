// Copyright 2024 Oxide Computer Company
//! Runs a task to send best-effort notifications to Nexus
//!
//! If the `notify-nexus` feature is disabled, then the task simply drops values
//! from the queue.

use chrono::{DateTime, Utc};
use slog::{debug, error, info, o, warn, Logger};
use std::net::Ipv6Addr;
use tokio::sync::mpsc;

use nexus_client::types::{
    DownstairsClientStopped, DownstairsClientStoppedReason, RepairFinishInfo,
    RepairProgress, RepairStartInfo,
};
use omicron_uuid_kinds::{
    DownstairsKind, TypedUuid, UpstairsKind, UpstairsRepairKind,
};

#[derive(Debug)]
pub(crate) enum NotifyRequest {
    ClientTaskStopped {
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        time: DateTime<Utc>,
        reason: DownstairsClientStoppedReason,
    },
    RepairStart {
        upstairs_id: TypedUuid<UpstairsKind>,
        info: RepairStartInfo,
    },
    RepairProgress {
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        info: RepairProgress,
    },
    RepairFinish {
        upstairs_id: TypedUuid<UpstairsKind>,
        info: RepairFinishInfo,
    },
    ReconcileStart {
        upstairs_id: TypedUuid<UpstairsKind>,
        info: RepairStartInfo,
    },
    ReconcileProgress {
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        info: RepairProgress,
    },
    ReconcileFinish {
        upstairs_id: TypedUuid<UpstairsKind>,
        info: RepairFinishInfo,
    },
}

pub(crate) struct NotifyQueue {
    tx: mpsc::Sender<NotifyRequest>,
    log: Logger,
}

impl NotifyQueue {
    pub fn send(&self, r: NotifyRequest) {
        if let Err(r) = self.tx.try_send(r) {
            warn!(self.log, "could not send notify {r:?}; queue is full");
        }
    }
}

pub(crate) fn spawn_notify_task(addr: Ipv6Addr, log: &Logger) -> NotifyQueue {
    let (tx, rx) = mpsc::channel(128);
    let task_log = log.new(o!("job" => "notify"));
    tokio::spawn(async move {
        #[cfg(feature = "notify-nexus")]
        {
            notify_task_nexus(addr, rx, task_log).await
        }

        #[cfg(not(feature = "notify-nexus"))]
        {
            let _ = addr;
            notify_task_dummy(task_log, rx).await
        }
    });
    NotifyQueue {
        tx,
        log: log.new(o!("job" => "notify_queue")),
    }
}

pub(crate) fn spawn_dummy_task(log: &Logger) -> NotifyQueue {
    let (tx, rx) = mpsc::channel(128);
    let task_log = log.new(o!("job" => "notify"));
    tokio::spawn(async move { notify_task_dummy(task_log, rx).await });
    NotifyQueue {
        tx,
        log: log.new(o!("job" => "notify_queue")),
    }
}

async fn notify_task_nexus(
    addr: Ipv6Addr,
    mut rx: mpsc::Receiver<NotifyRequest>,
    log: Logger,
) {
    let reqwest_client = reqwest::ClientBuilder::new()
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();
    while let Some(m) = rx.recv().await {
        debug!(log, "notify {m:?}");
        let client = reqwest_client.clone();
        let Some(nexus_client) = get_nexus_client(&log, client, addr).await
        else {
            // Exit if no Nexus client returned from DNS - our notification
            // is best effort.
            continue;
        };
        let (r, s) = match m {
            NotifyRequest::ClientTaskStopped {
                upstairs_id,
                downstairs_id,
                time,
                reason,
            } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_downstairs_client_stopped(
                            &upstairs_id,
                            &downstairs_id,
                            &DownstairsClientStopped { time, reason },
                        )
                        .await
                })
                .await,
                "client stopped",
            ),
            NotifyRequest::RepairStart { upstairs_id, info } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_start(&upstairs_id, &info)
                        .await
                })
                .await,
                "repair start",
            ),
            NotifyRequest::RepairProgress {
                upstairs_id,
                repair_id,
                info,
            } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_progress(
                            &upstairs_id,
                            &repair_id,
                            &info,
                        )
                        .await
                })
                .await,
                "repair progress",
            ),
            NotifyRequest::RepairFinish { upstairs_id, info } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_finish(&upstairs_id, &info)
                        .await
                })
                .await,
                "repair finish",
            ),
            NotifyRequest::ReconcileStart { upstairs_id, info } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_start(&upstairs_id, &info)
                        .await
                })
                .await,
                "reconcile start",
            ),
            NotifyRequest::ReconcileProgress {
                upstairs_id,
                repair_id,
                info,
            } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_progress(
                            &upstairs_id,
                            &repair_id,
                            &info,
                        )
                        .await
                })
                .await,
                "reconcile progress",
            ),
            NotifyRequest::ReconcileFinish { upstairs_id, info } => (
                omicron_common::retry_until_known_result(&log, || async {
                    nexus_client
                        .cpapi_upstairs_repair_finish(&upstairs_id, &info)
                        .await
                })
                .await,
                "reconcile finish",
            ),
        };
        match r {
            Ok(_) => {
                info!(log, "notified Nexus of {s}");
            }

            Err(e) => {
                error!(log, "failed to notify Nexus of {s}: {e}");
            }
        }
    }
    info!(log, "notify_task exiting");
}

/// Gets a Nexus client based on any IPv6 address
pub(crate) async fn get_nexus_client(
    log: &Logger,
    client: reqwest::Client,
    addr: Ipv6Addr,
) -> Option<nexus_client::Client> {
    use internal_dns::resolver::Resolver;
    use internal_dns::ServiceName;

    // Use any rack internal address for `Resolver::new_from_ip`, as that will
    // use the AZ_PREFIX to find internal DNS servers.
    let resolver = match Resolver::new_from_ip(log.clone(), addr) {
        Ok(resolver) => resolver,
        Err(e) => {
            error!(log, "could not make resolver: {e}");
            return None;
        }
    };

    let nexus_address =
        match resolver.lookup_socket_v6(ServiceName::Nexus).await {
            Ok(addr) => addr,
            Err(e) => {
                error!(log, "lookup Nexus address failed: {e}");
                return None;
            }
        };

    Some(nexus_client::Client::new_with_client(
        &format!("http://{}", nexus_address),
        client,
        log.clone(),
    ))
}

////////////////////////////////////////////////////////////////////////////////

async fn notify_task_dummy(log: Logger, mut rx: mpsc::Receiver<NotifyRequest>) {
    while let Some(m) = rx.recv().await {
        debug!(log, "notify {m:?}");
    }
    info!(log, "notify_task exiting");
}
