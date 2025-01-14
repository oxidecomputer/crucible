// Copyright 2024 Oxide Computer Company
//! Runs a task to send best-effort notifications to Nexus
//!
//! The queue receives Crucible-flavored types, and converts them to
//! Nexus-flavored types internally.

use chrono::{DateTime, Utc};
use slog::{debug, error, info, o, warn, Logger};
use std::net::{Ipv6Addr, SocketAddr};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::client::ClientRunResult;
use nexus_client::types::{
    DownstairsClientStopped, DownstairsClientStoppedReason,
    DownstairsUnderRepair, RepairFinishInfo, RepairProgress, RepairStartInfo,
    UpstairsRepairType,
};
use omicron_uuid_kinds::{GenericUuid, TypedUuid};

#[derive(Debug)]
pub(crate) enum NotifyRequest {
    ClientTaskStopped {
        upstairs_id: Uuid,
        downstairs_id: Uuid,
        reason: ClientRunResult,
    },
    LiveRepairStart {
        upstairs_id: Uuid,
        repair_id: Uuid,
        session_id: Uuid,
        repairs: Vec<(Uuid, SocketAddr)>,
    },
    LiveRepairProgress {
        upstairs_id: Uuid,
        repair_id: Uuid,
        current_item: i64,
        total_items: i64,
    },
    LiveRepairFinish {
        upstairs_id: Uuid,
        repair_id: Uuid,
        session_id: Uuid,
        repairs: Vec<(Uuid, SocketAddr)>,
        aborted: bool,
    },
    ReconcileStart {
        upstairs_id: Uuid,
        repair_id: Uuid,
        session_id: Uuid,
        repairs: Vec<(Uuid, SocketAddr)>,
    },
    ReconcileProgress {
        upstairs_id: Uuid,
        repair_id: Uuid,
        current_item: i64,
        total_items: i64,
    },
    ReconcileFinish {
        upstairs_id: Uuid,
        repair_id: Uuid,
        session_id: Uuid,
        repairs: Vec<(Uuid, SocketAddr)>,
        aborted: bool,
    },
}

pub(crate) struct NotifyQueue {
    tx: mpsc::Sender<(DateTime<Utc>, NotifyRequest)>,
    log: Logger,
}

impl NotifyQueue {
    /// Insert a time-stamped request into the queue
    pub fn send(&self, r: NotifyRequest) {
        let now = Utc::now();
        if let Err(r) = self.tx.try_send((now, r)) {
            warn!(self.log, "could not send notify {r:?}; queue is full");
        }
    }
}

pub(crate) fn spawn_notify_task(addr: Ipv6Addr, log: &Logger) -> NotifyQueue {
    let (tx, rx) = mpsc::channel(128);
    let task_log = log.new(slog::o!("job" => "notify"));
    tokio::spawn(async move { notify_task_nexus(addr, rx, task_log).await });
    NotifyQueue {
        tx,
        log: log.new(o!("job" => "notify_queue")),
    }
}

async fn notify_task_nexus(
    addr: Ipv6Addr,
    mut rx: mpsc::Receiver<(DateTime<Utc>, NotifyRequest)>,
    log: Logger,
) {
    let reqwest_client = reqwest::ClientBuilder::new()
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();
    while let Some((time, m)) = rx.recv().await {
        debug!(log, "notify {m:?}");
        let client = reqwest_client.clone();
        let Some(nexus_client) = get_nexus_client(&log, client, addr).await
        else {
            // Skip if no Nexus client returned from DNS; our notification is
            // best effort.
            warn!(
                log,
                "could not find nexus client; \
                 dropping nexus notification {m:?}"
            );
            continue;
        };
        let (r, s) = match m {
            NotifyRequest::ClientTaskStopped {
                upstairs_id,
                downstairs_id,
                reason,
            } => {
                let upstairs_id = TypedUuid::from_untyped_uuid(upstairs_id);
                let downstairs_id = TypedUuid::from_untyped_uuid(downstairs_id);
                let reason = match reason {
                    ClientRunResult::ConnectionTimeout => {
                        DownstairsClientStoppedReason::ConnectionTimeout
                    }
                    ClientRunResult::ConnectionFailed(_) => {
                        // skip this notification, it's too noisy during connection
                        // retries
                        //DownstairsClientStoppedReason::ConnectionFailed
                        continue;
                    }
                    ClientRunResult::Timeout => {
                        DownstairsClientStoppedReason::Timeout
                    }
                    ClientRunResult::WriteFailed(_) => {
                        DownstairsClientStoppedReason::WriteFailed
                    }
                    ClientRunResult::ReadFailed(_) => {
                        DownstairsClientStoppedReason::ReadFailed
                    }
                    ClientRunResult::RequestedStop => {
                        // skip this notification, it fires for *every* Upstairs
                        // deactivation
                        //DownstairsClientStoppedReason::RequestedStop
                        continue;
                    }
                    ClientRunResult::Finished => {
                        DownstairsClientStoppedReason::Finished
                    }
                    ClientRunResult::QueueClosed => {
                        DownstairsClientStoppedReason::QueueClosed
                    }
                    ClientRunResult::ReceiveTaskCancelled => {
                        DownstairsClientStoppedReason::ReceiveTaskCancelled
                    }
                };

                (
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
                )
            }
            NotifyRequest::LiveRepairStart {
                upstairs_id,
                repair_id,
                session_id,
                ref repairs,
            }
            | NotifyRequest::ReconcileStart {
                upstairs_id,
                repair_id,
                session_id,
                ref repairs,
            } => {
                let upstairs_id = TypedUuid::from_untyped_uuid(upstairs_id);
                let (description, repair_type) =
                    if matches!(m, NotifyRequest::LiveRepairStart { .. }) {
                        ("live repair start", UpstairsRepairType::Live)
                    } else {
                        ("reconcile start", UpstairsRepairType::Reconciliation)
                    };
                let info = RepairStartInfo {
                    time,
                    repair_id: TypedUuid::from_untyped_uuid(repair_id),
                    repair_type,
                    session_id: TypedUuid::from_untyped_uuid(session_id),
                    repairs: repairs
                        .iter()
                        .map(|(region_uuid, target_addr)| {
                            DownstairsUnderRepair {
                                region_uuid: (*region_uuid).into(),
                                target_addr: target_addr.to_string(),
                            }
                        })
                        .collect(),
                };

                (
                    omicron_common::retry_until_known_result(&log, || async {
                        nexus_client
                            .cpapi_upstairs_repair_start(&upstairs_id, &info)
                            .await
                    })
                    .await,
                    description,
                )
            }
            NotifyRequest::LiveRepairProgress {
                upstairs_id,
                repair_id,
                current_item,
                total_items,
            }
            | NotifyRequest::ReconcileProgress {
                upstairs_id,
                repair_id,
                current_item,
                total_items,
            } => {
                let upstairs_id = TypedUuid::from_untyped_uuid(upstairs_id);
                let repair_id = TypedUuid::from_untyped_uuid(repair_id);
                let description =
                    if matches!(m, NotifyRequest::LiveRepairProgress { .. }) {
                        "live repair progress"
                    } else {
                        "reconcile progress"
                    };

                (
                    omicron_common::retry_until_known_result(&log, || async {
                        nexus_client
                            .cpapi_upstairs_repair_progress(
                                &upstairs_id,
                                &repair_id,
                                &RepairProgress {
                                    current_item,
                                    total_items,
                                    time,
                                },
                            )
                            .await
                    })
                    .await,
                    description,
                )
            }
            NotifyRequest::LiveRepairFinish {
                upstairs_id,
                repair_id,
                session_id,
                aborted,
                ref repairs,
            }
            | NotifyRequest::ReconcileFinish {
                upstairs_id,
                repair_id,
                session_id,
                aborted,
                ref repairs,
            } => {
                let upstairs_id = TypedUuid::from_untyped_uuid(upstairs_id);
                let (description, repair_type) =
                    if matches!(m, NotifyRequest::LiveRepairFinish { .. }) {
                        ("live repair finish", UpstairsRepairType::Live)
                    } else {
                        ("reconcile finish", UpstairsRepairType::Reconciliation)
                    };
                let info = RepairFinishInfo {
                    time,
                    repair_id: TypedUuid::from_untyped_uuid(repair_id),
                    repair_type,
                    session_id: TypedUuid::from_untyped_uuid(session_id),
                    repairs: repairs
                        .iter()
                        .map(|(region_uuid, target_addr)| {
                            DownstairsUnderRepair {
                                region_uuid: (*region_uuid).into(),
                                target_addr: target_addr.to_string(),
                            }
                        })
                        .collect(),
                    aborted,
                };

                (
                    omicron_common::retry_until_known_result(&log, || async {
                        nexus_client
                            .cpapi_upstairs_repair_finish(&upstairs_id, &info)
                            .await
                    })
                    .await,
                    description,
                )
            }
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

/// Gets a Nexus client based on any rack-internal IPv6 address
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
