// Copyright 2024 Oxide Computer Company
//! Runs a task to send best-effort notifications to Nexus
//!
//! The queue receives Crucible-flavored types, and converts them to
//! Nexus-flavored types internally.

use chrono::{DateTime, Utc};
use rand::prelude::SliceRandom;
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
pub(crate) enum NotifyQos {
    High,
    Low,
}

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

impl NotifyRequest {
    pub(crate) fn qos(&self) -> NotifyQos {
        match &self {
            NotifyRequest::LiveRepairStart { .. }
            | NotifyRequest::LiveRepairFinish { .. }
            | NotifyRequest::ReconcileStart { .. }
            | NotifyRequest::ReconcileFinish { .. } => NotifyQos::High,

            NotifyRequest::ClientTaskStopped { .. }
            | NotifyRequest::LiveRepairProgress { .. }
            | NotifyRequest::ReconcileProgress { .. } => NotifyQos::Low,
        }
    }
}

pub(crate) struct NotifyQueue {
    tx_high: mpsc::Sender<(DateTime<Utc>, NotifyRequest)>,
    tx_low: mpsc::Sender<(DateTime<Utc>, NotifyRequest)>,
    log: Logger,
}

impl NotifyQueue {
    /// Insert a time-stamped request into the queue
    pub fn send(&self, r: NotifyRequest) {
        let now = Utc::now();
        let qos = r.qos();
        let queue = match &qos {
            NotifyQos::High => &self.tx_high,
            NotifyQos::Low => &self.tx_low,
        };

        if let Err(e) = queue.try_send((now, r)) {
            warn!(self.log, "could not send {qos:?} notify: {e}",);
        }
    }
}

pub(crate) fn spawn_notify_task(addr: Ipv6Addr, log: &Logger) -> NotifyQueue {
    let (tx_high, rx_high) = mpsc::channel(128);
    let (tx_low, rx_low) = mpsc::channel(128);
    let task_log = log.new(slog::o!("job" => "notify"));

    tokio::spawn(async move {
        notify_task_nexus(addr, rx_high, rx_low, task_log).await
    });

    NotifyQueue {
        tx_high,
        tx_low,
        log: log.new(o!("job" => "notify_queue")),
    }
}

struct Notification {
    message: (DateTime<Utc>, NotifyRequest),
    qos: NotifyQos,
    retries: usize,
}

async fn notify_task_nexus(
    addr: Ipv6Addr,
    mut rx_high: mpsc::Receiver<(DateTime<Utc>, NotifyRequest)>,
    mut rx_low: mpsc::Receiver<(DateTime<Utc>, NotifyRequest)>,
    log: Logger,
) {
    info!(log, "notify_task started");

    // Store high QoS messages if they can't be sent
    let mut stored_notification: Option<Notification> = None;

    let reqwest_client = reqwest::ClientBuilder::new()
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();

    loop {
        let r = tokio::select! {
            biased;

            Some(n) = async { stored_notification.take() } => Some(n),

            i = rx_high.recv() => i.map(|message| Notification {
                message,
                qos: NotifyQos::High,
                retries: 0,
            }),

            i = rx_low.recv() => i.map(|message| Notification {
                message,
                qos: NotifyQos::Low,
                retries: 0,
            }),
        };

        let Some(Notification {
            message: (time, m),
            qos,
            retries,
        }) = r
        else {
            error!(log, "one of the notify channels was closed!");
            break;
        };

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

        let (r, s) = match &m {
            NotifyRequest::ClientTaskStopped {
                upstairs_id,
                downstairs_id,
                reason,
            } => {
                let upstairs_id = TypedUuid::from_untyped_uuid(*upstairs_id);
                let downstairs_id =
                    TypedUuid::from_untyped_uuid(*downstairs_id);
                let reason = match reason {
                    ClientRunResult::ConnectionTimeout => {
                        DownstairsClientStoppedReason::ConnectionTimeout
                    }
                    ClientRunResult::ConnectionFailed(_) => {
                        // skip this notification, it's too noisy during
                        // connection retries
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
                    nexus_client
                        .cpapi_downstairs_client_stopped(
                            &upstairs_id,
                            &downstairs_id,
                            &DownstairsClientStopped { time, reason },
                        )
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
                let upstairs_id = TypedUuid::from_untyped_uuid(*upstairs_id);
                let (description, repair_type) =
                    if matches!(m, NotifyRequest::LiveRepairStart { .. }) {
                        ("live repair start", UpstairsRepairType::Live)
                    } else {
                        ("reconcile start", UpstairsRepairType::Reconciliation)
                    };
                let info = RepairStartInfo {
                    time,
                    repair_id: TypedUuid::from_untyped_uuid(*repair_id),
                    repair_type,
                    session_id: TypedUuid::from_untyped_uuid(*session_id),
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
                    nexus_client
                        .cpapi_upstairs_repair_start(&upstairs_id, &info)
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
                let upstairs_id = TypedUuid::from_untyped_uuid(*upstairs_id);
                let repair_id = TypedUuid::from_untyped_uuid(*repair_id);
                let description =
                    if matches!(m, NotifyRequest::LiveRepairProgress { .. }) {
                        "live repair progress"
                    } else {
                        "reconcile progress"
                    };

                (
                    nexus_client
                        .cpapi_upstairs_repair_progress(
                            &upstairs_id,
                            &repair_id,
                            &RepairProgress {
                                current_item: *current_item,
                                total_items: *total_items,
                                time,
                            },
                        )
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
                let upstairs_id = TypedUuid::from_untyped_uuid(*upstairs_id);
                let (description, repair_type) =
                    if matches!(m, NotifyRequest::LiveRepairFinish { .. }) {
                        ("live repair finish", UpstairsRepairType::Live)
                    } else {
                        ("reconcile finish", UpstairsRepairType::Reconciliation)
                    };
                let info = RepairFinishInfo {
                    time,
                    repair_id: TypedUuid::from_untyped_uuid(*repair_id),
                    repair_type,
                    session_id: TypedUuid::from_untyped_uuid(*session_id),
                    repairs: repairs
                        .iter()
                        .map(|(region_uuid, target_addr)| {
                            DownstairsUnderRepair {
                                region_uuid: (*region_uuid).into(),
                                target_addr: target_addr.to_string(),
                            }
                        })
                        .collect(),
                    aborted: *aborted,
                };

                (
                    nexus_client
                        .cpapi_upstairs_repair_finish(&upstairs_id, &info)
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

                // If there's a problem notifying Nexus, it could be due to
                // Nexus being gone before the DNS was updated. If this is the
                // case, then retrying should eventually pick a different Nexus
                // and succeed. Store high priority messages so they can be
                // resent.
                if matches!(qos, NotifyQos::High) {
                    // If we've retried too many times, then drop this message.
                    // Unfortunately if this is true then other notifications
                    // will also likely fail.
                    if retries > 3 {
                        warn!(log, "retries > 3, dropping {m:?}");
                    } else {
                        stored_notification = Some(Notification {
                            message: (time, m),
                            qos,
                            retries: retries + 1,
                        });
                    }
                }
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
        match resolver.lookup_all_socket_v6(ServiceName::Nexus).await {
            Ok(addrs) => {
                if addrs.is_empty() {
                    error!(log, "no Nexus addresses returned!");
                    return None;
                }

                let Some(addr) = addrs.choose(&mut rand::thread_rng()) else {
                    error!(log, "somehow, choose failed!");
                    return None;
                };

                *addr
            }

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
