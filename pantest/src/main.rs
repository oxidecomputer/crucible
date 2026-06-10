// Copyright 2026 Oxide Computer Company

use anyhow::{Context, Result, bail};
use clap::Parser;
use crucible_pantry_client::Client;
use crucible_pantry_client::types::{
    AttachRequest, CrucibleOpts, VolumeConstructionRequest, VolumeInfo,
};
use dsc_client::Client as DscClient;
use slog::{Logger, info, warn};
use uuid::Uuid;

use crucible_common::build_logger;

#[derive(Debug, Parser)]
#[clap(name = "pantest", about = "Crucible Pantry test program")]
struct Args {
    /// IP:Port for the pantry server
    #[clap(short, long)]
    pantry: std::net::SocketAddr,

    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Parser)]
enum Cmd {
    /// Get pantry status
    Status,

    /// Attach a volume constructed from dsc info
    Attach {
        /// IP:Port for a dsc server
        #[clap(short, long)]
        dsc: std::net::SocketAddr,

        /// Volume ID (auto-generated if not provided)
        #[clap(short, long)]
        volume_id: Option<Uuid>,

        /// Encryption key (base64-encoded, 32 bytes).
        /// Generate one with: $(openssl rand -base64 32)
        #[clap(short, long)]
        key: Option<String>,

        /// Generation number
        #[clap(short, long, default_value_t = 1)]
        generation: u64,
    },

    /// Detach a volume
    Detach {
        /// Volume ID to detach
        volume_id: Uuid,
    },

    /// Get status of an attached volume
    VolumeStatus {
        /// Volume ID to query
        volume_id: Uuid,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let log = build_logger();

    let pantry_url = format!("http://{}", args.pantry);
    let pantry = Client::new(&pantry_url);

    match args.cmd {
        Cmd::Status => cmd_status(&log, &pantry).await?,
        Cmd::Attach {
            dsc,
            volume_id,
            key,
            generation,
        } => {
            let volume_id = volume_id.unwrap_or_else(Uuid::new_v4);
            let dsc_client = DscClient::new(&format!("http://{dsc}"));
            cmd_attach(
                &log,
                &pantry,
                &dsc_client,
                dsc,
                volume_id,
                key,
                generation,
            )
            .await?;
        }
        Cmd::Detach { volume_id } => {
            cmd_detach(&log, &pantry, volume_id).await?;
        }
        Cmd::VolumeStatus { volume_id } => {
            cmd_volume_status(&log, &pantry, volume_id).await?;
        }
    }

    Ok(())
}

/// Query dsc for region info and downstairs targets, then build
/// a VolumeConstructionRequest suitable for the pantry.
async fn build_vcr_from_dsc(
    log: &Logger,
    dsc: &DscClient,
    dsc_addr: std::net::SocketAddr,
    volume_id: Uuid,
    key: Option<String>,
    generation: u64,
) -> Result<VolumeConstructionRequest> {
    let ri = dsc
        .dsc_get_region_info()
        .await
        .context("get region info")?
        .into_inner();

    info!(
        log, "Region info from dsc";
        "block_size" => ri.block_size,
        "blocks_per_extent" => ri.blocks_per_extent,
        "extent_count" => ri.extent_count,
    );

    let region_count = dsc
        .dsc_get_region_count()
        .await
        .context("get region count")?
        .into_inner();

    if region_count < 3 {
        bail!("Need at least 3 regions, dsc reports {region_count}");
    }

    let sv_count = region_count / 3;
    let remainder = region_count % 3;
    info!(
        log,
        "dsc has {region_count} regions, {sv_count} sub-volumes",
    );
    if remainder != 0 {
        warn!(
            log,
            "{remainder} regions will not be part of any sub-volume",
        );
    }

    let read_only = dsc
        .dsc_get_read_only()
        .await
        .context("get read_only")?
        .into_inner();

    if read_only {
        info!(log, "dsc reports read-only mode");
    }

    let mut sub_volumes = Vec::new();

    for sv in 0..sv_count {
        let mut targets = Vec::new();
        for cid in (sv * 3)..(sv * 3 + 3) {
            let port = dsc
                .dsc_get_port(cid)
                .await
                .with_context(|| format!("get port for cid {cid}"))?
                .into_inner();
            let addr =
                std::net::SocketAddr::new(dsc_addr.ip(), port.try_into()?);
            targets.push(addr.to_string());
        }

        info!(
            log, "Sub-volume {sv}";
            "targets" => ?targets,
        );

        sub_volumes.push(VolumeConstructionRequest::Region {
            block_size: ri.block_size,
            blocks_per_extent: ri.blocks_per_extent,
            extent_count: ri.extent_count,
            gen_: generation,
            opts: CrucibleOpts {
                id: Uuid::new_v4(),
                target: targets,
                lossy: false,
                read_only,
                flush_timeout: None,
                key: key.clone(),
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
            },
        });
    }

    let vcr = VolumeConstructionRequest::Volume {
        id: volume_id,
        block_size: ri.block_size,
        sub_volumes,
        read_only_parent: None,
    };

    Ok(vcr)
}

async fn cmd_status(log: &Logger, pantry: &Client) -> Result<()> {
    let status = pantry
        .pantry_status()
        .await
        .context("get pantry status")?
        .into_inner();

    info!(log, "Pantry status:");
    info!(log, "  Volumes: {:?}", status.volumes);
    info!(log, "  Job handles: {}", status.num_job_handles);

    Ok(())
}

async fn cmd_attach(
    log: &Logger,
    pantry: &Client,
    dsc: &DscClient,
    dsc_addr: std::net::SocketAddr,
    volume_id: Uuid,
    key: Option<String>,
    generation: u64,
) -> Result<()> {
    if key.is_some() {
        info!(log, "Encryption enabled");
    }
    let vcr =
        build_vcr_from_dsc(log, dsc, dsc_addr, volume_id, key, generation)
            .await?;

    let volume_id_str = volume_id.to_string();
    info!(log, "Attaching volume {volume_id_str}");

    let result = pantry
        .attach(
            &volume_id_str,
            &AttachRequest {
                volume_construction_request: vcr,
            },
        )
        .await
        .context("attach")?
        .into_inner();

    info!(log, "Attached volume"; "id" => result.id);

    Ok(())
}

async fn cmd_detach(
    log: &Logger,
    pantry: &Client,
    volume_id: Uuid,
) -> Result<()> {
    let volume_id_str = volume_id.to_string();
    info!(log, "Detaching volume {volume_id_str}");

    pantry.detach(&volume_id_str).await.context("detach")?;

    info!(log, "Detached volume {volume_id_str}");

    Ok(())
}

async fn cmd_volume_status(
    log: &Logger,
    pantry: &Client,
    volume_id: Uuid,
) -> Result<()> {
    let status = pantry
        .volume_status(&volume_id.to_string())
        .await
        .context("volume status")?
        .into_inner();

    info!(log, "Volume status for {volume_id}");
    info!(log, "  Active: {}", status.active);
    info!(log, "  Seen active: {}", status.seen_active);
    info!(log, "  Job handles: {}", status.num_job_handles);
    print_volume_info(log, &status.info, 1);

    Ok(())
}

fn print_volume_info(log: &Logger, info: &VolumeInfo, depth: usize) {
    let indent = "  ".repeat(depth);
    match info {
        VolumeInfo::Volume {
            sub_volumes,
            read_only_parent,
        } => {
            info!(log, "{indent}Volume:");
            for (i, sv) in sub_volumes.iter().enumerate() {
                info!(log, "{indent}  Sub-volume {i}:");
                print_volume_info(log, sv, depth + 2);
            }
            if let Some(rop) = read_only_parent {
                info!(log, "{indent}  Read-only parent:");
                print_volume_info(log, rop, depth + 2);
            }
        }
        VolumeInfo::Upstairs {
            upstairs_id,
            session_id,
            generation,
            state,
            read_only,
            encrypted,
            live_repair_in_progress,
            reconcile_in_progress,
            targets,
            block_size,
        } => {
            info!(log, "{indent}Upstairs: {upstairs_id}");
            info!(log, "{indent}  Session: {session_id}");
            info!(log, "{indent}  Generation: {generation}");
            info!(log, "{indent}  State: {state:?}");
            info!(log, "{indent}  Read-only: {read_only}");
            info!(log, "{indent}  Encrypted: {encrypted}");
            if let Some(bs) = block_size {
                info!(log, "{indent}  Block size: {bs}");
            }
            if *live_repair_in_progress {
                warn!(log, "{indent}  Live repair in progress!");
            }
            if *reconcile_in_progress {
                warn!(log, "{indent}  Reconcile in progress!");
            }
            for (i, ds) in targets.iter().enumerate() {
                info!(
                    log,
                    "{indent}  Downstairs [{i}]: {:?} addr={:?}",
                    ds.state,
                    ds.target_addr,
                );
            }
        }
    }
}
