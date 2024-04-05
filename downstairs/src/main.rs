// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Parser;
use slog::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

use crucible_common::build_logger;
use crucible_downstairs::admin::*;
use crucible_downstairs::*;
use crucible_protocol::{JobId, CRUCIBLE_MESSAGE_VERSION};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Ro,
    Rw,
}

impl std::str::FromStr for Mode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "ro" => Mode::Ro,
            "rw" => Mode::Rw,
            _ => {
                bail!("not a valid mode!");
            }
        })
    }
}

#[derive(Debug, Parser)]
#[clap(about = "disk-side storage component")]
enum Args {
    /// Clone the extents from another region into this region.
    ///
    /// The other downstairs should be running read-only.  All data in
    /// the region here will be replaced.
    Clone {
        /// Directory where the region is located.
        #[clap(short, long, name = "DIRECTORY", action)]
        data: PathBuf,

        /// Source IP:Port where the extent files will come from.
        #[clap(short, long, action)]
        source: SocketAddr,

        #[clap(short, long, action)]
        trace_endpoint: Option<String>,
    },
    Create {
        /// Block size.
        #[clap(long, default_value = "512", action)]
        block_size: u64,

        /// Directory where the region files we be located.
        #[clap(short, long, name = "DIRECTORY", action)]
        data: PathBuf,

        /// Number of blocks per extent file.
        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        /// Number of extent files.
        #[clap(long, default_value = "15", action)]
        extent_count: u64,

        /// Import data for the extent from this file.
        #[clap(
            short,
            long,
            name = "FILE",
            action,
            conflicts_with = "clone_source"
        )]
        import_path: Option<PathBuf>,

        /// UUID for the region.
        #[clap(short, long, name = "UUID", action)]
        uuid: Uuid,

        /// Will the region be encrypted.
        #[clap(long, action)]
        encrypted: bool,

        /// Clone another downstairs after creating.
        ///
        /// IP:Port where the extent files will come from.
        #[clap(long, action, conflicts_with = "import-path")]
        clone_source: Option<SocketAddr>,
    },
    /*
     * Dump region information.
     * Multiple directories can be passed (up to 3)
     * With -e, you can dump just a single extent which will include
     * a block by block comparison.
     * With -b, you can dump a single block to see a detailed comparison.
     */
    Dump {
        /*
         * Directories containing a region.
         */
        #[clap(short, long, name = "DIRECTORY", action)]
        data: Vec<PathBuf>,

        /*
         * Just dump this extent number
         */
        #[clap(short, long, action)]
        extent: Option<u32>,

        /*
         * Detailed view for a block
         */
        #[clap(short, long, action)]
        block: Option<u64>,

        /*
         * Only show differences
         */
        #[clap(short, long, action)]
        only_show_differences: bool,

        /// No color output
        #[clap(long, action)]
        no_color: bool,
    },
    Export {
        /*
         * Number of blocks to export.
         */
        #[clap(long, default_value = "0", name = "COUNT", action)]
        count: u64,

        #[clap(short, long, name = "DIRECTORY", action)]
        data: PathBuf,

        #[clap(short, long, name = "OUT_FILE", action)]
        export_path: PathBuf,

        #[clap(short, long, default_value = "0", name = "SKIP", action)]
        skip: u64,
    },
    Run {
        /// Address the downstairs will listen for the upstairs on.
        #[clap(
            short,
            long,
            default_value = "0.0.0.0",
            name = "ADDRESS",
            action
        )]
        address: IpAddr,

        /// Directory where the region is located.
        #[clap(short, long, name = "DIRECTORY", action)]
        data: PathBuf,

        /// Test option, makes the search for new work sleep and sometimes
        /// skip doing work.
        #[clap(long, action)]
        lossy: bool,

        /*
         * If this option is provided along with the address:port of the
         * oximeter server, the downstairs will publish stats.
         */
        /// Use this address:port to send stats to an Oximeter server.
        #[clap(long, name = "OXIMETER_ADDRESS:PORT", action)]
        oximeter: Option<SocketAddr>,

        /// Listen on this port for the upstairs to connect to us.
        #[clap(short, long, default_value = "9000", action)]
        port: u16,

        /// Randomly return read errors
        #[clap(long, action)]
        read_errors: bool,

        /// Randomly return write errors
        #[clap(long, action)]
        write_errors: bool,

        /// Randomly return flush errors
        #[clap(long, action)]
        flush_errors: bool,

        #[clap(short, long, action)]
        trace_endpoint: Option<String>,

        // TLS options
        #[clap(long, action)]
        cert_pem: Option<String>,
        #[clap(long, action)]
        key_pem: Option<String>,
        #[clap(long, action)]
        root_cert_pem: Option<String>,

        #[clap(long, default_value = "rw", action)]
        mode: Mode,
    },
    RepairAPI,
    Serve {
        #[clap(short, long, action)]
        trace_endpoint: Option<String>,

        // Dropshot server details
        #[clap(long, default_value = "127.0.0.1:4567", action)]
        bind_addr: SocketAddr,
    },
    Version,
    /// Measure an isolated downstairs
    Dynamometer {
        #[clap(long, default_value_t = 512)]
        block_size: u64,

        #[clap(short, long, name = "DIRECTORY")]
        data: PathBuf,

        #[clap(long, default_value_t = 100)]
        extent_size: u64,

        #[clap(long, default_value_t = 15)]
        extent_count: u64,

        #[clap(long)]
        encrypted: bool,

        /// Number of writes to submit at one time to region_write
        #[clap(short, long, default_value_t = 1)]
        num_writes: usize,

        /// Number of samples to exit for
        #[clap(short, long, default_value_t = 10)]
        samples: usize,

        /// Flush per iops
        #[clap(long, conflicts_with_all = ["flush_per_blocks", "flush_per_ms"])]
        flush_per_iops: Option<usize>,

        /// Flush per blocks written
        #[clap(long, conflicts_with_all = ["flush_per_iops", "flush_per_ms"])]
        flush_per_blocks: Option<usize>,

        /// Flush per ms
        #[clap(long, value_parser = parse_duration, conflicts_with_all = ["flush_per_iops", "flush_per_blocks"])]
        flush_per_ms: Option<Duration>,
    },
}

fn parse_duration(arg: &str) -> Result<Duration, std::num::ParseIntError> {
    let ms = arg.parse()?;
    Ok(Duration::from_millis(ms))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;

    let log = build_logger();

    match args {
        Args::Clone {
            data,
            source,
            trace_endpoint,
        } => {
            // Instrumentation is shared.
            if let Some(endpoint) = trace_endpoint {
                let tracer = opentelemetry_jaeger::new_agent_pipeline()
                    .with_endpoint(endpoint) // usually port 6831
                    .with_service_name("downstairs")
                    .install_simple()
                    .expect("Error initializing Jaeger exporter");

                let telemetry =
                    tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(telemetry)
                    .try_init()
                    .expect("Error init tracing subscriber");
            }

            let d = Downstairs::new_builder(&data, true)
                .set_logger(log)
                .build()
                .await?;

            clone_region(d, source).await
        }
        Args::Create {
            block_size,
            data,
            extent_size,
            extent_count,
            import_path,
            uuid,
            encrypted,
            clone_source,
        } => {
            let mut region = create_region(
                block_size,
                data.clone(),
                extent_size,
                extent_count,
                uuid,
                encrypted,
                log.clone(),
            )
            .await?;

            if let Some(ref ip) = import_path {
                downstairs_import(&mut region, ip).await.unwrap();
                /*
                 * The region we just created should now have a flush so the
                 * new data and inital flush number is written to disk.
                 */
                region.region_flush(1, 0, &None, JobId(0), None).await?;
            } else if let Some(ref clone_source) = clone_source {
                info!(log, "Cloning from: {:?}", clone_source);
                let d = Downstairs::new_builder(&data, false)
                    .set_logger(log.clone())
                    .build()
                    .await?;
                clone_region(d, *clone_source).await?
            }

            info!(log, "UUID: {:?}", region.def().uuid());
            info!(
                log,
                "Blocks per extent:{} Total Extents: {}",
                region.def().extent_size().value,
                region.def().extent_count(),
            );
            // Now, clone it!
            Ok(())
        }
        Args::Dump {
            data,
            extent,
            block,
            only_show_differences,
            no_color,
        } => {
            if data.is_empty() {
                bail!("Need at least one data directory to dump");
            }
            dump_region(
                data,
                extent,
                block,
                only_show_differences,
                no_color,
                log,
            )
            .await
        }
        Args::Export {
            count,
            data,
            export_path,
            skip,
        } => {
            // Open Region read only
            let mut region = region::Region::open(
                data,
                Default::default(),
                true,
                true,
                &log,
            )
            .await?;

            downstairs_export(&mut region, export_path, skip, count).await
        }
        Args::Run {
            address,
            data,
            oximeter,
            lossy,
            port,
            read_errors,
            write_errors,
            flush_errors,
            trace_endpoint,
            cert_pem,
            key_pem,
            root_cert_pem,
            mode,
        } => {
            // Instrumentation is shared.
            if let Some(endpoint) = trace_endpoint {
                let tracer = opentelemetry_jaeger::new_agent_pipeline()
                    .with_endpoint(endpoint) // usually port 6831
                    .with_service_name("downstairs")
                    .install_simple()
                    .expect("Error initializing Jaeger exporter");

                let telemetry =
                    tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(telemetry)
                    .try_init()
                    .expect("Error init tracing subscriber");
            }

            let read_only = mode == Mode::Ro;

            let d = Downstairs::new_builder(&data, read_only)
                .set_lossy(lossy)
                .set_logger(log)
                .set_test_errors(read_errors, write_errors, flush_errors)
                .build()
                .await?;

            let downstairs_join_handle = start_downstairs(
                d,
                address,
                oximeter,
                port,
                // TODO accept as an argument?
                port + crucible_common::REPAIR_PORT_OFFSET,
                cert_pem,
                key_pem,
                root_cert_pem,
            )
            .await?;

            downstairs_join_handle.await?
        }
        Args::RepairAPI => repair::write_openapi(&mut std::io::stdout()),
        Args::Serve {
            trace_endpoint,
            bind_addr,
        } => {
            // Instrumentation is shared.
            if let Some(endpoint) = trace_endpoint {
                let tracer = opentelemetry_jaeger::new_agent_pipeline()
                    .with_endpoint(endpoint) // usually port 6831
                    .with_service_name("downstairs")
                    .install_simple()
                    .expect("Error initializing Jaeger exporter");

                let telemetry =
                    tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(telemetry)
                    .try_init()
                    .expect("Error init tracing subscriber");
            }

            run_dropshot(bind_addr, &log).await
        }
        Args::Version => {
            let info = crucible_common::BuildInfo::default();
            println!("Crucible Version: {}", info);
            println!(
                "Upstairs <-> Downstairs Message Version: {}",
                CRUCIBLE_MESSAGE_VERSION
            );
            Ok(())
        }
        Args::Dynamometer {
            block_size,
            data,
            extent_size,
            extent_count,
            encrypted,
            num_writes,
            samples,
            flush_per_iops,
            flush_per_blocks,
            flush_per_ms,
        } => {
            let uuid = Uuid::new_v4();

            let region = create_region(
                block_size,
                data,
                extent_size,
                extent_count,
                uuid,
                encrypted,
                log.clone(),
            )
            .await?;

            let flush_config = if let Some(flush_per_iops) = flush_per_iops {
                DynoFlushConfig::FlushPerIops(flush_per_iops)
            } else if let Some(flush_per_blocks) = flush_per_blocks {
                DynoFlushConfig::FlushPerBlocks(flush_per_blocks)
            } else if let Some(flush_per_ms) = flush_per_ms {
                DynoFlushConfig::FlushPerMs(flush_per_ms)
            } else {
                DynoFlushConfig::None
            };

            dynamometer(region, num_writes, samples, flush_config).await
        }
    }
}
