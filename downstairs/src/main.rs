// Copyright 2023 Oxide Computer Company
#![cfg_attr(usdt_need_asm, feature(asm))]
#![cfg_attr(all(target_os = "macos", usdt_need_asm_sym), feature(asm_sym))]

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Parser;
use slog::{info, o, Drain, Logger};
use slog_dtrace::{with_drain, ProbeRegistration};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use usdt::register_probes;
use uuid::Uuid;

use crucible_downstairs::admin::*;
use crucible_downstairs::*;

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
    Create {
        #[clap(long, default_value = "512", action)]
        block_size: u64,

        #[clap(short, long, name = "DIRECTORY", action)]
        data: PathBuf,

        #[clap(long, default_value = "100", action)]
        extent_size: u64,

        #[clap(long, default_value = "15", action)]
        extent_count: u64,

        #[clap(short, long, name = "FILE", action)]
        import_path: Option<PathBuf>,

        #[clap(short, long, name = "UUID", action)]
        uuid: Uuid,

        #[clap(long, action)]
        encrypted: bool,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;

    /*
     * Everyone needs a region
     */
    let mut region;

    // Register DTrace, and setup slog logging to use it.
    register_probes().unwrap();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(slog::Level::Info)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let (drain, registration) = with_drain(drain);
    if let ProbeRegistration::Failed(ref e) = registration {
        panic!("Failed to register probes: {:#?}", e);
    }
    let log = Logger::root(drain.fuse(), o!());

    match args {
        Args::Create {
            block_size,
            data,
            extent_size,
            extent_count,
            import_path,
            uuid,
            encrypted,
        } => {
            let mut region = create_region(
                block_size,
                data,
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
                region.region_flush(1, 0, &None, 0, None).await?;
            }

            info!(log, "UUID: {:?}", region.def().uuid());
            info!(
                log,
                "Blocks per extent:{} Total Extents: {}",
                region.def().extent_size().value,
                region.def().extent_count(),
            );
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
            .await?;
            Ok(())
        }
        Args::Export {
            count,
            data,
            export_path,
            skip,
        } => {
            // Open Region read only
            region = region::Region::open(
                data,
                Default::default(),
                true,
                true,
                &log,
            )
            .await?;

            downstairs_export(&mut region, export_path, skip, count).await?;
            Ok(())
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
            /*
             * If any of our async tasks in our runtime panic, then we should
             * exit the program right away.
             */
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default_panic(info);
                std::process::exit(1);
            }));

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
            let d = build_downstairs_for_region(
                &data,
                lossy,
                read_errors,
                write_errors,
                flush_errors,
                read_only,
                Some(log),
            )
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
            /*
             * If any of our async tasks in our runtime panic, then we should
             * exit the program right away.
             */
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default_panic(info);
                std::process::exit(1);
            }));

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
    }
}
