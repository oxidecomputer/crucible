use anyhow::{bail, Result};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{error, info, o, Logger};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use structopt::StructOpt;

const PROG: &str = "crucible-agent";
const PORT_MIN: u16 = 17000;
const PORT_MAX: u16 = 17999;

mod datafile;
mod model;
mod server;

use model::State;

#[derive(Debug, StructOpt)]
#[structopt(name = PROG, about = "Crucible zone management agent")]
enum Args {
    OpenApi {
        #[structopt(short = "o", name = "FILE", parse(try_from_str))]
        output: PathBuf,
    },
    Run {
        #[structopt(short = "d", name = "DATA_DIR", parse(try_from_str))]
        data_dir: PathBuf,

        #[structopt(short = "l", name = "IP:PORT", parse(try_from_str))]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args_safe()?;

    match args {
        Args::OpenApi { output } => {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output)?;
            let api = server::make_api()?;
            api.openapi("Crucible Agent", "0.0.0").write(&mut f)?;
            Ok(())
        }
        Args::Run { data_dir, listen } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }
            .to_logger(PROG)?;

            info!(log, "data directory: {:?}", data_dir);
            info!(log, "listen IP: {:?}", listen);

            let mut dfpath = data_dir.clone();
            std::fs::create_dir_all(&dfpath)?;
            dfpath.push("crucible.json");

            let df = Arc::new(datafile::DataFile::new(
                log.new(o!("component" => "datafile")),
                &dfpath,
                PORT_MIN,
                PORT_MAX,
            )?);

            let mut datapath = data_dir.clone();
            datapath.push("regions");
            std::fs::create_dir_all(&datapath)?;

            /*
             * Create the worker thread that will perform provisioning and
             * deprovisioning tasks.
             */
            let log0 = log.new(o!("component" => "worker"));
            let df0 = Arc::clone(&df);
            std::thread::spawn(|| worker(log0, df0, datapath));

            server::run_server(&log, listen, df).await
        }
    }
}

fn worker(log: Logger, df: Arc<datafile::DataFile>, datapath: PathBuf) {
    let prog = "/ws/oxide/crucible/target/debug/crucible-downstairs";

    loop {
        let r = df.first_in_states(&[State::Tombstoned, State::Requested]);

        match &r.state {
            State::Tombstoned => {
                let mut dir = datapath.clone();
                dir.push(&r.id.0);

                let res = worker_region_destroy(&log, &r, &dir)
                    .and_then(|_| df.destroyed(&r.id));

                if let Err(e) = res {
                    error!(log, "region {:?} destroy failed: {:?}", r.id.0, e);
                    df.fail(&r.id);
                }
            }
            State::Requested => {
                let mut dir = datapath.clone();
                dir.push(&r.id.0);

                let res = worker_region_create(&log, prog, &r, &dir)
                    .and_then(|_| df.created(&r.id));

                if let Err(e) = res {
                    error!(log, "region {:?} create failed: {:?}", r.id.0, e);
                    df.fail(&r.id);
                }
            }
            _ => {
                eprintln!("worker got unexpected region state: {:?}", r);
                std::process::exit(1);
            }
        }
    }
}

fn worker_region_create(
    log: &Logger,
    prog: &str,
    region: &model::Region,
    dir: &Path,
) -> Result<()> {
    let log = log.new(o!("region" => region.id.0.to_string()));

    /*
     * We may have crashed half way through a previous provision.  To make this
     * idempotent, clean out the target data directory and try again.
     *
     * XXX This could obviously be improved.
     */
    if dir.exists() {
        info!(log, "removing directory {:?}", dir);
        std::fs::remove_dir_all(&dir)?;
    }

    /*
     * Run the downstairs program in the mode where it will create the data
     * files.
     */
    let image = "/dev/null"; /* XXX */
    info!(log, "creating region files from image {:?}", image);
    let cmd = Command::new(prog)
        .env_clear()
        .arg("--create")
        .arg("--data")
        .arg(dir)
        .arg("--import-path")
        .arg(image)
        .arg("--block-size")
        .arg(region.block_size.to_string())
        .arg("--extent-size")
        .arg(region.extent_size.to_string())
        .arg("--extent-count")
        .arg(region.extent_count.to_string())
        .output()?;

    if cmd.status.success() {
        info!(log, "region files created ok");
    } else {
        let err = String::from_utf8_lossy(&cmd.stderr);
        let out = String::from_utf8_lossy(&cmd.stdout);
        error!(log, "downstairs create failed: out {:?} err {:?}", out, err);
        bail!("region files create failure");
    }

    /*
     * - create/enable SMF instance
     */

    Ok(())
}

fn worker_region_destroy(
    log: &Logger,
    region: &model::Region,
    dir: &Path,
) -> Result<()> {
    let log = log.new(o!("region" => region.id.0.to_string()));

    /*
     * - stop/destroy SMF instance
     */

    if dir.exists() {
        info!(log, "removing directory {:?}", dir);
        std::fs::remove_dir_all(&dir)?;
    }

    Ok(())
}
