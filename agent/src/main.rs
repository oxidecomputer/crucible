use anyhow::{bail, Result};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{info, o, Logger};
use std::net::SocketAddr;
use std::path::PathBuf;
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
        Args::Run { data_dir, listen } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }
            .to_logger(PROG)?;

            info!(log, "data directory: {:?}", data_dir);
            info!(log, "listen IP: {:?}", listen);

            let mut dfpath = data_dir.clone();
            dfpath.push("crucible.json");

            let df = Arc::new(datafile::DataFile::new(
                log.new(o!("component" => "datafile")),
                &dfpath,
                PORT_MIN,
                PORT_MAX,
            )?);

            /*
             * Create the worker thread that will perform provisioning and
             * deprovisioning tasks.
             */
            let log0 = log.new(o!("component" => "worker"));
            let df0 = Arc::clone(&df);
            std::thread::spawn(|| worker(log0, df0));

            server::run_server(&log, listen, df).await
        }
    }
}

fn worker(log: Logger, df: Arc<datafile::DataFile>) {
    loop {
        let r = df.first_in_states(&[State::Tombstoned, State::Requested]);

        match &r.state {
            State::Tombstoned => {
                /*
                 * - disable SMF instance
                 * - delete SMF instance
                 * - delete region files in the file system
                 * - mark region as Destroyed
                 */
                df.fail(&r.id); /* XXX */
            }
            State::Requested => {
                /*
                 * - create region files in the file system
                 * - create SMF instance
                 * - mark region as Created
                 */
                df.fail(&r.id); /* XXX */
            }
            x => {
                eprintln!("worker got unexpected region state: {:?}", r);
                std::process::exit(1);
            }
        }
    }
}
