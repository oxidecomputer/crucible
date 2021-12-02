// Copyright 2021 Oxide Computer Company

use anyhow::{anyhow, bail, Result};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{error, info, o, warn, Logger};
use std::collections::HashSet;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use structopt::StructOpt;

const PROG: &str = "crucible-agent";
const SERVICE: &str = "oxide/crucible/downstairs";

mod datafile;
mod model;
mod server;

use model::State;

#[derive(Debug, StructOpt)]
#[structopt(name = PROG, about = "Crucible zone management agent")]
enum Args {
    OpenApi {
        #[structopt(short = "o", parse(try_from_str))]
        output: PathBuf,
    },
    Run {
        #[structopt(short = "d", parse(try_from_str))]
        data_dir: PathBuf,

        #[structopt(short = "l", parse(try_from_str))]
        listen: SocketAddr,

        #[allow(dead_code)]
        #[structopt(short = "i", parse(try_from_str))]
        uuid: uuid::Uuid,

        #[structopt(short = "D", parse(try_from_str))]
        downstairs_program: PathBuf,

        #[structopt(short = "P", parse(try_from_str))]
        lowport: u16,

        #[structopt(short = "p", parse(try_from_str))]
        prefix: String,

        #[allow(dead_code)]
        #[structopt(short = "n", parse(try_from_str))]
        nexus: Option<SocketAddr>,
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
            write_openapi(&mut f)
        }
        Args::Run {
            data_dir,
            listen,
            uuid: _,
            nexus: _,
            downstairs_program,
            lowport,
            prefix,
        } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }
            .to_logger(PROG)?;

            info!(log, "data directory: {:?}", data_dir);
            info!(log, "listen IP: {:?}", listen);
            info!(log, "SMF instance name prefix: {:?}", prefix);

            let mut dfpath = data_dir.clone();
            std::fs::create_dir_all(&dfpath)?;
            dfpath.push("crucible.json");

            let df = Arc::new(datafile::DataFile::new(
                log.new(o!("component" => "datafile")),
                &dfpath,
                lowport,
                lowport + 999,
            )?);

            let mut datapath = data_dir.clone();
            datapath.push("regions");
            std::fs::create_dir_all(&datapath)?;

            /*
             * Ensure that the SMF service we will use exists already.  If
             * not, something is seriously wrong with this
             * machine.
             */
            {
                let scf = crucible_smf::Scf::new()?;
                let scope = scf.scope_local()?;

                let svc = scope.get_service(SERVICE)?;
                if svc.is_none() {
                    bail!("SMF service {} does not exist", SERVICE);
                }
            }

            apply_smf(&log, &df, datapath.clone(), &prefix)?;

            /*
             * Create the worker thread that will perform provisioning and
             * deprovisioning tasks.
             */
            let log0 = log.new(o!("component" => "worker"));
            let df0 = Arc::clone(&df);
            std::thread::spawn(|| {
                worker(log0, df0, datapath, downstairs_program, prefix)
            });

            server::run_server(&log, listen, df).await
        }
    }
}

fn write_openapi<W: Write>(f: &mut W) -> Result<()> {
    let api = server::make_api()?;
    api.openapi("Crucible Agent", "0.0.0").write(f)?;
    Ok(())
}

/**
 * Provisioning requests will update the local intent store, and provision
 * downstairs data files, but SMF services are a bit different.
 *
 * If the zone is upgraded, we will likely start from a blank SMF repository
 * database and will need to set up all of our instances again.  As such,
 * the process of aligning SMF with the intent datastore is a separate
 * idempotent routine that we can call both at startup and after any changes
 * to the intent store.
 */
fn apply_smf(
    log: &Logger,
    df: &Arc<datafile::DataFile>,
    datapath: PathBuf,
    prefix: &str,
) -> Result<()> {
    let all = df.all();

    let scf = crucible_smf::Scf::new()?;
    let scope = scf.scope_local()?;
    let svc = scope
        .get_service(SERVICE)?
        .ok_or(anyhow!("service missing"))?;

    /*
     * First, check to see if there are any instances that we do not expect,
     * and remove them.
     */
    let expected_instances = all
        .iter()
        .filter(|r| r.state == State::Created)
        .map(|r| format!("{}-{}", prefix, r.id.0))
        .collect::<HashSet<_>>();
    let mut insts = svc.instances()?;
    while let Some(inst) = insts.next().transpose()? {
        let n = inst.name()?;

        if &n == "default" {
            continue;
        }

        if !n.starts_with(&format!("{}-", prefix)) {
            warn!(log, "ignoring instance with other prefix: {:?}", n);
            continue;
        }

        if !expected_instances.contains(&n) {
            error!(log, "remove instance: {}", n);
            info!(log, "instance states: {:?}", inst.states()?);
            /*
             * XXX just disable for now.
             */
            inst.disable(false)?;
            continue;
        }

        info!(log, "found expected instance: {}", n);
    }

    /*
     * Second, create any instances that are missing.
     */
    for r in all.iter() {
        if r.state != State::Created {
            continue;
        }

        let name = format!("{}-{}", prefix, r.id.0);
        let mut dir = datapath.clone();
        dir.push(&r.id.0);
        let datadir = dir.to_str().unwrap().to_string();

        let inst = if let Some(inst) = svc.get_instance(&name)? {
            inst
        } else {
            info!(log, "creating missing instance {}", name);
            let inst = svc.add_instance(&name)?;
            info!(log, "ok, have {}", inst.fmri()?);
            inst
        };

        /*
         * Determine the contents of the running snapshot.
         */
        let reconfig = if let Some(snap) = inst.get_snapshot("running")? {
            /*
             * Just check the values.
             */
            if let Some(pg) = snap.get_pg("config")? {
                let mut found_directory = false;
                let dir = pg.get_property("directory")?;
                if let Some(dir) = dir {
                    if let Some(val) = dir.value()? {
                        if val.as_string()? == datadir {
                            found_directory = true;
                        }
                    }
                }

                let mut found_port = false;
                let dir = pg.get_property("port")?;
                if let Some(dir) = dir {
                    if let Some(val) = dir.value()? {
                        if val.as_string()? == r.port_number.to_string() {
                            found_port = true;
                        }
                    }
                }

                !found_port || !found_directory
            } else {
                true
            }
        } else {
            /*
             * No running snapshot means the service has never started.  Prod
             * the restarter by disabling it, then we'll create everything
             * from scratch.
             */
            inst.disable(false)?;
            true
        };

        if reconfig {
            use crucible_smf::scf_type_t::*;

            /*
             * Ensure that there is a "config" property group:
             */
            let pg = if let Some(pg) = inst.get_pg("config")? {
                pg
            } else {
                inst.add_pg("config", "application")?
            };

            info!(log, "reconfiguring {}", inst.fmri()?);

            let tx = pg.transaction()?;
            tx.start()?;
            let mut entries = Vec::new();

            /*
             * An expression of our values:
             */
            let mut e = if pg.get_property("directory")?.is_some() {
                info!(log, "change directory");
                tx.property_change("directory", SCF_TYPE_ASTRING)?
            } else {
                info!(log, "add directory");
                tx.property_new("directory", SCF_TYPE_ASTRING)?
            };
            e.add_from_string(SCF_TYPE_ASTRING, &datadir)?;
            entries.push(e);

            let mut e = if pg.get_property("port")?.is_some() {
                info!(log, "change port");
                tx.property_change("port", SCF_TYPE_COUNT)?
            } else {
                info!(log, "add port");
                tx.property_new("port", SCF_TYPE_COUNT)?
            };
            e.add_from_string(SCF_TYPE_COUNT, &r.port_number.to_string())?;
            entries.push(e);

            info!(log, "commit");
            match tx.commit()? {
                crucible_smf::CommitResult::Success => {
                    info!(log, "ok!");
                }
                crucible_smf::CommitResult::OutOfDate => {
                    error!(log, "concurrent modification?!");
                }
            }
        } else {
            info!(log, "do not need to reconfigure {}", inst.fmri()?);
        }

        /*
         * Finally, make sure the instance is enabled.
         */
        inst.enable(false)?;
    }

    Ok(())
}

fn worker(
    log: Logger,
    df: Arc<datafile::DataFile>,
    datapath: PathBuf,
    downstairs_program: PathBuf,
    prefix: String,
) {
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

                let res =
                    worker_region_create(&log, &downstairs_program, &r, &dir)
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

        info!(log, "applying SMF actions...");
        if let Err(e) = apply_smf(&log, &df, datapath.clone(), &prefix) {
            error!(log, "SMF application failure: {:?}", e);
        } else {
            info!(log, "SMF ok!");
        }
    }
}

fn worker_region_create(
    log: &Logger,
    prog: &Path,
    region: &model::Region,
    dir: &Path,
) -> Result<()> {
    let log = log.new(o!("region" => region.id.0.to_string()));

    /*
     * We may have crashed half way through a previous provision.  To make
     * this idempotent, clean out the target data directory and try
     * again.
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
    //let image = "/dev/null"; /* XXX */
    let image = "/var/tmp/alpine.iso";
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
    let scf = crucible_smf::Scf::new()?;
    let _scope = scf.scope_local()?;

    /*
     * First, ensure that the service exists.
     */

    // TODO this is where Josh stopped...

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

#[cfg(test)]
mod tests {
    use crate::write_openapi;

    #[test]
    fn test_openapi() {
        let mut raw = Vec::new();
        write_openapi(&mut raw).unwrap();
        let actual = String::from_utf8(raw).unwrap();
        expectorate::assert_contents("../openapi/crucible-agent.json", &actual);
    }
}
