// Copyright 2021 Oxide Computer Company
#![allow(clippy::needless_collect)]
use anyhow::{anyhow, bail, Result};
use clap::Parser;
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use slog::{error, info, o, warn, Logger};
use std::collections::HashSet;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const PROG: &str = "crucible-agent";
const SERVICE: &str = "oxide/crucible/downstairs";

mod datafile;
mod model;
mod server;

use model::State;

#[derive(Debug, Parser)]
#[clap(name = PROG, about = "Crucible zone management agent")]
enum Args {
    OpenApi {
        #[clap(short = 'o', action)]
        output: PathBuf,
    },
    Run {
        // zfs dataset to be used by the crucible agent
        #[clap(long, action)]
        dataset: PathBuf,

        #[clap(short = 'l', action)]
        listen: SocketAddr,

        #[clap(short = 'D', action)]
        downstairs_program: PathBuf,

        #[clap(short = 'P', action)]
        lowport: u16,

        #[clap(short = 'p', action)]
        downstairs_prefix: String,

        #[clap(short = 's', action)]
        snapshot_prefix: String,
    },
}

pub struct ZFSDataset {
    dataset: String,
}

impl ZFSDataset {
    // From either dataset name or path, create the ZFSDataset object
    pub fn new(dataset: String) -> Result<ZFSDataset> {
        // Validate the argument is a dataset
        let cmd = std::process::Command::new("zfs")
            .arg("list")
            .arg("-pH")
            .arg("-o")
            .arg("name")
            .arg(&dataset)
            .output()?;

        if !cmd.status.success() {
            bail!("zfs list failed!");
        }

        Ok(ZFSDataset {
            dataset: String::from_utf8(cmd.stdout)?.trim_end().to_string(),
        })
    }

    pub fn from_child_dataset(&self, child: &str) -> Result<ZFSDataset> {
        let dataset = format!("{}/{}", self.dataset, child);

        // Does it exist already?
        let cmd = std::process::Command::new("zfs")
            .arg("list")
            .arg(&dataset)
            .output()?;

        if cmd.status.success() {
            return Ok(ZFSDataset { dataset });
        }

        bail!("Dataset does not exist!");
    }

    // Given "dataset", ensure that "dataset/child" exists, and return it.
    pub fn ensure_child_dataset(&self, child: &str) -> Result<ZFSDataset> {
        let dataset = format!("{}/{}", self.dataset, child);

        // Does it exist already?
        let cmd = std::process::Command::new("zfs")
            .arg("list")
            .arg(&dataset)
            .output()?;

        if cmd.status.success() {
            return Ok(ZFSDataset { dataset });
        }

        // If not, create it
        let cmd = std::process::Command::new("zfs")
            .arg("create")
            .arg(&dataset)
            .output()?;

        if !cmd.status.success() {
            let out = String::from_utf8_lossy(&cmd.stdout);
            let err = String::from_utf8_lossy(&cmd.stderr);
            bail!("zfs create failed! out:{} err:{}", out, err);
        }

        Ok(ZFSDataset { dataset })
    }

    pub fn path(&self) -> Result<PathBuf> {
        let cmd = std::process::Command::new("zfs")
            .arg("list")
            .arg("-pH")
            .arg("-o")
            .arg("mountpoint")
            .arg(&self.dataset)
            .output()?;

        let out = String::from_utf8(cmd.stdout)?;

        if !cmd.status.success() {
            let err = String::from_utf8_lossy(&cmd.stderr);
            bail!("zfs list mountpoint failed! out:{} err:{}", out, err);
        }

        Ok(Path::new(&out.trim_end()).to_path_buf())
    }

    pub fn destroy(self, log: &Logger) -> Result<()> {
        // Retry a few times: apply_smf will remove the corresponding downstairs
        // instance but this may take a few seconds to propagate.
        for i in 0..5 {
            let cmd = std::process::Command::new("zfs")
                .arg("destroy")
                .arg(&self.dataset)
                .output()?;

            if !cmd.status.success() {
                let out = String::from_utf8_lossy(&cmd.stdout);
                let err = String::from_utf8_lossy(&cmd.stderr);

                error!(
                    log,
                    "zfs dataset {} delete attempt {} failed: out:{} err:{}",
                    self.dataset,
                    i,
                    out,
                    err,
                );

                if i == 4 {
                    bail!(
                        "zfs list mountpoint failed! out:{} err:{}",
                        out,
                        err
                    );
                }

                std::thread::sleep(std::time::Duration::from_secs(2));
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn dataset(&self) -> String {
        self.dataset.clone()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    /*
     * If any of our async tasks in our runtime panic, then we should exit
     * the program right away.
     */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let args = Args::try_parse()?;

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
            dataset,
            listen,
            downstairs_program,
            lowport,
            downstairs_prefix,
            snapshot_prefix,
        } => {
            let log = ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Info,
            }
            .to_logger(PROG)?;

            info!(log, "dataset: {:?}", dataset);
            info!(log, "listen IP: {:?}", listen);
            info!(
                log,
                "SMF instance name downstairs_prefix: {:?}", downstairs_prefix
            );

            // Look up data directory from dataset mountpoint
            let dataset = ZFSDataset::new(
                dataset.into_os_string().into_string().unwrap(),
            )
            .unwrap();

            let df = Arc::new(datafile::DataFile::new(
                log.new(o!("component" => "datafile")),
                &dataset.path()?,
                listen,
                lowport,
                lowport + 999, // TODO high port as an argument?
            )?);

            let regions_dataset =
                dataset.ensure_child_dataset("regions").unwrap();

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

            // Apply any outstanding actions.
            //
            // Note: ? here means that the failure of apply_smf will cause the
            // binary to terminate.
            apply_smf(
                &log,
                &df,
                regions_dataset.path().unwrap(),
                &downstairs_prefix,
                &snapshot_prefix,
            )?;

            /*
             * Create the worker thread that will perform provisioning and
             * deprovisioning tasks.
             */
            let log0 = log.new(o!("component" => "worker"));
            let df0 = Arc::clone(&df);
            std::thread::spawn(|| {
                worker(
                    log0,
                    df0,
                    regions_dataset,
                    downstairs_program,
                    downstairs_prefix,
                    snapshot_prefix,
                )
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
 * database and will need to set up all of our instances again. As such,
 * the process of aligning SMF with the intent datastore is a separate
 * idempotent routine that we can call both at startup and after any changes
 * to the intent store.
 */
fn apply_smf(
    log: &Logger,
    df: &Arc<datafile::DataFile>,
    datapath: PathBuf,
    downstairs_prefix: &str,
    snapshot_prefix: &str,
) -> Result<()> {
    let regions = df.regions();
    let mut running_snapshots = df.running_snapshots();

    let scf = crucible_smf::Scf::new()?;
    let scope = scf.scope_local()?;
    let svc = scope
        .get_service(SERVICE)?
        .ok_or_else(|| anyhow!("service missing"))?;

    /*
     * First, check to see if there are any instances that we do not expect,
     * and remove them.
     */
    let expected_downstairs_instances = regions
        .iter()
        .filter(|r| r.state == State::Created)
        .map(|r| format!("{}-{}", downstairs_prefix, r.id.0))
        .collect::<HashSet<_>>();

    let expected_snapshot_instances: Vec<String> = running_snapshots
        .iter()
        .flat_map(|(_, n)| {
            n.iter()
                .map(|(_, s)| {
                    format!("{}-{}-{}", snapshot_prefix, s.id.0, s.name)
                })
                .collect::<Vec<String>>()
        })
        .collect();

    let mut insts = svc.instances()?;

    while let Some(inst) = insts.next().transpose()? {
        let n = inst.name()?;

        if &n == "default" {
            continue;
        }

        // Look for either a running downstairs or snapshot:
        //
        // downstairs-6dec576f-0655-4f90-afd6-93ce0f5aacb9
        // snapshot-6dec576f-0655-4f90-afd6-93ce0f5aacb9-1644509677
        if n.starts_with(&format!("{}-", downstairs_prefix)) {
            if !expected_downstairs_instances.contains(&n) {
                error!(log, "remove downstairs instance: {}", n);
                info!(log, "instance states: {:?}", inst.states()?);

                /*
                 * XXX just disable for now.
                 */
                inst.disable(false)?;
                continue;
            }

            info!(log, "found expected downstairs instance: {}", n);
        } else if n.starts_with(&format!("{}-", snapshot_prefix)) {
            if !expected_snapshot_instances.contains(&n) {
                error!(log, "remove snapshot instance: {}", n);

                /*
                 * XXX just disable for now.
                 */
                inst.disable(false)?;
                continue;
            }

            info!(log, "found expected snapshot instance: {}", n);
        } else {
            warn!(log, "ignoring instance: {:?}", n);
        }
    }

    /*
     * Second, create any downstairs and snapshot instances that are missing.
     */
    for r in regions.iter() {
        if r.state != State::Created {
            continue;
        }

        let name = format!("{}-{}", downstairs_prefix, r.id.0);

        let mut dir = datapath.clone();
        dir.push(&r.id.0);

        let properties = {
            let mut properties = r.get_smf_properties(&dir);

            // Instruct downstairs process to listen on the same IP as the
            // agent, because there is currently only one address in the
            // crucible zone that both processes must share and that address is
            // what will be used by other zones. In the future this could be a
            // parameter that comes along with the region POST parameters.
            properties.push(crate::model::SmfProperty {
                name: "address",
                typ: crucible_smf::scf_type_t::SCF_TYPE_ASTRING,
                val: df.get_listen_addr().ip().to_string(),
            });

            properties
        };

        let inst = if let Some(inst) = svc.get_instance(&name)? {
            inst
        } else {
            info!(log, "creating missing downstairs instance {}", name);
            let inst = svc.add_instance(&name)?;
            info!(log, "ok, have {}", inst.fmri()?);
            inst
        };

        /*
         * Determine the contents of the running snapshot.
         */
        let reconfig = if let Some(snap) = inst.get_snapshot("running")? {
            /*
             * Just check the propval values.
             */
            if let Some(pg) = snap.get_pg("config")? {
                let mut reconfig = false;

                for property in &properties {
                    let existing_val = pg.get_property(property.name)?;
                    if let Some(existing_val) = existing_val {
                        if let Some(val) = existing_val.value()? {
                            if val.as_string()? != property.val {
                                reconfig = true;
                                info!(
                                    log,
                                    "existing {} value {} does not match {}",
                                    property.name,
                                    val.as_string()?,
                                    property.val,
                                );
                            }
                        } else {
                            reconfig = true;
                            info!(
                                log,
                                "{} value call returned None", property.name,
                            );
                        }
                    } else {
                        reconfig = true;
                        info!(log, "{} value missing", property.name,);
                    }
                }

                // reconfig is required if propvals are missing, or wrong
                if reconfig {
                    info!(log, "reconfig required");
                }

                reconfig
            } else {
                info!(log, "reconfig required, no property group");
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
            /*
             * Ensure that there is a "config" property group:
             */
            let pg = if let Some(pg) = inst.get_pg("config")? {
                info!(log, "using existing config property group");
                pg
            } else {
                info!(log, "creating config property group");
                inst.add_pg("config", "application")?
            };

            info!(log, "reconfiguring {}", inst.fmri()?);

            let tx = pg.transaction()?;
            tx.start()?;

            /*
             * An expression of our values:
             */
            for property in &properties {
                info!(
                    log,
                    "ensure {} {:?} {}",
                    property.name,
                    property.typ,
                    property.val
                );

                tx.property_ensure(property.name, property.typ, &property.val)?;
            }

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

    for (_, region_snapshots) in running_snapshots.iter_mut() {
        for snapshot in region_snapshots.values_mut() {
            let name = format!(
                "{}-{}-{}",
                snapshot_prefix, snapshot.id.0, snapshot.name
            );

            let mut dir = datapath.clone();
            dir.push(&snapshot.id.0);
            dir.push(".zfs");
            dir.push("snapshot");
            dir.push(snapshot.name.clone());

            let properties = {
                let mut properties = snapshot.get_smf_properties(&dir);

                // Instruct downstairs process to listen on the same IP as the
                // agent, because there is currently only one address in the
                // crucible zone that both processes must share and that address
                // is what will be used by other zones. In the future this could
                // be a parameter that comes along with the region POST
                // parameters.
                properties.push(crate::model::SmfProperty {
                    name: "address",
                    typ: crucible_smf::scf_type_t::SCF_TYPE_ASTRING,
                    val: df.get_listen_addr().ip().to_string(),
                });

                properties
            };

            let inst = if let Some(inst) = svc.get_instance(&name)? {
                inst
            } else {
                info!(log, "creating missing snapshot instance {}", name);
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
                    let mut reconfig = false;

                    for property in &properties {
                        let existing_val = pg.get_property(property.name)?;
                        if let Some(existing_val) = existing_val {
                            if let Some(val) = existing_val.value()? {
                                if val.as_string()? != property.val {
                                    reconfig = true;
                                    info!(
                                        log,
                                        "existing {} value {} does not match {}",
                                        property.name,
                                        val.as_string()?,
                                        property.val,
                                    );
                                }
                            } else {
                                reconfig = true;
                                info!(
                                    log,
                                    "{} value call returned None",
                                    property.name,
                                );
                            }
                        } else {
                            reconfig = true;
                            info!(log, "{} value missing", property.name,);
                        }
                    }

                    // reconfig is required if propvals are missing, or wrong
                    if reconfig {
                        info!(log, "reconfig required");
                    }

                    reconfig
                } else {
                    info!(log, "reconfig required, no property group");
                    true
                }
            } else {
                /*
                 * No running snapshot means the service has never started.
                 * Prod the restarter by disabling it, then
                 * we'll create everything from scratch.
                 */
                inst.disable(false)?;
                true
            };

            if reconfig {
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

                /*
                 * An expression of our values:
                 */
                for property in &properties {
                    info!(
                        log,
                        "ensure {} {:?} {}",
                        property.name,
                        property.typ,
                        property.val
                    );

                    tx.property_ensure(
                        property.name,
                        property.typ,
                        &property.val,
                    )?;
                }

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
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::model::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_collect_behaviour() {
        let mut running_snapshots =
            BTreeMap::<RegionId, BTreeMap<String, RunningSnapshot>>::new();

        running_snapshots.insert(RegionId("r1".into()), BTreeMap::new());
        running_snapshots
            .get_mut(&RegionId("r1".into()))
            .unwrap()
            .insert(
                "first".into(),
                RunningSnapshot {
                    id: RegionId("r1".into()),
                    name: "first".into(),
                    port_number: 1,
                    state: State::Created,
                },
            );
        running_snapshots
            .get_mut(&RegionId("r1".into()))
            .unwrap()
            .insert(
                "second".into(),
                RunningSnapshot {
                    id: RegionId("r1".into()),
                    name: "second".into(),
                    port_number: 1,
                    state: State::Created,
                },
            );

        running_snapshots.insert(RegionId("r2".into()), BTreeMap::new());
        running_snapshots
            .get_mut(&RegionId("r2".into()))
            .unwrap()
            .insert(
                "third".into(),
                RunningSnapshot {
                    id: RegionId("r2".into()),
                    name: "third".into(),
                    port_number: 1,
                    state: State::Created,
                },
            );

        let snapshot_prefix = "snapshot";

        let expected_snapshot_instances: Vec<String> = running_snapshots
            .iter()
            .flat_map(|(_, n)| {
                n.iter()
                    .map(|(_, s)| {
                        format!("{}-{}-{}", snapshot_prefix, s.id.0, s.name)
                    })
                    .collect::<Vec<String>>()
            })
            .collect();

        assert_eq!(
            expected_snapshot_instances,
            vec![
                "snapshot-r1-first".to_string(),
                "snapshot-r1-second".to_string(),
                "snapshot-r2-third".to_string(),
            ],
        );
    }
}

/**
 * For region with state Tombstoned, destroy the region.
 *
 * For region with state Requested, create the region.
 */
fn worker(
    log: Logger,
    df: Arc<datafile::DataFile>,
    regions_dataset: ZFSDataset,
    downstairs_program: PathBuf,
    downstairs_prefix: String,
    snapshot_prefix: String,
) {
    // XXX unwraps here ok?
    loop {
        /*
         * This loop fires whenever there's work to do. This work may be:
         *
         * - create a region
         * - delete a region
         * - create a running snapshot
         * - delete a running snapshot
         *
         * We use first_in_states to both get the next available Region
         * and to wait on the condvar when there are no regions available.
         *
         * If the region is State::Requested, we create that region then run
         * the apply_smf().
         * If the region is State:Tombstoned, we apply_smf() first, then we
         * finish up destroying the region.
         */
        let r = df.first_in_states(&[State::Tombstoned, State::Requested]);

        match &r.state {
            State::Requested => {
                // if regions need to be created, do that before apply_smf.
                let region_dataset =
                    regions_dataset.ensure_child_dataset(&r.id.0).unwrap();

                let res = worker_region_create(
                    &log,
                    &downstairs_program,
                    &r,
                    &region_dataset.path().unwrap(),
                )
                .and_then(|_| df.created(&r.id));

                if let Err(e) = res {
                    error!(log, "region {:?} create failed: {:?}", r.id.0, e);
                    df.fail(&r.id);
                }

                info!(log, "applying SMF actions post create...");
                let result = apply_smf(
                    &log,
                    &df,
                    regions_dataset.path().unwrap(),
                    &downstairs_prefix,
                    &snapshot_prefix,
                );
                if let Err(e) = result {
                    error!(log, "SMF application failure: {:?}", e);
                } else {
                    info!(log, "SMF ok!");
                }
            }
            State::Tombstoned => {
                info!(log, "applying SMF actions before removal...");
                let result = apply_smf(
                    &log,
                    &df,
                    regions_dataset.path().unwrap(),
                    &downstairs_prefix,
                    &snapshot_prefix,
                );

                if let Err(e) = result {
                    error!(log, "SMF application failure: {:?}", e);
                } else {
                    info!(log, "SMF ok!");
                }

                // After SMF successfully shuts off downstairs, remove zfs
                // dataset.
                let region_dataset =
                    regions_dataset.from_child_dataset(&r.id.0).unwrap();

                let res = worker_region_destroy(&log, &r, region_dataset)
                    .and_then(|_| df.destroyed(&r.id));

                if let Err(e) = res {
                    error!(log, "region {:?} destroy failed: {:?}", r.id.0, e);
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
    prog: &Path,
    region: &model::Region,
    dir: &Path,
) -> Result<()> {
    let log = log.new(o!("region" => region.id.0.to_string()));

    /*
     * We may have crashed half way through a previous provision. To make
     * this idempotent, clean out the target data directory and try
     * again.
     */
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if entry.file_type()?.is_dir() {
            info!(log, "removing existing directory {:?}", path);
            std::fs::remove_dir_all(&path)?;
        } else {
            info!(log, "removing existing file {:?}", path);
            std::fs::remove_file(&path)?;
        }
    }

    /*
     * Run the downstairs program in the mode where it will create the data
     * files (note region starts blank).
     */
    info!(log, "creating region {:?} at {:?}", region, dir);
    let cmd = Command::new(prog)
        .env_clear()
        .arg("create")
        .arg("--uuid")
        .arg(region.id.0.clone())
        .arg("--data")
        .arg(dir)
        .arg("--block-size")
        .arg(region.block_size.to_string())
        .arg("--extent-size")
        .arg(region.extent_size.to_string())
        .arg("--extent-count")
        .arg(region.extent_count.to_string())
        .arg(if region.encrypted { "--encrypted" } else { "" })
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
     * If there are X509 files, write those out to the same directory.
     */
    if let Some(cert_pem) = &region.cert_pem {
        let mut path = dir.to_path_buf();
        path.push("cert.pem");
        std::fs::write(path, &cert_pem)?;
    }

    if let Some(key_pem) = &region.key_pem {
        let mut path = dir.to_path_buf();
        path.push("key.pem");
        std::fs::write(path, &key_pem)?;
    }

    if let Some(root_pem) = &region.root_pem {
        let mut path = dir.to_path_buf();
        path.push("root.pem");
        std::fs::write(path, &root_pem)?;
    }

    /*
     * `apply_smf` will then create the appropriate instance
     */

    Ok(())
}

fn worker_region_destroy(
    log: &Logger,
    region: &model::Region,
    region_dataset: ZFSDataset,
) -> Result<()> {
    let log = log.new(o!("region" => region.id.0.to_string()));

    let region_dataset_name = region_dataset.dataset();

    info!(log, "deleting zfs dataset {:?}", region_dataset_name);

    // Note: zfs destroy will fail if snapshots exist, but previous steps should
    // prevent that scenario.
    region_dataset.destroy(&log)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use openapiv3::OpenAPI;

    use crate::write_openapi;

    #[test]
    fn test_crucible_agent_openapi() {
        let mut raw = Vec::new();
        write_openapi(&mut raw).unwrap();
        let actual = String::from_utf8(raw).unwrap();

        // Make sure the result parses as a valid OpenAPI spec.
        let spec = serde_json::from_str::<OpenAPI>(&actual)
            .expect("output was not valid OpenAPI");

        // Check for lint errors.
        let errors = openapi_lint::validate(&spec);
        assert!(errors.is_empty(), "{}", errors.join("\n\n"));

        expectorate::assert_contents("../openapi/crucible-agent.json", &actual);
    }
}
