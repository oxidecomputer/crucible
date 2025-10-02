// Copyright 2021 Oxide Computer Company

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use crucible_agent_types::smf::SmfProperty;
use dropshot::{ConfigLogging, ConfigLoggingIfExists, ConfigLoggingLevel};
use slog::{debug, error, info, o, Logger};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const PROG: &str = "crucible-agent";
const SERVICE: &str = "oxide/crucible/downstairs";

/*
 * As a safety mechanism to prevent the agent from allocating more space to
 * regions than we have physical space on disk, we use a zfs reservation on
 * each dataset we create. While it does prevent us from allocating more space
 * to regions than the disk could accommodate, it is not a complete solution in
 * that we ideally would like Nexus to track allocations (for crucible and
 * other significant datasets), an adequate capacity margin to ensure that disk
 * performance doesn't become pathological, and monitors any over-use for
 * appropriate mitigation (migration, termination, etc).
 *
 * As for how much we reserve, it's the region size itself, plus the amount
 * we need for metadata (currently around 17%) then an additional 8% that, if
 * things are behaving, we will have for snapshots and to prevent us from
 * using all the data in the pool.
 *
 * In addition, we throw a quota of 3x the region size. The failure modes of
 * exhausting the quota are potentially severe and not (as yet) well-tested so
 * we want some buffer between the reservation and the quota. If the region has
 * grown that big however, something is potentially very wrong and we should
 * prevent it from growing any larger and impacting other regions on the disk.
 */
const RESERVATION_FACTOR: f64 = 1.25;
const QUOTA_FACTOR: u64 = 3;

mod datafile;
mod resource;
mod server;
mod smf_interface;
mod snapshot_interface;

use crucible_agent_types::region::{self, State};
use smf_interface::*;

use crate::resource::Resource;

#[derive(Debug, Parser)]
#[clap(name = PROG, about = "Crucible zone management agent")]
enum Args {
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

#[derive(Debug)]
pub struct ZFSDataset {
    dataset: String,
}

impl ZFSDataset {
    /// From either dataset name or path, create the ZFSDataset object.
    ///
    /// Fails if the dataset name does not exist, or the path does not exist (or
    /// belong to a dataset).
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
            let stderr =
                String::from_utf8_lossy(&cmd.stderr).trim_end().to_string();
            bail!("zfs list failed! {stderr}");
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
    pub fn ensure_child_dataset(
        &self,
        child: &str,
        reservation: Option<u64>,
        quota: Option<u64>,
        log: &Logger,
    ) -> Result<ZFSDataset> {
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
        let mut cmd = std::process::Command::new("zfs");
        cmd.arg("create");

        if let Some(reservation) = reservation {
            info!(log, "zfs set reservation of {reservation} for {dataset}");
            cmd.arg("-o").arg(format!("reservation={}", reservation));
        }

        if let Some(quota) = quota {
            info!(log, "zfs set quota of {quota} for {dataset}");
            cmd.arg("-o").arg(format!("quota={}", quota));
        }

        let res = cmd.arg(&dataset).output()?;

        if !res.status.success() {
            let out = String::from_utf8_lossy(&res.stdout);
            let err = String::from_utf8_lossy(&res.stderr);
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
                        "zfs destroy dataset failed! out:{} err:{}",
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
    let args = Args::try_parse()?;

    match args {
        Args::Run {
            dataset,
            listen,
            downstairs_program,
            lowport,
            downstairs_prefix,
            snapshot_prefix,
        } => {
            let log = ConfigLogging::File {
                level: ConfigLoggingLevel::Info,
                path: "/dev/stdout".into(),
                if_exists: ConfigLoggingIfExists::Append,
            }
            .to_logger(PROG)?;

            let info = crucible_common::BuildInfo::default();
            info!(log, "Crucible Version: {}", info);
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
                Arc::new(snapshot_interface::ZfsSnapshotInterface::new(
                    log.new(
                        o!("component" => String::from("ZfsSnapshotInterface")),
                    ),
                )),
            )?);

            let regions_dataset = dataset
                .ensure_child_dataset("regions", None, None, &log)
                .unwrap();

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

fn apply_smf(
    log: &Logger,
    df: &Arc<datafile::DataFile>,
    datapath: PathBuf,
    downstairs_prefix: &str,
    snapshot_prefix: &str,
) -> Result<()> {
    let scf = crucible_smf::Scf::new()?;
    let scope = scf.scope_local()?;
    let svc = scope
        .get_service(SERVICE)?
        .ok_or_else(|| anyhow!("service missing"))?;

    let smf_interface = RealSmf::new(&svc)?;

    apply_smf_actual(
        &smf_interface,
        log,
        df,
        datapath,
        downstairs_prefix,
        snapshot_prefix,
    )
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
fn apply_smf_actual<T>(
    smf_interface: &T,
    log: &Logger,
    df: &Arc<datafile::DataFile>,
    datapath: PathBuf,
    downstairs_prefix: &str,
    snapshot_prefix: &str,
) -> Result<()>
where
    T: SmfInterface,
{
    let regions = df.regions();
    let mut running_snapshots = df.running_snapshots();

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
                .filter(|(_, rs)| rs.state == State::Created)
                .map(|(_, rs)| {
                    format!("{}-{}-{}", snapshot_prefix, rs.id.0, rs.name)
                })
                .collect::<Vec<String>>()
        })
        .collect();

    for inst in smf_interface.instances()? {
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
                let states = inst.states()?;
                match states {
                    (Some(crucible_smf::State::Disabled), _) => {
                        // already disabled, no message required
                    }

                    _ => {
                        info!(
                            log,
                            "disabling downstairs instance: {} (instance states: {:?})",
                            n,
                            states,
                        );
                    }
                }

                /*
                 * XXX just disable for now.
                 */
                inst.disable(false)?;
                continue;
            }

            debug!(log, "found expected downstairs instance: {}", n);
        } else if n.starts_with(&format!("{}-", snapshot_prefix)) {
            if !expected_snapshot_instances.contains(&n) {
                let states = inst.states()?;
                match states {
                    (Some(crucible_smf::State::Disabled), _) => {
                        // already disabled, no message required
                    }

                    _ => {
                        info!(
                            log,
                            "disabling snapshot instance: {} (instance states: {:?})",
                            n,
                            states,
                        );
                    }
                }

                /*
                 * XXX just disable for now.
                 */
                inst.disable(false)?;
                continue;
            }

            debug!(log, "found expected snapshot instance: {}", n);
        } else {
            debug!(log, "ignoring instance: {:?}", n);
        }
    }

    /*
     * Second, create any downstairs and snapshot instances that are missing.
     */
    for r in regions.iter() {
        // If the region is in state Created, then the dataset exists, so start
        // a downstairs that points to it.
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
            properties.push(SmfProperty {
                name: "address",
                typ: crucible_smf::scf_type_t::SCF_TYPE_ASTRING,
                val: df.get_listen_addr().ip().to_string(),
            });

            // If the region has a source, then it was created as a clone and
            // must be started read only.
            if r.source.is_some() {
                properties.push(SmfProperty {
                    name: "mode",
                    typ: crucible_smf::scf_type_t::SCF_TYPE_ASTRING,
                    val: "ro".to_string(),
                });
                info!(log, "{:?} marking clone as read_only", name);
            }

            properties
        };

        let inst = if let Some(inst) = smf_interface.get_instance(&name)? {
            inst
        } else {
            info!(log, "creating missing downstairs instance {}", name);
            let inst = smf_interface.add_instance(&name)?;
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
            debug!(log, "do not need to reconfigure {}", inst.fmri()?);
        }

        /*
         * Finally, make sure the instance is enabled.
         */
        inst.enable(false)?;
    }

    for (_, region_snapshots) in running_snapshots.iter_mut() {
        for snapshot in region_snapshots.values_mut() {
            // The agent is not responsible for creating snapshots, only
            // starting a read-only downstairs that points to them. When the
            // original request to start a running snapshot is made, the object
            // begins in state Requested, and once apply_smf returns
            // successfully has its state changed to Created (this is used by
            // the caller to query if the service has been created yet).
            //
            // If the agent zone is bounced, we want it to read the datafile
            // and restore all the previously created SMF instances. This means
            // that we have to take action on both Requested and Created for
            // running snapshots. This is in contrast to Region, which has
            // different actions for Requested and Created.
            if !matches!(snapshot.state, State::Requested | State::Created) {
                continue;
            }

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
                properties.push(SmfProperty {
                    name: "address",
                    typ: crucible_smf::scf_type_t::SCF_TYPE_ASTRING,
                    val: df.get_listen_addr().ip().to_string(),
                });

                properties
            };

            let inst = if let Some(inst) = smf_interface.get_instance(&name)? {
                inst
            } else {
                info!(log, "creating missing snapshot instance {}", name);
                let inst = smf_interface.add_instance(&name)?;
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
                debug!(log, "do not need to reconfigure {}", inst.fmri()?);
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
    use super::*;

    use crate::snapshot_interface::SnapshotInterface;
    use crate::snapshot_interface::TestSnapshotInterface;

    use crucible_agent_types::{region::*, snapshot::*};
    use slog::{o, Drain, Logger};
    use std::collections::BTreeMap;
    use tempfile::*;
    use uuid::Uuid;

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

    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    /// Wrap a datafile, mock SMF interface, and mock snapshot interface, in order to test the
    /// agent's SMF related behaviour.
    pub struct TestSmfHarness {
        log: Logger,
        dir: TempDir,
        pub df: Arc<datafile::DataFile>,
        smf_interface: MockSmf,
        snapshot_interface: Arc<TestSnapshotInterface>,
    }

    impl TestSmfHarness {
        pub fn new() -> Result<TestSmfHarness> {
            let log = csl();
            let dir = tempdir()?;
            let snapshot_interface =
                Arc::new(TestSnapshotInterface::new(log.new(
                    o!("component" => String::from("test_snapshot_interface")),
                )));

            let df = Arc::new(datafile::DataFile::new(
                log.new(
                    o!("component" => String::from("test_harness_datafile")),
                ),
                dir.path(),
                "127.0.0.1:0".parse()?,
                1000,
                2000,
                snapshot_interface.clone(),
            )?);

            Ok(TestSmfHarness {
                log: log.clone(),
                dir,
                df,
                smf_interface: MockSmf::new(SERVICE.to_string()),
                snapshot_interface,
            })
        }

        pub fn path_buf(&self) -> PathBuf {
            self.dir.path().to_path_buf()
        }

        pub fn apply_smf(&self) -> Result<()> {
            let mut path_buf = self.dir.path().to_path_buf();
            path_buf.push("regions");

            apply_smf_actual(
                &self.smf_interface,
                &self.log,
                &self.df,
                path_buf,
                "downstairs",
                "snapshot",
            )
        }

        pub fn smf_is_empty(&self) -> bool {
            self.smf_interface.config_is_empty()
        }

        pub fn region_path(&self, region_id: &RegionId) -> PathBuf {
            let mut path_buf = self.path_buf();
            path_buf.push("regions");
            path_buf.push(&region_id.0);
            path_buf
        }

        pub fn region_path_string(&self, region_id: &RegionId) -> String {
            self.region_path(region_id)
                .into_os_string()
                .into_string()
                .unwrap()
        }

        pub fn create_snapshot(
            &self,
            region_id: String,
            snapshot_name: String,
        ) {
            self.snapshot_interface.create_snapshot(
                self.path_buf(),
                region_id,
                snapshot_name,
            );
        }

        pub fn snapshot_path(
            &self,
            region_id: &RegionId,
            snapshot_name: String,
        ) -> PathBuf {
            let mut path_buf = self.region_path(region_id);
            path_buf.push(".zfs");
            path_buf.push("snapshot");
            path_buf.push(snapshot_name);
            path_buf
        }

        pub fn snapshot_path_string(
            &self,
            region_id: &RegionId,
            snapshot_name: String,
        ) -> String {
            self.snapshot_path(region_id, snapshot_name)
                .into_os_string()
                .into_string()
                .unwrap()
        }
    }

    impl Drop for TestSmfHarness {
        fn drop(&mut self) {
            // If the agent zone bounces, it should read state from the datafile and recreate
            // everything. Compare here during the drop.
            let after_bounce_smf_interface = MockSmf::new(SERVICE.to_string());

            let mut path_buf = self.dir.path().to_path_buf();
            path_buf.push("regions");

            apply_smf_actual(
                &after_bounce_smf_interface,
                &self.log,
                &self.df,
                path_buf,
                "downstairs",
                "snapshot",
            )
            .unwrap();

            // Prune disabled services: a bounced agent zone will lose all these, and the agent
            // will not recreate them.
            self.smf_interface.prune();

            assert_eq!(self.smf_interface, after_bounce_smf_interface);
        }
    }

    #[test]
    fn test_smf_region_good() -> Result<()> {
        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Call apply_smf before `df.created`
        harness.apply_smf()?;

        // Nothing should have happened
        assert!(harness.smf_is_empty());

        // Pretend to actually create the region
        harness.df.created(&region_id)?;

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was created
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.region_path_string(&region_id)
            );
            assert!(pg.get_property("mode")?.is_none());
        }

        // Tombstone the region
        harness.df.destroy(&region_id)?;

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was disabled
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))?
                .unwrap();
            assert!(!instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.region_path_string(&region_id)
            );
        }

        Ok(())
    }

    #[test]
    fn test_smf_region_failed() -> Result<()> {
        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Pretend creating the region failed
        harness.df.fail(&region_id);

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was never created
        assert!(harness.smf_is_empty());

        Ok(())
    }

    #[test]
    fn test_smf_region_source_ro() {
        // Verify that a region created with a source endpoint will result
        // in an SMF service with the mode property set to 'ro'.
        let harness = TestSmfHarness::new().unwrap();

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request with a source
        harness
            .df
            .create_region_request(CreateRegion {
                id: region_id.clone(),

                block_size: 512,
                extent_size: 10,
                extent_count: 20,
                encrypted: true,

                cert_pem: None,
                key_pem: None,
                root_pem: None,
                source: Some("127.0.0.1:8899".parse().unwrap()),
            })
            .unwrap();

        // Pretend to actually create the region
        harness.df.created(&region_id).unwrap();

        // Now call apply_smf
        harness.apply_smf().unwrap();

        // Verify The downstairs SMF instance was created with the expected mode
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))
                .unwrap()
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config").unwrap().unwrap();

            let directory = pg.get_property("directory").unwrap().unwrap();
            assert_eq!(
                directory.value().unwrap().unwrap().as_string().unwrap(),
                harness.region_path_string(&region_id)
            );
            let mode = pg.get_property("mode").unwrap().unwrap();
            assert_eq!(
                mode.value().unwrap().unwrap().as_string().unwrap(),
                "ro".to_string()
            );
        }
    }

    #[test]
    fn test_smf_region_bounce_idempotent() -> Result<()> {
        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Pretend to actually create the region
        harness.df.created(&region_id)?;

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was created
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.region_path_string(&region_id)
            );
        }

        Ok(())
    }

    #[test]
    fn test_smf_region_create_then_destroy() -> Result<()> {
        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Call apply_smf before `df.created`
        harness.apply_smf()?;

        // Nothing should have happened
        assert!(harness.smf_is_empty());

        // Pretend Nexus requests to destroy the region in another thread
        harness.df.destroy(&region_id)?;

        // In this thread, still try to create the region
        harness.df.created(&region_id)?;

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was *not* created
        assert!(harness.smf_is_empty());

        Ok(())
    }

    #[test]
    fn test_smf_running_snapshot() -> Result<()> {
        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());
        let snapshot_name = Uuid::new_v4().to_string();

        // Submit a create region request
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Pretend to actually create the region
        harness.df.created(&region_id)?;

        // Now call apply_smf
        harness.apply_smf()?;

        // The downstairs SMF instance was created
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.region_path_string(&region_id)
            );
        }

        // Pretend to create a snapshot
        harness.create_snapshot(region_id.0.to_string(), snapshot_name.clone());

        // Submit a create running snapshot request
        harness.df.create_running_snapshot_request(
            CreateRunningSnapshotRequest {
                id: region_id.clone(),
                name: snapshot_name.clone(),

                cert_pem: None,
                key_pem: None,
                root_pem: None,
            },
        )?;

        // Call apply_smf
        harness.apply_smf()?;

        // The running snapshot should exist
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!(
                    "snapshot-{}-{}",
                    region_id.0, snapshot_name
                ))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.snapshot_path_string(&region_id, snapshot_name.clone())
            );
        }

        // Running snapshots transition from state Requested to state Created
        // when the smf apply is ok. Make sure (via the Drop comparison that
        // TestSmfHarness does) that when the agent zone bounces it reads
        // from the datafile, then creates running snapshots for objects in
        // state Created too.
        harness.df.created_rs(&region_id, &snapshot_name)?;

        Ok(())
    }

    #[test]
    fn test_smf_datafile_race_running_snapshots() -> Result<()> {
        // Test a race between the state changes that occur from what the user
        // is requesting, and the state changes that occur from worker.

        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());
        let snapshot_name = Uuid::new_v4().to_string();

        // Submit a create region request (http)
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // Pretend to actually create the region (worker)
        harness.df.created(&region_id)?;

        // Now call apply_smf (worker)
        harness.apply_smf()?;

        // The downstairs SMF instance was created
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!("downstairs-{}", region_id.0))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.region_path_string(&region_id)
            );
        }

        // Pretend to create a snapshot (pantry, or from propolis)
        harness.create_snapshot(region_id.0.to_string(), snapshot_name.clone());

        // Submit a create running snapshot request
        harness.df.create_running_snapshot_request(
            CreateRunningSnapshotRequest {
                id: region_id.clone(),
                name: snapshot_name.clone(),

                cert_pem: None,
                key_pem: None,
                root_pem: None,
            },
        )?;

        // From Nexus's perspective, the saga to create a snapshot might have
        // already completed before apply_smf runs.

        // Call apply_smf (worker)
        harness.apply_smf()?;

        // The running snapshot should exist
        {
            let instance = harness
                .smf_interface
                .get_instance(&format!(
                    "snapshot-{}-{}",
                    region_id.0, snapshot_name
                ))?
                .unwrap();
            assert!(instance.enabled());

            let pg = instance.get_pg("config")?.unwrap();

            let directory = pg.get_property("directory")?.unwrap();
            assert_eq!(
                directory.value()?.unwrap().as_string()?,
                harness.snapshot_path_string(&region_id, snapshot_name.clone())
            );
        }

        // After apply_smf, the running snapshot will be in state Requested
        {
            let running_snapshots = harness.df.running_snapshots();
            let region_running_snapshots =
                running_snapshots.get(&region_id).unwrap();
            let running_snapshot =
                region_running_snapshots.get(&snapshot_name).unwrap();
            assert_eq!(running_snapshot.state, State::Requested);
        }

        // Say a snapshot_delete saga runs now. Before the worker can call
        // `df.created_rs`, there's a destroy_rs call that comes in and sets it
        // to tombstoned.
        harness.df.delete_running_snapshot_request(
            DeleteRunningSnapshotRequest {
                id: region_id.clone(),
                name: snapshot_name.clone(),
            },
        )?;

        // Now it's in state Tombstoned
        {
            let running_snapshots = harness.df.running_snapshots();
            let region_running_snapshots =
                running_snapshots.get(&region_id).unwrap();
            let running_snapshot =
                region_running_snapshots.get(&snapshot_name).unwrap();
            assert_eq!(running_snapshot.state, State::Tombstoned);
        }

        // worker finishes up with calling `created_rs`
        harness.df.created_rs(&region_id, &snapshot_name)?;

        // There's a check in `created_rs` that sees if the RunningSnapshot is
        // in state Tombstoned, and bails out. The state should still be
        // Tombstoned.
        {
            let running_snapshots = harness.df.running_snapshots();
            let region_running_snapshots =
                running_snapshots.get(&region_id).unwrap();
            let running_snapshot =
                region_running_snapshots.get(&snapshot_name).unwrap();
            assert_eq!(running_snapshot.state, State::Tombstoned);
        }

        // `delete_running_snapshot_request` will notify the worker to run, so
        // run apply_smf.
        harness.apply_smf()?;

        Ok(())
    }

    #[test]
    fn test_smf_datafile_race_region() -> Result<()> {
        // Test a race between the state changes that occur from what the user
        // is requesting, and the state changes that occur from worker.

        let harness = TestSmfHarness::new()?;

        let region_id = RegionId(Uuid::new_v4().to_string());

        // Submit a create region request (http)
        harness.df.create_region_request(CreateRegion {
            id: region_id.clone(),

            block_size: 512,
            extent_size: 10,
            extent_count: 10,
            encrypted: true,

            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        })?;

        // The Region is Requested before `worker` ensures that the child
        // dataset exists.
        {
            let region = harness.df.get(&region_id).unwrap();
            assert_eq!(region.state, State::Requested);
        }

        // If Nexus then requests to destroy the region,
        harness.df.destroy(&region_id)?;

        // the State will be Tombstoned
        {
            let region = harness.df.get(&region_id).unwrap();
            assert_eq!(region.state, State::Tombstoned);
        }

        // Worker will create the child dataset, then call `df.created`:
        harness.df.created(&region_id)?;

        // It should still be Tombstoned
        {
            let region = harness.df.get(&region_id).unwrap();
            assert_eq!(region.state, State::Tombstoned);
        }

        // `apply_smf` will be called twice
        harness.apply_smf()?;
        harness.apply_smf()?;

        Ok(())
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
    let regions_dataset_path = match regions_dataset.path() {
        Ok(regions_dataset_path) => regions_dataset_path,
        Err(e) => {
            panic!(
                "Cannot get regions_dataset_path for {:?}: {}",
                regions_dataset, e,
            );
        }
    };

    loop {
        /*
         * This loop fires whenever there's work to do. This work may be:
         *
         * - create a region
         * - delete a region
         * - create a running snapshot
         * - delete a running snapshot
         *
         * We use first_in_states to both get the next available Resource enum,
         * which wraps either a Region or RegionSnapshot that has changed.
         * Otherwise, first_in_states will wait on the condvar.
         */
        let work = df.first_in_states(&[State::Tombstoned, State::Requested]);

        match work {
            Resource::Region(r) => {
                /*
                 * If the region is State::Requested, we create that region
                 * then run the apply_smf().
                 *
                 * If the region is State:Tombstoned, we apply_smf() first,
                 * then we finish up destroying the region.
                 */
                match &r.state {
                    State::Requested => 'requested: {
                        /*
                         * Compute the actual size required for a full region,
                         * then add our metadata overhead to that.
                         */
                        let region_size = r.block_size
                            * r.extent_size
                            * r.extent_count as u64;
                        let reservation =
                            (region_size as f64 * RESERVATION_FACTOR).round()
                                as u64;
                        let quota = region_size * QUOTA_FACTOR;

                        info!(
                            log,
                            "Region size:{} reservation:{} quota:{}",
                            region_size,
                            reservation,
                            quota,
                        );

                        // If regions need to be created, do that before
                        // apply_smf.
                        let region_dataset = match regions_dataset
                            .ensure_child_dataset(
                                &r.id.0,
                                Some(reservation),
                                Some(quota),
                                &log,
                            ) {
                            Ok(region_dataset) => region_dataset,
                            Err(e) => {
                                error!(
                                    log,
                                    "Dataset {} creation failed: {}",
                                    &r.id.0,
                                    e,
                                );
                                df.fail(&r.id);
                                break 'requested;
                            }
                        };

                        let dataset_path = match region_dataset.path() {
                            Ok(dataset_path) => dataset_path,
                            Err(e) => {
                                error!(
                                    log,
                                    "Failed to find path for dataset {}: {}",
                                    &r.id.0,
                                    e,
                                );
                                df.fail(&r.id);
                                break 'requested;
                            }
                        };

                        // It's important that a region transition to "Created"
                        // only after it has been created as a dataset:
                        // after the crucible agent restarts, `apply_smf` will
                        // only start downstairs services for those in
                        // "Created". If the `df.created` is moved to after this
                        // function's `apply_smf` call, and there is a crash
                        // before that moved `df.created` is set, then the agent
                        // will not start a downstairs service for this region
                        // when rebooted.
                        let res = worker_region_create(
                            &log,
                            &downstairs_program,
                            &r,
                            &dataset_path,
                        )
                        .and_then(|_| df.created(&r.id));

                        if let Err(e) = res {
                            error!(
                                log,
                                "region {:?} create failed: {:?}", r.id.0, e
                            );
                            df.fail(&r.id);
                            break 'requested;
                        }

                        info!(log, "applying SMF actions post create...");
                        let result = apply_smf(
                            &log,
                            &df,
                            regions_dataset_path.clone(),
                            &downstairs_prefix,
                            &snapshot_prefix,
                        );

                        if let Err(e) = result {
                            error!(log, "SMF application failure: {:?}", e);
                        } else {
                            info!(log, "SMF ok!");
                        }
                    }

                    State::Tombstoned => 'tombstoned: {
                        info!(log, "applying SMF actions before removal...");
                        let result = apply_smf(
                            &log,
                            &df,
                            regions_dataset_path.clone(),
                            &downstairs_prefix,
                            &snapshot_prefix,
                        );

                        if let Err(e) = result {
                            error!(log, "SMF application failure: {:?}", e);
                        } else {
                            info!(log, "SMF ok!");
                        }

                        // After SMF successfully shuts off downstairs, remove
                        // zfs dataset.
                        let region_dataset =
                            match regions_dataset.from_child_dataset(&r.id.0) {
                                Ok(region_dataset) => region_dataset,
                                Err(e) => {
                                    error!(
                                        log,
                                        "Cannot find region {:?} to remove: {}",
                                        r.id.0,
                                        e,
                                    );
                                    let _ = df.destroyed(&r.id);
                                    break 'tombstoned;
                                }
                            };
                        let res =
                            worker_region_destroy(&log, &r, region_dataset)
                                .and_then(|_| df.destroyed(&r.id));

                        if let Err(e) = res {
                            error!(
                                log,
                                "region {:?} destroy failed: {:?}", r.id.0, e
                            );
                            df.fail(&r.id);
                        }
                    }
                    _ => {
                        error!(
                            log,
                            "worker got unexpected region state: {:?}", r
                        );
                        std::process::exit(1);
                    }
                }
            }

            Resource::RunningSnapshot(region_id, snapshot_name, rs) => {
                /*
                 * No matter what the state is, we run apply_smf(). Creating and
                 * deleting running snapshots only requires us to create and
                 * delete services. The snapshots are not created by us.
                 *
                 * If the running snapshot is Requested, we apply_smf() first,
                 * then set the state to Created. This is a little different
                 * from how Regions are handled.
                 *
                 * If the running snapshot is Tombstoned, we apply_smf() first,
                 * then we set the state to Destroyed
                 */
                info!(
                    log,
                    "applying SMF actions for region {} running snapshot {} (state {:?})...",
                    rs.id.0,
                    rs.name,
                    rs.state,
                );

                let result = apply_smf(
                    &log,
                    &df,
                    regions_dataset_path.clone(),
                    &downstairs_prefix,
                    &snapshot_prefix,
                );

                if let Err(e) = result {
                    error!(log, "SMF application failure: {:?}", e);

                    // There's no fail_rs here: a future `apply_smf` should
                    // attempt to start the service again.
                } else {
                    info!(log, "SMF ok!");

                    // `apply_smf` returned Ok, so the desired state transition
                    // succeeded: update the datafile.
                    let res = match &rs.state {
                        State::Requested => {
                            df.created_rs(&region_id, &snapshot_name)
                        }

                        State::Tombstoned => {
                            df.destroyed_rs(&region_id, &snapshot_name)
                        }

                        _ => {
                            error!(
                                log,
                                "worker got unexpected running snapshot state: {:?}",
                                rs,
                            );
                            std::process::exit(1);
                        }
                    };

                    if let Err(e) = res {
                        error!(
                            log,
                            "running snapshot {} state change failed: {:?}",
                            rs.id.0,
                            e
                        );

                        df.fail_rs(&region_id, &snapshot_name);
                    }
                }
            }
        }
    }
}

fn worker_region_create(
    log: &Logger,
    prog: &Path,
    region: &region::Region,
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

    let mut binding = Command::new(prog);
    let mut cmd = binding
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
        .arg(region.extent_count.to_string());

    if region.encrypted {
        cmd = cmd.arg("--encrypted");
    }

    if let Some(source) = region.source {
        cmd = cmd.arg("--clone-source").arg(source.to_string());
    }

    info!(log, "downstairs create with: {:?}", cmd);
    let cmd = cmd.output()?;

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
        std::fs::write(path, cert_pem)?;
    }

    if let Some(key_pem) = &region.key_pem {
        let mut path = dir.to_path_buf();
        path.push("key.pem");
        std::fs::write(path, key_pem)?;
    }

    if let Some(root_pem) = &region.root_pem {
        let mut path = dir.to_path_buf();
        path.push("root.pem");
        std::fs::write(path, root_pem)?;
    }

    /*
     * `apply_smf` will then create the appropriate instance
     */

    Ok(())
}

fn worker_region_destroy(
    log: &Logger,
    region: &region::Region,
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
