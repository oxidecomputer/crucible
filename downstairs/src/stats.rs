// Copyright 2023 Oxide Computer Company
use super::*;

use dropshot::{ConfigLogging, ConfigLoggingIfExists, ConfigLoggingLevel};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::{
    types::{Cumulative, Sample},
    Metric, MetricsError, Producer, Target,
};
use oximeter_producer::{Config, LogConfig, Server};

// These structs are used to construct the required stats for Oximeter.
#[derive(Debug, Copy, Clone, Target)]
pub struct CrucibleDownstairs {
    // The UUID of the downstairs
    pub downstairs_uuid: Uuid,
}
#[derive(Debug, Default, Copy, Clone, Metric)]
pub struct Connect {
    // Count of times this downstairs has started a connection to an upstairs
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Copy, Clone, Metric)]
pub struct Write {
    // Count of region writes this downstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Copy, Clone, Metric)]
pub struct Read {
    // Count of region reads this downstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Copy, Clone, Metric)]
pub struct Flush {
    // Count of region flushes this downstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}

// All the counter stats in one struct.
#[derive(Clone, Debug)]
pub struct DsCountStat {
    stat_name: CrucibleDownstairs,
    up_connect_count: Connect,
    write_count: Write,
    read_count: Read,
    flush_count: Flush,
}

impl DsCountStat {
    pub fn new(downstairs_uuid: Uuid) -> Self {
        DsCountStat {
            stat_name: CrucibleDownstairs { downstairs_uuid },
            up_connect_count: Default::default(),
            write_count: Default::default(),
            read_count: Default::default(),
            flush_count: Default::default(),
        }
    }
}

// This struct wraps the stat struct in an Arc/Mutex so the worker tasks can
// share it with the producer trait.
#[derive(Clone, Debug)]
pub struct DsStatOuter {
    pub ds_stat_wrap: Arc<std::sync::Mutex<DsCountStat>>,
}

impl DsStatOuter {
    /*
     * When an operation happens that we wish to record in Oximeter,
     * one of these methods will be called.  Each method will get the
     * correct field of DsCountStat to record the update.
     */
    pub fn add_connection(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().unwrap();
        let datum = dss.up_connect_count.datum_mut();
        *datum += 1;
    }
    pub fn add_write(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().unwrap();
        let datum = dss.write_count.datum_mut();
        *datum += 1;
    }
    pub fn add_read(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().unwrap();
        let datum = dss.read_count.datum_mut();
        *datum += 1;
    }
    pub fn add_flush(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().unwrap();
        let datum = dss.flush_count.datum_mut();
        *datum += 1;
    }
}

// This trait is what is called to update the data to send to Oximeter.
// It is called on whatever interval was specified when setting up the
// connection to Oximeter.  Since we get a lock in here (and on every
// IO, don't call this too frequently, for some value of frequently that
// I'm not sure of.
impl Producer for DsStatOuter {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let dss = self.ds_stat_wrap.lock().unwrap();

        let name = &dss.stat_name;
        let data = vec![
            Sample::new(name, &dss.up_connect_count)?,
            Sample::new(name, &dss.flush_count)?,
            Sample::new(name, &dss.write_count)?,
            Sample::new(name, &dss.read_count)?,
        ];

        // Yield the available samples.
        Ok(Box::new(data.into_iter()))
    }
}

/*
 * Setup Oximeter
 * This starts a dropshot server, and then registers the DsStatOuter
 * producer with Oximeter.
 *
 * TODO: Make this take options other than the default for where to
 * connect to.
 *
 */
pub async fn ox_stats(
    dss: DsStatOuter,
    registration_address: SocketAddr,
    my_address: SocketAddr,
    log: &Logger,
) -> Result<()> {
    let logging_config = ConfigLogging::File {
        level: ConfigLoggingLevel::Error,
        path: "/dev/stdout".into(),
        if_exists: ConfigLoggingIfExists::Append,
    };

    // Use the downstairs's UUID itself in the producer registration, to keep
    // the same identity across restarts (if any).
    let server_info = ProducerEndpoint {
        id: dss.ds_stat_wrap.lock().unwrap().stat_name.downstairs_uuid,
        kind: ProducerKind::Service,
        address: my_address,
        interval: Duration::from_secs(10),
    };

    let config = Config {
        server_info,
        registration_address: Some(registration_address),
        request_body_max_bytes: 2048,
        log: LogConfig::Config(logging_config),
    };

    // If the server is not responding when the downstairs starts, keep
    // trying.
    loop {
        match Server::start(&config) {
            Ok(server) => {
                server.registry().register_producer(dss.clone()).unwrap();
                info!(log, "Oximeter producer registered, now serve_forever");

                server.serve_forever().await.unwrap();
            }
            Err(e) => {
                warn!(log, "Can't connect to oximeter server:\n{}", e);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
