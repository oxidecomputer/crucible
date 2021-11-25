// Copyright 2021 Oxide Computer Company
use super::*;

use dropshot::{ConfigDropshot, ConfigLogging, ConfigLoggingLevel};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use oximeter::{
    types::{Cumulative, Sample},
    Error, Metric, Producer, Target,
};
use oximeter_producer::{Config, Server};


// These structs are used to construct the required stats for Oximeter.
#[derive(Debug, Copy, Clone, Target)]
pub struct DsStat {
        pub ds_uuid: Uuid,
}
#[derive(Debug, Copy, Clone, Metric)]
pub struct DsConnect {
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Copy, Clone, Metric)]
pub struct DsWrite {
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Copy, Clone, Metric)]
pub struct DsRead {
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Copy, Clone, Metric)]
pub struct DsFlush {
    #[datum]
    pub count: Cumulative<i64>,
}

// All the counter stats in one struct.
#[derive(Clone, Debug)]
pub struct DsCountStat {
    stat_name: DsStat,
    up_connect_count: Vec<DsConnect>,
    write_count: Vec<DsWrite>,
    read_count: Vec<DsRead>,
    flush_count: Vec<DsFlush>,
}

impl DsCountStat {
	pub fn new(ds_uuid: Uuid) -> Self {
		DsCountStat {
			stat_name: DsStat { ds_uuid, },
			up_connect_count: vec![DsConnect { count: Cumulative::default() }],
			write_count: vec![DsWrite { count: Cumulative::default() }],
			read_count: vec![DsRead { count: Cumulative::default() }],
			flush_count: vec![DsFlush { count: Cumulative::default() }],
		}
    }
}

// This struct wraps the stat struct in an Arc/Mutex so the worker tasks can
// share it with the producer trait.
#[derive(Clone, Debug)]
pub struct DsStatOuter {
    pub ds_stat_wrap: Arc<Mutex<DsCountStat>>,
}

impl DsStatOuter {
    /*
     * When an operation happens that we wish to record in Oximeter,
     * one of these methods will be called.  Each method will get the
     * correct field of DsCountStat to record the update.
     */
    pub async fn add_connection(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().await;
        let dsc = dss.up_connect_count.get_mut(0).unwrap();
        let datum = dsc.datum_mut();
        *datum += 1;
    }
    pub async fn add_write(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().await;
        let dsc = dss.write_count.get_mut(0).unwrap();
        let datum = dsc.datum_mut();
        *datum += 1;
    }
    pub async fn add_read(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().await;
        let dsc = dss.read_count.get_mut(0).unwrap();
        let datum = dsc.datum_mut();
        *datum += 1;
    }
    pub async fn add_flush(&mut self) {
        let mut dss = self.ds_stat_wrap.lock().await;
        let dsc = dss.flush_count.get_mut(0).unwrap();
        let datum = dsc.datum_mut();
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
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, Error> {
        let mut dss = executor::block_on(self.ds_stat_wrap.lock());
        let len = dss.up_connect_count.len()
            + dss.write_count.len()
            + dss.read_count.len()
            + dss.flush_count.len();

        let mut data = Vec::with_capacity(len);
        let name = dss.stat_name;

        for counter in dss.up_connect_count.iter_mut() {
            data.push(Sample::new(&name, counter));
        }
        for counter in dss.flush_count.iter_mut() {
            data.push(Sample::new(&name, counter));
        }
        for counter in dss.write_count.iter_mut() {
            data.push(Sample::new(&name, counter));
        }
        for counter in dss.read_count.iter_mut() {
            data.push(Sample::new(&name, counter));
        }
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
pub async fn ox_stats(dss: DsStatOuter) -> Result<()> {

    let address = "[::1]:0".parse().unwrap();
    let dropshot_config =
        ConfigDropshot { bind_address: address, request_body_max_bytes: 2048 };
    let logging_config =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Error };

    let server_info = ProducerEndpoint {
        id: Uuid::new_v4().into(),
        address,
        base_route: "/collect".to_string(),
        interval: Duration::from_secs(10),
    };

    let config = Config {
        server_info,
        registration_address: "127.0.0.1:12221".parse().unwrap(),
        dropshot_config,
        logging_config,
    };

    // If the server is not responding when the downstairs starts, keep
    // trying.
    loop {
        let server = Server::start(&config).await;
        match server {
            Ok(server) => {
                server.registry().register_producer(dss.clone()).unwrap();
                println!("Oximeter producer registered, now serve_forever");
                server.serve_forever().await.unwrap();
            },
            Err(e) => {
                println!("Can't connect to oximeter server:\n{}", e);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
