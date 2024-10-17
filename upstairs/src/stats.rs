// Copyright 2022 Oxide Computer Company
use super::*;

use oximeter::{
    types::{Cumulative, Sample},
    Metric, MetricsError, Producer, Target,
};

// These structs are used to construct the desired stats for Oximeter.
#[derive(Debug, Target)]
pub struct CrucibleUpstairs {
    /// The UUID of the region
    pub upstairs_uuid: Uuid,
}
#[derive(Debug, Default, Metric)]
pub struct Activated {
    /// Count of times this upstairs has activated.
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct Write {
    /// Count of region writes this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct WriteBytes {
    /// Count of bytes written
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct Read {
    /// Count of region reads this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct ReadBytes {
    /// Count of bytes read
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct Flush {
    /// Count of region flushes this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct Barrier {
    /// Count of region barriers this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct FlushClose {
    /// Count of extent flush close operations this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct ExtentRepair {
    /// Count of extent repair operations this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct ExtentNoOp {
    /// Count of extent NoOp operations this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}
#[derive(Debug, Default, Metric)]
pub struct ExtentReopen {
    /// Count of extent reopen operations this upstairs has completed
    #[datum]
    pub count: Cumulative<i64>,
}

// All the counter stats in one struct.
#[derive(Debug)]
pub struct UpCountStat {
    stat_name: CrucibleUpstairs,
    activated_count: Activated,
}

impl UpCountStat {
    pub fn new(upstairs_uuid: Uuid) -> Self {
        UpCountStat {
            stat_name: CrucibleUpstairs { upstairs_uuid },
            activated_count: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DownstairsCountStat {
    write_count: Write,
    write_bytes: WriteBytes,
    read_count: Read,
    read_bytes: ReadBytes,
    flush_count: Flush,
    barrier_count: Barrier,
    flush_close_count: FlushClose,
    extent_repair_count: ExtentRepair,
    extent_noop_count: ExtentNoOp,
    extent_reopen_count: ExtentReopen,
}

// This struct wraps the stat struct in an Arc/Mutex so the worker tasks can
// share it with the producer trait.
#[derive(Clone, Debug)]
pub struct UpStatOuter {
    pub up_stat_wrap: Arc<std::sync::Mutex<UpCountStat>>,
    ds_stat_wrap: DownstairsStatOuter,
}

#[derive(Clone, Debug, Default)]
pub struct DownstairsStatOuter(Arc<std::sync::Mutex<DownstairsCountStat>>);

impl UpStatOuter {
    pub fn new(uuid: Uuid) -> Self {
        Self {
            up_stat_wrap: Arc::new(std::sync::Mutex::new(UpCountStat::new(
                uuid,
            ))),
            ds_stat_wrap: DownstairsStatOuter::default(),
        }
    }

    pub fn add_activation(&self) {
        let mut ups = self.up_stat_wrap.lock().unwrap();
        let datum = ups.activated_count.datum_mut();
        *datum += 1;
    }

    /// Returns a handle to the nested `DownstairsStatOuter`
    ///
    /// The handle shares the same datums with the `UpStatOuter`
    pub fn ds_stats(&self) -> DownstairsStatOuter {
        self.ds_stat_wrap.clone()
    }
}

impl DownstairsStatOuter {
    pub fn add_write(&self, bytes: i64) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.write_bytes.datum_mut();
        *datum += bytes;
        let datum = ups.write_count.datum_mut();
        *datum += 1;
    }
    pub fn add_read(&self, bytes: i64) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.read_bytes.datum_mut();
        *datum += bytes;
        let datum = ups.read_count.datum_mut();
        *datum += 1;
    }
    pub fn add_flush(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.flush_count.datum_mut();
        *datum += 1;
    }
    pub fn add_barrier(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.barrier_count.datum_mut();
        *datum += 1;
    }
    pub fn add_flush_close(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.flush_close_count.datum_mut();
        *datum += 1;
    }
    pub fn add_extent_repair(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.extent_repair_count.datum_mut();
        *datum += 1;
    }
    pub fn add_extent_noop(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.extent_noop_count.datum_mut();
        *datum += 1;
    }
    pub fn add_extent_reopen(&self) {
        let mut ups = self.0.lock().unwrap();
        let datum = ups.extent_reopen_count.datum_mut();
        *datum += 1;
    }
}

// This trait is what is called to update the data to send to Oximeter.
// It is called on whatever interval was specified when setting up the
// connection to Oximeter.  Since we get a lock in here (and on every
// IO, don't call this too frequently, for some value of frequently that
// I'm not sure of.
impl Producer for UpStatOuter {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let ups = self.up_stat_wrap.lock().unwrap();
        let dss = self.ds_stat_wrap.0.lock().unwrap();

        let name = &ups.stat_name;
        let data = vec![
            Sample::new(name, &ups.activated_count)?,
            Sample::new(name, &dss.flush_count)?,
            Sample::new(name, &dss.write_count)?,
            Sample::new(name, &dss.write_bytes)?,
            Sample::new(name, &dss.read_count)?,
            Sample::new(name, &dss.read_bytes)?,
            Sample::new(name, &dss.flush_close_count)?,
            Sample::new(name, &dss.extent_repair_count)?,
            Sample::new(name, &dss.extent_noop_count)?,
            Sample::new(name, &dss.extent_reopen_count)?,
        ];

        // Yield the available samples.
        Ok(Box::new(data.into_iter()))
    }
}
