// Copyright 2026 Oxide Computer Company

pub mod region {
    pub use crate::v1::region::CreateRegion;
    pub use crate::v1::region::Region;
    pub use crate::v1::region::RegionId;
    pub use crate::v1::region::RegionPath;
    pub use crate::v1::region::State;
}

pub mod snapshot {
    pub use crate::v1::snapshot::DeleteSnapshotPath;
    pub use crate::v1::snapshot::GetSnapshotPath;
    pub use crate::v1::snapshot::GetSnapshotResponse;
    pub use crate::v1::snapshot::RunSnapshotPath;
    pub use crate::v1::snapshot::RunningSnapshot;
    pub use crate::v1::snapshot::Snapshot;
}
