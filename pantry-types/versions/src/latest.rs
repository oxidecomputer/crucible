// Copyright 2026 Oxide Computer Company

pub mod pantry {
    pub use crate::v1::pantry::AttachBackgroundRequest;
    pub use crate::v1::pantry::AttachRequest;
    pub use crate::v1::pantry::AttachResult;
    pub use crate::v1::pantry::BulkReadRequest;
    pub use crate::v1::pantry::BulkReadResponse;
    pub use crate::v1::pantry::BulkWriteRequest;
    pub use crate::v1::pantry::ExpectedDigest;
    pub use crate::v1::pantry::ImportFromUrlRequest;
    pub use crate::v1::pantry::ImportFromUrlResponse;
    pub use crate::v1::pantry::JobPath;
    pub use crate::v1::pantry::JobPollResponse;
    pub use crate::v1::pantry::JobResultOkResponse;
    pub use crate::v1::pantry::PantryStatus;
    pub use crate::v1::pantry::ReplaceRequest;
    pub use crate::v1::pantry::ScrubResponse;
    pub use crate::v1::pantry::SnapshotRequest;
    pub use crate::v1::pantry::ValidateRequest;
    pub use crate::v1::pantry::ValidateResponse;
    pub use crate::v1::pantry::VolumePath;
    pub use crate::v1::pantry::VolumeStatus;

    // Re-export ReplaceResult from crucible_client_types since the pantry API
    // uses it.
    pub use crucible_client_types::ReplaceResult;
}
