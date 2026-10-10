// Copyright 2026 Oxide Computer Company

use crate::latest;
use crate::v1;

impl From<latest::pantry::VolumeStatus> for v1::pantry::VolumeStatus {
    fn from(latest: latest::pantry::VolumeStatus) -> v1::pantry::VolumeStatus {
        v1::pantry::VolumeStatus {
            active: latest.active,
            seen_active: latest.seen_active,
            num_job_handles: latest.num_job_handles,
        }
    }
}
