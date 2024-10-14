// Copyright 2024 Oxide Computer Company
use crate::{ClientData, ClientId, ClientMap};
use std::sync::Arc;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

// Internally, accounting uses MIN_BLOCK_SIZE as the fundamental unit
use crucible_common::MIN_BLOCK_SIZE;

/// System-wide IO limits
#[derive(Clone, Debug)]
pub struct IOLimits {
    clients: ClientData<ClientIOLimits>,
}

impl IOLimits {
    /// Builds a new `IOLimits` object with the given limits
    pub fn new(max_jobs: usize, max_io_bytes: usize) -> Self {
        let max_io_blocks = max_io_bytes / MIN_BLOCK_SIZE;
        Self {
            clients: ClientData::from_fn(|_i| ClientIOLimits {
                io_blocks: Semaphore::new(max_io_blocks).into(),
                jobs: Semaphore::new(max_jobs).into(),
            }),
        }
    }

    /// Claims some number of blocks (and one job)
    ///
    /// This function yields until permits are available
    pub async fn claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, AcquireError> {
        let blocks = bytes / MIN_BLOCK_SIZE as u32;
        let mut guards = ClientMap::new();
        for cid in ClientId::iter() {
            guards.insert(cid, self.clients[cid].claim(blocks).await?);
        }
        Ok(IOLimitGuard { guards })
    }
}

/// Per-client IO limits
#[derive(Clone, Debug)]
struct ClientIOLimits {
    /// Semaphore to claim IO blocks on behalf of a job
    io_blocks: Arc<Semaphore>,

    /// Semaphore to claim individual IO jobs
    jobs: Arc<Semaphore>,
}

impl ClientIOLimits {
    /// Claims a certain number of blocks (and one job)
    ///
    /// This function waits until the given resources are available.
    async fn claim(
        &self,
        blocks: u32,
    ) -> Result<ClientIOLimitGuard, AcquireError> {
        let io_blocks =
            self.io_blocks.clone().acquire_many_owned(blocks).await?;
        let jobs = self.jobs.clone().acquire_owned().await?;
        Ok(ClientIOLimitGuard { io_blocks, jobs })
    }
}

/// Handle owning some amount of system-wide IO
#[derive(Debug)]
pub struct IOLimitGuard {
    guards: ClientMap<ClientIOLimitGuard>,
}

impl IOLimitGuard {
    /// Drops the IO limit guard for the given client
    ///
    /// If the client has already been marked as done, this is a no-op
    pub fn done(&mut self, cid: ClientId) {
        let _ = self.guards.take(&cid);
    }

    /// Checks whether the given client has been marked as done
    pub fn is_done(&self, cid: ClientId) -> bool {
        !self.guards.contains(&cid)
    }

    /// Returns an empty `IOLimitGuard`
    ///
    /// This can be used for unit testing, or for IOs which should not count
    /// towards our IO limits.
    pub fn empty() -> Self {
        Self {
            guards: ClientMap::new(),
        }
    }
}

/// Handle owning some amount of per-client IO
#[derive(Debug)]
struct ClientIOLimitGuard {
    io_blocks: OwnedSemaphorePermit,
    jobs: OwnedSemaphorePermit,
}
