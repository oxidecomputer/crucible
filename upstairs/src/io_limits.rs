// Copyright 2024 Oxide Computer Company
use crate::{ClientData, ClientId, ClientMap};
use std::sync::Arc;
use tokio::sync::{
    AcquireError, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

// Internally, accounting uses MIN_BLOCK_SIZE as the fundamental unit
use crucible_common::MIN_BLOCK_SIZE;

/// Per-client IO limits
#[derive(Clone, Debug)]
pub struct ClientIOLimits {
    /// Semaphore to claim IO blocks on behalf of a job
    io_blocks: Arc<Semaphore>,

    /// Semaphore to claim individual IO jobs
    jobs: Arc<Semaphore>,
}

impl ClientIOLimits {
    /// Builds a new `ClientIOLimits` object with the given limits
    pub fn new(max_jobs: usize, max_io_bytes: usize) -> Self {
        let max_io_blocks = max_io_bytes / MIN_BLOCK_SIZE;
        ClientIOLimits {
            io_blocks: Semaphore::new(max_io_blocks).into(),
            jobs: Semaphore::new(max_jobs).into(),
        }
    }

    /// Claims a certain number of blocks (and one job)
    ///
    /// This function waits until the given resources are available.
    pub async fn claim(
        &self,
        bytes: u32,
    ) -> Result<ClientIOLimitGuard, AcquireError> {
        let io_blocks = self
            .io_blocks
            .clone()
            .acquire_many_owned(bytes / MIN_BLOCK_SIZE as u32)
            .await?;
        let jobs = self.jobs.clone().acquire_owned().await?;
        Ok(ClientIOLimitGuard { io_blocks, jobs })
    }

    /// Tries to claim a certain number of blocks (and one job)
    pub fn try_claim(
        &self,
        bytes: u32,
    ) -> Result<ClientIOLimitGuard, TryAcquireError> {
        let io_blocks = self
            .io_blocks
            .clone()
            .try_acquire_many_owned(bytes / MIN_BLOCK_SIZE as u32)?;
        let jobs = self.jobs.clone().try_acquire_owned()?;
        Ok(ClientIOLimitGuard { io_blocks, jobs })
    }
}

/// Read-write handle for IO limits
#[derive(Clone, Debug)]
pub struct IOLimits(ClientData<ClientIOLimits>);

impl IOLimits {
    /// Builds a new set of IO limits
    pub fn new(max_jobs: usize, max_io_bytes: usize) -> Self {
        Self(ClientData::from_fn(|_i| {
            ClientIOLimits::new(max_jobs, max_io_bytes)
        }))
    }

    /// Returns a per-client IO limit handle
    ///
    /// This handle shares permits with the parent
    pub fn client_limits(&self, i: ClientId) -> ClientIOLimits {
        self.0[i].clone()
    }

    /// Returns a view handle for the IO limits
    pub fn view(&self) -> IOLimitView {
        IOLimitView(self.clone())
    }

    pub async fn claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, AcquireError> {
        let mut out = ClientData::from_fn(|_| None);
        for i in ClientId::iter() {
            out[i] = Some(self.0[i].claim(bytes).await?);
        }
        Ok(IOLimitGuard(out.map(Option::unwrap)))
    }

    pub fn try_claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, TryAcquireError> {
        let mut out = ClientData::from_fn(|_| None);
        for i in ClientId::iter() {
            out[i] = Some(self.0[i].try_claim(bytes)?);
        }
        Ok(IOLimitGuard(out.map(Option::unwrap)))
    }
}

/// View into global IO limits
///
/// This is equivalent to an [`IOLimits`], but exposes a limited API
#[derive(Clone, Debug)]
pub struct IOLimitView(IOLimits);

impl IOLimitView {
    pub async fn claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, AcquireError> {
        self.0.claim(bytes).await
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Handle owning some amount of per-client IO
#[derive(Debug)]
pub struct ClientIOLimitGuard {
    #[allow(unused)] // XXX switch to expect(unused) in rustc 1.81.0
    io_blocks: OwnedSemaphorePermit,
    #[allow(unused)]
    jobs: OwnedSemaphorePermit,
}

impl ClientIOLimitGuard {
    #[cfg(test)]
    pub fn dummy() -> Self {
        let a = Arc::new(Semaphore::new(1));
        let b = Arc::new(Semaphore::new(1));
        let io_blocks = a.try_acquire_owned().unwrap();
        let jobs = b.try_acquire_owned().unwrap();
        ClientIOLimitGuard { io_blocks, jobs }
    }
}

#[derive(Debug)]
pub struct IOLimitGuard(ClientData<ClientIOLimitGuard>);

impl From<IOLimitGuard> for ClientMap<ClientIOLimitGuard> {
    fn from(i: IOLimitGuard) -> Self {
        i.0.into()
    }
}

impl IOLimitGuard {
    #[cfg(test)]
    pub fn dummy() -> Self {
        Self(ClientData::from_fn(|_i| ClientIOLimitGuard::dummy()))
    }
}
