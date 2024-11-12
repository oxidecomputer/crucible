// Copyright 2024 Oxide Computer Company
use crate::{ClientData, ClientId, ClientMap};
use std::sync::Arc;
use tokio::sync::{
    AcquireError, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

/// Per-client IO limits
#[derive(Clone, Debug)]
pub struct ClientIOLimits {
    /// Semaphore to claim IO bytes on behalf of a job
    io_bytes: Arc<Semaphore>,

    /// Semaphore to claim individual IO jobs
    jobs: Arc<Semaphore>,
}

impl ClientIOLimits {
    /// Builds a new `ClientIOLimits` object with the given limits
    pub fn new(max_jobs: usize, max_io_bytes: usize) -> Self {
        ClientIOLimits {
            io_bytes: Semaphore::new(max_io_bytes).into(),
            jobs: Semaphore::new(max_jobs).into(),
        }
    }

    /// Claims a certain number of bytes (and one job)
    ///
    /// This function waits until the given resources are available; as such, it
    /// should not be called from the same task which is processing jobs (since
    /// that could create a deadlock).
    async fn claim(
        &self,
        bytes: u32,
    ) -> Result<ClientIOLimitGuard, AcquireError> {
        let io_bytes = self.io_bytes.clone().acquire_many_owned(bytes).await?;
        let jobs = self.jobs.clone().acquire_owned().await?;
        Ok(ClientIOLimitGuard { io_bytes, jobs })
    }

    /// Tries to claim a certain number of bytes (and one job)
    pub fn try_claim(
        &self,
        bytes: u32,
    ) -> Result<ClientIOLimitGuard, TryAcquireError> {
        let io_bytes = self.io_bytes.clone().try_acquire_many_owned(bytes)?;
        let jobs = self.jobs.clone().try_acquire_owned()?;
        Ok(ClientIOLimitGuard { io_bytes, jobs })
    }
}

/// Read-write handle for IO limits across all 3x clients
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

    /// Try to claim some number of bytes (and one job)
    ///
    /// Returns `Err((ClientId, e))` if any of the claims fail
    pub fn try_claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, (ClientId, TryAcquireError)> {
        let mut out = ClientData::from_fn(|_| None);
        for i in ClientId::iter() {
            out[i] = Some(self.0[i].try_claim(bytes).map_err(|e| (i, e))?);
        }

        Ok(IOLimitGuard(out.map(Option::unwrap)))
    }
}

/// View into global IO limits
///
/// This is equivalent to an [`IOLimits`], but exposes a different API.  It
/// should be owned by a separate task, to avoid deadlocks when trying to claim
/// resources.
#[derive(Clone, Debug)]
pub struct IOLimitView(IOLimits);

impl IOLimitView {
    /// Claim some number of bytes (and one job)
    ///
    /// This function waits until the given resources are available; as such, it
    /// should not be called from the same task which is processing jobs (since
    /// that could create a deadlock).
    pub async fn claim(
        &self,
        bytes: u32,
    ) -> Result<IOLimitGuard, AcquireError> {
        let mut out = ClientData::from_fn(|_| None);
        let lim = &self.0;
        for i in ClientId::iter() {
            out[i] = Some(lim.0[i].claim(bytes).await?);
        }
        Ok(IOLimitGuard(out.map(Option::unwrap)))
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Handle owning some amount of per-client IO
///
/// The IO permits are released when this handle is dropped
#[derive(Debug)]
pub struct ClientIOLimitGuard {
    #[expect(unused)]
    io_bytes: OwnedSemaphorePermit,
    #[expect(unused)]
    jobs: OwnedSemaphorePermit,
}

impl ClientIOLimitGuard {
    #[cfg(test)]
    pub fn dummy() -> Self {
        let a = Arc::new(Semaphore::new(1));
        let b = Arc::new(Semaphore::new(1));
        let io_bytes = a.try_acquire_owned().unwrap();
        let jobs = b.try_acquire_owned().unwrap();
        ClientIOLimitGuard { io_bytes, jobs }
    }
}

/// Handle which stores IO limit guards for all 3x clients
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
