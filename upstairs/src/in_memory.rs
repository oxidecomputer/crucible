// Copyright 2021 Oxide Computer Company

use super::*;

struct Inner {
    bytes: Vec<u8>,
    owned: Vec<bool>,
}

/// Implement BlockIO for a block of memory
pub struct InMemoryBlockIO {
    uuid: Uuid,
    block_size: u64,
    inner: Mutex<Inner>,
}

impl InMemoryBlockIO {
    pub fn new(id: Uuid, block_size: u64, total_size: usize) -> Self {
        Self {
            uuid: id,
            block_size,
            inner: Mutex::new(Inner {
                bytes: vec![0; total_size],
                owned: vec![false; total_size],
            }),
        }
    }
}

#[async_trait]
impl BlockIO for InMemoryBlockIO {
    async fn activate(&self) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn deactivate(&self) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        Ok(true)
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        let inner = self.inner.lock().await;
        Ok(inner.bytes.len() as u64)
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.block_size)
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        Ok(self.uuid)
    }

    /// Read from `self` into `data`, setting ownership accordingly
    async fn read(
        &self,
        offset: Block,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let inner = self.inner.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        data.write_with_ownership(
            0,
            &inner.bytes[start..][..data.len()],
            &inner.owned[start..][..data.len()],
        );

        Ok(())
    }

    /// Write from `data` into `self`, setting all owned bits to `true`
    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let mut inner = self.inner.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data.len() {
            inner.bytes[start + i] = data[i];
            inner.owned[start + i] = true;
        }

        Ok(())
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let mut inner = self.inner.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data.len() {
            if !inner.owned[start + i] {
                inner.bytes[start + i] = data[i];
                inner.owned[start + i] = true;
            }
        }

        Ok(())
    }

    async fn flush(
        &self,
        _snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        Ok(WQCounts {
            up_count: 0,
            ds_count: 0,
            active_count: 0,
        })
    }
}
