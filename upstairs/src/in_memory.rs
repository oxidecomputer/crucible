// Copyright 2021 Oxide Computer Company

use super::*;

pub struct InMemoryBlockIO {
    uuid: Uuid,
    block_size: u64,
    bytes: Mutex<Vec<u8>>,
    owned: Mutex<Vec<bool>>,
}

impl InMemoryBlockIO {
    pub fn new(id: Uuid, block_size: u64, total_size: usize) -> Self {
        Self {
            uuid: id,
            block_size,
            bytes: Mutex::new(vec![0; total_size]),
            owned: Mutex::new(vec![false; total_size]),
        }
    }
}

#[async_trait]
impl BlockIO for InMemoryBlockIO {
    async fn activate(&self, _gen: u64) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn deactivate(&self) -> Result<BlockReqWaiter, CrucibleError> {
        BlockReqWaiter::immediate().await
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        Ok(true)
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.bytes.lock().await.len() as u64)
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.block_size)
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        Ok(self.uuid)
    }

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let mut data_vec = data.as_vec().await;
        let mut owned_vec = data.owned_vec().await;

        let bytes = self.bytes.lock().await;
        let owned = self.owned.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data_vec.len() {
            data_vec[i] = bytes[start + i];
            owned_vec[i] = owned[start + i];
        }

        BlockReqWaiter::immediate().await
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let mut bytes = self.bytes.lock().await;
        let mut owned = self.owned.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data.len() {
            bytes[start + i] = data[i];
            owned[start + i] = true;
        }

        BlockReqWaiter::immediate().await
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let mut bytes = self.bytes.lock().await;
        let mut owned = self.owned.lock().await;

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data.len() {
            if !owned[start + i] {
                bytes[start + i] = data[i];
                owned[start + i] = true;
            }
        }

        BlockReqWaiter::immediate().await
    }

    async fn flush(
        &self,
        _snapshot_details: Option<SnapshotDetails>,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        BlockReqWaiter::immediate().await
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        Ok(WQCounts {
            up_count: 0,
            ds_count: 0,
        })
    }
}
