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

    fn check_size(&self, size: usize) -> Result<(), CrucibleError> {
        if size as u64 % self.block_size == 0 {
            Ok(())
        } else {
            Err(CrucibleError::InvalidNumberOfBlocks(format!(
                "data length {} is not divisible by block size {}",
                size, self.block_size
            )))
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

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<(), CrucibleError> {
        self.check_size(data.len())?;
        let inner = self.inner.lock().await;

        let mut data_vec = data.as_vec().await;
        let mut owned_vec = data.owned_vec().await;

        let data_start = offset.value as usize * self.block_size as usize;
        let data_len = data_vec.len();
        data_vec
            .copy_from_slice(&inner.bytes[data_start..data_start + data_len]);

        let owned_start = offset.value as usize;
        let owned_len = owned_vec.len();
        owned_vec.copy_from_slice(
            &inner.owned[owned_start..owned_start + owned_len],
        );

        Ok(())
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.check_size(data.len())?;

        let mut inner = self.inner.lock().await;

        let data_start = offset.value as usize * self.block_size as usize;
        let data_len = data.len();
        inner.bytes[data_start..data_start + data_len].copy_from_slice(&data);

        let owned_start = offset.value as usize;
        let owned_len = data_len / self.block_size as usize;
        inner.owned[owned_start..owned_start + owned_len].fill(true);

        Ok(())
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.check_size(data.len())?;

        let mut inner = self.inner.lock().await;

        let start = offset.value as usize;

        let bs = self.block_size as usize;
        for i in 0..data.len() / bs {
            if !inner.owned[start + i] {
                inner.bytes[(start + i) * bs..(start + i + 1) * bs]
                    .copy_from_slice(&data[i * bs..(i + 1) * bs]);
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
