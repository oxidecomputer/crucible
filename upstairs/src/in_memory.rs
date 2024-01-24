// Copyright 2021 Oxide Computer Company

use super::*;

struct Inner {
    bytes: Vec<u8>,

    /// Ownership is tracked per block
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
        // TODO: make this take (block_count, block_size) so that it matches
        // Buffer; this requires changing a bunch of call sites.
        assert_eq!(total_size % block_size as usize, 0);
        Self {
            uuid: id,
            block_size,
            inner: Mutex::new(Inner {
                bytes: vec![0; total_size],
                owned: vec![false; total_size / block_size as usize],
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
        let bs = self.check_data_size(data.len()).await? as usize;
        let inner = self.inner.lock().await;

        let start = offset.value as usize * bs;

        data.write_with_ownership(
            0,
            &inner.bytes[start..][..data.len()],
            &inner.owned[offset.value as usize..][..data.len() / bs],
        );

        Ok(())
    }

    /// Write from `data` into `self`, setting all owned bits to `true`
    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        let bs = self.check_data_size(data.len()).await? as usize;
        let mut inner = self.inner.lock().await;

        let start_block = offset.value as usize;
        for (b, chunk) in data.chunks(bs).enumerate() {
            let block = start_block + b;
            inner.owned[block] = true;
            inner.bytes[block * bs..][..bs].copy_from_slice(chunk);
        }

        Ok(())
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        let bs = self.check_data_size(data.len()).await? as usize;
        let mut inner = self.inner.lock().await;

        let start_block = offset.value as usize;
        for (b, chunk) in data.chunks(bs).enumerate() {
            let block = start_block + b;
            if !inner.owned[block] {
                inner.owned[block] = true;
                inner.bytes[block * bs..][..bs].copy_from_slice(chunk);
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
