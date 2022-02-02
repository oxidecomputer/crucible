// Copyright 2021 Oxide Computer Company

use super::*;

pub struct InMemoryBlockIO {
    uuid: Uuid,
    block_size: u64,
    bytes: Mutex<Vec<u8>>,
    owned: Mutex<Vec<bool>>,
}

impl InMemoryBlockIO {
    pub fn new(block_size: u64, total_size: usize) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            block_size,
            bytes: Mutex::new(vec![0; total_size]),
            owned: Mutex::new(vec![false; total_size]),
        }
    }
}

impl BlockIO for InMemoryBlockIO {
    fn activate(&self, _gen: u64) -> Result<(), CrucibleError> {
        Ok(())
    }

    fn query_is_active(&self) -> Result<bool, CrucibleError> {
        Ok(true)
    }

    fn total_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.bytes.lock().unwrap().len() as u64)
    }

    fn get_block_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.block_size)
    }

    fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        Ok(self.uuid)
    }

    fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let mut data_vec = data.as_vec();
        let mut owned_vec = data.owned_vec();

        let bytes = self.bytes.lock().unwrap();
        let owned = self.owned.lock().unwrap();

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data_vec.len() {
            data_vec[i] = bytes[start + i];
            owned_vec[i] = owned[start + i];
        }

        BlockReqWaiter::immediate()
    }

    fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let mut bytes = self.bytes.lock().unwrap();
        let mut owned = self.owned.lock().unwrap();

        let start = offset.value as usize * self.block_size as usize;

        for i in 0..data.len() {
            bytes[start + i] = data[i];
            owned[start + i] = true;
        }

        BlockReqWaiter::immediate()
    }

    fn flush(&self) -> Result<BlockReqWaiter, CrucibleError> {
        BlockReqWaiter::immediate()
    }

    fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        Ok(WQCounts {
            up_count: 0,
            ds_count: 0,
        })
    }
}
