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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn wait_all_is_ordered_for_in_memory() {
        // In a loop, test that submitting a big batch of operations and waiting
        // on their BlockReqWaiters in parallel is correct, even for
        // InMemoryBlockIO (which does not perform any internal queueing). This
        // code should look exactly like the matching integration tests because
        // users of structs that impl BlockIO should expect the same behaviour.
        //
        // It's important that this test use multiple worker_threads!
        for x in 0..100 {
            const BLOCK_SIZE: usize = 512;
            const NUM_BLOCKS: usize = 512;

            let in_memory = InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE as u64,
                NUM_BLOCKS * BLOCK_SIZE,
            );

            let mut waiters = Vec::with_capacity(NUM_BLOCKS);

            for i in 0..NUM_BLOCKS {
                let byte_val: u8 = ((x + i) % 256) as u8;
                waiters.push(
                    // the writes we submit should overlap:
                    //
                    // 0: 00000000
                    // 1:     11111111
                    // 2:         22222222
                    // ...
                    //
                    in_memory.write(
                        Block::new_512(i as u64),
                        // every write but the last should overlap
                        if (i + 1) < NUM_BLOCKS {
                            Bytes::from(vec![byte_val; BLOCK_SIZE * 2])
                        } else {
                            Bytes::from(vec![byte_val; BLOCK_SIZE])
                        },
                    ).await.unwrap(),
                );
            }

            waiters.push(in_memory.flush(None).await.unwrap());

            super::wait_all(waiters).await.unwrap();

            // Read the data, and validate the in-memory contents
            let read_buffer = Buffer::new(NUM_BLOCKS * BLOCK_SIZE);
            in_memory
                .read(Block::new_512(0), read_buffer.clone())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();

            let data = read_buffer.as_vec().await;

            for i in 0..NUM_BLOCKS {
                let actual = &data[(i * BLOCK_SIZE)..((i + 1) * BLOCK_SIZE)];
                let byte_val: u8 = ((x + i) % 256) as u8;
                let expected = vec![byte_val; BLOCK_SIZE];
                assert_eq!(actual, expected);
            }
        }
    }
}
