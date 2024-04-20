// Copyright 2023 Oxide Computer Company

use super::*;

use std::fs::{File, OpenOptions};
use std::io::SeekFrom;

/// Implement BlockIO for a file
pub struct FileBlockIO {
    uuid: Uuid,
    block_size: u64,
    total_size: u64,
    file: Mutex<File>,
}

impl FileBlockIO {
    pub fn new(id: Uuid, block_size: u64, path: String) -> Result<Self> {
        match OpenOptions::new().read(true).write(true).open(&path) {
            Err(e) => {
                bail!("Error: e {} No extent file found for {:?}", e, path);
            }
            Ok(f) => {
                let total_size = f.metadata()?.len();

                Ok(Self {
                    uuid: id,
                    block_size,
                    total_size,
                    file: Mutex::new(f),
                })
            }
        }
    }
}

#[async_trait]
impl BlockIO for FileBlockIO {
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
        Ok(self.total_size)
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
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let start: usize = (offset.value * self.block_size) as usize;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start as u64))?;

        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf)?;
        data.write(0, &buf);

        Ok(())
    }

    async fn write(
        &self,
        offset: Block,
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let start = offset.value * self.block_size;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start))?;
        file.write_all(&data[..])?;

        Ok(())
    }

    async fn write_unwritten(
        &self,
        _offset: Block,
        _data: BytesMut,
    ) -> Result<(), CrucibleError> {
        crucible_bail!(
            Unsupported,
            "write_unwritten unsupported for FileBlockIO"
        )
    }

    async fn flush(
        &self,
        _snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        let mut file = self.file.lock().await;
        file.flush()?;
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
