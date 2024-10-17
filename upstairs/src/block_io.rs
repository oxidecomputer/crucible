// Copyright 2023 Oxide Computer Company

use super::*;

use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU32, Ordering};

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

    async fn activate_with_gen(&self, _gen: u64) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn deactivate(&self) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError> {
        Ok(None)
    }

    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        Ok(WQCounts {
            up_count: 0,
            ds_count: 0,
            active_count: 0,
        })
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
        offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let start: usize = (offset.0 * self.block_size) as usize;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start as u64))?;

        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf)?;
        data.write(0, &buf);

        Ok(())
    }

    async fn write(
        &self,
        offset: BlockIndex,
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let start = offset.0 * self.block_size;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start))?;
        file.write_all(&data[..])?;

        Ok(())
    }

    async fn write_unwritten(
        &self,
        _offset: BlockIndex,
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

// Implement BlockIO over an HTTP(S) url
use reqwest::header::{CONTENT_LENGTH, RANGE};
use reqwest::Client;
use std::str::FromStr;

pub struct ReqwestBlockIO {
    uuid: Uuid,
    block_size: u64,
    total_size: u64,
    client: Client,
    url: String,
    count: AtomicU32, // Used for dtrace probes
}

impl ReqwestBlockIO {
    pub async fn new(
        id: Uuid,
        block_size: u64,
        url: String,
    ) -> Result<Self, CrucibleError> {
        let client = Client::new();

        let response = client
            .head(&url)
            .send()
            .await
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;
        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or("no content length!")
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;
        let total_size = u64::from_str(
            content_length
                .to_str()
                .map_err(|e| CrucibleError::GenericError(e.to_string()))?,
        )
        .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        Ok(Self {
            uuid: id,
            block_size,
            total_size,
            client,
            url,
            count: AtomicU32::new(0),
        })
    }

    // Increment the counter to allow all IOs to have a unique number
    // for dtrace probes.
    pub fn next_count(&self) -> u32 {
        self.count.fetch_add(1, Ordering::Relaxed)
    }
}

#[async_trait]
impl BlockIO for ReqwestBlockIO {
    async fn activate(&self) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn activate_with_gen(&self, _gen: u64) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn deactivate(&self) -> Result<(), CrucibleError> {
        Ok(())
    }

    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        Ok(WQCounts {
            up_count: 0,
            ds_count: 0,
            active_count: 0,
        })
    }

    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError> {
        Ok(None)
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
        offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        self.check_data_size(data.len()).await?;
        let cc = self.next_count();
        cdt::reqwest__read__start!(|| (cc, self.uuid));

        let start = offset.0 * self.block_size;

        let response = self
            .client
            .get(&self.url)
            .header(
                RANGE,
                format!("bytes={}-{}", start, start + data.len() as u64 - 1),
            )
            .send()
            .await
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or("no content length!")
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;
        let total_size = u64::from_str(
            content_length
                .to_str()
                .map_err(|e| CrucibleError::GenericError(e.to_string()))?,
        )
        .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        // Did the HTTP server _not_ honour the Range request?
        if total_size != data.len() as u64 {
            crucible_bail!(
                IoError,
                "Requested {} bytes but HTTP server returned {}!",
                data.len(),
                total_size
            );
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| CrucibleError::GenericError(e.to_string()))?;

        data.write(0, &bytes);

        cdt::reqwest__read__done!(|| (cc, self.uuid));
        Ok(())
    }

    async fn write(
        &self,
        _offset: BlockIndex,
        _data: BytesMut,
    ) -> Result<(), CrucibleError> {
        crucible_bail!(Unsupported, "write unsupported for ReqwestBlockIO")
    }

    async fn write_unwritten(
        &self,
        _offset: BlockIndex,
        _data: BytesMut,
    ) -> Result<(), CrucibleError> {
        crucible_bail!(
            Unsupported,
            "write_unwritten unsupported for ReqwestBlockIO"
        )
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
