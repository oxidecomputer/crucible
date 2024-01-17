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
    owned: Mutex<Vec<bool>>,
}

impl FileBlockIO {
    pub fn new(id: Uuid, block_size: u64, path: String) -> Result<Self> {
        match OpenOptions::new().read(true).write(true).open(&path) {
            Err(e) => {
                bail!("Error: e {} No extent file found for {:?}", e, path);
            }
            Ok(f) => {
                let total_size = f.metadata()?.len();
                let owned = vec![false; total_size as usize];

                Ok(Self {
                    uuid: id,
                    block_size,
                    total_size,
                    file: Mutex::new(f),
                    owned: Mutex::new(owned),
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
        mut data: Buffer,
    ) -> Result<Buffer, CrucibleError> {
        let start: usize = (offset.value * self.block_size) as usize;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start as u64))?;

        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf)?;

        let owned = self.owned.lock().await;
        data.write_with_ownership(0, &buf, &owned[start..][..data.len()]);

        Ok(data)
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        let start = offset.value * self.block_size;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start))?;
        file.write_all(&data[..])?;

        let mut owned = self.owned.lock().await;
        owned[(start as usize)..][..data.len()].fill(true);

        Ok(())
    }

    async fn write_unwritten(
        &self,
        _offset: Block,
        _data: Bytes,
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
        mut data: Buffer,
    ) -> Result<Buffer, CrucibleError> {
        let cc = self.next_count();
        cdt::reqwest__read__start!(|| (cc, self.uuid));

        let start = offset.value * self.block_size;

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
        Ok(data)
    }

    async fn write(
        &self,
        _offset: Block,
        _data: Bytes,
    ) -> Result<(), CrucibleError> {
        crucible_bail!(Unsupported, "write unsupported for ReqwestBlockIO")
    }

    async fn write_unwritten(
        &self,
        _offset: Block,
        _data: Bytes,
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

#[cfg(test)]
mod test {
    use super::*;

    use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};
    use tempfile::tempdir;

    const BLOCK_SIZE: usize = 512;

    /// Test that `block_io`, which is at least of size 1024, has the proper
    /// ownership behaviour.
    async fn test_ownership(block_io: impl BlockIO) -> Result<()> {
        // Ownership starts off false

        let buffer = Buffer::new(1024);
        let buffer = block_io
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer)
            .await?;

        assert_eq!(buffer.owned_ref(), &[false; 1024]);

        // Ownership is set by writing to blocks

        block_io
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![9; 512]),
            )
            .await?;

        // Ownership is returned properly

        let buffer = Buffer::new(1024);
        let buffer = block_io
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer)
            .await?;

        let mut expected = vec![9u8; 512];
        expected.extend(vec![0u8; 512]);

        assert_eq!(&*buffer, &expected);

        let mut expected_ownership = vec![true; 512];
        expected_ownership.extend(vec![false; 512]);

        assert_eq!(buffer.owned_ref(), &expected_ownership);

        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_file() -> Result<()> {
        let dir = tempdir()?;
        let mut path = dir.path().to_path_buf();
        path.push("downstairs");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.set_len(2048)?;
        drop(file);

        let block_io = FileBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            path.into_os_string().into_string().unwrap(),
        )?;

        test_ownership(block_io).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_in_memory() -> Result<()> {
        let block_io =
            InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE as u64, 2048);

        test_ownership(block_io).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_http() -> Result<()> {
        // ReqwestBlockIO does not support write so we can't use the
        // `test_ownership` helper. Also, httptest's server does not support
        // Range requests, so we have to limit it to one block.

        let server = ServerBuilder::new().run()?;

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/image.raw"))
                .times(1..)
                .respond_with(
                    status_code(200)
                        .append_header("Content-Length", format!("{}", 512)),
                ),
        );

        server.expect(
            Expectation::matching(request::method_path("GET", "/image.raw"))
                .times(1..)
                .respond_with(status_code(200).body(vec![9; 512])),
        );

        let block_io = ReqwestBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            server.url("/image.raw").to_string(),
        )
        .await?;

        // Ownership from this will be true always, `ReqwestBlockIO` does not
        // support writes.

        let buffer = Buffer::new(512);
        let buffer = block_io
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer)
            .await?;

        assert_eq!(&*buffer, &[9; 512]);
        assert_eq!(buffer.owned_ref(), &[true; 512]);

        Ok(())
    }
}
