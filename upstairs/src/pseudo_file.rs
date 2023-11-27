// Copyright 2021 Oxide Computer Company
use super::*;
use std::io::{Read, Write};

/*
 * IO operations are okay to submit directly to Upstairs if:
 *
 * - the offset is block aligned, and
 * - the size is a multiple of block size
 *
 * If either of these is not true, then perform some fix up here.
 */
#[derive(Debug)]
pub struct IOSpan {
    // IOP details
    offset: u64,
    sz: u64,

    // Block and details
    block_size: u64,
    phase: u64,
    buffer: Buffer,
    affected_block_numbers: Vec<u64>,
}

impl IOSpan {
    // Create an IOSpan given a IO operation at offset and size.
    pub fn new(offset: u64, sz: u64, block_size: u64) -> IOSpan {
        let start_block = offset / block_size;
        let end_block = (offset + sz - 1) / block_size;

        let affected_block_numbers: Vec<u64> =
            (start_block..=end_block).collect();

        Self {
            offset,
            sz,
            block_size,
            phase: offset % block_size,
            buffer: Buffer::new(
                affected_block_numbers.len() * block_size as usize,
            ),
            affected_block_numbers,
        }
    }

    pub fn is_block_regular(&self) -> bool {
        let is_block_aligned = (self.offset % self.block_size) == 0;
        let is_block_sized = (self.sz % self.block_size) == 0;

        is_block_aligned && is_block_sized
    }

    #[cfg(test)]
    pub fn affected_block_count(&self) -> usize {
        self.affected_block_numbers.len()
    }

    #[cfg(test)]
    pub fn affected_block_numbers(&self) -> &Vec<u64> {
        &self.affected_block_numbers
    }

    #[cfg(test)]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    #[instrument(skip(block_io))]
    pub async fn read_affected_blocks_from_volume<T: BlockIO>(
        &mut self,
        block_io: &Arc<T>,
    ) -> Result<(), CrucibleError> {
        block_io
            .read(
                Block::new(
                    self.affected_block_numbers[0],
                    self.block_size.trailing_zeros(),
                ),
                self.buffer.clone(),
            )
            .await
    }

    #[instrument(skip(block_io))]
    pub async fn write_affected_blocks_to_volume<T: BlockIO>(
        &self,
        block_io: &Arc<T>,
    ) -> Result<(), CrucibleError> {
        let bytes = Bytes::from(self.buffer.as_vec().await.clone());

        block_io
            .write(
                Block::new(
                    self.affected_block_numbers[0],
                    self.block_size.trailing_zeros(),
                ),
                bytes,
            )
            .await
    }

    #[instrument]
    pub async fn read_from_blocks_into_buffer(&self, data: &mut [u8]) {
        assert_eq!(data.len(), self.sz as usize);

        for (i, item) in data.iter_mut().enumerate() {
            *item = self.buffer.as_vec().await[self.phase as usize + i];
        }
    }

    #[instrument]
    pub async fn write_from_buffer_into_blocks(&self, data: &[u8]) {
        assert_eq!(data.len(), self.sz as usize);

        for (i, item) in data.iter().enumerate() {
            self.buffer.as_vec().await[self.phase as usize + i] = *item;
        }
    }
}

/*
 * Wrap a Crucible volume and implement Read + Write + Seek traits.
 */
pub struct CruciblePseudoFile<T: BlockIO> {
    active: bool,
    block_io: Arc<T>,
    offset: u64,
    sz: u64,
    block_size: u64,
    rmw_lock: RwLock<bool>,
    uuid: Uuid,
}

impl<T: BlockIO> CruciblePseudoFile<T> {
    pub fn from(block_io: Arc<T>) -> Result<Self, CrucibleError> {
        Ok(CruciblePseudoFile {
            active: false,
            block_io,
            offset: 0,
            sz: 0,
            block_size: 0,
            rmw_lock: RwLock::new(false),
            uuid: Uuid::default(),
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn sz(&self) -> u64 {
        self.sz
    }

    pub async fn activate(&mut self) -> Result<(), CrucibleError> {
        if let Err(e) = self.block_io.activate().await {
            match e {
                CrucibleError::UpstairsAlreadyActive => {
                    // underlying block io is already active, but pseudo file
                    // needs fields below populated
                }
                _ => {
                    return Err(e);
                }
            }
        }

        self.sz = self.block_io.total_size().await?;
        self.block_size = self.block_io.get_block_size().await?;
        self.uuid = self.block_io.get_uuid().await?;

        self.active = true;

        Ok(())
    }

    pub async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        self.block_io.show_work().await
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

/*
 * The Read + Write impls here translate arbitrary sized operations into
 * calls for the underlying Crucible API.
 */
impl<T: BlockIO> Read for CruciblePseudoFile<T> {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        if !self.active {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                CrucibleError::UpstairsInactive,
            ));
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self._read(buf))
        })
        .map_err(|e| e.into())
    }
}

impl<T: BlockIO> Write for CruciblePseudoFile<T> {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        if !self.active {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                CrucibleError::UpstairsInactive,
            ));
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self._write(buf))
        })
        .map_err(|e| e.into())
    }

    fn flush(&mut self) -> IOResult<()> {
        if !self.active {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                CrucibleError::UpstairsInactive,
            ));
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self._flush())
        })
        .map_err(|e| e.into())
    }
}

impl<T: BlockIO> Seek for CruciblePseudoFile<T> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        // TODO: let guard = self.rmw_lock.write().unwrap() ?
        // TODO: does not check against block device size

        let mut offset: i64 = self.offset as i64;
        match pos {
            SeekFrom::Start(v) => {
                offset = v as i64;
            }
            SeekFrom::Current(v) => {
                offset += v;
            }
            SeekFrom::End(v) => {
                offset = self.sz as i64 + v;
            }
        }

        if offset < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "offset is negative!",
            ))
        } else {
            // offset >= 0
            self.offset = offset as u64;
            Ok(self.offset)
        }
    }

    fn stream_position(&mut self) -> IOResult<u64> {
        Ok(self.offset)
    }
}

impl<T: BlockIO> CruciblePseudoFile<T> {
    async fn _read(&mut self, buf: &mut [u8]) -> Result<usize, CrucibleError> {
        let _guard = self.rmw_lock.read().await;

        let mut span =
            IOSpan::new(self.offset, buf.len() as u64, self.block_size);

        span.read_affected_blocks_from_volume(&self.block_io)
            .await?;

        span.read_from_blocks_into_buffer(buf).await;

        // TODO: for block devices, we can't increment offset past the
        // device size but we're supposed to be pretending to be a proper
        // file here
        self.offset += buf.len() as u64;

        Ok(buf.len())
    }

    async fn _write(&mut self, buf: &[u8]) -> Result<usize, CrucibleError> {
        let mut span =
            IOSpan::new(self.offset, buf.len() as u64, self.block_size);

        /*
         * Crucible's dependency system will properly resolve requests in
         * the order they are received but if the request is not block
         * aligned and block sized we need to do read-modify-write (RMW)]
         * here. Use a reader-writer lock, and grab the write portion of
         * the lock when doing RMW to cause all other operations (which
         * only grab the read portion
         * of the lock) to pause. Otherwise all operations can use the
         * read portion of this lock and Crucible will sort it out.
         */
        if !span.is_block_regular() {
            let _guard = self.rmw_lock.write().await;

            span.read_affected_blocks_from_volume(&self.block_io)
                .await?;

            span.write_from_buffer_into_blocks(buf).await;

            span.write_affected_blocks_to_volume(&self.block_io).await?;
        } else {
            let _guard = self.rmw_lock.read().await;

            let offset = Block::new(
                self.offset / self.block_size,
                self.block_size.trailing_zeros(),
            );
            let bytes = BytesMut::from(buf);
            self.block_io.write(offset, bytes.freeze()).await?;
        }

        // TODO: can't increment offset past the device size
        self.offset += buf.len() as u64;

        Ok(buf.len())
    }

    async fn _flush(&mut self) -> Result<(), CrucibleError> {
        let _guard = self.rmw_lock.write().await;

        self.block_io.flush(None).await?;

        Ok(())
    }
}
