// Copyright 2023 Oxide Computer Company
use crate::ReadResponse;
use bytes::{Bytes, BytesMut};

/*
 * Provides a strongly-owned Buffer that Read operations will write into.
 *
 * Ownership of a block is defined as true if that block has been written to: we
 * say a block is "owned" if the bytes were written to by something, rather than
 * having been initialized to zero. For an Upstairs, a block is owned if it was
 * returned with a non-zero number of block contexts, encrypted or not. This is
 * important when using authenticated encryption to distinguish between zeroes
 * that the Guest has written and blocks that were zero to begin with.
 *
 * It's safe to set ownership to `true` if there's no persistence of ownership
 * information. Persistence is required otherwise: if a particular `BlockIO`
 * implementation is dropped and recreated, the ownership should not be lost as
 * a result.
 *
 * Because persistence is required, ownership will always come from the
 * Downstairs (or other `BlockIO` implementations that persist ownership
 * information) and be propagated "up".
 *
 * The `Buffer` is a block-oriented data structure: any functions which read or
 * write to it's data must be block-aligned and block sized.  Otherwise, these
 * functions will panic.  Any function which panics also notes those conditions
 * in its docstring.
 */
#[must_use]
#[derive(Debug, PartialEq, Default)]
pub struct Buffer {
    block_size: usize,
    data: BytesMut,

    /// Per-block ownership data, using 0 = false and 1 = true
    ///
    /// `owned.len() == data.len() / block_size`
    owned: BytesMut,
}

impl Buffer {
    pub fn new(block_count: usize, block_size: usize) -> Buffer {
        Self::repeat(0, block_count, block_size)
    }

    /// Builds a new buffer that repeats the given value
    pub fn repeat(v: u8, block_count: usize, block_size: usize) -> Self {
        let mut out = Self::default();
        out.reset_with(v, block_count, block_size);
        out
    }

    pub fn with_capacity(block_count: usize, block_size: usize) -> Buffer {
        let len = block_count * block_size;
        let data = BytesMut::with_capacity(len);
        let owned = BytesMut::with_capacity(block_count);

        Buffer {
            block_size,
            data,
            owned,
        }
    }

    /// Extracts and freezes the underlying `BytesMut` bearing buffered data.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.data.freeze()
    }

    /// Extracts the underlying `BytesMut`
    #[must_use]
    pub fn into_bytes_mut(self) -> BytesMut {
        self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Writes data to the buffer, setting `owned` to `true`
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write(&mut self, offset: usize, data: &[u8]) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() % self.block_size, 0);

        self.data[offset..][..data.len()].copy_from_slice(data);
        self.owned[offset / self.block_size..][..data.len() / self.block_size]
            .fill(1);
    }

    /// Writes data to the buffer where `owned` is true
    ///
    /// If `owned[i]` is `false`, then that block is not written
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    /// - `data` and `owned` must be the same size
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write_if_owned(
        &mut self,
        offset: usize,
        data: &[u8],
        owned: &[bool],
    ) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(data.len() % self.block_size, 0);
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() / self.block_size, owned.len());

        let start_block = offset / self.block_size;
        for (b, chunk) in data.chunks(self.block_size).enumerate() {
            debug_assert_eq!(chunk.len(), self.block_size);
            if owned[b] {
                let block = start_block + b;
                self.owned[block] = 1;
                self.block_mut(block).copy_from_slice(chunk);
            }
        }
    }

    /// Writes a `ReadResponse` into the buffer, setting `owned` to true
    ///
    /// The `ReadResponse` must contain a single block's worth of data.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The response data length must be block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn write_read_response(
        &mut self,
        offset: usize,
        response: &ReadResponse,
    ) {
        assert!(offset + response.data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(response.data.len(), self.block_size);
        if !response.block_contexts.is_empty() {
            let block = offset / self.block_size;
            self.owned[block] = 1;
            self.block_mut(block).copy_from_slice(&response.data);
        }
    }

    /// Reads buffer data into the given array
    ///
    /// Only blocks with `self.owned` are changed; other blocks are left
    /// unmodified.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - The response data length must be divisible by block size
    /// - Data cannot exceed the buffer's length
    ///
    /// If any of these conditions are not met, the function will panic.
    pub(crate) fn read(&self, offset: usize, data: &mut [u8]) {
        assert!(offset + data.len() <= self.data.len());
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(data.len() % self.block_size, 0);

        let start_block = offset / self.block_size;
        for (b, chunk) in data.chunks_mut(self.block_size).enumerate() {
            debug_assert_eq!(chunk.len(), self.block_size);
            let block = start_block + b;
            if self.owned[block] != 0 {
                chunk.copy_from_slice(self.block(block));
            }
        }
    }

    /// Consumes the buffer and returns a `Vec<u8>` object
    ///
    /// This is inefficient and should only be used during testing
    #[cfg(test)]
    pub fn into_vec(self) -> Vec<u8> {
        self.data.into_iter().collect()
    }

    /// Consume and layer buffer contents on top of this one
    ///
    /// Returns an uninitialized buffer, for ease of memory reuse.
    ///
    /// # Panics
    /// - The offset must be block-aligned
    /// - Both buffers must have the same block size
    ///
    /// If either of these conditions is not met, the function will panic
    #[must_use]
    pub(crate) fn eat(
        &mut self,
        offset: usize,
        mut buffer: Buffer,
    ) -> UninitializedBuffer {
        assert_eq!(offset % self.block_size, 0);
        assert_eq!(self.block_size, buffer.block_size);

        let start_block = offset / self.block_size;

        // Special case: if we're reading the entire buffer and the incoming
        // buffer is fully owned, then swap pointers instead of copying data.
        if start_block == 0
            && self.len() == buffer.len()
            && buffer.owned.iter().all(|o| *o != 0)
        {
            std::mem::swap(self, &mut buffer);
        } else {
            for (b, (owned, chunk)) in buffer.blocks().enumerate() {
                if owned {
                    let block = start_block + b;
                    self.owned[block] = 1;
                    self.block_mut(block).copy_from_slice(chunk);
                }
            }
        }
        UninitializedBuffer(buffer)
    }

    pub fn owned_ref(&self) -> &[u8] {
        &self.owned
    }

    fn reset_with(&mut self, v: u8, block_count: usize, block_size: usize) {
        self.data.clear();
        self.owned.clear();

        let len = block_count * block_size;
        self.data.resize(len, v);
        self.owned.resize(block_count, 0);
        self.block_size = block_size;
    }

    pub fn reset(&mut self, block_count: usize, block_size: usize) {
        self.reset_with(0, block_count, block_size);
    }

    /// Returns a reference to a particular block
    pub fn block(&self, b: usize) -> &[u8] {
        &self.data[b * self.block_size..][..self.block_size]
    }

    /// Returns a mutable reference to a particular block
    pub fn block_mut(&mut self, b: usize) -> &mut [u8] {
        &mut self.data[b * self.block_size..][..self.block_size]
    }

    /// Returns an iterator over `(owned, block)` tuples
    pub fn blocks(&self) -> impl Iterator<Item = (bool, &[u8])> {
        self.owned
            .iter()
            .map(|v| *v != 0)
            .zip(self.data.chunks(self.block_size))
    }

    /// Splits the buffer into two at the given block index.
    ///
    /// Afterwards `self` contains blocks `[0, block_index)`, and the returned
    /// `Buffer` contains blocks `[block_index, capacity)`.
    ///
    /// This is an `O(1)` operation that just increases the reference count and
    /// sets a few indices.
    pub(crate) fn split_off(&mut self, block_index: usize) -> Self {
        let data = self.data.split_off(block_index * self.block_size);
        let owned = self.owned.split_off(block_index);
        Self {
            block_size: self.block_size,
            data,
            owned,
        }
    }

    /// Splits the buffer into two at the given block index.
    ///
    /// Afterwards `self` contains blocks `[block_index, len)`, and the returned
    /// `Buffer` contains blocks `[0, block_index)`.
    ///
    /// This is an `O(1)` operation that just increases the reference count and
    /// sets a few indices.
    pub(crate) fn split_to(&mut self, block_index: usize) -> Self {
        let data = self.data.split_to(block_index * self.block_size);
        let owned = self.owned.split_to(block_index);
        Self {
            block_size: self.block_size,
            data,
            owned,
        }
    }

    /// Absorbs a `Buffer` that was previously split off
    ///
    /// If the two `Buffer` objects were previously contiguous and not mutated
    /// in a way that causes re-allocation i.e., if other was created by calling
    /// `split_off` on this `Buffer`, then this is an `O(1)` operation that
    /// just decreases a reference count and sets a few indices. Otherwise, this
    /// method calls `extend_from_slice` on both the data and ownership
    /// `Buffer`.
    ///
    /// # Panics
    /// If `self.block_size != other.block_size`
    pub(crate) fn unsplit(&mut self, other: Buffer) {
        assert_eq!(self.block_size, other.block_size);
        self.data.unsplit(other.data);
        self.owned.unsplit(other.owned);
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/// Buffer with unknown contents, which must be reset before it can be used
#[derive(Debug, Default)]
pub(crate) struct UninitializedBuffer(Buffer);

impl UninitializedBuffer {
    /// Resets to the given size, clearing the `owned` array
    ///
    /// The data array is resized (padding with 0s) but is not cleared; it is
    /// expected that this function will be called right before reading into it,
    /// so clearing it is unnecessary.
    pub(crate) fn reset_owned(
        self,
        block_count: usize,
        block_size: usize,
    ) -> Buffer {
        let mut out = self.0;
        let len = block_count * block_size;
        out.data.resize(len, 0u8);
        out.owned.fill(0u8);
        out.owned.resize(block_count, 0u8);
        out.block_size = block_size;
        out
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_buffer_sane() {
        const BLOCK_SIZE: usize = 512;
        let mut data = Buffer::new(2, BLOCK_SIZE);

        data.write(0, &[99u8; BLOCK_SIZE]);

        let mut read_data = vec![0u8; BLOCK_SIZE];
        data.read(0, &mut read_data);

        assert_eq!(&read_data[..], &data[..BLOCK_SIZE]);
        assert_eq!(&data[..BLOCK_SIZE], &[99u8; BLOCK_SIZE]);
    }

    #[test]
    fn test_buffer_len() {
        const READ_SIZE: usize = 512;
        let data = Buffer::repeat(0x99, 1, READ_SIZE);
        assert_eq!(data.len(), READ_SIZE);
    }

    #[test]
    fn test_buffer_len_over_block_size() {
        const READ_SIZE: usize = 1024;
        let data = Buffer::repeat(0x99, 2, 512);
        assert_eq!(data.len(), READ_SIZE);
    }

    #[test]
    fn test_buffer_writes() {
        const BLOCK_COUNT: usize = 8;
        const BLOCK_SIZE: usize = 64;
        let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE); // 512 bytes

        assert_eq!(&data[..], &vec![0u8; 512]);

        data.write(64, &[1u8; 64]);

        assert_eq!(&data[0..64], &vec![0u8; 64]);
        assert_eq!(&data[64..128], &vec![1u8; 64]);
        assert_eq!(&data[128..], &vec![0u8; 512 - 64 - 64]);

        data.write(128, &[7u8; 128]);

        assert_eq!(&data[0..64], &vec![0u8; 64]);
        assert_eq!(&data[64..128], &vec![1u8; 64]);
        assert_eq!(&data[128..256], &vec![7u8; 128]);
        assert_eq!(&data[256..], &vec![0u8; 256]);
    }

    #[test]
    fn test_buffer_eats() {
        // We use an artificially low BLOCK_SIZE here for ease of alignment
        const BLOCK_COUNT: usize = 8;
        const BLOCK_SIZE: usize = 64;
        let data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE); // 512 bytes

        assert_eq!(&data[..], &vec![0u8; 512]);

        let mut buffer = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
        let _ = buffer.eat(0, data);

        assert_eq!(&buffer[..], &vec![0u8; 512]);

        let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
        data.write(64, &[1u8; 64]);
        let _ = buffer.eat(0, data);

        assert_eq!(&buffer[0..64], &vec![0u8; 64]);
        assert_eq!(&buffer[64..128], &vec![1u8; 64]);
        assert_eq!(&buffer[128..], &vec![0u8; 512 - 64 - 64]);

        let mut data = Buffer::new(BLOCK_COUNT, BLOCK_SIZE);
        data.write(128, &[7u8; 128]);
        let _ = buffer.eat(0, data);

        assert_eq!(&buffer[0..64], &vec![0u8; 64]);
        assert_eq!(&buffer[64..128], &vec![1u8; 64]);
        assert_eq!(&buffer[128..256], &vec![7u8; 128]);
        assert_eq!(&buffer[256..], &vec![0u8; 256]);
    }

    #[test]
    fn block_eat_without_memcpy() {
        let mut upper = Buffer::new(10, 512);
        let mut lower = Buffer::new(10, 512);

        // Write block 1
        lower.write(512, &[0xFE; 512]);
        let prev_ptr = upper.as_ptr();

        // Eating a partially owned buffer requires a memcpy
        let _ = upper.eat(0, lower);
        assert_eq!(prev_ptr, upper.as_ptr());

        // Eating the entire owned buffer just swaps pointers
        let mut lower = Buffer::new(10, 512);
        lower.write(0, &[0xFE; 512 * 10]);
        let prev_ptr = lower.as_ptr();
        let _ = upper.eat(0, lower);
        assert_eq!(prev_ptr, upper.as_ptr());
    }
}
