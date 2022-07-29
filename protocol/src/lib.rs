// Copyright 2021 Oxide Computer Company
use std::cmp::Ordering;
use std::net::SocketAddr;

use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

const MAX_FRM_LEN: usize = 100 * 1024 * 1024; // 100M

use crucible_common::{Block, CrucibleError, RegionDefinition};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Write {
    pub eid: u64,
    pub offset: Block,
    pub data: bytes::Bytes,
    pub encryption_context: Option<EncryptionContext>,

    /*
     * If this is a non-encrypted write, then the integrity hasher has the
     * data as an input:
     *
     *   let hasher = Hasher()
     *   hasher.write(&data)
     *   hash = hasher.digest()
     *
     * If this is an encrypted write, then the integrity hasher has the
     * nonce, then tag, then data written to it.
     *
     *   let hasher = Hasher()
     *   hasher.write(&nonce)
     *   hasher.write(&tag)
     *   hasher.write(&data)
     *   hash = hasher.digest()
     */
    pub hash: u64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub eid: u64,
    pub offset: Block,
    pub num_blocks: u64,
}

// Note: if you change this, you may have to add to the dump commands that show
// block specific data.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub eid: u64,
    pub offset: Block,
    pub num_blocks: u64,

    pub data: bytes::BytesMut,
    pub encryption_contexts: Vec<EncryptionContext>,
    pub hashes: Vec<u64>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EncryptionContext {
    pub nonce: Vec<u8>,
    pub tag: Vec<u8>,
}

impl ReadResponse {
    pub fn from_request(request: &ReadRequest, bs: usize) -> ReadResponse {
        /*
         * XXX Some thought will need to be given to where the read
         * data buffer is created, both on this side and the remote.
         * Also, we (I) need to figure out how to read data into an
         * uninitialized buffer. Until then, we have this workaround.
         */
        let sz = request.num_blocks as usize * bs;
        let mut data = BytesMut::with_capacity(sz);
        data.resize(sz, 1);

        ReadResponse {
            eid: request.eid,
            offset: request.offset,
            num_blocks: request.num_blocks,
            data,
            encryption_contexts: vec![],
            hashes: vec![],
        }
    }

    pub fn from_request_with_data(
        request: &ReadRequest,
        data: &[u8],
    ) -> ReadResponse {
        ReadResponse {
            eid: request.eid,
            offset: request.offset,
            num_blocks: request.num_blocks,
            data: BytesMut::from(data),
            encryption_contexts: vec![],
            hashes: vec![crucible_common::integrity_hash(&[data])],
        }
    }
}

/**
 * These enums are for messages sent between an Upstairs and a Downstairs
 */
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SnapshotDetails {
    pub snapshot_name: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Message {
    /**
     * Initial negotiation messages
     */
    HereIAm {
        version: u32,
        upstairs_id: Uuid,
        session_id: Uuid,
        gen: u64,
    },
    YesItsMe {
        version: u32,
    },

    /**
     * Forcefully tell this downstairs to promote us (an Upstairs) to
     * active.
     *
     * Kick out the old Upstairs.
     */
    PromoteToActive {
        upstairs_id: Uuid,
        session_id: Uuid,
        gen: u64,
    },
    YouAreNowActive {
        upstairs_id: Uuid,
        session_id: Uuid,
        gen: u64,
    },
    YouAreNoLongerActive {
        new_upstairs_id: Uuid,
        new_session_id: Uuid,
        new_gen: u64,
    },

    /*
     * If downstairs sees a UUID that doesn't match what was negotiated, it
     * will send this message.
     */
    UuidMismatch {
        expected_id: Uuid,
    },

    /*
     * Ping related
     */
    Ruok,
    Imok,

    /*
     * Repair related
     * We use rep_id here (Repair ID) instead of job_id to be clear that
     * this is repair work and not actual IO.  The repair work uses a
     * different work queue  and each repair job must finish on all three
     * downstairs before the next one can be sent.
     */
    /// Send a close the given extent ID on the downstairs.
    ExtentClose {
        repair_id: u64,
        extent_id: usize,
    },

    /// Send a request to reopen the given extent.
    ExtentReopen {
        repair_id: u64,
        extent_id: usize,
    },

    /// Flush just this extent on just this downstairs client.
    ExtentFlush {
        repair_id: u64,
        extent_id: usize,
        client_id: u8,
        flush_number: u64,
        gen_number: u64,
    },

    /// Replace an extent with data from the given downstairs.
    ExtentRepair {
        repair_id: u64,
        extent_id: usize,
        source_client_id: u8,
        source_repair_address: SocketAddr,
        dest_clients: Vec<u8>,
    },

    /// The given repair job ID has finished without error
    RepairAckId {
        repair_id: u64,
    },

    /// A problem with the given extent
    ExtentError {
        repair_id: u64,
        extent_id: usize,
        error: CrucibleError,
    },

    /*
     * Metadata exchange
     */
    RegionInfoPlease,
    RegionInfo {
        region_def: RegionDefinition,
    },

    ExtentVersionsPlease,
    ExtentVersions {
        gen_numbers: Vec<u64>,
        flush_numbers: Vec<u64>,
        dirty_bits: Vec<bool>,
    },

    LastFlush {
        last_flush_number: u64,
    },
    LastFlushAck {
        last_flush_number: u64,
    },

    /*
     * IO related
     */
    Write {
        upstairs_id: Uuid,
        job_id: u64,
        dependencies: Vec<u64>,
        writes: Vec<Write>,
    },
    WriteAck {
        upstairs_id: Uuid,
        job_id: u64,
        result: Result<(), CrucibleError>,
    },

    Flush {
        upstairs_id: Uuid,
        job_id: u64,
        dependencies: Vec<u64>,
        flush_number: u64,
        gen_number: u64,
        snapshot_details: Option<SnapshotDetails>,
    },
    FlushAck {
        upstairs_id: Uuid,
        job_id: u64,
        result: Result<(), CrucibleError>,
    },

    ReadRequest {
        upstairs_id: Uuid,
        job_id: u64,
        dependencies: Vec<u64>,
        requests: Vec<ReadRequest>,
    },
    ReadResponse {
        upstairs_id: Uuid,
        job_id: u64,
        responses: Result<Vec<ReadResponse>, CrucibleError>,
    },

    WriteUnwritten {
        upstairs_id: Uuid,
        job_id: u64,
        dependencies: Vec<u64>,
        writes: Vec<Write>,
    },
    WriteUnwrittenAck {
        upstairs_id: Uuid,
        job_id: u64,
        result: Result<(), CrucibleError>,
    },

    /*
     * Misc
     */
    Unknown(u32, BytesMut),
}

#[derive(Debug)]
pub struct CrucibleEncoder {}

impl CrucibleEncoder {
    pub fn new() -> Self {
        CrucibleEncoder {}
    }

    fn serialized_size<T: serde::Serialize>(
        m: T,
    ) -> Result<usize, anyhow::Error> {
        let serialized_len: usize = bincode::serialized_size(&m)? as usize;
        let len = serialized_len + 4;

        Ok(len)
    }

    fn a_write(bs: usize) -> Write {
        Write {
            eid: 1,
            offset: Block::new(1, bs.trailing_zeros()),
            data: {
                let sz = bs;
                let mut data = Vec::with_capacity(sz);
                data.resize(sz, 1);
                bytes::Bytes::from(data)
            },
            encryption_context: Some(EncryptionContext {
                nonce: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                tag: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            }),
            hash: 0,
        }
    }

    /*
     * Binary search to find the maximum number of blocks we can send.
     *
     * Attempts at deterministically computing the number of blocks
     * repeatedly failed, so binary search instead. Note that this computes
     * the maximum size that this Tokio encoding / decoding supports
     * given our constant MAX_FRM_LEN.
     */
    pub fn max_io_blocks(bs: usize) -> Result<usize, anyhow::Error> {
        let size_of_write_message =
            CrucibleEncoder::serialized_size(CrucibleEncoder::a_write(bs))?;

        // Maximum frame length divided by a write of one block is the lower
        // bound.
        let lower_size_write_message = Message::Write {
            upstairs_id: Uuid::new_v4(),
            job_id: 1,
            dependencies: vec![1],
            writes: (0..(MAX_FRM_LEN / size_of_write_message))
                .map(|_| CrucibleEncoder::a_write(bs))
                .collect(),
        };

        assert!(
            CrucibleEncoder::serialized_size(&lower_size_write_message)?
                < MAX_FRM_LEN
        );

        // The upper bound is the maximum frame length divided by the block
        // size.
        let upper_size_write_message = Message::Write {
            upstairs_id: Uuid::new_v4(),
            job_id: 1,
            dependencies: vec![1],
            writes: (0..(MAX_FRM_LEN / bs))
                .map(|_| CrucibleEncoder::a_write(bs))
                .collect(),
        };

        assert!(
            CrucibleEncoder::serialized_size(&upper_size_write_message)?
                > MAX_FRM_LEN
        );

        // Binary search for the number of blocks that represents the largest IO
        // given MAX_FRM_LEN.

        let mut lower = match lower_size_write_message {
            Message::Write {
                upstairs_id: _,
                job_id: _,
                dependencies: _,
                writes,
            } => writes.len(),
            _ => {
                bail!("wat");
            }
        };

        let mut upper = match upper_size_write_message {
            Message::Write {
                upstairs_id: _,
                job_id: _,
                dependencies: _,
                writes,
            } => writes.len(),
            _ => {
                bail!("wat");
            }
        };

        let mut mid = (lower + upper) / 2;

        loop {
            if (mid + 1) == upper {
                return Ok(mid);
            }

            let mid_size_write_message = Message::Write {
                upstairs_id: Uuid::new_v4(),
                job_id: 1,
                dependencies: vec![1],
                writes: (0..mid)
                    .map(|_| CrucibleEncoder::a_write(bs))
                    .collect(),
            };

            let mid_size =
                CrucibleEncoder::serialized_size(&mid_size_write_message)?;

            match mid_size.cmp(&MAX_FRM_LEN) {
                Ordering::Greater => {
                    upper = mid;
                }
                Ordering::Equal => {
                    return Ok(mid);
                }
                Ordering::Less => {
                    lower = mid;
                }
            }

            mid = (lower + upper) / 2;
        }
    }
}

impl Default for CrucibleEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/*
 * A frame is [len | serialized message].
 */

impl Encoder<Message> for CrucibleEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        m: Message,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let len = CrucibleEncoder::serialized_size(&m)?;

        dst.reserve(len);
        dst.put_u32_le(len as u32);
        bincode::serialize_into(dst.writer(), &m)?;

        Ok(())
    }
}

impl Encoder<&Message> for CrucibleEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        m: &Message,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let len = CrucibleEncoder::serialized_size(&m)?;

        dst.reserve(len);
        dst.put_u32_le(len as u32);
        bincode::serialize_into(dst.writer(), &m)?;

        Ok(())
    }
}

pub struct CrucibleDecoder {}

impl CrucibleDecoder {
    pub fn new() -> Self {
        CrucibleDecoder {}
    }
}

impl Default for CrucibleDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for CrucibleDecoder {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            /*
             * Wait for the u32 length prefix.
             */
            return Ok(None);
        }

        /*
         * Get the length prefix from the frame.
         */
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[0..4]);
        let len = u32::from_le_bytes(length_bytes) as usize;

        if len > MAX_FRM_LEN {
            bail!("frame is {} bytes, more than maximum {}", len, MAX_FRM_LEN);
        }

        if src.len() < len {
            /*
             * Wait for an entire frame.  Expand the buffer to fit.
             */
            src.reserve(len);
            return Ok(None);
        }

        src.advance(4);

        let message = bincode::deserialize_from(src.reader());

        Ok(Some(message?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    fn round_trip(input: &Message) -> Result<Message> {
        let mut enc = CrucibleEncoder::new();
        let mut buf = BytesMut::new();
        enc.encode(input.clone(), &mut buf)?;

        let mut dec = CrucibleDecoder::new();
        let output = dec.decode(&mut buf)?;
        if let Some(output) = output {
            Ok(output)
        } else {
            bail!("expected message, got None");
        }
    }

    #[test]
    fn rt_here_i_am() -> Result<()> {
        let input = Message::HereIAm {
            version: 2,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 123,
        };
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_yes_its_me() -> Result<()> {
        let input = Message::YesItsMe { version: 20000 };
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_ruok() -> Result<()> {
        let input = Message::Ruok;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_imok() -> Result<()> {
        let input = Message::Imok;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_evp() -> Result<()> {
        let input = Message::ExtentVersionsPlease;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_ev_0() -> Result<()> {
        let input = Message::ExtentVersions {
            gen_numbers: vec![],
            flush_numbers: vec![],
            dirty_bits: vec![],
        };
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_ev_7() -> Result<()> {
        let input = Message::ExtentVersions {
            gen_numbers: vec![1, 2, 3, 4, u64::MAX, 1, 0],
            flush_numbers: vec![1, 2, 3, 4, u64::MAX, 1, 0],
            dirty_bits: vec![true, true, false, true, true, false, true],
        };
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn correctly_detect_truncated_message() -> Result<()> {
        let mut encoder = CrucibleEncoder::new();
        let mut decoder = CrucibleDecoder::new();

        let input = Message::HereIAm {
            version: 0,
            upstairs_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            gen: 23849183,
        };
        let mut buffer = BytesMut::new();

        encoder.encode(input, &mut buffer)?;

        buffer.truncate(buffer.len() - 1);

        let result = decoder.decode(&mut buffer);

        match result {
            Err(_) => {
                result?;
            }
            Ok(v) => {
                assert_eq!(v, None);
            }
        };

        Ok(())
    }
}
