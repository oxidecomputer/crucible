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
     * Initial negotiation: version, upstairs uuid.
     */
    HereIAm(u32, Uuid),
    YesItsMe(u32),

    /*
     * Forcefully tell this downstairs to promote us (an Upstairs) to
     * active.
     *
     * Kick out the old Upstairs.
     */
    PromoteToActive(Uuid),
    YouAreNowActive(Uuid),
    YouAreNoLongerActive(Uuid), // UUID of new active Upstairs

    /*
     * If downstairs sees a UUID that doesn't match what was negotiated, it
     * will send this message.
     */
    UuidMismatch(Uuid),

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
    /// We send the downstairs the repair ID (rep_id) and the extent number.
    ExtentClose(u64, usize),
    /// Send a request (with rep_id) to reopen the given extent.
    ExtentReopen(u64, usize),
    /// Ack the Re-Open of an extent from the downstairs using the rep_id.

    /// Flush just this extent on just this downstairs client.
    /// rep_id, extent ID, downstairs client ID, flush number, gen number.
    ExtentFlush(u64, usize, u8, u64, u64),
    /// Replace an extent with data from the given downstairs.
    /// rep_id, extent ID, source extent, Vec of extents to repair
    ExtentRepair(u64, usize, u8, SocketAddr, Vec<u8>),

    /// The given repair job ID has finished without error
    RepairAckId(u64),
    /// A problem with the given extent
    ExtentError(u64, usize, CrucibleError),
    /*
     * Metadata exchange
     */
    RegionInfoPlease,
    RegionInfo(RegionDefinition),
    ExtentVersionsPlease,
    LastFlush(u64),
    LastFlushAck(u64),
    ExtentVersions(Vec<u64>, Vec<u64>, Vec<bool>),

    /*
     * Write: Uuid, job id, dependencies, [Write]
     * WriteAck: Uuid, job id, result
     */
    Write(Uuid, u64, Vec<u64>, Vec<Write>),
    WriteAck(Uuid, u64, Result<(), CrucibleError>),

    /*
     * Flush: Uuid, job id, dependencies, flush number generation number,
     *        and optional snapshot details
     * FlushAck: Uuid, job id, result
     */
    Flush(Uuid, u64, Vec<u64>, u64, u64, Option<SnapshotDetails>),
    FlushAck(Uuid, u64, Result<(), CrucibleError>),

    /*
     * ReadRequest: Uuid, job id, dependencies, [ReadRequest]
     * ReadResponse: Uuid, job id, Result<[ReadRequest]>
     */
    ReadRequest(Uuid, u64, Vec<u64>, Vec<ReadRequest>),
    ReadResponse(Uuid, u64, Result<Vec<ReadResponse>, CrucibleError>),

    // ReadWithFill: Uuid, job id, dependencies, [ReadRequest], [Write]
    ReadFill(Uuid, u64, Vec<u64>, Vec<Write>),
    ReadFillAck(Uuid, u64, Result<(), CrucibleError>),

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
        let lower_size_write_message = Message::Write(
            Uuid::new_v4(),
            1,
            vec![1],
            (0..(MAX_FRM_LEN / size_of_write_message))
                .map(|_| CrucibleEncoder::a_write(bs))
                .collect(),
        );

        assert!(
            CrucibleEncoder::serialized_size(&lower_size_write_message)?
                < MAX_FRM_LEN
        );

        // The upper bound is the maximum frame length divided by the block
        // size.
        let upper_size_write_message = Message::Write(
            Uuid::new_v4(),
            1,
            vec![1],
            (0..(MAX_FRM_LEN / bs))
                .map(|_| CrucibleEncoder::a_write(bs))
                .collect(),
        );

        assert!(
            CrucibleEncoder::serialized_size(&upper_size_write_message)?
                > MAX_FRM_LEN
        );

        // Binary search for the number of blocks that represents the largest IO
        // given MAX_FRM_LEN.

        let mut lower = match lower_size_write_message {
            Message::Write(_, _, _, vec) => vec.len(),
            _ => {
                bail!("wat");
            }
        };

        let mut upper = match upper_size_write_message {
            Message::Write(_, _, _, vec) => vec.len(),
            _ => {
                bail!("wat");
            }
        };

        let mut mid = (lower + upper) / 2;

        loop {
            if (mid + 1) == upper {
                return Ok(mid);
            }

            let mid_size_write_message = Message::Write(
                Uuid::new_v4(),
                1,
                vec![1],
                (0..mid).map(|_| CrucibleEncoder::a_write(bs)).collect(),
            );

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
        let input = Message::HereIAm(2, Uuid::new_v4());
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_yes_its_me() -> Result<()> {
        let input = Message::YesItsMe(20000);
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
        let input = Message::ExtentVersions(vec![], vec![], vec![]);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_ev_7() -> Result<()> {
        let input = Message::ExtentVersions(
            vec![1, 2, 3, 4, u64::MAX, 1, 0],
            vec![1, 2, 3, 4, u64::MAX, 1, 0],
            vec![true, true, false, true, true, false, true],
        );
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn correctly_detect_truncated_message() -> Result<()> {
        let mut encoder = CrucibleEncoder::new();
        let mut decoder = CrucibleDecoder::new();

        let input = Message::HereIAm(0, Uuid::new_v4());
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
