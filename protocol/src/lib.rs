// Copyright 2021 Oxide Computer Company
use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

const MAX_FRM_LEN: usize = 1024 * 1024;

use crucible_common::Block;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Message {
    HereIAm(u32),
    YesItsMe(u32),
    Ruok,
    Imok,
    ExtentVersionsPlease,
    ExtentVersions(u64, u64, u32, Vec<u64>),
    Write(u64, u64, Vec<u64>, Block, bytes::Bytes),
    WriteAck(u64),
    Flush(u64, Vec<u64>, u64),
    FlushAck(u64),
    ReadRequest(u64, Vec<u64>, u64, Block, u64),
    ReadResponse(u64, bytes::Bytes),
    Unknown(u32, BytesMut),
}

pub struct CrucibleEncoder {}

impl CrucibleEncoder {
    pub fn new() -> Self {
        CrucibleEncoder {}
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
        let bytes = bincode::serialize(&m)?;

        let len = bytes.len() + 4;

        dst.reserve(len);
        dst.put_u32_le(len as u32);
        dst.extend_from_slice(&bytes);

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

        // src may contain more frames, so truncate based on this frame's length
        let message = bincode::deserialize(&src[0..(len - 4)]);

        // deserializing does not consume from src, make sure to do so here
        src.advance(len - 4);

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
        let input = Message::HereIAm(2);
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
        let input = Message::ExtentVersions(2, 4, 6, vec![]);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_ev_7() -> Result<()> {
        let input =
            Message::ExtentVersions(2, 4, 6, vec![1, 2, 3, 4, u64::MAX, 1, 0]);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }
}
