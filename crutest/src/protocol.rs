// Copyright 2022 Oxide Computer Company
use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use super::*;

const MAX_FRM_LEN: usize = 100 * 1024 * 1024; // 100M

use crucible_common::CrucibleError;

/// Messages sent between the CLI client and the CLI server.
/// Note that the server does the work, sends any write data,
/// checks any read data.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum CliMessage {
    Activate(u64),
    ActiveIs(bool),
    // Tell the cliserver to commit the current write log
    Commit,
    Deactivate,
    // Generic command success
    DoneOk,
    Error(CrucibleError),
    // Print the expected read count output for a block
    Expected(usize),
    ExpectedResponse(usize, Vec<u8>),
    // Record the current write count to the verify-out file.
    Export,
    // Run the fill test.
    Fill,
    Flush,
    Generic(usize),
    Info(u64, u64, u64),
    InfoPlease,
    IsActive,
    MyUuid(Uuid),
    Perf(usize, usize, usize, usize, usize),
    Read(usize, usize),
    RandRead,
    ReadResponse(usize, Result<Vec<u8>, CrucibleError>),
    RandWrite,
    // Show the work queues
    ShowWork,
    // Run the Verify test.
    Verify,
    Write(usize, usize),
    Unknown(u32, BytesMut),
    Uuid,
}

#[derive(Debug)]
pub struct CliEncoder {}

impl CliEncoder {
    pub fn new() -> Self {
        CliEncoder {}
    }

    fn serialized_size<T: serde::Serialize>(
        m: T,
    ) -> Result<usize, anyhow::Error> {
        let serialized_len: usize = bincode::serialized_size(&m)? as usize;
        let len = serialized_len + 4;

        Ok(len)
    }
}

impl Default for CliEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/*
 * A frame is [len | serialized message].
 */

impl Encoder<CliMessage> for CliEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        m: CliMessage,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let len = CliEncoder::serialized_size(&m)?;

        dst.reserve(len);
        dst.put_u32_le(len as u32);
        bincode::serialize_into(dst.writer(), &m)?;

        Ok(())
    }
}

impl Encoder<&CliMessage> for CliEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        m: &CliMessage,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let len = CliEncoder::serialized_size(&m)?;

        dst.reserve(len);
        dst.put_u32_le(len as u32);
        bincode::serialize_into(dst.writer(), &m)?;

        Ok(())
    }
}

pub struct CliDecoder {}

impl CliDecoder {
    pub fn new() -> Self {
        CliDecoder {}
    }
}

impl Default for CliDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for CliDecoder {
    type Item = CliMessage;
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

    fn round_trip(input: &CliMessage) -> Result<CliMessage> {
        let mut enc = CliEncoder::new();
        let mut buf = BytesMut::new();
        enc.encode(input.clone(), &mut buf)?;

        let mut dec = CliDecoder::new();
        let output = dec.decode(&mut buf)?;
        if let Some(output) = output {
            Ok(output)
        } else {
            bail!("expected message, got None");
        }
    }

    #[test]
    fn rt_uuid() -> Result<()> {
        let input = CliMessage::Uuid;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_my_uuid() -> Result<()> {
        let input = CliMessage::MyUuid(Uuid::new_v4());
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_info_please() -> Result<()> {
        let input = CliMessage::InfoPlease;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_info() -> Result<()> {
        let input = CliMessage::Info(1, 2, 99);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_activate() -> Result<()> {
        let input = CliMessage::Activate(99);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_commit() -> Result<()> {
        let input = CliMessage::Commit;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_deactivate() -> Result<()> {
        let input = CliMessage::Deactivate;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_done_ok() -> Result<()> {
        let input = CliMessage::DoneOk;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_is_active() -> Result<()> {
        let input = CliMessage::IsActive;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_read() -> Result<()> {
        let input = CliMessage::Read(32, 22);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_is_show() -> Result<()> {
        let input = CliMessage::ShowWork;
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_write() -> Result<()> {
        let input = CliMessage::Write(32, 22);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_generic() -> Result<()> {
        let input = CliMessage::Generic(2);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    #[test]
    fn rt_perf() -> Result<()> {
        let input = CliMessage::Perf(2, 3, 4, 2, 2);
        assert_eq!(input, round_trip(&input)?);
        Ok(())
    }

    // ZZZ tests for readresponse, Error
    #[test]
    fn correctly_detect_truncated_message() -> Result<()> {
        let mut encoder = CliEncoder::new();
        let mut decoder = CliDecoder::new();

        let input = CliMessage::MyUuid(Uuid::new_v4());
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
