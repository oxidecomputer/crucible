use anyhow::bail;
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

const MAX_FRM_LEN: usize = 1024 * 1024;

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    HereIAm(u32),
    YesItsMe(u32),
    Ruok,
    Imok,
    Unknown(u32, BytesMut),
}

pub struct CrucibleEncoder {}

impl CrucibleEncoder {
    pub fn new() -> Self {
        CrucibleEncoder {}
    }
}

impl Encoder<Message> for CrucibleEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        m: Message,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match m {
            Message::Unknown(code, buf) => {
                if code == 0 {
                    bail!("message code must be non-zero");
                }

                let len = buf.len() + 2 * 4;
                dst.reserve(len);
                dst.put_u32_le(len as u32);
                dst.put_u32_le(code);
                dst.extend_from_slice(&buf);

                Ok(())
            }
            Message::HereIAm(version) => {
                let len = 3 * 4;
                dst.reserve(len);
                dst.put_u32_le(len as u32);
                dst.put_u32_le(1);
                dst.put_u32_le(version);

                Ok(())
            }
            Message::YesItsMe(version) => {
                let len = 3 * 4;
                dst.reserve(len);
                dst.put_u32_le(len as u32);
                dst.put_u32_le(2);
                dst.put_u32_le(version);

                Ok(())
            }
            Message::Ruok => {
                dst.reserve(8);
                dst.put_u32_le(8);
                dst.put_u32_le(3);

                Ok(())
            }
            Message::Imok => {
                dst.reserve(8);
                dst.put_u32_le(8);
                dst.put_u32_le(4);

                Ok(())
            }
        }
    }
}

pub struct CrucibleDecoder {}

impl CrucibleDecoder {
    pub fn new() -> Self {
        CrucibleDecoder {}
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
        let bodylen = len - 8;

        if src.len() < len {
            /*
             * Wait for an entire frame.  Expand the buffer to fit.
             */
            src.reserve(len);
            return Ok(None);
        }
        src.advance(4);

        let chklen = |n: usize| -> Result<(), Self::Error> {
            if *&bodylen != len - 8 {
                bail!("message length {}, expected {} bytes", bodylen, n);
            }
            Ok(())
        };

        Ok(match src.get_u32_le() {
            0 => bail!("message code must be non-zero"),
            1 => {
                chklen(4)?;

                let version = src.get_u32_le();
                Some(Message::HereIAm(version))
            }
            2 => {
                chklen(4)?;

                let version = src.get_u32_le();
                Some(Message::YesItsMe(version))
            }
            3 => {
                chklen(0)?;
                Some(Message::Ruok)
            }
            4 => {
                chklen(0)?;
                Some(Message::Imok)
            }
            code => {
                let buf = src.split_to(len - 2 * 4);
                Some(Message::Unknown(code, buf))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CrucibleDecoder, CrucibleEncoder, Message};
    use anyhow::{bail, Result};
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn basic_encode() -> Result<()> {
        let expect: Vec<u8> = vec![
            0x0B, 0x00, 0x00, 0x00, /* length */
            0x99, 0x00, 0x00, 0x00, /* code */
            0xAA, 0xBB, 0xCC, /* data */
        ];
        let mut data = BytesMut::new();
        data.resize(expect.len() - 8, 0);
        data.copy_from_slice(&expect[8..]);
        let m = Message::Unknown(153, data);

        let mut buf = BytesMut::new();

        let mut enc = CrucibleEncoder::new();
        enc.encode(m, &mut buf)?;

        assert_eq!(buf, expect);
        Ok(())
    }

    #[test]
    fn basic_decode() -> Result<()> {
        let expect: Vec<u8> = vec![
            0x0B, 0x00, 0x00, 0x00, /* length */
            0x99, 0x00, 0x00, 0x00, /* code */
            0xAA, 0xBB, 0xCC, /* data */
        ];

        let mut data = BytesMut::new();
        data.resize(expect.len() - 8, 0);
        data.copy_from_slice(&expect[8..]);
        let m = Message::Unknown(153, data);

        let mut buf = BytesMut::new();
        buf.resize(expect.len(), 0);
        buf.copy_from_slice(expect.as_slice());

        let mut dec = CrucibleDecoder::new();
        let res = dec.decode(&mut buf)?;

        assert_eq!(Some(m), res);
        Ok(())
    }

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
}
