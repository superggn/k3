use std::io::{Read, Write};

use crate::{CommandRequest, CommandResponse, KvError};
use bytes::{Buf, BufMut, BytesMut};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

pub const LEN_LEN: usize = 4;
pub const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
pub const COMPRESSION_LIMIT: usize = 1436;
// highest bit of 4 bytes
const COMPRESSION_BIT: usize = 1 << 31;

pub trait FrameCodec
where
    Self: Message + Sized + Default + Send + Sync,
{
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let raw_bit_size = self.encoded_len();
        if raw_bit_size > MAX_FRAME {
            return Err(KvError::FrameError);
        }
        buf.put_u32(raw_bit_size as _);
        if raw_bit_size > COMPRESSION_LIMIT {
            let mut encoded_buf = Vec::with_capacity(raw_bit_size);
            self.encode(&mut encoded_buf)?;
            // buf: [place for length_info] + [place for payload]
            let payload = buf.split_off(LEN_LEN);
            buf.clear(); // buf => [vacant]
            // encoder => data writer
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&encoded_buf[..])?; // buf1 => [encoded_payload]
            let compressed = encoder.finish()?.into_inner();
            debug!(
                "encode a frame: size {}({})",
                raw_bit_size,
                compressed.len()
            );
            buf.put_u32((compressed.len() | COMPRESSION_BIT) as _);
            buf.unsplit(compressed);
            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("Got a frame: msg len {}, compressed {}", len, compressed);
        if compressed {
            // decoder => data reader
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut unzipped = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut unzipped)?;
            buf.advance(len);
            Ok(Self::decode(&unzipped[..unzipped.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

impl FrameCodec for CommandRequest {}
impl FrameCodec for CommandResponse {}

pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Send + Unpin,
{
    let header = stream.read_u32().await? as usize;
    let (len, _compressed) = decode_header(header);
    buf.reserve(LEN_LEN + len); // init buf len
    buf.put_u32(header as _); // insert len info => buf
    unsafe { buf.advance_mut(len) };
    stream.read_exact(&mut buf[LEN_LEN..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use crate::utils::DummyStream;
    use bytes::Bytes;

    #[test]
    fn command_req_codec_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        assert!(!is_compressed(&buf));
        let cmd_1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd_1);
    }

    #[test]
    fn command_resp_codec_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = 1.into();
        let resp: CommandResponse = value.into();
        resp.encode_frame(&mut buf).unwrap();
        assert!(!is_compressed(&buf));
        let resp_1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(resp, resp_1);
    }

    #[test]
    fn command_resp_compression_codec_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let resp: CommandResponse = value.into();
        resp.encode_frame(&mut buf).unwrap();
        assert!(is_compressed(&buf));
        let resp_1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(resp, resp_1);
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };
        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();
        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1);
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let [v] = data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}
