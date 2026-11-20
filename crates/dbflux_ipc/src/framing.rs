use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{self, Read, Write};

const MAX_MSG_SIZE: u32 = 16 * 1024 * 1024;

pub fn send_msg<W: Write, T: Serialize>(mut writer: W, msg: &T) -> io::Result<()> {
    let bytes = postcard::to_allocvec(msg).map_err(io::Error::other)?;
    let len = bytes.len() as u32;

    if len > MAX_MSG_SIZE {
        return Err(io::Error::other("message too large"));
    }

    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn recv_msg<R: Read, T: DeserializeOwned>(mut reader: R) -> io::Result<T> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    if len > MAX_MSG_SIZE as usize {
        return Err(io::Error::other("message too large"));
    }

    let mut buf = Vec::with_capacity(len.min(64 * 1024));
    reader.take(len as u64).read_to_end(&mut buf)?;
    if buf.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "framed message body shorter than announced length",
        ));
    }

    postcard::from_bytes(&buf).map_err(io::Error::other)
}

/// Closed app-control telemetry messages need much less than the generic IPC cap.
#[cfg(feature = "telemetry")]
pub const TELEMETRY_FRAME_CAP: usize = 4096;

#[cfg(feature = "telemetry")]
pub async fn send_telemetry_msg<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    message: &T,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    // A fixed serialization buffer also bounds outgoing allocation.
    let mut buffer = [0; TELEMETRY_FRAME_CAP];
    let bytes = postcard::to_slice(message, &mut buffer)
        .map_err(|_| io::Error::other("invalid telemetry frame"))?;
    writer
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

#[cfg(feature = "telemetry")]
pub async fn recv_telemetry_msg<R: tokio::io::AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
) -> io::Result<T> {
    use tokio::io::AsyncReadExt;
    let mut header = [0; 4];
    reader.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header) as usize;
    if length > TELEMETRY_FRAME_CAP {
        return Err(io::Error::other("telemetry frame too large"));
    }
    let mut buffer = [0; TELEMETRY_FRAME_CAP];
    let frame = buffer
        .get_mut(..length)
        .ok_or_else(|| io::Error::other("telemetry frame too large"))?;
    reader.read_exact(frame).await?;
    postcard::from_bytes(frame).map_err(|_| io::Error::other("invalid telemetry frame"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[cfg(feature = "telemetry")]
    #[test]
    fn bounded_async_framing_roundtrip_and_rejections() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut encoded = Vec::new();
            send_telemetry_msg(&mut encoded, &42u8).await.unwrap();
            assert_eq!(encoded, vec![1, 0, 0, 0, 42]);
            assert_eq!(
                recv_telemetry_msg::<_, u8>(&mut encoded.as_slice())
                    .await
                    .unwrap(),
                42
            );
            for bytes in [vec![1, 16, 0, 0], vec![1, 0, 0, 0], vec![1, 0, 0, 0, 255]] {
                assert!(
                    recv_telemetry_msg::<_, bool>(&mut bytes.as_slice())
                        .await
                        .is_err()
                );
            }
            let oversized = vec![0u8; TELEMETRY_FRAME_CAP];
            assert!(
                send_telemetry_msg(&mut Vec::new(), &oversized)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn recv_msg_short_body_returns_unexpected_eof() {
        let announced: u32 = 1024;
        let mut framed = Vec::new();
        framed.extend_from_slice(&announced.to_le_bytes());
        framed.extend_from_slice(&[0u8; 10]);

        let err = recv_msg::<_, Vec<u8>>(Cursor::new(framed)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn send_recv_roundtrip_near_cap() {
        let payload: Vec<u8> = vec![0xABu8; 4 * 1024 * 1024];
        let mut buf = Vec::new();
        send_msg(&mut buf, &payload).unwrap();
        let decoded: Vec<u8> = recv_msg(Cursor::new(buf)).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn recv_msg_rejects_oversized_header() {
        let oversized_len: u32 = MAX_MSG_SIZE + 1;
        let mut framed = Vec::new();
        framed.extend_from_slice(&oversized_len.to_le_bytes());

        let result = recv_msg::<_, Vec<u8>>(Cursor::new(framed));
        assert!(result.is_err());
    }
}
