use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{self, Read, Write};

const MAX_MSG_SIZE: u32 = 16 * 1024 * 1024;

pub fn send_msg<W: Write, T: Serialize>(mut writer: W, msg: &T) -> io::Result<()> {
    let bytes = bincode::serialize(msg).map_err(io::Error::other)?;
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

    let mut buf = Vec::new();
    reader.take(len as u64).read_to_end(&mut buf)?;
    if buf.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "framed message body shorter than announced length",
        ));
    }

    bincode::deserialize(&buf).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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
