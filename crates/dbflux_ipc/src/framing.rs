use serde::de::DeserializeOwned;
use serde::Serialize;
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

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;

    bincode::deserialize(&buf).map_err(io::Error::other)
}
