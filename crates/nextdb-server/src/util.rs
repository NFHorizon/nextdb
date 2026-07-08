use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn normalize_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).clamp(1, 500)
}

pub fn time_bucket_day(timestamp_ms: u64) -> u64 {
    timestamp_ms / 86_400_000
}

pub fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex value");
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex digit"),
    }
}

pub(crate) fn shard_index(key: &str, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    let digest = Sha256::digest(key.as_bytes());
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(first) as usize) % shard_count
}

pub(crate) struct Sha256Writer<'a, H: Digest> {
    pub(crate) hasher: &'a mut H,
}

impl<H: Digest> io::Write for Sha256Writer<'_, H> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Digest::update(self.hasher, buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
