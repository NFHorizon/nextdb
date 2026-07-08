use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::actor::ActorSnapshot;

const SNAPSHOT_MAGIC: [u8; 4] = *b"NDBS";
const SNAPSHOT_VERSION_V1: u16 = 1;
const SNAPSHOT_VERSION: u16 = SNAPSHOT_VERSION_V1;
const SNAPSHOT_ENCODING_POSTCARD_ZSTD: u16 = 1;
const SNAPSHOT_HEADER_LEN: usize = 16;
const SNAPSHOT_ZSTD_LEVEL: i32 = 3;
const SNAPSHOT_MAX_PAYLOAD_BYTES: u32 = 256 * 1024 * 1024;

#[derive(Clone)]
pub struct SnapshotStore {
    path: PathBuf,
}

#[derive(Debug, Serialize)]
struct SnapshotEnvelope<'a> {
    lsn: u64,
    schema_version: u32,
    snapshot_json: &'a [u8],
}

#[derive(Debug, Deserialize)]
struct OwnedSnapshotEnvelope {
    lsn: u64,
    schema_version: u32,
    snapshot_json: Vec<u8>,
}

impl SnapshotStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub async fn load(&self) -> Result<Option<ActorSnapshot>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)
            .await
            .with_context(|| format!("read actor snapshot at {}", self.path.display()))?;
        Ok(Some(decode_snapshot(&bytes).with_context(|| {
            format!("decode actor snapshot at {}", self.path.display())
        })?))
    }

    pub async fn save(&self, snapshot: &ActorSnapshot) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp = self.path.with_extension("snapshot.tmp");
        fs::write(&tmp, encode_snapshot(snapshot)?).await?;
        fs::rename(tmp, &self.path).await?;
        Ok(())
    }
}

fn encode_snapshot(snapshot: &ActorSnapshot) -> Result<Vec<u8>> {
    let snapshot_json =
        serde_json::to_vec(snapshot).context("encode actor snapshot JSON payload")?;
    let envelope = SnapshotEnvelope {
        lsn: snapshot.lsn,
        schema_version: snapshot.schema_version,
        snapshot_json: &snapshot_json,
    };
    let postcard = postcard::to_allocvec(&envelope).context("encode actor snapshot as postcard")?;
    let compressed = zstd::bulk::compress(&postcard, SNAPSHOT_ZSTD_LEVEL)
        .context("compress actor snapshot with zstd")?;
    if compressed.len() > SNAPSHOT_MAX_PAYLOAD_BYTES as usize {
        anyhow::bail!(
            "actor snapshot exceeds max size: {} bytes > {} bytes",
            compressed.len(),
            SNAPSHOT_MAX_PAYLOAD_BYTES
        );
    }
    let mut encoded = Vec::with_capacity(SNAPSHOT_HEADER_LEN + compressed.len());
    encoded.extend_from_slice(&SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&SNAPSHOT_ENCODING_POSTCARD_ZSTD.to_be_bytes());
    encoded.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&crc32c::crc32c(&compressed).to_be_bytes());
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn decode_snapshot(bytes: &[u8]) -> Result<ActorSnapshot> {
    if !is_framed_snapshot(bytes) {
        return serde_json::from_slice(bytes).context("parse legacy JSON actor snapshot");
    }
    if bytes.len() < SNAPSHOT_HEADER_LEN {
        anyhow::bail!("truncated actor snapshot header");
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != SNAPSHOT_VERSION_V1 {
        anyhow::bail!("unsupported actor snapshot version {version}");
    }
    let encoding = u16::from_be_bytes([bytes[6], bytes[7]]);
    if encoding != SNAPSHOT_ENCODING_POSTCARD_ZSTD {
        anyhow::bail!("unsupported actor snapshot encoding {encoding}");
    }
    let len = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if len == 0 {
        anyhow::bail!("empty actor snapshot payload");
    }
    if len > SNAPSHOT_MAX_PAYLOAD_BYTES {
        anyhow::bail!(
            "actor snapshot exceeds max size: {len} bytes > {SNAPSHOT_MAX_PAYLOAD_BYTES} bytes"
        );
    }
    let start = SNAPSHOT_HEADER_LEN;
    let end = start + len as usize;
    if end != bytes.len() {
        anyhow::bail!("actor snapshot payload length mismatch");
    }
    let payload = &bytes[start..end];
    let expected = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let actual = crc32c::crc32c(payload);
    if actual != expected {
        anyhow::bail!(
            "actor snapshot CRC32C mismatch: expected {expected:08x}, found {actual:08x}"
        );
    }

    let postcard = zstd::bulk::decompress(payload, SNAPSHOT_MAX_PAYLOAD_BYTES as usize)
        .context("decompress zstd actor snapshot")?;
    let envelope: OwnedSnapshotEnvelope =
        postcard::from_bytes(&postcard).context("parse postcard actor snapshot envelope")?;
    let snapshot: ActorSnapshot =
        serde_json::from_slice(&envelope.snapshot_json).context("parse actor snapshot payload")?;
    if envelope.lsn != snapshot.lsn {
        anyhow::bail!(
            "actor snapshot envelope LSN {} does not match snapshot LSN {}",
            envelope.lsn,
            snapshot.lsn
        );
    }
    if envelope.schema_version != snapshot.schema_version {
        anyhow::bail!(
            "actor snapshot envelope schema version {} does not match snapshot schema version {}",
            envelope.schema_version,
            snapshot.schema_version
        );
    }
    Ok(snapshot)
}

fn is_framed_snapshot(bytes: &[u8]) -> bool {
    bytes.len() >= SNAPSHOT_MAGIC.len() && bytes[..SNAPSHOT_MAGIC.len()] == SNAPSHOT_MAGIC
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use uuid::Uuid;

    use super::*;
    use crate::{actor::RoomSnapshot, model::Message};

    fn snapshot_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("nextdb-{name}-{}.snapshot", Uuid::now_v7()))
    }

    fn actor_snapshot() -> ActorSnapshot {
        let mut rooms = HashMap::new();
        rooms.insert(
            "general".to_string(),
            RoomSnapshot {
                messages: vec![Message {
                    id: "m1".to_string(),
                    room_id: "general".to_string(),
                    sender_id: "u1".to_string(),
                    body: "hello".to_string(),
                    attachments: Vec::new(),
                    created_at_ms: 10,
                    lsn: 7,
                    path: "rooms/general/messages/m1".to_string(),
                }],
                last_accessed_ms: 11,
            },
        );
        ActorSnapshot {
            lsn: 7,
            schema_version: 3,
            record_hot: None,
            rooms,
            actor_states: Vec::new(),
        }
    }

    #[tokio::test]
    async fn save_writes_zstd_postcard_snapshot() {
        let path = snapshot_path("postcard");
        let store = SnapshotStore::new(path.clone());
        let snapshot = actor_snapshot();

        store.save(&snapshot).await.expect("save snapshot");

        let bytes = fs::read(&path).await.expect("read snapshot file");
        assert!(is_framed_snapshot(&bytes));
        assert_eq!(&bytes[..4], &SNAPSHOT_MAGIC);
        assert_eq!(
            u16::from_be_bytes([bytes[4], bytes[5]]),
            SNAPSHOT_VERSION_V1
        );
        assert_eq!(
            u16::from_be_bytes([bytes[6], bytes[7]]),
            SNAPSHOT_ENCODING_POSTCARD_ZSTD
        );
        assert!(serde_json::from_slice::<ActorSnapshot>(&bytes).is_err());

        let loaded = store.load().await.expect("load snapshot").unwrap();
        assert_eq!(loaded.lsn, snapshot.lsn);
        assert_eq!(loaded.schema_version, snapshot.schema_version);
        assert_eq!(loaded.rooms["general"].messages[0].body, "hello");

        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn load_accepts_legacy_json_snapshot() {
        let path = snapshot_path("legacy-json");
        let store = SnapshotStore::new(path.clone());
        let snapshot = actor_snapshot();
        fs::write(
            &path,
            serde_json::to_vec_pretty(&snapshot).expect("encode legacy snapshot"),
        )
        .await
        .expect("write legacy snapshot");

        let loaded = store.load().await.expect("load legacy snapshot").unwrap();

        assert_eq!(loaded.lsn, snapshot.lsn);
        assert_eq!(loaded.schema_version, snapshot.schema_version);
        assert_eq!(loaded.rooms["general"].last_accessed_ms, 11);

        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn load_rejects_snapshot_crc32c_mismatch() {
        let path = snapshot_path("crc");
        let store = SnapshotStore::new(path.clone());
        let snapshot = actor_snapshot();
        store.save(&snapshot).await.expect("save snapshot");

        let mut bytes = fs::read(&path).await.expect("read snapshot file");
        bytes[SNAPSHOT_HEADER_LEN] ^= 0x01;
        fs::write(&path, bytes)
            .await
            .expect("corrupt snapshot file");

        let err = store
            .load()
            .await
            .expect_err("CRC mismatch should reject snapshot");

        assert!(format!("{err:#}").contains("actor snapshot CRC32C mismatch"));

        let _ = fs::remove_file(path).await;
    }
}
