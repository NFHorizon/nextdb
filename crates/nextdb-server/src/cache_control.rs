use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCacheProfile {
    pub version: u64,
    pub lease_ttl_ms: u64,
    #[serde(default = "default_max_objects")]
    pub max_objects: usize,
    #[serde(default = "default_max_object_bytes")]
    pub max_object_bytes: u64,
    pub max_room_messages: usize,
    #[serde(default = "default_max_user_events")]
    pub max_user_events: usize,
    pub max_records_per_table: usize,
    #[serde(default)]
    pub max_nested_partitions: usize,
    #[serde(default = "default_max_pending_writes")]
    pub max_pending_writes: usize,
    #[serde(default = "default_max_pending_write_bytes")]
    pub max_pending_write_bytes: u64,
    pub offline_writes: bool,
}

fn default_max_user_events() -> usize {
    10_000
}

fn default_max_objects() -> usize {
    10_000
}

fn default_max_object_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_max_pending_writes() -> usize {
    10_000
}

fn default_max_pending_write_bytes() -> u64 {
    512 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCacheInvalidationEntry {
    pub id: String,
    pub generation: u64,
    pub scope: ClientCacheInvalidationScope,
    pub key: Option<String>,
    pub table: Option<String>,
    pub parent_key: Option<String>,
    pub nested: Option<String>,
    pub min_valid_lsn: u64,
    pub reason: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientCacheInvalidationScope {
    All,
    Profile,
    Object,
    Room,
    User,
    Table,
    NestedTable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCacheControl {
    pub profile: ClientCacheProfile,
    pub invalidations: Vec<ClientCacheInvalidationEntry>,
}

impl ClientCacheControl {
    pub fn default_with_env() -> Self {
        Self {
            profile: ClientCacheProfile {
                version: parse_u64_env("NEXTDB_CLIENT_CACHE_PROFILE_VERSION").unwrap_or(1),
                lease_ttl_ms: parse_u64_env("NEXTDB_CLIENT_CACHE_LEASE_MS").unwrap_or(60_000),
                max_objects: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_OBJECTS").unwrap_or(10_000),
                max_object_bytes: parse_u64_env("NEXTDB_CLIENT_CACHE_MAX_OBJECT_BYTES")
                    .unwrap_or(512 * 1024 * 1024),
                max_room_messages: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_ROOM_MESSAGES")
                    .unwrap_or(10_000),
                max_user_events: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_USER_EVENTS")
                    .unwrap_or(10_000),
                max_records_per_table: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_RECORDS_PER_TABLE")
                    .unwrap_or(10_000),
                max_nested_partitions: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_NESTED_PARTITIONS")
                    .unwrap_or(0),
                max_pending_writes: parse_usize_env("NEXTDB_CLIENT_CACHE_MAX_PENDING_WRITES")
                    .unwrap_or(10_000),
                max_pending_write_bytes: parse_u64_env(
                    "NEXTDB_CLIENT_CACHE_MAX_PENDING_WRITE_BYTES",
                )
                .unwrap_or(512 * 1024 * 1024),
                offline_writes: parse_bool_env("NEXTDB_CLIENT_CACHE_OFFLINE_WRITES")
                    .unwrap_or(true),
            },
            invalidations: Vec::new(),
        }
    }

    pub fn next_generation(&self) -> u64 {
        self.invalidations
            .last()
            .map(|entry| entry.generation.saturating_add(1))
            .unwrap_or(1)
    }

    pub fn invalidations_after(&self, generation: u64) -> Vec<ClientCacheInvalidationEntry> {
        self.invalidations
            .iter()
            .filter(|entry| entry.generation > generation)
            .cloned()
            .collect()
    }
}

pub async fn load_cache_control(path: &PathBuf) -> Result<ClientCacheControl> {
    if !path.exists() {
        let control = ClientCacheControl::default_with_env();
        persist_cache_control(path, &control).await?;
        return Ok(control);
    }
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read client cache control at {}", path.display()))?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn persist_cache_control(path: &PathBuf, control: &ClientCacheControl) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(path, serde_json::to_vec_pretty(control)?).await?;
    Ok(())
}

fn parse_u64_env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn parse_usize_env(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

fn parse_bool_env(name: &str) -> Option<bool> {
    match std::env::var(name)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
