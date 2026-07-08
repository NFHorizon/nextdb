use std::{collections::BTreeMap, path::PathBuf};

use serde::Serialize;

use crate::schema::DatabaseSchema;
use crate::wal::{self, WalRemoteAckPolicy};

const DEFAULT_HOT_WINDOW: usize = 5_000;
const DEFAULT_MAX_HOT_ROOMS: usize = 10_000;
pub(crate) const DEFAULT_CHECKPOINT_EVERY_LSN: u64 = 1_000;
pub(crate) const DEFAULT_WAL_BATCH_MAX: usize = wal::DEFAULT_WAL_BATCH_MAX;
pub(crate) const DEFAULT_WAL_BATCH_WAIT_MS: u64 = wal::DEFAULT_WAL_BATCH_WAIT_MS;
pub(crate) const DEFAULT_RECORD_HOT_PREWARM_LIMIT: usize = 0;
pub(crate) const DEFAULT_OBJECT_GC_GRACE_MS: u64 = 86_400_000;
pub(crate) const DEFAULT_WAL_SHARD_COUNT: usize = 1;
pub(crate) const MAX_WAL_SHARDS: usize = 256;
pub(crate) const DEFAULT_REALTIME_EVENT_BATCH_MAX: usize = 128;
pub(crate) const DEFAULT_AUTO_COMPACT_WAL: bool = false;
pub(crate) const DEFAULT_TOPOLOGY_LEASE_MS: u64 = 30_000;
pub(crate) const DEFAULT_RESTART_WRITE_WAIT_MS: u64 = 10_000;
pub(crate) const MAX_BATCH_MESSAGES: usize = 10_000;
pub(crate) const MAX_RECORD_TRANSACTION_OPERATIONS: usize = 500;
pub(crate) const MAX_RECORD_BATCH_OPERATIONS: usize = 5_000;
pub(crate) const EXPORT_BUNDLE_ARCHIVE_CONTENT_TYPE: &str =
    "application/vnd.nextdb.export-bundle-archive+json";
pub(crate) const EXPORT_BUNDLE_ARCHIVE_FORMAT: &str = "nextdb.export-bundle-archive.v1";
const DEFAULT_MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_MESSAGE_BYTES: u64 = 64 * 1024;
const DEFAULT_MAX_USER_EVENT_BYTES: u64 = 1024 * 1024;
const DEFAULT_MAX_RECORD_VALUE_BYTES: u64 = 1024 * 1024;
const DEFAULT_MAX_LIVE_QUERY_RESULT_ROWS: usize = 250;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeLimits {
    pub(crate) max_object_bytes: u64,
    pub(crate) max_message_bytes: u64,
    pub(crate) max_user_event_bytes: u64,
    pub(crate) max_record_value_bytes: u64,
    pub(crate) max_live_queries_per_connection: usize,
    pub(crate) max_live_queries_per_table_per_connection: usize,
    pub(crate) max_live_queries_per_user: usize,
    pub(crate) max_live_query_result_rows: usize,
}

impl RuntimeLimits {
    pub(crate) fn from_env() -> Self {
        Self {
            max_object_bytes: env_u64("NEXTDB_MAX_OBJECT_BYTES", DEFAULT_MAX_OBJECT_BYTES),
            max_message_bytes: env_u64("NEXTDB_MAX_MESSAGE_BYTES", DEFAULT_MAX_MESSAGE_BYTES),
            max_user_event_bytes: env_u64(
                "NEXTDB_MAX_USER_EVENT_BYTES",
                DEFAULT_MAX_USER_EVENT_BYTES,
            ),
            max_record_value_bytes: env_u64(
                "NEXTDB_MAX_RECORD_VALUE_BYTES",
                DEFAULT_MAX_RECORD_VALUE_BYTES,
            ),
            max_live_queries_per_connection: env_usize("NEXTDB_MAX_LIVE_QUERIES_PER_CONNECTION", 0),
            max_live_queries_per_table_per_connection: env_usize(
                "NEXTDB_MAX_LIVE_QUERIES_PER_TABLE_PER_CONNECTION",
                0,
            ),
            max_live_queries_per_user: env_usize("NEXTDB_MAX_LIVE_QUERIES_PER_USER", 0),
            max_live_query_result_rows: env_usize(
                "NEXTDB_MAX_LIVE_QUERY_RESULT_ROWS",
                DEFAULT_MAX_LIVE_QUERY_RESULT_ROWS,
            ),
        }
    }
}

pub(crate) fn read_secret_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn effective_actor_runtime_config(schema: &DatabaseSchema) -> (usize, usize, u64) {
    let hot_window = std::env::var("NEXTDB_HOT_WINDOW")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .or_else(|| schema.message_live_window())
        .unwrap_or(DEFAULT_HOT_WINDOW);
    let max_hot_rooms = std::env::var("NEXTDB_MAX_HOT_ROOMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .or_else(|| schema.room_lru_max_items())
        .unwrap_or(DEFAULT_MAX_HOT_ROOMS);
    let hot_room_idle_ttl_ms = env_u64("NEXTDB_HOT_ROOM_IDLE_TTL_MS", 0);
    (hot_window, max_hot_rooms, hot_room_idle_ttl_ms)
}

pub(crate) fn parse_user_token_env(name: &str) -> BTreeMap<String, String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .filter_map(|entry| {
            let (user_id, token) = entry.split_once('=')?;
            let user_id = user_id.trim();
            let token = token.trim();
            if user_id.is_empty() || token.is_empty() {
                return None;
            }
            Some((user_id.to_string(), token.to_string()))
        })
        .collect()
}

pub(crate) fn parse_bool_env(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

pub(crate) fn parse_path_list_env(name: &str) -> Vec<PathBuf> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .collect()
}

pub(crate) fn parse_wal_remote_ack_policy() -> WalRemoteAckPolicy {
    let value = std::env::var("NEXTDB_WAL_REMOTE_ACKS")
        .unwrap_or_else(|_| "all".to_string())
        .trim()
        .to_ascii_lowercase();
    match value.as_str() {
        "all" => WalRemoteAckPolicy::All,
        "quorum" | "majority" => WalRemoteAckPolicy::Quorum,
        "none" | "async" | "0" => WalRemoteAckPolicy::None,
        value => value
            .parse::<usize>()
            .map(WalRemoteAckPolicy::Count)
            .unwrap_or(WalRemoteAckPolicy::All),
    }
}

pub(crate) fn parse_url_list_value(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(crate) fn env_bool(name: &str, default: bool) -> bool {
    parse_bool_env(name).unwrap_or(default)
}

pub(crate) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
