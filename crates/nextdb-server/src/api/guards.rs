use crate::{AppState, api::error::ApiError};

pub(crate) fn ensure_bytes_limit(label: &str, actual: u64, limit: u64) -> Result<(), ApiError> {
    if limit > 0 && actual > limit {
        return Err(ApiError::payload_too_large(format!(
            "{label} exceeds limit: {actual} bytes > {limit} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn ensure_json_value_limit(
    label: &str,
    value: &serde_json::Value,
    limit: u64,
) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec(value).map_err(|err| ApiError::internal(err.into()))?;
    ensure_bytes_limit(label, bytes.len() as u64, limit)
}

pub(crate) fn ensure_shard_index(state: &AppState, shard: usize) -> Result<(), ApiError> {
    if shard >= state.wal_shards.len() {
        return Err(ApiError::bad_request("shard is out of range"));
    }
    Ok(())
}
