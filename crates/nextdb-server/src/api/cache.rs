use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

use crate::{
    AppState,
    api::error::ApiError,
    api::records::{nested_record_table, validate_nested_table_path},
    cache_control::{
        ClientCacheControl, ClientCacheInvalidationEntry, ClientCacheInvalidationScope,
        ClientCacheProfile, persist_cache_control,
    },
    object_store::ensure_safe_object_id,
    util::now_ms,
};
use uuid::Uuid;

use axum::{
    Json,
    extract::{Query, State},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheProfileQuery {
    pub(crate) client_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) after_invalidation_generation: Option<u64>,
    pub(crate) schema_version: Option<u32>,
    pub(crate) cursor_lsn: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheLease {
    pub(crate) client_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) profile_version: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheProfileResponse {
    pub(crate) runtime_id: String,
    pub(crate) profile: ClientCacheProfile,
    pub(crate) lease: ClientCacheLease,
    pub(crate) invalidations: Vec<ClientCacheInvalidationEntry>,
    pub(crate) current_lsn: u64,
    pub(crate) schema_version: u32,
    pub(crate) reset_required: bool,
}

pub(crate) async fn get_cache_profile(
    State(state): State<AppState>,
    Query(query): Query<ClientCacheProfileQuery>,
) -> Json<ClientCacheProfileResponse> {
    let issued_at_ms = now_ms();
    let control = state.cache_control.read().await.clone();
    let client_id = query
        .client_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "anonymous".to_string());
    let invalidations =
        control.invalidations_after(query.after_invalidation_generation.unwrap_or(0));
    let schema_version = state.schema.version();
    let reset_required = query
        .schema_version
        .is_some_and(|client_schema_version| client_schema_version != schema_version)
        || query
            .cursor_lsn
            .is_some_and(|cursor_lsn| cursor_lsn > state.current_lsn.load(Ordering::Relaxed));

    Json(ClientCacheProfileResponse {
        runtime_id: state.runtime_id.clone(),
        lease: ClientCacheLease {
            client_id,
            session_id: query.session_id,
            issued_at_ms,
            expires_at_ms: issued_at_ms.saturating_add(control.profile.lease_ttl_ms),
            profile_version: control.profile.version,
        },
        profile: control.profile,
        invalidations,
        current_lsn: state.current_lsn.load(Ordering::Relaxed),
        schema_version,
        reset_required,
    })
}

pub(crate) async fn invalidate_cache(
    State(state): State<AppState>,
    Json(request): Json<ClientCacheInvalidateRequest>,
) -> Result<Json<ClientCacheInvalidateResponse>, ApiError> {
    let key = request
        .key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty());
    let table = request
        .table
        .as_deref()
        .map(str::trim)
        .filter(|table| !table.is_empty());
    let parent_key = request
        .parent_key
        .as_deref()
        .map(str::trim)
        .filter(|parent_key| !parent_key.is_empty());
    let nested = request
        .nested
        .as_deref()
        .map(str::trim)
        .filter(|nested| !nested.is_empty());

    match request.scope {
        ClientCacheInvalidationScope::All | ClientCacheInvalidationScope::Profile => {}
        ClientCacheInvalidationScope::Object
        | ClientCacheInvalidationScope::Room
        | ClientCacheInvalidationScope::User
        | ClientCacheInvalidationScope::Table => {
            if key.is_none() {
                return Err(ApiError::bad_request(
                    "cache invalidation key is required for object, room, user, and table scope",
                ));
            }
        }
        ClientCacheInvalidationScope::NestedTable => {
            let Some(table) = table else {
                return Err(ApiError::bad_request(
                    "cache invalidation table is required for nestedTable scope",
                ));
            };
            let Some(parent_key) = parent_key else {
                return Err(ApiError::bad_request(
                    "cache invalidation parentKey is required for nestedTable scope",
                ));
            };
            let Some(nested) = nested else {
                return Err(ApiError::bad_request(
                    "cache invalidation nested is required for nestedTable scope",
                ));
            };
            validate_nested_table_path(table, parent_key, nested, &state)?;
        }
    }
    if request.scope == ClientCacheInvalidationScope::Object
        && request
            .key
            .as_deref()
            .is_some_and(|key| !ensure_safe_object_id(key.trim()))
    {
        return Err(ApiError::bad_request(
            "invalid object cache invalidation key",
        ));
    }

    let (entry, control) = append_cache_invalidation(
        &state,
        request.scope,
        if request.scope == ClientCacheInvalidationScope::NestedTable {
            Some(format!(
                "{}:{}",
                nested_record_table(table.unwrap_or_default(), nested.unwrap_or_default()),
                parent_key.unwrap_or_default()
            ))
        } else {
            key.map(str::to_string)
        },
        table.map(str::to_string),
        parent_key.map(str::to_string),
        nested.map(str::to_string),
        request.min_valid_lsn.unwrap_or(0),
        request
            .reason
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "operator invalidation".to_string()),
    )
    .await?;
    Ok(Json(ClientCacheInvalidateResponse { entry, control }))
}

pub(crate) async fn update_cache_profile(
    State(state): State<AppState>,
    Json(request): Json<ClientCacheProfileUpdateRequest>,
) -> Result<Json<ClientCacheProfileUpdateResponse>, ApiError> {
    if request
        .lease_ttl_ms
        .is_some_and(|lease_ttl_ms| lease_ttl_ms == 0)
    {
        return Err(ApiError::bad_request(
            "cache profile leaseTtlMs must be greater than 0",
        ));
    }

    let mut changed = false;
    let profile = {
        let mut control = state.cache_control.write().await;
        if let Some(expected_version) = request.expected_version
            && expected_version != control.profile.version
        {
            return Err(ApiError::conflict_with_details(
                format!(
                    "cache profile version conflict: expected {}, got {}",
                    expected_version, control.profile.version
                ),
                serde_json::json!({
                    "cacheProfileVersionConflict": true,
                    "expectedVersion": expected_version,
                    "activeVersion": control.profile.version,
                }),
            ));
        }

        if let Some(value) = request.lease_ttl_ms {
            changed |= control.profile.lease_ttl_ms != value;
            control.profile.lease_ttl_ms = value;
        }
        if let Some(value) = request.max_objects {
            changed |= control.profile.max_objects != value;
            control.profile.max_objects = value;
        }
        if let Some(value) = request.max_object_bytes {
            changed |= control.profile.max_object_bytes != value;
            control.profile.max_object_bytes = value;
        }
        if let Some(value) = request.max_room_messages {
            changed |= control.profile.max_room_messages != value;
            control.profile.max_room_messages = value;
        }
        if let Some(value) = request.max_user_events {
            changed |= control.profile.max_user_events != value;
            control.profile.max_user_events = value;
        }
        if let Some(value) = request.max_records_per_table {
            changed |= control.profile.max_records_per_table != value;
            control.profile.max_records_per_table = value;
        }
        if let Some(value) = request.max_nested_partitions {
            changed |= control.profile.max_nested_partitions != value;
            control.profile.max_nested_partitions = value;
        }
        if let Some(value) = request.max_pending_writes {
            changed |= control.profile.max_pending_writes != value;
            control.profile.max_pending_writes = value;
        }
        if let Some(value) = request.max_pending_write_bytes {
            changed |= control.profile.max_pending_write_bytes != value;
            control.profile.max_pending_write_bytes = value;
        }
        if let Some(value) = request.offline_writes {
            changed |= control.profile.offline_writes != value;
            control.profile.offline_writes = value;
        }

        if !changed {
            return Err(ApiError::bad_request(
                "cache profile update did not change any field",
            ));
        }
        control.profile.version = control.profile.version.saturating_add(1);
        persist_cache_control(&state.cache_control_path, &control)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        control.profile.clone()
    };

    let (invalidation, _) = append_cache_invalidation(
        &state,
        ClientCacheInvalidationScope::Profile,
        None,
        None,
        None,
        None,
        state.current_lsn.load(Ordering::Relaxed),
        request
            .reason
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "cache profile updated".to_string()),
    )
    .await?;
    Ok(Json(ClientCacheProfileUpdateResponse {
        profile,
        invalidation,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_cache_invalidation(
    state: &AppState,
    scope: ClientCacheInvalidationScope,
    key: Option<String>,
    table: Option<String>,
    parent_key: Option<String>,
    nested: Option<String>,
    min_valid_lsn: u64,
    reason: String,
) -> Result<(ClientCacheInvalidationEntry, ClientCacheControl), ApiError> {
    let mut control = state.cache_control.write().await;
    let generation = control.next_generation();
    let entry = ClientCacheInvalidationEntry {
        id: Uuid::now_v7().to_string(),
        generation,
        scope,
        key,
        table,
        parent_key,
        nested,
        min_valid_lsn,
        reason,
        created_at_ms: now_ms(),
    };
    control.invalidations.push(entry.clone());
    if control.invalidations.len() > 1_000 {
        let drop_count = control.invalidations.len() - 1_000;
        control.invalidations.drain(0..drop_count);
    }
    persist_cache_control(&state.cache_control_path, &control)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let _ = state.cache_invalidations.send(entry.clone());
    Ok((entry, control.clone()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheInvalidateRequest {
    pub(crate) scope: ClientCacheInvalidationScope,
    pub(crate) key: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) min_valid_lsn: Option<u64>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheProfileUpdateRequest {
    pub(crate) expected_version: Option<u64>,
    pub(crate) lease_ttl_ms: Option<u64>,
    pub(crate) max_objects: Option<usize>,
    pub(crate) max_object_bytes: Option<u64>,
    pub(crate) max_room_messages: Option<usize>,
    pub(crate) max_user_events: Option<usize>,
    pub(crate) max_records_per_table: Option<usize>,
    pub(crate) max_nested_partitions: Option<usize>,
    pub(crate) max_pending_writes: Option<usize>,
    pub(crate) max_pending_write_bytes: Option<u64>,
    pub(crate) offline_writes: Option<bool>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheInvalidateResponse {
    pub(crate) entry: ClientCacheInvalidationEntry,
    pub(crate) control: ClientCacheControl,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientCacheProfileUpdateResponse {
    pub(crate) profile: ClientCacheProfile,
    pub(crate) invalidation: ClientCacheInvalidationEntry,
}
