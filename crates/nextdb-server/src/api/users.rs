use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::api::wal::{
    append_ordered_wal_record, ensure_shard_not_frozen, maybe_checkpoint,
    writable_wal_shard_for_key,
};
use crate::{
    AppState,
    api::{
        auth::ensure_user_token_authorized,
        error::ApiError,
        events::publish_delivery_event,
        guards::ensure_shard_index,
        mutation::{
            CommittedMutation, UserEventsQuery, UserEventsResponse, find_committed_mutation,
            normalize_client_mutation_id,
        },
        runtime::{begin_runtime_write, ensure_runtime_accepting_writes},
    },
    connection::ConnectionTransport,
    model::{
        DeliveryEvent, Durability, UserEvent, UserProfile, UserProfileDraft, WalPayload, WalRecord,
    },
    util::{normalize_limit, now_ms, shard_index},
};

use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, Uri},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectQuery {
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) transport: Option<ConnectionTransport>,
    pub(crate) metadata: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpsertUserRequest {
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) metadata: serde_json::Value,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListUsersQuery {
    pub(crate) after_user_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) shard: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserResponse {
    pub(crate) user: UserProfile,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListUsersResponse {
    pub(crate) users: Vec<UserProfile>,
    pub(crate) next_after_user_id: Option<String>,
    pub(crate) has_more: bool,
}

pub(crate) async fn get_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(user_id): AxumPath<String>,
) -> Result<Json<UserResponse>, ApiError> {
    if !ensure_safe_user_id(&user_id) {
        return Err(ApiError::bad_request("invalid userId"));
    }
    ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
    let user = state
        .users
        .get_user(&user_id)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    Ok(Json(UserResponse { user }))
}

pub(crate) async fn upsert_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(user_id): AxumPath<String>,
    Json(request): Json<UpsertUserRequest>,
) -> Result<Json<UserResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    if !ensure_safe_user_id(&user_id) {
        return Err(ApiError::bad_request("invalid userId"));
    }
    ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
    let client_mutation_id = normalize_client_mutation_id(request.client_mutation_id)?;
    if let Some(existing) = find_committed_mutation(&state, client_mutation_id.as_deref())? {
        return match existing {
            CommittedMutation::UserUpserted { user } => Ok(Json(UserResponse { user })),
            _ => Err(ApiError::conflict(
                "clientMutationId was already used for a different mutation kind",
            )),
        };
    }
    let existing = state.users.get_user(&user_id)?;
    let now = now_ms();
    let draft = UserProfileDraft {
        user_id: user_id.clone(),
        client_mutation_id,
        display_name: request
            .display_name
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        metadata: if request.metadata.is_null() {
            serde_json::json!({})
        } else {
            request.metadata
        },
        created_at_ms: existing
            .as_ref()
            .map(|user| user.created_at_ms)
            .unwrap_or(now),
        updated_at_ms: now,
        path: format!("users/{user_id}"),
    };
    let _write = begin_runtime_write(&state).await?;
    let shard = writable_wal_shard_for_key(&state, &user_id).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let record = append_ordered_wal_record(
        &state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::UserUpserted {
            user: draft.clone(),
        },
    )
    .await?;
    state.users.apply_wal_record(&record)?;
    let user = draft.into_profile(record.lsn);
    publish_delivery_event(
        &state,
        DeliveryEvent::UserUpserted {
            user_id: user.user_id.clone(),
            user: user.clone(),
        },
    );
    maybe_checkpoint(&state).await?;
    Ok(Json(UserResponse { user }))
}

pub(crate) async fn list_user_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(user_id): AxumPath<String>,
    Query(query): Query<UserEventsQuery>,
) -> Result<Json<UserEventsResponse>, ApiError> {
    if !ensure_safe_user_id(&user_id) {
        return Err(ApiError::bad_request("invalid userId"));
    }
    ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
    let limit = normalize_limit(query.limit);
    let events = state.users.list_events(&user_id, query.before_lsn, limit)?;
    Ok(Json(UserEventsResponse { user_id, events }))
}

pub(crate) async fn list_users(
    State(state): State<AppState>,
    Query(query): Query<ListUsersQuery>,
) -> Result<Json<ListUsersResponse>, ApiError> {
    if query
        .after_user_id
        .as_deref()
        .is_some_and(|user_id| !ensure_safe_user_id(user_id))
    {
        return Err(ApiError::bad_request("invalid afterUserId"));
    }
    if let Some(shard) = query.shard {
        ensure_shard_index(&state, shard)?;
    }
    let limit = normalize_limit(query.limit);
    Ok(Json(state.users.list_users(
        query.after_user_id.as_deref(),
        query.shard,
        state.cluster.shard_count(),
        limit,
    )?))
}

pub(crate) fn ensure_safe_user_id(user_id: &str) -> bool {
    !user_id.is_empty()
        && user_id.len() <= 160
        && user_id
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | ':' | '.'))
}

#[derive(Clone, Default)]
pub(crate) struct UserProjection {
    state: Arc<RwLock<UserProjectionState>>,
}

#[derive(Default)]
struct UserProjectionState {
    profiles: BTreeMap<String, UserProfile>,
    events: BTreeMap<String, Vec<UserEvent>>,
}

impl UserProjection {
    pub(crate) fn from_wal_records(records: &[WalRecord]) -> Self {
        let mut state = UserProjectionState::default();
        for record in records {
            state.apply_wal_record(record);
        }
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub(crate) fn get_user(&self, user_id: &str) -> Result<Option<UserProfile>, ApiError> {
        Ok(self
            .state
            .read()
            .map_err(|_| ApiError::internal(anyhow::anyhow!("user projection poisoned")))?
            .profiles
            .get(user_id)
            .cloned())
    }

    pub(crate) fn list_users(
        &self,
        after_user_id: Option<&str>,
        shard: Option<usize>,
        shard_count: usize,
        limit: usize,
    ) -> Result<ListUsersResponse, ApiError> {
        let state = self
            .state
            .read()
            .map_err(|_| ApiError::internal(anyhow::anyhow!("user projection poisoned")))?;
        let mut users = Vec::new();
        for user in state.profiles.values() {
            if after_user_id.is_some_and(|after| user.user_id.as_str() <= after) {
                continue;
            }
            if shard.is_some_and(|shard| shard_index(&user.user_id, shard_count) != shard) {
                continue;
            }
            users.push(user.clone());
            if users.len() > limit {
                break;
            }
        }
        let has_more = users.len() > limit;
        if has_more {
            users.truncate(limit);
        }
        let next_after_user_id = users.last().map(|user| user.user_id.clone());
        Ok(ListUsersResponse {
            users,
            next_after_user_id,
            has_more,
        })
    }

    pub(crate) fn list_events(
        &self,
        user_id: &str,
        before_lsn: Option<u64>,
        limit: usize,
    ) -> Result<Vec<UserEvent>, ApiError> {
        let state = self
            .state
            .read()
            .map_err(|_| ApiError::internal(anyhow::anyhow!("user projection poisoned")))?;
        Ok(state
            .events
            .get(user_id)
            .map(|events| {
                events
                    .iter()
                    .rev()
                    .filter(|event| before_lsn.is_none_or(|before| event.lsn < before))
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    pub(crate) fn apply_wal_record(&self, record: &WalRecord) -> Result<(), ApiError> {
        self.state
            .write()
            .map_err(|_| ApiError::internal(anyhow::anyhow!("user projection poisoned")))?
            .apply_wal_record(record);
        Ok(())
    }
}

impl UserProjectionState {
    fn apply_wal_record(&mut self, record: &WalRecord) {
        match &record.payload {
            WalPayload::UserUpserted { user } => {
                let profile = user.clone().into_profile(record.lsn);
                self.profiles.insert(profile.user_id.clone(), profile);
            }
            WalPayload::UserEventPublished { event } => {
                let event = event.clone().into_event(record.lsn);
                let events = self.events.entry(event.user_id.clone()).or_default();
                if !events.iter().any(|existing| existing.lsn == event.lsn) {
                    events.push(event);
                    events.sort_by(|left, right| {
                        left.lsn
                            .cmp(&right.lsn)
                            .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
                            .then_with(|| left.id.cmp(&right.id))
                    });
                }
            }
            _ => {}
        }
    }
}
