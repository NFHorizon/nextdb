use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    api::{
        auth::ensure_user_token_authorized, error::ApiError, events::publish_delivery_event,
        guards::ensure_json_value_limit, objects::validate_event_payload_object_refs,
        runtime::ensure_runtime_accepting_writes,
    },
    model::DeliveryEvent,
    realtime::{
        self, RealtimeChannelStateSnapshot, RealtimeChannelSummary, RealtimeMember,
        unique_member_user_ids,
    },
    util::now_ms,
};

use axum::{
    Json,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, Uri},
};
use tracing::warn;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeJoinRequest {
    pub(crate) user_id: String,
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeJoinResponse {
    pub(crate) channel_id: String,
    pub(crate) member: RealtimeMember,
    pub(crate) members: Vec<RealtimeMember>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeLeaveRequest {
    pub(crate) user_id: String,
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeLeaveResponse {
    pub(crate) channel_id: String,
    pub(crate) removed: bool,
    pub(crate) members: Vec<RealtimeMember>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimePresenceUpdateRequest {
    pub(crate) user_id: String,
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimePresenceUpdateResponse {
    pub(crate) channel_id: String,
    pub(crate) member: RealtimeMember,
    pub(crate) members: Vec<RealtimeMember>,
    pub(crate) sequence: u64,
    pub(crate) delivered: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeSignalRequest {
    pub(crate) from_user_id: String,
    pub(crate) to_user_id: String,
    pub(crate) kind: String,
    pub(crate) payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeBroadcastRequest {
    pub(crate) from_user_id: String,
    pub(crate) kind: String,
    pub(crate) payload: serde_json::Value,
    pub(crate) include_self: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeStateUpdateRequest {
    pub(crate) from_user_id: String,
    pub(crate) state: serde_json::Value,
    pub(crate) expected_version: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeSignalResponse {
    pub(crate) channel_id: String,
    pub(crate) sequence: u64,
    pub(crate) timestamp_ms: u64,
    pub(crate) delivered: bool,
    pub(crate) delivered_sessions: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeBroadcastResponse {
    pub(crate) channel_id: String,
    pub(crate) sequence: u64,
    pub(crate) delivered: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeChannelStateResponse {
    pub(crate) channel_id: String,
    pub(crate) state: RealtimeChannelStateSnapshot,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeStateUpdateResponse {
    pub(crate) channel_id: String,
    pub(crate) state: RealtimeChannelStateSnapshot,
    pub(crate) sequence: u64,
    pub(crate) delivered: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeMembersResponse {
    pub(crate) channel_id: String,
    pub(crate) members: Vec<RealtimeMember>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeChannelListResponse {
    pub(crate) channels: Vec<RealtimeChannelSummary>,
    pub(crate) total: usize,
}

pub(crate) async fn realtime_members(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
) -> Json<RealtimeMembersResponse> {
    let members = state.realtime.members(&channel_id).await;
    Json(RealtimeMembersResponse {
        channel_id,
        members,
    })
}

pub(crate) async fn realtime_channel_list(
    State(state): State<AppState>,
) -> Json<RealtimeChannelListResponse> {
    let channels = state.realtime.list_channels().await;
    Json(RealtimeChannelListResponse {
        total: channels.len(),
        channels,
    })
}

pub(crate) async fn realtime_channel_state(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
) -> Json<RealtimeChannelStateResponse> {
    let snapshot = state.realtime.state(&channel_id).await;
    Json(RealtimeChannelStateResponse {
        channel_id,
        state: snapshot,
    })
}

pub(crate) async fn realtime_join(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimeJoinRequest>,
) -> Result<Json<RealtimeJoinResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_user_token_authorized(&state, &headers, &uri, &request.user_id)?;
    if request.user_id.trim().is_empty() {
        return Err(ApiError::bad_request("userId is required"));
    }
    if let Some(session_id) = request.session_id.as_deref() {
        if session_id.trim().is_empty() {
            return Err(ApiError::bad_request("sessionId cannot be empty"));
        }
        if !state
            .connections
            .has_user_session(&request.user_id, session_id)
            .await
        {
            return Err(ApiError::bad_request(
                "sessionId must reference an active connection for userId",
            ));
        }
    }
    let join = state
        .realtime
        .join(
            channel_id,
            request.user_id,
            request.session_id,
            request.metadata,
        )
        .await;
    state.aggregates.publish_presence_update(
        &join.channel_id,
        &join.members,
        state.current_lsn.load(std::sync::atomic::Ordering::Acquire),
        now_ms(),
    );

    for member_user_id in unique_member_user_ids(&join.previous_members) {
        publish_volatile_user_event_to_realtime_members(
            &state,
            &member_user_id,
            &join.previous_members,
            "realtime.channel.memberJoined",
            serde_json::json!({
                "channelId": join.channel_id,
                "member": join.member,
            }),
        )
        .await?;
    }

    Ok(Json(RealtimeJoinResponse {
        channel_id: join.channel_id,
        member: join.member,
        members: join.members,
    }))
}

pub(crate) async fn realtime_leave(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimeLeaveRequest>,
) -> Result<Json<RealtimeLeaveResponse>, ApiError> {
    ensure_user_token_authorized(&state, &headers, &uri, &request.user_id)?;
    if request.user_id.trim().is_empty() {
        return Err(ApiError::bad_request("userId is required"));
    }
    let leave = state
        .realtime
        .leave(&channel_id, &request.user_id, request.session_id.as_deref())
        .await;
    if !leave.removed.is_empty() {
        state.aggregates.publish_presence_update(
            &leave.channel_id,
            &leave.remaining,
            state.current_lsn.load(std::sync::atomic::Ordering::Acquire),
            now_ms(),
        );
        publish_realtime_member_left(&state, &leave).await;
    }

    Ok(Json(RealtimeLeaveResponse {
        channel_id: leave.channel_id,
        removed: !leave.removed.is_empty(),
        members: leave.remaining,
    }))
}

pub(crate) async fn update_realtime_presence(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimePresenceUpdateRequest>,
) -> Result<Json<RealtimePresenceUpdateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_user_token_authorized(&state, &headers, &uri, &request.user_id)?;
    if request.user_id.trim().is_empty() {
        return Err(ApiError::bad_request("userId is required"));
    }
    if let Some(session_id) = request.session_id.as_deref()
        && session_id.trim().is_empty()
    {
        return Err(ApiError::bad_request("sessionId cannot be empty"));
    }
    ensure_json_value_limit(
        "realtime member metadata",
        &request.metadata,
        state.limits.max_user_event_bytes,
    )?;

    let update = commit_realtime_presence(
        &state,
        channel_id,
        request.user_id,
        request.session_id,
        request.metadata,
    )
    .await?;
    let member = update.updated.first().cloned().ok_or_else(|| {
        ApiError::internal(anyhow::anyhow!("presence update returned no members"))
    })?;

    Ok(Json(RealtimePresenceUpdateResponse {
        channel_id: update.channel_id,
        member,
        members: update.members,
        sequence: update.sequence,
        delivered: update.delivered,
    }))
}

pub(crate) async fn commit_realtime_presence(
    state: &AppState,
    channel_id: String,
    user_id: String,
    session_id: Option<String>,
    metadata: serde_json::Value,
) -> Result<RealtimePresenceCommit, ApiError> {
    if user_id.trim().is_empty() {
        return Err(ApiError::bad_request("userId is required"));
    }
    if let Some(session_id) = session_id.as_deref()
        && session_id.trim().is_empty()
    {
        return Err(ApiError::bad_request("sessionId cannot be empty"));
    }
    ensure_json_value_limit(
        "realtime member metadata",
        &metadata,
        state.limits.max_user_event_bytes,
    )?;

    let update = if let Some(session_id) = session_id.as_deref() {
        let update = state
            .realtime
            .update_member(&channel_id, &user_id, Some(session_id), metadata)
            .await
            .ok_or_else(|| {
                ApiError::bad_request(
                    "userId/sessionId must join the realtime channel before updating presence",
                )
            })?;
        realtime::RealtimeMemberBatchUpdate {
            channel_id: update.channel_id,
            updated: vec![update.member],
            members: update.members,
        }
    } else {
        state
            .realtime
            .update_user_members(&channel_id, &user_id, metadata)
            .await
            .ok_or_else(|| {
                ApiError::bad_request(
                    "userId must join the realtime channel before updating presence",
                )
            })?
    };

    let sequence = state.realtime.next_sequence(&channel_id).await;
    let mut delivered = 0;
    for member in &update.updated {
        let timestamp_ms = member.updated_at_ms;
        let payload = serde_json::json!({
            "channelId": update.channel_id.clone(),
            "member": member,
            "sequence": sequence,
            "timestampMs": timestamp_ms,
        });
        state
            .schema
            .validate_event_payload("realtime.channel.memberUpdated", &payload)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        for user_id in unique_member_user_ids(&update.members) {
            delivered += publish_volatile_user_event_to_realtime_members(
                state,
                &user_id,
                &update.members,
                "realtime.channel.memberUpdated",
                payload.clone(),
            )
            .await?;
        }
    }

    Ok(RealtimePresenceCommit {
        channel_id: update.channel_id,
        updated: update.updated,
        members: update.members,
        sequence,
        delivered,
    })
}

pub(crate) async fn update_realtime_channel_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimeStateUpdateRequest>,
) -> Result<Json<RealtimeStateUpdateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_user_token_authorized(&state, &headers, &uri, &request.from_user_id)?;
    if request.from_user_id.trim().is_empty() {
        return Err(ApiError::bad_request("fromUserId is required"));
    }
    let response = commit_realtime_channel_state(
        &state,
        channel_id,
        request.from_user_id,
        request.state,
        request.expected_version,
    )
    .await?;
    Ok(Json(response))
}

pub(crate) async fn commit_realtime_channel_state(
    state: &AppState,
    channel_id: String,
    from_user_id: String,
    state_value: serde_json::Value,
    expected_version: Option<u64>,
) -> Result<RealtimeStateUpdateResponse, ApiError> {
    if from_user_id.trim().is_empty() {
        return Err(ApiError::bad_request("fromUserId is required"));
    }
    if !state.realtime.has_member(&channel_id, &from_user_id).await {
        return Err(ApiError::bad_request(
            "fromUserId must join the realtime channel before updating state",
        ));
    }
    ensure_json_value_limit(
        "realtime channel state",
        &state_value,
        state.limits.max_user_event_bytes,
    )?;

    let current = state.realtime.state(&channel_id).await;
    if expected_version.is_some_and(|expected| expected != current.version) {
        return Err(ApiError::conflict_with_details(
            "realtime channel state version conflict",
            serde_json::json!({
                "stateVersionConflict": true,
                "expectedVersion": expected_version,
                "currentVersion": current.version,
                "current": current,
            }),
        ));
    }
    let candidate_payload = serde_json::json!({
        "channelId": channel_id.clone(),
        "fromUserId": from_user_id.clone(),
        "state": {
            "channelId": channel_id.clone(),
            "version": current.version.saturating_add(1),
            "state": state_value.clone(),
            "updatedAtMs": now_ms(),
        },
        "sequence": 0_u64,
        "timestampMs": now_ms(),
    });
    state
        .schema
        .validate_event_payload("realtime.channel.state", &candidate_payload)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    validate_event_payload_object_refs(state, "realtime.channel.state", &candidate_payload).await?;

    let snapshot = state
        .realtime
        .update_state(&channel_id, state_value, expected_version)
        .await
        .map_err(|conflict| {
            ApiError::conflict_with_details(
                "realtime channel state version conflict",
                serde_json::json!({
                    "stateVersionConflict": true,
                    "expectedVersion": conflict.expected_version,
                    "currentVersion": conflict.current.version,
                    "current": conflict.current,
                }),
            )
        })?;
    let sequence = state.realtime.next_sequence(&channel_id).await;
    let members = state.realtime.members(&channel_id).await;
    let mut delivered = 0;
    for user_id in unique_member_user_ids(&members) {
        delivered += publish_volatile_user_event_to_realtime_members(
            state,
            &user_id,
            &members,
            "realtime.channel.state",
            serde_json::json!({
                "channelId": channel_id.clone(),
                "fromUserId": from_user_id.clone(),
                "state": snapshot.clone(),
                "sequence": sequence,
                "timestampMs": snapshot.updated_at_ms,
            }),
        )
        .await?;
    }

    Ok(RealtimeStateUpdateResponse {
        channel_id,
        state: snapshot,
        sequence,
        delivered,
    })
}

pub(crate) async fn realtime_signal(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimeSignalRequest>,
) -> Result<Json<RealtimeSignalResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_user_token_authorized(&state, &headers, &uri, &request.from_user_id)?;
    if request.from_user_id.trim().is_empty() || request.to_user_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "fromUserId and toUserId are required",
        ));
    }
    if request.kind.trim().is_empty() {
        return Err(ApiError::bad_request("kind is required"));
    }
    if !state
        .realtime
        .has_member(&channel_id, &request.from_user_id)
        .await
    {
        return Err(ApiError::bad_request(
            "fromUserId must join the realtime channel before signaling",
        ));
    }
    if !state
        .realtime
        .has_member(&channel_id, &request.to_user_id)
        .await
    {
        return Err(ApiError::bad_request(
            "toUserId must join the realtime channel before signaling",
        ));
    }
    let timestamp_ms = now_ms();
    let candidate_payload = serde_json::json!({
        "channelId": channel_id.clone(),
        "fromUserId": request.from_user_id.clone(),
        "toUserId": request.to_user_id.clone(),
        "kind": request.kind.clone(),
        "payload": request.payload.clone(),
        "sequence": 0_u64,
        "timestampMs": timestamp_ms,
    });
    validate_volatile_event_payload(&state, "realtime.channel.signal", &candidate_payload).await?;
    let sequence = state.realtime.next_sequence(&channel_id).await;
    let members = state.realtime.members(&channel_id).await;
    let delivered = publish_volatile_user_event_to_realtime_members(
        &state,
        &request.to_user_id,
        &members,
        "realtime.channel.signal",
        serde_json::json!({
            "channelId": channel_id.clone(),
            "fromUserId": candidate_payload["fromUserId"].clone(),
            "toUserId": candidate_payload["toUserId"].clone(),
            "kind": candidate_payload["kind"].clone(),
            "payload": candidate_payload["payload"].clone(),
            "sequence": sequence,
            "timestampMs": timestamp_ms,
        }),
    )
    .await?;
    Ok(Json(RealtimeSignalResponse {
        channel_id,
        sequence,
        timestamp_ms,
        delivered: delivered > 0,
        delivered_sessions: delivered,
    }))
}

pub(crate) async fn realtime_broadcast(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    AxumPath(channel_id): AxumPath<String>,
    Json(request): Json<RealtimeBroadcastRequest>,
) -> Result<Json<RealtimeBroadcastResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_user_token_authorized(&state, &headers, &uri, &request.from_user_id)?;
    let response = commit_realtime_channel_broadcast(
        &state,
        channel_id,
        request.from_user_id,
        request.kind,
        request.payload,
        request.include_self,
    )
    .await?;
    Ok(Json(response))
}

pub(crate) async fn commit_realtime_channel_broadcast(
    state: &AppState,
    channel_id: String,
    from_user_id: String,
    kind: String,
    payload: serde_json::Value,
    include_self: Option<bool>,
) -> Result<RealtimeBroadcastResponse, ApiError> {
    if from_user_id.trim().is_empty() {
        return Err(ApiError::bad_request("fromUserId is required"));
    }
    if kind.trim().is_empty() {
        return Err(ApiError::bad_request("kind is required"));
    }
    if !state.realtime.has_member(&channel_id, &from_user_id).await {
        return Err(ApiError::bad_request(
            "fromUserId must join the realtime channel before broadcasting",
        ));
    }

    let timestamp_ms = now_ms();
    let include_self = include_self.unwrap_or(true);
    let candidate_payload = serde_json::json!({
        "channelId": channel_id.clone(),
        "fromUserId": from_user_id,
        "kind": kind,
        "payload": payload,
        "sequence": 0_u64,
        "timestampMs": timestamp_ms,
    });
    validate_volatile_event_payload(state, "realtime.channel.event", &candidate_payload).await?;
    let sequence = state.realtime.next_sequence(&channel_id).await;
    let members = state.realtime.members(&channel_id).await;
    let mut delivered = 0;
    for user_id in unique_member_user_ids(&members) {
        if !include_self && user_id == candidate_payload["fromUserId"].as_str().unwrap_or_default()
        {
            continue;
        }
        delivered += publish_volatile_user_event_to_realtime_members(
            state,
            &user_id,
            &members,
            "realtime.channel.event",
            serde_json::json!({
                "channelId": channel_id.clone(),
                "fromUserId": candidate_payload["fromUserId"].clone(),
                "kind": candidate_payload["kind"].clone(),
                "payload": candidate_payload["payload"].clone(),
                "sequence": sequence,
                "timestampMs": timestamp_ms,
            }),
        )
        .await?;
    }

    Ok(RealtimeBroadcastResponse {
        channel_id,
        sequence,
        delivered,
    })
}

pub(crate) async fn publish_volatile_user_event(
    state: &AppState,
    user_id: &str,
    name: impl Into<String>,
    payload: serde_json::Value,
) -> Result<usize, ApiError> {
    let name = name.into();
    validate_volatile_event_payload(state, &name, &payload).await?;
    let delivered = state.connections.count_user(user_id).await;
    publish_delivery_event(
        state,
        DeliveryEvent::VolatileUserEvent {
            user_id: user_id.to_string(),
            name,
            payload,
            target_session_ids: None,
        },
    );
    Ok(delivered)
}

async fn publish_volatile_user_event_to_realtime_members(
    state: &AppState,
    user_id: &str,
    members: &[RealtimeMember],
    name: impl Into<String>,
    payload: serde_json::Value,
) -> Result<usize, ApiError> {
    match realtime_user_delivery_scope(user_id, members) {
        Some(RealtimeUserDeliveryScope::AllUserSessions) => {
            publish_volatile_user_event(state, user_id, name, payload).await
        }
        Some(RealtimeUserDeliveryScope::SessionIds(session_ids)) => {
            publish_volatile_user_event_to_sessions(state, user_id, session_ids, name, payload)
                .await
        }
        None => Ok(0),
    }
}

async fn publish_volatile_user_event_to_sessions(
    state: &AppState,
    user_id: &str,
    target_session_ids: BTreeSet<String>,
    name: impl Into<String>,
    payload: serde_json::Value,
) -> Result<usize, ApiError> {
    let name = name.into();
    validate_volatile_event_payload(state, &name, &payload).await?;
    let delivered = state
        .connections
        .count_user_sessions_in(user_id, &target_session_ids)
        .await;
    publish_delivery_event(
        state,
        DeliveryEvent::VolatileUserEvent {
            user_id: user_id.to_string(),
            name,
            payload,
            target_session_ids: Some(target_session_ids),
        },
    );
    Ok(delivered)
}

async fn validate_volatile_event_payload(
    state: &AppState,
    name: &str,
    payload: &serde_json::Value,
) -> Result<(), ApiError> {
    ensure_json_value_limit(
        "volatile user event payload",
        payload,
        state.limits.max_user_event_bytes,
    )?;
    state
        .schema
        .validate_event_payload(name, payload)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    validate_event_payload_object_refs(state, name, payload).await
}

fn realtime_user_delivery_scope(
    user_id: &str,
    members: &[RealtimeMember],
) -> Option<RealtimeUserDeliveryScope> {
    let mut session_ids = BTreeSet::new();
    for member in members.iter().filter(|member| member.user_id == user_id) {
        let Some(session_id) = member.session_id.as_ref() else {
            return Some(RealtimeUserDeliveryScope::AllUserSessions);
        };
        session_ids.insert(session_id.clone());
    }

    (!session_ids.is_empty()).then_some(RealtimeUserDeliveryScope::SessionIds(session_ids))
}

pub(crate) async fn publish_realtime_member_left(
    state: &AppState,
    leave: &realtime::RealtimeLeave,
) {
    for remaining_user_id in unique_member_user_ids(&leave.remaining) {
        if let Err(err) = publish_volatile_user_event_to_realtime_members(
            state,
            &remaining_user_id,
            &leave.remaining,
            "realtime.channel.memberLeft",
            serde_json::json!({
                "channelId": leave.channel_id,
                "members": leave.removed.clone(),
            }),
        )
        .await
        {
            warn!(
                "failed to publish realtime memberLeft event: {}",
                err.message
            );
        }
    }
}

pub(crate) struct RealtimePresenceCommit {
    pub(crate) channel_id: String,
    pub(crate) updated: Vec<RealtimeMember>,
    pub(crate) members: Vec<RealtimeMember>,
    pub(crate) sequence: u64,
    pub(crate) delivered: usize,
}

pub(crate) enum RealtimeUserDeliveryScope {
    AllUserSessions,
    SessionIds(BTreeSet<String>),
}
