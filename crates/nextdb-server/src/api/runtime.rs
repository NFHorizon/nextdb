use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use anyhow::{Context, Result as AnyResult, anyhow};
use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppState,
    actor::{self, ActorId, ActorKernelMessage, ActorRoomStatus, actor_reminder_entry_from_draft},
    api::behavior::{
        BehaviorInvokeResponse, invoke_behavior_internal, note_host_http_callback_scheduled,
    },
    api::error::ApiError,
    api::mutation::{
        CommittedMutation, client_mutation_index_from_wal_records, messages_from_wal_records,
    },
    api::records::{
        ListRecordsQuery, QueryRecordsByIndexQuery, RecordPredicate,
        deserialize_optional_record_predicate, execute_record_index_query,
        execute_record_list_query, get_record_from_live_or_disk, list_records_from_live_or_disk,
        nested_record_key, nested_record_prefix, nested_record_table, records_from_wal_records,
        schema_indexes_by_table, schema_orders_by_table, validate_nested_table_path,
        validate_table_path,
    },
    api::users::UserProjection,
    api::wal::{append_ordered_wal_record, writable_wal_shard_for_key},
    behavior::{BehaviorInvokeRequest, BehaviorReadPlan},
    config::DEFAULT_RESTART_WRITE_WAIT_MS,
    model::{ActorReminderDraft, DbRecord, Durability, Message, WalPayload, WalRecord},
    record_hot::RecordHotCacheStatus,
    record_store::{RecordProjectionStatus, ensure_safe_record_component},
    schema::{DatabaseSchema, SchemaRegistry, SchemaStoragePolicyReport},
    tasks::{RuntimeDrainState, RuntimeWriteGuard, RuntimeWriteState},
    util::{normalize_limit, now_ms},
    wal::{self, WalCompactReport, read_records_from_wal_paths},
};

const DEFAULT_BEHAVIOR_CONTINUATION_MAX_DEPTH: u32 = 32;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminSnapshotResponse {
    pub(crate) lsn: u64,
    pub(crate) room_count: usize,
    pub(crate) record_hot_table_count: usize,
    pub(crate) record_hot_record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimePrepareRestartRequest {
    pub(crate) reason: Option<String>,
    pub(crate) snapshot: Option<bool>,
    pub(crate) compact_wal: Option<bool>,
    pub(crate) wait_for_writes_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimePrepareRestartResponse {
    pub(crate) drain: RuntimeDrainState,
    pub(crate) runtime_writes: RuntimeWriteState,
    pub(crate) writes_quiesced: bool,
    pub(crate) write_wait_timed_out: bool,
    pub(crate) waited_for_writes_ms: u64,
    pub(crate) ready_for_restart: bool,
    pub(crate) snapshot: Option<AdminSnapshotResponse>,
    pub(crate) compact_wal: Option<WalCompactResponse>,
    pub(crate) current_lsn: u64,
    pub(crate) prepared_at_ms: u64,
}

pub(crate) async fn get_runtime_drain(State(state): State<AppState>) -> Json<RuntimeDrainState> {
    Json(state.runtime_drain.read().await.clone())
}

pub(crate) async fn set_runtime_drain(
    State(state): State<AppState>,
    Json(request): Json<RuntimeDrainRequest>,
) -> Json<RuntimeDrainState> {
    Json(set_runtime_drain_state(&state, request.draining, request.reason).await)
}

pub(crate) async fn set_runtime_drain_state(
    state: &AppState,
    draining: bool,
    reason: Option<String>,
) -> RuntimeDrainState {
    let mut drain = state.runtime_drain.write().await;
    drain.draining = draining;
    drain.reason = reason
        .map(|reason| reason.trim().to_string())
        .filter(|reason| !reason.is_empty());
    drain.updated_at_ms = Some(now_ms());
    info!(
        draining = drain.draining,
        reason = drain.reason.as_deref().unwrap_or(""),
        "runtime drain state changed"
    );
    drain.clone()
}

pub(crate) async fn ensure_runtime_accepting_writes(state: &AppState) -> Result<(), ApiError> {
    let drain = state.runtime_drain.read().await;
    if !drain.draining {
        return Ok(());
    }
    Err(ApiError::unavailable_with_details(
        "node is draining; retry another replica",
        serde_json::json!({
            "draining": true,
            "reason": drain.reason.clone(),
            "updatedAtMs": drain.updated_at_ms,
        }),
    ))
}

pub(crate) async fn begin_runtime_write(state: &AppState) -> Result<RuntimeWriteGuard, ApiError> {
    let gate = state.runtime_write_gate.clone().read_owned().await;
    ensure_runtime_accepting_writes(state).await?;
    state.runtime_writes.begin();
    Ok(RuntimeWriteGuard {
        tracker: state.runtime_writes.clone(),
        _gate: gate,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeRecordActivationRequest {
    pub(crate) table: String,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) key: Option<String>,
    #[serde(default)]
    pub(crate) keys: Vec<String>,
    pub(crate) index_name: Option<String>,
    pub(crate) value: Option<serde_json::Value>,
    pub(crate) values: Option<serde_json::Value>,
    pub(crate) lower: Option<serde_json::Value>,
    pub(crate) upper: Option<serde_json::Value>,
    pub(crate) lower_values: Option<serde_json::Value>,
    pub(crate) upper_values: Option<serde_json::Value>,
    pub(crate) after_key: Option<String>,
    pub(crate) after_cursor: Option<String>,
    pub(crate) order: Option<String>,
    pub(crate) limit: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_record_predicate")]
    pub(crate) predicate: Option<RecordPredicate>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeRecordActivationResponse {
    pub(crate) table: String,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) requested: usize,
    pub(crate) found: usize,
    pub(crate) activated: usize,
    pub(crate) evicted: usize,
    pub(crate) actor_scope: Option<actor::ScopeRowsActivationResult>,
    pub(crate) actor_scopes: Vec<actor::ScopeRowsActivationResult>,
    pub(crate) before: RecordHotCacheStatus,
    pub(crate) after: RecordHotCacheStatus,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeRoomActivationRequest {
    pub(crate) room_id: String,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeRoomActivationResponse {
    pub(crate) room_id: String,
    pub(crate) requested: usize,
    pub(crate) found: usize,
    pub(crate) activated: bool,
    pub(crate) evicted: bool,
    pub(crate) before_room_count: usize,
    pub(crate) after_room_count: usize,
    pub(crate) source: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorActivationRequest {
    pub(crate) kind: actor::ActorKind,
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorActivationResponse {
    pub(crate) actor_id: ActorId,
    pub(crate) shard_index: usize,
    pub(crate) activated: bool,
    pub(crate) turn_count: u64,
    pub(crate) last_accessed_ms: u64,
    pub(crate) before: actor::ActorKernelStatus,
    pub(crate) after: actor::ActorKernelStatus,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderScheduleRequest {
    pub(crate) kind: actor::ActorKind,
    pub(crate) key: String,
    pub(crate) reminder_id: Option<String>,
    pub(crate) due_at_ms: Option<u64>,
    pub(crate) delay_ms: Option<u64>,
    pub(crate) payload: Option<serde_json::Value>,
    #[serde(skip)]
    pub(crate) idempotency: Option<ActorReminderIdempotency>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ActorReminderIdempotency;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderCancelRequest {
    pub(crate) kind: actor::ActorKind,
    pub(crate) key: String,
    pub(crate) reminder_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderRunDueRequest {
    pub(crate) limit: Option<usize>,
    pub(crate) now_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeBehaviorContinuationPayload {
    #[serde(rename = "type")]
    payload_type: String,
    behavior: String,
    mutation: String,
    user_id: Option<String>,
    client_mutation_id: Option<String>,
    input: Option<serde_json::Value>,
    read: Option<BehaviorReadPlan>,
    context: Option<serde_json::Value>,
    reply_to: Option<RuntimeBehaviorContinuationReplyTarget>,
    call_chain_id: Option<String>,
    call_depth: Option<u32>,
    max_depth: Option<u32>,
    deadline_ms: Option<u64>,
    path: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeBehaviorContinuationReplyTarget {
    actor_kind: actor::ActorKind,
    actor_key: String,
    reminder_id: Option<String>,
    continuation: Box<RuntimeBehaviorContinuationPayload>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderMutationResponse {
    pub(crate) reminder: actor::ActorReminderEntry,
    pub(crate) lsn: u64,
    pub(crate) accepted_at_ms: u64,
    pub(crate) pending: actor::ActorReminderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorReminderRecordStatus {
    Scheduled,
    Cancelled,
    Fired,
}

#[derive(Debug, Clone)]
pub(crate) struct ActorReminderRecord {
    pub(crate) reminder: ActorReminderDraft,
    pub(crate) scheduled_lsn: u64,
    pub(crate) scheduled_at_ms: u64,
    pub(crate) status: ActorReminderRecordStatus,
}

pub(crate) type ActorReminderIndexKey = (String, String, String);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderCancelResponse {
    pub(crate) actor_id: ActorId,
    pub(crate) reminder_id: String,
    pub(crate) cancelled: bool,
    pub(crate) lsn: u64,
    pub(crate) accepted_at_ms: u64,
    pub(crate) pending: actor::ActorReminderStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderFireResult {
    pub(crate) reminder: actor::ActorReminderEntry,
    pub(crate) turn: actor::ActorTurnResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) behavior: Option<BehaviorInvokeResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reply: Option<RuntimeActorReminderMutationResponse>,
    pub(crate) fired_lsn: u64,
    pub(crate) fired_at_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActorReminderRunDueResponse {
    pub(crate) checked_at_ms: u64,
    pub(crate) requested: usize,
    pub(crate) fired: Vec<RuntimeActorReminderFireResult>,
    pub(crate) pending: actor::ActorReminderStatus,
    pub(crate) maintenance: actor::ActorReminderMaintenanceStatus,
}

impl RuntimeBehaviorContinuationPayload {
    fn from_reminder_payload(
        payload: &Option<serde_json::Value>,
        now_ms: u64,
    ) -> Result<Option<Self>, ApiError> {
        let Some(payload) = payload else {
            return Ok(None);
        };
        if payload.get("type").and_then(|value| value.as_str()) != Some("behaviorContinuation") {
            return Ok(None);
        }
        let continuation: Self = serde_json::from_value(payload.clone()).map_err(|err| {
            ApiError::bad_request(format!("invalid behavior continuation payload: {err}"))
        })?;
        if continuation.behavior.trim().is_empty() {
            return Err(ApiError::bad_request(
                "behavior continuation behavior is required",
            ));
        }
        if continuation.mutation.trim().is_empty() {
            return Err(ApiError::bad_request(
                "behavior continuation mutation is required",
            ));
        }
        if continuation.payload_type != "behaviorContinuation" {
            return Ok(None);
        }
        continuation.validate_limits(now_ms)?;
        Ok(Some(continuation))
    }

    fn target(&self) -> String {
        format!("{}.{}", self.behavior, self.mutation)
    }

    fn validate_limits(&self, now_ms: u64) -> Result<(), ApiError> {
        if let Some(deadline_ms) = self.deadline_ms
            && now_ms > deadline_ms
        {
            return Err(ApiError::bad_request(
                "behavior continuation deadlineMs has expired",
            ));
        }
        let max_depth = self
            .max_depth
            .unwrap_or(DEFAULT_BEHAVIOR_CONTINUATION_MAX_DEPTH)
            .max(1);
        let call_depth = self.call_depth.unwrap_or(0);
        if call_depth >= max_depth {
            return Err(ApiError::bad_request(
                "behavior continuation maxDepth exceeded",
            ));
        }
        let target = self.target();
        if self
            .path
            .as_ref()
            .is_some_and(|path| path.iter().any(|entry| entry == &target))
        {
            return Err(ApiError::bad_request(
                "behavior continuation cycle detected",
            ));
        }
        Ok(())
    }

    fn into_behavior_request(self) -> BehaviorInvokeRequest {
        let target = self.target();
        let call_depth = self.call_depth.unwrap_or(0).saturating_add(1);
        let max_depth = self
            .max_depth
            .unwrap_or(DEFAULT_BEHAVIOR_CONTINUATION_MAX_DEPTH);
        let mut context = self.context.unwrap_or(serde_json::Value::Null);
        let mut path = self.path.unwrap_or_default();
        path.push(target);
        let call_chain_id = self.call_chain_id.clone();
        let deadline_ms = self.deadline_ms;
        context = match context {
            serde_json::Value::Object(mut object) => {
                if let Some(call_chain_id) = call_chain_id {
                    object.insert(
                        "callChainId".to_string(),
                        serde_json::Value::String(call_chain_id),
                    );
                }
                object.insert("callDepth".to_string(), serde_json::json!(call_depth));
                object.insert("maxDepth".to_string(), serde_json::json!(max_depth));
                if let Some(deadline_ms) = deadline_ms {
                    object.insert("deadlineMs".to_string(), serde_json::json!(deadline_ms));
                }
                object.insert("path".to_string(), serde_json::json!(path));
                serde_json::Value::Object(object)
            }
            serde_json::Value::Null => serde_json::json!({
                "callChainId": call_chain_id,
                "callDepth": call_depth,
                "maxDepth": max_depth,
                "deadlineMs": deadline_ms,
                "path": path,
            }),
            other => serde_json::json!({
                "callChainId": call_chain_id,
                "callDepth": call_depth,
                "maxDepth": max_depth,
                "deadlineMs": deadline_ms,
                "path": path,
                "value": other,
            }),
        };
        BehaviorInvokeRequest {
            behavior: self.behavior,
            mutation: self.mutation,
            user_id: self.user_id,
            client_mutation_id: self.client_mutation_id,
            input: self.input.unwrap_or_else(|| serde_json::json!({})),
            read: self.read.unwrap_or_default(),
            context,
        }
    }

    fn reply_schedule_request(
        &self,
        response: &BehaviorInvokeResponse,
        now_ms: u64,
    ) -> Result<Option<RuntimeActorReminderScheduleRequest>, ApiError> {
        let Some(reply_to) = self.reply_to.clone() else {
            return Ok(None);
        };
        let target_depth = self.call_depth.unwrap_or(0).saturating_add(1);
        let mut target_path = self.path.clone().unwrap_or_default();
        target_path.push(self.target());
        let mut continuation = *reply_to.continuation;
        if continuation.call_chain_id.is_none() {
            continuation.call_chain_id = self.call_chain_id.clone();
        }
        if continuation.call_depth.is_none() {
            continuation.call_depth = Some(target_depth);
        }
        if continuation.max_depth.is_none() {
            continuation.max_depth = self.max_depth;
        }
        if continuation.deadline_ms.is_none() {
            continuation.deadline_ms = self.deadline_ms;
        }
        if continuation.path.is_none() {
            continuation.path = Some(target_path);
        }
        continuation.input = Some(behavior_reply_input(continuation.input.take(), response)?);
        continuation.validate_limits(now_ms)?;
        let payload = serde_json::to_value(&continuation).map_err(|err| {
            ApiError::internal(anyhow::anyhow!(
                "serialize behavior reply continuation: {err}"
            ))
        })?;
        Ok(Some(RuntimeActorReminderScheduleRequest {
            kind: reply_to.actor_kind,
            key: reply_to.actor_key,
            reminder_id: reply_to.reminder_id,
            due_at_ms: Some(now_ms.max(1)),
            delay_ms: None,
            payload: Some(payload),
            idempotency: None,
        }))
    }
}

fn behavior_reply_input(
    existing_input: Option<serde_json::Value>,
    response: &BehaviorInvokeResponse,
) -> Result<serde_json::Value, ApiError> {
    let response_value = serde_json::to_value(response).map_err(|err| {
        ApiError::internal(anyhow::anyhow!(
            "serialize behavior response for reply: {err}"
        ))
    })?;
    Ok(match existing_input {
        Some(serde_json::Value::Object(mut object)) => {
            object.insert("behaviorResponse".to_string(), response_value);
            serde_json::Value::Object(object)
        }
        Some(value) => serde_json::json!({
            "input": value,
            "behaviorResponse": response_value,
        }),
        None => serde_json::json!({
            "behaviorResponse": response_value,
        }),
    })
}

pub(crate) fn validate_behavior_continuation_payload(
    payload: &serde_json::Value,
    now_ms: u64,
) -> Result<(), ApiError> {
    if payload.get("type").and_then(|value| value.as_str()) != Some("behaviorContinuation") {
        return Err(ApiError::bad_request(
            "behavior continuation payload type must be behaviorContinuation",
        ));
    }
    RuntimeBehaviorContinuationPayload::from_reminder_payload(&Some(payload.clone()), now_ms)?;
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeActivationStatusResponse {
    pub(crate) rooms: Vec<ActorRoomStatus>,
    pub(crate) room_count: usize,
    pub(crate) actor_kernel: actor::ActorKernelStatus,
    pub(crate) actor_shards: Vec<actor::ActorShardRuntimeStatus>,
    pub(crate) max_hot_rooms: usize,
    pub(crate) hot_window: usize,
    pub(crate) hot_room_idle_ttl_ms: u64,
    pub(crate) hot_room_maintenance_interval_ms: u64,
    pub(crate) hot_room_idle_maintenance: actor::ActorIdleMaintenanceStatus,
    pub(crate) actor_scope_residency_maintenance_interval_ms: u64,
    pub(crate) actor_scope_residency_maintenance_limit: usize,
    pub(crate) actor_scope_residency_maintenance: actor::ActorScopeResidencyMaintenanceStatus,
    pub(crate) actor_split_maintenance_interval_ms: u64,
    pub(crate) actor_split_maintenance_limit: usize,
    pub(crate) actor_split_maintenance: actor::ActorSplitMaintenanceStatus,
    pub(crate) actor_reminder_maintenance_interval_ms: u64,
    pub(crate) actor_reminder_maintenance_limit: usize,
    pub(crate) actor_reminders: actor::ActorReminderStatus,
    pub(crate) actor_reminder_maintenance: actor::ActorReminderMaintenanceStatus,
    pub(crate) record_hot_maintenance_interval_ms: u64,
    pub(crate) record_hot_prewarm_limit: usize,
    pub(crate) record_hot_prewarm: RecordHotPrewarmStatus,
    pub(crate) record_hot_cache: RecordHotCacheStatus,
}

pub(crate) async fn runtime_activation_status(
    State(state): State<AppState>,
) -> Json<RuntimeActivationStatusResponse> {
    let rooms = state.actors.room_statuses().await;
    let actor_kernel = state.actors.kernel_status().await;
    Json(RuntimeActivationStatusResponse {
        room_count: rooms.len(),
        rooms,
        actor_kernel,
        actor_shards: state.actors.shard_statuses(),
        max_hot_rooms: state.actors.max_hot_rooms(),
        hot_window: state.actors.hot_window(),
        hot_room_idle_ttl_ms: state.actors.hot_room_idle_ttl_ms(),
        hot_room_maintenance_interval_ms: state.hot_room_maintenance_interval_ms,
        hot_room_idle_maintenance: state.actors.idle_maintenance_status(),
        actor_scope_residency_maintenance_interval_ms: state
            .actor_scope_residency_maintenance_interval_ms,
        actor_scope_residency_maintenance_limit: state.actor_scope_residency_maintenance_limit,
        actor_scope_residency_maintenance: state.actors.scope_residency_maintenance_status(),
        actor_split_maintenance_interval_ms: state.actor_split_maintenance_interval_ms,
        actor_split_maintenance_limit: state.actor_split_maintenance_limit,
        actor_split_maintenance: state.actors.split_maintenance_status(),
        actor_reminder_maintenance_interval_ms: state.actor_reminder_maintenance_interval_ms,
        actor_reminder_maintenance_limit: state.actor_reminder_maintenance_limit,
        actor_reminders: state.actors.reminder_status(64),
        actor_reminder_maintenance: state.actors.reminder_maintenance_status(),
        record_hot_maintenance_interval_ms: state.record_hot_maintenance_interval_ms,
        record_hot_prewarm_limit: state.record_hot_prewarm_limit,
        record_hot_prewarm: state.record_hot_prewarm.read().await.clone(),
        record_hot_cache: state.record_hot.status().await,
    })
}

pub(crate) async fn projection_status(
    State(state): State<AppState>,
) -> Result<Json<RecordProjectionStatus>, ApiError> {
    let status = state
        .records
        .projection_status()
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(status))
}

pub(crate) async fn projection_rebuild_status(
    State(state): State<AppState>,
) -> Json<ProjectionRebuildStatus> {
    Json(state.projection_rebuild_status.read().await.clone())
}

type StartupProjectionSeed = (
    Vec<Message>,
    Vec<DbRecord>,
    UserProjection,
    BTreeMap<String, CommittedMutation>,
);

pub(crate) fn read_startup_projections_from_wal_paths(
    paths: &[PathBuf],
) -> AnyResult<StartupProjectionSeed> {
    let wal_records = read_records_from_wal_paths(paths)?;
    let messages = messages_from_wal_records(&wal_records);
    let users = UserProjection::from_wal_records(&wal_records);
    let client_mutations = client_mutation_index_from_wal_records(&wal_records);
    let records = records_from_wal_records(wal_records);
    Ok((messages, records, users, client_mutations))
}

pub(crate) fn read_all_records_from_wal_paths(paths: &[PathBuf]) -> AnyResult<Vec<DbRecord>> {
    Ok(records_from_wal_records(read_records_from_wal_paths(
        paths,
    )?))
}

pub(crate) async fn restore_missing_wal_from_replicas(
    primary: &PathBuf,
    replicas: &[PathBuf],
) -> AnyResult<WalRestoreReport> {
    let mut report = WalRestoreReport {
        primary: primary.clone(),
        replicas_checked: replicas.to_vec(),
        restored: false,
        restored_from: None,
        archive_files_restored: 0,
    };
    if primary.exists() || replicas.is_empty() {
        return Ok(report);
    }

    let Some(replica) = replicas.iter().find(|path| path.exists()) else {
        return Ok(report);
    };
    if let Some(parent) = primary.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::copy(replica, primary).await.with_context(|| {
        format!(
            "restore WAL {} from {}",
            primary.display(),
            replica.display()
        )
    })?;
    report.restored = true;
    report.restored_from = Some(replica.clone());

    let replica_archive = replica.parent().map(|parent| parent.join("archive"));
    let primary_archive = primary.parent().map(|parent| parent.join("archive"));
    if let (Some(replica_archive), Some(primary_archive)) = (replica_archive, primary_archive)
        && replica_archive.exists()
    {
        fs::create_dir_all(&primary_archive).await?;
        let mut entries = fs::read_dir(&replica_archive).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let dest = primary_archive.join(entry.file_name());
            if !dest.exists() {
                fs::copy(entry.path(), dest).await?;
                report.archive_files_restored += 1;
            }
        }
    }

    Ok(report)
}

pub(crate) async fn recover_schema_from_wal(
    registry: &SchemaRegistry,
    paths: &[PathBuf],
) -> AnyResult<SchemaWalRecoveryReport> {
    let mut history = BTreeMap::<u32, DatabaseSchema>::new();
    let mut latest = None::<(u64, DatabaseSchema)>;
    for wal_record in read_records_from_wal_paths(paths)? {
        let WalPayload::SchemaApplied { schema, .. } = wal_record.payload else {
            continue;
        };
        schema.validation_report().into_result()?;
        history.insert(schema.version, schema.clone());
        if latest
            .as_ref()
            .is_none_or(|(latest_lsn, _)| wal_record.lsn > *latest_lsn)
        {
            latest = Some((wal_record.lsn, schema));
        }
    }

    for schema in history.values() {
        registry.persist_history_schema(schema).await?;
    }

    if let Some((latest_lsn, latest_schema)) = latest {
        registry.persist_candidate(&latest_schema).await?;
        registry.apply(latest_schema.clone());
        Ok(SchemaWalRecoveryReport {
            recovered: true,
            latest_lsn: Some(latest_lsn),
            latest_version: Some(latest_schema.version),
            history_versions: history.into_keys().collect(),
        })
    } else {
        Ok(SchemaWalRecoveryReport {
            recovered: false,
            latest_lsn: None,
            latest_version: None,
            history_versions: registry
                .history()
                .await?
                .into_iter()
                .map(|entry| entry.version)
                .collect(),
        })
    }
}

pub(crate) async fn rebuild_projections(
    State(state): State<AppState>,
    request: Option<Json<ProjectionRebuildRequest>>,
) -> Result<Json<ProjectionRebuildResponse>, ApiError> {
    let background = request
        .as_ref()
        .is_some_and(|Json(request)| request.background);
    if background {
        let started = mark_projection_rebuild_running(&state, true).await?;
        let task_state = state.clone();
        tokio::spawn(async move {
            let _guard = task_state.schema_apply_lock.lock().await;
            let result = rebuild_projections_internal(&task_state).await;
            finish_projection_rebuild(&task_state, result).await;
        });
        return Ok(Json(ProjectionRebuildResponse::from_status(started)));
    }

    let _guard = state.schema_apply_lock.lock().await;
    mark_projection_rebuild_running(&state, false).await?;
    let result = rebuild_projections_internal(&state).await;
    let finished = finish_projection_rebuild(&state, result).await;
    if let Some(error) = &finished.error {
        return Err(ApiError::internal(anyhow!(error.clone())));
    }
    Ok(Json(ProjectionRebuildResponse::from_status(finished)))
}

async fn rebuild_projections_internal(
    state: &AppState,
) -> Result<ProjectionRebuildCounts, ApiError> {
    let (messages, records, _, _) =
        read_startup_projections_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
    let count = state
        .chat_log
        .force_rebuild_from_messages(&messages)
        .await
        .map_err(ApiError::internal)?;
    let record_count = state
        .records
        .force_rebuild_from_records_with_indexes(
            &records,
            &schema_indexes_by_table(&state.schema.schema()),
            &schema_orders_by_table(&state.schema.schema())
                .map_err(|err| ApiError::bad_request(err.to_string()))?,
        )
        .await
        .map_err(ApiError::internal)?;
    state
        .record_hot
        .reconfigure(
            &state.schema.schema(),
            &records,
            state.record_hot_durable_idle_ttl_ms,
        )
        .await;
    let refs = state
        .object_refs
        .rebuild_for_schema(&messages, &records, &state.schema.schema())
        .await
        .map_err(ApiError::internal)?;
    Ok(ProjectionRebuildCounts {
        messages: count,
        records: record_count,
        object_refs: refs.refs.len(),
    })
}

async fn mark_projection_rebuild_running(
    state: &AppState,
    background: bool,
) -> Result<ProjectionRebuildStatus, ApiError> {
    let mut status = state.projection_rebuild_status.write().await;
    if status.phase == ProjectionRebuildPhase::Running {
        return Err(ApiError::conflict("projection rebuild already running"));
    }
    let next = ProjectionRebuildStatus {
        phase: ProjectionRebuildPhase::Running,
        run_id: Some(Uuid::now_v7().to_string()),
        background,
        started_at_ms: Some(now_ms()),
        finished_at_ms: None,
        messages: None,
        records: None,
        object_refs: None,
        error: None,
    };
    *status = next.clone();
    Ok(next)
}

async fn finish_projection_rebuild(
    state: &AppState,
    result: Result<ProjectionRebuildCounts, ApiError>,
) -> ProjectionRebuildStatus {
    let mut status = state.projection_rebuild_status.write().await;
    match result {
        Ok(counts) => {
            status.phase = ProjectionRebuildPhase::Succeeded;
            status.finished_at_ms = Some(now_ms());
            status.messages = Some(counts.messages);
            status.records = Some(counts.records);
            status.object_refs = Some(counts.object_refs);
            status.error = None;
        }
        Err(err) => {
            status.phase = ProjectionRebuildPhase::Failed;
            status.finished_at_ms = Some(now_ms());
            status.error = Some(err.message);
        }
    }
    status.clone()
}

pub(crate) async fn create_snapshot(
    State(state): State<AppState>,
) -> Result<Json<AdminSnapshotResponse>, ApiError> {
    let lsn = state.current_lsn.load(Ordering::Acquire);
    Ok(Json(write_snapshot(&state, lsn).await?))
}

pub(crate) async fn write_snapshot(
    state: &AppState,
    lsn: u64,
) -> Result<AdminSnapshotResponse, ApiError> {
    let mut snapshot = state
        .actors
        .snapshot_with_schema(lsn, state.schema.version())
        .await;
    snapshot.record_hot = Some(state.record_hot.snapshot().await);
    let room_count = snapshot.rooms.len();
    let record_hot_table_count = snapshot
        .record_hot
        .as_ref()
        .map(|record_hot| record_hot.table_count())
        .unwrap_or(0);
    let record_hot_record_count = snapshot
        .record_hot
        .as_ref()
        .map(|record_hot| record_hot.record_count())
        .unwrap_or(0);
    state
        .snapshots
        .save(&snapshot)
        .await
        .map_err(ApiError::internal)?;
    state.last_snapshot_lsn.store(lsn, Ordering::Release);
    Ok(AdminSnapshotResponse {
        lsn,
        room_count,
        record_hot_table_count,
        record_hot_record_count,
    })
}

pub(crate) async fn compact_wal(
    State(state): State<AppState>,
) -> Result<Json<WalCompactResponse>, ApiError> {
    Ok(Json(run_compact_wal(&state).await?))
}

pub(crate) async fn run_compact_wal(state: &AppState) -> Result<WalCompactResponse, ApiError> {
    let last_snapshot_lsn = state.last_snapshot_lsn.load(Ordering::Acquire);
    if last_snapshot_lsn == 0 {
        return Err(ApiError::bad_request(
            "create a snapshot before compacting WAL",
        ));
    }
    let last_compaction_lsn = state.last_compaction_lsn.load(Ordering::Acquire);
    if last_snapshot_lsn <= last_compaction_lsn {
        return Ok(WalCompactResponse {
            reports: Vec::new(),
            archived: 0,
            retained: 0,
            last_snapshot_lsn,
        });
    }

    let mut reports = Vec::with_capacity(state.wal_shards.len());
    for shard in &state.wal_shards {
        let archive_dir = shard
            .path
            .parent()
            .map(|parent| parent.join("archive"))
            .unwrap_or_else(|| PathBuf::from("archive"));
        reports.push(
            shard
                .writer
                .compact(last_snapshot_lsn, archive_dir)
                .await
                .map_err(ApiError::internal)?,
        );
    }
    let archived = reports.iter().map(|report| report.archived).sum();
    let retained = reports.iter().map(|report| report.retained).sum();
    state
        .last_compaction_lsn
        .store(last_snapshot_lsn, Ordering::Release);
    Ok(WalCompactResponse {
        reports,
        archived,
        retained,
        last_snapshot_lsn,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ProjectionRebuildPhase {
    Idle,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectionRebuildStatus {
    pub(crate) phase: ProjectionRebuildPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    pub(crate) background: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) messages: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) records: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) object_refs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

impl Default for ProjectionRebuildStatus {
    fn default() -> Self {
        Self {
            phase: ProjectionRebuildPhase::Idle,
            run_id: None,
            background: false,
            started_at_ms: None,
            finished_at_ms: None,
            messages: None,
            records: None,
            object_refs: None,
            error: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectionRebuildRequest {
    #[serde(default)]
    pub(crate) background: bool,
}

#[derive(Debug, Clone)]
struct ProjectionRebuildCounts {
    messages: usize,
    records: usize,
    object_refs: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectionRebuildResponse {
    pub(crate) messages: usize,
    pub(crate) records: usize,
    pub(crate) object_refs: usize,
    pub(crate) phase: ProjectionRebuildPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    pub(crate) background: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

impl ProjectionRebuildResponse {
    fn from_status(status: ProjectionRebuildStatus) -> Self {
        Self {
            messages: status.messages.unwrap_or_default(),
            records: status.records.unwrap_or_default(),
            object_refs: status.object_refs.unwrap_or_default(),
            phase: status.phase,
            run_id: status.run_id,
            background: status.background,
            started_at_ms: status.started_at_ms,
            finished_at_ms: status.finished_at_ms,
            error: status.error,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalCompactResponse {
    pub(crate) reports: Vec<WalCompactReport>,
    pub(crate) archived: usize,
    pub(crate) retained: usize,
    pub(crate) last_snapshot_lsn: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeStoragePolicyResponse {
    pub(crate) hot_window: usize,
    pub(crate) max_hot_rooms: usize,
    pub(crate) schema: SchemaStoragePolicyReport,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeDrainRequest {
    pub(crate) draining: bool,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordHotPrewarmStatus {
    pub(crate) enabled: bool,
    pub(crate) limit_per_table: usize,
    pub(crate) last_started_at_ms: Option<u64>,
    pub(crate) last_finished_at_ms: Option<u64>,
    pub(crate) total_found: usize,
    pub(crate) total_activated: usize,
    pub(crate) tables: Vec<RecordHotPrewarmTableStatus>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordHotPrewarmTableStatus {
    pub(crate) table: String,
    pub(crate) found: usize,
    pub(crate) activated: usize,
    pub(crate) before_records: usize,
    pub(crate) after_records: usize,
}

pub(crate) fn spawn_record_hot_prewarm(state: AppState) {
    tokio::spawn(async move {
        if let Err(err) = run_record_hot_prewarm(&state).await {
            state.record_hot_prewarm.write().await.last_error = Some(err.message);
        }
    });
}

async fn run_record_hot_prewarm(state: &AppState) -> Result<(), ApiError> {
    let limit = state.record_hot_prewarm_limit;
    if limit == 0 {
        return Ok(());
    }

    {
        let mut status = state.record_hot_prewarm.write().await;
        status.enabled = true;
        status.limit_per_table = limit;
        status.last_started_at_ms = Some(now_ms());
        status.last_finished_at_ms = None;
        status.total_found = 0;
        status.total_activated = 0;
        status.tables.clear();
        status.last_error = None;
    }

    let hot_status = state.record_hot.status().await;
    let mut table_reports = Vec::new();
    let mut total_found = 0_usize;
    let mut total_activated = 0_usize;
    for table in hot_status.tables {
        let before = state.record_hot.status().await;
        let before_records = before
            .tables
            .iter()
            .find(|candidate| candidate.table == table.table)
            .map(|candidate| candidate.records)
            .unwrap_or(0);
        let mut records = state
            .records
            .list_recent(&table.table, Some(limit))
            .await
            .map_err(ApiError::internal)?;
        records.sort_by(|left, right| {
            left.lsn
                .cmp(&right.lsn)
                .then_with(|| left.updated_at_ms.cmp(&right.updated_at_ms))
                .then_with(|| left.key.cmp(&right.key))
        });
        let found = records.len();
        state.record_hot.hydrate_durable_many(&records).await;
        let after = state.record_hot.status().await;
        let after_records = after
            .tables
            .iter()
            .find(|candidate| candidate.table == table.table)
            .map(|candidate| candidate.records)
            .unwrap_or(0);
        let activated = after_records.saturating_sub(before_records);
        total_found += found;
        total_activated += activated;
        table_reports.push(RecordHotPrewarmTableStatus {
            table: table.table,
            found,
            activated,
            before_records,
            after_records,
        });
    }

    let mut status = state.record_hot_prewarm.write().await;
    status.last_finished_at_ms = Some(now_ms());
    status.total_found = total_found;
    status.total_activated = total_activated;
    status.tables = table_reports;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StartupRecoveryReport {
    pub(crate) snapshot_loaded: bool,
    pub(crate) snapshot_lsn: u64,
    pub(crate) snapshot_schema_version: Option<u32>,
    pub(crate) snapshot_room_count: usize,
    pub(crate) snapshot_record_hot_table_count: usize,
    pub(crate) snapshot_record_hot_record_count: usize,
    pub(crate) schema_wal_recovery: SchemaWalRecoveryReport,
    pub(crate) wal_restores: Vec<WalRestoreReport>,
    pub(crate) wal_replay: Vec<WalReplayReport>,
    pub(crate) wal_records_scanned: usize,
    pub(crate) wal_records_after_snapshot: usize,
    pub(crate) highest_lsn: u64,
    pub(crate) rebuilt_messages: usize,
    pub(crate) rebuilt_records: usize,
    pub(crate) rebuilt_object_refs: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaWalRecoveryReport {
    pub(crate) recovered: bool,
    pub(crate) latest_lsn: Option<u64>,
    pub(crate) latest_version: Option<u32>,
    pub(crate) history_versions: Vec<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalRestoreReport {
    pub(crate) primary: PathBuf,
    pub(crate) replicas_checked: Vec<PathBuf>,
    pub(crate) restored: bool,
    pub(crate) restored_from: Option<PathBuf>,
    pub(crate) archive_files_restored: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalReplayReport {
    pub(crate) shard: usize,
    pub(crate) path: PathBuf,
    pub(crate) since_lsn: u64,
    pub(crate) highest_lsn: u64,
    pub(crate) scanned_records: usize,
    pub(crate) records_after_snapshot: usize,
    pub(crate) quarantined_wal: Option<wal::WalQuarantineReport>,
}

pub(crate) struct RuntimeRecordActivationTarget {
    pub(crate) table: String,
    pub(crate) logical_table: String,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) key_prefix: Option<String>,
}

pub(crate) async fn prepare_runtime_restart(
    State(state): State<AppState>,
    Json(request): Json<RuntimePrepareRestartRequest>,
) -> Result<Json<RuntimePrepareRestartResponse>, ApiError> {
    Ok(Json(
        prepare_runtime_restart_internal(&state, request).await?,
    ))
}

pub(crate) async fn activate_runtime_records(
    State(state): State<AppState>,
    Json(request): Json<RuntimeRecordActivationRequest>,
) -> Result<Json<RuntimeRecordActivationResponse>, ApiError> {
    Ok(Json(
        activate_runtime_records_internal(&state, request).await?,
    ))
}

pub(crate) async fn activate_runtime_records_internal(
    state: &AppState,
    request: RuntimeRecordActivationRequest,
) -> Result<RuntimeRecordActivationResponse, ApiError> {
    let before = state.record_hot.status().await;
    let target = runtime_record_activation_target(state, &request)?;
    if !state.record_hot.is_hot_table(&target.logical_table).await {
        return Err(ApiError::bad_request(format!(
            "{} is not configured for resident, lru, actorPartition, or chatLog storage",
            target.logical_table
        )));
    }
    let keys = runtime_record_activation_keys(&request, &target)?;
    let mut found = 0_usize;
    let mut activated_records = Vec::new();
    let requested;
    if keys.is_empty() {
        let limit = normalize_limit(request.limit);
        let index_name = request
            .index_name
            .as_deref()
            .map(str::trim)
            .filter(|index_name| !index_name.is_empty())
            .map(ToString::to_string);
        if index_name.is_none() && runtime_record_activation_has_index_filter_options(&request) {
            return Err(ApiError::bad_request(
                "record activation index options require indexName",
            ));
        }
        let records = if let Some(index_name) = index_name {
            execute_record_index_query(
                state,
                target.table.clone(),
                target.parent_key.clone(),
                target.nested.clone(),
                index_name,
                QueryRecordsByIndexQuery {
                    consistency: Default::default(),
                    value: runtime_record_activation_index_param(&request.value)?,
                    values: runtime_record_activation_index_param(&request.values)?,
                    lower: runtime_record_activation_index_param(&request.lower)?,
                    upper: runtime_record_activation_index_param(&request.upper)?,
                    lower_values: runtime_record_activation_index_param(&request.lower_values)?,
                    upper_values: runtime_record_activation_index_param(&request.upper_values)?,
                    after_key: request.after_key.clone(),
                    after_cursor: request.after_cursor.clone(),
                    limit: Some(limit),
                    shard: None,
                    predicate: request.predicate.clone(),
                },
            )
            .await?
            .records
        } else if target.nested.is_some() {
            execute_record_list_query(
                state,
                target.table.clone(),
                target.parent_key.clone(),
                target.nested.clone(),
                ListRecordsQuery {
                    consistency: Default::default(),
                    after_key: request.after_key.clone(),
                    after_cursor: request.after_cursor.clone(),
                    limit: Some(limit),
                    order: request.order.clone(),
                    shard: None,
                    predicate: request.predicate.clone(),
                },
            )
            .await?
            .records
        } else {
            list_records_from_live_or_disk(
                state,
                &target.logical_table,
                request.after_key.as_deref(),
                limit,
            )
            .await?
        };
        requested = limit;
        found = records.len();
        activated_records = records;
    } else {
        requested = keys.len();
        for key in &keys {
            if let Some(record) =
                get_record_from_live_or_disk(state, &target.logical_table, key).await?
            {
                found += 1;
                activated_records.push(record);
            }
        }
    }
    let actor_scopes = activate_runtime_record_scopes(state, &target, activated_records).await;
    let actor_scope = actor_scopes.first().cloned();
    let after = state.record_hot.status().await;
    Ok(RuntimeRecordActivationResponse {
        table: target.logical_table,
        parent_key: target.parent_key,
        nested: target.nested,
        requested,
        found,
        activated: after.record_count.saturating_sub(before.record_count),
        evicted: before.record_count.saturating_sub(after.record_count),
        actor_scope,
        actor_scopes,
        before,
        after,
    })
}

async fn activate_runtime_record_scopes(
    state: &AppState,
    target: &RuntimeRecordActivationTarget,
    records: Vec<DbRecord>,
) -> Vec<actor::ScopeRowsActivationResult> {
    if records.is_empty() {
        return Vec::new();
    }
    let table_key = runtime_record_activation_table_key(&target.logical_table);
    let mut by_scope = BTreeMap::<String, Vec<DbRecord>>::new();
    for record in records {
        by_scope
            .entry(runtime_record_activation_scope_key(target, &record.key))
            .or_default()
            .push(record);
    }
    let mut activated = Vec::with_capacity(by_scope.len());
    for (scope_key, records) in by_scope {
        activated.push(
            state
                .actors
                .upsert_scope_rows(table_key.clone(), scope_key, records)
                .await,
        );
    }
    activated
}

fn runtime_record_activation_scope_key(
    target: &RuntimeRecordActivationTarget,
    record_key: &str,
) -> String {
    actor::record_actor_scope_key(&target.logical_table, record_key)
}

fn runtime_record_activation_table_key(logical_table: &str) -> String {
    actor::record_actor_table_key(logical_table)
}

pub(crate) async fn evict_runtime_records(
    State(state): State<AppState>,
    Json(request): Json<RuntimeRecordActivationRequest>,
) -> Result<Json<RuntimeRecordActivationResponse>, ApiError> {
    Ok(Json(evict_runtime_records_internal(&state, request).await?))
}

pub(crate) async fn evict_runtime_records_internal(
    state: &AppState,
    request: RuntimeRecordActivationRequest,
) -> Result<RuntimeRecordActivationResponse, ApiError> {
    let before = state.record_hot.status().await;
    let target = runtime_record_activation_target(state, &request)?;
    if !state.record_hot.is_lru_table(&target.logical_table).await {
        return Err(ApiError::bad_request(format!(
            "{} is not an lru table; resident, actorPartition, and chatLog tables are not manually evictable",
            target.logical_table
        )));
    }
    if runtime_record_activation_has_index_query_options(&request) {
        return Err(ApiError::bad_request(
            "record eviction does not support index query options; provide key, keys, or a hot key-order page",
        ));
    }
    let mut keys = runtime_record_activation_keys(&request, &target)?;
    if keys.is_empty() {
        let limit = normalize_limit(request.limit);
        keys = if let Some(prefix) = target.key_prefix.as_deref() {
            let after_key = request.after_key.as_deref().map(|key| {
                nested_record_key(target.parent_key.as_deref().unwrap_or_default(), key)
            });
            state
                .record_hot
                .list_by_key_prefix(
                    &target.logical_table,
                    prefix,
                    after_key.as_deref(),
                    Some(limit),
                )
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|record| record.key)
                .collect()
        } else {
            state
                .record_hot
                .list(
                    &target.logical_table,
                    request.after_key.as_deref(),
                    Some(limit),
                )
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|record| record.key)
                .collect()
        };
    }
    let requested = keys.len();
    let evicted = state
        .record_hot
        .evict_many(&target.logical_table, keys.iter().map(String::as_str))
        .await;
    let after = state.record_hot.status().await;
    Ok(RuntimeRecordActivationResponse {
        table: target.logical_table,
        parent_key: target.parent_key,
        nested: target.nested,
        requested,
        found: evicted,
        activated: after.record_count.saturating_sub(before.record_count),
        evicted,
        actor_scope: None,
        actor_scopes: Vec::new(),
        before,
        after,
    })
}

fn runtime_record_activation_target(
    state: &AppState,
    request: &RuntimeRecordActivationRequest,
) -> Result<RuntimeRecordActivationTarget, ApiError> {
    let table = request.table.trim().to_string();
    let nested = request
        .nested
        .as_ref()
        .map(|nested| nested.trim())
        .filter(|nested| !nested.is_empty())
        .map(ToString::to_string);
    let parent_key = request
        .parent_key
        .as_ref()
        .map(|parent_key| parent_key.trim())
        .filter(|parent_key| !parent_key.is_empty())
        .map(ToString::to_string);

    if let Some(nested) = nested {
        let parent_key = parent_key
            .ok_or_else(|| ApiError::bad_request("parentKey is required for nested activation"))?;
        validate_nested_table_path(&table, &parent_key, &nested, state)?;
        let logical_table = nested_record_table(&table, &nested);
        let key_prefix = nested_record_prefix(&parent_key);
        return Ok(RuntimeRecordActivationTarget {
            table,
            logical_table,
            parent_key: Some(parent_key),
            nested: Some(nested),
            key_prefix: Some(key_prefix),
        });
    }

    if parent_key.is_some() {
        return Err(ApiError::bad_request(
            "nested is required when parentKey is provided",
        ));
    }

    validate_table_path(&table, state)?;
    Ok(RuntimeRecordActivationTarget {
        logical_table: table.clone(),
        table,
        parent_key: None,
        nested: None,
        key_prefix: None,
    })
}

fn runtime_record_activation_keys(
    request: &RuntimeRecordActivationRequest,
    target: &RuntimeRecordActivationTarget,
) -> Result<Vec<String>, ApiError> {
    let mut keys = Vec::new();
    if let Some(key) = request
        .key
        .as_ref()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
    {
        keys.push(runtime_record_activation_logical_key(key, target)?);
    }
    for key in &request.keys {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let key = runtime_record_activation_logical_key(key, target)?;
        if !keys.iter().any(|existing| existing == &key) {
            keys.push(key);
        }
    }
    if !keys.is_empty()
        && (runtime_record_activation_has_index_query_options(request)
            || request.after_key.is_some()
            || request.order.is_some()
            || request.limit.is_some())
    {
        return Err(ApiError::bad_request(
            "record activation keys cannot be combined with index query options, afterKey, order, or limit",
        ));
    }
    Ok(keys)
}

fn runtime_record_activation_logical_key(
    key: &str,
    target: &RuntimeRecordActivationTarget,
) -> Result<String, ApiError> {
    if let Some(parent_key) = target.parent_key.as_deref() {
        if !ensure_safe_record_component(key) {
            return Err(ApiError::bad_request("invalid nested record key"));
        }
        return Ok(nested_record_key(parent_key, key));
    }
    if !ensure_safe_record_component(key) {
        return Err(ApiError::bad_request("invalid record key"));
    }
    Ok(key.to_string())
}

fn runtime_record_activation_has_index_query_options(
    request: &RuntimeRecordActivationRequest,
) -> bool {
    request
        .index_name
        .as_deref()
        .is_some_and(|index_name| !index_name.trim().is_empty())
        || runtime_record_activation_has_index_filter_options(request)
}

fn runtime_record_activation_has_index_filter_options(
    request: &RuntimeRecordActivationRequest,
) -> bool {
    request.value.is_some()
        || request.values.is_some()
        || request.lower.is_some()
        || request.upper.is_some()
        || request.lower_values.is_some()
        || request.upper_values.is_some()
        || request.after_cursor.is_some()
        || request.predicate.is_some()
}

fn runtime_record_activation_index_param(
    value: &Option<serde_json::Value>,
) -> Result<Option<String>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        serde_json::Value::String(value) => Ok(Some(value.clone())),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) | serde_json::Value::Null => {
            Ok(Some(value.to_string()))
        }
        serde_json::Value::Array(_) => serde_json::to_string(value)
            .map(Some)
            .map_err(|err| ApiError::bad_request(format!("invalid index parameter: {err}"))),
        serde_json::Value::Object(_) => Err(ApiError::bad_request(
            "runtime record index activation values must be scalar or array",
        )),
    }
}

pub(crate) fn runtime_record_activation_predicate_param(
    value: Option<serde_json::Value>,
) -> Result<Option<RecordPredicate>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let predicate = match value {
        serde_json::Value::String(text) => {
            if text.trim().is_empty() {
                return Ok(None);
            }
            serde_json::from_str::<RecordPredicate>(&text)
        }
        other => serde_json::from_value::<RecordPredicate>(other),
    }
    .map_err(|err| ApiError::bad_request(format!("invalid record predicate: {err}")))?;
    Ok(Some(predicate))
}

pub(crate) async fn activate_runtime_room(
    State(state): State<AppState>,
    Json(request): Json<RuntimeRoomActivationRequest>,
) -> Result<Json<RuntimeRoomActivationResponse>, ApiError> {
    Ok(Json(activate_runtime_room_internal(&state, request).await?))
}

pub(crate) async fn activate_runtime_room_internal(
    state: &AppState,
    request: RuntimeRoomActivationRequest,
) -> Result<RuntimeRoomActivationResponse, ApiError> {
    let room_id = request.room_id.trim().to_string();
    if room_id.is_empty() {
        return Err(ApiError::bad_request("roomId is required"));
    }
    let requested = normalize_limit(request.limit.or(Some(state.actors.hot_window())));
    let before_room_count = state.actors.room_count().await;
    let was_active = state.actors.has_room(&room_id).await;
    let messages = state
        .chat_log
        .latest(&room_id, None, requested)
        .await
        .map_err(ApiError::internal)?;
    if !messages.is_empty() {
        state.actors.apply_messages(messages.clone()).await;
    }
    let after_room_count = state.actors.room_count().await;
    let is_active = state.actors.has_room(&room_id).await;
    Ok(RuntimeRoomActivationResponse {
        room_id,
        requested,
        found: messages.len(),
        activated: !was_active && is_active,
        evicted: after_room_count < before_room_count + usize::from(!was_active && is_active),
        before_room_count,
        after_room_count,
        source: if messages.is_empty() {
            "missing"
        } else {
            "chatLog"
        },
    })
}

pub(crate) async fn activate_runtime_actor(
    State(state): State<AppState>,
    Json(request): Json<RuntimeActorActivationRequest>,
) -> Result<Json<RuntimeActorActivationResponse>, ApiError> {
    let key = request.key.trim().to_string();
    if key.is_empty() {
        return Err(ApiError::bad_request("key is required"));
    }
    let actor_id = ActorId {
        kind: request.kind,
        key,
    };
    let before = state.actors.kernel_status().await;
    let turn = state
        .actors
        .run_actor_turn(actor_id, ActorKernelMessage::Touch)
        .await;
    let after = state.actors.kernel_status().await;
    Ok(Json(RuntimeActorActivationResponse {
        actor_id: turn.actor_id,
        shard_index: turn.shard_index,
        activated: turn.created,
        turn_count: turn.turn_count,
        last_accessed_ms: turn.last_accessed_ms,
        before,
        after,
    }))
}

pub(crate) async fn schedule_actor_reminder(
    State(state): State<AppState>,
    Json(request): Json<RuntimeActorReminderScheduleRequest>,
) -> Result<Json<RuntimeActorReminderMutationResponse>, ApiError> {
    Ok(Json(
        schedule_actor_reminder_internal(&state, request).await?,
    ))
}

pub(crate) async fn cancel_actor_reminder(
    State(state): State<AppState>,
    Json(request): Json<RuntimeActorReminderCancelRequest>,
) -> Result<Json<RuntimeActorReminderCancelResponse>, ApiError> {
    Ok(Json(cancel_actor_reminder_internal(&state, request).await?))
}

pub(crate) async fn run_due_actor_reminders(
    State(state): State<AppState>,
    Json(request): Json<RuntimeActorReminderRunDueRequest>,
) -> Result<Json<RuntimeActorReminderRunDueResponse>, ApiError> {
    Ok(Json(
        run_due_actor_reminders_once(&state, request.limit, request.now_ms).await?,
    ))
}

pub(crate) fn actor_reminder_index_from_wal_records(
    wal_records: &[WalRecord],
) -> BTreeMap<ActorReminderIndexKey, ActorReminderRecord> {
    let mut ordered = wal_records.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|record| record.lsn);
    let mut reminders = BTreeMap::<ActorReminderIndexKey, ActorReminderRecord>::new();
    for wal_record in ordered {
        match &wal_record.payload {
            WalPayload::ActorReminderScheduled { reminder } => {
                reminders.insert(
                    actor_reminder_index_key_from_draft(reminder),
                    ActorReminderRecord {
                        reminder: reminder.clone(),
                        scheduled_lsn: wal_record.lsn,
                        scheduled_at_ms: wal_record.timestamp_ms,
                        status: ActorReminderRecordStatus::Scheduled,
                    },
                );
            }
            WalPayload::ActorReminderCancelled {
                actor_kind,
                actor_key,
                reminder_id,
                ..
            } => {
                if let Some(record) = reminders.get_mut(&actor_reminder_index_key_from_parts(
                    actor_kind,
                    actor_key,
                    reminder_id,
                )) {
                    record.status = ActorReminderRecordStatus::Cancelled;
                }
            }
            WalPayload::ActorReminderFired {
                actor_kind,
                actor_key,
                reminder_id,
                ..
            } => {
                if let Some(record) = reminders.get_mut(&actor_reminder_index_key_from_parts(
                    actor_kind,
                    actor_key,
                    reminder_id,
                )) {
                    record.status = ActorReminderRecordStatus::Fired;
                }
            }
            _ => {}
        }
    }
    reminders
}

pub(crate) async fn schedule_actor_reminder_internal(
    state: &AppState,
    request: RuntimeActorReminderScheduleRequest,
) -> Result<RuntimeActorReminderMutationResponse, ApiError> {
    let _guard = begin_runtime_write(state).await?;
    schedule_actor_reminder_after_runtime_write(state, request).await
}

async fn schedule_actor_reminder_after_runtime_write(
    state: &AppState,
    request: RuntimeActorReminderScheduleRequest,
) -> Result<RuntimeActorReminderMutationResponse, ApiError> {
    let accepted_at_ms = now_ms();
    let actor_id = runtime_actor_id(request.kind, request.key)?;
    let reminder_id = request
        .reminder_id
        .map(|reminder_id| reminder_id.trim().to_string())
        .filter(|reminder_id| !reminder_id.is_empty())
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let due_at_ms = request
        .due_at_ms
        .or_else(|| {
            request
                .delay_ms
                .map(|delay_ms| accepted_at_ms.saturating_add(delay_ms))
        })
        .ok_or_else(|| ApiError::bad_request("dueAtMs or delayMs is required"))?;
    if due_at_ms == 0 {
        return Err(ApiError::bad_request("dueAtMs must be greater than zero"));
    }
    RuntimeBehaviorContinuationPayload::from_reminder_payload(&request.payload, accepted_at_ms)?;
    let reminder = ActorReminderDraft {
        actor_kind: actor_id.kind.as_str().to_string(),
        actor_key: actor_id.key.clone(),
        reminder_id,
        due_at_ms,
        payload: request.payload,
    };
    if request.idempotency.is_some() {
        if let Some(existing) = existing_idempotent_actor_reminder(state, &reminder)? {
            let entry = actor_reminder_entry_from_draft(&existing.reminder)
                .ok_or_else(|| ApiError::bad_request("invalid actor reminder target"))?;
            return Ok(RuntimeActorReminderMutationResponse {
                reminder: entry,
                lsn: existing.scheduled_lsn,
                accepted_at_ms: existing.scheduled_at_ms,
                pending: state.actors.reminder_status(64),
            });
        }
    }
    let shard_key = actor_reminder_shard_key(&actor_id, &reminder.reminder_id);
    let shard = writable_wal_shard_for_key(state, &shard_key).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::ActorReminderScheduled {
            reminder: reminder.clone(),
        },
    )
    .await?;
    record_actor_reminder_scheduled(state, &reminder, wal_record.lsn, wal_record.timestamp_ms)?;
    note_host_http_callback_scheduled(state, &reminder)?;
    let entry = actor_reminder_entry_from_draft(&reminder)
        .ok_or_else(|| ApiError::bad_request("invalid actor reminder target"))?;
    state.actors.schedule_reminder(entry.clone());
    Ok(RuntimeActorReminderMutationResponse {
        reminder: entry,
        lsn: wal_record.lsn,
        accepted_at_ms,
        pending: state.actors.reminder_status(64),
    })
}

pub(crate) async fn cancel_actor_reminder_internal(
    state: &AppState,
    request: RuntimeActorReminderCancelRequest,
) -> Result<RuntimeActorReminderCancelResponse, ApiError> {
    let _guard = begin_runtime_write(state).await?;
    let accepted_at_ms = now_ms();
    let actor_id = runtime_actor_id(request.kind, request.key)?;
    let reminder_id = request.reminder_id.trim().to_string();
    if reminder_id.is_empty() {
        return Err(ApiError::bad_request("reminderId is required"));
    }
    let shard_key = actor_reminder_shard_key(&actor_id, &reminder_id);
    let shard = writable_wal_shard_for_key(state, &shard_key).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::ActorReminderCancelled {
            actor_kind: actor_id.kind.as_str().to_string(),
            actor_key: actor_id.key.clone(),
            reminder_id: reminder_id.clone(),
            cancelled_at_ms: accepted_at_ms,
        },
    )
    .await?;
    let cancelled = state.actors.cancel_reminder(&actor_id, &reminder_id);
    record_actor_reminder_cancelled(&state, &actor_id, &reminder_id)?;
    Ok(RuntimeActorReminderCancelResponse {
        actor_id,
        reminder_id,
        cancelled,
        lsn: wal_record.lsn,
        accepted_at_ms,
        pending: state.actors.reminder_status(64),
    })
}

pub(crate) async fn run_due_actor_reminders_once(
    state: &AppState,
    limit: Option<usize>,
    now_override_ms: Option<u64>,
) -> Result<RuntimeActorReminderRunDueResponse, ApiError> {
    let _guard = begin_runtime_write(state).await?;
    let checked_at_ms = now_override_ms.unwrap_or_else(now_ms);
    let requested = limit.unwrap_or(64).max(1);
    let due = state.actors.take_due_reminders(checked_at_ms, requested);
    let mut fired = Vec::with_capacity(due.len());
    let mut requeue = Vec::new();
    for reminder in due {
        let continuation = match RuntimeBehaviorContinuationPayload::from_reminder_payload(
            &reminder.payload,
            checked_at_ms,
        ) {
            Ok(continuation) => continuation,
            Err(err) => {
                requeue.push(reminder);
                state.actors.requeue_reminders(requeue);
                return Err(err);
            }
        };
        let turn = state
            .actors
            .run_actor_turn(
                reminder.actor_id.clone(),
                ActorKernelMessage::ReminderFired {
                    reminder_id: reminder.reminder_id.clone(),
                    payload: reminder.payload.clone(),
                },
            )
            .await;
        let (behavior, reply) = match continuation {
            Some(continuation) => {
                match invoke_behavior_internal(state, continuation.clone().into_behavior_request())
                    .await
                {
                    Ok(response) => {
                        let reply =
                            match continuation.reply_schedule_request(&response, checked_at_ms)? {
                                Some(request) => match schedule_actor_reminder_after_runtime_write(
                                    state, request,
                                )
                                .await
                                {
                                    Ok(response) => Some(response),
                                    Err(err) => {
                                        requeue.push(reminder);
                                        state.actors.requeue_reminders(requeue);
                                        return Err(err);
                                    }
                                },
                                None => None,
                            };
                        (Some(response), reply)
                    }
                    Err(err) => {
                        requeue.push(reminder);
                        state.actors.requeue_reminders(requeue);
                        return Err(err);
                    }
                }
            }
            None => (None, None),
        };
        let fired_at_ms = now_ms();
        let shard_key = actor_reminder_shard_key(&reminder.actor_id, &reminder.reminder_id);
        let shard = match writable_wal_shard_for_key(state, &shard_key).await {
            Ok(shard) => shard,
            Err(err) => {
                requeue.push(reminder);
                state.actors.requeue_reminders(requeue);
                return Err(err);
            }
        };
        match append_ordered_wal_record(
            state,
            shard,
            Durability::Strict,
            state.schema.version(),
            WalPayload::ActorReminderFired {
                actor_kind: reminder.actor_id.kind.as_str().to_string(),
                actor_key: reminder.actor_id.key.clone(),
                reminder_id: reminder.reminder_id.clone(),
                due_at_ms: reminder.due_at_ms,
                fired_at_ms,
            },
        )
        .await
        {
            Ok(wal_record) => {
                record_actor_reminder_fired(state, &reminder)?;
                fired.push(RuntimeActorReminderFireResult {
                    reminder,
                    turn,
                    behavior,
                    reply,
                    fired_lsn: wal_record.lsn,
                    fired_at_ms,
                });
            }
            Err(err) => {
                requeue.push(reminder);
                state.actors.requeue_reminders(requeue);
                return Err(err);
            }
        }
    }
    state
        .actors
        .record_reminder_sweep(checked_at_ms, fired.len());
    Ok(RuntimeActorReminderRunDueResponse {
        checked_at_ms,
        requested,
        fired,
        pending: state.actors.reminder_status(64),
        maintenance: state.actors.reminder_maintenance_status(),
    })
}

fn runtime_actor_id(kind: actor::ActorKind, key: String) -> Result<ActorId, ApiError> {
    let key = key.trim().to_string();
    if key.is_empty() {
        return Err(ApiError::bad_request("key is required"));
    }
    Ok(ActorId { kind, key })
}

fn actor_reminder_shard_key(actor_id: &ActorId, reminder_id: &str) -> String {
    format!(
        "actor-reminder:{}:{}:{}",
        actor_id.kind.as_str(),
        actor_id.key,
        reminder_id
    )
}

fn actor_reminder_index_key_from_draft(draft: &ActorReminderDraft) -> ActorReminderIndexKey {
    actor_reminder_index_key_from_parts(&draft.actor_kind, &draft.actor_key, &draft.reminder_id)
}

fn actor_reminder_index_key_from_entry(entry: &actor::ActorReminderEntry) -> ActorReminderIndexKey {
    actor_reminder_index_key_from_parts(
        entry.actor_id.kind.as_str(),
        &entry.actor_id.key,
        &entry.reminder_id,
    )
}

fn actor_reminder_index_key_from_parts(
    actor_kind: &str,
    actor_key: &str,
    reminder_id: &str,
) -> ActorReminderIndexKey {
    (
        actor_kind.trim().to_string(),
        actor_key.trim().to_string(),
        reminder_id.trim().to_string(),
    )
}

fn existing_idempotent_actor_reminder(
    state: &AppState,
    reminder: &ActorReminderDraft,
) -> Result<Option<ActorReminderRecord>, ApiError> {
    let existing = state
        .actor_reminders
        .read()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("actor reminder index poisoned")))?
        .get(&actor_reminder_index_key_from_draft(reminder))
        .cloned();
    let Some(existing) = existing else {
        return Ok(None);
    };
    if !actor_reminder_draft_matches(&existing.reminder, reminder) {
        return Err(ApiError::bad_request(
            "scheduleActorReminder reminderId was already used for a different reminder",
        ));
    }
    Ok(Some(existing))
}

fn actor_reminder_draft_matches(
    existing: &ActorReminderDraft,
    reminder: &ActorReminderDraft,
) -> bool {
    existing.actor_kind == reminder.actor_kind
        && existing.actor_key == reminder.actor_key
        && existing.reminder_id == reminder.reminder_id
        && existing.due_at_ms == reminder.due_at_ms
        && existing.payload == reminder.payload
}

fn record_actor_reminder_scheduled(
    state: &AppState,
    reminder: &ActorReminderDraft,
    scheduled_lsn: u64,
    scheduled_at_ms: u64,
) -> Result<(), ApiError> {
    state
        .actor_reminders
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("actor reminder index poisoned")))?
        .insert(
            actor_reminder_index_key_from_draft(reminder),
            ActorReminderRecord {
                reminder: reminder.clone(),
                scheduled_lsn,
                scheduled_at_ms,
                status: ActorReminderRecordStatus::Scheduled,
            },
        );
    Ok(())
}

fn record_actor_reminder_cancelled(
    state: &AppState,
    actor_id: &ActorId,
    reminder_id: &str,
) -> Result<(), ApiError> {
    if let Some(record) = state
        .actor_reminders
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("actor reminder index poisoned")))?
        .get_mut(&actor_reminder_index_key_from_parts(
            actor_id.kind.as_str(),
            &actor_id.key,
            reminder_id,
        ))
    {
        record.status = ActorReminderRecordStatus::Cancelled;
    }
    Ok(())
}

fn record_actor_reminder_fired(
    state: &AppState,
    reminder: &actor::ActorReminderEntry,
) -> Result<(), ApiError> {
    if let Some(record) = state
        .actor_reminders
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("actor reminder index poisoned")))?
        .get_mut(&actor_reminder_index_key_from_entry(reminder))
    {
        record.status = ActorReminderRecordStatus::Fired;
    }
    Ok(())
}

pub(crate) async fn evict_runtime_room(
    State(state): State<AppState>,
    Json(request): Json<RuntimeRoomActivationRequest>,
) -> Result<Json<RuntimeRoomActivationResponse>, ApiError> {
    Ok(Json(evict_runtime_room_internal(&state, request).await?))
}

pub(crate) async fn evict_runtime_room_internal(
    state: &AppState,
    request: RuntimeRoomActivationRequest,
) -> Result<RuntimeRoomActivationResponse, ApiError> {
    let room_id = request.room_id.trim().to_string();
    if room_id.is_empty() {
        return Err(ApiError::bad_request("roomId is required"));
    }
    let before_room_count = state.actors.room_count().await;
    let evicted = state.actors.evict_room(&room_id).await;
    let after_room_count = state.actors.room_count().await;
    Ok(RuntimeRoomActivationResponse {
        room_id,
        requested: 1,
        found: usize::from(evicted),
        activated: false,
        evicted,
        before_room_count,
        after_room_count,
        source: if evicted { "live" } else { "missing" },
    })
}

pub(crate) async fn prepare_runtime_restart_internal(
    state: &AppState,
    request: RuntimePrepareRestartRequest,
) -> Result<RuntimePrepareRestartResponse, ApiError> {
    let prepared_at_ms = now_ms();
    let wait_for_writes_ms = request
        .wait_for_writes_ms
        .unwrap_or(DEFAULT_RESTART_WRITE_WAIT_MS);
    let drain = set_runtime_drain_state(
        state,
        true,
        Some(
            request
                .reason
                .unwrap_or_else(|| "prepare restart".to_string()),
        ),
    )
    .await;
    let wait_started = Instant::now();
    let writes_quiesced = wait_for_runtime_writes(state, wait_for_writes_ms).await;
    let waited_for_writes_ms = elapsed_ms(wait_started);
    let runtime_writes = state.runtime_writes.snapshot();
    let write_wait_timed_out = !writes_quiesced;
    let snapshot = if request.snapshot.unwrap_or(true) && writes_quiesced {
        let current_lsn = state.current_lsn.load(Ordering::Acquire);
        Some(write_snapshot(state, current_lsn).await?)
    } else {
        None
    };
    let compact_wal = if request.compact_wal.unwrap_or(false) && writes_quiesced {
        Some(run_compact_wal(state).await?)
    } else {
        None
    };
    let ready_for_restart = writes_quiesced
        && (!request.snapshot.unwrap_or(true) || snapshot.is_some())
        && (!request.compact_wal.unwrap_or(false) || compact_wal.is_some());
    Ok(RuntimePrepareRestartResponse {
        drain,
        runtime_writes,
        writes_quiesced,
        write_wait_timed_out,
        waited_for_writes_ms,
        ready_for_restart,
        snapshot,
        compact_wal,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        prepared_at_ms,
    })
}

async fn wait_for_runtime_writes(state: &AppState, timeout_ms: u64) -> bool {
    if timeout_ms == 0 {
        return state.runtime_writes.snapshot().in_flight == 0;
    }
    let wait = state.runtime_write_gate.clone().write_owned();
    match tokio::time::timeout(Duration::from_millis(timeout_ms), wait).await {
        Ok(gate) => {
            drop(gate);
            true
        }
        Err(_) => false,
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

pub(crate) async fn shutdown_signal(state: AppState) {
    wait_for_shutdown_signal().await;
    match prepare_runtime_restart_internal(
        &state,
        RuntimePrepareRestartRequest {
            reason: Some("process shutdown signal".to_string()),
            snapshot: Some(true),
            compact_wal: Some(false),
            wait_for_writes_ms: Some(DEFAULT_RESTART_WRITE_WAIT_MS),
        },
    )
    .await
    {
        Ok(response) => {
            info!(
                current_lsn = response.current_lsn,
                snapshot_lsn = response.snapshot.as_ref().map(|snapshot| snapshot.lsn),
                "runtime prepared for graceful shutdown"
            );
        }
        Err(err) => {
            error!(
                error = %err.message,
                "failed to prepare runtime for graceful shutdown"
            );
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to install SIGTERM handler; falling back to Ctrl-C only"
                );
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_record_activation_scope_key_uses_top_level_buckets() {
        let target = RuntimeRecordActivationTarget {
            table: "rooms".to_string(),
            logical_table: "rooms".to_string(),
            parent_key: None,
            nested: None,
            key_prefix: None,
        };
        let bucket = crate::util::shard_index("room-a", 256);

        assert_eq!(
            runtime_record_activation_scope_key(&target, "room-a"),
            format!("table:rooms/bucket:{bucket:02x}")
        );
    }

    #[test]
    fn runtime_record_activation_scope_key_uses_nested_parent_partition() {
        let target = RuntimeRecordActivationTarget {
            table: "rooms".to_string(),
            logical_table: "rooms.messages".to_string(),
            parent_key: Some("room-a".to_string()),
            nested: Some("messages".to_string()),
            key_prefix: Some(nested_record_prefix("room-a")),
        };

        assert_eq!(
            runtime_record_activation_scope_key(&target, "room-a:message-1"),
            "table:rooms.messages/parent:room-a"
        );
    }

    #[test]
    fn behavior_continuation_payload_builds_behavior_request() {
        let payload = serde_json::json!({
            "type": "behaviorContinuation",
            "behavior": "matchmaking",
            "mutation": "tick",
            "userId": "system",
            "input": { "roomId": "room-a" },
            "callChainId": "chain-1",
            "callDepth": 1,
            "maxDepth": 4,
            "deadlineMs": 2000,
            "path": ["matchmaking.prepare"]
        });
        let continuation =
            RuntimeBehaviorContinuationPayload::from_reminder_payload(&Some(payload), 1000)
                .expect("parse continuation")
                .expect("continuation payload");
        let request = continuation.into_behavior_request();

        assert_eq!(request.behavior, "matchmaking");
        assert_eq!(request.mutation, "tick");
        assert_eq!(request.user_id.as_deref(), Some("system"));
        assert_eq!(request.input["roomId"], serde_json::json!("room-a"));
        assert_eq!(request.context["callChainId"], serde_json::json!("chain-1"));
        assert_eq!(request.context["callDepth"], serde_json::json!(2));
        assert_eq!(request.context["maxDepth"], serde_json::json!(4));
        assert_eq!(request.context["deadlineMs"], serde_json::json!(2000));
        assert_eq!(
            request.context["path"],
            serde_json::json!(["matchmaking.prepare", "matchmaking.tick"])
        );
    }

    #[test]
    fn behavior_continuation_reply_schedules_callback_with_response_input() {
        let continuation = RuntimeBehaviorContinuationPayload {
            payload_type: "behaviorContinuation".to_string(),
            behavior: "worker".to_string(),
            mutation: "run".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: None,
            input: Some(serde_json::json!({ "task": 1 })),
            read: None,
            context: None,
            reply_to: Some(RuntimeBehaviorContinuationReplyTarget {
                actor_kind: actor::ActorKind::Room,
                actor_key: "reply-room".to_string(),
                reminder_id: Some("reply-1".to_string()),
                continuation: Box::new(RuntimeBehaviorContinuationPayload {
                    payload_type: "behaviorContinuation".to_string(),
                    behavior: "reply".to_string(),
                    mutation: "done".to_string(),
                    user_id: None,
                    client_mutation_id: None,
                    input: Some(serde_json::json!({ "existing": true })),
                    read: None,
                    context: None,
                    reply_to: None,
                    call_chain_id: None,
                    call_depth: None,
                    max_depth: None,
                    deadline_ms: None,
                    path: None,
                }),
            }),
            call_chain_id: Some("chain-1".to_string()),
            call_depth: Some(2),
            max_depth: Some(8),
            deadline_ms: Some(10_000),
            path: Some(vec!["root.start".to_string()]),
        };
        let response = BehaviorInvokeResponse {
            output: crate::behavior::BehaviorInvokeOutput {
                commands: Vec::new(),
                result: serde_json::json!({ "ok": true }),
            },
            metadata: crate::behavior::BehaviorInvocationMetadata {
                behavior: "worker".to_string(),
                behavior_version: "v1".to_string(),
                epoch: 7,
            },
            committed: Vec::new(),
        };

        let request = continuation
            .reply_schedule_request(&response, 100)
            .expect("reply schedule request")
            .expect("reply target");
        assert_eq!(request.kind, actor::ActorKind::Room);
        assert_eq!(request.key, "reply-room");
        assert_eq!(request.reminder_id.as_deref(), Some("reply-1"));
        assert_eq!(request.due_at_ms, Some(100));
        assert_eq!(request.delay_ms, None);

        let payload = request.payload.expect("reply payload");
        assert_eq!(payload["type"], serde_json::json!("behaviorContinuation"));
        assert_eq!(payload["behavior"], serde_json::json!("reply"));
        assert_eq!(payload["mutation"], serde_json::json!("done"));
        assert_eq!(payload["callChainId"], serde_json::json!("chain-1"));
        assert_eq!(payload["callDepth"], serde_json::json!(3));
        assert_eq!(payload["maxDepth"], serde_json::json!(8));
        assert_eq!(payload["deadlineMs"], serde_json::json!(10_000));
        assert_eq!(
            payload["path"],
            serde_json::json!(["root.start", "worker.run"])
        );
        assert_eq!(payload["input"]["existing"], serde_json::json!(true));
        assert_eq!(
            payload["input"]["behaviorResponse"]["output"]["result"],
            serde_json::json!({ "ok": true })
        );
        assert_eq!(
            payload["input"]["behaviorResponse"]["metadata"]["behavior"],
            serde_json::json!("worker")
        );
    }

    #[test]
    fn behavior_continuation_payload_rejects_expired_depth_and_cycles() {
        let expired = serde_json::json!({
            "type": "behaviorContinuation",
            "behavior": "matchmaking",
            "mutation": "tick",
            "deadlineMs": 999
        });
        assert!(
            RuntimeBehaviorContinuationPayload::from_reminder_payload(&Some(expired), 1000)
                .is_err()
        );

        let too_deep = serde_json::json!({
            "type": "behaviorContinuation",
            "behavior": "matchmaking",
            "mutation": "tick",
            "callDepth": 4,
            "maxDepth": 4
        });
        assert!(
            RuntimeBehaviorContinuationPayload::from_reminder_payload(&Some(too_deep), 1000)
                .is_err()
        );

        let cycle = serde_json::json!({
            "type": "behaviorContinuation",
            "behavior": "matchmaking",
            "mutation": "tick",
            "path": ["matchmaking.tick"]
        });
        assert!(
            RuntimeBehaviorContinuationPayload::from_reminder_payload(&Some(cycle), 1000).is_err()
        );
    }

    fn wal_record(lsn: u64, payload: WalPayload) -> WalRecord {
        WalRecord {
            lsn,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: lsn * 100,
            schema_version: 1,
            durability: Durability::Strict,
            payload,
            checksum: None,
        }
    }

    fn reminder(reminder_id: &str, due_at_ms: u64) -> ActorReminderDraft {
        ActorReminderDraft {
            actor_kind: "room".to_string(),
            actor_key: "room-a".to_string(),
            reminder_id: reminder_id.to_string(),
            due_at_ms,
            payload: Some(serde_json::json!({ "tick": reminder_id })),
        }
    }

    #[test]
    fn actor_reminder_index_tracks_terminal_status_and_latest_schedule() {
        let cancelled = reminder("cancelled", 1000);
        let fired = reminder("fired", 2000);
        let rescheduled_initial = reminder("rescheduled", 3000);
        let rescheduled_latest = reminder("rescheduled", 4000);
        let records = vec![
            wal_record(
                1,
                WalPayload::ActorReminderScheduled {
                    reminder: cancelled.clone(),
                },
            ),
            wal_record(
                2,
                WalPayload::ActorReminderCancelled {
                    actor_kind: cancelled.actor_kind.clone(),
                    actor_key: cancelled.actor_key.clone(),
                    reminder_id: cancelled.reminder_id.clone(),
                    cancelled_at_ms: 200,
                },
            ),
            wal_record(
                3,
                WalPayload::ActorReminderScheduled {
                    reminder: fired.clone(),
                },
            ),
            wal_record(
                4,
                WalPayload::ActorReminderFired {
                    actor_kind: fired.actor_kind.clone(),
                    actor_key: fired.actor_key.clone(),
                    reminder_id: fired.reminder_id.clone(),
                    due_at_ms: fired.due_at_ms,
                    fired_at_ms: 400,
                },
            ),
            wal_record(
                5,
                WalPayload::ActorReminderScheduled {
                    reminder: rescheduled_initial,
                },
            ),
            wal_record(
                6,
                WalPayload::ActorReminderScheduled {
                    reminder: rescheduled_latest.clone(),
                },
            ),
        ];

        let index = actor_reminder_index_from_wal_records(&records);

        assert_eq!(
            index[&actor_reminder_index_key_from_draft(&cancelled)].status,
            ActorReminderRecordStatus::Cancelled
        );
        assert_eq!(
            index[&actor_reminder_index_key_from_draft(&fired)].status,
            ActorReminderRecordStatus::Fired
        );
        let rescheduled = &index[&actor_reminder_index_key_from_draft(&rescheduled_latest)];
        assert_eq!(rescheduled.status, ActorReminderRecordStatus::Scheduled);
        assert_eq!(rescheduled.scheduled_lsn, 6);
        assert_eq!(rescheduled.scheduled_at_ms, 600);
        assert_eq!(rescheduled.reminder.due_at_ms, 4000);
    }
}
