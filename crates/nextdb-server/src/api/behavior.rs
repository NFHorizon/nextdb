use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{
    Method,
    header::{HeaderMap as ReqwestHeaderMap, HeaderName, HeaderValue},
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    AppState,
    api::{
        audit::{
            AuditReplayQuery, AuditReplayResponse, AuditTraceKind, AuditTraceQuery,
            audit_replay_target, audit_trace_target, matches_audit_trace_target,
            replay_audit_target,
        },
        auth::{ensure_global_client_token_authorized, ensure_user_token_authorized},
        connections::{ConnectionDisconnectRequest, request_connection_disconnect},
        error::ApiError,
        events::publish_delivery_event,
        mutation::{
            MutateResponse, behavior_command_client_mutation_id, normalize_client_mutation_id,
            publish_user_event, send_message,
        },
        objects::{
            commit_object_delete, commit_object_put, validate_behavior_input_object_refs,
            validate_event_payload_object_refs,
        },
        realtime::{
            commit_realtime_channel_broadcast, commit_realtime_channel_state,
            commit_realtime_presence, publish_volatile_user_event,
        },
        records::{
            RecordTransactionOperationRequest, RecordTransactionOperationResponse,
            RecordTransactionRequest, commit_record_delete, commit_record_transaction,
            commit_record_upsert, get_record_from_live_or_disk, nested_record_key,
            nested_record_table, validate_nested_record_path, validate_record_path,
        },
        runtime::{
            ActorReminderIdempotency, RuntimeActorReminderMutationResponse,
            RuntimeActorReminderScheduleRequest, RuntimeRecordActivationRequest,
            RuntimeRecordActivationResponse, RuntimeRoomActivationRequest,
            RuntimeRoomActivationResponse, activate_runtime_records_internal,
            activate_runtime_room_internal, begin_runtime_write, ensure_runtime_accepting_writes,
            evict_runtime_records_internal, evict_runtime_room_internal,
            runtime_record_activation_predicate_param, schedule_actor_reminder_internal,
            validate_behavior_continuation_payload,
        },
        wal::{
            append_ordered_wal_record, ensure_shard_not_frozen, writable_wal_shard_for_index,
            writable_wal_shard_for_key,
        },
    },
    behavior::{
        BehaviorAuditReplayKind, BehaviorAuditReplayRead, BehaviorAuditTraceKind,
        BehaviorAuditTraceRead, BehaviorCommand, BehaviorConnectionSessionsRead,
        BehaviorInvocationMetadata, BehaviorInvokeOutput, BehaviorInvokeRequest,
        BehaviorLatestMessagesRead, BehaviorManifest, BehaviorNestedRecordRead, BehaviorObjectRead,
        BehaviorRealtimeChannelMembersRead, BehaviorRealtimeChannelStateRead, BehaviorRecordRead,
        BehaviorRecordTransactionOperation,
    },
    model::{
        BehaviorPublishedDraft, DbRecord, DeliveryEvent, Durability, HostHttpRequestDraft, Message,
        ObjectMetadata, UserEvent, WalPayload, WalRecord,
    },
    object_store::ensure_safe_object_id,
    realtime::{RealtimeChannelStateSnapshot, RealtimeMember},
    schema::{DatabaseSchema, FieldSchema, FieldType},
    util::{hex_lower, normalize_limit},
    wal::read_records_from_wal_paths,
};

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, Uri},
};

const HOST_HTTP_DEFAULT_TIMEOUT_MS: u64 = 5_000;
const HOST_HTTP_MAX_TIMEOUT_MS: u64 = 30_000;
const HOST_HTTP_MAX_HEADERS: usize = 32;
const HOST_HTTP_MAX_HEADER_BYTES: usize = 16 * 1024;
const HOST_HTTP_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;
const HOST_HTTP_MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const HOST_HTTP_REQUEST_ID_HEADER: &str = "x-nextdb-request-id";
const HOST_HTTP_IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BehaviorReloadResponse {
    pub(crate) loaded: usize,
    pub(crate) epoch: u64,
    pub(crate) published_lsn: u64,
}

pub(crate) async fn list_behaviors(State(state): State<AppState>) -> Json<Vec<BehaviorManifest>> {
    Json(state.behaviors.list().await)
}

pub(crate) async fn reload_behaviors(
    State(state): State<AppState>,
) -> Result<Json<BehaviorReloadResponse>, ApiError> {
    reload_behaviors_internal(&state).await.map(Json)
}

pub(crate) async fn reload_behaviors_internal(
    state: &AppState,
) -> Result<BehaviorReloadResponse, ApiError> {
    let _write = begin_runtime_write(state).await?;
    let active_schema = state.schema.schema();
    let plan = state
        .behaviors
        .prepare_reload_checked(state.behavior_root.clone(), |manifest| {
            validate_behavior_manifest_schema(&active_schema, manifest)
        })
        .await
        .map_err(ApiError::internal)?;
    let shard = writable_wal_shard_for_index(state, 0).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let publish = BehaviorPublishedDraft {
        epoch: plan.epoch(),
        loaded: plan.loaded_count(),
        manifests: plan.manifests().to_vec(),
        published_at_ms: crate::util::now_ms(),
    };
    let record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        active_schema.version,
        WalPayload::BehaviorPublished { publish },
    )
    .await?;
    let loaded = plan.loaded_count();
    let epoch = plan.epoch();
    state.behaviors.commit_reload(plan).await;
    Ok(BehaviorReloadResponse {
        loaded,
        epoch,
        published_lsn: record.lsn,
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BehaviorInvokeResponse {
    pub(crate) output: BehaviorInvokeOutput,
    pub(crate) metadata: BehaviorInvocationMetadata,
    pub(crate) committed: Vec<BehaviorCommittedResponse>,
}

#[derive(Debug, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum BehaviorCommittedResponse {
    MessageCreated {
        message: Message,
    },
    ObjectCommitted {
        object: ObjectMetadata,
    },
    ObjectDeleted {
        object_id: String,
        deleted: bool,
        lsn: u64,
    },
    UserEventPublished {
        event: UserEvent,
    },
    RecordUpserted {
        record: DbRecord,
    },
    RecordDeleted {
        table: String,
        key: String,
        lsn: u64,
    },
    RecordTransactionCommitted {
        lsn: u64,
        operations: Vec<RecordTransactionOperationResponse>,
    },
    RealtimeChannelStateUpdated {
        channel_id: String,
        state: RealtimeChannelStateSnapshot,
        sequence: u64,
        delivered: usize,
    },
    RealtimeChannelBroadcasted {
        channel_id: String,
        sequence: u64,
        delivered: usize,
    },
    RealtimePresenceUpdated {
        channel_id: String,
        members: Vec<RealtimeMember>,
        sequence: u64,
        delivered: usize,
    },
    VolatileUserPublished {
        user_id: String,
        name: String,
        delivered: usize,
    },
    ConnectionsDisconnectRequested {
        user_id: Option<String>,
        session_id: Option<String>,
        reason: String,
        targeted: usize,
        targeted_session_ids: Vec<String>,
    },
    RuntimeRecordsActivated {
        response: RuntimeRecordActivationResponse,
    },
    RuntimeRecordsEvicted {
        response: RuntimeRecordActivationResponse,
    },
    RuntimeRoomActivated {
        response: RuntimeRoomActivationResponse,
    },
    RuntimeRoomEvicted {
        response: RuntimeRoomActivationResponse,
    },
    ActorReminderScheduled {
        response: RuntimeActorReminderMutationResponse,
    },
    HostHttpRequested {
        request_id: String,
        method: String,
        url: String,
        actor_kind: String,
        actor_key: String,
        reminder_id: String,
        accepted_at_ms: u64,
        requested_lsn: u64,
    },
    VolatilePublished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostHttpRequestStatus {
    Requested,
    CallbackScheduled,
    Completed,
}

#[derive(Debug, Clone)]
pub(crate) struct HostHttpRequestRecord {
    pub(crate) request: HostHttpRequestDraft,
    pub(crate) requested_lsn: u64,
    pub(crate) status: HostHttpRequestStatus,
}

struct HostHttpAccepted {
    request_id: String,
    method: String,
    url: String,
    actor_kind: String,
    actor_key: String,
    reminder_id: String,
    accepted_at_ms: u64,
}

#[derive(Clone)]
struct HostHttpRequest {
    request_id: String,
    method: Method,
    url: String,
    raw_headers: BTreeMap<String, String>,
    headers: ReqwestHeaderMap,
    body: Option<HostHttpRequestBody>,
    body_base64: Option<String>,
    timeout_ms: u64,
    actor_kind: crate::actor::ActorKind,
    actor_key: String,
    reminder_id: String,
    continuation: Value,
}

#[derive(Clone)]
enum HostHttpRequestBody {
    Json(Value),
    Bytes(Vec<u8>),
}

impl HostHttpRequest {
    fn to_draft(&self, requested_at_ms: u64) -> HostHttpRequestDraft {
        HostHttpRequestDraft {
            request_id: self.request_id.clone(),
            method: self.method.as_str().to_string(),
            url: self.url.clone(),
            headers: self.raw_headers.clone(),
            body: match &self.body {
                Some(HostHttpRequestBody::Json(body)) => Some(body.clone()),
                Some(HostHttpRequestBody::Bytes(_)) | None => None,
            },
            body_base64: self.body_base64.clone(),
            timeout_ms: self.timeout_ms,
            actor_kind: self.actor_kind.as_str().to_string(),
            actor_key: self.actor_key.clone(),
            reminder_id: self.reminder_id.clone(),
            continuation: self.continuation.clone(),
            requested_at_ms,
        }
    }
}

fn spawn_host_http_request(state: AppState, request: HostHttpRequest) -> HostHttpAccepted {
    let accepted = HostHttpAccepted {
        request_id: request.request_id.clone(),
        method: request.method.as_str().to_string(),
        url: request.url.clone(),
        actor_kind: request.actor_kind.as_str().to_string(),
        actor_key: request.actor_key.clone(),
        reminder_id: request.reminder_id.clone(),
        accepted_at_ms: crate::util::now_ms(),
    };
    tokio::spawn(async move {
        let result = execute_host_http_request(&request).await;
        let payload = host_http_continuation_payload(
            request.continuation,
            &request.request_id,
            &request.method,
            &request.url,
            result,
        );
        let schedule = RuntimeActorReminderScheduleRequest {
            kind: request.actor_kind,
            key: request.actor_key,
            reminder_id: Some(request.reminder_id),
            due_at_ms: Some(crate::util::now_ms().max(1)),
            delay_ms: None,
            payload: Some(payload),
            idempotency: None,
        };
        if let Err(err) = schedule_actor_reminder_internal(&state, schedule).await {
            tracing::warn!(
                error = ?err,
                "failed to schedule host HTTP behavior continuation"
            );
            return;
        }
        if let Err(err) = append_host_http_completed(&state, &request.request_id).await {
            tracing::warn!(error = ?err, "failed to append host HTTP completion fact");
        }
    });
    accepted
}

async fn append_host_http_completed(state: &AppState, request_id: &str) -> Result<(), ApiError> {
    let shard = writable_wal_shard_for_key(state, request_id).await?;
    append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::HostHttpCompleted {
            request_id: request_id.to_string(),
            completed_at_ms: crate::util::now_ms(),
        },
    )
    .await?;
    if let Some(record) = state
        .host_http_requests
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("host HTTP request index poisoned")))?
        .get_mut(request_id)
    {
        record.status = HostHttpRequestStatus::Completed;
    }
    Ok(())
}

pub(crate) fn pending_host_http_requests_from_wal_records(
    wal_records: &[WalRecord],
) -> Vec<HostHttpRequestDraft> {
    host_http_request_index_from_wal_records(wal_records)
        .into_values()
        .filter(|record| record.status == HostHttpRequestStatus::Requested)
        .map(|record| record.request)
        .collect()
}

pub(crate) fn host_http_request_index_from_wal_records(
    wal_records: &[WalRecord],
) -> BTreeMap<String, HostHttpRequestRecord> {
    let mut ordered = wal_records.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|record| record.lsn);
    let mut requests = BTreeMap::<String, HostHttpRequestRecord>::new();
    for wal_record in ordered {
        match &wal_record.payload {
            WalPayload::HostHttpRequested { request } => {
                requests.insert(
                    request.request_id.clone(),
                    HostHttpRequestRecord {
                        request: request.clone(),
                        requested_lsn: wal_record.lsn,
                        status: HostHttpRequestStatus::Requested,
                    },
                );
            }
            WalPayload::HostHttpCompleted { request_id, .. } => {
                if let Some(record) = requests.get_mut(request_id) {
                    record.status = HostHttpRequestStatus::Completed;
                }
            }
            WalPayload::ActorReminderScheduled { reminder } => {
                for record in requests.values_mut() {
                    if record.request.actor_kind == reminder.actor_kind
                        && record.request.actor_key == reminder.actor_key
                        && record.request.reminder_id == reminder.reminder_id
                        && record.status == HostHttpRequestStatus::Requested
                    {
                        record.status = HostHttpRequestStatus::CallbackScheduled;
                    }
                }
            }
            _ => {}
        }
    }
    requests
}

pub(crate) fn replay_pending_host_http_requests(
    state: &AppState,
    requests: Vec<HostHttpRequestDraft>,
) -> usize {
    let mut replayed = 0;
    for request in requests {
        match host_http_request_from_draft(request) {
            Ok(request) => {
                spawn_host_http_request(state.clone(), request);
                replayed += 1;
            }
            Err(err) => {
                tracing::warn!(error = ?err, "skipping invalid pending host HTTP request");
            }
        }
    }
    replayed
}

fn host_http_request_from_draft(draft: HostHttpRequestDraft) -> Result<HostHttpRequest, ApiError> {
    let actor_kind = crate::actor::ActorKind::from_wal_str(&draft.actor_kind)
        .ok_or_else(|| ApiError::bad_request("invalid pending host HTTP actorKind"))?;
    parse_host_http_request(
        Some(draft.request_id),
        draft.method,
        draft.url,
        draft.headers,
        draft.body,
        draft.body_base64,
        Some(draft.timeout_ms),
        actor_kind,
        draft.actor_key,
        Some(draft.reminder_id),
        draft.continuation,
    )
}

async fn append_host_http_requested(
    state: &AppState,
    request: &HostHttpRequest,
) -> Result<(u64, HostHttpRequestDraft), ApiError> {
    let requested_at_ms = crate::util::now_ms();
    let draft = request.to_draft(requested_at_ms);
    let shard = writable_wal_shard_for_key(state, &request.request_id).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::HostHttpRequested {
            request: draft.clone(),
        },
    )
    .await?;
    state
        .host_http_requests
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("host HTTP request index poisoned")))?
        .insert(
            draft.request_id.clone(),
            HostHttpRequestRecord {
                request: draft.clone(),
                requested_lsn: wal_record.lsn,
                status: HostHttpRequestStatus::Requested,
            },
        );
    Ok((wal_record.lsn, draft))
}

async fn execute_host_http_request(request: &HostHttpRequest) -> Value {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(request.timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return serde_json::json!({
                "ok": false,
                "error": format!("create HTTP client: {err}"),
            });
        }
    };
    let mut builder = client
        .request(request.method.clone(), request.url.clone())
        .headers(host_http_execution_headers(request));
    builder = match &request.body {
        Some(HostHttpRequestBody::Json(body)) => builder.json(body),
        Some(HostHttpRequestBody::Bytes(body)) => builder.body(body.clone()),
        None => builder,
    };
    let response = match builder.send().await {
        Ok(response) => response,
        Err(err) => {
            return serde_json::json!({
                "ok": false,
                "error": err.to_string(),
            });
        }
    };
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(err) => {
            return serde_json::json!({
                "ok": false,
                "status": status,
                "headers": headers,
                "error": format!("read response body: {err}"),
            });
        }
    };
    if body.len() > HOST_HTTP_MAX_RESPONSE_BODY_BYTES {
        return serde_json::json!({
            "ok": false,
            "status": status,
            "headers": headers,
            "error": format!("response body exceeded {HOST_HTTP_MAX_RESPONSE_BODY_BYTES} bytes"),
        });
    }
    let body_text = String::from_utf8_lossy(&body).to_string();
    serde_json::json!({
        "ok": (200..=299).contains(&status),
        "status": status,
        "headers": headers,
        "bodyText": body_text,
    })
}

fn host_http_continuation_payload(
    mut continuation: Value,
    request_id: &str,
    method: &Method,
    url: &str,
    result: Value,
) -> Value {
    let host_http = serde_json::json!({
        "requestId": request_id,
        "method": method.as_str(),
        "url": url,
        "result": result,
    });
    let prior_input = continuation
        .as_object_mut()
        .and_then(|object| object.remove("input"))
        .unwrap_or(Value::Null);
    if let Some(object) = continuation.as_object_mut() {
        object.insert(
            "input".to_string(),
            serde_json::json!({
                "hostHttp": host_http,
                "input": prior_input,
            }),
        );
    }
    continuation
}

fn parse_host_http_request(
    request_id: Option<String>,
    method: String,
    url: String,
    headers: BTreeMap<String, String>,
    body: Option<Value>,
    body_base64: Option<String>,
    timeout_ms: Option<u64>,
    actor_kind: crate::actor::ActorKind,
    actor_key: String,
    reminder_id: Option<String>,
    continuation: Value,
) -> Result<HostHttpRequest, ApiError> {
    let request_id = request_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
    if request_id.len() > 128 {
        return Err(ApiError::bad_request(
            "requestHostHttp requestId must be at most 128 characters",
        ));
    }
    let method = parse_host_http_method(&method)?;
    if url.trim().is_empty() {
        return Err(ApiError::bad_request("requestHostHttp url is required"));
    }
    if !url.starts_with("https://") && !url.starts_with("http://") {
        return Err(ApiError::bad_request(
            "requestHostHttp url must start with http:// or https://",
        ));
    }
    if actor_key.trim().is_empty() {
        return Err(ApiError::bad_request(
            "requestHostHttp actorKey is required",
        ));
    }
    validate_behavior_continuation_payload(&continuation, crate::util::now_ms())?;
    let reminder_id = reminder_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("host-http-{request_id}"));
    if reminder_id.len() > 160 {
        return Err(ApiError::bad_request(
            "requestHostHttp reminderId must be at most 160 characters",
        ));
    }
    let raw_headers = headers.clone();
    let raw_body_base64 = body_base64.clone();
    let headers = parse_host_http_headers(headers)?;
    let body = parse_host_http_body(body, body_base64)?;
    let timeout_ms = timeout_ms
        .unwrap_or(HOST_HTTP_DEFAULT_TIMEOUT_MS)
        .clamp(1, HOST_HTTP_MAX_TIMEOUT_MS);
    Ok(HostHttpRequest {
        request_id,
        method,
        url,
        raw_headers,
        headers,
        body,
        body_base64: raw_body_base64,
        timeout_ms,
        actor_kind,
        actor_key,
        reminder_id,
        continuation,
    })
}

fn host_http_request_matches_draft(
    request: &HostHttpRequest,
    draft: &HostHttpRequestDraft,
) -> bool {
    request.method.as_str() == draft.method
        && request.url == draft.url
        && request.raw_headers == draft.headers
        && request.body_base64 == draft.body_base64
        && match (&request.body, &draft.body) {
            (Some(HostHttpRequestBody::Json(left)), Some(right)) => left == right,
            (Some(HostHttpRequestBody::Bytes(_)), None) | (None, None) => true,
            _ => false,
        }
        && request.timeout_ms == draft.timeout_ms
        && request.actor_kind.as_str() == draft.actor_kind
        && request.actor_key == draft.actor_key
        && request.reminder_id == draft.reminder_id
        && request.continuation == draft.continuation
}

fn existing_host_http_request(
    state: &AppState,
    request: &HostHttpRequest,
) -> Result<Option<HostHttpRequestRecord>, ApiError> {
    let existing = state
        .host_http_requests
        .read()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("host HTTP request index poisoned")))?
        .get(&request.request_id)
        .cloned();
    let Some(existing) = existing else {
        return Ok(None);
    };
    if !host_http_request_matches_draft(request, &existing.request) {
        return Err(ApiError::bad_request(
            "requestHostHttp requestId was already used for a different request",
        ));
    }
    Ok(Some(existing))
}

pub(crate) fn note_host_http_callback_scheduled(
    state: &AppState,
    reminder: &crate::model::ActorReminderDraft,
) -> Result<(), ApiError> {
    let mut requests = state
        .host_http_requests
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("host HTTP request index poisoned")))?;
    for record in requests.values_mut() {
        if record.request.actor_kind == reminder.actor_kind
            && record.request.actor_key == reminder.actor_key
            && record.request.reminder_id == reminder.reminder_id
            && record.status == HostHttpRequestStatus::Requested
        {
            record.status = HostHttpRequestStatus::CallbackScheduled;
        }
    }
    Ok(())
}

fn parse_host_http_method(method: &str) -> Result<Method, ApiError> {
    let normalized = method.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" => {
            Method::from_bytes(normalized.as_bytes()).map_err(|err| {
                ApiError::bad_request(format!("invalid requestHostHttp method: {err}"))
            })
        }
        _ => Err(ApiError::bad_request(
            "requestHostHttp method must be one of GET, POST, PUT, PATCH, DELETE, HEAD",
        )),
    }
}

fn parse_host_http_headers(
    headers: BTreeMap<String, String>,
) -> Result<ReqwestHeaderMap, ApiError> {
    if headers.len() > HOST_HTTP_MAX_HEADERS {
        return Err(ApiError::bad_request(format!(
            "requestHostHttp headers must contain at most {HOST_HTTP_MAX_HEADERS} entries"
        )));
    }
    let mut total_bytes = 0usize;
    let mut out = ReqwestHeaderMap::new();
    for (name, value) in headers {
        total_bytes = total_bytes
            .saturating_add(name.len())
            .saturating_add(value.len());
        if total_bytes > HOST_HTTP_MAX_HEADER_BYTES {
            return Err(ApiError::bad_request(format!(
                "requestHostHttp headers exceeded {HOST_HTTP_MAX_HEADER_BYTES} bytes"
            )));
        }
        let lower = name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "host" | "content-length" | "connection" | HOST_HTTP_REQUEST_ID_HEADER
        ) {
            return Err(ApiError::bad_request(format!(
                "requestHostHttp header '{name}' is managed by the host"
            )));
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|err| {
            ApiError::bad_request(format!("invalid requestHostHttp header name: {err}"))
        })?;
        let value = HeaderValue::from_str(&value).map_err(|err| {
            ApiError::bad_request(format!("invalid requestHostHttp header value: {err}"))
        })?;
        out.insert(name, value);
    }
    Ok(out)
}

fn host_http_execution_headers(request: &HostHttpRequest) -> ReqwestHeaderMap {
    let mut headers = request.headers.clone();
    if let Ok(value) = HeaderValue::from_str(&request.request_id) {
        headers.insert(HeaderName::from_static(HOST_HTTP_REQUEST_ID_HEADER), value);
    }
    if !headers.contains_key(HOST_HTTP_IDEMPOTENCY_KEY_HEADER)
        && let Ok(value) = HeaderValue::from_str(&request.request_id)
    {
        headers.insert(
            HeaderName::from_static(HOST_HTTP_IDEMPOTENCY_KEY_HEADER),
            value,
        );
    }
    headers
}

fn parse_host_http_body(
    body: Option<Value>,
    body_base64: Option<String>,
) -> Result<Option<HostHttpRequestBody>, ApiError> {
    match (body, body_base64) {
        (Some(_), Some(_)) => Err(ApiError::bad_request(
            "requestHostHttp body and bodyBase64 are mutually exclusive",
        )),
        (Some(body), None) => {
            let encoded = serde_json::to_vec(&body)
                .map_err(|err| ApiError::internal(anyhow::anyhow!(err)))?;
            if encoded.len() > HOST_HTTP_MAX_REQUEST_BODY_BYTES {
                return Err(ApiError::bad_request(format!(
                    "requestHostHttp JSON body exceeded {HOST_HTTP_MAX_REQUEST_BODY_BYTES} bytes"
                )));
            }
            Ok(Some(HostHttpRequestBody::Json(body)))
        }
        (None, Some(body_base64)) => {
            let body = BASE64_STANDARD
                .decode(body_base64.as_bytes())
                .map_err(|err| {
                    ApiError::bad_request(format!("invalid requestHostHttp bodyBase64: {err}"))
                })?;
            if body.len() > HOST_HTTP_MAX_REQUEST_BODY_BYTES {
                return Err(ApiError::bad_request(format!(
                    "requestHostHttp bodyBase64 decoded body exceeded {HOST_HTTP_MAX_REQUEST_BODY_BYTES} bytes"
                )));
            }
            Ok(Some(HostHttpRequestBody::Bytes(body)))
        }
        (None, None) => Ok(None),
    }
}

pub(crate) fn validate_behavior_manifest_schema(
    schema: &DatabaseSchema,
    manifest: &BehaviorManifest,
) -> anyhow::Result<()> {
    if manifest.inputs.is_empty() {
        return Ok(());
    }
    let behavior_schema = schema
        .behaviors
        .get(&manifest.name)
        .ok_or_else(|| anyhow::anyhow!("behavior '{}' is not declared in schema", manifest.name))?;
    for (mutation, manifest_input) in &manifest.inputs {
        let schema_input = behavior_schema.mutations.get(mutation).ok_or_else(|| {
            anyhow::anyhow!(
                "behavior '{}.{}' input is not declared in schema",
                manifest.name,
                mutation
            )
        })?;
        if let Some(reason) = behavior_input_schema_incompatibility(manifest_input, schema_input) {
            anyhow::bail!(
                "behavior '{}.{}' manifest input does not match active schema: {}",
                manifest.name,
                mutation,
                reason
            );
        }
    }
    Ok(())
}

pub(crate) async fn validate_loaded_behavior_manifests_schema(
    state: &AppState,
    schema: &DatabaseSchema,
) -> Result<(), ApiError> {
    for manifest in state.behaviors.list().await {
        validate_behavior_manifest_schema(schema, &manifest)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    }
    Ok(())
}

fn behavior_input_schema_incompatibility(
    manifest_input: &FieldSchema,
    schema_input: &FieldSchema,
) -> Option<String> {
    behavior_field_schema_incompatibility("input", manifest_input, schema_input)
}

fn behavior_field_schema_incompatibility(
    path: &str,
    manifest_field: &FieldSchema,
    schema_field: &FieldSchema,
) -> Option<String> {
    if manifest_field.optional != schema_field.optional {
        return Some(format!(
            "{path} optional flag differs: manifest={}, schema={}",
            manifest_field.optional, schema_field.optional
        ));
    }
    behavior_field_type_incompatibility(path, &manifest_field.field_type, &schema_field.field_type)
}

fn behavior_field_type_incompatibility(
    path: &str,
    manifest_type: &FieldType,
    schema_type: &FieldType,
) -> Option<String> {
    match (manifest_type, schema_type) {
        (
            FieldType::Object {
                fields: manifest_fields,
            },
            FieldType::Object {
                fields: schema_fields,
            },
        ) => {
            for (field_name, manifest_field) in manifest_fields {
                let Some(schema_field) = schema_fields.get(field_name) else {
                    return Some(format!("{path}.{field_name} is missing from schema"));
                };
                if let Some(reason) = behavior_field_schema_incompatibility(
                    &format!("{path}.{field_name}"),
                    manifest_field,
                    schema_field,
                ) {
                    return Some(reason);
                }
            }
            for (field_name, schema_field) in schema_fields {
                if !manifest_fields.contains_key(field_name) && !schema_field.optional {
                    return Some(format!(
                        "{path}.{field_name} is required by schema but absent from manifest"
                    ));
                }
            }
            None
        }
        (
            FieldType::List {
                item: manifest_item,
            },
            FieldType::List { item: schema_item },
        ) => behavior_field_type_incompatibility(&format!("{path}[]"), manifest_item, schema_item),
        _ if manifest_type == schema_type => None,
        _ => Some(format!(
            "{path} type differs: manifest={manifest_type:?}, schema={schema_type:?}"
        )),
    }
}

pub(crate) async fn invoke_behavior(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<BehaviorInvokeRequest>,
) -> Result<Json<BehaviorInvokeResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    if let Some(user_id) = request.user_id.as_deref() {
        ensure_user_token_authorized(&state, &headers, &uri, user_id)?;
    } else {
        ensure_global_client_token_authorized(&state, &headers, &uri)?;
    }
    Ok(Json(invoke_behavior_internal(&state, request).await?))
}

pub(crate) async fn invoke_behavior_internal(
    state: &AppState,
    mut request: BehaviorInvokeRequest,
) -> Result<BehaviorInvokeResponse, ApiError> {
    state
        .schema
        .validate_behavior_input(&request.behavior, &request.mutation, &request.input)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    validate_behavior_input_object_refs(
        &state,
        &request.behavior,
        &request.mutation,
        &request.input,
    )
    .await?;
    let behavior_client_mutation_id =
        normalize_client_mutation_id(request.client_mutation_id.clone())?;
    if behavior_client_mutation_id
        .as_ref()
        .is_some_and(|id| id.len() > 128)
    {
        return Err(ApiError::bad_request(
            "behavior clientMutationId must be at most 128 characters",
        ));
    }
    request.client_mutation_id = behavior_client_mutation_id.clone();
    state
        .behaviors
        .validate_read_capabilities(&request.behavior, &request.read)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    hydrate_behavior_request_context(&state, &mut request).await?;

    let invoke_result = state
        .behaviors
        .invoke(request.clone())
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let output = invoke_result.output;
    let metadata = invoke_result.metadata;
    state
        .behaviors
        .validate_command_scopes(&request.behavior, &output.commands)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    if behavior_client_mutation_id.is_some()
        && output.commands.iter().any(|command| {
            matches!(
                command,
                BehaviorCommand::PublishVolatile { .. }
                    | BehaviorCommand::PublishUserVolatile { .. }
                    | BehaviorCommand::BroadcastRealtimeChannel { .. }
                    | BehaviorCommand::UpdateRealtimePresence { .. }
                    | BehaviorCommand::UpdateRealtimeChannelState { .. }
                    | BehaviorCommand::DisconnectConnections { .. }
                    | BehaviorCommand::ActivateRuntimeRecords { .. }
                    | BehaviorCommand::EvictRuntimeRecords { .. }
                    | BehaviorCommand::ActivateRuntimeRoom { .. }
                    | BehaviorCommand::EvictRuntimeRoom { .. }
            )
        })
    {
        return Err(ApiError::bad_request(
            "behavior clientMutationId requires replay-safe durable host commands; publishVolatile, publishUserVolatile, broadcastRealtimeChannel, updateRealtimePresence, updateRealtimeChannelState, disconnectConnections, and runtime activation commands are not retry-safe",
        ));
    }

    let mut committed = Vec::new();
    for (command_index, command) in output.commands.clone().into_iter().enumerate() {
        match command {
            BehaviorCommand::SendMessage {
                room_id,
                body,
                attachments,
                durability,
            } => {
                let user_id = request.user_id.clone().ok_or_else(|| {
                    ApiError::bad_request("behavior sendMessage command requires userId")
                })?;
                let response = send_message(
                    state.clone(),
                    room_id,
                    user_id,
                    body,
                    attachments,
                    durability,
                    behavior_command_client_mutation_id(
                        behavior_client_mutation_id.as_deref(),
                        command_index,
                        "sendMessage",
                    ),
                )
                .await?;
                let MutateResponse::MessageCreated { message } = response.0 else {
                    return Err(ApiError::internal(anyhow::anyhow!(
                        "behavior sendMessage returned an unexpected response"
                    )));
                };
                committed.push(BehaviorCommittedResponse::MessageCreated { message });
            }
            BehaviorCommand::PublishVolatile {
                room_id,
                name,
                payload,
            } => {
                state
                    .schema
                    .validate_event_payload(&name, &payload)
                    .map_err(|err| ApiError::bad_request(err.to_string()))?;
                validate_event_payload_object_refs(&state, &name, &payload).await?;
                publish_delivery_event(
                    &state,
                    DeliveryEvent::VolatileRoomEvent {
                        room_id,
                        name,
                        payload,
                    },
                );
                committed.push(BehaviorCommittedResponse::VolatilePublished);
            }
            BehaviorCommand::PublishUserVolatile {
                user_id,
                name,
                payload,
            } => {
                let delivered =
                    publish_volatile_user_event(&state, &user_id, name.clone(), payload).await?;
                committed.push(BehaviorCommittedResponse::VolatileUserPublished {
                    user_id,
                    name,
                    delivered,
                });
            }
            BehaviorCommand::PublishUserEvent {
                user_id,
                name,
                payload,
                durability,
                client_mutation_id,
            } => {
                let response = publish_user_event(
                    state.clone(),
                    user_id,
                    name,
                    payload,
                    durability,
                    client_mutation_id.or_else(|| {
                        behavior_command_client_mutation_id(
                            behavior_client_mutation_id.as_deref(),
                            command_index,
                            "publishUserEvent",
                        )
                    }),
                )
                .await?;
                let MutateResponse::UserEventPublished { event } = response.0 else {
                    return Err(ApiError::internal(anyhow::anyhow!(
                        "behavior publishUserEvent returned an unexpected response"
                    )));
                };
                committed.push(BehaviorCommittedResponse::UserEventPublished { event });
            }
            BehaviorCommand::PutObject {
                body_base64,
                content_type,
                object_id,
                client_mutation_id,
            } => {
                let body = BASE64_STANDARD
                    .decode(body_base64.as_bytes())
                    .map_err(|err| {
                        ApiError::bad_request(format!("invalid object bodyBase64: {err}"))
                    })?;
                let object = commit_object_put(
                    &state,
                    object_id,
                    content_type,
                    Bytes::from(body),
                    client_mutation_id.or_else(|| {
                        behavior_command_client_mutation_id(
                            behavior_client_mutation_id.as_deref(),
                            command_index,
                            "putObject",
                        )
                    }),
                )
                .await?;
                committed.push(BehaviorCommittedResponse::ObjectCommitted { object });
            }
            BehaviorCommand::DeleteObject {
                object_id,
                force,
                client_mutation_id,
            } => {
                let response = commit_object_delete(
                    &state,
                    object_id,
                    force.unwrap_or(false),
                    client_mutation_id.or_else(|| {
                        behavior_command_client_mutation_id(
                            behavior_client_mutation_id.as_deref(),
                            command_index,
                            "deleteObject",
                        )
                    }),
                )
                .await?;
                committed.push(BehaviorCommittedResponse::ObjectDeleted {
                    object_id: response.object_id,
                    deleted: response.deleted,
                    lsn: response.lsn,
                });
            }
            BehaviorCommand::UpsertRecord {
                table,
                key,
                value,
                durability,
                expected_lsn,
            } => {
                let record = commit_record_upsert(
                    &state,
                    table,
                    key,
                    value,
                    durability,
                    expected_lsn,
                    behavior_command_client_mutation_id(
                        behavior_client_mutation_id.as_deref(),
                        command_index,
                        "upsertRecord",
                    ),
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RecordUpserted { record });
            }
            BehaviorCommand::DeleteRecord {
                table,
                key,
                durability,
                expected_lsn,
            } => {
                let response = commit_record_delete(
                    &state,
                    table,
                    key,
                    durability,
                    expected_lsn,
                    behavior_command_client_mutation_id(
                        behavior_client_mutation_id.as_deref(),
                        command_index,
                        "deleteRecord",
                    ),
                )
                .await?;
                if response.deleted {
                    committed.push(BehaviorCommittedResponse::RecordDeleted {
                        table: response.table,
                        key: response.key,
                        lsn: response.lsn,
                    });
                }
            }
            BehaviorCommand::RecordTransaction {
                operations,
                durability,
            } => {
                let response = commit_record_transaction(
                    &state,
                    RecordTransactionRequest {
                        durability,
                        client_mutation_id: behavior_command_client_mutation_id(
                            behavior_client_mutation_id.as_deref(),
                            command_index,
                            "recordTransaction",
                        ),
                        operations: operations
                            .into_iter()
                            .map(|operation| match operation {
                                BehaviorRecordTransactionOperation::Upsert {
                                    table,
                                    key,
                                    value,
                                    expected_lsn,
                                } => RecordTransactionOperationRequest::Upsert {
                                    table,
                                    key,
                                    value,
                                    expected_lsn,
                                },
                                BehaviorRecordTransactionOperation::Delete {
                                    table,
                                    key,
                                    expected_lsn,
                                } => RecordTransactionOperationRequest::Delete {
                                    table,
                                    key,
                                    expected_lsn,
                                },
                                BehaviorRecordTransactionOperation::NestedUpsert {
                                    table,
                                    parent_key,
                                    nested,
                                    nested_key,
                                    value,
                                    expected_lsn,
                                } => RecordTransactionOperationRequest::NestedUpsert {
                                    table,
                                    parent_key,
                                    nested,
                                    nested_key,
                                    value,
                                    expected_lsn,
                                },
                                BehaviorRecordTransactionOperation::NestedDelete {
                                    table,
                                    parent_key,
                                    nested,
                                    nested_key,
                                    expected_lsn,
                                } => RecordTransactionOperationRequest::NestedDelete {
                                    table,
                                    parent_key,
                                    nested,
                                    nested_key,
                                    expected_lsn,
                                },
                            })
                            .collect(),
                    },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RecordTransactionCommitted {
                    lsn: response.lsn,
                    operations: response.operations,
                });
            }
            BehaviorCommand::BroadcastRealtimeChannel {
                channel_id,
                kind,
                payload,
                include_self,
            } => {
                let user_id = request.user_id.clone().ok_or_else(|| {
                    ApiError::bad_request(
                        "behavior broadcastRealtimeChannel command requires userId",
                    )
                })?;
                let response = commit_realtime_channel_broadcast(
                    &state,
                    channel_id,
                    user_id,
                    kind,
                    payload,
                    include_self,
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RealtimeChannelBroadcasted {
                    channel_id: response.channel_id,
                    sequence: response.sequence,
                    delivered: response.delivered,
                });
            }
            BehaviorCommand::UpdateRealtimeChannelState {
                channel_id,
                state: state_value,
                expected_version,
            } => {
                let user_id = request.user_id.clone().ok_or_else(|| {
                    ApiError::bad_request(
                        "behavior updateRealtimeChannelState command requires userId",
                    )
                })?;
                let response = commit_realtime_channel_state(
                    &state,
                    channel_id,
                    user_id,
                    state_value,
                    expected_version,
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RealtimeChannelStateUpdated {
                    channel_id: response.channel_id,
                    state: response.state,
                    sequence: response.sequence,
                    delivered: response.delivered,
                });
            }
            BehaviorCommand::UpdateRealtimePresence {
                channel_id,
                metadata,
                session_id,
            } => {
                let user_id = request.user_id.clone().ok_or_else(|| {
                    ApiError::bad_request("behavior updateRealtimePresence command requires userId")
                })?;
                let response =
                    commit_realtime_presence(&state, channel_id, user_id, session_id, metadata)
                        .await?;
                committed.push(BehaviorCommittedResponse::RealtimePresenceUpdated {
                    channel_id: response.channel_id,
                    members: response.updated,
                    sequence: response.sequence,
                    delivered: response.delivered,
                });
            }
            BehaviorCommand::DisconnectConnections {
                user_id,
                session_id,
                reason,
            } => {
                let response = request_connection_disconnect(
                    &state,
                    ConnectionDisconnectRequest {
                        user_id,
                        session_id,
                        reason: reason.or_else(|| {
                            Some(format!(
                                "behavior '{}' requested disconnect",
                                request.behavior
                            ))
                        }),
                    },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::ConnectionsDisconnectRequested {
                    user_id: response.user_id,
                    session_id: response.session_id,
                    reason: response.reason,
                    targeted: response.targeted,
                    targeted_session_ids: response.targeted_session_ids,
                });
            }
            BehaviorCommand::ActivateRuntimeRecords {
                table,
                parent_key,
                nested,
                key,
                keys,
                index_name,
                value,
                values,
                lower,
                upper,
                lower_values,
                upper_values,
                after_key,
                after_cursor,
                order,
                limit,
                predicate,
            } => {
                let response = activate_runtime_records_internal(
                    &state,
                    RuntimeRecordActivationRequest {
                        table,
                        parent_key,
                        nested,
                        key,
                        keys,
                        index_name,
                        value,
                        values,
                        lower,
                        upper,
                        lower_values,
                        upper_values,
                        after_key,
                        after_cursor,
                        order,
                        limit,
                        predicate: runtime_record_activation_predicate_param(predicate)?,
                    },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RuntimeRecordsActivated { response });
            }
            BehaviorCommand::EvictRuntimeRecords {
                table,
                parent_key,
                nested,
                key,
                keys,
                after_key,
                limit,
            } => {
                let response = evict_runtime_records_internal(
                    &state,
                    RuntimeRecordActivationRequest {
                        table,
                        parent_key,
                        nested,
                        key,
                        keys,
                        index_name: None,
                        value: None,
                        values: None,
                        lower: None,
                        upper: None,
                        lower_values: None,
                        upper_values: None,
                        after_key,
                        after_cursor: None,
                        order: None,
                        limit,
                        predicate: None,
                    },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RuntimeRecordsEvicted { response });
            }
            BehaviorCommand::ActivateRuntimeRoom { room_id, limit } => {
                let response = activate_runtime_room_internal(
                    &state,
                    RuntimeRoomActivationRequest { room_id, limit },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RuntimeRoomActivated { response });
            }
            BehaviorCommand::EvictRuntimeRoom { room_id, limit } => {
                let response = evict_runtime_room_internal(
                    &state,
                    RuntimeRoomActivationRequest { room_id, limit },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::RuntimeRoomEvicted { response });
            }
            BehaviorCommand::ScheduleActorReminder {
                kind,
                key,
                reminder_id,
                due_at_ms,
                delay_ms,
                payload,
            } => {
                if behavior_client_mutation_id.is_some() && due_at_ms.is_none() {
                    return Err(ApiError::bad_request(
                        "behavior scheduleActorReminder with clientMutationId requires dueAtMs; delayMs is relative and cannot be replayed exactly",
                    ));
                }
                let reminder_id = reminder_id.or_else(|| {
                    behavior_command_client_mutation_id(
                        behavior_client_mutation_id.as_deref(),
                        command_index,
                        "scheduleActorReminder",
                    )
                });
                let response = schedule_actor_reminder_internal(
                    &state,
                    RuntimeActorReminderScheduleRequest {
                        kind,
                        key,
                        reminder_id,
                        due_at_ms,
                        delay_ms,
                        payload,
                        idempotency: behavior_client_mutation_id
                            .as_ref()
                            .map(|_| ActorReminderIdempotency),
                    },
                )
                .await?;
                committed.push(BehaviorCommittedResponse::ActorReminderScheduled { response });
            }
            BehaviorCommand::RequestHostHttp {
                request_id,
                method,
                url,
                headers,
                body,
                body_base64,
                timeout_ms,
                actor_kind,
                actor_key,
                reminder_id,
                continuation,
            } => {
                let request_id = request_id.or_else(|| {
                    behavior_command_client_mutation_id(
                        behavior_client_mutation_id.as_deref(),
                        command_index,
                        "requestHostHttp",
                    )
                });
                let request = parse_host_http_request(
                    request_id,
                    method,
                    url,
                    headers,
                    body,
                    body_base64,
                    timeout_ms,
                    actor_kind,
                    actor_key,
                    reminder_id,
                    continuation,
                )?;
                if let Some(existing) = existing_host_http_request(&state, &request)? {
                    committed.push(BehaviorCommittedResponse::HostHttpRequested {
                        request_id: existing.request.request_id,
                        method: existing.request.method,
                        url: existing.request.url,
                        actor_kind: existing.request.actor_kind,
                        actor_key: existing.request.actor_key,
                        reminder_id: existing.request.reminder_id,
                        accepted_at_ms: existing.request.requested_at_ms,
                        requested_lsn: existing.requested_lsn,
                    });
                    continue;
                }
                let (requested_lsn, _) = append_host_http_requested(&state, &request).await?;
                let accepted = spawn_host_http_request(state.clone(), request);
                committed.push(BehaviorCommittedResponse::HostHttpRequested {
                    request_id: accepted.request_id,
                    method: accepted.method,
                    url: accepted.url,
                    actor_kind: accepted.actor_kind,
                    actor_key: accepted.actor_key,
                    reminder_id: accepted.reminder_id,
                    accepted_at_ms: accepted.accepted_at_ms,
                    requested_lsn,
                });
            }
        }
    }

    Ok(BehaviorInvokeResponse {
        output,
        metadata,
        committed,
    })
}

async fn hydrate_behavior_request_context(
    state: &AppState,
    request: &mut BehaviorInvokeRequest,
) -> Result<(), ApiError> {
    let records = hydrate_behavior_records(state, &request.read.records).await?;
    let nested_records =
        hydrate_behavior_nested_records(state, &request.read.nested_records).await?;
    let latest_messages =
        hydrate_behavior_latest_messages(state, &request.read.latest_messages).await?;
    let objects = hydrate_behavior_objects(state, &request.read.objects).await?;
    let object_bodies = hydrate_behavior_object_bodies(state, &request.read.object_bodies).await?;
    let realtime_channel_members =
        hydrate_behavior_realtime_channel_members(state, &request.read.realtime_channel_members)
            .await?;
    let realtime_channel_states =
        hydrate_behavior_realtime_channel_states(state, &request.read.realtime_channel_states)
            .await?;
    let connection_sessions =
        hydrate_behavior_connection_sessions(state, &request.read.connection_sessions).await?;
    let audit_traces = hydrate_behavior_audit_traces(state, &request.read.audit_traces).await?;
    let audit_replays = hydrate_behavior_audit_replays(state, &request.read.audit_replays).await?;
    let request_context = std::mem::replace(&mut request.context, serde_json::Value::Null);
    let request_context =
        behavior_request_context_with_runtime_ctx(&state.runtime_id, request, request_context);
    request.context = serde_json::json!({
        "records": records,
        "nestedRecords": nested_records,
        "latestMessages": latest_messages,
        "objects": objects,
        "objectBodies": object_bodies,
        "realtimeChannelMembers": realtime_channel_members,
        "realtimeChannelStates": realtime_channel_states,
        "connectionSessions": connection_sessions,
        "auditTraces": audit_traces,
        "auditReplays": audit_replays,
        "requestContext": request_context,
    });
    Ok(())
}

fn behavior_request_context_with_runtime_ctx(
    runtime_id: &str,
    request: &BehaviorInvokeRequest,
    request_context: Value,
) -> Value {
    let timestamp_ms = request_context
        .get("ctx")
        .and_then(|ctx| ctx.get("timestampMs"))
        .and_then(|value| value.as_u64())
        .unwrap_or_else(crate::util::now_ms);
    let rng_seed = request_context
        .get("ctx")
        .and_then(|ctx| ctx.get("rngSeed"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| behavior_rng_seed(runtime_id, request, timestamp_ms));
    let sender = serde_json::json!({
        "kind": if request.user_id.is_some() { "user" } else { "system" },
        "userId": request.user_id.as_deref(),
        "behavior": request.behavior.as_str(),
        "mutation": request.mutation.as_str(),
        "clientMutationId": request.client_mutation_id.as_deref(),
    });
    let mut ctx = request_context
        .get("ctx")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    ctx.insert("timestampMs".to_string(), serde_json::json!(timestamp_ms));
    ctx.insert("sender".to_string(), sender);
    ctx.insert("rngSeed".to_string(), serde_json::json!(rng_seed));
    match request_context {
        Value::Object(mut object) => {
            object.insert("ctx".to_string(), Value::Object(ctx));
            Value::Object(object)
        }
        Value::Null => serde_json::json!({ "ctx": Value::Object(ctx) }),
        other => serde_json::json!({
            "ctx": Value::Object(ctx),
            "value": other,
        }),
    }
}

fn behavior_rng_seed(
    runtime_id: &str,
    request: &BehaviorInvokeRequest,
    timestamp_ms: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(runtime_id.as_bytes());
    hasher.update([0]);
    hasher.update(request.behavior.as_bytes());
    hasher.update([0]);
    hasher.update(request.mutation.as_bytes());
    hasher.update([0]);
    if let Some(user_id) = request.user_id.as_deref() {
        hasher.update(user_id.as_bytes());
    }
    hasher.update([0]);
    if let Some(client_mutation_id) = request.client_mutation_id.as_deref() {
        hasher.update(client_mutation_id.as_bytes());
    }
    hasher.update([0]);
    hasher.update(timestamp_ms.to_le_bytes());
    hex_lower(&hasher.finalize())
}

async fn hydrate_behavior_records(
    state: &AppState,
    reads: &[BehaviorRecordRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        validate_record_path(&read.table, &read.key, state)?;
        let record = get_record_from_live_or_disk(state, &read.table, &read.key).await?;
        out.push(serde_json::json!({
            "table": read.table,
            "key": read.key,
            "record": record,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_nested_records(
    state: &AppState,
    reads: &[BehaviorNestedRecordRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        validate_nested_record_path(
            &read.table,
            &read.parent_key,
            &read.nested,
            &read.nested_key,
            state,
        )?;
        let logical_table = nested_record_table(&read.table, &read.nested);
        let logical_key = nested_record_key(&read.parent_key, &read.nested_key);
        let record = get_record_from_live_or_disk(state, &logical_table, &logical_key).await?;
        out.push(serde_json::json!({
            "table": read.table,
            "parentKey": read.parent_key,
            "nested": read.nested,
            "nestedKey": read.nested_key,
            "logicalTable": logical_table,
            "logicalKey": logical_key,
            "record": record,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_latest_messages(
    state: &AppState,
    reads: &[BehaviorLatestMessagesRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        let limit = normalize_limit(read.limit);
        let hot = state
            .actors
            .latest_messages(&read.room_id, None, limit)
            .await;
        let messages = if hot.len() >= limit {
            hot
        } else {
            state
                .chat_log
                .latest(&read.room_id, None, limit)
                .await
                .map_err(ApiError::internal)?
        };
        out.push(serde_json::json!({
            "roomId": read.room_id,
            "messages": messages,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_objects(
    state: &AppState,
    reads: &[BehaviorObjectRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        if !ensure_safe_object_id(&read.object_id) {
            return Err(ApiError::bad_request("invalid object id"));
        }
        let object = if state.objects.metadata_exists(&read.object_id) {
            Some(
                state
                    .objects
                    .metadata(&read.object_id)
                    .await
                    .map_err(ApiError::internal)?,
            )
        } else {
            None
        };
        out.push(serde_json::json!({
            "objectId": read.object_id,
            "object": object,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_object_bodies(
    state: &AppState,
    reads: &[BehaviorObjectRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        if !ensure_safe_object_id(&read.object_id) {
            return Err(ApiError::bad_request("invalid object id"));
        }
        let (object, body_base64) = if state.objects.metadata_exists(&read.object_id) {
            let (metadata, body) = state
                .objects
                .body(&read.object_id)
                .await
                .map_err(ApiError::internal)?;
            (Some(metadata), Some(BASE64_STANDARD.encode(&body)))
        } else {
            (None, None)
        };
        out.push(serde_json::json!({
            "objectId": read.object_id,
            "object": object,
            "bodyBase64": body_base64,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_realtime_channel_states(
    state: &AppState,
    reads: &[BehaviorRealtimeChannelStateRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        if read.channel_id.trim().is_empty() {
            return Err(ApiError::bad_request("realtime channelId is required"));
        }
        let snapshot = state.realtime.state(&read.channel_id).await;
        out.push(serde_json::json!({
            "channelId": read.channel_id,
            "state": snapshot,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_realtime_channel_members(
    state: &AppState,
    reads: &[BehaviorRealtimeChannelMembersRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        if read.channel_id.trim().is_empty() {
            return Err(ApiError::bad_request("realtime channelId is required"));
        }
        let members = state.realtime.members(&read.channel_id).await;
        out.push(serde_json::json!({
            "channelId": read.channel_id,
            "members": members,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_connection_sessions(
    state: &AppState,
    reads: &[BehaviorConnectionSessionsRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        let user_id = read
            .user_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let session_id = read
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let mut sessions = state.connections.list(user_id, read.transport).await;
        if let Some(session_id) = session_id {
            sessions.retain(|session| session.session_id == session_id);
        }
        out.push(serde_json::json!({
            "userId": user_id,
            "sessionId": session_id,
            "transport": read.transport,
            "sessions": sessions,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_audit_traces(
    state: &AppState,
    reads: &[BehaviorAuditTraceRead],
) -> Result<Vec<serde_json::Value>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        let after_lsn = read.after_lsn.unwrap_or(0);
        let limit = normalize_limit(read.limit);
        let target = audit_trace_target(behavior_audit_trace_query(read.clone()))?;
        let records = read_records_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
        let mut page = Vec::new();
        let mut has_more = false;
        for record in records {
            if record.lsn <= after_lsn {
                continue;
            }
            if !matches_audit_trace_target(&record, &target) {
                continue;
            }
            if page.len() >= limit {
                has_more = true;
                break;
            }
            page.push(record);
        }
        let next_after_lsn = page.last().map(|record| record.lsn).unwrap_or(after_lsn);
        out.push(serde_json::json!({
            "target": target,
            "records": page,
            "nextAfterLsn": next_after_lsn,
            "hasMore": has_more,
        }));
    }
    Ok(out)
}

async fn hydrate_behavior_audit_replays(
    state: &AppState,
    reads: &[BehaviorAuditReplayRead],
) -> Result<Vec<AuditReplayResponse>, ApiError> {
    let mut out = Vec::with_capacity(reads.len());
    for read in reads {
        let at_lsn = read.at_lsn.unwrap_or(u64::MAX);
        let target = audit_replay_target(behavior_audit_replay_query(read.clone()))?;
        let records = read_records_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
        out.push(replay_audit_target(records, target, at_lsn));
    }
    Ok(out)
}

fn behavior_audit_trace_query(read: BehaviorAuditTraceRead) -> AuditTraceQuery {
    AuditTraceQuery {
        after_lsn: read.after_lsn,
        limit: read.limit,
        kind: behavior_audit_trace_kind(read.kind),
        id: read.id,
        table: read.table,
        record_key: read.record_key,
        parent_key: read.parent_key,
        nested: read.nested,
        nested_key: read.nested_key,
        path: read.path,
        client_mutation_id: read.client_mutation_id,
    }
}

fn behavior_audit_replay_query(read: BehaviorAuditReplayRead) -> AuditReplayQuery {
    AuditReplayQuery {
        at_lsn: read.at_lsn,
        kind: behavior_audit_replay_kind(read.kind),
        id: read.id,
        table: read.table,
        record_key: read.record_key,
        parent_key: read.parent_key,
        nested: read.nested,
        nested_key: read.nested_key,
    }
}

fn behavior_audit_trace_kind(kind: BehaviorAuditTraceKind) -> AuditTraceKind {
    match kind {
        BehaviorAuditTraceKind::Room => AuditTraceKind::Room,
        BehaviorAuditTraceKind::User => AuditTraceKind::User,
        BehaviorAuditTraceKind::Object => AuditTraceKind::Object,
        BehaviorAuditTraceKind::Record => AuditTraceKind::Record,
        BehaviorAuditTraceKind::NestedRecord => AuditTraceKind::NestedRecord,
        BehaviorAuditTraceKind::Path => AuditTraceKind::Path,
        BehaviorAuditTraceKind::ClientMutation => AuditTraceKind::ClientMutation,
    }
}

fn behavior_audit_replay_kind(kind: BehaviorAuditReplayKind) -> AuditTraceKind {
    match kind {
        BehaviorAuditReplayKind::User => AuditTraceKind::User,
        BehaviorAuditReplayKind::Object => AuditTraceKind::Object,
        BehaviorAuditReplayKind::Record => AuditTraceKind::Record,
        BehaviorAuditReplayKind::NestedRecord => AuditTraceKind::NestedRecord,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ActorReminderDraft;

    fn host_http_draft(request_id: &str) -> HostHttpRequestDraft {
        HostHttpRequestDraft {
            request_id: request_id.to_string(),
            method: "GET".to_string(),
            url: "https://api.example.test/v1/jobs".to_string(),
            headers: BTreeMap::new(),
            body: None,
            body_base64: None,
            timeout_ms: 1000,
            actor_kind: "scope".to_string(),
            actor_key: "table:jobs/bucket:00".to_string(),
            reminder_id: format!("host-http-{request_id}"),
            continuation: serde_json::json!({
                "type": "behaviorContinuation",
                "behavior": "jobs",
                "mutation": "onHttpResult"
            }),
            requested_at_ms: 100,
        }
    }

    fn wal_record(lsn: u64, payload: WalPayload) -> WalRecord {
        WalRecord {
            lsn,
            shard: 0,
            shard_epoch: 1,
            owner_node_id: "test".to_string(),
            timestamp_ms: lsn,
            schema_version: 1,
            durability: Durability::Strict,
            payload,
            checksum: None,
        }
    }

    #[test]
    fn pending_host_http_requests_restore_unfinished_only() {
        let pending = host_http_draft("pending");
        let completed = host_http_draft("completed");
        let scheduled = host_http_draft("scheduled");
        let records = vec![
            wal_record(
                1,
                WalPayload::HostHttpRequested {
                    request: pending.clone(),
                },
            ),
            wal_record(
                2,
                WalPayload::HostHttpRequested {
                    request: completed.clone(),
                },
            ),
            wal_record(
                3,
                WalPayload::HostHttpRequested {
                    request: scheduled.clone(),
                },
            ),
            wal_record(
                4,
                WalPayload::HostHttpCompleted {
                    request_id: completed.request_id,
                    completed_at_ms: 400,
                },
            ),
            wal_record(
                5,
                WalPayload::ActorReminderScheduled {
                    reminder: ActorReminderDraft {
                        actor_kind: scheduled.actor_kind,
                        actor_key: scheduled.actor_key,
                        reminder_id: scheduled.reminder_id,
                        due_at_ms: 500,
                        payload: Some(scheduled.continuation),
                    },
                },
            ),
        ];

        let restored = pending_host_http_requests_from_wal_records(&records);

        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].request_id, pending.request_id);
    }

    #[test]
    fn host_http_request_index_tracks_terminal_statuses() {
        let pending = host_http_draft("pending");
        let completed = host_http_draft("completed");
        let scheduled = host_http_draft("scheduled");
        let records = vec![
            wal_record(
                1,
                WalPayload::HostHttpRequested {
                    request: pending.clone(),
                },
            ),
            wal_record(
                2,
                WalPayload::HostHttpRequested {
                    request: completed.clone(),
                },
            ),
            wal_record(
                3,
                WalPayload::HostHttpRequested {
                    request: scheduled.clone(),
                },
            ),
            wal_record(
                4,
                WalPayload::ActorReminderScheduled {
                    reminder: ActorReminderDraft {
                        actor_kind: scheduled.actor_kind.clone(),
                        actor_key: scheduled.actor_key.clone(),
                        reminder_id: scheduled.reminder_id.clone(),
                        due_at_ms: 500,
                        payload: Some(scheduled.continuation.clone()),
                    },
                },
            ),
            wal_record(
                5,
                WalPayload::HostHttpCompleted {
                    request_id: completed.request_id.clone(),
                    completed_at_ms: 500,
                },
            ),
        ];

        let index = host_http_request_index_from_wal_records(&records);

        assert_eq!(index["pending"].requested_lsn, 1);
        assert_eq!(index["pending"].status, HostHttpRequestStatus::Requested);
        assert_eq!(
            index["scheduled"].status,
            HostHttpRequestStatus::CallbackScheduled
        );
        assert_eq!(index["completed"].status, HostHttpRequestStatus::Completed);
    }

    #[test]
    fn host_http_request_matching_ignores_requested_time_only() {
        let mut draft = host_http_draft("same");
        let request =
            host_http_request_from_draft(draft.clone()).expect("draft should parse as request");

        draft.requested_at_ms += 1;
        assert!(host_http_request_matches_draft(&request, &draft));

        draft.url = "https://api.example.test/v1/other".to_string();
        assert!(!host_http_request_matches_draft(&request, &draft));
    }

    #[test]
    fn host_http_execution_headers_include_stable_idempotency_keys() {
        let request =
            host_http_request_from_draft(host_http_draft("headers")).expect("draft should parse");

        let headers = host_http_execution_headers(&request);

        assert_eq!(
            headers
                .get(HOST_HTTP_REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("headers")
        );
        assert_eq!(
            headers
                .get(HOST_HTTP_IDEMPOTENCY_KEY_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("headers")
        );
    }

    #[test]
    fn host_http_execution_headers_preserve_explicit_idempotency_key() {
        let mut draft = host_http_draft("headers-explicit");
        draft.headers.insert(
            HOST_HTTP_IDEMPOTENCY_KEY_HEADER.to_string(),
            "business-key".to_string(),
        );
        let request = host_http_request_from_draft(draft).expect("draft should parse");

        let headers = host_http_execution_headers(&request);

        assert_eq!(
            headers
                .get(HOST_HTTP_REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("headers-explicit")
        );
        assert_eq!(
            headers
                .get(HOST_HTTP_IDEMPOTENCY_KEY_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("business-key")
        );
    }

    #[test]
    fn host_http_request_id_header_is_host_managed() {
        let mut draft = host_http_draft("managed-header");
        draft.headers.insert(
            HOST_HTTP_REQUEST_ID_HEADER.to_string(),
            "spoofed".to_string(),
        );

        let error = match host_http_request_from_draft(draft) {
            Ok(_) => panic!("managed header should reject"),
            Err(error) => error,
        };

        assert!(
            error
                .message
                .contains("requestHostHttp header 'x-nextdb-request-id' is managed by the host"),
            "{}",
            error.message
        );
    }

    #[test]
    fn host_http_request_roundtrips_through_draft() {
        let request =
            host_http_request_from_draft(host_http_draft("roundtrip")).expect("draft should parse");
        let draft = request.to_draft(123);

        assert_eq!(draft.request_id, "roundtrip");
        assert_eq!(draft.method, "GET");
        assert_eq!(draft.actor_kind, "scope");
        assert_eq!(draft.requested_at_ms, 123);
    }

    #[test]
    fn behavior_runtime_context_injects_sender_timestamp_and_rng_seed() {
        let request = BehaviorInvokeRequest {
            behavior: "matchmaking".to_string(),
            mutation: "tick".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: Some("cmid-1".to_string()),
            input: serde_json::json!({}),
            read: crate::behavior::BehaviorReadPlan::default(),
            context: Value::Null,
        };
        let context = behavior_request_context_with_runtime_ctx(
            "runtime-1",
            &request,
            serde_json::json!({
                "callChainId": "chain-1",
                "ctx": {
                    "timestampMs": 1234,
                    "rngSeed": "seed-1",
                    "custom": "keep"
                }
            }),
        );

        assert_eq!(context["callChainId"], serde_json::json!("chain-1"));
        assert_eq!(context["ctx"]["timestampMs"], serde_json::json!(1234));
        assert_eq!(context["ctx"]["rngSeed"], serde_json::json!("seed-1"));
        assert_eq!(context["ctx"]["custom"], serde_json::json!("keep"));
        assert_eq!(context["ctx"]["sender"]["kind"], serde_json::json!("user"));
        assert_eq!(
            context["ctx"]["sender"]["userId"],
            serde_json::json!("alice")
        );
        assert_eq!(
            context["ctx"]["sender"]["clientMutationId"],
            serde_json::json!("cmid-1")
        );
    }

    #[test]
    fn behavior_rng_seed_is_stable_for_same_turn_identity() {
        let mut request = BehaviorInvokeRequest {
            behavior: "matchmaking".to_string(),
            mutation: "tick".to_string(),
            user_id: None,
            client_mutation_id: None,
            input: serde_json::json!({}),
            read: crate::behavior::BehaviorReadPlan::default(),
            context: Value::Null,
        };

        let first = behavior_rng_seed("runtime-1", &request, 1234);
        let second = behavior_rng_seed("runtime-1", &request, 1234);
        request.mutation = "other".to_string();
        let other = behavior_rng_seed("runtime-1", &request, 1234);

        assert_eq!(first, second);
        assert_ne!(first, other);
        assert_eq!(first.len(), 64);
    }
}
