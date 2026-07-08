use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    api::error::ApiError,
    api::records::{nested_record_key, nested_record_prefix, nested_record_table},
    model::{
        ClientMutationRecord, DbRecord, DbRecordDeleteDraft, DbRecordDraft, DbRecordMutationDraft,
        ObjectMetadata, UserProfile, WalPayload, WalRecord,
    },
    record_store::ensure_safe_record_component,
    util::normalize_limit,
    wal::{read_records_from_wal_paths, read_records_from_wal_paths_after_lsn},
};

use axum::{
    Json,
    extract::{Query, State},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditQuery {
    pub(crate) after_lsn: Option<u64>,
    pub(crate) limit: Option<usize>,
    pub(crate) payload_type: Option<String>,
    pub(crate) room_id: Option<String>,
    pub(crate) user_id: Option<String>,
    pub(crate) object_id: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) record_key: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditResponse {
    pub(crate) records: Vec<WalRecord>,
    pub(crate) next_after_lsn: u64,
    pub(crate) has_more: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AuditTraceKind {
    Room,
    User,
    Object,
    Record,
    NestedRecord,
    Path,
    ClientMutation,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditTraceQuery {
    pub(crate) after_lsn: Option<u64>,
    pub(crate) limit: Option<usize>,
    pub(crate) kind: AuditTraceKind,
    pub(crate) id: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) record_key: Option<String>,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) nested_key: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditTraceTarget {
    pub(crate) kind: AuditTraceKind,
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nested: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nested_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditTraceResponse {
    pub(crate) target: AuditTraceTarget,
    pub(crate) records: Vec<WalRecord>,
    pub(crate) next_after_lsn: u64,
    pub(crate) has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditReplayQuery {
    pub(crate) at_lsn: Option<u64>,
    pub(crate) kind: AuditTraceKind,
    pub(crate) id: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) record_key: Option<String>,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) nested_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AuditReplayStatus {
    Exists,
    Deleted,
    Missing,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditReplayDelete {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) object_id: Option<String>,
    pub(crate) path: String,
    pub(crate) deleted_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) force: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditReplayResponse {
    pub(crate) target: AuditTraceTarget,
    pub(crate) at_lsn: u64,
    pub(crate) status: AuditReplayStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record: Option<DbRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) user: Option<UserProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) object: Option<ObjectMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delete: Option<AuditReplayDelete>,
}

pub(crate) async fn audit_wal(
    State(state): State<AppState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<AuditResponse>, ApiError> {
    let after_lsn = query.after_lsn.unwrap_or(0);
    let limit = normalize_limit(query.limit);
    let records = read_records_from_wal_paths_after_lsn(&state.wal_paths, after_lsn)
        .map_err(ApiError::internal)?;
    let mut page = Vec::new();
    let mut has_more = false;

    for record in records {
        if record.lsn <= after_lsn {
            continue;
        }
        if !matches_audit_query(&record, &query) {
            continue;
        }
        if page.len() >= limit {
            has_more = true;
            break;
        }
        page.push(record);
    }

    let next_after_lsn = page.last().map(|record| record.lsn).unwrap_or(after_lsn);
    Ok(Json(AuditResponse {
        records: page,
        next_after_lsn,
        has_more,
    }))
}

pub(crate) async fn audit_trace(
    State(state): State<AppState>,
    Query(query): Query<AuditTraceQuery>,
) -> Result<Json<AuditTraceResponse>, ApiError> {
    let after_lsn = query.after_lsn.unwrap_or(0);
    let limit = normalize_limit(query.limit);
    let target = audit_trace_target(query)?;
    let records = read_records_from_wal_paths_after_lsn(&state.wal_paths, after_lsn)
        .map_err(ApiError::internal)?;
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
    Ok(Json(AuditTraceResponse {
        target,
        records: page,
        next_after_lsn,
        has_more,
    }))
}

pub(crate) async fn audit_replay(
    State(state): State<AppState>,
    Query(query): Query<AuditReplayQuery>,
) -> Result<Json<AuditReplayResponse>, ApiError> {
    let at_lsn = query.at_lsn.unwrap_or(u64::MAX);
    let target = audit_replay_target(query)?;
    let records = read_records_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
    Ok(Json(replay_audit_target(records, target, at_lsn)))
}

fn matches_audit_query(record: &WalRecord, query: &AuditQuery) -> bool {
    if query
        .payload_type
        .as_deref()
        .is_some_and(|expected| expected != wal_payload_type(&record.payload))
    {
        return false;
    }

    if query.room_id.as_deref().is_some_and(|room_id| {
        !matches!(
            &record.payload,
            WalPayload::MessageCreated { message } if message.room_id == room_id
        )
    }) {
        return false;
    }

    if query.user_id.as_deref().is_some_and(|user_id| {
        !matches!(
            &record.payload,
            WalPayload::UserEventPublished { event } if event.user_id == user_id
        )
    }) {
        return false;
    }

    if query.object_id.as_deref().is_some_and(|object_id| {
        !matches!(
            &record.payload,
            WalPayload::ObjectCommitted { object, .. } if object.id == object_id
        ) && !matches!(
            &record.payload,
            WalPayload::ObjectDeleted { object_id: deleted_object_id, .. } if deleted_object_id == object_id
        ) && !matches!(
            &record.payload,
            WalPayload::MessageCreated { message } if message.attachments.iter().any(|attachment| attachment.id == object_id)
        ) && !matches!(
            &record.payload,
            WalPayload::RecordUpserted { record } if value_contains_object_id(&record.value, object_id)
        ) && !matches!(
            &record.payload,
            WalPayload::RecordTransactionCommitted { operations, .. }
                if operations.iter().any(|operation| matches!(
                    operation,
                    DbRecordMutationDraft::Upsert { record } if value_contains_object_id(&record.value, object_id)
                ))
        )
    }) {
        return false;
    }

    if query.table.as_deref().is_some_and(|table| {
        !matches!(
            &record.payload,
            WalPayload::RecordUpserted { record } if record.table == table
        ) && !matches!(
            &record.payload,
            WalPayload::RecordDeleted { record } if record.table == table
        ) && !matches!(
            &record.payload,
            WalPayload::RecordTransactionCommitted { operations, .. }
                if operations.iter().any(|operation| match operation {
                    DbRecordMutationDraft::Upsert { record } => record.table == table,
                    DbRecordMutationDraft::Delete { record } => record.table == table,
                })
        )
    }) {
        return false;
    }

    if query
        .record_key
        .as_deref()
        .is_some_and(|key| !payload_matches_record_key(&record.payload, key))
    {
        return false;
    }

    if query
        .path
        .as_deref()
        .is_some_and(|path| !payload_matches_path(&record.payload, path))
    {
        return false;
    }

    if query
        .client_mutation_id
        .as_deref()
        .is_some_and(|client_mutation_id| {
            !payload_matches_client_mutation_id(&record.payload, client_mutation_id)
        })
    {
        return false;
    }

    true
}

pub(crate) fn audit_trace_target(query: AuditTraceQuery) -> Result<AuditTraceTarget, ApiError> {
    match query.kind {
        AuditTraceKind::Room => {
            let id = required_trace_value(query.id, "id is required for room trace")?;
            ensure_trace_component(&id, "invalid room id")?;
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::Room,
                id,
                table: None,
                record_key: None,
                parent_key: None,
                nested: None,
                nested_key: None,
                path: None,
                client_mutation_id: None,
            })
        }
        AuditTraceKind::User => {
            let id = required_trace_value(query.id, "id is required for user trace")?;
            ensure_trace_component(&id, "invalid user id")?;
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::User,
                id,
                table: None,
                record_key: None,
                parent_key: None,
                nested: None,
                nested_key: None,
                path: None,
                client_mutation_id: None,
            })
        }
        AuditTraceKind::Object => {
            let id = required_trace_value(query.id, "id is required for object trace")?;
            ensure_trace_component(&id, "invalid object id")?;
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::Object,
                id,
                table: None,
                record_key: None,
                parent_key: None,
                nested: None,
                nested_key: None,
                path: None,
                client_mutation_id: None,
            })
        }
        AuditTraceKind::Record => {
            let table = required_trace_value(query.table, "table is required for record trace")?;
            let record_key = query.record_key.or(query.id);
            let record_key =
                required_trace_value(record_key, "recordKey or id is required for record trace")?;
            ensure_trace_component(&table, "invalid table name")?;
            ensure_trace_component(&record_key, "invalid record key")?;
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::Record,
                id: record_key.clone(),
                table: Some(table),
                record_key: Some(record_key),
                parent_key: None,
                nested: None,
                nested_key: None,
                path: None,
                client_mutation_id: None,
            })
        }
        AuditTraceKind::NestedRecord => {
            let table =
                required_trace_value(query.table, "table is required for nestedRecord trace")?;
            let parent_key = required_trace_value(
                query.parent_key,
                "parentKey is required for nestedRecord trace",
            )?;
            let nested =
                required_trace_value(query.nested, "nested is required for nestedRecord trace")?;
            let nested_key = query.nested_key.or(query.id);
            let nested_key = required_trace_value(
                nested_key,
                "nestedKey or id is required for nestedRecord trace",
            )?;
            ensure_trace_component(&table, "invalid table name")?;
            ensure_trace_component(&parent_key, "invalid parent key")?;
            ensure_trace_component(&nested, "invalid nested table name")?;
            ensure_trace_component(&nested_key, "invalid nested key")?;
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::NestedRecord,
                id: nested_key.clone(),
                table: Some(table),
                record_key: Some(nested_record_key(&parent_key, &nested_key)),
                parent_key: Some(parent_key),
                nested: Some(nested),
                nested_key: Some(nested_key),
                path: None,
                client_mutation_id: None,
            })
        }
        AuditTraceKind::Path => {
            let path = query.path.or(query.id);
            let path = required_trace_value(path, "path or id is required for path trace")?;
            if path.trim().is_empty() || path.contains("..") || path.starts_with('/') {
                return Err(ApiError::bad_request("invalid trace path"));
            }
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::Path,
                id: path.clone(),
                table: None,
                record_key: None,
                parent_key: None,
                nested: None,
                nested_key: None,
                path: Some(path),
                client_mutation_id: None,
            })
        }
        AuditTraceKind::ClientMutation => {
            let client_mutation_id = query.client_mutation_id.or(query.id);
            let client_mutation_id = required_trace_value(
                client_mutation_id,
                "clientMutationId or id is required for clientMutation trace",
            )?;
            if client_mutation_id.trim().is_empty() {
                return Err(ApiError::bad_request("invalid client mutation id"));
            }
            Ok(AuditTraceTarget {
                kind: AuditTraceKind::ClientMutation,
                id: client_mutation_id.clone(),
                table: None,
                record_key: None,
                parent_key: None,
                nested: None,
                nested_key: None,
                path: None,
                client_mutation_id: Some(client_mutation_id),
            })
        }
    }
}

pub(crate) fn audit_replay_target(query: AuditReplayQuery) -> Result<AuditTraceTarget, ApiError> {
    let trace_query = AuditTraceQuery {
        after_lsn: None,
        limit: None,
        kind: query.kind,
        id: query.id,
        table: query.table,
        record_key: query.record_key,
        parent_key: query.parent_key,
        nested: query.nested,
        nested_key: query.nested_key,
        path: None,
        client_mutation_id: None,
    };
    let target = audit_trace_target(trace_query)?;
    match target.kind {
        AuditTraceKind::Record
        | AuditTraceKind::NestedRecord
        | AuditTraceKind::User
        | AuditTraceKind::Object => Ok(target),
        AuditTraceKind::Room | AuditTraceKind::Path | AuditTraceKind::ClientMutation => Err(
            ApiError::bad_request("replay supports record, nestedRecord, user, and object targets"),
        ),
    }
}

pub(crate) fn replay_audit_target(
    records: Vec<WalRecord>,
    target: AuditTraceTarget,
    at_lsn: u64,
) -> AuditReplayResponse {
    let mut response = AuditReplayResponse {
        target,
        at_lsn,
        status: AuditReplayStatus::Missing,
        source_lsn: None,
        record: None,
        user: None,
        object: None,
        delete: None,
    };

    for wal_record in records {
        if wal_record.lsn > at_lsn {
            break;
        }
        replay_wal_record_into_response(&mut response, wal_record);
    }

    response
}

fn replay_wal_record_into_response(response: &mut AuditReplayResponse, wal_record: WalRecord) {
    match response.target.kind {
        AuditTraceKind::Record | AuditTraceKind::NestedRecord => {
            let Some(table) = replay_target_table(&response.target) else {
                return;
            };
            let Some(key) = response.target.record_key.clone() else {
                return;
            };
            replay_record_payload(response, wal_record, &table, &key);
        }
        AuditTraceKind::User => replay_user_payload(response, wal_record),
        AuditTraceKind::Object => replay_object_payload(response, wal_record),
        AuditTraceKind::Room | AuditTraceKind::Path | AuditTraceKind::ClientMutation => {}
    }
}

fn replay_target_table(target: &AuditTraceTarget) -> Option<String> {
    match target.kind {
        AuditTraceKind::Record => target.table.clone(),
        AuditTraceKind::NestedRecord => Some(nested_record_table(
            target.table.as_deref()?,
            target.nested.as_deref()?,
        )),
        _ => None,
    }
}

fn replay_record_payload(
    response: &mut AuditReplayResponse,
    wal_record: WalRecord,
    table: &str,
    key: &str,
) {
    match wal_record.payload {
        WalPayload::RecordUpserted { record } if record.table == table && record.key == key => {
            set_replayed_record(response, wal_record.lsn, record);
        }
        WalPayload::RecordDeleted { record } if record.table == table && record.key == key => {
            set_replayed_record_delete(response, wal_record.lsn, record);
        }
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            for operation in operations {
                match operation {
                    DbRecordMutationDraft::Upsert { record }
                        if record.table == table && record.key == key =>
                    {
                        set_replayed_record(response, wal_record.lsn, record);
                    }
                    DbRecordMutationDraft::Delete { record }
                        if record.table == table && record.key == key =>
                    {
                        set_replayed_record_delete(response, wal_record.lsn, record);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn set_replayed_record(response: &mut AuditReplayResponse, lsn: u64, record: DbRecordDraft) {
    response.status = AuditReplayStatus::Exists;
    response.source_lsn = Some(lsn);
    response.record = Some(record.into_record(lsn));
    response.user = None;
    response.object = None;
    response.delete = None;
}

fn set_replayed_record_delete(
    response: &mut AuditReplayResponse,
    lsn: u64,
    record: DbRecordDeleteDraft,
) {
    response.status = AuditReplayStatus::Deleted;
    response.source_lsn = Some(lsn);
    response.record = None;
    response.user = None;
    response.object = None;
    response.delete = Some(AuditReplayDelete {
        table: Some(record.table),
        key: Some(record.key),
        object_id: None,
        path: record.path,
        deleted_at_ms: record.deleted_at_ms,
        force: None,
    });
}

fn replay_user_payload(response: &mut AuditReplayResponse, wal_record: WalRecord) {
    let WalPayload::UserUpserted { user } = wal_record.payload else {
        return;
    };
    if user.user_id != response.target.id {
        return;
    }
    response.status = AuditReplayStatus::Exists;
    response.source_lsn = Some(wal_record.lsn);
    response.record = None;
    response.user = Some(user.into_profile(wal_record.lsn));
    response.object = None;
    response.delete = None;
}

fn replay_object_payload(response: &mut AuditReplayResponse, wal_record: WalRecord) {
    match wal_record.payload {
        WalPayload::ObjectCommitted { object, .. } if object.id == response.target.id => {
            response.status = AuditReplayStatus::Exists;
            response.source_lsn = Some(wal_record.lsn);
            response.record = None;
            response.user = None;
            response.object = Some(object);
            response.delete = None;
        }
        WalPayload::ObjectDeleted {
            object_id,
            deleted_at_ms,
            path,
            force,
            ..
        } if object_id == response.target.id => {
            response.status = AuditReplayStatus::Deleted;
            response.source_lsn = Some(wal_record.lsn);
            response.record = None;
            response.user = None;
            response.object = None;
            response.delete = Some(AuditReplayDelete {
                table: None,
                key: None,
                object_id: Some(object_id),
                path,
                deleted_at_ms,
                force: Some(force),
            });
        }
        _ => {}
    }
}

fn required_trace_value(value: Option<String>, message: &'static str) -> Result<String, ApiError> {
    let value = value.unwrap_or_default();
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(message));
    }
    Ok(value)
}

fn ensure_trace_component(value: &str, message: &'static str) -> Result<(), ApiError> {
    if ensure_safe_record_component(value) {
        Ok(())
    } else {
        Err(ApiError::bad_request(message))
    }
}

pub(crate) fn matches_audit_trace_target(record: &WalRecord, target: &AuditTraceTarget) -> bool {
    match target.kind {
        AuditTraceKind::Room => payload_matches_room_trace(&record.payload, &target.id),
        AuditTraceKind::User => payload_matches_user_trace(&record.payload, &target.id),
        AuditTraceKind::Object => payload_matches_object_trace(&record.payload, &target.id),
        AuditTraceKind::Record => {
            let Some(table) = target.table.as_deref() else {
                return false;
            };
            let Some(record_key) = target.record_key.as_deref() else {
                return false;
            };
            payload_matches_table_record(&record.payload, table, record_key)
        }
        AuditTraceKind::NestedRecord => {
            let Some(table) = target.table.as_deref() else {
                return false;
            };
            let Some(nested) = target.nested.as_deref() else {
                return false;
            };
            let Some(record_key) = target.record_key.as_deref() else {
                return false;
            };
            let logical_table = nested_record_table(table, nested);
            payload_matches_table_record(&record.payload, &logical_table, record_key)
        }
        AuditTraceKind::Path => target
            .path
            .as_deref()
            .is_some_and(|path| payload_matches_path(&record.payload, path)),
        AuditTraceKind::ClientMutation => target
            .client_mutation_id
            .as_deref()
            .is_some_and(|id| payload_matches_client_mutation_id(&record.payload, id)),
    }
}

fn payload_matches_room_trace(payload: &WalPayload, room_id: &str) -> bool {
    match payload {
        WalPayload::MessageCreated { message } => message.room_id == room_id,
        WalPayload::RecordUpserted { record } => {
            record.table == "rooms" && record.key == room_id
                || record.table == "rooms.messages"
                    && record.key.starts_with(&nested_record_prefix(room_id))
        }
        WalPayload::RecordDeleted { record } => {
            record.table == "rooms" && record.key == room_id
                || record.table == "rooms.messages"
                    && record.key.starts_with(&nested_record_prefix(room_id))
        }
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            operations.iter().any(|operation| match operation {
                DbRecordMutationDraft::Upsert { record } => {
                    record.table == "rooms" && record.key == room_id
                        || record.table == "rooms.messages"
                            && record.key.starts_with(&nested_record_prefix(room_id))
                }
                DbRecordMutationDraft::Delete { record } => {
                    record.table == "rooms" && record.key == room_id
                        || record.table == "rooms.messages"
                            && record.key.starts_with(&nested_record_prefix(room_id))
                }
            })
        }
        WalPayload::UserEventPublished { .. }
        | WalPayload::UserUpserted { .. }
        | WalPayload::ObjectCommitted { .. }
        | WalPayload::ObjectDeleted { .. }
        | WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. }
        | WalPayload::ClientMutationRecorded { .. } => false,
    }
}

fn payload_matches_user_trace(payload: &WalPayload, user_id: &str) -> bool {
    match payload {
        WalPayload::MessageCreated { message } => message.sender_id == user_id,
        WalPayload::UserEventPublished { event } => event.user_id == user_id,
        WalPayload::UserUpserted { user } => user.user_id == user_id,
        WalPayload::ObjectCommitted { .. }
        | WalPayload::ObjectDeleted { .. }
        | WalPayload::RecordUpserted { .. }
        | WalPayload::RecordDeleted { .. }
        | WalPayload::RecordTransactionCommitted { .. }
        | WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. }
        | WalPayload::ClientMutationRecorded { .. } => false,
    }
}

fn payload_matches_object_trace(payload: &WalPayload, object_id: &str) -> bool {
    matches!(
        payload,
        WalPayload::ObjectCommitted { object, .. } if object.id == object_id
    ) || matches!(
        payload,
        WalPayload::ObjectDeleted { object_id: deleted_object_id, .. } if deleted_object_id == object_id
    ) || matches!(
        payload,
        WalPayload::MessageCreated { message } if message.attachments.iter().any(|attachment| attachment.id == object_id)
    ) || matches!(
        payload,
        WalPayload::RecordUpserted { record } if value_contains_object_id(&record.value, object_id)
    ) || matches!(
        payload,
        WalPayload::RecordTransactionCommitted { operations, .. }
            if operations.iter().any(|operation| matches!(
                operation,
                DbRecordMutationDraft::Upsert { record } if value_contains_object_id(&record.value, object_id)
            ))
    )
}

fn payload_matches_table_record(payload: &WalPayload, table: &str, key: &str) -> bool {
    match payload {
        WalPayload::RecordUpserted { record } => record.table == table && record.key == key,
        WalPayload::RecordDeleted { record } => record.table == table && record.key == key,
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            operations.iter().any(|operation| match operation {
                DbRecordMutationDraft::Upsert { record } => {
                    record.table == table && record.key == key
                }
                DbRecordMutationDraft::Delete { record } => {
                    record.table == table && record.key == key
                }
            })
        }
        WalPayload::MessageCreated { .. }
        | WalPayload::UserEventPublished { .. }
        | WalPayload::UserUpserted { .. }
        | WalPayload::ObjectCommitted { .. }
        | WalPayload::ObjectDeleted { .. }
        | WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. }
        | WalPayload::ClientMutationRecorded { .. } => false,
    }
}

fn payload_matches_record_key(payload: &WalPayload, key: &str) -> bool {
    match payload {
        WalPayload::MessageCreated { message } => message.id == key,
        WalPayload::UserEventPublished { event } => event.id == key,
        WalPayload::UserUpserted { user } => user.user_id == key,
        WalPayload::ObjectCommitted { object, .. } => object.id == key,
        WalPayload::ObjectDeleted { object_id, .. } => object_id == key,
        WalPayload::RecordUpserted { record } => record.key == key,
        WalPayload::RecordDeleted { record } => record.key == key,
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            operations.iter().any(|operation| match operation {
                DbRecordMutationDraft::Upsert { record } => record.key == key,
                DbRecordMutationDraft::Delete { record } => record.key == key,
            })
        }
        WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. }
        | WalPayload::ClientMutationRecorded { .. } => false,
    }
}

fn payload_matches_path(payload: &WalPayload, path: &str) -> bool {
    match payload {
        WalPayload::MessageCreated { message } => message.path == path,
        WalPayload::UserEventPublished { event } => event.path == path,
        WalPayload::UserUpserted { user } => user.path == path,
        WalPayload::ObjectCommitted { object, .. } => object.path == path,
        WalPayload::ObjectDeleted {
            path: deleted_path, ..
        } => deleted_path == path,
        WalPayload::RecordUpserted { record } => record.path == path,
        WalPayload::RecordDeleted { record } => record.path == path,
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            operations.iter().any(|operation| match operation {
                DbRecordMutationDraft::Upsert { record } => record.path == path,
                DbRecordMutationDraft::Delete { record } => record.path == path,
            })
        }
        WalPayload::ClientMutationRecorded { record, .. } => client_mutation_record_path(record)
            .as_deref()
            .is_some_and(|record_path| record_path == path),
        WalPayload::HostHttpRequested { request } => {
            path == format!("host-http/requests/{}", request.request_id)
        }
        WalPayload::HostHttpCompleted { request_id, .. } => {
            path == format!("host-http/requests/{request_id}")
        }
        WalPayload::SchemaApplied { schema, .. } => {
            path == format!("schema/versions/{}", schema.version)
        }
        WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. } => false,
    }
}

fn payload_matches_client_mutation_id(payload: &WalPayload, client_mutation_id: &str) -> bool {
    match payload {
        WalPayload::MessageCreated { message } => {
            message.client_mutation_id.as_deref() == Some(client_mutation_id)
        }
        WalPayload::UserEventPublished { event } => {
            event.client_mutation_id.as_deref() == Some(client_mutation_id)
        }
        WalPayload::UserUpserted { user } => {
            user.client_mutation_id.as_deref() == Some(client_mutation_id)
        }
        WalPayload::ObjectCommitted {
            client_mutation_id: existing,
            ..
        }
        | WalPayload::ObjectDeleted {
            client_mutation_id: existing,
            ..
        }
        | WalPayload::RecordTransactionCommitted {
            client_mutation_id: existing,
            ..
        } => existing.as_deref() == Some(client_mutation_id),
        WalPayload::RecordUpserted { record } => {
            record.client_mutation_id.as_deref() == Some(client_mutation_id)
        }
        WalPayload::RecordDeleted { record } => {
            record.client_mutation_id.as_deref() == Some(client_mutation_id)
        }
        WalPayload::ClientMutationRecorded {
            client_mutation_id: existing,
            ..
        } => existing == client_mutation_id,
        WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. } => false,
    }
}

fn client_mutation_record_path(record: &ClientMutationRecord) -> Option<String> {
    match record {
        ClientMutationRecord::RecordDeleteNoop { path, .. } => Some(path.clone()),
        ClientMutationRecord::RecordTransactionNoop => None,
        ClientMutationRecord::ObjectDeleteNoop { path, .. } => Some(path.clone()),
    }
}

pub(crate) fn wal_payload_type(payload: &WalPayload) -> &'static str {
    match payload {
        WalPayload::MessageCreated { .. } => "messageCreated",
        WalPayload::UserEventPublished { .. } => "userEventPublished",
        WalPayload::UserUpserted { .. } => "userUpserted",
        WalPayload::ObjectCommitted { .. } => "objectCommitted",
        WalPayload::ObjectDeleted { .. } => "objectDeleted",
        WalPayload::RecordUpserted { .. } => "recordUpserted",
        WalPayload::RecordDeleted { .. } => "recordDeleted",
        WalPayload::RecordTransactionCommitted { .. } => "recordTransactionCommitted",
        WalPayload::SchemaApplied { .. } => "schemaApplied",
        WalPayload::BehaviorPublished { .. } => "behaviorPublished",
        WalPayload::ActorReminderScheduled { .. } => "actorReminderScheduled",
        WalPayload::ActorReminderCancelled { .. } => "actorReminderCancelled",
        WalPayload::ActorReminderFired { .. } => "actorReminderFired",
        WalPayload::HostHttpRequested { .. } => "hostHttpRequested",
        WalPayload::HostHttpCompleted { .. } => "hostHttpCompleted",
        WalPayload::ClientMutationRecorded { .. } => "clientMutationRecorded",
    }
}

fn value_contains_object_id(value: &serde_json::Value, object_id: &str) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            object
                .get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| id == object_id)
                && object
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|path| path.starts_with("objects/"))
                || object
                    .values()
                    .any(|value| value_contains_object_id(value, object_id))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| value_contains_object_id(value, object_id)),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => false,
    }
}
