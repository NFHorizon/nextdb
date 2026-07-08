#![cfg_attr(not(feature = "cluster"), allow(dead_code))]

use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::api::wal::{
    append_ordered_wal_record, cluster_role_for_shard, ensure_shard_not_frozen, maybe_checkpoint,
    writable_wal_shard_for_key,
};
use crate::{
    AppState,
    api::{
        cache::append_cache_invalidation,
        error::ApiError,
        events::publish_delivery_event,
        guards::{ensure_bytes_limit, ensure_shard_index},
        mutation::{CommittedMutation, find_committed_mutation, normalize_client_mutation_id},
        runtime::{begin_runtime_write, ensure_runtime_accepting_writes},
    },
    cache_control::ClientCacheInvalidationScope,
    cluster::ShardRole,
    model::{
        ClientMutationRecord, DbRecord, DeliveryEvent, Durability, ObjectMetadata, WalPayload,
    },
    object_refs::{
        DeclaredObjectRef, ObjectReferences, collect_declared_object_refs,
        collect_declared_object_refs_from_type,
    },
    object_store::ensure_safe_object_id,
    schema::DatabaseSchema,
    util::{normalize_limit, now_ms, shard_index},
    wal::{self, WalRemoteAckPolicy},
};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PutObjectQuery {
    pub(crate) content_type: Option<String>,
    pub(crate) object_id: Option<String>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListObjectsQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) after_id: Option<String>,
    pub(crate) shard: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectGcQuery {
    pub(crate) dry_run: Option<bool>,
    pub(crate) force: Option<bool>,
    pub(crate) grace_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteObjectQuery {
    pub(crate) force: Option<bool>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteObjectResponse {
    pub(crate) object_id: String,
    pub(crate) deleted: bool,
    pub(crate) lsn: u64,
    pub(crate) deleted_at_ms: Option<u64>,
    pub(crate) path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectGcResponse {
    pub(crate) dry_run: bool,
    pub(crate) force: bool,
    pub(crate) grace_ms: u64,
    pub(crate) deleted: Vec<String>,
    pub(crate) retained: Vec<String>,
    pub(crate) protected: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectRepairQuery {
    pub(crate) shard: Option<usize>,
    pub(crate) object_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectRepairResponse {
    pub(crate) repaired: Vec<ObjectRepairReport>,
    pub(crate) current_lsn: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectRepairReport {
    pub(crate) shard: usize,
    pub(crate) object_id: Option<String>,
    pub(crate) remote_ack_policy: WalRemoteAckPolicy,
    pub(crate) remote_required_acks: usize,
    pub(crate) remote_replica_count: usize,
    pub(crate) repaired_replicas: usize,
    pub(crate) objects_sent: usize,
    pub(crate) satisfied: bool,
    pub(crate) replicas: Vec<ObjectRepairReplicaReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectRepairReplicaReport {
    pub(crate) url: String,
    pub(crate) ok: bool,
    pub(crate) sent: usize,
    pub(crate) stored: usize,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectReplicateResponse {
    pub(crate) object: ObjectMetadata,
    pub(crate) stored: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListObjectsResponse {
    pub(crate) objects: Vec<ObjectMetadata>,
    pub(crate) next_after_id: Option<String>,
    pub(crate) has_more: bool,
}

pub(crate) async fn list_objects(
    State(state): State<AppState>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Json<ListObjectsResponse>, ApiError> {
    if query
        .after_id
        .as_deref()
        .is_some_and(|id| !ensure_safe_object_id(id))
    {
        return Err(ApiError::bad_request("invalid afterId"));
    }
    if let Some(shard) = query.shard {
        ensure_shard_index(&state, shard)?;
    }
    let limit = normalize_limit(query.limit);
    let mut objects = state
        .objects
        .list_metadata()
        .await
        .map_err(ApiError::internal)?;
    objects.retain(|object| match query.shard {
        Some(shard) => shard_index(&object.id, state.cluster.shard_count()) == shard,
        None => true,
    });
    objects.retain(|object| match query.after_id.as_deref() {
        Some(after_id) => object.id.as_str() > after_id,
        None => true,
    });
    let has_more = objects.len() > limit;
    if has_more {
        objects.truncate(limit);
    }
    let next_after_id = objects.last().map(|object| object.id.clone());
    Ok(Json(ListObjectsResponse {
        objects,
        next_after_id,
        has_more,
    }))
}

pub(crate) async fn get_object_metadata(
    State(state): State<AppState>,
    AxumPath(object_id): AxumPath<String>,
) -> Result<Json<ObjectMetadata>, ApiError> {
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid object id"));
    }
    if !state.objects.metadata_exists(&object_id) {
        return Err(ApiError::not_found("object not found"));
    }
    let metadata = state
        .objects
        .metadata(&object_id)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(metadata))
}

pub(crate) async fn get_object_references(
    State(state): State<AppState>,
    AxumPath(object_id): AxumPath<String>,
) -> Result<Json<ObjectReferences>, ApiError> {
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid object id"));
    }
    let object_exists = state.objects.metadata_exists(&object_id);
    Ok(Json(
        state
            .object_refs
            .references_for(&object_id)
            .await
            .with_object_exists(object_exists),
    ))
}

pub(crate) async fn validate_behavior_input_object_refs(
    state: &AppState,
    behavior: &str,
    mutation: &str,
    value: &serde_json::Value,
) -> Result<(), ApiError> {
    let schema = state.schema.schema();
    let field = schema
        .behaviors
        .get(behavior)
        .and_then(|behavior_schema| behavior_schema.mutations.get(mutation))
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "schema missing behavior input {behavior}.{mutation}"
            ))
        })?;
    let mut refs = Vec::new();
    collect_declared_object_refs_from_type(
        &format!("behavior.{behavior}.{mutation}"),
        &field.field_type,
        value,
        &mut refs,
    )?;
    validate_declared_object_refs_exist(state, refs).await
}

pub(crate) async fn validate_event_payload_object_refs(
    state: &AppState,
    event_name: &str,
    value: &serde_json::Value,
) -> Result<(), ApiError> {
    let schema = state.schema.schema();
    let Some(event) = schema.events.get(event_name) else {
        return Ok(());
    };
    let mut refs = Vec::new();
    collect_declared_object_refs_from_type(
        &format!("events.{event_name}.payload"),
        &event.payload.field_type,
        value,
        &mut refs,
    )?;
    validate_declared_object_refs_exist(state, refs).await
}

pub(crate) async fn validate_records_object_refs_against_schema(
    state: &AppState,
    schema: &DatabaseSchema,
    records: &[DbRecord],
) -> Result<(), ApiError> {
    for record in records {
        validate_record_object_refs_against_schema(state, schema, record).await?;
    }
    Ok(())
}

async fn validate_record_object_refs_against_schema(
    state: &AppState,
    schema: &DatabaseSchema,
    record: &DbRecord,
) -> Result<(), ApiError> {
    let refs = if let Some((table, nested)) = record.table.split_once('.') {
        let Some(table_schema) = schema.tables.get(table) else {
            return Ok(());
        };
        let Some(nested_schema) = table_schema.nested.get(nested) else {
            return Ok(());
        };
        collect_declared_object_refs(
            &format!("{} in tables.{table}.nested.{nested}", record.path),
            &nested_schema.fields,
            &record.value,
        )?
    } else {
        let Some(table_schema) = schema.tables.get(&record.table) else {
            return Ok(());
        };
        collect_declared_object_refs(
            &format!("{} in tables.{}", record.path, record.table),
            &table_schema.fields,
            &record.value,
        )?
    };
    validate_declared_object_refs_exist(state, refs).await
}

pub(crate) async fn validate_declared_object_refs_exist(
    state: &AppState,
    refs: Vec<DeclaredObjectRef>,
) -> Result<(), ApiError> {
    for object_ref in refs {
        if !ensure_safe_object_id(&object_ref.id) {
            return Err(ApiError::bad_request(format!(
                "{}.id must be a safe object id",
                object_ref.path
            )));
        }
        if !state.objects.metadata_exists(&object_ref.id) {
            return Err(ApiError::not_found(format!(
                "{} object ref not found: {}",
                object_ref.path, object_ref.id
            )));
        }
        let metadata = state
            .objects
            .metadata(&object_ref.id)
            .await
            .map_err(ApiError::internal)?;
        if metadata.path != object_ref.object_path
            || metadata.content_type != object_ref.content_type
            || metadata.byte_size != object_ref.byte_size
            || metadata.sha256 != object_ref.sha256
        {
            return Err(ApiError::bad_request(format!(
                "{} object ref metadata does not match object {}",
                object_ref.path, object_ref.id
            )));
        }
    }
    Ok(())
}

pub(crate) async fn put_object(
    State(state): State<AppState>,
    Query(query): Query<PutObjectQuery>,
    body: Bytes,
) -> Result<Json<ObjectMetadata>, ApiError> {
    let metadata = commit_object_put(
        &state,
        query.object_id,
        query
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        body,
        query.client_mutation_id,
    )
    .await?;
    Ok(Json(metadata))
}

pub(crate) async fn commit_object_put(
    state: &AppState,
    object_id: Option<String>,
    content_type: String,
    body: Bytes,
    client_mutation_id: Option<String>,
) -> Result<ObjectMetadata, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())? {
        match existing {
            CommittedMutation::ObjectCommitted { object } => return Ok(object),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    ensure_bytes_limit(
        "object body",
        body.len() as u64,
        state.limits.max_object_bytes,
    )?;
    let object_id = object_id.unwrap_or_else(|| Uuid::now_v7().to_string());
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid objectId"));
    }
    if state.objects.metadata_exists(&object_id) {
        return Err(ApiError::conflict("object id already exists"));
    }
    let shard = writable_wal_shard_for_key(state, &object_id).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let _write = begin_runtime_write(state).await?;
    let metadata = state
        .objects
        .put_with_id(object_id, content_type, body.clone())
        .await
        .map_err(ApiError::internal)?;
    if let Err(err) = replicate_object_to_remotes(state, shard.index, &metadata, body).await {
        rollback_uncommitted_object(state, &metadata.id).await;
        return Err(err);
    }

    let record = match append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::ObjectCommitted {
            object: metadata.clone(),
            client_mutation_id,
        },
    )
    .await
    {
        Ok(record) => record,
        Err(err) => {
            rollback_uncommitted_object(state, &metadata.id).await;
            return Err(err);
        }
    };
    publish_delivery_event(
        state,
        DeliveryEvent::ObjectCommitted {
            object: metadata.clone(),
            lsn: record.lsn,
        },
    );
    maybe_checkpoint(state).await?;

    Ok(metadata)
}

async fn rollback_uncommitted_object(state: &AppState, object_id: &str) {
    if let Err(err) = state.objects.delete_object(object_id).await {
        warn!("failed to roll back uncommitted object {object_id}: {err}");
    }
}

pub(crate) async fn delete_object(
    State(state): State<AppState>,
    AxumPath(object_id): AxumPath<String>,
    Query(query): Query<DeleteObjectQuery>,
) -> Result<Json<DeleteObjectResponse>, ApiError> {
    Ok(Json(
        commit_object_delete(
            &state,
            object_id,
            query.force.unwrap_or(false),
            query.client_mutation_id,
        )
        .await?,
    ))
}

pub(crate) async fn commit_object_delete(
    state: &AppState,
    object_id: String,
    force: bool,
    client_mutation_id: Option<String>,
) -> Result<DeleteObjectResponse, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid object id"));
    }
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())? {
        match existing {
            CommittedMutation::ObjectDeleted { response } => return Ok(response),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }

    let path = format!("objects/{object_id}");
    if !state.objects.metadata_exists(&object_id) {
        if let Some(client_mutation_id) = client_mutation_id {
            let _write = begin_runtime_write(state).await?;
            let shard = writable_wal_shard_for_key(state, &object_id).await?;
            ensure_shard_not_frozen(state, shard.index).await?;
            let wal_record = append_ordered_wal_record(
                state,
                shard,
                Durability::Strict,
                state.schema.version(),
                WalPayload::ClientMutationRecorded {
                    client_mutation_id,
                    record: ClientMutationRecord::ObjectDeleteNoop {
                        object_id: object_id.clone(),
                        path: path.clone(),
                    },
                },
            )
            .await?;
            maybe_checkpoint(state).await?;
            return Ok(DeleteObjectResponse {
                object_id,
                deleted: false,
                lsn: wal_record.lsn,
                deleted_at_ms: None,
                path,
            });
        }
        return Ok(DeleteObjectResponse {
            object_id,
            deleted: false,
            lsn: 0,
            deleted_at_ms: None,
            path,
        });
    }

    let references = state.object_refs.references_for(&object_id).await;
    if !force && references.ref_count > 0 {
        return Err(ApiError::conflict(format!(
            "object is still referenced by {} source(s)",
            references.ref_count
        )));
    }

    let shard = writable_wal_shard_for_key(state, &object_id).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let deleted_at_ms = now_ms();
    let _write = begin_runtime_write(state).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        Durability::Strict,
        state.schema.version(),
        WalPayload::ObjectDeleted {
            object_id: object_id.clone(),
            deleted_at_ms,
            path: path.clone(),
            force,
            client_mutation_id,
        },
    )
    .await?;

    state
        .objects
        .delete_object(&object_id)
        .await
        .map_err(ApiError::internal)?;
    append_cache_invalidation(
        state,
        ClientCacheInvalidationScope::Object,
        Some(object_id.clone()),
        None,
        None,
        None,
        wal_record.lsn,
        "object deleted".to_string(),
    )
    .await?;
    publish_delivery_event(
        state,
        DeliveryEvent::ObjectDeleted {
            object_id: object_id.clone(),
            deleted_at_ms,
            lsn: wal_record.lsn,
            path: path.clone(),
            force,
        },
    );
    maybe_checkpoint(state).await?;

    Ok(DeleteObjectResponse {
        object_id,
        deleted: true,
        lsn: wal_record.lsn,
        deleted_at_ms: Some(deleted_at_ms),
        path,
    })
}

pub(crate) async fn gc_objects(
    State(state): State<AppState>,
    Query(query): Query<ObjectGcQuery>,
) -> Result<Json<ObjectGcResponse>, ApiError> {
    let dry_run = query.dry_run.unwrap_or(true);
    let force = query.force.unwrap_or(false);
    let grace_ms = query.grace_ms.unwrap_or(state.object_gc_grace_ms);
    let now = now_ms();
    let referenced = state.object_refs.referenced_ids().await;
    let objects = state
        .objects
        .list_metadata()
        .await
        .map_err(ApiError::internal)?;

    let mut retained = Vec::new();
    let mut deleted = Vec::new();
    let mut protected = Vec::new();
    for object in objects {
        if referenced.contains(&object.id) {
            retained.push(object.id);
        } else if !force && now.saturating_sub(object.created_at_ms) < grace_ms {
            protected.push(object.id);
        } else {
            if !dry_run {
                state
                    .objects
                    .delete_object(&object.id)
                    .await
                    .map_err(ApiError::internal)?;
            }
            deleted.push(object.id);
        }
    }

    Ok(Json(ObjectGcResponse {
        dry_run,
        force,
        grace_ms,
        deleted,
        retained,
        protected,
    }))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectByteRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

pub(crate) async fn get_object_body(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(object_id): AxumPath<String>,
) -> Result<Response, ApiError> {
    if !ensure_safe_object_id(&object_id) {
        return Err(ApiError::bad_request("invalid object id"));
    }
    if !state.objects.metadata_exists(&object_id) {
        return Err(ApiError::not_found("object not found"));
    }
    let metadata = state
        .objects
        .metadata(&object_id)
        .await
        .map_err(ApiError::internal)?;
    let range = parse_object_range_header(headers.get(header::RANGE), metadata.byte_size)?;
    if let Some(range) = range {
        let (_, body) = state
            .objects
            .body_range(&object_id, range.start, range.end)
            .await
            .map_err(ApiError::internal)?;
        return object_body_response(StatusCode::PARTIAL_CONTENT, metadata, body, Some(range));
    }

    let (_, body) = state
        .objects
        .body(&object_id)
        .await
        .map_err(ApiError::internal)?;
    object_body_response(StatusCode::OK, metadata, body, None)
}

fn parse_object_range_header(
    header_value: Option<&axum::http::HeaderValue>,
    total_size: u64,
) -> Result<Option<ObjectByteRange>, ApiError> {
    let Some(header_value) = header_value else {
        return Ok(None);
    };
    let value = header_value
        .to_str()
        .map_err(|_| ApiError::range_not_satisfiable(total_size, "Range header must be ASCII"))?
        .trim();
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "only bytes ranges are supported",
        ));
    };
    if spec.contains(',') {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "multiple ranges are not supported",
        ));
    }
    let Some((start_raw, end_raw)) = spec.split_once('-') else {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "invalid Range header",
        ));
    };
    if total_size == 0 {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "object body is empty",
        ));
    }
    if start_raw.is_empty() {
        let suffix_len = end_raw
            .parse::<u64>()
            .map_err(|_| ApiError::range_not_satisfiable(total_size, "invalid suffix range"))?;
        if suffix_len == 0 {
            return Err(ApiError::range_not_satisfiable(
                total_size,
                "suffix range must be positive",
            ));
        }
        let start = total_size.saturating_sub(suffix_len);
        return Ok(Some(ObjectByteRange {
            start,
            end: total_size - 1,
        }));
    }
    let start = start_raw
        .parse::<u64>()
        .map_err(|_| ApiError::range_not_satisfiable(total_size, "invalid range start"))?;
    if start >= total_size {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "range starts past object end",
        ));
    }
    let end = if end_raw.is_empty() {
        total_size - 1
    } else {
        end_raw
            .parse::<u64>()
            .map_err(|_| ApiError::range_not_satisfiable(total_size, "invalid range end"))?
            .min(total_size - 1)
    };
    if start > end {
        return Err(ApiError::range_not_satisfiable(
            total_size,
            "range start is after range end",
        ));
    }
    Ok(Some(ObjectByteRange { start, end }))
}

fn object_body_response(
    status: StatusCode,
    metadata: ObjectMetadata,
    body: Bytes,
    range: Option<ObjectByteRange>,
) -> Result<Response, ApiError> {
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, metadata.content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, body.len().to_string());
    if let Some(range) = range {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end, metadata.byte_size),
        );
    }
    builder
        .body(Body::from(body))
        .map_err(|err| ApiError::internal(err.into()))
}

pub(crate) async fn replicate_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ObjectReplicateResponse>, ApiError> {
    authorize_object_replication(&state, &headers)?;
    let metadata_header = headers
        .get("x-nextdb-object-metadata")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::bad_request("missing x-nextdb-object-metadata header"))?;
    let metadata: ObjectMetadata = serde_json::from_str(metadata_header)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let stored = state
        .objects
        .put_replicated(metadata.clone(), body)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(ObjectReplicateResponse {
        object: metadata,
        stored,
    }))
}

pub(crate) async fn replicate_object_to_remotes(
    state: &AppState,
    shard: usize,
    metadata: &ObjectMetadata,
    body: Bytes,
) -> Result<(), ApiError> {
    let remote_replica_urls = object_remote_replica_urls_for_shard(state, shard).await;
    if remote_replica_urls.is_empty() {
        return Ok(());
    }

    let required_acks = state
        .wal_shards
        .get(shard)
        .map(|shard| wal::required_remote_acks(shard.remote_ack_policy, remote_replica_urls.len()))
        .unwrap_or(remote_replica_urls.len());
    let http = reqwest::Client::new();
    let metadata_json =
        serde_json::to_string(metadata).map_err(|err| ApiError::internal(err.into()))?;
    let mut acked = 0_usize;
    let mut errors = Vec::new();
    for replica in &remote_replica_urls {
        let endpoint = object_replication_endpoint(replica);
        let mut builder = http
            .post(&endpoint)
            .header("x-nextdb-object-metadata", metadata_json.as_str())
            .body(body.clone());
        if let Some(token) = &state.object_replication_token {
            builder = builder
                .bearer_auth(token)
                .header("x-nextdb-object-replication-token", token);
        }
        match builder.send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    acked += 1;
                } else {
                    let text = response.text().await.unwrap_or_default();
                    errors.push(format!("{endpoint}: {status} {text}"));
                }
            }
            Err(err) => {
                errors.push(format!("{endpoint}: {err}"));
            }
        }
    }

    if acked < required_acks {
        return Err(ApiError::bad_request(format!(
            "object replica ack policy {:?} requires {required_acks} acks, got {acked}: {}",
            state
                .wal_shards
                .get(shard)
                .map(|shard| shard.remote_ack_policy)
                .unwrap_or(WalRemoteAckPolicy::All),
            errors.join("; ")
        )));
    }

    Ok(())
}

pub(crate) async fn repair_objects(
    State(state): State<AppState>,
    Query(query): Query<ObjectRepairQuery>,
) -> Result<Json<ObjectRepairResponse>, ApiError> {
    if let Some(object_id) = query.object_id.as_deref()
        && !ensure_safe_object_id(object_id)
    {
        return Err(ApiError::bad_request("invalid objectId"));
    }
    let shards: Vec<usize> = match query.shard {
        Some(shard) => {
            ensure_shard_index(&state, shard)?;
            vec![shard]
        }
        None => (0..state.wal_shards.len()).collect(),
    };
    let mut repaired = Vec::with_capacity(shards.len());
    for shard in shards {
        repaired.push(repair_objects_for_shard(&state, shard, query.object_id.as_deref()).await?);
    }
    Ok(Json(ObjectRepairResponse {
        repaired,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
    }))
}

pub(crate) async fn repair_objects_for_shard(
    state: &AppState,
    shard: usize,
    object_id: Option<&str>,
) -> Result<ObjectRepairReport, ApiError> {
    ensure_shard_index(state, shard)?;
    let remote_replica_urls = object_remote_replica_urls_for_shard(state, shard).await;
    let remote_ack_policy = state
        .wal_shards
        .get(shard)
        .map(|shard| shard.remote_ack_policy)
        .unwrap_or(WalRemoteAckPolicy::All);
    let required_acks = wal::required_remote_acks(remote_ack_policy, remote_replica_urls.len());
    let objects = state
        .objects
        .list_metadata()
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|object| object_id.is_none_or(|object_id| object.id == object_id))
        .filter(|object| shard_index(&object.id, state.cluster.shard_count()) == shard)
        .collect::<Vec<_>>();

    let mut replicas = Vec::with_capacity(remote_replica_urls.len());
    let mut repaired_replicas = 0_usize;
    let mut objects_sent = 0_usize;
    let http = reqwest::Client::new();
    for replica in remote_replica_urls {
        let endpoint = object_replication_endpoint(&replica);
        let mut sent = 0_usize;
        let mut stored = 0_usize;
        let mut error = None;
        for object in &objects {
            let (_, body) = state
                .objects
                .body(&object.id)
                .await
                .map_err(ApiError::internal)?;
            match replicate_object_to_endpoint(
                &http,
                &endpoint,
                object,
                body,
                state.object_replication_token.as_ref(),
            )
            .await
            {
                Ok(was_stored) => {
                    sent += 1;
                    if was_stored {
                        stored += 1;
                    }
                }
                Err(err) => {
                    error = Some(err.to_string());
                    break;
                }
            }
        }
        let ok = error.is_none();
        if ok {
            repaired_replicas += 1;
            objects_sent += sent;
        }
        replicas.push(ObjectRepairReplicaReport {
            url: endpoint,
            ok,
            sent,
            stored,
            error,
        });
    }

    Ok(ObjectRepairReport {
        shard,
        object_id: object_id.map(ToOwned::to_owned),
        remote_ack_policy,
        remote_required_acks: required_acks,
        remote_replica_count: replicas.len(),
        repaired_replicas,
        objects_sent,
        satisfied: repaired_replicas >= required_acks,
        replicas,
    })
}

pub(crate) async fn object_remote_replica_urls_for_shard(
    state: &AppState,
    shard: usize,
) -> Vec<String> {
    if !crate::cluster::cluster_enabled() {
        return Vec::new();
    }

    if let Some(urls) = &state.explicit_object_remote_replica_urls {
        return urls.clone();
    }

    let overrides = state.topology_overrides.read().await;
    state
        .cluster
        .replicas_for_shard_with_overrides(shard, &overrides)
        .into_iter()
        .filter(|node_id| node_id != state.cluster.node_id())
        .filter_map(|node_id| state.cluster.node_url_for(&node_id))
        .collect()
}

async fn replicate_object_to_endpoint(
    http: &reqwest::Client,
    endpoint: &str,
    metadata: &ObjectMetadata,
    body: Bytes,
    token: Option<&String>,
) -> Result<bool> {
    let metadata_json = serde_json::to_string(metadata)?;
    let mut builder = http
        .post(endpoint)
        .header("x-nextdb-object-metadata", metadata_json)
        .body(body);
    if let Some(token) = token {
        builder = builder
            .bearer_auth(token)
            .header("x-nextdb-object-replication-token", token);
    }
    let response = builder
        .send()
        .await
        .with_context(|| format!("replicate object {} to {endpoint}", metadata.id))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "object replica rejected object {}: {status} {text}",
            metadata.id
        );
    }
    let body = response
        .json::<ObjectReplicateResponse>()
        .await
        .with_context(|| format!("decode object replica response from {endpoint}"))?;
    Ok(body.stored)
}

fn authorize_object_replication(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = &state.object_replication_token else {
        return Ok(());
    };

    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let token = headers
        .get("x-nextdb-object-replication-token")
        .and_then(|value| value.to_str().ok())
        .or(bearer);

    if token == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(ApiError::unauthorized("invalid object replication token"))
    }
}

fn object_replication_endpoint(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/objects/replicate") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/objects/replicate")
    }
}

pub(crate) async fn run_object_repair_controller_once(state: &AppState) -> Result<(), ApiError> {
    let mut last_shards = Vec::new();
    let mut last_objects_sent = 0_usize;
    let mut last_repaired_replicas = 0_usize;
    let mut last_satisfied = true;
    let mut last_error = None;

    for shard in 0..state.wal_shards.len() {
        if cluster_role_for_shard(state, shard).await != ShardRole::Owner {
            continue;
        }
        let report = repair_objects_for_shard(state, shard, None).await?;
        last_shards.push(shard);
        last_objects_sent += report.objects_sent;
        last_repaired_replicas += report.repaired_replicas;
        if !report.satisfied {
            last_satisfied = false;
            if last_error.is_none() {
                last_error = Some(format!(
                    "shard {shard} object repair satisfied {}/{} remote ack(s)",
                    report.repaired_replicas, report.remote_required_acks
                ));
            }
        }
    }

    let mut controller = state.object_repair_controller.write().await;
    controller.last_run_at_ms = Some(now_ms());
    controller.last_shards = last_shards;
    controller.last_objects_sent = last_objects_sent;
    controller.last_repaired_replicas = last_repaired_replicas;
    controller.last_satisfied = last_satisfied;
    controller.last_error = last_error;
    Ok(())
}
