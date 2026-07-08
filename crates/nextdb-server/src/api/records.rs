use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::api::wal::{
    append_ordered_wal_record, ensure_shard_not_frozen, maybe_checkpoint,
    writable_wal_shard_for_key,
};

const RECORD_READ_CONSISTENCY_WAIT_MS: u64 = 5_000;
#[cfg(test)]
const RECORD_READ_CONSISTENCY_TEST_WAIT_MS: u64 = 50;
use crate::{
    AppState,
    api::{
        error::ApiError,
        events::publish_delivery_event,
        guards::{ensure_json_value_limit, ensure_shard_index},
        mutation::{CommittedMutation, find_committed_mutation, normalize_client_mutation_id},
        objects::validate_declared_object_refs_exist,
        runtime::{begin_runtime_write, ensure_runtime_accepting_writes},
    },
    config::{MAX_RECORD_BATCH_OPERATIONS, MAX_RECORD_TRANSACTION_OPERATIONS},
    model::{
        ClientMutationRecord, DbRecord, DbRecordDeleteDraft, DbRecordDraft, DbRecordMutationDraft,
        DeliveryEvent, Durability, WalPayload, WalRecord,
    },
    object_refs::collect_declared_object_refs,
    record_hot::RecordHotKeyOrderOverlay,
    record_projection::RecordProjectionMutation,
    record_store::{
        IndexedDbRecord, OrderedDbRecord, RecordOrderTerm, compare_index_values,
        ensure_safe_record_component, index_range_cursor, order_record_cursor,
        parse_index_range_cursor, parse_record_order_terms, record_index_values,
    },
    schema::{DatabaseSchema, IndexSchema, StorageClass},
    util::{hex_lower, normalize_limit, now_ms, shard_index},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpsertRecordRequest {
    pub(crate) value: serde_json::Value,
    #[serde(default)]
    pub(crate) durability: Durability,
    pub(crate) expected_lsn: Option<u64>,
    pub(crate) client_mutation_id: Option<String>,
}

pub(crate) async fn upsert_record(
    State(state): State<AppState>,
    axum::extract::Path((table, key)): axum::extract::Path<(String, String)>,
    Json(request): Json<UpsertRecordRequest>,
) -> Result<Json<RecordResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    let record = commit_record_upsert(
        &state,
        table,
        key,
        request.value,
        request.durability,
        request.expected_lsn,
        request.client_mutation_id,
    )
    .await?;
    Ok(Json(RecordResponse { record }))
}

pub(crate) async fn delete_record(
    State(state): State<AppState>,
    axum::extract::Path((table, key)): axum::extract::Path<(String, String)>,
    Query(query): Query<DeleteRecordQuery>,
) -> Result<Json<DeleteRecordResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    let response = commit_record_delete(
        &state,
        table,
        key,
        query.durability,
        query.expected_lsn,
        query.client_mutation_id,
    )
    .await?;
    Ok(Json(response))
}

pub(crate) async fn get_record(
    State(state): State<AppState>,
    axum::extract::Path((table, key)): axum::extract::Path<(String, String)>,
    Query(query): Query<RecordReadConsistencyQuery>,
) -> Result<Json<RecordResponse>, ApiError> {
    validate_record_path(&table, &key, &state)?;
    resolve_record_read_consistency(&state, &query).await?;
    let record = get_record_from_live_or_disk(&state, &table, &key)
        .await?
        .ok_or_else(|| ApiError::not_found("record not found"))?;
    Ok(Json(RecordResponse { record }))
}

pub(crate) async fn list_records(
    State(state): State<AppState>,
    axum::extract::Path(table): axum::extract::Path<String>,
    Query(query): Query<ListRecordsQuery>,
) -> Result<Json<ListRecordsResponse>, ApiError> {
    resolve_record_read_consistency(&state, &query.consistency).await?;
    execute_record_list_query(&state, table, None, None, query)
        .await
        .map(Json)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListRecordsQuery {
    #[serde(flatten)]
    pub(crate) consistency: RecordReadConsistencyQuery,
    pub(crate) after_key: Option<String>,
    pub(crate) after_cursor: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) order: Option<String>,
    pub(crate) shard: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_record_predicate")]
    pub(crate) predicate: Option<RecordPredicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryRecordsByIndexQuery {
    #[serde(flatten)]
    pub(crate) consistency: RecordReadConsistencyQuery,
    pub(crate) value: Option<String>,
    pub(crate) values: Option<String>,
    pub(crate) lower: Option<String>,
    pub(crate) upper: Option<String>,
    pub(crate) lower_values: Option<String>,
    pub(crate) upper_values: Option<String>,
    pub(crate) after_key: Option<String>,
    pub(crate) after_cursor: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) shard: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_record_predicate")]
    pub(crate) predicate: Option<RecordPredicate>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RecordReadConsistency {
    #[serde(alias = "local")]
    Eventual,
    #[serde(alias = "readYourWrites")]
    ReadYourWrites,
    Strong,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordReadConsistencyQuery {
    #[serde(default)]
    pub(crate) consistency: Option<RecordReadConsistency>,
    #[serde(default)]
    pub(crate) min_lsn: Option<u64>,
}

impl Default for RecordReadConsistencyQuery {
    fn default() -> Self {
        Self {
            consistency: None,
            min_lsn: None,
        }
    }
}

pub(crate) async fn resolve_record_read_consistency(
    state: &AppState,
    query: &RecordReadConsistencyQuery,
) -> Result<(), ApiError> {
    let level = query.consistency.unwrap_or(RecordReadConsistency::Eventual);
    let current_lsn = state.current_lsn.load(std::sync::atomic::Ordering::Acquire);
    let wait_lsn = match level {
        RecordReadConsistency::Eventual => None,
        RecordReadConsistency::ReadYourWrites => Some(query.min_lsn.ok_or_else(|| {
            ApiError::bad_request("read-your-writes consistency requires minLsn")
        })?),
        RecordReadConsistency::Strong => Some(current_lsn),
    };
    if let Some(lsn) = wait_lsn {
        tokio::time::timeout(
            Duration::from_millis(record_read_consistency_wait_ms()),
            state.record_projection_applier.wait_for_lsn(lsn),
        )
        .await
        .map_err(|_| {
            ApiError::unavailable_with_details(
                format!("record projection did not catch up to LSN {lsn}"),
                serde_json::json!({
                    "consistency": level,
                    "minLsn": query.min_lsn,
                    "waitedForLsn": lsn,
                    "currentLsn": current_lsn,
                }),
            )
        })?
        .map_err(ApiError::internal)?;
    }
    Ok(())
}

fn record_read_consistency_wait_ms() -> u64 {
    if cfg!(test) {
        #[cfg(test)]
        {
            return RECORD_READ_CONSISTENCY_TEST_WAIT_MS;
        }
    }
    RECORD_READ_CONSISTENCY_WAIT_MS
}

impl QueryRecordsByIndexQuery {
    pub(crate) fn is_range_query(&self) -> bool {
        self.lower.is_some()
            || self.upper.is_some()
            || self.lower_values.is_some()
            || self.upper_values.is_some()
            || self.after_cursor.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordPredicate {
    #[serde(default)]
    pub(crate) all: Vec<RecordPredicateTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordPredicateTerm {
    pub(crate) field: String,
    pub(crate) op: RecordPredicateOp,
    #[serde(default)]
    pub(crate) value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RecordPredicateOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Contains,
    StartsWith,
    Exists,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteRecordQuery {
    #[serde(default)]
    pub(crate) durability: Durability,
    pub(crate) expected_lsn: Option<u64>,
    pub(crate) client_mutation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordTransactionRequest {
    #[serde(default)]
    pub(crate) durability: Durability,
    pub(crate) client_mutation_id: Option<String>,
    pub(crate) operations: Vec<RecordTransactionOperationRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordBatchRequest {
    #[serde(default)]
    pub(crate) durability: Durability,
    pub(crate) client_mutation_id: Option<String>,
    pub(crate) operations: Vec<RecordTransactionOperationRequest>,
}

pub(crate) async fn record_transaction(
    State(state): State<AppState>,
    Json(request): Json<RecordTransactionRequest>,
) -> Result<Json<RecordTransactionResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    let response = commit_record_transaction(&state, request).await?;
    Ok(Json(response))
}

pub(crate) async fn record_batch(
    State(state): State<AppState>,
    Json(request): Json<RecordBatchRequest>,
) -> Result<Json<RecordBatchResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    let response = commit_record_batch(&state, request).await?;
    Ok(Json(response))
}

pub(crate) async fn commit_record_transaction(
    state: &AppState,
    request: RecordTransactionRequest,
) -> Result<RecordTransactionResponse, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    if request.durability == Durability::Volatile {
        return Err(ApiError::bad_request(
            "record transactions cannot be volatile; use realtime events for lossy data",
        ));
    }
    if request.operations.is_empty() {
        return Err(ApiError::bad_request(
            "record transaction requires at least one operation",
        ));
    }
    if request.operations.len() > MAX_RECORD_TRANSACTION_OPERATIONS {
        return Err(ApiError::bad_request(
            "record transaction supports at most 500 operations",
        ));
    }
    let client_mutation_id = normalize_client_mutation_id(request.client_mutation_id.clone())?;
    if let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())? {
        match existing {
            CommittedMutation::RecordTransactionCommitted { response } => return Ok(response),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }

    let prepared = prepare_record_transaction(state, request).await?;
    if prepared.operations.is_empty() {
        if let Some(client_mutation_id) = client_mutation_id {
            let _write = begin_runtime_write(state).await?;
            let shard = state
                .wal_shards
                .get(prepared.shard)
                .ok_or_else(|| ApiError::internal(anyhow::anyhow!("prepared shard is missing")))?;
            ensure_shard_not_frozen(state, shard.index).await?;
            let wal_record = append_ordered_wal_record(
                state,
                shard,
                prepared.durability,
                state.schema.version(),
                WalPayload::ClientMutationRecorded {
                    client_mutation_id,
                    record: ClientMutationRecord::RecordTransactionNoop,
                },
            )
            .await?;
            maybe_checkpoint(state).await?;
            return Ok(RecordTransactionResponse {
                lsn: wal_record.lsn,
                operations: Vec::new(),
            });
        }
        return Ok(RecordTransactionResponse {
            lsn: 0,
            operations: Vec::new(),
        });
    }

    let _write = begin_runtime_write(state).await?;
    let shard = state
        .wal_shards
        .get(prepared.shard)
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("prepared shard is missing")))?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        prepared.durability,
        state.schema.version(),
        WalPayload::RecordTransactionCommitted {
            operations: prepared.operations.clone(),
            client_mutation_id,
        },
    )
    .await?;

    let operations =
        apply_record_transaction_operations(state, prepared.operations, wal_record.lsn, true)
            .await?;
    maybe_checkpoint(state).await?;
    Ok(RecordTransactionResponse {
        lsn: wal_record.lsn,
        operations,
    })
}

pub(crate) async fn commit_record_batch(
    state: &AppState,
    request: RecordBatchRequest,
) -> Result<RecordBatchResponse, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    if request.durability == Durability::Volatile {
        return Err(ApiError::bad_request(
            "record batches cannot be volatile; use realtime events for lossy data",
        ));
    }
    if request.operations.is_empty() {
        return Err(ApiError::bad_request(
            "record batch requires at least one operation",
        ));
    }
    if request.operations.len() > MAX_RECORD_BATCH_OPERATIONS {
        return Err(ApiError::bad_request(format!(
            "record batch supports at most {MAX_RECORD_BATCH_OPERATIONS} operations"
        )));
    }

    let client_mutation_id = normalize_client_mutation_id(request.client_mutation_id)?;
    let mut seen_keys = BTreeSet::new();
    let mut groups: BTreeMap<usize, Vec<RecordTransactionOperationRequest>> = BTreeMap::new();
    for operation in request.operations {
        ensure_record_batch_key_once(&mut seen_keys, &operation)?;
        let shard_key = record_transaction_operation_shard_key(&operation);
        let shard = shard_index(&shard_key, state.cluster.shard_count());
        groups.entry(shard).or_default().push(operation);
    }

    let split_transaction = groups.len() > 1
        || groups
            .values()
            .any(|operations| operations.len() > MAX_RECORD_TRANSACTION_OPERATIONS);
    let mut highest_lsn = 0;
    let mut transaction_count = 0;
    let mut responses = Vec::new();

    for (shard, operations) in groups {
        for (chunk_index, chunk) in operations
            .chunks(MAX_RECORD_TRANSACTION_OPERATIONS)
            .enumerate()
        {
            let child_client_mutation_id = client_mutation_id.as_ref().map(|base| {
                record_batch_child_client_mutation_id(base, shard, chunk_index, split_transaction)
            });
            let response = commit_record_transaction(
                state,
                RecordTransactionRequest {
                    durability: request.durability,
                    client_mutation_id: child_client_mutation_id,
                    operations: chunk.to_vec(),
                },
            )
            .await?;
            highest_lsn = highest_lsn.max(response.lsn);
            transaction_count += 1;
            responses.extend(response.operations);
        }
    }

    Ok(RecordBatchResponse {
        lsn: highest_lsn,
        transaction_count,
        operations: responses,
    })
}

async fn prepare_record_transaction(
    state: &AppState,
    request: RecordTransactionRequest,
) -> Result<PreparedRecordTransaction, ApiError> {
    let mut seen_keys = BTreeSet::new();
    let mut touched_partitions = BTreeSet::new();
    let mut nested_partition: Option<String> = None;
    let mut shard_index: Option<usize> = None;
    let mut operations = Vec::new();
    let mut provisional_upserts = Vec::new();
    let mut deletes = BTreeSet::new();
    let mut delete_identities = BTreeSet::new();
    let timestamp_ms = now_ms();

    for operation in request.operations {
        match operation {
            RecordTransactionOperationRequest::Upsert {
                table,
                key,
                value,
                expected_lsn,
            } => {
                ensure_json_value_limit(
                    "record transaction upsert value",
                    &value,
                    state.limits.max_record_value_bytes,
                )?;
                validate_record_identity(&table, &key, &value, state)?;
                validate_record_object_refs(state, &table, &value).await?;
                ensure_transaction_partition(
                    &mut touched_partitions,
                    &mut nested_partition,
                    &format!("{table}:{key}"),
                    false,
                )?;
                ensure_transaction_key_once(&mut seen_keys, &table, &key)?;
                let shard = writable_wal_shard_for_key(state, &format!("{table}:{key}")).await?;
                ensure_same_transaction_shard(&mut shard_index, shard.index)?;
                ensure_expected_record_lsn(state, &table, &key, expected_lsn).await?;
                let path = format!("tables/{table}/{key}");
                let draft = DbRecordDraft {
                    table: table.clone(),
                    key: key.clone(),
                    value,
                    updated_at_ms: timestamp_ms,
                    path,
                    client_mutation_id: None,
                };
                provisional_upserts.push(draft.clone().into_record(0));
                operations.push(DbRecordMutationDraft::Upsert { record: draft });
            }
            RecordTransactionOperationRequest::Delete {
                table,
                key,
                expected_lsn,
            } => {
                validate_table_path(&table, state)?;
                if !ensure_safe_record_component(&key) {
                    return Err(ApiError::bad_request("invalid record key"));
                }
                ensure_transaction_partition(
                    &mut touched_partitions,
                    &mut nested_partition,
                    &format!("{table}:{key}"),
                    false,
                )?;
                ensure_transaction_key_once(&mut seen_keys, &table, &key)?;
                let shard = writable_wal_shard_for_key(state, &format!("{table}:{key}")).await?;
                ensure_same_transaction_shard(&mut shard_index, shard.index)?;
                let current_lsn =
                    ensure_expected_record_lsn(state, &table, &key, expected_lsn).await?;
                if current_lsn == 0 {
                    continue;
                }
                let path = format!("tables/{table}/{key}");
                deletes.insert(path.clone());
                delete_identities.insert(record_identity(&table, &key));
                operations.push(DbRecordMutationDraft::Delete {
                    record: DbRecordDeleteDraft {
                        table,
                        key,
                        deleted_at_ms: timestamp_ms,
                        path,
                        client_mutation_id: None,
                    },
                });
            }
            RecordTransactionOperationRequest::NestedUpsert {
                table,
                parent_key,
                nested,
                nested_key,
                value,
                expected_lsn,
            } => {
                ensure_json_value_limit(
                    "record transaction nested upsert value",
                    &value,
                    state.limits.max_record_value_bytes,
                )?;
                validate_nested_record_identity(
                    &table,
                    &parent_key,
                    &nested,
                    &nested_key,
                    &value,
                    state,
                )?;
                validate_nested_record_object_refs(state, &table, &nested, &value).await?;
                let logical_table = nested_record_table(&table, &nested);
                let logical_key = nested_record_key(&parent_key, &nested_key);
                let partition = format!("{table}:{parent_key}");
                ensure_transaction_partition(
                    &mut touched_partitions,
                    &mut nested_partition,
                    &partition,
                    true,
                )?;
                ensure_transaction_key_once(&mut seen_keys, &logical_table, &logical_key)?;
                let shard = writable_wal_shard_for_key(state, &partition).await?;
                ensure_same_transaction_shard(&mut shard_index, shard.index)?;
                ensure_expected_record_lsn(state, &logical_table, &logical_key, expected_lsn)
                    .await?;
                let path = nested_record_path(&table, &parent_key, &nested, &nested_key);
                let draft = DbRecordDraft {
                    table: logical_table,
                    key: logical_key,
                    value,
                    updated_at_ms: timestamp_ms,
                    path,
                    client_mutation_id: None,
                };
                provisional_upserts.push(draft.clone().into_record(0));
                operations.push(DbRecordMutationDraft::Upsert { record: draft });
            }
            RecordTransactionOperationRequest::NestedDelete {
                table,
                parent_key,
                nested,
                nested_key,
                expected_lsn,
            } => {
                validate_nested_record_path(&table, &parent_key, &nested, &nested_key, state)?;
                let logical_table = nested_record_table(&table, &nested);
                let logical_key = nested_record_key(&parent_key, &nested_key);
                let partition = format!("{table}:{parent_key}");
                ensure_transaction_partition(
                    &mut touched_partitions,
                    &mut nested_partition,
                    &partition,
                    true,
                )?;
                ensure_transaction_key_once(&mut seen_keys, &logical_table, &logical_key)?;
                let shard = writable_wal_shard_for_key(state, &partition).await?;
                ensure_same_transaction_shard(&mut shard_index, shard.index)?;
                let current_lsn =
                    ensure_expected_record_lsn(state, &logical_table, &logical_key, expected_lsn)
                        .await?;
                if current_lsn == 0 {
                    continue;
                }
                let path = nested_record_path(&table, &parent_key, &nested, &nested_key);
                deletes.insert(path.clone());
                delete_identities.insert(record_identity(&logical_table, &logical_key));
                operations.push(DbRecordMutationDraft::Delete {
                    record: DbRecordDeleteDraft {
                        table: logical_table,
                        key: logical_key,
                        deleted_at_ms: timestamp_ms,
                        path,
                        client_mutation_id: None,
                    },
                });
            }
        }
    }

    validate_transaction_unique_indexes(
        state,
        &provisional_upserts,
        &deletes,
        &delete_identities,
        true,
    )
    .await?;

    Ok(PreparedRecordTransaction {
        shard: shard_index.unwrap_or(0),
        durability: request.durability,
        operations,
    })
}

pub(crate) async fn ensure_expected_record_lsn(
    state: &AppState,
    table: &str,
    key: &str,
    expected_lsn: Option<u64>,
) -> Result<u64, ApiError> {
    let current = state
        .records
        .get(table, key)
        .await
        .map_err(ApiError::internal)?;
    let current_lsn = current.as_ref().map(|record| record.lsn).unwrap_or(0);
    if let Some(expected_lsn) = expected_lsn
        && current_lsn != expected_lsn
    {
        return Err(ApiError::conflict(format!(
            "record version conflict: expected lsn {expected_lsn}, found {current_lsn}"
        )));
    }
    Ok(current_lsn)
}

pub(crate) async fn ensure_expected_record_lsn_for_live_state(
    state: &AppState,
    table: &str,
    key: &str,
    expected_lsn: Option<u64>,
) -> Result<u64, ApiError> {
    let hot = state.record_hot.get(table, key).await;
    let current = match hot {
        Some(Some(record)) => Some(record),
        Some(None)
            if state
                .record_hot
                .durable_delete_lsn(table, key)
                .await
                .flatten()
                .is_some() =>
        {
            None
        }
        _ => state
            .records
            .get(table, key)
            .await
            .map_err(ApiError::internal)?,
    };
    let current_lsn = current.as_ref().map(|record| record.lsn).unwrap_or(0);
    if let Some(expected_lsn) = expected_lsn
        && current_lsn != expected_lsn
    {
        return Err(ApiError::conflict(format!(
            "record version conflict: expected lsn {expected_lsn}, found {current_lsn}"
        )));
    }
    Ok(current_lsn)
}

pub(crate) fn ensure_record_table_accepts_volatile(
    state: &AppState,
    table: &str,
) -> Result<(), ApiError> {
    let accepts = state
        .schema
        .record_table_accepts_volatile(table)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !accepts {
        return Err(ApiError::bad_request(
            "volatile record writes require actorPartition, resident, or lru storage",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_transaction_unique_indexes(
    state: &AppState,
    upserts: &[DbRecord],
    deletes: &BTreeSet<String>,
    delete_identities: &BTreeSet<String>,
    _durable_commit: bool,
) -> Result<(), ApiError> {
    let upsert_paths = upserts
        .iter()
        .map(|record| record.path.clone())
        .collect::<BTreeSet<_>>();
    let mut unique_values = BTreeMap::new();

    for record in upserts {
        let indexes = state
            .schema
            .record_indexes(&record.table)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        for (index_name, index) in indexes {
            let values = record_index_values(record, &index)
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
            if !index.unique {
                continue;
            }
            let identity = (
                record.table.clone(),
                index_name.clone(),
                serde_json::to_string(&values).map_err(|err| ApiError::internal(err.into()))?,
            );
            if let Some(existing_key) = unique_values.insert(identity, record.key.clone())
                && existing_key != record.key
            {
                return Err(ApiError::conflict(format!(
                    "unique index violation on {}.{}",
                    record.table, index_name
                )));
            }
            let existing = state
                .records
                .query_index(&record.table, &index_name, &values, None, None)
                .await
                .map_err(ApiError::internal)?;
            for existing in existing {
                let Some(current) = record_current_for_unique_validation(state, existing).await?
                else {
                    continue;
                };
                if !record_conflicts_with_unique_candidate(
                    record,
                    &index,
                    &values,
                    &current,
                    deletes,
                    delete_identities,
                    &upsert_paths,
                )? {
                    continue;
                }
                return Err(ApiError::conflict(format!(
                    "unique index violation on {}.{} for record {}",
                    record.table, index_name, record.key
                )));
            }
            if let Some(hot_records) = state
                .record_hot
                .scan_key_order(&record.table, None, None)
                .await
            {
                for hot in hot_records {
                    if !record_conflicts_with_unique_candidate(
                        record,
                        &index,
                        &values,
                        &hot,
                        deletes,
                        delete_identities,
                        &upsert_paths,
                    )? {
                        continue;
                    }
                    return Err(ApiError::conflict(format!(
                        "unique index violation on {}.{} for record {}",
                        record.table, index_name, record.key
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn record_current_for_unique_validation(
    state: &AppState,
    existing: DbRecord,
) -> Result<Option<DbRecord>, ApiError> {
    match state.record_hot.get(&existing.table, &existing.key).await {
        Some(Some(hot)) => Ok(Some(hot)),
        Some(None)
            if state
                .record_hot
                .durable_delete_lsn(&existing.table, &existing.key)
                .await
                .flatten()
                .is_some() =>
        {
            Ok(None)
        }
        _ => Ok(Some(existing)),
    }
}

fn record_conflicts_with_unique_candidate(
    candidate: &DbRecord,
    index: &IndexSchema,
    values: &[serde_json::Value],
    existing: &DbRecord,
    deletes: &BTreeSet<String>,
    delete_identities: &BTreeSet<String>,
    upsert_paths: &BTreeSet<String>,
) -> Result<bool, ApiError> {
    if existing.key == candidate.key
        || deletes.contains(&existing.path)
        || delete_identities.contains(&record_identity(&existing.table, &existing.key))
        || upsert_paths.contains(&existing.path)
    {
        return Ok(false);
    }
    let existing_values = record_index_values(existing, index)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    Ok(existing_values == values)
}

pub(crate) async fn get_record_from_live_or_disk(
    state: &AppState,
    table: &str,
    key: &str,
) -> Result<Option<DbRecord>, ApiError> {
    match state.record_hot.get(table, key).await {
        Some(Some(record)) => Ok(Some(record)),
        Some(None)
            if state
                .record_hot
                .durable_delete_lsn(table, key)
                .await
                .flatten()
                .is_some() =>
        {
            Ok(None)
        }
        Some(None) if state.record_hot.is_windowed_table(table).await => {
            let record = state
                .records
                .get(table, key)
                .await
                .map_err(ApiError::internal)?;
            if let Some(record) = record.as_ref() {
                state.record_hot.hydrate_durable_many([record]).await;
            }
            Ok(record)
        }
        Some(None) => Ok(None),
        None => state
            .records
            .get(table, key)
            .await
            .map_err(ApiError::internal),
    }
}

pub(crate) async fn list_records_from_live_or_disk(
    state: &AppState,
    table: &str,
    after_key: Option<&str>,
    limit: usize,
) -> Result<Vec<DbRecord>, ApiError> {
    if state.record_hot.is_windowed_table(table).await {
        let overlay = state
            .record_hot
            .scan_key_order_overlay(table, None, after_key)
            .await
            .unwrap_or_default();
        let disk_records = collect_matching_key_order_records(
            state,
            table,
            None,
            after_key,
            &overlay.shadow_keys,
            limit,
        )
        .await?;
        let records = merge_key_order_records(disk_records, overlay.records, limit);
        return Ok(records);
    }

    match state.record_hot.list(table, after_key, Some(limit)).await {
        Some(records) => Ok(records),
        None => state
            .records
            .list(table, after_key, Some(limit))
            .await
            .map_err(ApiError::internal),
    }
}

pub(crate) async fn list_records_by_prefix_from_live_or_disk(
    state: &AppState,
    table: &str,
    key_prefix: &str,
    after_key: Option<&str>,
    limit: usize,
) -> Result<Vec<DbRecord>, ApiError> {
    if state.record_hot.is_windowed_table(table).await {
        let overlay = state
            .record_hot
            .scan_key_order_overlay(table, Some(key_prefix), after_key)
            .await
            .unwrap_or_default();
        let disk_records = collect_matching_key_order_records(
            state,
            table,
            Some(key_prefix),
            after_key,
            &overlay.shadow_keys,
            limit,
        )
        .await?;
        let records = merge_key_order_records(disk_records, overlay.records, limit);
        return Ok(records);
    }

    match state
        .record_hot
        .list_by_key_prefix(table, key_prefix, after_key, Some(limit))
        .await
    {
        Some(records) => Ok(records),
        None => state
            .records
            .list_by_key_prefix(table, key_prefix, after_key, Some(limit))
            .await
            .map_err(ApiError::internal),
    }
}

async fn collect_matching_key_order_records(
    state: &AppState,
    table: &str,
    key_prefix: Option<&str>,
    after_key: Option<&str>,
    shadow_keys: &BTreeSet<String>,
    target: usize,
) -> Result<Vec<DbRecord>, ApiError> {
    let mut out = Vec::new();
    let mut scan_after_key = after_key.map(str::to_string);
    'scan: loop {
        let records = if let Some(key_prefix) = key_prefix {
            state
                .records
                .list_by_key_prefix(table, key_prefix, scan_after_key.as_deref(), Some(500))
                .await
        } else {
            state
                .records
                .list(table, scan_after_key.as_deref(), Some(500))
                .await
        }
        .map_err(ApiError::internal)?;
        if records.is_empty() {
            break;
        }
        let batch_len = records.len();
        for record in records {
            scan_after_key = Some(record.key.clone());
            if shadow_keys.contains(&record.key) {
                continue;
            }
            out.push(record);
            if out.len() >= target {
                break 'scan;
            }
        }
        if batch_len < 500 {
            break;
        }
    }
    Ok(out)
}

pub(crate) fn merge_key_order_records(
    disk_records: Vec<DbRecord>,
    hot_records: Vec<DbRecord>,
    limit: usize,
) -> Vec<DbRecord> {
    let mut disk = disk_records.into_iter().peekable();
    let mut hot = hot_records.into_iter().peekable();
    let mut merged = Vec::with_capacity(limit);

    while merged.len() < limit {
        match (disk.peek(), hot.peek()) {
            (Some(disk_record), Some(hot_record)) => match disk_record.key.cmp(&hot_record.key) {
                std::cmp::Ordering::Less => {
                    if let Some(record) = disk.next() {
                        merged.push(record);
                    }
                }
                std::cmp::Ordering::Equal => {
                    disk.next();
                    if let Some(record) = hot.next() {
                        merged.push(record);
                    }
                }
                std::cmp::Ordering::Greater => {
                    if let Some(record) = hot.next() {
                        merged.push(record);
                    }
                }
            },
            (Some(_), None) => {
                if let Some(record) = disk.next() {
                    merged.push(record);
                }
            }
            (None, Some(_)) => {
                if let Some(record) = hot.next() {
                    merged.push(record);
                }
            }
            (None, None) => break,
        }
    }

    merged
}

pub(crate) fn validate_record_identity(
    table: &str,
    key: &str,
    value: &serde_json::Value,
    state: &AppState,
) -> Result<(), ApiError> {
    validate_record_path(table, key, state)?;
    state
        .schema
        .validate_table_record(table, value)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
        && id != key
    {
        return Err(ApiError::bad_request("record value.id must match key"));
    }
    Ok(())
}

pub(crate) fn validate_nested_record_identity(
    table: &str,
    parent_key: &str,
    nested: &str,
    nested_key: &str,
    value: &serde_json::Value,
    state: &AppState,
) -> Result<(), ApiError> {
    validate_nested_record_path(table, parent_key, nested, nested_key, state)?;
    state
        .schema
        .validate_nested_table_record(table, nested, value)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
        && id != nested_key
    {
        return Err(ApiError::bad_request(
            "nested record value.id must match key",
        ));
    }
    if let Some(value_parent_key) = value
        .get("parentId")
        .or_else(|| value.get("parentKey"))
        .and_then(serde_json::Value::as_str)
        && value_parent_key != parent_key
    {
        return Err(ApiError::bad_request(
            "nested record parentId/parentKey must match parent key",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_record_object_refs(
    state: &AppState,
    table: &str,
    value: &serde_json::Value,
) -> Result<(), ApiError> {
    let schema = state.schema.schema();
    let table_schema = schema
        .tables
        .get(table)
        .ok_or_else(|| ApiError::bad_request(format!("schema missing table {table}")))?;
    let refs =
        collect_declared_object_refs(&format!("tables.{table}"), &table_schema.fields, value)?;
    validate_declared_object_refs_exist(state, refs).await
}

pub(crate) async fn validate_nested_record_object_refs(
    state: &AppState,
    table: &str,
    nested: &str,
    value: &serde_json::Value,
) -> Result<(), ApiError> {
    let schema = state.schema.schema();
    let nested_schema = schema
        .tables
        .get(table)
        .and_then(|table_schema| table_schema.nested.get(nested))
        .ok_or_else(|| {
            ApiError::bad_request(format!("schema missing nested table {table}.{nested}"))
        })?;
    let refs = collect_declared_object_refs(
        &format!("tables.{table}.nested.{nested}"),
        &nested_schema.fields,
        value,
    )?;
    validate_declared_object_refs_exist(state, refs).await
}
pub(crate) fn merge_key_order_records_matching_with_shadow_keys<F>(
    disk_records: Vec<DbRecord>,
    hot_records: Vec<DbRecord>,
    shadow_keys: &BTreeSet<String>,
    limit: usize,
    mut matches: F,
) -> (Vec<DbRecord>, bool)
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut disk = disk_records
        .into_iter()
        .filter(|record| !shadow_keys.contains(&record.key))
        .peekable();
    let mut hot = hot_records.into_iter().peekable();
    let mut records = Vec::with_capacity(limit.saturating_add(1));

    while records.len() <= limit {
        let Some(record) = (match (disk.peek(), hot.peek()) {
            (Some(disk_record), Some(hot_record)) => match disk_record.key.cmp(&hot_record.key) {
                std::cmp::Ordering::Less => disk.next(),
                std::cmp::Ordering::Equal => {
                    disk.next();
                    hot.next()
                }
                std::cmp::Ordering::Greater => hot.next(),
            },
            (Some(_), None) => disk.next(),
            (None, Some(_)) => hot.next(),
            (None, None) => None,
        }) else {
            break;
        };
        if matches(&record) {
            records.push(record);
        }
    }

    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }
    (records, has_more)
}

async fn collect_hot_key_order_overlay(
    state: &AppState,
    table: &str,
    key_prefix: Option<&str>,
    after_key: Option<&str>,
) -> RecordHotKeyOrderOverlay {
    state
        .record_hot
        .scan_key_order_overlay(table, key_prefix, after_key)
        .await
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn split_matching_hot_records<F>(
    hot_records: Vec<DbRecord>,
    mut matches: F,
) -> (Vec<DbRecord>, BTreeSet<String>)
where
    F: FnMut(&DbRecord) -> bool,
{
    let hot_keys = record_key_set(&hot_records);
    let matching_records = hot_records
        .into_iter()
        .filter(|record| matches(record))
        .collect::<Vec<_>>();
    (matching_records, hot_keys)
}

fn split_matching_hot_overlay<F>(
    overlay: RecordHotKeyOrderOverlay,
    mut matches: F,
) -> (Vec<DbRecord>, BTreeSet<String>)
where
    F: FnMut(&DbRecord) -> bool,
{
    let matching_records = overlay
        .records
        .into_iter()
        .filter(|record| matches(record))
        .collect::<Vec<_>>();
    (matching_records, overlay.shadow_keys)
}

async fn collect_hot_index_value_records(
    state: &AppState,
    table: &str,
    key_prefix: Option<&str>,
    after_key: Option<&str>,
    index: &IndexSchema,
    values: &[serde_json::Value],
) -> (Vec<DbRecord>, BTreeSet<String>) {
    let overlay = collect_hot_key_order_overlay(state, table, key_prefix, after_key).await;
    split_matching_hot_overlay(overlay, |record| {
        record_matches_index_values(record, index, values)
    })
}

#[allow(clippy::too_many_arguments)]
async fn collect_matching_exact_index_records<F>(
    state: &AppState,
    table: &str,
    index_name: &str,
    values: &[serde_json::Value],
    key_prefix: Option<&str>,
    after_key: Option<&str>,
    shadow_keys: &BTreeSet<String>,
    target: usize,
    mut matches: F,
) -> Result<Vec<DbRecord>, ApiError>
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut out = Vec::new();
    let mut scan_after_key = after_key.map(str::to_string);
    'scan: loop {
        let records = if let Some(key_prefix) = key_prefix {
            state
                .records
                .query_index_by_key_prefix(
                    table,
                    index_name,
                    values,
                    key_prefix,
                    scan_after_key.as_deref(),
                    Some(500),
                )
                .await
        } else {
            state
                .records
                .query_index(
                    table,
                    index_name,
                    values,
                    scan_after_key.as_deref(),
                    Some(500),
                )
                .await
        }
        .map_err(ApiError::internal)?;
        if records.is_empty() {
            break;
        }
        let batch_len = records.len();
        for record in records {
            scan_after_key = Some(record.key.clone());
            if shadow_keys.contains(&record.key) || !matches(&record) {
                continue;
            }
            out.push(record);
            if out.len() >= target {
                break 'scan;
            }
        }
        if batch_len < 500 {
            break;
        }
    }
    Ok(out)
}

pub(crate) fn disk_window_for_hot_overlay(limit: usize, hot_record_count: usize) -> usize {
    limit.saturating_add(hot_record_count).saturating_add(1)
}

fn predicate_scan_target(limit: usize) -> usize {
    limit.saturating_add(1)
}

#[cfg(test)]
pub(crate) fn record_key_set(records: &[DbRecord]) -> BTreeSet<String> {
    records
        .iter()
        .map(|record| record.key.clone())
        .collect::<BTreeSet<_>>()
}

async fn collect_hot_ordered_records(
    state: &AppState,
    table: &str,
    key_prefix: &str,
    order: &[RecordOrderTerm],
    after_cursor: Option<&str>,
) -> (Vec<OrderedDbRecord>, BTreeSet<String>) {
    let overlay = collect_hot_key_order_overlay(state, table, Some(key_prefix), None).await;
    let hot_keys = overlay.shadow_keys;
    let mut records = overlay
        .records
        .into_iter()
        .filter_map(|record| {
            let cursor = order_record_cursor(&record, order);
            if after_cursor.is_some_and(|after_cursor| cursor.as_str() <= after_cursor) {
                return None;
            }
            Some(OrderedDbRecord { record, cursor })
        })
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.cursor.cmp(&right.cursor));
    (records, hot_keys)
}

pub(crate) fn split_matching_ordered_hot_records<F>(
    hot_records: Vec<OrderedDbRecord>,
    mut matches: F,
) -> Vec<OrderedDbRecord>
where
    F: FnMut(&DbRecord) -> bool,
{
    hot_records
        .into_iter()
        .filter(|record| matches(&record.record))
        .collect::<Vec<_>>()
}

pub(crate) fn merge_ordered_records_matching_with_shadow_keys<F>(
    disk_records: Vec<OrderedDbRecord>,
    hot_records: Vec<OrderedDbRecord>,
    shadow_keys: &BTreeSet<String>,
    limit: usize,
    mut matches: F,
) -> (Vec<OrderedDbRecord>, bool)
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut disk = disk_records
        .into_iter()
        .filter(|record| !shadow_keys.contains(&record.record.key))
        .peekable();
    let mut hot = hot_records.into_iter().peekable();

    let mut matched = Vec::with_capacity(limit.saturating_add(1));
    while matched.len() <= limit {
        let Some(record) = (match (disk.peek(), hot.peek()) {
            (Some(disk_record), Some(hot_record)) => {
                match disk_record.cursor.cmp(&hot_record.cursor) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => disk.next(),
                    std::cmp::Ordering::Greater => hot.next(),
                }
            }
            (Some(_), None) => disk.next(),
            (None, Some(_)) => hot.next(),
            (None, None) => None,
        }) else {
            break;
        };
        if matches(&record.record) {
            matched.push(record);
        }
    }
    let has_more = matched.len() > limit;
    if has_more {
        matched.truncate(limit);
    }
    (matched, has_more)
}

#[allow(clippy::too_many_arguments)]
async fn collect_matching_index_range_records<F>(
    state: &AppState,
    table: &str,
    index_name: &str,
    lower: Option<&[serde_json::Value]>,
    upper: Option<&[serde_json::Value]>,
    key_prefix: Option<&str>,
    after_cursor: Option<&str>,
    shadow_keys: &BTreeSet<String>,
    target: usize,
    mut matches: F,
) -> Result<Vec<IndexedDbRecord>, ApiError>
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut out = Vec::new();
    let mut scan_after_cursor = after_cursor.map(str::to_string);
    'scan: loop {
        let batch = state
            .records
            .query_index_range(
                table,
                index_name,
                lower,
                upper,
                key_prefix,
                scan_after_cursor.as_deref(),
                Some(500),
            )
            .await
            .map_err(ApiError::internal)?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for record in batch {
            scan_after_cursor = Some(record.cursor.clone());
            if shadow_keys.contains(&record.record.key) || !matches(&record.record) {
                continue;
            }
            out.push(record);
            if out.len() >= target {
                break 'scan;
            }
        }
        if batch_len < 500 {
            break;
        }
    }
    Ok(out)
}

pub(crate) type IndexRangeMergeEntry = (Vec<serde_json::Value>, String, String, DbRecord);

fn compare_index_range_merge_entries(
    left: &IndexRangeMergeEntry,
    right: &IndexRangeMergeEntry,
) -> std::cmp::Ordering {
    compare_index_values(&left.0, &right.0).then_with(|| left.1.cmp(&right.1))
}

#[allow(clippy::too_many_arguments)]
async fn collect_hot_index_range_overlay<F>(
    state: &AppState,
    table: &str,
    key_prefix: Option<&str>,
    lower: Option<&[serde_json::Value]>,
    upper: Option<&[serde_json::Value]>,
    after_cursor: Option<&(Vec<serde_json::Value>, String)>,
    index: &IndexSchema,
    mut matches: F,
) -> Result<(BTreeSet<String>, Vec<IndexRangeMergeEntry>), ApiError>
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut hot_keys = BTreeSet::new();
    let mut entries = Vec::new();

    if let Some(overlay) = state
        .record_hot
        .scan_key_order_overlay(table, key_prefix, None)
        .await
    {
        hot_keys = overlay.shadow_keys;
        for record in overlay.records {
            let Ok(values) = record_index_values(&record, index) else {
                continue;
            };
            if !index_values_in_range(&values, lower, upper)
                || !index_values_after_cursor(&values, &record.key, after_cursor)
                || !matches(&record)
            {
                continue;
            }
            let cursor = index_range_cursor(&values, &record.key).map_err(ApiError::internal)?;
            entries.push((values, record.key.clone(), cursor, record));
        }
    }

    Ok((hot_keys, entries))
}

pub(crate) fn merge_index_range_records_with_hot_entries<F>(
    disk_records: Vec<IndexedDbRecord>,
    hot_keys: BTreeSet<String>,
    mut entries: Vec<IndexRangeMergeEntry>,
    index: &IndexSchema,
    limit: usize,
    mut matches: F,
) -> Result<(Vec<DbRecord>, Option<String>, bool), ApiError>
where
    F: FnMut(&DbRecord) -> bool,
{
    let mut disk_entries = Vec::with_capacity(disk_records.len().min(limit.saturating_add(1)));
    for IndexedDbRecord { record, cursor } in disk_records {
        if hot_keys.contains(&record.key) || !matches(&record) {
            continue;
        }
        let values = record_index_values(&record, index).map_err(ApiError::internal)?;
        disk_entries.push((values, record.key.clone(), cursor, record));
    }

    entries.sort_by(compare_index_range_merge_entries);
    let mut disk = disk_entries.into_iter().peekable();
    let mut hot = entries.into_iter().peekable();
    let mut merged = Vec::with_capacity(limit.saturating_add(1));

    while merged.len() <= limit {
        let Some(entry) = (match (disk.peek(), hot.peek()) {
            (Some(disk_entry), Some(hot_entry)) => {
                match compare_index_range_merge_entries(disk_entry, hot_entry) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => disk.next(),
                    std::cmp::Ordering::Greater => hot.next(),
                }
            }
            (Some(_), None) => disk.next(),
            (None, Some(_)) => hot.next(),
            (None, None) => None,
        }) else {
            break;
        };
        merged.push(entry);
    }

    let has_more = merged.len() > limit;
    if has_more {
        merged.truncate(limit);
    }
    let next_cursor = if has_more {
        merged.last().map(|(_, _, cursor, _)| cursor.clone())
    } else {
        None
    };
    let records = merged
        .into_iter()
        .map(|(_, _, _, record)| record)
        .collect::<Vec<_>>();
    Ok((records, next_cursor, has_more))
}

#[allow(clippy::too_many_arguments)]
async fn list_index_range_from_live_or_disk(
    state: &AppState,
    table: &str,
    index_name: &str,
    key_prefix: Option<&str>,
    lower: Option<&[serde_json::Value]>,
    upper: Option<&[serde_json::Value]>,
    after_cursor: Option<&str>,
    index: &IndexSchema,
    limit: usize,
) -> Result<(Vec<DbRecord>, Option<String>, bool), ApiError> {
    let after_cursor = after_cursor
        .map(parse_index_range_cursor)
        .transpose()
        .map_err(ApiError::internal)?;
    let (hot_keys, hot_entries) = collect_hot_index_range_overlay(
        state,
        table,
        key_prefix,
        lower,
        upper,
        after_cursor.as_ref(),
        index,
        |_| true,
    )
    .await?;
    let disk_after_cursor = after_cursor
        .as_ref()
        .and_then(|(values, key)| index_range_cursor(values, key).ok());
    let disk_records = collect_matching_index_range_records(
        state,
        table,
        index_name,
        lower,
        upper,
        key_prefix,
        disk_after_cursor.as_deref(),
        &hot_keys,
        predicate_scan_target(limit),
        |_| true,
    )
    .await?;
    merge_index_range_records_with_hot_entries(
        disk_records,
        hot_keys,
        hot_entries,
        index,
        limit,
        |_| true,
    )
}

#[allow(clippy::too_many_arguments)]
async fn list_matching_index_range_from_live_or_disk<F>(
    state: &AppState,
    table: &str,
    index_name: &str,
    key_prefix: Option<&str>,
    lower: Option<&[serde_json::Value]>,
    upper: Option<&[serde_json::Value]>,
    after_cursor: Option<&str>,
    index: &IndexSchema,
    limit: usize,
    mut matches: F,
) -> Result<(Vec<DbRecord>, Option<String>, bool), ApiError>
where
    F: FnMut(&DbRecord) -> bool,
{
    let after_cursor = after_cursor
        .map(parse_index_range_cursor)
        .transpose()
        .map_err(ApiError::internal)?;
    let (hot_keys, hot_entries) = collect_hot_index_range_overlay(
        state,
        table,
        key_prefix,
        lower,
        upper,
        after_cursor.as_ref(),
        index,
        |record| matches(record),
    )
    .await?;
    let disk_after_cursor = after_cursor
        .as_ref()
        .and_then(|(values, key)| index_range_cursor(values, key).ok());
    let disk_records = collect_matching_index_range_records(
        state,
        table,
        index_name,
        lower,
        upper,
        key_prefix,
        disk_after_cursor.as_deref(),
        &hot_keys,
        predicate_scan_target(limit),
        |record| matches(record),
    )
    .await?;
    merge_index_range_records_with_hot_entries(
        disk_records,
        hot_keys,
        hot_entries,
        index,
        limit,
        |_| true,
    )
}

fn index_values_in_range(
    values: &[serde_json::Value],
    lower: Option<&[serde_json::Value]>,
    upper: Option<&[serde_json::Value]>,
) -> bool {
    lower.is_none_or(|lower| compare_index_values(values, lower) != std::cmp::Ordering::Less)
        && upper
            .is_none_or(|upper| compare_index_values(values, upper) != std::cmp::Ordering::Greater)
}

fn index_values_after_cursor(
    values: &[serde_json::Value],
    key: &str,
    after_cursor: Option<&(Vec<serde_json::Value>, String)>,
) -> bool {
    after_cursor.is_none_or(|(after_values, after_key)| {
        compare_index_values(values, after_values).then_with(|| key.cmp(after_key))
            == std::cmp::Ordering::Greater
    })
}

async fn hydrate_record_hot(state: &AppState, records: &[DbRecord]) {
    state.record_hot.hydrate_durable_many(records).await;
}

async fn record_list_response(
    state: &AppState,
    table: String,
    records: Vec<DbRecord>,
    next_after_key: Option<String>,
    next_cursor: Option<String>,
    has_more: bool,
) -> ListRecordsResponse {
    hydrate_record_hot(state, &records).await;
    ListRecordsResponse {
        table,
        records,
        next_after_key,
        next_cursor,
        has_more,
    }
}

fn record_matches_shard(state: &AppState, record: &DbRecord, shard: Option<usize>) -> bool {
    let Some(shard) = shard else {
        return true;
    };
    shard_index(
        &format!("{}:{}", record.table, record.key),
        state.cluster.shard_count(),
    ) == shard
}

pub(crate) async fn execute_record_list_query(
    state: &AppState,
    table: String,
    parent_key: Option<String>,
    nested: Option<String>,
    query: ListRecordsQuery,
) -> Result<ListRecordsResponse, ApiError> {
    if let Some(predicate) = query.predicate.as_ref() {
        validate_record_predicate(predicate)?;
    }
    if let Some(nested) = nested {
        let parent_key =
            parent_key.ok_or_else(|| ApiError::bad_request("parentKey is required"))?;
        validate_nested_table_path(&table, &parent_key, &nested, state)?;
        let logical_table = nested_record_table(&table, &nested);
        let prefix = nested_record_prefix(&parent_key);
        let limit = normalize_limit(query.limit);
        let (mut records, next_cursor) = if query.order.as_deref() == Some("schema") {
            let order = nested_schema_order(state, &table, &nested)?;
            if let Some(predicate) = query.predicate.as_ref() {
                let (hot_records, hot_shadow_keys) = collect_hot_ordered_records(
                    state,
                    &logical_table,
                    &prefix,
                    &order,
                    query.after_cursor.as_deref(),
                )
                .await;
                let hot_records = split_matching_ordered_hot_records(hot_records, |record| {
                    record_matches_predicate(record, Some(predicate))
                });
                let disk_target = predicate_scan_target(limit);
                let mut matched = Vec::new();
                let mut scan_after_key = query
                    .after_key
                    .as_deref()
                    .map(|key| nested_record_key(&parent_key, key));
                let mut scan_after_cursor = query.after_cursor.clone();
                'scan: loop {
                    let ordered_records = state
                        .records
                        .list_by_key_prefix_ordered(
                            &logical_table,
                            &prefix,
                            &order,
                            scan_after_key.as_deref(),
                            scan_after_cursor.as_deref(),
                            Some(500),
                        )
                        .await
                        .map_err(ApiError::internal)?;
                    if ordered_records.is_empty() {
                        break;
                    }
                    let batch_len = ordered_records.len();
                    for ordered in ordered_records {
                        scan_after_key = Some(ordered.record.key.clone());
                        scan_after_cursor = Some(ordered.cursor.clone());
                        if hot_shadow_keys.contains(&ordered.record.key) {
                            continue;
                        }
                        if record_matches_predicate(&ordered.record, Some(predicate)) {
                            matched.push(ordered);
                            if matched.len() >= disk_target {
                                break 'scan;
                            }
                        }
                    }
                    if batch_len < 500 {
                        break;
                    }
                }
                let (matched, has_more) = merge_ordered_records_matching_with_shadow_keys(
                    matched,
                    hot_records,
                    &hot_shadow_keys,
                    limit,
                    |_| true,
                );
                let next_cursor = matched.last().map(|record| record.cursor.clone());
                let records: Vec<DbRecord> =
                    matched.into_iter().map(|record| record.record).collect();
                let next_after_key = records
                    .last()
                    .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
                return Ok(record_list_response(
                    state,
                    logical_table,
                    records,
                    next_after_key,
                    next_cursor,
                    has_more,
                )
                .await);
            }
            let (hot_records, hot_shadow_keys) = collect_hot_ordered_records(
                state,
                &logical_table,
                &prefix,
                &order,
                query.after_cursor.as_deref(),
            )
            .await;
            let ordered_records = state
                .records
                .list_by_key_prefix_ordered(
                    &logical_table,
                    &prefix,
                    &order,
                    query.after_key.as_deref(),
                    query.after_cursor.as_deref(),
                    Some(disk_window_for_hot_overlay(limit, hot_shadow_keys.len())),
                )
                .await
                .map_err(ApiError::internal)?;
            let (ordered_records, has_extra) = merge_ordered_records_matching_with_shadow_keys(
                ordered_records,
                hot_records,
                &hot_shadow_keys,
                limit,
                |_| true,
            );
            let next_cursor = ordered_records.last().map(|record| record.cursor.clone());
            let records: Vec<DbRecord> = ordered_records
                .into_iter()
                .map(|record| record.record)
                .collect();
            let next_after_key = records
                .last()
                .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
            return Ok(record_list_response(
                state,
                logical_table,
                records,
                next_after_key,
                next_cursor,
                has_extra,
            )
            .await);
        } else {
            let after_key = query
                .after_key
                .as_deref()
                .map(|key| nested_record_key(&parent_key, key));
            if let Some(predicate) = query.predicate.as_ref() {
                let overlay = collect_hot_key_order_overlay(
                    state,
                    &logical_table,
                    Some(&prefix),
                    after_key.as_deref(),
                )
                .await;
                let (hot_records, hot_keys) = split_matching_hot_overlay(overlay, |record| {
                    record_matches_predicate(record, Some(predicate))
                });
                let disk_target = predicate_scan_target(limit);
                let mut matched = Vec::new();
                let mut scan_after_key = after_key.clone();
                'scan: loop {
                    let records = state
                        .records
                        .list_by_key_prefix(
                            &logical_table,
                            &prefix,
                            scan_after_key.as_deref(),
                            Some(500),
                        )
                        .await
                        .map_err(ApiError::internal)?;
                    if records.is_empty() {
                        break;
                    }
                    let batch_len = records.len();
                    for record in records {
                        scan_after_key = Some(record.key.clone());
                        if hot_keys.contains(&record.key) {
                            continue;
                        }
                        if record_matches_predicate(&record, Some(predicate)) {
                            matched.push(record);
                            if matched.len() >= disk_target {
                                break 'scan;
                            }
                        }
                    }
                    if batch_len < 500 {
                        break;
                    }
                }
                let (matched, has_more) = merge_key_order_records_matching_with_shadow_keys(
                    matched,
                    hot_records,
                    &hot_keys,
                    limit,
                    |_| true,
                );
                let next_after_key = matched
                    .last()
                    .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
                return Ok(record_list_response(
                    state,
                    logical_table,
                    matched,
                    next_after_key,
                    None,
                    has_more,
                )
                .await);
            }
            let records = list_records_by_prefix_from_live_or_disk(
                state,
                &logical_table,
                &prefix,
                after_key.as_deref(),
                limit + 1,
            )
            .await?;
            (records, None)
        };
        let has_more = records.len() > limit;
        if has_more {
            records.truncate(limit);
        }
        let next_after_key = records
            .last()
            .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
        return Ok(record_list_response(
            state,
            logical_table,
            records,
            next_after_key,
            next_cursor,
            has_more,
        )
        .await);
    }

    validate_table_path(&table, state)?;
    let limit = normalize_limit(query.limit);
    if let Some(shard) = query.shard {
        ensure_shard_index(state, shard)?;
    }
    if query.predicate.is_some() || query.shard.is_some() {
        let overlay =
            collect_hot_key_order_overlay(state, &table, None, query.after_key.as_deref()).await;
        let (hot_records, hot_keys) = split_matching_hot_overlay(overlay, |record| {
            record_matches_shard(state, record, query.shard)
                && record_matches_predicate(record, query.predicate.as_ref())
        });
        let disk_target = predicate_scan_target(limit);
        let mut matched = Vec::new();
        let mut scan_after_key = query.after_key.clone();
        'scan: loop {
            let records = state
                .records
                .list(&table, scan_after_key.as_deref(), Some(500))
                .await
                .map_err(ApiError::internal)?;
            if records.is_empty() {
                break;
            }
            let batch_len = records.len();
            for record in records {
                scan_after_key = Some(record.key.clone());
                if hot_keys.contains(&record.key) {
                    continue;
                }
                if record_matches_shard(state, &record, query.shard)
                    && record_matches_predicate(&record, query.predicate.as_ref())
                {
                    matched.push(record);
                    if matched.len() >= disk_target {
                        break 'scan;
                    }
                }
            }
            if batch_len < 500 {
                break;
            }
        }
        let (matched, has_more) = merge_key_order_records_matching_with_shadow_keys(
            matched,
            hot_records,
            &hot_keys,
            limit,
            |_| true,
        );
        let next_after_key = matched.last().map(|record| record.key.clone());
        return Ok(
            record_list_response(state, table, matched, next_after_key, None, has_more).await,
        );
    }
    let mut records =
        list_records_from_live_or_disk(state, &table, query.after_key.as_deref(), limit + 1)
            .await?;
    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }
    let next_after_key = records.last().map(|record| record.key.clone());
    Ok(record_list_response(state, table, records, next_after_key, None, has_more).await)
}

pub(crate) async fn execute_record_index_query(
    state: &AppState,
    table: String,
    parent_key: Option<String>,
    nested: Option<String>,
    index_name: String,
    query: QueryRecordsByIndexQuery,
) -> Result<ListRecordsResponse, ApiError> {
    if let Some(predicate) = query.predicate.as_ref() {
        validate_record_predicate(predicate)?;
    }
    if let Some(nested) = nested {
        let parent_key =
            parent_key.ok_or_else(|| ApiError::bad_request("parentKey is required"))?;
        validate_nested_table_path(&table, &parent_key, &nested, state)?;
        if !ensure_safe_record_component(&index_name) {
            return Err(ApiError::bad_request("invalid index name"));
        }
        let indexes = state
            .schema
            .nested_table_indexes(&table, &nested)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let index = indexes
            .get(&index_name)
            .ok_or_else(|| ApiError::not_found("nested index not found"))?;
        let limit = normalize_limit(query.limit);
        let logical_table = nested_record_table(&table, &nested);
        let prefix = nested_record_prefix(&parent_key);
        let after_key = query
            .after_key
            .as_deref()
            .map(|key| nested_record_key(&parent_key, key));
        if query.is_range_query() {
            let lower = parse_index_bound_values(&query.lower, &query.lower_values, index)?;
            let upper = parse_index_bound_values(&query.upper, &query.upper_values, index)?;
            if query.predicate.is_none() {
                let (records, next_cursor, has_more) = list_index_range_from_live_or_disk(
                    state,
                    &logical_table,
                    &index_name,
                    Some(&prefix),
                    lower.as_deref(),
                    upper.as_deref(),
                    query.after_cursor.as_deref(),
                    index,
                    limit,
                )
                .await?;
                let next_after_key = records
                    .last()
                    .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
                return Ok(record_list_response(
                    state,
                    logical_table,
                    records,
                    next_after_key,
                    next_cursor,
                    has_more,
                )
                .await);
            }
            let (records, next_cursor, has_more) = list_matching_index_range_from_live_or_disk(
                state,
                &logical_table,
                &index_name,
                Some(&prefix),
                lower.as_deref(),
                upper.as_deref(),
                query.after_cursor.as_deref(),
                index,
                limit,
                |record| record_matches_predicate(record, query.predicate.as_ref()),
            )
            .await?;
            let next_after_key = records
                .last()
                .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
            return Ok(record_list_response(
                state,
                logical_table,
                records,
                next_after_key,
                next_cursor,
                has_more,
            )
            .await);
        }
        let values = parse_index_query_values(&query, index)?;
        if let Some(predicate) = query.predicate.as_ref() {
            let (hot_records, hot_keys) = collect_hot_index_value_records(
                state,
                &logical_table,
                Some(&prefix),
                after_key.as_deref(),
                index,
                &values,
            )
            .await;
            let matched = collect_matching_exact_index_records(
                state,
                &logical_table,
                &index_name,
                &values,
                Some(&prefix),
                after_key.as_deref(),
                &hot_keys,
                predicate_scan_target(limit),
                |record| record_matches_predicate(record, Some(predicate)),
            )
            .await?;
            let (matched, has_more) = merge_key_order_records_matching_with_shadow_keys(
                matched,
                hot_records,
                &hot_keys,
                limit,
                |record| record_matches_predicate(record, Some(predicate)),
            );
            let next_after_key = matched
                .last()
                .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
            return Ok(record_list_response(
                state,
                logical_table,
                matched,
                next_after_key,
                None,
                has_more,
            )
            .await);
        }
        let (hot_records, hot_keys) = collect_hot_index_value_records(
            state,
            &logical_table,
            Some(&prefix),
            after_key.as_deref(),
            index,
            &values,
        )
        .await;
        let disk_records = collect_matching_exact_index_records(
            state,
            &logical_table,
            &index_name,
            &values,
            Some(&prefix),
            after_key.as_deref(),
            &hot_keys,
            predicate_scan_target(limit),
            |_| true,
        )
        .await?;
        let (records, has_more) = merge_key_order_records_matching_with_shadow_keys(
            disk_records,
            hot_records,
            &hot_keys,
            limit,
            |_| true,
        );
        let next_after_key = records
            .last()
            .and_then(|record| nested_key_from_logical_key(&parent_key, &record.key));
        return Ok(record_list_response(
            state,
            logical_table,
            records,
            next_after_key,
            None,
            has_more,
        )
        .await);
    }

    validate_table_path(&table, state)?;
    if !ensure_safe_record_component(&index_name) {
        return Err(ApiError::bad_request("invalid index name"));
    }
    let indexes = state
        .schema
        .table_indexes(&table)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let index = indexes
        .get(&index_name)
        .ok_or_else(|| ApiError::not_found("index not found"))?;
    let limit = normalize_limit(query.limit);
    if let Some(shard) = query.shard {
        ensure_shard_index(state, shard)?;
    }
    if query.is_range_query() {
        let lower = parse_index_bound_values(&query.lower, &query.lower_values, index)?;
        let upper = parse_index_bound_values(&query.upper, &query.upper_values, index)?;
        if query.predicate.is_none() && query.shard.is_none() {
            let (records, next_cursor, has_more) = list_index_range_from_live_or_disk(
                state,
                &table,
                &index_name,
                None,
                lower.as_deref(),
                upper.as_deref(),
                query.after_cursor.as_deref(),
                index,
                limit,
            )
            .await?;
            let next_after_key = records.last().map(|record| record.key.clone());
            return Ok(record_list_response(
                state,
                table,
                records,
                next_after_key,
                next_cursor,
                has_more,
            )
            .await);
        }
        let (records, next_cursor, has_more) = list_matching_index_range_from_live_or_disk(
            state,
            &table,
            &index_name,
            None,
            lower.as_deref(),
            upper.as_deref(),
            query.after_cursor.as_deref(),
            index,
            limit,
            |record| {
                record_matches_shard(state, record, query.shard)
                    && record_matches_predicate(record, query.predicate.as_ref())
            },
        )
        .await?;
        let next_after_key = records.last().map(|record| record.key.clone());
        return Ok(record_list_response(
            state,
            table,
            records,
            next_after_key,
            next_cursor,
            has_more,
        )
        .await);
    }
    let values = parse_index_query_values(&query, index)?;
    if query.predicate.is_some() || query.shard.is_some() {
        let (hot_records, hot_keys) = collect_hot_index_value_records(
            state,
            &table,
            None,
            query.after_key.as_deref(),
            index,
            &values,
        )
        .await;
        let matched = collect_matching_exact_index_records(
            state,
            &table,
            &index_name,
            &values,
            None,
            query.after_key.as_deref(),
            &hot_keys,
            predicate_scan_target(limit),
            |record| {
                record_matches_shard(state, record, query.shard)
                    && record_matches_predicate(record, query.predicate.as_ref())
            },
        )
        .await?;
        let (matched, has_more) = merge_key_order_records_matching_with_shadow_keys(
            matched,
            hot_records,
            &hot_keys,
            limit,
            |record| {
                record_matches_shard(state, record, query.shard)
                    && record_matches_predicate(record, query.predicate.as_ref())
            },
        );
        let next_after_key = matched.last().map(|record| record.key.clone());
        return Ok(
            record_list_response(state, table, matched, next_after_key, None, has_more).await,
        );
    }
    let (hot_records, hot_keys) = collect_hot_index_value_records(
        state,
        &table,
        None,
        query.after_key.as_deref(),
        index,
        &values,
    )
    .await;
    let disk_records = collect_matching_exact_index_records(
        state,
        &table,
        &index_name,
        &values,
        None,
        query.after_key.as_deref(),
        &hot_keys,
        predicate_scan_target(limit),
        |_| true,
    )
    .await?;
    let (records, has_more) = merge_key_order_records_matching_with_shadow_keys(
        disk_records,
        hot_records,
        &hot_keys,
        limit,
        |_| true,
    );
    let next_after_key = records.last().map(|record| record.key.clone());
    Ok(record_list_response(state, table, records, next_after_key, None, has_more).await)
}

pub(crate) async fn commit_record_upsert(
    state: &AppState,
    table: String,
    key: String,
    value: serde_json::Value,
    durability: Durability,
    expected_lsn: Option<u64>,
    client_mutation_id: Option<String>,
) -> Result<DbRecord, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if durability != Durability::Volatile
        && let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())?
    {
        match existing {
            CommittedMutation::RecordUpserted { record } => return Ok(record),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    ensure_json_value_limit("record value", &value, state.limits.max_record_value_bytes)?;
    validate_record_identity(&table, &key, &value, state)?;
    validate_record_object_refs(state, &table, &value).await?;
    let indexes = state
        .schema
        .table_indexes(&table)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    commit_prepared_record_upsert(
        state,
        PreparedRecordUpsert {
            path: format!("tables/{table}/{key}"),
            shard_key: format!("{table}:{key}"),
            table,
            key,
            value,
            indexes,
        },
        durability,
        expected_lsn,
        client_mutation_id,
    )
    .await
}

pub(crate) async fn commit_prepared_record_upsert(
    state: &AppState,
    prepared: PreparedRecordUpsert,
    durability: Durability,
    expected_lsn: Option<u64>,
    client_mutation_id: Option<String>,
) -> Result<DbRecord, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    if durability != Durability::Volatile
        && let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())?
    {
        match existing {
            CommittedMutation::RecordUpserted { record } => return Ok(record),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    let updated_at_ms = now_ms();
    let draft = DbRecordDraft {
        table: prepared.table.clone(),
        key: prepared.key.clone(),
        value: prepared.value,
        updated_at_ms,
        path: prepared.path,
        client_mutation_id,
    };

    if durability == Durability::Volatile {
        ensure_record_table_accepts_volatile(state, &draft.table)?;
        ensure_expected_record_lsn_for_live_state(state, &draft.table, &draft.key, expected_lsn)
            .await?;
        let preflight_record = draft.clone().into_record(0);
        validate_transaction_unique_indexes(
            state,
            &[preflight_record],
            &BTreeSet::new(),
            &BTreeSet::new(),
            false,
        )
        .await?;
        let mut record = draft.into_record(0);
        record.path = volatile_record_path(&record.path);
        state.record_hot.upsert(&record).await;
        publish_delivery_event(
            state,
            DeliveryEvent::RecordUpserted {
                table: prepared.table,
                key: prepared.key,
                record: record.clone(),
            },
        );
        return Ok(record);
    }

    let shard = writable_wal_shard_for_key(state, &prepared.shard_key).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    ensure_expected_record_lsn(state, &prepared.table, &prepared.key, expected_lsn).await?;
    let preflight_record = draft.clone().into_record(0);
    validate_transaction_unique_indexes(
        state,
        &[preflight_record],
        &BTreeSet::new(),
        &BTreeSet::new(),
        true,
    )
    .await?;

    let _write = begin_runtime_write(state).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        durability,
        state.schema.version(),
        WalPayload::RecordUpserted {
            record: draft.clone(),
        },
    )
    .await?;

    let record = draft.into_record(wal_record.lsn);
    let order = record_order_for_logical_table(state, &record.table)?;
    state.record_hot.upsert(&record).await;
    state
        .object_refs
        .retain_record_for_schema(&state.schema.schema(), &record)
        .await
        .map_err(ApiError::internal)?;
    state
        .record_projection_applier
        .enqueue_upsert(
            wal_record.lsn,
            record.clone(),
            prepared.indexes.clone(),
            order.clone(),
        )
        .await
        .map_err(ApiError::internal)?;

    publish_delivery_event(
        state,
        DeliveryEvent::RecordUpserted {
            table: prepared.table,
            key: prepared.key,
            record: record.clone(),
        },
    );
    maybe_checkpoint(state).await?;

    Ok(record)
}

pub(crate) async fn commit_record_delete(
    state: &AppState,
    table: String,
    key: String,
    durability: Durability,
    expected_lsn: Option<u64>,
    client_mutation_id: Option<String>,
) -> Result<DeleteRecordResponse, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if durability != Durability::Volatile
        && let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())?
    {
        match existing {
            CommittedMutation::RecordDeleted { response } => return Ok(response),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    validate_table_path(&table, state)?;
    if !ensure_safe_record_component(&key) {
        return Err(ApiError::bad_request("invalid record key"));
    }
    commit_prepared_record_delete(
        state,
        PreparedRecordDelete {
            path: format!("tables/{table}/{key}"),
            shard_key: format!("{table}:{key}"),
            table,
            key,
        },
        durability,
        expected_lsn,
        client_mutation_id,
    )
    .await
}

pub(crate) async fn commit_prepared_record_delete(
    state: &AppState,
    prepared: PreparedRecordDelete,
    durability: Durability,
    expected_lsn: Option<u64>,
    client_mutation_id: Option<String>,
) -> Result<DeleteRecordResponse, ApiError> {
    ensure_runtime_accepting_writes(state).await?;
    if durability != Durability::Volatile
        && let Some(existing) = find_committed_mutation(state, client_mutation_id.as_deref())?
    {
        match existing {
            CommittedMutation::RecordDeleted { response } => return Ok(response),
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    if durability == Durability::Volatile {
        ensure_record_table_accepts_volatile(state, &prepared.table)?;
        ensure_expected_record_lsn_for_live_state(
            state,
            &prepared.table,
            &prepared.key,
            expected_lsn,
        )
        .await?;
        let Some(current) = state
            .record_hot
            .get(&prepared.table, &prepared.key)
            .await
            .flatten()
        else {
            return Ok(DeleteRecordResponse {
                table: prepared.table,
                key: prepared.key,
                deleted: false,
                lsn: 0,
                deleted_at_ms: None,
                path: volatile_record_path(&prepared.path),
            });
        };
        if !is_volatile_record_path(&current.path) {
            return Ok(DeleteRecordResponse {
                table: prepared.table,
                key: prepared.key,
                deleted: false,
                lsn: current.lsn,
                deleted_at_ms: None,
                path: current.path,
            });
        }
        state
            .record_hot
            .delete(&prepared.table, &prepared.key)
            .await;
        let deleted_at_ms = now_ms();
        let path = volatile_record_path(&prepared.path);
        publish_delivery_event(
            state,
            DeliveryEvent::RecordDeleted {
                table: prepared.table.clone(),
                key: prepared.key.clone(),
                deleted_at_ms,
                lsn: 0,
                path: path.clone(),
                previous_record: Some(current),
            },
        );
        return Ok(DeleteRecordResponse {
            table: prepared.table,
            key: prepared.key,
            deleted: true,
            lsn: 0,
            deleted_at_ms: Some(deleted_at_ms),
            path,
        });
    }

    let current = state
        .records
        .get(&prepared.table, &prepared.key)
        .await
        .map_err(ApiError::internal)?;
    let current_lsn = current.as_ref().map(|record| record.lsn).unwrap_or(0);
    if let Some(expected_lsn) = expected_lsn
        && current_lsn != expected_lsn
    {
        return Err(ApiError::conflict(format!(
            "record version conflict: expected lsn {expected_lsn}, found {current_lsn}"
        )));
    }

    if current.is_none() {
        if let Some(client_mutation_id) = client_mutation_id.clone() {
            let _write = begin_runtime_write(state).await?;
            let shard = writable_wal_shard_for_key(state, &prepared.shard_key).await?;
            ensure_shard_not_frozen(state, shard.index).await?;
            let wal_record = append_ordered_wal_record(
                state,
                shard,
                durability,
                state.schema.version(),
                WalPayload::ClientMutationRecorded {
                    client_mutation_id,
                    record: ClientMutationRecord::RecordDeleteNoop {
                        table: prepared.table.clone(),
                        key: prepared.key.clone(),
                        path: prepared.path.clone(),
                    },
                },
            )
            .await?;
            maybe_checkpoint(state).await?;
            return Ok(DeleteRecordResponse {
                table: prepared.table,
                key: prepared.key,
                deleted: false,
                lsn: wal_record.lsn,
                deleted_at_ms: None,
                path: prepared.path,
            });
        }
        return Ok(DeleteRecordResponse {
            table: prepared.table,
            key: prepared.key,
            deleted: false,
            lsn: current_lsn,
            deleted_at_ms: None,
            path: prepared.path,
        });
    }
    let previous_record = current.clone();

    let deleted_at_ms = now_ms();
    let draft = DbRecordDeleteDraft {
        table: prepared.table.clone(),
        key: prepared.key.clone(),
        deleted_at_ms,
        path: prepared.path.clone(),
        client_mutation_id,
    };

    let _write = begin_runtime_write(state).await?;
    let shard = writable_wal_shard_for_key(state, &prepared.shard_key).await?;
    ensure_shard_not_frozen(state, shard.index).await?;
    let wal_record = append_ordered_wal_record(
        state,
        shard,
        durability,
        state.schema.version(),
        WalPayload::RecordDeleted {
            record: draft.clone(),
        },
    )
    .await?;

    state
        .record_hot
        .delete_durable(&prepared.table, &prepared.key, wal_record.lsn)
        .await;
    state
        .object_refs
        .remove_record(&prepared.path)
        .await
        .map_err(ApiError::internal)?;
    state
        .record_projection_applier
        .enqueue_delete(wal_record.lsn, prepared.table.clone(), prepared.key.clone())
        .await
        .map_err(ApiError::internal)?;

    publish_delivery_event(
        state,
        DeliveryEvent::RecordDeleted {
            table: prepared.table.clone(),
            key: prepared.key.clone(),
            deleted_at_ms,
            lsn: wal_record.lsn,
            path: prepared.path.clone(),
            previous_record,
        },
    );
    maybe_checkpoint(state).await?;

    Ok(DeleteRecordResponse {
        table: prepared.table,
        key: prepared.key,
        deleted: true,
        lsn: wal_record.lsn,
        deleted_at_ms: Some(deleted_at_ms),
        path: prepared.path,
    })
}

pub(crate) async fn upsert_nested_record(
    State(state): State<AppState>,
    axum::extract::Path((table, parent_key, nested, nested_key)): axum::extract::Path<(
        String,
        String,
        String,
        String,
    )>,
    Json(request): Json<UpsertRecordRequest>,
) -> Result<Json<RecordResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    ensure_json_value_limit(
        "nested record value",
        &request.value,
        state.limits.max_record_value_bytes,
    )?;
    validate_nested_record_identity(
        &table,
        &parent_key,
        &nested,
        &nested_key,
        &request.value,
        &state,
    )?;
    validate_nested_record_object_refs(&state, &table, &nested, &request.value).await?;
    let logical_table = nested_record_table(&table, &nested);
    let logical_key = nested_record_key(&parent_key, &nested_key);
    let indexes = state
        .schema
        .nested_table_indexes(&table, &nested)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let record = commit_prepared_record_upsert(
        &state,
        PreparedRecordUpsert {
            path: nested_record_path(&table, &parent_key, &nested, &nested_key),
            shard_key: format!("{table}:{parent_key}"),
            table: logical_table,
            key: logical_key,
            value: request.value,
            indexes,
        },
        request.durability,
        request.expected_lsn,
        normalize_client_mutation_id(request.client_mutation_id)?,
    )
    .await?;
    Ok(Json(RecordResponse { record }))
}

pub(crate) async fn delete_nested_record(
    State(state): State<AppState>,
    axum::extract::Path((table, parent_key, nested, nested_key)): axum::extract::Path<(
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<DeleteRecordQuery>,
) -> Result<Json<DeleteRecordResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    validate_nested_record_path(&table, &parent_key, &nested, &nested_key, &state)?;
    let response = commit_prepared_record_delete(
        &state,
        PreparedRecordDelete {
            path: nested_record_path(&table, &parent_key, &nested, &nested_key),
            shard_key: format!("{table}:{parent_key}"),
            table: nested_record_table(&table, &nested),
            key: nested_record_key(&parent_key, &nested_key),
        },
        query.durability,
        query.expected_lsn,
        normalize_client_mutation_id(query.client_mutation_id)?,
    )
    .await?;
    Ok(Json(response))
}

pub(crate) async fn get_nested_record(
    State(state): State<AppState>,
    axum::extract::Path((table, parent_key, nested, nested_key)): axum::extract::Path<(
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<RecordReadConsistencyQuery>,
) -> Result<Json<RecordResponse>, ApiError> {
    validate_nested_record_path(&table, &parent_key, &nested, &nested_key, &state)?;
    resolve_record_read_consistency(&state, &query).await?;
    let logical_table = nested_record_table(&table, &nested);
    let logical_key = nested_record_key(&parent_key, &nested_key);
    let record = get_record_from_live_or_disk(&state, &logical_table, &logical_key)
        .await?
        .ok_or_else(|| ApiError::not_found("nested record not found"))?;
    Ok(Json(RecordResponse { record }))
}

pub(crate) async fn list_nested_records(
    State(state): State<AppState>,
    axum::extract::Path((table, parent_key, nested)): axum::extract::Path<(String, String, String)>,
    Query(query): Query<ListRecordsQuery>,
) -> Result<Json<ListRecordsResponse>, ApiError> {
    resolve_record_read_consistency(&state, &query.consistency).await?;
    execute_record_list_query(&state, table, Some(parent_key), Some(nested), query)
        .await
        .map(Json)
}

pub(crate) async fn query_records_by_index(
    State(state): State<AppState>,
    axum::extract::Path((table, index_name)): axum::extract::Path<(String, String)>,
    Query(query): Query<QueryRecordsByIndexQuery>,
) -> Result<Json<ListRecordsResponse>, ApiError> {
    resolve_record_read_consistency(&state, &query.consistency).await?;
    execute_record_index_query(&state, table, None, None, index_name, query)
        .await
        .map(Json)
}

pub(crate) async fn query_nested_records_by_index(
    State(state): State<AppState>,
    axum::extract::Path((table, parent_key, nested, index_name)): axum::extract::Path<(
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<QueryRecordsByIndexQuery>,
) -> Result<Json<ListRecordsResponse>, ApiError> {
    resolve_record_read_consistency(&state, &query.consistency).await?;
    execute_record_index_query(
        &state,
        table,
        Some(parent_key),
        Some(nested),
        index_name,
        query,
    )
    .await
    .map(Json)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum RecordTransactionOperationRequest {
    Upsert {
        table: String,
        key: String,
        value: serde_json::Value,
        expected_lsn: Option<u64>,
    },
    Delete {
        table: String,
        key: String,
        expected_lsn: Option<u64>,
    },
    NestedUpsert {
        table: String,
        parent_key: String,
        nested: String,
        nested_key: String,
        value: serde_json::Value,
        expected_lsn: Option<u64>,
    },
    NestedDelete {
        table: String,
        parent_key: String,
        nested: String,
        nested_key: String,
        expected_lsn: Option<u64>,
    },
}

pub(crate) fn validate_nested_record_path(
    table: &str,
    parent_key: &str,
    nested: &str,
    nested_key: &str,
    state: &AppState,
) -> Result<(), ApiError> {
    validate_nested_table_path(table, parent_key, nested, state)?;
    if !ensure_safe_record_component(nested_key) {
        return Err(ApiError::bad_request("invalid nested record key"));
    }
    Ok(())
}

pub(crate) fn validate_nested_table_path(
    table: &str,
    parent_key: &str,
    nested: &str,
    state: &AppState,
) -> Result<(), ApiError> {
    validate_table_path(table, state)?;
    if !ensure_safe_record_component(parent_key) {
        return Err(ApiError::bad_request("invalid parent record key"));
    }
    if !ensure_safe_record_component(nested) {
        return Err(ApiError::bad_request("invalid nested table name"));
    }
    let schema = state.schema.schema();
    if schema
        .tables
        .get(table)
        .and_then(|table| table.nested.get(nested))
        .is_none()
    {
        return Err(ApiError::not_found("nested table not found"));
    }
    Ok(())
}

pub(crate) fn validate_record_path(
    table: &str,
    key: &str,
    state: &AppState,
) -> Result<(), ApiError> {
    validate_table_path(table, state)?;
    if !ensure_safe_record_component(key) {
        return Err(ApiError::bad_request("invalid record key"));
    }
    Ok(())
}

pub(crate) fn validate_table_path(table: &str, state: &AppState) -> Result<(), ApiError> {
    if !ensure_safe_record_component(table) {
        return Err(ApiError::bad_request("invalid table name"));
    }
    if !state.schema.schema().tables.contains_key(table) {
        return Err(ApiError::not_found("table not found"));
    }
    Ok(())
}

pub(crate) fn nested_record_table(table: &str, nested: &str) -> String {
    format!("{table}.{nested}")
}

pub(crate) fn nested_record_key(parent_key: &str, nested_key: &str) -> String {
    format!("{parent_key}:{nested_key}")
}

pub(crate) fn nested_record_prefix(parent_key: &str) -> String {
    format!("{parent_key}:")
}

pub(crate) fn nested_record_path(
    table: &str,
    parent_key: &str,
    nested: &str,
    nested_key: &str,
) -> String {
    format!("tables/{table}/{parent_key}/{nested}/{nested_key}")
}

pub(crate) fn nested_key_from_logical_key(parent_key: &str, logical_key: &str) -> Option<String> {
    logical_key
        .strip_prefix(&nested_record_prefix(parent_key))
        .map(ToOwned::to_owned)
}

pub(crate) fn volatile_record_path(path: &str) -> String {
    if is_volatile_record_path(path) {
        path.to_string()
    } else {
        format!("volatile/{path}")
    }
}

pub(crate) fn is_volatile_record_path(path: &str) -> bool {
    path.starts_with("volatile/")
}

pub(crate) fn record_identity(table: &str, key: &str) -> String {
    format!("{table}/{key}")
}

pub(crate) fn record_transaction_operation_shard_key(
    operation: &RecordTransactionOperationRequest,
) -> String {
    match operation {
        RecordTransactionOperationRequest::Upsert { table, key, .. }
        | RecordTransactionOperationRequest::Delete { table, key, .. } => {
            format!("{table}:{key}")
        }
        RecordTransactionOperationRequest::NestedUpsert {
            table, parent_key, ..
        }
        | RecordTransactionOperationRequest::NestedDelete {
            table, parent_key, ..
        } => format!("{table}:{parent_key}"),
    }
}

pub(crate) fn record_transaction_operation_identity(
    operation: &RecordTransactionOperationRequest,
) -> String {
    match operation {
        RecordTransactionOperationRequest::Upsert { table, key, .. }
        | RecordTransactionOperationRequest::Delete { table, key, .. } => {
            format!("{table}/{key}")
        }
        RecordTransactionOperationRequest::NestedUpsert {
            table,
            parent_key,
            nested,
            nested_key,
            ..
        }
        | RecordTransactionOperationRequest::NestedDelete {
            table,
            parent_key,
            nested,
            nested_key,
            ..
        } => format!(
            "{}/{}",
            nested_record_table(table, nested),
            nested_record_key(parent_key, nested_key)
        ),
    }
}

pub(crate) fn record_batch_child_client_mutation_id(
    base: &str,
    shard: usize,
    chunk_index: usize,
    split_transaction: bool,
) -> String {
    if !split_transaction {
        return base.to_string();
    }
    let suffix = format!(":s{shard}p{chunk_index}");
    if base.len() + suffix.len() <= 160 {
        return format!("{base}{suffix}");
    }
    let digest = hex_lower(&Sha256::digest(base.as_bytes()));
    let digest = &digest[..16];
    let marker = format!(":h{digest}");
    let keep = 160_usize.saturating_sub(marker.len() + suffix.len());
    let prefix: String = base.chars().take(keep).collect();
    format!("{prefix}{marker}{suffix}")
}

pub(crate) fn ensure_transaction_key_once(
    seen_keys: &mut BTreeSet<String>,
    table: &str,
    key: &str,
) -> Result<(), ApiError> {
    let identity = format!("{table}/{key}");
    if !seen_keys.insert(identity.clone()) {
        return Err(ApiError::bad_request(format!(
            "record transaction contains duplicate operation for {identity}"
        )));
    }
    Ok(())
}

pub(crate) fn ensure_record_batch_key_once(
    seen_keys: &mut BTreeSet<String>,
    operation: &RecordTransactionOperationRequest,
) -> Result<(), ApiError> {
    let identity = record_transaction_operation_identity(operation);
    if !seen_keys.insert(identity.clone()) {
        return Err(ApiError::bad_request(format!(
            "record batch contains duplicate operation for {identity}"
        )));
    }
    Ok(())
}

pub(crate) fn ensure_transaction_partition(
    touched_partitions: &mut BTreeSet<String>,
    nested_partition: &mut Option<String>,
    partition: &str,
    is_nested: bool,
) -> Result<(), ApiError> {
    if is_nested {
        match nested_partition.as_deref() {
            Some(existing) if existing != partition => {
                return Err(ApiError::bad_request(format!(
                    "nested transaction operations must target one parent partition; got {existing} and {partition}"
                )));
            }
            Some(_) => {}
            None => {
                *nested_partition = Some(partition.to_string());
            }
        }
        if touched_partitions
            .iter()
            .any(|existing| existing.as_str() != partition)
        {
            return Err(ApiError::bad_request(format!(
                "nested transaction operations must not be mixed with a different partition than {partition}"
            )));
        }
    } else if nested_partition
        .as_deref()
        .is_some_and(|existing| existing != partition)
    {
        return Err(ApiError::bad_request(format!(
            "nested transaction operations must not be mixed with top-level partition {partition}"
        )));
    }

    touched_partitions.insert(partition.to_string());
    Ok(())
}

pub(crate) fn ensure_same_transaction_shard(
    shard_index: &mut Option<usize>,
    shard: usize,
) -> Result<(), ApiError> {
    match *shard_index {
        Some(existing) if existing != shard => Err(ApiError::bad_request(format!(
            "record transaction operations must target one shard; got shards {existing} and {shard}"
        ))),
        Some(_) => Ok(()),
        None => {
            *shard_index = Some(shard);
            Ok(())
        }
    }
}

pub(crate) fn nested_schema_order(
    state: &AppState,
    table: &str,
    nested: &str,
) -> Result<Vec<RecordOrderTerm>, ApiError> {
    let schema = state.schema.schema();
    let nested = schema
        .tables
        .get(table)
        .and_then(|table| table.nested.get(nested))
        .ok_or_else(|| ApiError::bad_request("nested table not found"))?;
    let StorageClass::ChatLog { order, .. } = &nested.storage else {
        return Err(ApiError::bad_request(
            "nested table does not define schema order",
        ));
    };
    let terms = parse_schema_order_terms(order)?;
    if terms.is_empty() {
        return Err(ApiError::bad_request("nested table schema order is empty"));
    }
    Ok(terms)
}

pub(crate) fn record_order_for_logical_table(
    state: &AppState,
    logical_table: &str,
) -> Result<Option<Vec<RecordOrderTerm>>, ApiError> {
    let Some((table_name, nested_name)) = logical_table.split_once('.') else {
        return Ok(None);
    };
    let schema = state.schema.schema();
    let Some(nested) = schema
        .tables
        .get(table_name)
        .and_then(|table| table.nested.get(nested_name))
    else {
        return Ok(None);
    };
    let StorageClass::ChatLog { order, .. } = &nested.storage else {
        return Ok(None);
    };
    Ok(Some(parse_schema_order_terms(order)?))
}

fn parse_schema_order_terms(order: &[String]) -> Result<Vec<RecordOrderTerm>, ApiError> {
    parse_record_order_terms(order).map_err(|err| ApiError::bad_request(err.to_string()))
}

pub(crate) fn schema_indexes_by_table(
    schema: &DatabaseSchema,
) -> BTreeMap<String, BTreeMap<String, IndexSchema>> {
    let mut out = BTreeMap::new();
    for (table_name, table) in &schema.tables {
        out.insert(table_name.clone(), table.indexes.clone());
        for (nested_name, nested) in &table.nested {
            out.insert(
                nested_record_table(table_name, nested_name),
                nested.indexes.clone(),
            );
        }
    }
    out
}

pub(crate) fn schema_orders_by_table(
    schema: &DatabaseSchema,
) -> anyhow::Result<BTreeMap<String, Vec<RecordOrderTerm>>> {
    let mut out = BTreeMap::new();
    for (table_name, table) in &schema.tables {
        for (nested_name, nested) in &table.nested {
            if let StorageClass::ChatLog { order, .. } = &nested.storage {
                out.insert(
                    nested_record_table(table_name, nested_name),
                    parse_record_order_terms(order)?,
                );
            }
        }
    }
    Ok(out)
}

pub(crate) fn records_from_wal_records(records: Vec<WalRecord>) -> Vec<DbRecord> {
    let mut records_by_path = BTreeMap::new();
    for wal_record in records {
        match wal_record.payload {
            WalPayload::RecordUpserted { record } => {
                let record = record.into_record(wal_record.lsn);
                records_by_path.insert(record.path.clone(), record);
            }
            WalPayload::RecordDeleted { record } => {
                records_by_path.remove(&record.path);
            }
            WalPayload::RecordTransactionCommitted { operations, .. } => {
                for operation in operations {
                    match operation {
                        DbRecordMutationDraft::Upsert { record } => {
                            let record = record.into_record(wal_record.lsn);
                            records_by_path.insert(record.path.clone(), record);
                        }
                        DbRecordMutationDraft::Delete { record } => {
                            records_by_path.remove(&record.path);
                        }
                    }
                }
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
            | WalPayload::ClientMutationRecorded { .. } => {}
        }
    }
    records_by_path.into_values().collect()
}

pub(crate) fn validate_record_predicate(predicate: &RecordPredicate) -> Result<(), ApiError> {
    if predicate.all.len() > 32 {
        return Err(ApiError::bad_request(
            "record predicate supports at most 32 all terms",
        ));
    }
    for term in &predicate.all {
        if term.field.is_empty() || term.field.len() > 256 {
            return Err(ApiError::bad_request("invalid predicate field"));
        }
        for component in term.field.split('.') {
            if component.is_empty()
                || component.len() > 64
                || !component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err(ApiError::bad_request("invalid predicate field"));
            }
        }
        if !matches!(term.op, RecordPredicateOp::Exists) && term.value.is_none() {
            return Err(ApiError::bad_request("predicate term requires value"));
        }
    }
    Ok(())
}

pub(crate) fn record_matches_predicate(
    record: &DbRecord,
    predicate: Option<&RecordPredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    predicate
        .all
        .iter()
        .all(|term| record_matches_predicate_term(record, term))
}

pub(crate) fn record_matches_index_values(
    record: &DbRecord,
    index: &IndexSchema,
    values: &[serde_json::Value],
) -> bool {
    record_index_values(record, index).is_ok_and(|record_values| record_values == values)
}

pub(crate) fn record_matches_index_value_prefix(
    record: &DbRecord,
    index: &IndexSchema,
    values: &[serde_json::Value],
) -> bool {
    record_index_values(record, index).is_ok_and(|record_values| record_values.starts_with(values))
}

pub(crate) fn parse_index_query_values(
    query: &QueryRecordsByIndexQuery,
    index: &IndexSchema,
) -> Result<Vec<serde_json::Value>, ApiError> {
    let values = if let Some(values) = &query.values {
        serde_json::from_str::<Vec<serde_json::Value>>(values)
            .map_err(|err| ApiError::bad_request(format!("invalid values JSON: {err}")))?
    } else if let Some(value) = &query.value {
        vec![parse_index_value(value)]
    } else {
        return Err(ApiError::bad_request("value or values is required"));
    };
    if values.len() != index.fields.len() {
        return Err(ApiError::bad_request(format!(
            "index requires {} value(s), got {}",
            index.fields.len(),
            values.len()
        )));
    }
    if values.iter().any(|value| {
        !matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
                | serde_json::Value::Null
        )
    }) {
        return Err(ApiError::bad_request("index query values must be scalar"));
    }
    Ok(values)
}

pub(crate) fn parse_index_prefix_values(
    values_json: &str,
    index: &IndexSchema,
) -> Result<Vec<serde_json::Value>, ApiError> {
    let values = serde_json::from_str::<Vec<serde_json::Value>>(values_json)
        .map_err(|err| ApiError::bad_request(format!("invalid indexValues JSON: {err}")))?;
    if values.is_empty() {
        return Err(ApiError::bad_request("indexValues must not be empty"));
    }
    if values.len() > index.fields.len() {
        return Err(ApiError::bad_request(format!(
            "index prefix accepts at most {} value(s), got {}",
            index.fields.len(),
            values.len()
        )));
    }
    if values.iter().any(|value| {
        !matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
                | serde_json::Value::Null
        )
    }) {
        return Err(ApiError::bad_request("indexValues must be scalar"));
    }
    Ok(values)
}

pub(crate) fn parse_index_bound_values(
    value: &Option<String>,
    values: &Option<String>,
    index: &IndexSchema,
) -> Result<Option<Vec<serde_json::Value>>, ApiError> {
    let Some(values) = parse_optional_index_values(value, values)? else {
        return Ok(None);
    };
    if values.len() != index.fields.len() {
        return Err(ApiError::bad_request(format!(
            "index range bound requires {} value(s), got {}",
            index.fields.len(),
            values.len()
        )));
    }
    if values.iter().any(|value| {
        !matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
                | serde_json::Value::Null
        )
    }) {
        return Err(ApiError::bad_request("index range values must be scalar"));
    }
    Ok(Some(values))
}

fn record_matches_predicate_term(record: &DbRecord, term: &RecordPredicateTerm) -> bool {
    let actual = value_at_field_path(&record.value, &term.field);
    match term.op {
        RecordPredicateOp::Exists => {
            let expected = term
                .value
                .as_ref()
                .and_then(|value| value.as_bool())
                .unwrap_or(true);
            actual.is_some() == expected
        }
        RecordPredicateOp::Eq => actual
            .zip(term.value.as_ref())
            .is_some_and(|(actual, expected)| actual == expected),
        RecordPredicateOp::Ne => actual
            .zip(term.value.as_ref())
            .is_some_and(|(actual, expected)| actual != expected),
        RecordPredicateOp::Lt => {
            actual
                .zip(term.value.as_ref())
                .is_some_and(|(actual, expected)| {
                    compare_predicate_values(actual, expected)
                        .is_some_and(|ordering| ordering.is_lt())
                })
        }
        RecordPredicateOp::Lte => {
            actual
                .zip(term.value.as_ref())
                .is_some_and(|(actual, expected)| {
                    compare_predicate_values(actual, expected)
                        .is_some_and(|ordering| ordering.is_le())
                })
        }
        RecordPredicateOp::Gt => {
            actual
                .zip(term.value.as_ref())
                .is_some_and(|(actual, expected)| {
                    compare_predicate_values(actual, expected)
                        .is_some_and(|ordering| ordering.is_gt())
                })
        }
        RecordPredicateOp::Gte => {
            actual
                .zip(term.value.as_ref())
                .is_some_and(|(actual, expected)| {
                    compare_predicate_values(actual, expected)
                        .is_some_and(|ordering| ordering.is_ge())
                })
        }
        RecordPredicateOp::Contains => actual
            .zip(term.value.as_ref())
            .is_some_and(|(actual, expected)| predicate_contains(actual, expected)),
        RecordPredicateOp::StartsWith => actual
            .and_then(|value| value.as_str())
            .zip(term.value.as_ref().and_then(|value| value.as_str()))
            .is_some_and(|(actual, expected)| actual.starts_with(expected)),
    }
}

fn value_at_field_path<'a>(
    mut value: &'a serde_json::Value,
    field_path: &str,
) -> Option<&'a serde_json::Value> {
    for component in field_path.split('.') {
        value = value.as_object()?.get(component)?;
    }
    Some(value)
}

fn compare_predicate_values(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    if let (Some(actual), Some(expected)) = (actual.as_f64(), expected.as_f64()) {
        return actual.partial_cmp(&expected);
    }
    if let (Some(actual), Some(expected)) = (actual.as_str(), expected.as_str()) {
        return Some(actual.cmp(expected));
    }
    None
}

fn predicate_contains(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    if let (Some(actual), Some(expected)) = (actual.as_str(), expected.as_str()) {
        return actual.contains(expected);
    }
    actual
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item == expected))
}

fn parse_optional_index_values(
    value: &Option<String>,
    values: &Option<String>,
) -> Result<Option<Vec<serde_json::Value>>, ApiError> {
    if let Some(values) = values {
        return serde_json::from_str::<Vec<serde_json::Value>>(values)
            .map(Some)
            .map_err(|err| ApiError::bad_request(format!("invalid values JSON: {err}")));
    }
    Ok(value.as_ref().map(|value| vec![parse_index_value(value)]))
}

fn parse_index_value(value: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .filter(|value| {
            matches!(
                value,
                serde_json::Value::String(_)
                    | serde_json::Value::Number(_)
                    | serde_json::Value::Bool(_)
                    | serde_json::Value::Null
            )
        })
        .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordResponse {
    pub(crate) record: DbRecord,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteRecordResponse {
    pub(crate) table: String,
    pub(crate) key: String,
    pub(crate) deleted: bool,
    pub(crate) lsn: u64,
    pub(crate) deleted_at_ms: Option<u64>,
    pub(crate) path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordTransactionResponse {
    pub(crate) lsn: u64,
    pub(crate) operations: Vec<RecordTransactionOperationResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordBatchResponse {
    pub(crate) lsn: u64,
    pub(crate) transaction_count: usize,
    pub(crate) operations: Vec<RecordTransactionOperationResponse>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum RecordTransactionOperationResponse {
    RecordUpserted {
        record: DbRecord,
    },
    RecordDeleted {
        table: String,
        key: String,
        deleted_at_ms: u64,
        lsn: u64,
        path: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListRecordsResponse {
    pub(crate) table: String,
    pub(crate) records: Vec<DbRecord>,
    pub(crate) next_after_key: Option<String>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordQueryRemovedRecord {
    pub(crate) table: String,
    pub(crate) key: String,
    pub(crate) path: String,
    pub(crate) deleted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lsn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordQueryDiff {
    pub(crate) table: String,
    pub(crate) added: Vec<DbRecord>,
    pub(crate) updated: Vec<DbRecord>,
    pub(crate) removed: Vec<RecordQueryRemovedRecord>,
    pub(crate) keys: Vec<String>,
    pub(crate) next_after_key: Option<String>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) has_more: bool,
}

pub(crate) fn deserialize_optional_record_predicate<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<RecordPredicate>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
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
            serde_json::from_str::<RecordPredicate>(&text).map_err(serde::de::Error::custom)?
        }
        other => {
            serde_json::from_value::<RecordPredicate>(other).map_err(serde::de::Error::custom)?
        }
    };
    Ok(Some(predicate))
}

type PreparedProjectionUpsert = (
    DbRecord,
    BTreeMap<String, IndexSchema>,
    Option<Vec<RecordOrderTerm>>,
);

pub(crate) async fn apply_record_transaction_operations(
    state: &AppState,
    operations: Vec<DbRecordMutationDraft>,
    lsn: u64,
    emit_events: bool,
) -> Result<Vec<RecordTransactionOperationResponse>, ApiError> {
    let mut upserts: Vec<PreparedProjectionUpsert> = Vec::new();
    let mut deletes = Vec::new();
    let mut responses = Vec::new();

    for operation in operations {
        match operation {
            DbRecordMutationDraft::Upsert { record: draft } => {
                let table = draft.table.clone();
                let indexes = state
                    .schema
                    .record_indexes(&table)
                    .map_err(|err| ApiError::bad_request(err.to_string()))?;
                let record = draft.into_record(lsn);
                let order = record_order_for_logical_table(state, &record.table)?;
                responses.push(RecordTransactionOperationResponse::RecordUpserted {
                    record: record.clone(),
                });
                upserts.push((record, indexes, order));
            }
            DbRecordMutationDraft::Delete { record } => {
                responses.push(RecordTransactionOperationResponse::RecordDeleted {
                    table: record.table.clone(),
                    key: record.key.clone(),
                    deleted_at_ms: record.deleted_at_ms,
                    lsn,
                    path: record.path.clone(),
                });
                deletes.push(record);
            }
        }
    }

    let mut projection_mutations = Vec::with_capacity(upserts.len() + deletes.len());
    for delete in &deletes {
        state
            .record_hot
            .delete_durable(&delete.table, &delete.key, lsn)
            .await;
        projection_mutations.push(RecordProjectionMutation::Delete {
            table: delete.table.clone(),
            key: delete.key.clone(),
        });
    }
    state
        .record_hot
        .upsert_many(upserts.iter().map(|(record, _, _)| record))
        .await;
    for (record, indexes, order) in &upserts {
        projection_mutations.push(RecordProjectionMutation::Upsert {
            record: record.clone(),
            indexes: indexes.clone(),
            order: order.clone(),
        });
    }

    state
        .object_refs
        .apply_record_changes_for_schema(
            &state.schema.schema(),
            deletes.iter().map(|delete| delete.path.as_str()),
            upserts.iter().map(|(record, _, _)| record),
        )
        .await
        .map_err(ApiError::internal)?;
    state
        .record_projection_applier
        .enqueue_transaction(lsn, projection_mutations)
        .await
        .map_err(ApiError::internal)?;

    if emit_events {
        for response in &responses {
            match response {
                RecordTransactionOperationResponse::RecordUpserted { record } => {
                    publish_delivery_event(
                        state,
                        DeliveryEvent::RecordUpserted {
                            table: record.table.clone(),
                            key: record.key.clone(),
                            record: record.clone(),
                        },
                    );
                }
                RecordTransactionOperationResponse::RecordDeleted {
                    table,
                    key,
                    deleted_at_ms,
                    lsn,
                    path,
                } => {
                    publish_delivery_event(
                        state,
                        DeliveryEvent::RecordDeleted {
                            table: table.clone(),
                            key: key.clone(),
                            deleted_at_ms: *deleted_at_ms,
                            lsn: *lsn,
                            path: path.clone(),
                            previous_record: None,
                        },
                    );
                }
            }
        }
    }

    Ok(responses)
}

pub(crate) struct PreparedRecordUpsert {
    pub(crate) table: String,
    pub(crate) key: String,
    pub(crate) value: serde_json::Value,
    pub(crate) path: String,
    pub(crate) shard_key: String,
    pub(crate) indexes: BTreeMap<String, IndexSchema>,
}

pub(crate) struct PreparedRecordDelete {
    pub(crate) table: String,
    pub(crate) key: String,
    pub(crate) path: String,
    pub(crate) shard_key: String,
}

pub(crate) struct PreparedRecordTransaction {
    pub(crate) shard: usize,
    pub(crate) durability: Durability,
    pub(crate) operations: Vec<DbRecordMutationDraft>,
}
