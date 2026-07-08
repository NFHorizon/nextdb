#![cfg_attr(not(feature = "cluster"), allow(dead_code))]

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, header},
};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::warn;

use crate::{
    AppState,
    api::{
        behavior::validate_loaded_behavior_manifests_schema,
        error::ApiError,
        events::publish_delivery_event,
        guards::ensure_shard_index,
        mutation::committed_mutation_entry_from_wal_record,
        objects::validate_records_object_refs_against_schema,
        records::{
            apply_record_transaction_operations, record_order_for_logical_table,
            schema_indexes_by_table, schema_orders_by_table,
        },
        runtime::{read_all_records_from_wal_paths, run_compact_wal, write_snapshot},
    },
    cluster::ShardRole,
    model::{DeliveryEvent, Durability, WalChecksumStatus, WalPayload, WalRecord},
    schema::{DatabaseSchema, SchemaMigrationPlan},
    util::{now_ms, shard_index},
    wal::{
        self, WalAppendRequest, WalChecksumSealFileReport, WalChecksumSealReport,
        WalRemoteRepairReport, WalRemoteReplica, WalShard, read_records_from_wal_paths,
    },
};

fn allocate_lsn(state: &AppState) -> u64 {
    state.next_lsn.fetch_add(1, Ordering::AcqRel) + 1
}

pub(crate) async fn append_ordered_wal_record(
    state: &AppState,
    shard: &WalShard,
    durability: Durability,
    schema_version: u32,
    payload: WalPayload,
) -> Result<WalRecord, ApiError> {
    let shard_epoch = cluster_epoch_for_shard(state, shard.index).await;
    let owner_node_id = state.cluster.node_id().to_string();
    let pending = {
        let _send_guard = shard.append_send_lock.lock().await;
        shard
            .writer
            .enqueue_many(vec![WalAppendRequest {
                lsn: allocate_lsn(state),
                shard_epoch,
                owner_node_id,
                durability,
                schema_version,
                payload,
            }])
            .await
            .map_err(map_wal_append_error)?
    };
    let mut records = pending.wait().await.map_err(map_wal_append_error)?;
    let record = records
        .pop()
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("missing WAL append record")))?;
    commit_lsn(state, record.lsn);
    note_committed_mutation_record(state, &record)?;
    Ok(record)
}

pub(crate) async fn append_ordered_wal_records(
    state: &AppState,
    shard: &WalShard,
    durability: Durability,
    schema_version: u32,
    payloads: Vec<WalPayload>,
) -> Result<Vec<WalRecord>, ApiError> {
    if payloads.is_empty() {
        return Ok(Vec::new());
    }
    let shard_epoch = cluster_epoch_for_shard(state, shard.index).await;
    let owner_node_id = state.cluster.node_id().to_string();
    let pending = {
        let _send_guard = shard.append_send_lock.lock().await;
        let requests = payloads
            .into_iter()
            .map(|payload| WalAppendRequest {
                lsn: allocate_lsn(state),
                shard_epoch,
                owner_node_id: owner_node_id.clone(),
                durability,
                schema_version,
                payload,
            })
            .collect();
        shard
            .writer
            .enqueue_many(requests)
            .await
            .map_err(map_wal_append_error)?
    };
    let records = pending.wait().await.map_err(map_wal_append_error)?;
    for record in &records {
        commit_lsn(state, record.lsn);
    }
    note_committed_mutation_records(state, &records)?;
    Ok(records)
}

fn commit_lsn(state: &AppState, lsn: u64) {
    state.current_lsn.fetch_max(lsn, Ordering::AcqRel);
}

pub(crate) fn note_committed_mutation_record(
    state: &AppState,
    record: &WalRecord,
) -> Result<(), ApiError> {
    let Some((client_mutation_id, mutation)) = committed_mutation_entry_from_wal_record(record)
    else {
        return Ok(());
    };
    state
        .client_mutations
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("client mutation index poisoned")))?
        .insert(client_mutation_id, mutation);
    Ok(())
}

fn note_committed_mutation_records(
    state: &AppState,
    records: &[WalRecord],
) -> Result<(), ApiError> {
    let entries = records
        .iter()
        .filter_map(committed_mutation_entry_from_wal_record)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Ok(());
    }
    let mut client_mutations = state
        .client_mutations
        .write()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("client mutation index poisoned")))?;
    for (client_mutation_id, mutation) in entries {
        client_mutations.insert(client_mutation_id, mutation);
    }
    Ok(())
}

fn map_wal_append_error(error: anyhow::Error) -> ApiError {
    let message = error.to_string();
    if message.contains("replicated WAL record epoch")
        || message.contains("replicated WAL record owner")
        || message.contains("replicated WAL record shard")
    {
        ApiError::conflict(message)
    } else {
        ApiError::internal(error)
    }
}

fn wal_shard_for_key<'a>(state: &'a AppState, key: &str) -> Result<&'a WalShard, ApiError> {
    let index = shard_index(key, state.wal_shards.len());
    state
        .wal_shards
        .get(index)
        .ok_or_else(|| ApiError::internal(anyhow::anyhow!("WAL shard {index} is not configured")))
}

pub(crate) async fn cluster_owner_for_shard(state: &AppState, shard: usize) -> String {
    let overrides = state.topology_overrides.read().await;
    state
        .cluster
        .owner_for_shard_with_overrides(shard, &overrides)
}

pub(crate) async fn cluster_epoch_for_shard(state: &AppState, shard: usize) -> u64 {
    let overrides = state.topology_overrides.read().await;
    state
        .cluster
        .epoch_for_shard_with_overrides(shard, &overrides)
}

pub(crate) async fn cluster_role_for_shard(state: &AppState, shard: usize) -> ShardRole {
    let overrides = state.topology_overrides.read().await;
    state
        .cluster
        .role_for_shard_with_overrides(shard, &overrides)
}

pub(crate) async fn writable_wal_shard_for_key<'a>(
    state: &'a AppState,
    key: &str,
) -> Result<&'a WalShard, ApiError> {
    let shard = wal_shard_for_key(state, key)?;
    writable_wal_shard_for_index(state, shard.index).await
}

pub(crate) async fn writable_wal_shard_for_index(
    state: &AppState,
    shard_index: usize,
) -> Result<&WalShard, ApiError> {
    let shard = state.wal_shards.get(shard_index).ok_or_else(|| {
        ApiError::internal(anyhow::anyhow!("WAL shard {shard_index} is not configured"))
    })?;
    let role = cluster_role_for_shard(state, shard.index).await;
    if state.cluster.enforce_ownership() && role != ShardRole::Owner {
        let owner = cluster_owner_for_shard(state, shard.index).await;
        let owner_url = state
            .cluster
            .node_url_for(&owner)
            .unwrap_or_else(|| "unknown".to_string());
        return Err(ApiError::owner_conflict(shard.index, owner, owner_url));
    }
    Ok(shard)
}

pub(crate) fn latest_lsn_for_shard(state: &AppState, shard: usize) -> Result<u64, ApiError> {
    let path = state
        .wal_paths
        .get(shard)
        .ok_or_else(|| ApiError::bad_request("shard is out of range"))?;
    Ok(wal::read_records_including_archives(path)
        .map_err(ApiError::internal)?
        .into_iter()
        .filter(|record| record.shard == shard)
        .map(|record| record.lsn)
        .max()
        .unwrap_or(0))
}

pub(crate) fn remote_ack_lsn_for_url(
    state: &AppState,
    shard: usize,
    target_url: &str,
) -> Option<u64> {
    let shard = state.wal_shards.get(shard)?;
    let status = shard.writer.try_status()?;
    let normalized_target = normalize_replication_url(target_url);
    status
        .remote_replicas
        .iter()
        .filter(|replica| replica.url == normalized_target)
        .map(|replica| replica.highest_acked_lsn)
        .max()
}

fn normalize_replication_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/admin/wal/replicate") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/admin/wal/replicate")
    }
}

pub(crate) async fn maybe_checkpoint(state: &AppState) -> Result<(), ApiError> {
    if state.checkpoint_every_lsn == 0 {
        return Ok(());
    }

    let current = state.current_lsn.load(Ordering::Acquire);
    let last = state.last_snapshot_lsn.load(Ordering::Acquire);
    if current.saturating_sub(last) < state.checkpoint_every_lsn {
        return Ok(());
    }

    if state
        .checkpoint_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(());
    }

    let state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_background_checkpoint(&state, current).await {
            warn!(error = %err.message, "background checkpoint failed");
        }
        state.checkpoint_in_flight.store(false, Ordering::Release);
    });
    Ok(())
}

async fn run_background_checkpoint(state: &AppState, trigger_lsn: u64) -> Result<(), ApiError> {
    let current = state.current_lsn.load(Ordering::Acquire).max(trigger_lsn);
    let last = state.last_snapshot_lsn.load(Ordering::Acquire);
    if current <= last {
        return Ok(());
    }

    let _ = write_snapshot(state, current).await?;
    if state.auto_compact_wal
        && let Err(err) = run_compact_wal(state).await
    {
        warn!(
            error = %err.message,
            "automatic WAL compaction failed after checkpoint"
        );
    }
    Ok(())
}

pub(crate) async fn wal_integrity(
    State(state): State<AppState>,
) -> Result<Json<wal::WalIntegrityReport>, ApiError> {
    Ok(Json(wal::inspect_integrity(&state.wal_paths)))
}

pub(crate) async fn replicate_wal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WalReplicateRequest>,
) -> Result<Json<WalReplicateResponse>, ApiError> {
    authorize_replication(&state, &headers)?;
    let shard = state
        .wal_shards
        .get(request.shard)
        .ok_or_else(|| ApiError::bad_request("replication shard is out of range"))?;

    let mut requested_records = Vec::with_capacity(request.records.len());
    for mut record in request.records {
        validate_replicated_wal_fence(&state, request.shard, &record).await?;
        if record.lsn == 0 {
            return Err(ApiError::bad_request(
                "replicated WAL records must have lsn",
            ));
        }
        if record.durability == Durability::Volatile {
            return Err(ApiError::bad_request(
                "volatile records must not be replicated through WAL",
            ));
        }
        match record.verify_checksum().map_err(ApiError::internal)? {
            WalChecksumStatus::Valid => {}
            WalChecksumStatus::Missing => record.ensure_checksum().map_err(ApiError::internal)?,
            WalChecksumStatus::Mismatch { expected } => {
                return Err(ApiError::bad_request(format!(
                    "replicated WAL checksum mismatch for LSN {}: expected {expected}, found {}",
                    record.lsn,
                    record.checksum.as_deref().unwrap_or_default()
                )));
            }
        }
        requested_records.push(record);
    }

    let existing_lsns: HashSet<u64> = read_records_from_wal_paths(&state.wal_paths)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|record| record.lsn)
        .collect();
    let requested = requested_records.len();
    let mut unique_records = BTreeMap::new();
    for record in requested_records {
        if !existing_lsns.contains(&record.lsn) {
            unique_records.entry(record.lsn).or_insert(record);
        }
    }
    let records: Vec<WalRecord> = unique_records.into_values().collect();

    let accepted = if records.is_empty() {
        0
    } else {
        let sync = request.sync.unwrap_or(true);
        let accepted = shard
            .writer
            .replicate(records.clone(), sync)
            .await
            .map_err(ApiError::internal)?;
        let mut latest_projection_lsn = None;
        for record in &records {
            if let Some(lsn) =
                apply_replicated_wal_record(&state, record.clone(), latest_projection_lsn).await?
            {
                latest_projection_lsn = Some(lsn);
            }
        }
        wait_for_replicated_record_projection(&state, latest_projection_lsn).await?;
        if let Err(err) = maybe_checkpoint(&state).await {
            warn!(
                error = %err.message,
                "checkpoint failed after inbound WAL replication"
            );
        }
        accepted
    };

    Ok(Json(WalReplicateResponse {
        shard: request.shard,
        accepted,
        skipped: requested.saturating_sub(accepted),
        current_lsn: state.current_lsn.load(Ordering::Acquire),
    }))
}

pub(crate) async fn repair_wal_remotes(
    State(state): State<AppState>,
    Query(query): Query<WalRemoteRepairQuery>,
) -> Result<Json<WalRemoteRepairResponse>, ApiError> {
    let shards: Vec<usize> = match query.shard {
        Some(shard) => {
            ensure_shard_index(&state, shard)?;
            vec![shard]
        }
        None => (0..state.wal_shards.len()).collect(),
    };
    let sync = query.sync.unwrap_or(true);
    let mut repaired = Vec::with_capacity(shards.len());
    for shard in shards {
        let report = state
            .wal_shards
            .get(shard)
            .ok_or_else(|| ApiError::bad_request("shard is out of range"))?
            .writer
            .repair_remote_replicas(query.after_lsn, sync)
            .await
            .map_err(ApiError::internal)?;
        repaired.push(report);
    }
    Ok(Json(WalRemoteRepairResponse {
        repaired,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
    }))
}

async fn validate_replicated_wal_fence(
    state: &AppState,
    shard: usize,
    record: &WalRecord,
) -> Result<(), ApiError> {
    if record.shard != shard {
        return Err(ApiError::conflict(format!(
            "replicated WAL record shard {} does not match request shard {shard}",
            record.shard
        )));
    }

    let expected_epoch = cluster_epoch_for_shard(state, shard).await;
    if record.shard_epoch != expected_epoch {
        return Err(ApiError::conflict(format!(
            "replicated WAL record epoch {} does not match local shard epoch {expected_epoch}",
            record.shard_epoch
        )));
    }

    let expected_owner = cluster_owner_for_shard(state, shard).await;
    if !record.owner_node_id.is_empty() && record.owner_node_id != expected_owner {
        return Err(ApiError::conflict(format!(
            "replicated WAL record owner {} does not match configured shard owner {expected_owner}",
            record.owner_node_id
        )));
    }

    Ok(())
}

fn authorize_replication(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = &state.wal_replication_token else {
        return Ok(());
    };

    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let token = headers
        .get("x-nextdb-replication-token")
        .and_then(|value| value.to_str().ok())
        .or(bearer);

    if token == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(ApiError::unauthorized("invalid WAL replication token"))
    }
}

pub(crate) async fn apply_replicated_wal_record(
    state: &AppState,
    record: WalRecord,
    previous_projection_lsn: Option<u64>,
) -> Result<Option<u64>, ApiError> {
    state.current_lsn.fetch_max(record.lsn, Ordering::AcqRel);
    state.next_lsn.fetch_max(record.lsn, Ordering::AcqRel);
    note_committed_mutation_record(state, &record)?;
    state.users.apply_wal_record(&record)?;

    let projection_lsn = match record.payload {
        WalPayload::MessageCreated { message } => {
            let room_id = message.room_id.clone();
            let message = message.into_message(record.lsn);
            state
                .chat_log
                .append(&message)
                .await
                .map_err(ApiError::internal)?;
            state.actors.apply_message(message.clone()).await;
            state
                .object_refs
                .retain_message(&message)
                .await
                .map_err(ApiError::internal)?;
            publish_delivery_event(state, DeliveryEvent::MessageCreated { room_id, message });
            None
        }
        WalPayload::UserEventPublished { event: draft } => {
            let user_id = draft.user_id.clone();
            let event = draft.into_event(record.lsn);
            publish_delivery_event(state, DeliveryEvent::UserEvent { user_id, event });
            None
        }
        WalPayload::UserUpserted { user: draft } => {
            let user = draft.into_profile(record.lsn);
            publish_delivery_event(
                state,
                DeliveryEvent::UserUpserted {
                    user_id: user.user_id.clone(),
                    user,
                },
            );
            None
        }
        WalPayload::RecordUpserted { record: draft } => {
            let table = draft.table.clone();
            let key = draft.key.clone();
            let record = draft.into_record(record.lsn);
            let projection_lsn = record.lsn;
            let indexes = state
                .schema
                .record_indexes(&table)
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
            let order = record_order_for_logical_table(state, &record.table)?;
            state.record_hot.upsert(&record).await;
            state
                .object_refs
                .retain_record_for_schema(&state.schema.schema(), &record)
                .await
                .map_err(ApiError::internal)?;
            state
                .record_projection_applier
                .enqueue_upsert(record.lsn, record.clone(), indexes, order)
                .await
                .map_err(ApiError::internal)?;
            publish_delivery_event(state, DeliveryEvent::RecordUpserted { table, key, record });
            Some(projection_lsn)
        }
        WalPayload::RecordDeleted { record: draft } => {
            state
                .record_hot
                .delete_durable(&draft.table, &draft.key, record.lsn)
                .await;
            state
                .object_refs
                .remove_record(&draft.path)
                .await
                .map_err(ApiError::internal)?;
            state
                .record_projection_applier
                .enqueue_delete(record.lsn, draft.table.clone(), draft.key.clone())
                .await
                .map_err(ApiError::internal)?;
            publish_delivery_event(
                state,
                DeliveryEvent::RecordDeleted {
                    table: draft.table,
                    key: draft.key,
                    deleted_at_ms: draft.deleted_at_ms,
                    lsn: record.lsn,
                    path: draft.path,
                    previous_record: None,
                },
            );
            Some(record.lsn)
        }
        WalPayload::RecordTransactionCommitted { operations, .. } => {
            apply_record_transaction_operations(state, operations, record.lsn, true).await?;
            Some(record.lsn)
        }
        WalPayload::SchemaApplied { schema, migration } => {
            wait_for_replicated_record_projection(state, previous_projection_lsn).await?;
            apply_replicated_schema(state, schema, migration).await?;
            None
        }
        WalPayload::ActorReminderScheduled { reminder } => {
            if let Some(entry) = crate::actor::actor_reminder_entry_from_draft(&reminder) {
                state.actors.schedule_reminder(entry);
            }
            None
        }
        WalPayload::ActorReminderCancelled {
            actor_kind,
            actor_key,
            reminder_id,
            ..
        }
        | WalPayload::ActorReminderFired {
            actor_kind,
            actor_key,
            reminder_id,
            ..
        } => {
            if let Some(entry) =
                crate::actor::actor_reminder_entry_from_draft(&crate::model::ActorReminderDraft {
                    actor_kind,
                    actor_key,
                    reminder_id,
                    due_at_ms: 1,
                    payload: None,
                })
            {
                state
                    .actors
                    .cancel_reminder(&entry.actor_id, &entry.reminder_id);
            }
            None
        }
        WalPayload::ObjectDeleted { object_id, .. } => {
            state
                .objects
                .delete_object(&object_id)
                .await
                .map_err(ApiError::internal)?;
            None
        }
        WalPayload::ObjectCommitted { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. }
        | WalPayload::ClientMutationRecorded { .. } => None,
    };

    Ok(projection_lsn)
}

pub(crate) async fn wait_for_replicated_record_projection(
    state: &AppState,
    lsn: Option<u64>,
) -> Result<(), ApiError> {
    let Some(lsn) = lsn else {
        return Ok(());
    };
    state
        .record_projection_applier
        .wait_for_lsn(lsn)
        .await
        .map_err(ApiError::internal)
}

async fn apply_replicated_schema(
    state: &AppState,
    schema: DatabaseSchema,
    _migration: SchemaMigrationPlan,
) -> Result<(), ApiError> {
    schema
        .validation_report()
        .into_result()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let records = read_all_records_from_wal_paths(&state.wal_paths).map_err(ApiError::internal)?;
    validate_loaded_behavior_manifests_schema(state, &schema).await?;
    validate_records_object_refs_against_schema(state, &schema, &records).await?;
    let schema_indexes = schema_indexes_by_table(&schema);
    let schema_orders =
        schema_orders_by_table(&schema).map_err(|err| ApiError::bad_request(err.to_string()))?;
    state
        .records
        .validate_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
        .await
        .map_err(ApiError::internal)?;
    state
        .records
        .force_rebuild_from_records_with_indexes(&records, &schema_indexes, &schema_orders)
        .await
        .map_err(ApiError::internal)?;
    state
        .schema
        .persist_candidate(&schema)
        .await
        .map_err(ApiError::internal)?;
    state
        .record_hot
        .reconfigure(&schema, &records, state.record_hot_durable_idle_ttl_ms)
        .await;
    state.schema.apply(schema);
    Ok(())
}

pub(crate) async fn ensure_shard_not_frozen(
    state: &AppState,
    shard: usize,
) -> Result<(), ApiError> {
    let controls = state.shard_controls.read().await;
    if let Some(control) = controls.get(&shard).filter(|control| control.frozen) {
        return Err(ApiError::locked(format!(
            "shard {shard} is frozen{}",
            control
                .reason
                .as_ref()
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default()
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalChecksumSealResponse {
    pub(crate) active: Vec<WalChecksumSealReport>,
    pub(crate) archives: Vec<WalChecksumSealArchiveReport>,
    pub(crate) records: usize,
    pub(crate) sealed: usize,
    pub(crate) already_sealed: usize,
    pub(crate) rewritten_files: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalChecksumSealArchiveReport {
    pub(crate) shard: usize,
    #[serde(flatten)]
    pub(crate) report: WalChecksumSealFileReport,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalArchiveRetentionQuery {
    pub(crate) dry_run: Option<bool>,
    pub(crate) before_lsn: Option<u64>,
    pub(crate) before_timestamp_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalArchiveRetentionFileReport {
    pub(crate) path: String,
    pub(crate) shard: usize,
    pub(crate) records: usize,
    pub(crate) min_lsn: Option<u64>,
    pub(crate) max_lsn: Option<u64>,
    pub(crate) min_timestamp_ms: Option<u64>,
    pub(crate) max_timestamp_ms: Option<u64>,
    pub(crate) action: String,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalArchiveRetentionResponse {
    pub(crate) dry_run: bool,
    pub(crate) before_lsn: Option<u64>,
    pub(crate) before_timestamp_ms: Option<u64>,
    pub(crate) candidates: usize,
    pub(crate) deleted: usize,
    pub(crate) retained: usize,
    pub(crate) reports: Vec<WalArchiveRetentionFileReport>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalReplicateRequest {
    pub(crate) shard: usize,
    pub(crate) records: Vec<WalRecord>,
    pub(crate) sync: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalReplicateResponse {
    pub(crate) shard: usize,
    pub(crate) accepted: usize,
    pub(crate) skipped: usize,
    pub(crate) current_lsn: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalRemoteRepairQuery {
    pub(crate) shard: Option<usize>,
    pub(crate) after_lsn: Option<u64>,
    pub(crate) sync: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WalRemoteRepairResponse {
    pub(crate) repaired: Vec<WalRemoteRepairReport>,
    pub(crate) current_lsn: u64,
}

pub(crate) async fn seal_wal_checksums(
    State(state): State<AppState>,
) -> Result<Json<WalChecksumSealResponse>, ApiError> {
    let mut active = Vec::with_capacity(state.wal_shards.len());
    let mut archives = Vec::new();

    for shard in &state.wal_shards {
        active.push(
            shard
                .writer
                .seal_checksums()
                .await
                .map_err(ApiError::internal)?,
        );
    }

    for (shard, wal_path) in state.wal_paths.iter().enumerate() {
        for archive_path in wal_archive_paths(wal_path).map_err(ApiError::internal)? {
            let report = crate::wal::seal_checksum_file(&archive_path)
                .await
                .map_err(ApiError::internal)?;
            archives.push(WalChecksumSealArchiveReport { shard, report });
        }
    }

    let mut records = 0usize;
    let mut sealed = 0usize;
    let mut already_sealed = 0usize;
    let mut rewritten_files = 0usize;
    for report in &active {
        records += report.records;
        sealed += report.sealed;
        already_sealed += report.already_sealed;
        rewritten_files += usize::from(report.rewritten);
        for replica in &report.replicas {
            records += replica.records;
            sealed += replica.sealed;
            already_sealed += replica.already_sealed;
            rewritten_files += usize::from(replica.rewritten);
        }
    }
    for archive in &archives {
        records += archive.report.records;
        sealed += archive.report.sealed;
        already_sealed += archive.report.already_sealed;
        rewritten_files += usize::from(archive.report.rewritten);
    }

    Ok(Json(WalChecksumSealResponse {
        active,
        archives,
        records,
        sealed,
        already_sealed,
        rewritten_files,
    }))
}

pub(crate) async fn retain_wal_archives(
    State(state): State<AppState>,
    Query(query): Query<WalArchiveRetentionQuery>,
) -> Result<Json<WalArchiveRetentionResponse>, ApiError> {
    Ok(Json(run_wal_archive_retention(&state, query).await?))
}

async fn run_wal_archive_retention(
    state: &AppState,
    query: WalArchiveRetentionQuery,
) -> Result<WalArchiveRetentionResponse, ApiError> {
    if query.before_lsn.is_none() && query.before_timestamp_ms.is_none() {
        return Err(ApiError::bad_request(
            "provide beforeLsn or beforeTimestampMs for WAL archive retention",
        ));
    }

    let dry_run = query.dry_run.unwrap_or(true);
    let mut reports = Vec::new();
    let mut candidates = 0usize;
    let mut deleted = 0usize;
    let mut retained = 0usize;

    for (shard, wal_path) in state.wal_paths.iter().enumerate() {
        for archive_path in wal_archive_paths(wal_path).map_err(ApiError::internal)? {
            let records = match crate::wal::read_records_file(&archive_path) {
                Ok(records) => records,
                Err(err) => {
                    retained += 1;
                    reports.push(WalArchiveRetentionFileReport {
                        path: archive_path.display().to_string(),
                        shard,
                        records: 0,
                        min_lsn: None,
                        max_lsn: None,
                        min_timestamp_ms: None,
                        max_timestamp_ms: None,
                        action: "retain".to_string(),
                        reason: Some(format!("failed to read archive file: {err:#}")),
                    });
                    continue;
                }
            };
            let records_len = records.len();
            let min_lsn = records.iter().map(|record| record.lsn).min();
            let max_lsn = records.iter().map(|record| record.lsn).max();
            let min_timestamp_ms = records.iter().map(|record| record.timestamp_ms).min();
            let max_timestamp_ms = records.iter().map(|record| record.timestamp_ms).max();

            let mut retain_reason = None;
            if records.is_empty() {
                retain_reason = Some("archive file has no records".to_string());
            } else if let Some(before_lsn) = query.before_lsn
                && max_lsn.is_some_and(|value| value >= before_lsn)
            {
                retain_reason = Some(format!("max LSN is not before {before_lsn}"));
            }
            if retain_reason.is_none()
                && let Some(before_timestamp_ms) = query.before_timestamp_ms
                && max_timestamp_ms.is_some_and(|value| value >= before_timestamp_ms)
            {
                retain_reason = Some(format!("max timestamp is not before {before_timestamp_ms}"));
            }

            let (action, reason) = if let Some(reason) = retain_reason {
                retained += 1;
                ("retain".to_string(), Some(reason))
            } else {
                candidates += 1;
                if dry_run {
                    ("delete".to_string(), None)
                } else {
                    fs::remove_file(&archive_path)
                        .await
                        .with_context(|| format!("delete WAL archive {}", archive_path.display()))
                        .map_err(ApiError::internal)?;
                    deleted += 1;
                    ("deleted".to_string(), None)
                }
            };

            reports.push(WalArchiveRetentionFileReport {
                path: archive_path.display().to_string(),
                shard,
                records: records_len,
                min_lsn,
                max_lsn,
                min_timestamp_ms,
                max_timestamp_ms,
                action,
                reason,
            });
        }
    }

    Ok(WalArchiveRetentionResponse {
        dry_run,
        before_lsn: query.before_lsn,
        before_timestamp_ms: query.before_timestamp_ms,
        candidates,
        deleted,
        retained,
        reports,
    })
}

fn wal_archive_paths(wal_path: &Path) -> Result<Vec<PathBuf>> {
    let archive_dir = wal_archive_dir_for_path(wal_path);
    if !archive_dir.exists() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in std::fs::read_dir(&archive_dir)
        .with_context(|| format!("read WAL archive dir {}", archive_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn wal_archive_dir_for_path(path: &Path) -> PathBuf {
    path.parent()
        .map(|parent| parent.join("archive"))
        .unwrap_or_else(|| PathBuf::from("archive"))
}

pub(crate) async fn refresh_wal_remote_replicas_for_shard(
    state: &AppState,
    shard: usize,
) -> Result<(), ApiError> {
    let Some(wal_shard) = state.wal_shards.get(shard) else {
        return Err(ApiError::bad_request("shard is out of range"));
    };
    let remote_replica_urls = remote_replica_urls_for_shard(state, shard).await;
    wal_shard
        .writer
        .configure_remote_replicas(
            remote_replica_urls
                .into_iter()
                .map(|url| WalRemoteReplica {
                    url,
                    token: state.wal_replication_token.clone(),
                })
                .collect(),
            wal_shard.remote_ack_policy,
        )
        .await
        .map_err(ApiError::internal)
}

async fn remote_replica_urls_for_shard(state: &AppState, shard: usize) -> Vec<String> {
    if !crate::cluster::cluster_enabled() {
        return Vec::new();
    }

    if let Some(urls) = &state.explicit_wal_remote_replica_urls {
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

pub(crate) async fn run_wal_repair_controller_once(state: &AppState) -> Result<(), ApiError> {
    let mut last_shards = Vec::new();
    let mut last_records_sent = 0_usize;
    let mut last_repaired_replicas = 0_usize;
    let mut last_satisfied = true;
    let mut last_error = None;

    for shard in 0..state.wal_shards.len() {
        if cluster_role_for_shard(state, shard).await != ShardRole::Owner {
            continue;
        }
        let report = state
            .wal_shards
            .get(shard)
            .ok_or_else(|| ApiError::bad_request("shard is out of range"))?
            .writer
            .repair_remote_replicas(None, true)
            .await
            .map_err(ApiError::internal)?;
        last_shards.push(shard);
        last_records_sent += report.records_sent;
        last_repaired_replicas += report.repaired_replicas;
        if !report.satisfied {
            last_satisfied = false;
            if last_error.is_none() {
                last_error = Some(format!(
                    "shard {shard} repair satisfied {}/{} remote ack(s)",
                    report.repaired_replicas, report.remote_required_acks
                ));
            }
        }
    }

    let mut controller = state.wal_repair_controller.write().await;
    controller.last_run_at_ms = Some(now_ms());
    controller.last_shards = last_shards;
    controller.last_records_sent = last_records_sent;
    controller.last_repaired_replicas = last_repaired_replicas;
    controller.last_satisfied = last_satisfied;
    controller.last_error = last_error;
    Ok(())
}
