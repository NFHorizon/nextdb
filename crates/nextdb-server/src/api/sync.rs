use std::{
    collections::{BTreeSet, HashSet},
    sync::atomic::Ordering,
};

use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    api::{
        error::ApiError,
        frames::{NestedTableSubscription, TableSubscription},
        records::validate_nested_table_path,
    },
    live_query::{record_matches_table_filters, record_matches_table_subscription},
    model::{DbRecordMutationDraft, DeliveryEvent, WalPayload, WalRecord},
    schema::DatabaseSchema,
    util::{normalize_limit, shard_index},
    wal::{self, WalRemoteAckPolicy, read_records_from_wal_paths_after_lsn},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncPullQuery {
    pub(crate) after_lsn: Option<u64>,
    pub(crate) rooms: Option<String>,
    pub(crate) users: Option<String>,
    pub(crate) tables: Option<String>,
    pub(crate) nested_tables: Option<String>,
    pub(crate) objects: Option<bool>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncPullResponse {
    pub(crate) events: Vec<DeliveryEvent>,
    pub(crate) next_after_lsn: u64,
    pub(crate) current_lsn: u64,
    pub(crate) has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncWaitQuery {
    pub(crate) min_lsn: u64,
    pub(crate) timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) consistency: ReadConsistency,
    pub(crate) shard_key: Option<String>,
    pub(crate) shard: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncWaitResponse {
    pub(crate) min_lsn: u64,
    pub(crate) current_lsn: u64,
    pub(crate) caught_up: bool,
    pub(crate) waited_ms: u64,
    pub(crate) consistency: ReadConsistency,
    pub(crate) shard: Option<usize>,
    pub(crate) remote_required_acks: usize,
    pub(crate) remote_acked: usize,
    pub(crate) remote_caught_up: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ReadConsistency {
    #[default]
    Local,
    Quorum,
    All,
}

pub(crate) struct SyncEventPage {
    pub(crate) events: Vec<DeliveryEvent>,
    pub(crate) next_after_lsn: u64,
    pub(crate) has_more: bool,
}

pub(crate) struct RemoteReadAckProgress {
    pub(crate) required_acks: usize,
    pub(crate) acked: usize,
    pub(crate) caught_up: bool,
}

pub(crate) async fn sync_pull(
    State(state): State<AppState>,
    Query(query): Query<SyncPullQuery>,
) -> Result<Json<SyncPullResponse>, ApiError> {
    let after_lsn = query.after_lsn.unwrap_or(0);
    let limit = normalize_limit(query.limit);
    let room_filter = parse_room_filter(query.rooms);
    let user_filter = parse_name_filter(query.users);
    let table_filter = parse_name_filter(query.tables);
    let nested_table_filter = parse_nested_table_filter(query.nested_tables, &state)?;
    let include_objects = query.objects.unwrap_or(false);
    let records = read_records_from_wal_paths_after_lsn(&state.wal_paths, after_lsn)
        .map_err(ApiError::internal)?;
    let page = sync_events_from_wal_records(
        records,
        after_lsn,
        &room_filter,
        &user_filter,
        &table_filter,
        &BTreeSet::new(),
        &nested_table_filter,
        None,
        include_objects,
        limit,
    );

    Ok(Json(SyncPullResponse {
        events: page.events,
        next_after_lsn: page.next_after_lsn,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        has_more: page.has_more,
    }))
}

pub(crate) async fn sync_wait(
    State(state): State<AppState>,
    Query(query): Query<SyncWaitQuery>,
) -> Result<Json<SyncWaitResponse>, ApiError> {
    let timeout_ms = query.timeout_ms.unwrap_or(5_000).min(30_000);
    let target_shard = sync_wait_target_shard(&state, &query)?;
    let started = std::time::Instant::now();
    loop {
        let current_lsn = state.current_lsn.load(Ordering::Acquire);
        let remote_ack =
            remote_read_ack_progress(&state, query.min_lsn, query.consistency, target_shard)
                .await?;
        if current_lsn >= query.min_lsn && remote_ack.caught_up {
            return Ok(Json(SyncWaitResponse {
                min_lsn: query.min_lsn,
                current_lsn,
                caught_up: true,
                waited_ms: started.elapsed().as_millis() as u64,
                consistency: query.consistency,
                shard: target_shard,
                remote_required_acks: remote_ack.required_acks,
                remote_acked: remote_ack.acked,
                remote_caught_up: remote_ack.caught_up,
            }));
        }
        if started.elapsed() >= std::time::Duration::from_millis(timeout_ms) {
            return Ok(Json(SyncWaitResponse {
                min_lsn: query.min_lsn,
                current_lsn,
                caught_up: false,
                waited_ms: started.elapsed().as_millis() as u64,
                consistency: query.consistency,
                shard: target_shard,
                remote_required_acks: remote_ack.required_acks,
                remote_acked: remote_ack.acked,
                remote_caught_up: remote_ack.caught_up,
            }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn sync_wait_target_shard(
    state: &AppState,
    query: &SyncWaitQuery,
) -> Result<Option<usize>, ApiError> {
    if query.consistency == ReadConsistency::Local {
        return Ok(query.shard);
    }
    if let Some(shard) = query.shard {
        if shard >= state.wal_shards.len() {
            return Err(ApiError::bad_request("shard is out of range"));
        }
        return Ok(Some(shard));
    }
    if let Some(shard_key) = query
        .shard_key
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        return Ok(Some(shard_index(shard_key, state.cluster.shard_count())));
    }
    Err(ApiError::bad_request(
        "sync wait consistency quorum/all requires shard or shardKey",
    ))
}

async fn remote_read_ack_progress(
    state: &AppState,
    min_lsn: u64,
    consistency: ReadConsistency,
    shard: Option<usize>,
) -> Result<RemoteReadAckProgress, ApiError> {
    let Some(shard) = shard else {
        return Ok(RemoteReadAckProgress {
            required_acks: 0,
            acked: 0,
            caught_up: true,
        });
    };
    let Some(wal_shard) = state.wal_shards.get(shard) else {
        return Err(ApiError::bad_request("shard is out of range"));
    };
    let status = wal_shard.writer.status().await;
    let required_acks = match consistency {
        ReadConsistency::Local => 0,
        ReadConsistency::Quorum => {
            wal::required_remote_acks(WalRemoteAckPolicy::Quorum, status.remote_replica_count)
        }
        ReadConsistency::All => status.remote_replica_count,
    };
    let acked = status
        .remote_replicas
        .iter()
        .filter(|replica| replica.highest_acked_lsn >= min_lsn)
        .count();
    Ok(RemoteReadAckProgress {
        required_acks,
        acked,
        caught_up: acked >= required_acks,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_events_from_wal_records(
    records: Vec<WalRecord>,
    after_lsn: u64,
    room_filter: &HashSet<String>,
    user_filter: &HashSet<String>,
    table_filter: &HashSet<String>,
    table_range_filter: &BTreeSet<TableSubscription>,
    nested_table_filter: &BTreeSet<NestedTableSubscription>,
    schema: Option<&DatabaseSchema>,
    include_objects: bool,
    limit: usize,
) -> SyncEventPage {
    let mut events = Vec::new();
    let mut next_after_lsn = after_lsn;
    let mut has_more = false;
    let scoped = !room_filter.is_empty()
        || !user_filter.is_empty()
        || !table_filter.is_empty()
        || !table_range_filter.is_empty()
        || !nested_table_filter.is_empty()
        || include_objects;

    for record in records {
        if record.lsn <= after_lsn {
            continue;
        }

        match record.payload {
            WalPayload::MessageCreated { message } => {
                if scoped && !room_filter.contains(&message.room_id) {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                let room_id = message.room_id.clone();
                events.push(DeliveryEvent::MessageCreated {
                    room_id,
                    message: message.into_message(record.lsn),
                });
            }
            WalPayload::UserEventPublished { event: draft } => {
                if scoped && !user_filter.contains(&draft.user_id) {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                let user_id = draft.user_id.clone();
                events.push(DeliveryEvent::UserEvent {
                    user_id,
                    event: draft.into_event(record.lsn),
                });
            }
            WalPayload::UserUpserted { user: draft } => {
                if scoped && !user_filter.contains(&draft.user_id) {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                let user_id = draft.user_id.clone();
                events.push(DeliveryEvent::UserUpserted {
                    user_id,
                    user: draft.into_profile(record.lsn),
                });
            }
            WalPayload::RecordUpserted { record: draft } => {
                let db_record = draft.clone().into_record(record.lsn);
                if scoped
                    && !record_matches_table_filters_with_record(
                        &draft.table,
                        &draft.key,
                        &db_record,
                        table_filter,
                        table_range_filter,
                        nested_table_filter,
                        schema,
                    )
                {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                let table = draft.table.clone();
                let key = draft.key.clone();
                events.push(DeliveryEvent::RecordUpserted {
                    table,
                    key,
                    record: db_record,
                });
            }
            WalPayload::RecordDeleted { record: draft } => {
                if scoped
                    && !record_matches_table_filters(
                        &draft.table,
                        &draft.key,
                        table_filter,
                        table_range_filter,
                        nested_table_filter,
                    )
                {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                events.push(DeliveryEvent::RecordDeleted {
                    table: draft.table,
                    key: draft.key,
                    deleted_at_ms: draft.deleted_at_ms,
                    lsn: record.lsn,
                    path: draft.path,
                    previous_record: None,
                });
            }
            WalPayload::RecordTransactionCommitted { operations, .. } => {
                let mut transaction_events = Vec::new();
                for operation in operations {
                    match operation {
                        DbRecordMutationDraft::Upsert { record: draft } => {
                            let db_record = draft.clone().into_record(record.lsn);
                            if scoped
                                && !record_matches_table_filters_with_record(
                                    &draft.table,
                                    &draft.key,
                                    &db_record,
                                    table_filter,
                                    table_range_filter,
                                    nested_table_filter,
                                    schema,
                                )
                            {
                                continue;
                            }
                            let table = draft.table.clone();
                            let key = draft.key.clone();
                            transaction_events.push(DeliveryEvent::RecordUpserted {
                                table,
                                key,
                                record: db_record,
                            });
                        }
                        DbRecordMutationDraft::Delete { record: draft } => {
                            if scoped
                                && !record_matches_table_filters(
                                    &draft.table,
                                    &draft.key,
                                    table_filter,
                                    table_range_filter,
                                    nested_table_filter,
                                )
                            {
                                continue;
                            }
                            transaction_events.push(DeliveryEvent::RecordDeleted {
                                table: draft.table,
                                key: draft.key,
                                deleted_at_ms: draft.deleted_at_ms,
                                lsn: record.lsn,
                                path: draft.path,
                                previous_record: None,
                            });
                        }
                    }
                }
                if transaction_events.is_empty() {
                    continue;
                }
                if !events.is_empty() && events.len() + transaction_events.len() > limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                events.extend(transaction_events);
            }
            WalPayload::ObjectCommitted { object, .. } => {
                if scoped && !include_objects {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                events.push(DeliveryEvent::ObjectCommitted {
                    object,
                    lsn: record.lsn,
                });
            }
            WalPayload::ObjectDeleted {
                object_id,
                deleted_at_ms,
                path,
                force,
                ..
            } => {
                if scoped && !include_objects {
                    continue;
                }
                if events.len() >= limit {
                    has_more = true;
                    break;
                }
                next_after_lsn = record.lsn;
                events.push(DeliveryEvent::ObjectDeleted {
                    object_id,
                    deleted_at_ms,
                    lsn: record.lsn,
                    path,
                    force,
                });
            }
            WalPayload::SchemaApplied { .. }
            | WalPayload::BehaviorPublished { .. }
            | WalPayload::ActorReminderScheduled { .. }
            | WalPayload::ActorReminderCancelled { .. }
            | WalPayload::ActorReminderFired { .. }
            | WalPayload::HostHttpRequested { .. }
            | WalPayload::HostHttpCompleted { .. }
            | WalPayload::ClientMutationRecorded { .. } => {}
        }
    }

    SyncEventPage {
        events,
        next_after_lsn,
        has_more,
    }
}

fn record_matches_table_filters_with_record(
    table: &str,
    key: &str,
    record: &crate::model::DbRecord,
    table_filter: &HashSet<String>,
    table_range_filter: &BTreeSet<TableSubscription>,
    nested_table_filter: &BTreeSet<NestedTableSubscription>,
    schema: Option<&DatabaseSchema>,
) -> bool {
    table_filter.contains(table)
        || table_range_filter.iter().any(|subscription| {
            if subscription.has_index_prefix() {
                schema.is_some_and(|schema| {
                    record_matches_table_subscription(
                        table,
                        key,
                        Some(record),
                        schema,
                        subscription,
                    )
                })
            } else {
                subscription.matches(table, key)
            }
        })
        || nested_table_filter.iter().any(|subscription| {
            subscription.logical_table() == table && key.starts_with(&subscription.key_prefix())
        })
}

fn parse_room_filter(rooms: Option<String>) -> HashSet<String> {
    parse_name_filter(rooms)
}

fn parse_name_filter(names: Option<String>) -> HashSet<String> {
    names
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|room| !room.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_nested_table_filter(
    nested_tables: Option<String>,
    state: &AppState,
) -> Result<BTreeSet<NestedTableSubscription>, ApiError> {
    let mut subscriptions = BTreeSet::new();
    for value in nested_tables.unwrap_or_default().split(',') {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        let Some(first_colon) = value.find(':') else {
            return Err(ApiError::bad_request(
                "nestedTables entries must use table:parentKey:nested",
            ));
        };
        let Some(last_colon) = value.rfind(':') else {
            return Err(ApiError::bad_request(
                "nestedTables entries must use table:parentKey:nested",
            ));
        };
        if first_colon == last_colon {
            return Err(ApiError::bad_request(
                "nestedTables entries must use table:parentKey:nested",
            ));
        }
        let table = value[..first_colon].trim();
        let parent_key = value[first_colon + 1..last_colon].trim();
        let nested = value[last_colon + 1..].trim();
        if table.is_empty() || parent_key.is_empty() || nested.is_empty() {
            return Err(ApiError::bad_request(
                "nestedTables entries must use table:parentKey:nested",
            ));
        }
        validate_nested_table_path(table, parent_key, nested, state)?;
        subscriptions.insert(NestedTableSubscription::new(
            table.to_string(),
            parent_key.to_string(),
            nested.to_string(),
        ));
    }
    Ok(subscriptions)
}
