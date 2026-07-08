use std::collections::{BTreeMap, HashSet};

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, Uri},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::wal::{
    append_ordered_wal_record, append_ordered_wal_records, ensure_shard_not_frozen,
    maybe_checkpoint, writable_wal_shard_for_key,
};
use crate::{
    AppState,
    api::{
        auth::{ensure_global_client_token_authorized, ensure_user_token_authorized},
        error::ApiError,
        events::{publish_delivery_event, publish_delivery_events},
        guards::{ensure_bytes_limit, ensure_json_value_limit},
        objects::{DeleteObjectResponse, validate_event_payload_object_refs},
        realtime::publish_volatile_user_event,
        records::{
            DeleteRecordResponse, RecordTransactionOperationResponse, RecordTransactionResponse,
        },
        runtime::{begin_runtime_write, ensure_runtime_accepting_writes},
    },
    config::MAX_BATCH_MESSAGES,
    model::{
        ClientMutationRecord, DbRecord, DbRecordMutationDraft, DeliveryEvent, Durability, Message,
        MessageDraft, ObjectMetadata, UserEvent, UserEventDraft, UserProfile, WalPayload,
        WalRecord,
    },
    object_store::ensure_safe_object_id,
    util::{normalize_limit, now_ms},
};

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum MutateRequest {
    SendMessage {
        room_id: String,
        user_id: String,
        body: String,
        client_mutation_id: Option<String>,
        #[serde(default)]
        attachments: Vec<String>,
        #[serde(default)]
        durability: Durability,
    },
    SendMessages {
        room_id: String,
        user_id: String,
        messages: Vec<SendMessagesItem>,
        #[serde(default)]
        durability: Durability,
    },
    PublishVolatile {
        room_id: String,
        name: String,
        payload: serde_json::Value,
    },
    PublishUserVolatile {
        user_id: String,
        name: String,
        payload: serde_json::Value,
    },
    PublishUserEvent {
        user_id: String,
        name: String,
        payload: serde_json::Value,
        client_mutation_id: Option<String>,
        #[serde(default)]
        durability: Durability,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendMessagesItem {
    pub(crate) body: String,
    pub(crate) client_mutation_id: Option<String>,
    #[serde(default)]
    pub(crate) attachments: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum MutateResponse {
    MessageCreated { message: Message },
    MessagesCreated { messages: Vec<Message> },
    UserEventPublished { event: UserEvent },
    VolatilePublished { delivered: usize },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessagesQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) before_lsn: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessagesResponse {
    pub(crate) room_id: String,
    pub(crate) source: &'static str,
    pub(crate) messages: Vec<Message>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserEventsQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) before_lsn: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserEventsResponse {
    pub(crate) user_id: String,
    pub(crate) events: Vec<UserEvent>,
}

#[derive(Clone)]
pub(crate) enum CommittedMutation {
    MessageCreated { message: Message },
    UserEventPublished { event: UserEvent },
    UserUpserted { user: UserProfile },
    ObjectCommitted { object: ObjectMetadata },
    ObjectDeleted { response: DeleteObjectResponse },
    RecordUpserted { record: DbRecord },
    RecordDeleted { response: DeleteRecordResponse },
    RecordTransactionCommitted { response: RecordTransactionResponse },
}

pub(crate) enum PreparedBatchMessage {
    Existing(Message),
    VolatileFast(Message),
    Draft(crate::model::MessageDraft),
}

pub(crate) fn normalize_client_mutation_id(
    value: Option<String>,
) -> Result<Option<String>, ApiError> {
    let Some(value) = value.map(|value| value.trim().to_string()) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > 160
        || !value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | ':' | '.'))
    {
        return Err(ApiError::bad_request("invalid clientMutationId"));
    }
    Ok(Some(value))
}

pub(crate) fn behavior_command_client_mutation_id(
    behavior_client_mutation_id: Option<&str>,
    command_index: usize,
    command_kind: &str,
) -> Option<String> {
    behavior_client_mutation_id.map(|id| format!("{id}:{command_index:03}:{command_kind}"))
}

pub(crate) fn find_committed_mutation(
    state: &AppState,
    client_mutation_id: Option<&str>,
) -> Result<Option<CommittedMutation>, ApiError> {
    let Some(client_mutation_id) = client_mutation_id else {
        return Ok(None);
    };
    Ok(state
        .client_mutations
        .read()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("client mutation index poisoned")))?
        .get(client_mutation_id)
        .cloned())
}

pub(crate) fn find_committed_mutations<'a, I>(
    state: &AppState,
    client_mutation_ids: I,
) -> Result<BTreeMap<String, CommittedMutation>, ApiError>
where
    I: IntoIterator<Item = &'a str>,
{
    let client_mutation_ids = client_mutation_ids.into_iter().collect::<Vec<_>>();
    if client_mutation_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let client_mutations = state
        .client_mutations
        .read()
        .map_err(|_| ApiError::internal(anyhow::anyhow!("client mutation index poisoned")))?;
    Ok(client_mutation_ids
        .into_iter()
        .filter_map(|client_mutation_id| {
            client_mutations
                .get(client_mutation_id)
                .cloned()
                .map(|mutation| (client_mutation_id.to_string(), mutation))
        })
        .collect())
}

pub(crate) fn committed_mutation_entry_from_wal_record(
    wal_record: &WalRecord,
) -> Option<(String, CommittedMutation)> {
    match &wal_record.payload {
        WalPayload::MessageCreated { message } => {
            let client_mutation_id = message.client_mutation_id.clone()?;
            Some((
                client_mutation_id,
                CommittedMutation::MessageCreated {
                    message: message.clone().into_message(wal_record.lsn),
                },
            ))
        }
        WalPayload::UserEventPublished { event } => {
            let client_mutation_id = event.client_mutation_id.clone()?;
            Some((
                client_mutation_id,
                CommittedMutation::UserEventPublished {
                    event: event.clone().into_event(wal_record.lsn),
                },
            ))
        }
        WalPayload::UserUpserted { user } => {
            let client_mutation_id = user.client_mutation_id.clone()?;
            Some((
                client_mutation_id,
                CommittedMutation::UserUpserted {
                    user: user.clone().into_profile(wal_record.lsn),
                },
            ))
        }
        WalPayload::ObjectCommitted {
            object,
            client_mutation_id,
        } => Some((
            client_mutation_id.clone()?,
            CommittedMutation::ObjectCommitted {
                object: object.clone(),
            },
        )),
        WalPayload::ObjectDeleted {
            object_id,
            deleted_at_ms,
            path,
            client_mutation_id,
            ..
        } => Some((
            client_mutation_id.clone()?,
            CommittedMutation::ObjectDeleted {
                response: DeleteObjectResponse {
                    object_id: object_id.clone(),
                    deleted: true,
                    lsn: wal_record.lsn,
                    deleted_at_ms: Some(*deleted_at_ms),
                    path: path.clone(),
                },
            },
        )),
        WalPayload::RecordUpserted { record } => {
            let client_mutation_id = record.client_mutation_id.clone()?;
            Some((
                client_mutation_id,
                CommittedMutation::RecordUpserted {
                    record: record.clone().into_record(wal_record.lsn),
                },
            ))
        }
        WalPayload::RecordDeleted { record } => {
            let client_mutation_id = record.client_mutation_id.clone()?;
            Some((
                client_mutation_id,
                CommittedMutation::RecordDeleted {
                    response: DeleteRecordResponse {
                        table: record.table.clone(),
                        key: record.key.clone(),
                        deleted: true,
                        lsn: wal_record.lsn,
                        deleted_at_ms: Some(record.deleted_at_ms),
                        path: record.path.clone(),
                    },
                },
            ))
        }
        WalPayload::RecordTransactionCommitted {
            operations,
            client_mutation_id,
        } => Some((
            client_mutation_id.clone()?,
            CommittedMutation::RecordTransactionCommitted {
                response: RecordTransactionResponse {
                    lsn: wal_record.lsn,
                    operations: record_transaction_response_operations(
                        operations.clone(),
                        wal_record.lsn,
                    ),
                },
            },
        )),
        WalPayload::ClientMutationRecorded {
            client_mutation_id,
            record,
        } => Some((
            client_mutation_id.clone(),
            match record {
                ClientMutationRecord::RecordDeleteNoop { table, key, path } => {
                    CommittedMutation::RecordDeleted {
                        response: DeleteRecordResponse {
                            table: table.clone(),
                            key: key.clone(),
                            deleted: false,
                            lsn: wal_record.lsn,
                            deleted_at_ms: None,
                            path: path.clone(),
                        },
                    }
                }
                ClientMutationRecord::RecordTransactionNoop => {
                    CommittedMutation::RecordTransactionCommitted {
                        response: RecordTransactionResponse {
                            lsn: wal_record.lsn,
                            operations: Vec::new(),
                        },
                    }
                }
                ClientMutationRecord::ObjectDeleteNoop { object_id, path } => {
                    CommittedMutation::ObjectDeleted {
                        response: DeleteObjectResponse {
                            object_id: object_id.clone(),
                            deleted: false,
                            lsn: wal_record.lsn,
                            deleted_at_ms: None,
                            path: path.clone(),
                        },
                    }
                }
            },
        )),
        WalPayload::SchemaApplied { .. }
        | WalPayload::BehaviorPublished { .. }
        | WalPayload::ActorReminderScheduled { .. }
        | WalPayload::ActorReminderCancelled { .. }
        | WalPayload::ActorReminderFired { .. }
        | WalPayload::HostHttpRequested { .. }
        | WalPayload::HostHttpCompleted { .. } => None,
    }
}

pub(crate) fn messages_from_wal_records(records: &[WalRecord]) -> Vec<Message> {
    records
        .iter()
        .filter_map(|record| match &record.payload {
            WalPayload::MessageCreated { message } => {
                Some(message.clone().into_message(record.lsn))
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn client_mutation_index_from_wal_records(
    records: &[WalRecord],
) -> BTreeMap<String, CommittedMutation> {
    records
        .iter()
        .filter_map(committed_mutation_entry_from_wal_record)
        .collect()
}

pub(crate) async fn mutate(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<MutateRequest>,
) -> Result<Json<MutateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    match request {
        MutateRequest::SendMessage {
            room_id,
            user_id,
            body,
            client_mutation_id,
            attachments,
            durability,
        } => {
            ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
            send_message(
                state,
                room_id,
                user_id,
                body,
                attachments,
                durability,
                client_mutation_id,
            )
            .await
        }
        MutateRequest::SendMessages {
            room_id,
            user_id,
            messages,
            durability,
        } => {
            ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
            send_messages(state, room_id, user_id, messages, durability).await
        }
        MutateRequest::PublishVolatile {
            room_id,
            name,
            payload,
        } => {
            ensure_global_client_token_authorized(&state, &headers, &uri)?;
            if room_id.trim().is_empty() {
                return Err(ApiError::bad_request("roomId is required"));
            }
            ensure_json_value_limit(
                "volatile room event payload",
                &payload,
                state.limits.max_user_event_bytes,
            )?;
            state
                .schema
                .validate_event_payload(&name, &payload)
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
            validate_event_payload_object_refs(&state, &name, &payload).await?;
            let delivered = state.connections.count_room_subscribers(&room_id).await;
            publish_delivery_event(
                &state,
                DeliveryEvent::VolatileRoomEvent {
                    room_id,
                    name,
                    payload,
                },
            );
            Ok(Json(MutateResponse::VolatilePublished { delivered }))
        }
        MutateRequest::PublishUserVolatile {
            user_id,
            name,
            payload,
        } => {
            ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
            let delivered = publish_volatile_user_event(&state, &user_id, name, payload).await?;
            Ok(Json(MutateResponse::VolatilePublished { delivered }))
        }
        MutateRequest::PublishUserEvent {
            user_id,
            name,
            payload,
            client_mutation_id,
            durability,
        } => {
            ensure_user_token_authorized(&state, &headers, &uri, &user_id)?;
            publish_user_event(
                state,
                user_id,
                name,
                payload,
                durability,
                client_mutation_id,
            )
            .await
        }
    }
}

pub(crate) async fn publish_user_event(
    state: AppState,
    user_id: String,
    name: String,
    payload: serde_json::Value,
    durability: Durability,
    client_mutation_id: Option<String>,
) -> Result<Json<MutateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    if user_id.trim().is_empty() {
        return Err(ApiError::bad_request("userId is required"));
    }
    if name.trim().is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    if durability == Durability::Volatile {
        return Err(ApiError::bad_request(
            "publishUserEvent cannot be volatile; use publishUserVolatile for lossy events",
        ));
    }
    ensure_json_value_limit(
        "user event payload",
        &payload,
        state.limits.max_user_event_bytes,
    )?;
    state
        .schema
        .validate_event_payload(&name, &payload)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    validate_event_payload_object_refs(&state, &name, &payload).await?;
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if let Some(existing) = find_committed_mutation(&state, client_mutation_id.as_deref())? {
        return match existing {
            CommittedMutation::UserEventPublished { event } => {
                Ok(Json(MutateResponse::UserEventPublished { event }))
            }
            _ => Err(ApiError::conflict(
                "clientMutationId was already used for a different mutation kind",
            )),
        };
    }

    let _write = begin_runtime_write(&state).await?;
    let id = Uuid::now_v7().to_string();
    let path = format!("users/{user_id}/events/{id}");
    let draft = UserEventDraft {
        id,
        client_mutation_id,
        user_id: user_id.clone(),
        name,
        payload,
        created_at_ms: now_ms(),
        path,
    };
    let shard = writable_wal_shard_for_key(&state, &user_id).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let record = append_ordered_wal_record(
        &state,
        shard,
        durability,
        state.schema.version(),
        WalPayload::UserEventPublished {
            event: draft.clone(),
        },
    )
    .await?;

    state.users.apply_wal_record(&record)?;
    let event = draft.into_event(record.lsn);
    publish_delivery_event(
        &state,
        DeliveryEvent::UserEvent {
            user_id,
            event: event.clone(),
        },
    );
    maybe_checkpoint(&state).await?;

    Ok(Json(MutateResponse::UserEventPublished { event }))
}

pub(crate) async fn send_message(
    state: AppState,
    room_id: String,
    user_id: String,
    body: String,
    attachments: Vec<String>,
    durability: Durability,
    client_mutation_id: Option<String>,
) -> Result<Json<MutateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    if body.trim().is_empty() {
        return Err(ApiError::bad_request("message body cannot be empty"));
    }
    let client_mutation_id = normalize_client_mutation_id(client_mutation_id)?;
    if durability != Durability::Volatile
        && let Some(existing) = find_committed_mutation(&state, client_mutation_id.as_deref())?
    {
        match existing {
            CommittedMutation::MessageCreated { message } => {
                return Ok(Json(MutateResponse::MessageCreated { message }));
            }
            _ => {
                return Err(ApiError::conflict(
                    "clientMutationId was already used for a different mutation kind",
                ));
            }
        }
    }
    ensure_bytes_limit(
        "message body",
        body.len() as u64,
        state.limits.max_message_bytes,
    )?;

    let id = Uuid::now_v7().to_string();
    let created_at_ms = now_ms();
    let path = if durability == Durability::Volatile {
        format!("volatile/rooms/{room_id}/messages/{id}")
    } else {
        format!("rooms/{room_id}/messages/{id}")
    };

    if durability == Durability::Volatile && attachments.is_empty() {
        let message = Message {
            id,
            room_id: room_id.clone(),
            sender_id: user_id,
            body,
            attachments: Vec::new(),
            created_at_ms,
            lsn: 0,
            path,
        };
        state.actors.apply_message(message.clone()).await;
        let event = DeliveryEvent::MessageCreated {
            room_id,
            message: message.clone(),
        };
        publish_delivery_event(&state, event);
        return Ok(Json(MutateResponse::MessageCreated { message }));
    }

    let attachment_refs = resolve_object_refs(&state, attachments).await?;
    let draft = MessageDraft {
        id,
        client_mutation_id,
        room_id: room_id.clone(),
        sender_id: user_id,
        body,
        attachments: attachment_refs,
        created_at_ms,
        path,
    };
    state
        .schema
        .validate_message_draft(&draft)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    if durability == Durability::Volatile {
        let message = draft.into_message(0);
        state.actors.apply_message(message.clone()).await;
        let event = DeliveryEvent::MessageCreated {
            room_id,
            message: message.clone(),
        };
        publish_delivery_event(&state, event);
        return Ok(Json(MutateResponse::MessageCreated { message }));
    }

    let _write = begin_runtime_write(&state).await?;
    let shard = writable_wal_shard_for_key(&state, &room_id).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;
    let record = append_ordered_wal_record(
        &state,
        shard,
        durability,
        state.schema.version(),
        WalPayload::MessageCreated { message: draft },
    )
    .await?;

    let message = message_from_wal_record(record)?;
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

    let event = DeliveryEvent::MessageCreated {
        room_id,
        message: message.clone(),
    };
    publish_delivery_event(&state, event);
    maybe_checkpoint(&state).await?;

    Ok(Json(MutateResponse::MessageCreated { message }))
}

async fn send_messages(
    state: AppState,
    room_id: String,
    user_id: String,
    items: Vec<SendMessagesItem>,
    durability: Durability,
) -> Result<Json<MutateResponse>, ApiError> {
    ensure_runtime_accepting_writes(&state).await?;
    if items.is_empty() {
        return Err(ApiError::bad_request("messages cannot be empty"));
    }
    if items.len() > MAX_BATCH_MESSAGES {
        return Err(ApiError::bad_request(format!(
            "messages exceeds batch limit: {} > {MAX_BATCH_MESSAGES}",
            items.len()
        )));
    }

    let mut seen_client_mutation_ids = HashSet::new();
    let mut normalized_items = Vec::with_capacity(items.len());
    let mut client_mutation_ids = Vec::new();
    for mut item in items {
        if item.body.trim().is_empty() {
            return Err(ApiError::bad_request("message body cannot be empty"));
        }
        ensure_bytes_limit(
            "message body",
            item.body.len() as u64,
            state.limits.max_message_bytes,
        )?;
        let client_mutation_id = normalize_client_mutation_id(item.client_mutation_id.take())?;
        if let Some(client_mutation_id) = &client_mutation_id {
            if !seen_client_mutation_ids.insert(client_mutation_id.clone()) {
                return Err(ApiError::bad_request(
                    "duplicate clientMutationId in sendMessages batch",
                ));
            }
            client_mutation_ids.push(client_mutation_id.clone());
        }
        normalized_items.push((item, client_mutation_id));
    }

    let committed_mutations = if durability == Durability::Volatile {
        BTreeMap::new()
    } else {
        find_committed_mutations(&state, client_mutation_ids.iter().map(String::as_str))?
    };

    let mut prepared = Vec::with_capacity(normalized_items.len());
    for (item, client_mutation_id) in normalized_items {
        if let Some(client_mutation_id) = &client_mutation_id
            && let Some(existing) = committed_mutations.get(client_mutation_id).cloned()
        {
            match existing {
                CommittedMutation::MessageCreated { message } => {
                    prepared.push(PreparedBatchMessage::Existing(message));
                    continue;
                }
                _ => {
                    return Err(ApiError::conflict(
                        "clientMutationId was already used for a different mutation kind",
                    ));
                }
            }
        }

        let id = Uuid::now_v7().to_string();
        let created_at_ms = now_ms();
        let path = if durability == Durability::Volatile {
            format!("volatile/rooms/{room_id}/messages/{id}")
        } else {
            format!("rooms/{room_id}/messages/{id}")
        };
        if durability == Durability::Volatile && item.attachments.is_empty() {
            prepared.push(PreparedBatchMessage::VolatileFast(Message {
                id,
                room_id: room_id.clone(),
                sender_id: user_id.clone(),
                body: item.body,
                attachments: Vec::new(),
                created_at_ms,
                lsn: 0,
                path,
            }));
            continue;
        }

        let attachment_refs = resolve_object_refs(&state, item.attachments).await?;
        let draft = MessageDraft {
            id,
            client_mutation_id,
            room_id: room_id.clone(),
            sender_id: user_id.clone(),
            body: item.body,
            attachments: attachment_refs,
            created_at_ms,
            path,
        };
        state
            .schema
            .validate_message_draft(&draft)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        prepared.push(PreparedBatchMessage::Draft(draft));
    }

    if durability == Durability::Volatile {
        let mut messages = Vec::with_capacity(prepared.len());
        for prepared_message in prepared {
            let message = match prepared_message {
                PreparedBatchMessage::Existing(message)
                | PreparedBatchMessage::VolatileFast(message) => message,
                PreparedBatchMessage::Draft(draft) => draft.into_message(0),
            };
            messages.push(message);
        }
        state.actors.apply_messages(messages.clone()).await;
        publish_message_created_events(&state, &room_id, &messages);
        return Ok(Json(MutateResponse::MessagesCreated { messages }));
    }

    let mut output = vec![None; prepared.len()];
    let mut wal_inputs = Vec::new();
    for (index, prepared_message) in prepared.into_iter().enumerate() {
        match prepared_message {
            PreparedBatchMessage::Existing(message) => output[index] = Some(message),
            PreparedBatchMessage::VolatileFast(message) => output[index] = Some(message),
            PreparedBatchMessage::Draft(draft) => wal_inputs.push((index, draft)),
        }
    }

    if wal_inputs.is_empty() {
        let messages = output
            .into_iter()
            .map(|message| {
                message.ok_or_else(|| ApiError::internal(anyhow::anyhow!("missing batch message")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Json(MutateResponse::MessagesCreated { messages }));
    }

    let _write = begin_runtime_write(&state).await?;
    let shard = writable_wal_shard_for_key(&state, &room_id).await?;
    ensure_shard_not_frozen(&state, shard.index).await?;

    let mut wal_indices = Vec::with_capacity(wal_inputs.len());
    let mut payloads = Vec::with_capacity(wal_inputs.len());
    for (index, draft) in wal_inputs {
        wal_indices.push(index);
        payloads.push(WalPayload::MessageCreated { message: draft });
    }
    let records =
        append_ordered_wal_records(&state, shard, durability, state.schema.version(), payloads)
            .await?;

    let mut committed_messages = Vec::with_capacity(records.len());
    for (index, record) in wal_indices.into_iter().zip(records) {
        let message = message_from_wal_record(record)?;
        output[index] = Some(message.clone());
        committed_messages.push(message);
    }

    let messages = output
        .into_iter()
        .map(|message| {
            message.ok_or_else(|| ApiError::internal(anyhow::anyhow!("missing batch message")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    state
        .chat_log
        .append_many(&committed_messages)
        .await
        .map_err(ApiError::internal)?;
    state
        .actors
        .apply_messages(committed_messages.clone())
        .await;
    state
        .object_refs
        .retain_messages(committed_messages.iter())
        .await
        .map_err(ApiError::internal)?;
    publish_message_created_events(&state, &room_id, &committed_messages);
    maybe_checkpoint(&state).await?;

    Ok(Json(MutateResponse::MessagesCreated { messages }))
}

fn publish_message_created_events(state: &AppState, room_id: &str, messages: &[Message]) {
    if messages.is_empty() {
        return;
    }
    publish_delivery_events(
        state,
        messages
            .iter()
            .map(|message| DeliveryEvent::MessageCreated {
                room_id: room_id.to_string(),
                message: message.clone(),
            })
            .collect(),
    );
}

fn message_from_wal_record(record: WalRecord) -> Result<Message, ApiError> {
    let lsn = record.lsn;
    match record.payload {
        WalPayload::MessageCreated { message } => Ok(message.into_message(lsn)),
        _ => Err(ApiError::internal(anyhow::anyhow!(
            "expected message WAL record"
        ))),
    }
}

async fn resolve_object_refs(
    state: &AppState,
    attachments: Vec<String>,
) -> Result<Vec<crate::model::ObjectRef>, ApiError> {
    let mut refs = Vec::with_capacity(attachments.len());
    let mut seen = HashSet::new();
    for object_id in attachments {
        if !ensure_safe_object_id(&object_id) {
            return Err(ApiError::bad_request("invalid attachment object id"));
        }
        if !seen.insert(object_id.clone()) {
            continue;
        }
        if !state.objects.metadata_exists(&object_id) {
            return Err(ApiError::not_found("attachment object not found"));
        }
        let metadata = state
            .objects
            .metadata(&object_id)
            .await
            .map_err(ApiError::internal)?;
        refs.push(metadata.into());
    }
    Ok(refs)
}

pub(crate) async fn latest_messages(
    State(state): State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<MessagesResponse>, ApiError> {
    let limit = normalize_limit(query.limit);
    let hot = state
        .actors
        .latest_messages(&room_id, query.before_lsn, limit)
        .await;

    if hot.len() >= limit {
        return Ok(Json(MessagesResponse {
            room_id,
            source: "live",
            messages: hot,
        }));
    }

    let messages = state
        .chat_log
        .latest(&room_id, query.before_lsn, limit)
        .await
        .map_err(ApiError::internal)?;
    if !hot.is_empty() {
        let mut seen = HashSet::new();
        let mut merged = Vec::with_capacity(limit);
        for message in hot.into_iter().chain(messages) {
            if seen.insert(message.id.clone()) {
                merged.push(message);
                if merged.len() >= limit {
                    break;
                }
            }
        }
        state.actors.apply_messages(merged.clone()).await;
        return Ok(Json(MessagesResponse {
            room_id,
            source: "live",
            messages: merged,
        }));
    }

    state.actors.apply_messages(messages.clone()).await;
    Ok(Json(MessagesResponse {
        room_id,
        source: "chatLog",
        messages,
    }))
}

fn record_transaction_response_operations(
    operations: Vec<DbRecordMutationDraft>,
    lsn: u64,
) -> Vec<RecordTransactionOperationResponse> {
    operations
        .into_iter()
        .map(|operation| match operation {
            DbRecordMutationDraft::Upsert { record } => {
                RecordTransactionOperationResponse::RecordUpserted {
                    record: record.into_record(lsn),
                }
            }
            DbRecordMutationDraft::Delete { record } => {
                RecordTransactionOperationResponse::RecordDeleted {
                    table: record.table,
                    key: record.key,
                    deleted_at_ms: record.deleted_at_ms,
                    lsn,
                    path: record.path,
                }
            }
        })
        .collect()
}
