use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Number, Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvokeRequest<T> {
    pub behavior: String,
    pub mutation: String,
    pub user_id: Option<String>,
    pub client_mutation_id: Option<String>,
    pub input: T,
    #[serde(default)]
    pub read: BehaviorReadPlan,
    #[serde(default)]
    pub context: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorReadPlan {
    #[serde(default)]
    pub records: Vec<BehaviorRecordRead>,
    #[serde(default)]
    pub nested_records: Vec<BehaviorNestedRecordRead>,
    #[serde(default)]
    pub latest_messages: Vec<BehaviorLatestMessagesRead>,
    #[serde(default)]
    pub objects: Vec<BehaviorObjectRead>,
    #[serde(default)]
    pub object_bodies: Vec<BehaviorObjectRead>,
    #[serde(default)]
    pub realtime_channel_members: Vec<BehaviorRealtimeChannelMembersRead>,
    #[serde(default)]
    pub realtime_channel_states: Vec<BehaviorRealtimeChannelStateRead>,
    #[serde(default)]
    pub connection_sessions: Vec<BehaviorConnectionSessionsRead>,
    #[serde(default)]
    pub audit_traces: Vec<BehaviorAuditTraceRead>,
    #[serde(default)]
    pub audit_replays: Vec<BehaviorAuditReplayRead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRecordRead {
    pub table: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorNestedRecordRead {
    pub table: String,
    pub parent_key: String,
    pub nested: String,
    pub nested_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorLatestMessagesRead {
    pub room_id: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorObjectRead {
    pub object_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRealtimeChannelStateRead {
    pub channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRealtimeChannelMembersRead {
    pub channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorConnectionSessionsRead {
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub transport: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorAuditTraceRead {
    pub kind: BehaviorAuditTraceKind,
    pub id: Option<String>,
    pub table: Option<String>,
    pub record_key: Option<String>,
    pub parent_key: Option<String>,
    pub nested: Option<String>,
    pub nested_key: Option<String>,
    pub path: Option<String>,
    pub client_mutation_id: Option<String>,
    pub after_lsn: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorAuditTraceKind {
    Room,
    User,
    Object,
    Record,
    NestedRecord,
    Path,
    ClientMutation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorAuditReplayRead {
    pub kind: BehaviorAuditReplayKind,
    pub id: Option<String>,
    pub table: Option<String>,
    pub record_key: Option<String>,
    pub parent_key: Option<String>,
    pub nested: Option<String>,
    pub nested_key: Option<String>,
    pub at_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BehaviorAuditReplayKind {
    User,
    Object,
    Record,
    NestedRecord,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeContext {
    pub timestamp_ms: u64,
    pub sender: BehaviorRuntimeSender,
    pub rng_seed: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorRuntimeSender {
    pub kind: String,
    pub user_id: Option<String>,
    pub behavior: String,
    pub mutation: String,
    pub client_mutation_id: Option<String>,
}

pub fn runtime_context<T>(request: &BehaviorInvokeRequest<T>) -> Option<BehaviorRuntimeContext> {
    serde_json::from_value(request.context.get("requestContext")?.get("ctx")?.clone()).ok()
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ActorKind {
    Room,
    Scope,
    Table,
    View,
    Aggregate,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorContinuationPayload {
    #[serde(rename = "type")]
    pub payload_type: &'static str,
    pub behavior: String,
    pub mutation: String,
    pub user_id: Option<String>,
    pub client_mutation_id: Option<String>,
    pub input: Option<Value>,
    pub read: Option<BehaviorReadPlan>,
    pub context: Option<Value>,
    pub reply_to: Option<Box<BehaviorContinuationReplyTarget>>,
    pub call_chain_id: Option<String>,
    pub call_depth: Option<u64>,
    pub max_depth: Option<u64>,
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub path: Vec<String>,
}

impl BehaviorContinuationPayload {
    pub fn new(behavior: impl Into<String>, mutation: impl Into<String>) -> Self {
        Self {
            payload_type: "behaviorContinuation",
            behavior: behavior.into(),
            mutation: mutation.into(),
            user_id: None,
            client_mutation_id: None,
            input: None,
            read: None,
            context: None,
            reply_to: None,
            call_chain_id: None,
            call_depth: None,
            max_depth: None,
            deadline_ms: None,
            path: Vec::new(),
        }
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_input(mut self, input: Value) -> Self {
        self.input = Some(input);
        self
    }

    pub fn with_read(mut self, read: BehaviorReadPlan) -> Self {
        self.read = Some(read);
        self
    }

    pub fn with_context(mut self, context: Value) -> Self {
        self.context = Some(context);
        self
    }

    pub fn with_reply_to(mut self, reply_to: BehaviorContinuationReplyTarget) -> Self {
        self.reply_to = Some(Box::new(reply_to));
        self
    }

    pub fn with_deadline_ms(mut self, deadline_ms: u64) -> Self {
        self.deadline_ms = Some(deadline_ms);
        self
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorContinuationReplyTarget {
    pub actor_kind: ActorKind,
    pub actor_key: String,
    pub reminder_id: Option<String>,
    pub continuation: BehaviorContinuationPayload,
}

impl BehaviorContinuationReplyTarget {
    pub fn new(
        actor_kind: ActorKind,
        actor_key: impl Into<String>,
        continuation: BehaviorContinuationPayload,
    ) -> Self {
        Self {
            actor_kind,
            actor_key: actor_key.into(),
            reminder_id: None,
            continuation,
        }
    }

    pub fn with_reminder_id(mut self, reminder_id: impl Into<String>) -> Self {
        self.reminder_id = Some(reminder_id.into());
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct BehaviorReminderOptions {
    pub reminder_id: Option<String>,
    pub due_at_ms: Option<u64>,
    pub delay_ms: Option<u64>,
    pub payload: Option<Value>,
    pub continuation: Option<BehaviorContinuationPayload>,
}

#[derive(Debug, Clone, Default)]
pub struct HostHttpOptions {
    pub request_id: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub body_base64: Option<String>,
    pub timeout_ms: Option<u64>,
    pub reminder_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorInvokeOutput {
    #[serde(default)]
    pub commands: Vec<BehaviorCommand>,
    pub result: Value,
}

impl BehaviorInvokeOutput {
    pub fn new(result: Value) -> Self {
        Self {
            commands: Vec::new(),
            result,
        }
    }

    pub fn with_command(mut self, command: BehaviorCommand) -> Self {
        self.commands.push(command);
        self
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            commands: Vec::new(),
            result: json!({
                "error": message.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BehaviorCommand {
    SendMessage {
        room_id: String,
        body: String,
        #[serde(default)]
        attachments: Vec<String>,
        #[serde(default)]
        durability: Durability,
    },
    PublishVolatile {
        room_id: String,
        name: String,
        payload: Value,
    },
    PublishUserVolatile {
        user_id: String,
        name: String,
        payload: Value,
    },
    PublishUserEvent {
        user_id: String,
        name: String,
        payload: Value,
        #[serde(default)]
        durability: Durability,
        client_mutation_id: Option<String>,
    },
    PutObject {
        body_base64: String,
        content_type: String,
        object_id: Option<String>,
        client_mutation_id: Option<String>,
    },
    DeleteObject {
        object_id: String,
        force: Option<bool>,
        client_mutation_id: Option<String>,
    },
    UpsertRecord {
        table: String,
        key: String,
        value: Value,
        #[serde(default)]
        durability: Durability,
        expected_lsn: Option<u64>,
    },
    DeleteRecord {
        table: String,
        key: String,
        #[serde(default)]
        durability: Durability,
        expected_lsn: Option<u64>,
    },
    RecordTransaction {
        operations: Vec<BehaviorRecordTransactionOperation>,
        #[serde(default)]
        durability: Durability,
    },
    BroadcastRealtimeChannel {
        channel_id: String,
        kind: String,
        payload: Value,
        include_self: Option<bool>,
    },
    UpdateRealtimeChannelState {
        channel_id: String,
        state: Value,
        expected_version: Option<u64>,
    },
    UpdateRealtimePresence {
        channel_id: String,
        metadata: Value,
        session_id: Option<String>,
    },
    DisconnectConnections {
        user_id: Option<String>,
        session_id: Option<String>,
        reason: Option<String>,
    },
    ActivateRuntimeRecords {
        table: String,
        parent_key: Option<String>,
        nested: Option<String>,
        key: Option<String>,
        #[serde(default)]
        keys: Vec<String>,
        after_key: Option<String>,
        order: Option<String>,
        limit: Option<usize>,
    },
    EvictRuntimeRecords {
        table: String,
        parent_key: Option<String>,
        nested: Option<String>,
        key: Option<String>,
        #[serde(default)]
        keys: Vec<String>,
        after_key: Option<String>,
        limit: Option<usize>,
    },
    ActivateRuntimeRoom {
        room_id: String,
        limit: Option<usize>,
    },
    EvictRuntimeRoom {
        room_id: String,
        limit: Option<usize>,
    },
    ScheduleActorReminder {
        kind: ActorKind,
        key: String,
        reminder_id: Option<String>,
        due_at_ms: Option<u64>,
        delay_ms: Option<u64>,
        payload: Option<Value>,
    },
    RequestHostHttp {
        request_id: Option<String>,
        method: String,
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        body: Option<Value>,
        body_base64: Option<String>,
        timeout_ms: Option<u64>,
        actor_kind: ActorKind,
        actor_key: String,
        reminder_id: Option<String>,
        continuation: BehaviorContinuationPayload,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BehaviorRecordTransactionOperation {
    Upsert {
        table: String,
        key: String,
        value: Value,
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
        value: Value,
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

impl BehaviorCommand {
    pub fn send_message(room_id: impl Into<String>, body: impl Into<String>) -> Self {
        Self::SendMessage {
            room_id: room_id.into(),
            body: body.into(),
            attachments: Vec::new(),
            durability: Durability::Strict,
        }
    }

    pub fn publish_volatile(
        room_id: impl Into<String>,
        name: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::PublishVolatile {
            room_id: room_id.into(),
            name: name.into(),
            payload,
        }
    }

    pub fn publish_user_volatile(
        user_id: impl Into<String>,
        name: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::PublishUserVolatile {
            user_id: user_id.into(),
            name: name.into(),
            payload,
        }
    }

    pub fn publish_user_event(
        user_id: impl Into<String>,
        name: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::PublishUserEvent {
            user_id: user_id.into(),
            name: name.into(),
            payload,
            durability: Durability::Strict,
            client_mutation_id: None,
        }
    }

    pub fn publish_user_event_with_mutation_id(
        user_id: impl Into<String>,
        name: impl Into<String>,
        payload: Value,
        client_mutation_id: impl Into<String>,
    ) -> Self {
        Self::PublishUserEvent {
            user_id: user_id.into(),
            name: name.into(),
            payload,
            durability: Durability::Strict,
            client_mutation_id: Some(client_mutation_id.into()),
        }
    }

    pub fn put_object(
        body: impl AsRef<[u8]>,
        content_type: impl Into<String>,
        object_id: Option<String>,
    ) -> Self {
        Self::PutObject {
            body_base64: BASE64_STANDARD.encode(body.as_ref()),
            content_type: content_type.into(),
            object_id,
            client_mutation_id: None,
        }
    }

    pub fn put_object_with_mutation_id(
        body: impl AsRef<[u8]>,
        content_type: impl Into<String>,
        object_id: Option<String>,
        client_mutation_id: impl Into<String>,
    ) -> Self {
        Self::PutObject {
            body_base64: BASE64_STANDARD.encode(body.as_ref()),
            content_type: content_type.into(),
            object_id,
            client_mutation_id: Some(client_mutation_id.into()),
        }
    }

    pub fn delete_object(object_id: impl Into<String>) -> Self {
        Self::DeleteObject {
            object_id: object_id.into(),
            force: None,
            client_mutation_id: None,
        }
    }

    pub fn force_delete_object(object_id: impl Into<String>) -> Self {
        Self::DeleteObject {
            object_id: object_id.into(),
            force: Some(true),
            client_mutation_id: None,
        }
    }

    pub fn upsert_record(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self::UpsertRecord {
            table: table.into(),
            key: key.into(),
            value,
            durability: Durability::Strict,
            expected_lsn: None,
        }
    }

    pub fn upsert_record_expected(
        table: impl Into<String>,
        key: impl Into<String>,
        value: Value,
        expected_lsn: u64,
    ) -> Self {
        Self::UpsertRecord {
            table: table.into(),
            key: key.into(),
            value,
            durability: Durability::Strict,
            expected_lsn: Some(expected_lsn),
        }
    }

    pub fn delete_record(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self::DeleteRecord {
            table: table.into(),
            key: key.into(),
            durability: Durability::Strict,
            expected_lsn: None,
        }
    }

    pub fn delete_record_expected(
        table: impl Into<String>,
        key: impl Into<String>,
        expected_lsn: u64,
    ) -> Self {
        Self::DeleteRecord {
            table: table.into(),
            key: key.into(),
            durability: Durability::Strict,
            expected_lsn: Some(expected_lsn),
        }
    }

    pub fn record_transaction(operations: Vec<BehaviorRecordTransactionOperation>) -> Self {
        Self::RecordTransaction {
            operations,
            durability: Durability::Strict,
        }
    }

    pub fn update_realtime_channel_state(channel_id: impl Into<String>, state: Value) -> Self {
        Self::UpdateRealtimeChannelState {
            channel_id: channel_id.into(),
            state,
            expected_version: None,
        }
    }

    pub fn update_realtime_channel_state_expected(
        channel_id: impl Into<String>,
        state: Value,
        expected_version: u64,
    ) -> Self {
        Self::UpdateRealtimeChannelState {
            channel_id: channel_id.into(),
            state,
            expected_version: Some(expected_version),
        }
    }

    pub fn update_realtime_presence(channel_id: impl Into<String>, metadata: Value) -> Self {
        Self::UpdateRealtimePresence {
            channel_id: channel_id.into(),
            metadata,
            session_id: None,
        }
    }

    pub fn update_realtime_presence_session(
        channel_id: impl Into<String>,
        metadata: Value,
        session_id: impl Into<String>,
    ) -> Self {
        Self::UpdateRealtimePresence {
            channel_id: channel_id.into(),
            metadata,
            session_id: Some(session_id.into()),
        }
    }

    pub fn broadcast_realtime_channel(
        channel_id: impl Into<String>,
        kind: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::BroadcastRealtimeChannel {
            channel_id: channel_id.into(),
            kind: kind.into(),
            payload,
            include_self: None,
        }
    }

    pub fn broadcast_realtime_channel_without_self(
        channel_id: impl Into<String>,
        kind: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::BroadcastRealtimeChannel {
            channel_id: channel_id.into(),
            kind: kind.into(),
            payload,
            include_self: Some(false),
        }
    }

    pub fn disconnect_connections(
        user_id: Option<String>,
        session_id: Option<String>,
        reason: Option<String>,
    ) -> Self {
        Self::DisconnectConnections {
            user_id,
            session_id,
            reason,
        }
    }

    pub fn disconnect_user(user_id: impl Into<String>, reason: Option<String>) -> Self {
        Self::DisconnectConnections {
            user_id: Some(user_id.into()),
            session_id: None,
            reason,
        }
    }

    pub fn disconnect_session(session_id: impl Into<String>, reason: Option<String>) -> Self {
        Self::DisconnectConnections {
            user_id: None,
            session_id: Some(session_id.into()),
            reason,
        }
    }

    pub fn activate_runtime_records(table: impl Into<String>, key: Option<String>) -> Self {
        Self::ActivateRuntimeRecords {
            table: table.into(),
            parent_key: None,
            nested: None,
            key,
            keys: Vec::new(),
            after_key: None,
            order: None,
            limit: None,
        }
    }

    pub fn activate_runtime_nested_records(
        table: impl Into<String>,
        parent_key: impl Into<String>,
        nested: impl Into<String>,
        key: Option<String>,
    ) -> Self {
        Self::ActivateRuntimeRecords {
            table: table.into(),
            parent_key: Some(parent_key.into()),
            nested: Some(nested.into()),
            key,
            keys: Vec::new(),
            after_key: None,
            order: None,
            limit: None,
        }
    }

    pub fn activate_runtime_records_page(
        table: impl Into<String>,
        after_key: Option<String>,
        limit: Option<usize>,
    ) -> Self {
        Self::ActivateRuntimeRecords {
            table: table.into(),
            parent_key: None,
            nested: None,
            key: None,
            keys: Vec::new(),
            after_key,
            order: None,
            limit,
        }
    }

    pub fn activate_runtime_nested_records_page(
        table: impl Into<String>,
        parent_key: impl Into<String>,
        nested: impl Into<String>,
        after_key: Option<String>,
        limit: Option<usize>,
        order: Option<String>,
    ) -> Self {
        Self::ActivateRuntimeRecords {
            table: table.into(),
            parent_key: Some(parent_key.into()),
            nested: Some(nested.into()),
            key: None,
            keys: Vec::new(),
            after_key,
            order,
            limit,
        }
    }

    pub fn evict_runtime_records(table: impl Into<String>, key: Option<String>) -> Self {
        Self::EvictRuntimeRecords {
            table: table.into(),
            parent_key: None,
            nested: None,
            key,
            keys: Vec::new(),
            after_key: None,
            limit: None,
        }
    }

    pub fn evict_runtime_records_page(
        table: impl Into<String>,
        after_key: Option<String>,
        limit: Option<usize>,
    ) -> Self {
        Self::EvictRuntimeRecords {
            table: table.into(),
            parent_key: None,
            nested: None,
            key: None,
            keys: Vec::new(),
            after_key,
            limit,
        }
    }

    pub fn activate_runtime_room(room_id: impl Into<String>, limit: Option<usize>) -> Self {
        Self::ActivateRuntimeRoom {
            room_id: room_id.into(),
            limit,
        }
    }

    pub fn evict_runtime_room(room_id: impl Into<String>) -> Self {
        Self::EvictRuntimeRoom {
            room_id: room_id.into(),
            limit: None,
        }
    }

    pub fn schedule_actor_reminder(
        kind: ActorKind,
        key: impl Into<String>,
        options: BehaviorReminderOptions,
    ) -> Self {
        let payload = match (options.payload, options.continuation) {
            (Some(payload), _) => Some(payload),
            (None, Some(continuation)) => serde_json::to_value(continuation).ok(),
            (None, None) => None,
        };
        Self::ScheduleActorReminder {
            kind,
            key: key.into(),
            reminder_id: options.reminder_id,
            due_at_ms: options.due_at_ms,
            delay_ms: options.delay_ms,
            payload,
        }
    }

    pub fn schedule_behavior_reminder(
        kind: ActorKind,
        key: impl Into<String>,
        behavior: impl Into<String>,
        mutation: impl Into<String>,
        mut options: BehaviorReminderOptions,
    ) -> Self {
        if options.continuation.is_none() {
            options.continuation = Some(BehaviorContinuationPayload::new(behavior, mutation));
        }
        Self::schedule_actor_reminder(kind, key, options)
    }

    pub fn request_host_http(
        method: impl Into<String>,
        url: impl Into<String>,
        actor_kind: ActorKind,
        actor_key: impl Into<String>,
        continuation: BehaviorContinuationPayload,
        options: HostHttpOptions,
    ) -> Self {
        Self::RequestHostHttp {
            request_id: options.request_id,
            method: method.into(),
            url: url.into(),
            headers: options.headers,
            body: options.body,
            body_base64: options.body_base64,
            timeout_ms: options.timeout_ms,
            actor_kind,
            actor_key: actor_key.into(),
            reminder_id: options.reminder_id,
            continuation,
        }
    }
}

impl BehaviorRecordTransactionOperation {
    pub fn upsert(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self::Upsert {
            table: table.into(),
            key: key.into(),
            value,
            expected_lsn: None,
        }
    }

    pub fn delete(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self::Delete {
            table: table.into(),
            key: key.into(),
            expected_lsn: None,
        }
    }

    pub fn nested_upsert(
        table: impl Into<String>,
        parent_key: impl Into<String>,
        nested: impl Into<String>,
        nested_key: impl Into<String>,
        value: Value,
    ) -> Self {
        Self::NestedUpsert {
            table: table.into(),
            parent_key: parent_key.into(),
            nested: nested.into(),
            nested_key: nested_key.into(),
            value,
            expected_lsn: None,
        }
    }

    pub fn nested_delete(
        table: impl Into<String>,
        parent_key: impl Into<String>,
        nested: impl Into<String>,
        nested_key: impl Into<String>,
    ) -> Self {
        Self::NestedDelete {
            table: table.into(),
            parent_key: parent_key.into(),
            nested: nested.into(),
            nested_key: nested_key.into(),
            expected_lsn: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Durability {
    Strict,
    Relaxed,
}

impl Default for Durability {
    fn default() -> Self {
        Self::Strict
    }
}

pub fn alloc_bytes(len: u32) -> u32 {
    let mut buffer = Vec::<u8>::with_capacity(len as usize);
    let ptr = buffer.as_mut_ptr();
    std::mem::forget(buffer);
    ptr as u32
}

/// # Safety
///
/// `ptr` and `len` must describe a buffer previously returned by `alloc_bytes`.
pub unsafe fn dealloc_bytes(ptr: u32, len: u32) {
    unsafe {
        let _ = Vec::from_raw_parts(ptr as *mut u8, 0, len as usize);
    }
}

/// # Safety
///
/// `ptr` and `len` must describe a readable guest-memory byte range owned by
/// the behavior module.
pub unsafe fn invoke<T, F>(ptr: u32, len: u32, handler: F) -> u64
where
    T: DeserializeOwned,
    F: FnOnce(BehaviorInvokeRequest<T>) -> BehaviorInvokeOutput,
{
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let output = match serde_json::from_slice::<BehaviorInvokeRequest<T>>(input) {
        Ok(request) => handler(request),
        Err(err) => BehaviorInvokeOutput::error(format!("invalid invoke request: {err}")),
    };
    let bytes = serde_json::to_vec(&output).expect("behavior output must serialize");
    let out_len = bytes.len() as u32;
    let out_ptr = alloc_bytes(out_len);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr as *mut u8, bytes.len());
    }
    pack_ptr_len(out_ptr, out_len)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BehaviorPostcardFrame {
    encoding: BehaviorPostcardPayloadEncoding,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BehaviorPostcardPayloadEncoding {
    Json,
    TypedSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyBehaviorPostcardFrame {
    json: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedBehaviorInvokeRequest<T> {
    behavior: String,
    mutation: String,
    user_id: Option<String>,
    client_mutation_id: Option<String>,
    input: T,
    #[serde(default)]
    read: BehaviorReadPlan,
    #[serde(default)]
    context: PostcardJsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypedBehaviorInvokeOutput {
    #[serde(default)]
    commands: Vec<PostcardJsonValue>,
    result: PostcardJsonValue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum PostcardJsonValue {
    #[default]
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Vec<PostcardJsonValue>),
    Object(BTreeMap<String, PostcardJsonValue>),
}

impl BehaviorPostcardFrame {
    fn json(json: Vec<u8>) -> Self {
        Self {
            encoding: BehaviorPostcardPayloadEncoding::Json,
            payload: json,
        }
    }

    fn typed_schema(payload: Vec<u8>) -> Self {
        Self {
            encoding: BehaviorPostcardPayloadEncoding::TypedSchema,
            payload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostcardPayloadEncoding {
    Json,
    TypedSchema,
}

/// # Safety
///
/// `ptr` and `len` must describe a readable guest-memory byte range owned by
/// the behavior module. The byte range must contain a postcard-encoded
/// `BehaviorPostcardFrame`. `encoding: "json"` payloads carry the stable JSON
/// ABI byte vector; `encoding: "typedSchema"` payloads carry a postcard-encoded
/// typed request. The SDK accepts both direct `BehaviorInvokeRequest<T>` payloads
/// and host-generated schema-neutral payloads that convert into `T`. Legacy
/// frames with a `json` field are still accepted.
pub unsafe fn invoke_postcard<T, F>(ptr: u32, len: u32, handler: F) -> u64
where
    T: DeserializeOwned,
    F: FnOnce(BehaviorInvokeRequest<T>) -> BehaviorInvokeOutput,
{
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let (output, output_encoding) = match decode_postcard_request::<T>(input) {
        Ok((request, encoding)) => (handler(request), encoding),
        Err(err) => (
            BehaviorInvokeOutput::error(format!("invalid postcard invoke request: {err}")),
            PostcardPayloadEncoding::Json,
        ),
    };
    let bytes =
        encode_postcard_output(&output, output_encoding).expect("behavior output must serialize");
    let out_len = bytes.len() as u32;
    let out_ptr = alloc_bytes(out_len);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr as *mut u8, bytes.len());
    }
    pack_ptr_len(out_ptr, out_len)
}

fn decode_postcard_request<T>(
    input: &[u8],
) -> Result<(BehaviorInvokeRequest<T>, PostcardPayloadEncoding), String>
where
    T: DeserializeOwned,
{
    match decode_postcard_payload(input)? {
        DecodedPostcardPayload::Json(json) => {
            serde_json::from_slice::<BehaviorInvokeRequest<T>>(&json)
                .map(|request| (request, PostcardPayloadEncoding::Json))
                .map_err(|err| err.to_string())
        }
        DecodedPostcardPayload::TypedSchema(payload) => decode_typed_schema_request::<T>(&payload)
            .map(|request| (request, PostcardPayloadEncoding::TypedSchema)),
    }
}

fn decode_typed_schema_request<T>(payload: &[u8]) -> Result<BehaviorInvokeRequest<T>, String>
where
    T: DeserializeOwned,
{
    match postcard::from_bytes::<TypedBehaviorInvokeRequest<T>>(payload) {
        Ok(request) => Ok(BehaviorInvokeRequest::from(request)),
        Err(direct_err) => {
            let request =
                postcard::from_bytes::<TypedBehaviorInvokeRequest<PostcardJsonValue>>(payload)
                    .map_err(|fallback_err| {
                        format!(
                            "invalid typedSchema request: typed decode error: {direct_err}; schema-neutral decode error: {fallback_err}"
                        )
                    })?;
            let input = serde_json::from_value::<T>(Value::from(request.input))
                .map_err(|err| format!("invalid typedSchema input: {err}"))?;
            Ok(BehaviorInvokeRequest {
                behavior: request.behavior,
                mutation: request.mutation,
                user_id: request.user_id,
                client_mutation_id: request.client_mutation_id,
                input,
                read: request.read,
                context: Value::from(request.context),
            })
        }
    }
}

fn encode_postcard_output(
    output: &BehaviorInvokeOutput,
    encoding: PostcardPayloadEncoding,
) -> Result<Vec<u8>, String> {
    let frame = match encoding {
        PostcardPayloadEncoding::Json => {
            let json = serde_json::to_vec(output).map_err(|err| err.to_string())?;
            BehaviorPostcardFrame::json(json)
        }
        PostcardPayloadEncoding::TypedSchema => {
            let payload =
                postcard::to_allocvec(&TypedBehaviorInvokeOutput::try_from_output(output)?)
                    .map_err(|err| err.to_string())?;
            BehaviorPostcardFrame::typed_schema(payload)
        }
    };
    postcard::to_allocvec(&frame).map_err(|err| err.to_string())
}

enum DecodedPostcardPayload {
    Json(Vec<u8>),
    TypedSchema(Vec<u8>),
}

fn decode_postcard_payload(input: &[u8]) -> Result<DecodedPostcardPayload, String> {
    match postcard::from_bytes::<BehaviorPostcardFrame>(input) {
        Ok(frame) => match frame.encoding {
            BehaviorPostcardPayloadEncoding::Json => Ok(DecodedPostcardPayload::Json(frame.payload)),
            BehaviorPostcardPayloadEncoding::TypedSchema => {
                Ok(DecodedPostcardPayload::TypedSchema(frame.payload))
            }
        },
        Err(frame_err) => postcard::from_bytes::<LegacyBehaviorPostcardFrame>(input)
            .map(|legacy| DecodedPostcardPayload::Json(legacy.json))
            .map_err(|legacy_err| {
                format!(
                    "invalid behavior postcard frame: typed frame error: {frame_err}; legacy frame error: {legacy_err}"
                )
            }),
    }
}

impl<T> From<TypedBehaviorInvokeRequest<T>> for BehaviorInvokeRequest<T> {
    fn from(request: TypedBehaviorInvokeRequest<T>) -> Self {
        Self {
            behavior: request.behavior,
            mutation: request.mutation,
            user_id: request.user_id,
            client_mutation_id: request.client_mutation_id,
            input: request.input,
            read: request.read,
            context: Value::from(request.context),
        }
    }
}

impl TypedBehaviorInvokeOutput {
    fn try_from_output(output: &BehaviorInvokeOutput) -> Result<Self, String> {
        let mut commands = Vec::with_capacity(output.commands.len());
        for command in &output.commands {
            let value = serde_json::to_value(command).map_err(|err| err.to_string())?;
            commands.push(PostcardJsonValue::try_from(value)?);
        }
        Ok(Self {
            commands,
            result: PostcardJsonValue::try_from(output.result.clone())?,
        })
    }
}

impl TryFrom<Value> for PostcardJsonValue {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(value) => Ok(Self::Bool(value)),
            Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Self::I64(value))
                } else if let Some(value) = value.as_u64() {
                    Ok(Self::U64(value))
                } else if let Some(value) = value.as_f64() {
                    Ok(Self::F64(value))
                } else {
                    Err("unsupported JSON number in typed behavior postcard payload".to_string())
                }
            }
            Value::String(value) => Ok(Self::String(value)),
            Value::Array(values) => values
                .into_iter()
                .map(PostcardJsonValue::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Object(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, PostcardJsonValue::try_from(value)?)))
                .collect::<Result<BTreeMap<_, _>, String>>()
                .map(Self::Object),
        }
    }
}

impl From<PostcardJsonValue> for Value {
    fn from(value: PostcardJsonValue) -> Self {
        match value {
            PostcardJsonValue::Null => Value::Null,
            PostcardJsonValue::Bool(value) => Value::Bool(value),
            PostcardJsonValue::I64(value) => Value::Number(Number::from(value)),
            PostcardJsonValue::U64(value) => Value::Number(Number::from(value)),
            PostcardJsonValue::F64(value) => Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            PostcardJsonValue::String(value) => Value::String(value),
            PostcardJsonValue::Array(values) => {
                Value::Array(values.into_iter().map(Value::from).collect())
            }
            PostcardJsonValue::Object(values) => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Value::from(value)))
                    .collect(),
            ),
        }
    }
}

pub fn pack_ptr_len(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | len as u64
}

#[macro_export]
macro_rules! nextdb_behavior {
    ($input:ty, $handler:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn alloc(len: u32) -> u32 {
            $crate::alloc_bytes(len)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn dealloc(ptr: u32, len: u32) {
            unsafe {
                $crate::dealloc_bytes(ptr, len);
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn invoke(ptr: u32, len: u32) -> u64 {
            unsafe { $crate::invoke::<$input, _>(ptr, len, $handler) }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn handle_message(ptr: u32, len: u32) -> u64 {
            unsafe { $crate::invoke::<$input, _>(ptr, len, $handler) }
        }
    };
}

#[macro_export]
macro_rules! nextdb_behavior_postcard {
    ($input:ty, $handler:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn alloc(len: u32) -> u32 {
            $crate::alloc_bytes(len)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn dealloc(ptr: u32, len: u32) {
            unsafe {
                $crate::dealloc_bytes(ptr, len);
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn invoke(ptr: u32, len: u32) -> u64 {
            unsafe { $crate::invoke_postcard::<$input, _>(ptr, len, $handler) }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn handle_message(ptr: u32, len: u32) -> u64 {
            unsafe { $crate::invoke_postcard::<$input, _>(ptr, len, $handler) }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_behavior_reminder_serializes_as_actor_reminder_continuation() {
        let command = BehaviorCommand::schedule_behavior_reminder(
            ActorKind::Room,
            "room-1",
            "echo",
            "echo.send",
            BehaviorReminderOptions {
                reminder_id: Some("reminder-1".to_string()),
                delay_ms: Some(10),
                continuation: Some(
                    BehaviorContinuationPayload::new("echo", "echo.send")
                        .with_user_id("alice")
                        .with_input(json!({ "roomId": "room-1", "body": "tick" })),
                ),
                ..BehaviorReminderOptions::default()
            },
        );

        assert_eq!(
            serde_json::to_value(command).expect("serialize command"),
            json!({
                "type": "scheduleActorReminder",
                "kind": "room",
                "key": "room-1",
                "reminderId": "reminder-1",
                "dueAtMs": null,
                "delayMs": 10,
                "payload": {
                    "type": "behaviorContinuation",
                    "behavior": "echo",
                    "mutation": "echo.send",
                    "userId": "alice",
                    "clientMutationId": null,
                    "input": { "roomId": "room-1", "body": "tick" },
                    "read": null,
                    "context": null,
                    "replyTo": null,
                    "callChainId": null,
                    "callDepth": null,
                    "maxDepth": null,
                    "deadlineMs": null,
                    "path": []
                }
            })
        );
    }

    #[test]
    fn behavior_continuation_serializes_reply_target() {
        let continuation = BehaviorContinuationPayload::new("worker", "run").with_reply_to(
            BehaviorContinuationReplyTarget::new(
                ActorKind::Room,
                "room-2",
                BehaviorContinuationPayload::new("reply", "done")
                    .with_input(json!({ "existing": true })),
            )
            .with_reminder_id("reply-1"),
        );

        assert_eq!(
            serde_json::to_value(continuation).expect("serialize continuation"),
            json!({
                "type": "behaviorContinuation",
                "behavior": "worker",
                "mutation": "run",
                "userId": null,
                "clientMutationId": null,
                "input": null,
                "read": null,
                "context": null,
                "replyTo": {
                    "actorKind": "room",
                    "actorKey": "room-2",
                    "reminderId": "reply-1",
                    "continuation": {
                        "type": "behaviorContinuation",
                        "behavior": "reply",
                        "mutation": "done",
                        "userId": null,
                        "clientMutationId": null,
                        "input": { "existing": true },
                        "read": null,
                        "context": null,
                        "replyTo": null,
                        "callChainId": null,
                        "callDepth": null,
                        "maxDepth": null,
                        "deadlineMs": null,
                        "path": []
                    }
                },
                "callChainId": null,
                "callDepth": null,
                "maxDepth": null,
                "deadlineMs": null,
                "path": []
            })
        );
    }

    #[test]
    fn request_host_http_serializes_continuation_and_actor_target() {
        let mut headers = BTreeMap::new();
        headers.insert("accept".to_string(), "application/json".to_string());
        let command = BehaviorCommand::request_host_http(
            "GET",
            "https://example.test/hook",
            ActorKind::Scope,
            "scope-1",
            BehaviorContinuationPayload::new("echo", "http.done"),
            HostHttpOptions {
                request_id: Some("req-1".to_string()),
                headers,
                timeout_ms: Some(1000),
                ..HostHttpOptions::default()
            },
        );

        assert_eq!(
            serde_json::to_value(command).expect("serialize command"),
            json!({
                "type": "requestHostHttp",
                "requestId": "req-1",
                "method": "GET",
                "url": "https://example.test/hook",
                "headers": { "accept": "application/json" },
                "body": null,
                "bodyBase64": null,
                "timeoutMs": 1000,
                "actorKind": "scope",
                "actorKey": "scope-1",
                "reminderId": null,
                "continuation": {
                    "type": "behaviorContinuation",
                    "behavior": "echo",
                    "mutation": "http.done",
                    "userId": null,
                    "clientMutationId": null,
                    "input": null,
                    "read": null,
                    "context": null,
                    "replyTo": null,
                    "callChainId": null,
                    "callDepth": null,
                    "maxDepth": null,
                    "deadlineMs": null,
                    "path": []
                }
            })
        );
    }

    #[test]
    fn runtime_context_reads_host_injected_context() {
        let request = BehaviorInvokeRequest {
            behavior: "echo".to_string(),
            mutation: "echo.send".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: Some("cmid".to_string()),
            input: json!({}),
            read: BehaviorReadPlan::default(),
            context: json!({
                "requestContext": {
                    "ctx": {
                        "timestampMs": 42,
                        "sender": {
                            "kind": "user",
                            "userId": "alice",
                            "behavior": "echo",
                            "mutation": "echo.send",
                            "clientMutationId": "cmid"
                        },
                        "rngSeed": "seed",
                        "extra": true
                    }
                }
            }),
        };

        let context = runtime_context(&request).expect("runtime context");
        assert_eq!(context.timestamp_ms, 42);
        assert_eq!(context.sender.user_id.as_deref(), Some("alice"));
        assert_eq!(context.rng_seed, "seed");
        assert_eq!(context.extra.get("extra"), Some(&json!(true)));
    }

    #[test]
    fn postcard_frame_round_trips_typed_request_and_output() {
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Input {
            room_id: String,
        }

        let request_json = serde_json::to_vec(&json!({
            "behavior": "echo",
            "mutation": "echo.send",
            "input": { "roomId": "room-1" }
        }))
        .expect("encode request json");
        let frame = postcard::to_allocvec(&BehaviorPostcardFrame::json(request_json.clone()))
            .expect("encode postcard frame");

        let (request, encoding) = decode_postcard_request::<Input>(&frame).expect("decode request");
        assert_eq!(encoding, PostcardPayloadEncoding::Json);
        assert_eq!(request.input.room_id, "room-1");
        let legacy_frame =
            postcard::to_allocvec(&LegacyBehaviorPostcardFrame { json: request_json })
                .expect("encode legacy postcard frame");
        let (legacy_request, legacy_encoding) =
            decode_postcard_request::<Input>(&legacy_frame).expect("decode legacy request");
        assert_eq!(legacy_encoding, PostcardPayloadEncoding::Json);
        assert_eq!(legacy_request.input.room_id, "room-1");

        let typed_request = TypedBehaviorInvokeRequest {
            behavior: "echo".to_string(),
            mutation: "echo.send".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: Some("cmid".to_string()),
            input: Input {
                room_id: "room-typed".to_string(),
            },
            read: BehaviorReadPlan::default(),
            context: PostcardJsonValue::try_from(json!({ "source": "typed" }))
                .expect("typed context"),
        };
        let typed_payload = postcard::to_allocvec(&typed_request).expect("encode typed request");
        let typed_schema_frame =
            postcard::to_allocvec(&BehaviorPostcardFrame::typed_schema(typed_payload))
                .expect("encode typed-schema frame");
        let (typed_decoded, typed_encoding) =
            decode_postcard_request::<Input>(&typed_schema_frame).expect("decode typed request");
        assert_eq!(typed_encoding, PostcardPayloadEncoding::TypedSchema);
        assert_eq!(typed_decoded.input.room_id, "room-typed");
        assert_eq!(typed_decoded.user_id.as_deref(), Some("alice"));

        let schema_neutral_request = TypedBehaviorInvokeRequest {
            behavior: "echo".to_string(),
            mutation: "echo.send".to_string(),
            user_id: Some("alice".to_string()),
            client_mutation_id: Some("cmid".to_string()),
            input: PostcardJsonValue::try_from(json!({ "roomId": "room-neutral" }))
                .expect("schema-neutral input"),
            read: BehaviorReadPlan::default(),
            context: PostcardJsonValue::try_from(json!({ "source": "neutral" }))
                .expect("schema-neutral context"),
        };
        let schema_neutral_payload =
            postcard::to_allocvec(&schema_neutral_request).expect("encode neutral request");
        let schema_neutral_frame =
            postcard::to_allocvec(&BehaviorPostcardFrame::typed_schema(schema_neutral_payload))
                .expect("encode neutral typed-schema frame");
        let (schema_neutral_decoded, schema_neutral_encoding) =
            decode_postcard_request::<Input>(&schema_neutral_frame)
                .expect("decode neutral request");
        assert_eq!(
            schema_neutral_encoding,
            PostcardPayloadEncoding::TypedSchema
        );
        assert_eq!(schema_neutral_decoded.input.room_id, "room-neutral");
        assert_eq!(schema_neutral_decoded.context["source"], json!("neutral"));

        let output = BehaviorInvokeOutput::new(json!({ "ok": true }));
        let encoded =
            encode_postcard_output(&output, PostcardPayloadEncoding::Json).expect("encode output");
        let decoded =
            postcard::from_bytes::<BehaviorPostcardFrame>(&encoded).expect("decode output frame");
        assert_eq!(decoded.encoding, BehaviorPostcardPayloadEncoding::Json);
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded.payload).expect("decode output json"),
            json!({ "commands": [], "result": { "ok": true } })
        );

        let typed_encoded = encode_postcard_output(&output, PostcardPayloadEncoding::TypedSchema)
            .expect("encode typed output");
        let typed_decoded_frame = postcard::from_bytes::<BehaviorPostcardFrame>(&typed_encoded)
            .expect("decode typed output frame");
        assert_eq!(
            typed_decoded_frame.encoding,
            BehaviorPostcardPayloadEncoding::TypedSchema
        );
        let typed_output =
            postcard::from_bytes::<TypedBehaviorInvokeOutput>(&typed_decoded_frame.payload)
                .expect("decode typed output payload");
        let typed_output = Value::from(typed_output.result);
        assert_eq!(typed_output, json!({ "ok": true }));
    }
}
