use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    schema::{DatabaseSchema, SchemaMigrationPlan},
    util::hex_lower,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BinaryJsonValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Vec<BinaryJsonValue>),
    Object(BTreeMap<String, BinaryJsonValue>),
}

impl BinaryJsonValue {
    pub(crate) fn from_json(value: &serde_json::Value) -> Result<Self> {
        Ok(match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Self::I64(value)
                } else if let Some(value) = value.as_u64() {
                    Self::U64(value)
                } else {
                    Self::F64(value.as_f64().context("JSON number is not finite")?)
                }
            }
            serde_json::Value::String(value) => Self::String(value.clone()),
            serde_json::Value::Array(values) => Self::Array(
                values
                    .iter()
                    .map(Self::from_json)
                    .collect::<Result<Vec<_>>>()?,
            ),
            serde_json::Value::Object(values) => Self::Object(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), Self::from_json(value)?)))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ),
        })
    }

    pub(crate) fn into_json(self) -> Result<serde_json::Value> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(value),
            Self::I64(value) => serde_json::Value::Number(value.into()),
            Self::U64(value) => serde_json::Value::Number(value.into()),
            Self::F64(value) => serde_json::Value::Number(
                serde_json::Number::from_f64(value)
                    .ok_or_else(|| anyhow::anyhow!("non-finite f64 JSON value"))?,
            ),
            Self::String(value) => serde_json::Value::String(value),
            Self::Array(values) => serde_json::Value::Array(
                values
                    .into_iter()
                    .map(Self::into_json)
                    .collect::<Result<Vec<_>>>()?,
            ),
            Self::Object(values) => {
                let mut object = serde_json::Map::with_capacity(values.len());
                for (key, value) in values {
                    if object.insert(key.clone(), value.into_json()?).is_some() {
                        bail!("duplicate JSON object key {key}");
                    }
                }
                serde_json::Value::Object(object)
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub room_id: String,
    pub sender_id: String,
    pub body: String,
    #[serde(default)]
    pub attachments: Vec<ObjectRef>,
    pub created_at_ms: u64,
    pub lsn: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDraft {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_mutation_id: Option<String>,
    pub room_id: String,
    pub sender_id: String,
    pub body: String,
    #[serde(default)]
    pub attachments: Vec<ObjectRef>,
    pub created_at_ms: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEvent {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub payload: serde_json::Value,
    pub created_at_ms: u64,
    pub lsn: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEventDraft {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_mutation_id: Option<String>,
    pub user_id: String,
    pub name: String,
    pub payload: serde_json::Value,
    pub created_at_ms: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserProfile {
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub lsn: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserProfileDraft {
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_mutation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DbRecord {
    pub table: String,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at_ms: u64,
    pub lsn: u64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DbRecordDraft {
    pub table: String,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at_ms: u64,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_mutation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DbRecordDeleteDraft {
    pub table: String,
    pub key: String,
    pub deleted_at_ms: u64,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_mutation_id: Option<String>,
}

use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DbRecordMutationDraft {
    Upsert { record: DbRecordDraft },
    Delete { record: DbRecordDeleteDraft },
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ClientMutationRecord {
    RecordDeleteNoop {
        table: String,
        key: String,
        path: String,
    },
    RecordTransactionNoop,
    ObjectDeleteNoop {
        object_id: String,
        path: String,
    },
}

impl DbRecordDraft {
    pub fn into_record(self, lsn: u64) -> DbRecord {
        DbRecord {
            table: self.table,
            key: self.key,
            value: self.value,
            updated_at_ms: self.updated_at_ms,
            lsn,
            path: self.path,
        }
    }
}

impl MessageDraft {
    pub fn into_message(self, lsn: u64) -> Message {
        Message {
            id: self.id,
            room_id: self.room_id,
            sender_id: self.sender_id,
            body: self.body,
            attachments: self.attachments,
            created_at_ms: self.created_at_ms,
            lsn,
            path: self.path,
        }
    }
}

impl UserEventDraft {
    pub fn into_event(self, lsn: u64) -> UserEvent {
        UserEvent {
            id: self.id,
            user_id: self.user_id,
            name: self.name,
            payload: self.payload,
            created_at_ms: self.created_at_ms,
            lsn,
            path: self.path,
        }
    }
}

impl UserProfileDraft {
    pub fn into_profile(self, lsn: u64) -> UserProfile {
        UserProfile {
            user_id: self.user_id,
            display_name: self.display_name,
            metadata: self.metadata,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            lsn,
            path: self.path,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Durability {
    #[default]
    Strict,
    Relaxed,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorPublishedManifest {
    pub name: String,
    pub version: String,
    pub module_path: String,
    pub mutations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorPublishedDraft {
    pub epoch: u64,
    pub loaded: usize,
    pub manifests: Vec<BehaviorPublishedManifest>,
    pub published_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostHttpRequestDraft {
    pub request_id: String,
    pub method: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_base64: Option<String>,
    pub timeout_ms: u64,
    pub actor_kind: String,
    pub actor_key: String,
    pub reminder_id: String,
    pub continuation: serde_json::Value,
    pub requested_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum WalPayload {
    MessageCreated {
        message: MessageDraft,
    },
    UserEventPublished {
        event: UserEventDraft,
    },
    UserUpserted {
        user: UserProfileDraft,
    },
    ObjectCommitted {
        object: ObjectMetadata,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_mutation_id: Option<String>,
    },
    ObjectDeleted {
        object_id: String,
        deleted_at_ms: u64,
        path: String,
        #[serde(default)]
        force: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_mutation_id: Option<String>,
    },
    RecordUpserted {
        record: DbRecordDraft,
    },
    RecordDeleted {
        record: DbRecordDeleteDraft,
    },
    RecordTransactionCommitted {
        operations: Vec<DbRecordMutationDraft>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_mutation_id: Option<String>,
    },
    SchemaApplied {
        schema: DatabaseSchema,
        migration: SchemaMigrationPlan,
    },
    BehaviorPublished {
        publish: BehaviorPublishedDraft,
    },
    ActorReminderScheduled {
        reminder: ActorReminderDraft,
    },
    ActorReminderCancelled {
        actor_kind: String,
        actor_key: String,
        reminder_id: String,
        cancelled_at_ms: u64,
    },
    ActorReminderFired {
        actor_kind: String,
        actor_key: String,
        reminder_id: String,
        due_at_ms: u64,
        fired_at_ms: u64,
    },
    HostHttpRequested {
        request: HostHttpRequestDraft,
    },
    HostHttpCompleted {
        request_id: String,
        completed_at_ms: u64,
    },
    ClientMutationRecorded {
        client_mutation_id: String,
        record: ClientMutationRecord,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorReminderDraft {
    pub actor_kind: String,
    pub actor_key: String,
    pub reminder_id: String,
    pub due_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalRecord {
    pub lsn: u64,
    #[serde(default)]
    pub shard: usize,
    #[serde(default = "default_shard_epoch")]
    pub shard_epoch: u64,
    #[serde(default)]
    pub owner_node_id: String,
    pub timestamp_ms: u64,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub durability: Durability,
    pub payload: WalPayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

fn default_schema_version() -> u32 {
    1
}

fn default_shard_epoch() -> u64 {
    1
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WalRecordChecksumMaterial<'a> {
    lsn: u64,
    shard: usize,
    shard_epoch: u64,
    owner_node_id: &'a str,
    timestamp_ms: u64,
    schema_version: u32,
    durability: Durability,
    payload: &'a WalPayload,
}

impl WalRecord {
    pub fn compute_checksum(&self) -> Result<String> {
        let material = WalRecordChecksumMaterial {
            lsn: self.lsn,
            shard: self.shard,
            shard_epoch: self.shard_epoch,
            owner_node_id: &self.owner_node_id,
            timestamp_ms: self.timestamp_ms,
            schema_version: self.schema_version,
            durability: self.durability,
            payload: &self.payload,
        };
        let bytes = serde_json::to_vec(&material)?;
        Ok(format!("sha256:{}", hex_lower(&Sha256::digest(bytes))))
    }

    pub fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum = Some(self.compute_checksum()?);
        Ok(())
    }

    pub fn ensure_checksum(&mut self) -> Result<()> {
        if self.checksum.is_none() {
            self.refresh_checksum()?;
        }
        Ok(())
    }

    pub fn verify_checksum(&self) -> Result<WalChecksumStatus> {
        let Some(checksum) = &self.checksum else {
            return Ok(WalChecksumStatus::Missing);
        };
        let expected = self.compute_checksum()?;
        if checksum == &expected {
            Ok(WalChecksumStatus::Valid)
        } else {
            Ok(WalChecksumStatus::Mismatch { expected })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalChecksumStatus {
    Missing,
    Valid,
    Mismatch { expected: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DeliveryEvent {
    MessageCreated {
        room_id: String,
        message: Message,
    },
    VolatileRoomEvent {
        room_id: String,
        name: String,
        payload: serde_json::Value,
    },
    VolatileUserEvent {
        user_id: String,
        name: String,
        payload: serde_json::Value,
        #[serde(default, skip_serializing)]
        target_session_ids: Option<BTreeSet<String>>,
    },
    UserEvent {
        user_id: String,
        event: UserEvent,
    },
    UserUpserted {
        user_id: String,
        user: UserProfile,
    },
    RecordUpserted {
        table: String,
        key: String,
        record: DbRecord,
    },
    RecordDeleted {
        table: String,
        key: String,
        deleted_at_ms: u64,
        lsn: u64,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous_record: Option<DbRecord>,
    },
    ObjectCommitted {
        object: ObjectMetadata,
        lsn: u64,
    },
    ObjectDeleted {
        object_id: String,
        deleted_at_ms: u64,
        lsn: u64,
        path: String,
        force: bool,
    },
}

pub(crate) type DeliveryEventBatch = Vec<DeliveryEvent>;
pub(crate) type SharedDeliveryEventBatch = Vec<std::sync::Arc<DeliveryEvent>>;

impl DeliveryEvent {
    pub fn room_id(&self) -> Option<&str> {
        match self {
            Self::MessageCreated { room_id, .. } | Self::VolatileRoomEvent { room_id, .. } => {
                Some(room_id)
            }
            Self::VolatileUserEvent { .. }
            | Self::UserEvent { .. }
            | Self::UserUpserted { .. }
            | Self::RecordUpserted { .. }
            | Self::RecordDeleted { .. }
            | Self::ObjectCommitted { .. }
            | Self::ObjectDeleted { .. } => None,
        }
    }

    pub fn user_id(&self) -> Option<&str> {
        match self {
            Self::VolatileUserEvent { user_id, .. }
            | Self::UserEvent { user_id, .. }
            | Self::UserUpserted { user_id, .. } => Some(user_id),
            Self::MessageCreated { .. }
            | Self::VolatileRoomEvent { .. }
            | Self::RecordUpserted { .. }
            | Self::RecordDeleted { .. }
            | Self::ObjectCommitted { .. }
            | Self::ObjectDeleted { .. } => None,
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            Self::RecordUpserted { table, .. } | Self::RecordDeleted { table, .. } => Some(table),
            Self::MessageCreated { .. }
            | Self::VolatileRoomEvent { .. }
            | Self::UserEvent { .. }
            | Self::UserUpserted { .. }
            | Self::VolatileUserEvent { .. }
            | Self::ObjectCommitted { .. }
            | Self::ObjectDeleted { .. } => None,
        }
    }

    pub fn record_key(&self) -> Option<&str> {
        match self {
            Self::RecordUpserted { key, .. } | Self::RecordDeleted { key, .. } => Some(key),
            Self::MessageCreated { .. }
            | Self::VolatileRoomEvent { .. }
            | Self::UserEvent { .. }
            | Self::UserUpserted { .. }
            | Self::VolatileUserEvent { .. }
            | Self::ObjectCommitted { .. }
            | Self::ObjectDeleted { .. } => None,
        }
    }

    pub fn is_object_event(&self) -> bool {
        matches!(
            self,
            Self::ObjectCommitted { .. } | Self::ObjectDeleted { .. }
        )
    }

    pub fn target_session_ids(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::VolatileUserEvent {
                target_session_ids, ..
            } => target_session_ids.as_ref(),
            Self::MessageCreated { .. }
            | Self::VolatileRoomEvent { .. }
            | Self::UserEvent { .. }
            | Self::UserUpserted { .. }
            | Self::RecordUpserted { .. }
            | Self::RecordDeleted { .. }
            | Self::ObjectCommitted { .. }
            | Self::ObjectDeleted { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMetadata {
    pub id: String,
    pub path: String,
    pub content_type: String,
    pub byte_size: u64,
    pub sha256: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef {
    pub id: String,
    pub path: String,
    pub content_type: String,
    pub byte_size: u64,
    pub sha256: String,
}

impl From<ObjectMetadata> for ObjectRef {
    fn from(metadata: ObjectMetadata) -> Self {
        Self {
            id: metadata.id,
            path: metadata.path,
            content_type: metadata.content_type,
            byte_size: metadata.byte_size,
            sha256: metadata.sha256,
        }
    }
}
