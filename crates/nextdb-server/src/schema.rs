use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;

use crate::model::{MessageDraft, ObjectRef};

#[derive(Clone)]
pub struct SchemaRegistry {
    path: PathBuf,
    schema: Arc<RwLock<DatabaseSchema>>,
}

impl SchemaRegistry {
    pub async fn load(path: PathBuf) -> Result<Self> {
        let schema = if path.exists() {
            let bytes = fs::read(&path).await?;
            serde_json::from_slice(&bytes)?
        } else {
            let schema = DatabaseSchema::default_nextdb();
            persist_schema(&path, &schema).await?;
            schema
        };
        let registry = Self {
            path,
            schema: Arc::new(RwLock::new(schema)),
        };
        registry.persist_history_version(&registry.schema()).await?;
        Ok(registry)
    }

    pub fn schema(&self) -> DatabaseSchema {
        self.schema
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn version(&self) -> u32 {
        self.schema
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .version
    }

    pub fn typescript(&self) -> String {
        generate_typescript(&self.schema())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn candidate_from_disk(&self) -> Result<DatabaseSchema> {
        let bytes = fs::read(&self.path).await?;
        let schema: DatabaseSchema = serde_json::from_slice(&bytes)?;
        schema.validation_report().into_result()?;
        Ok(schema)
    }

    pub fn apply(&self, schema: DatabaseSchema) {
        *self
            .schema
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = schema;
    }

    pub async fn persist_candidate(&self, schema: &DatabaseSchema) -> Result<()> {
        self.persist_history_version(schema).await?;
        persist_schema(&self.path, schema).await
    }

    pub async fn persist_history_schema(&self, schema: &DatabaseSchema) -> Result<()> {
        self.persist_history_version(schema).await
    }

    pub async fn history(&self) -> Result<Vec<SchemaHistoryEntry>> {
        let mut entries = BTreeMap::new();
        let history_dir = self.history_dir();
        if history_dir.exists() {
            let mut dir = fs::read_dir(history_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("json") {
                    continue;
                }
                let bytes = fs::read(&path).await?;
                let schema: DatabaseSchema = serde_json::from_slice(&bytes)?;
                entries.insert(
                    schema.version,
                    SchemaHistoryEntry::from_schema(&schema, false),
                );
            }
        }
        let current = self.schema();
        entries.insert(
            current.version,
            SchemaHistoryEntry::from_schema(&current, true),
        );
        let mut out: Vec<_> = entries.into_values().collect();
        let current_version = current.version;
        for entry in &mut out {
            entry.current = entry.version == current_version;
        }
        Ok(out)
    }

    pub async fn schema_version(&self, version: u32) -> Result<Option<DatabaseSchema>> {
        let path = self.history_path(version);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).await?;
        let schema = serde_json::from_slice(&bytes)?;
        Ok(Some(schema))
    }

    pub async fn migration_plan_from_disk(&self) -> Result<SchemaMigrationPlan> {
        let bytes = fs::read(&self.path).await?;
        let candidate: DatabaseSchema = serde_json::from_slice(&bytes)?;
        Ok(self.migration_plan_for(&candidate))
    }

    pub fn migration_plan_for(&self, candidate: &DatabaseSchema) -> SchemaMigrationPlan {
        SchemaMigrationPlan::between(&self.schema(), candidate)
    }

    pub fn validation_report(&self) -> SchemaValidationReport {
        self.schema().validation_report()
    }

    pub fn storage_policy_report(&self) -> SchemaStoragePolicyReport {
        self.schema().storage_policy_report()
    }

    pub fn validate_message_draft(&self, draft: &MessageDraft) -> Result<()> {
        self.schema().validate_message_draft(draft)
    }

    pub fn validate_table_record(&self, table_name: &str, value: &Value) -> Result<()> {
        self.schema().validate_table_record(table_name, value)
    }

    pub fn validate_nested_table_record(
        &self,
        table_name: &str,
        nested_name: &str,
        value: &Value,
    ) -> Result<()> {
        self.schema()
            .validate_nested_table_record(table_name, nested_name, value)
    }

    pub fn table_indexes(&self, table_name: &str) -> Result<BTreeMap<String, IndexSchema>> {
        let schema = self.schema();
        let table = schema
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("schema missing table {table_name}"))?;
        Ok(table.indexes.clone())
    }

    pub fn record_indexes(&self, table_name: &str) -> Result<BTreeMap<String, IndexSchema>> {
        let schema = self.schema();
        if let Some(table) = schema.tables.get(table_name) {
            return Ok(table.indexes.clone());
        }
        if let Some((parent_table, nested_table)) = table_name.split_once('.')
            && let Some(nested) = schema
                .tables
                .get(parent_table)
                .and_then(|table| table.nested.get(nested_table))
        {
            return Ok(nested.indexes.clone());
        }
        bail!("schema missing table {table_name}")
    }

    pub fn nested_table_indexes(
        &self,
        table_name: &str,
        nested_name: &str,
    ) -> Result<BTreeMap<String, IndexSchema>> {
        let schema = self.schema();
        let nested = schema
            .tables
            .get(table_name)
            .and_then(|table| table.nested.get(nested_name))
            .ok_or_else(|| {
                anyhow::anyhow!("schema missing nested table {table_name}.{nested_name}")
            })?;
        Ok(nested.indexes.clone())
    }

    pub fn record_table_accepts_volatile(&self, table_name: &str) -> Result<bool> {
        self.schema().record_table_accepts_volatile(table_name)
    }

    pub fn validate_behavior_input(
        &self,
        behavior_name: &str,
        mutation: &str,
        value: &Value,
    ) -> Result<()> {
        let schema = self.schema();
        let field = schema
            .behaviors
            .get(behavior_name)
            .and_then(|behavior| behavior.mutations.get(mutation))
            .ok_or_else(|| {
                anyhow::anyhow!("schema missing behavior input {behavior_name}.{mutation}")
            })?;
        validate_field(
            &format!("behavior.{behavior_name}.{mutation}"),
            field,
            value,
        )
    }

    pub fn validate_event_payload(&self, event_name: &str, value: &Value) -> Result<()> {
        let schema = self.schema();
        let Some(event) = schema.events.get(event_name) else {
            return Ok(());
        };
        validate_field(
            &format!("events.{event_name}.payload"),
            &event.payload,
            value,
        )
    }

    fn history_dir(&self) -> PathBuf {
        self.path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("history")
    }

    fn history_path(&self, version: u32) -> PathBuf {
        self.history_dir().join(format!("v{version}.json"))
    }

    async fn persist_history_version(&self, schema: &DatabaseSchema) -> Result<()> {
        persist_schema(&self.history_path(schema.version), schema).await
    }
}

async fn persist_schema(path: &Path, schema: &DatabaseSchema) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("nextdb.schema.json");
    let tmp = path.with_file_name(format!(".{file_name}.tmp"));
    fs::write(&tmp, serde_json::to_vec_pretty(schema)?).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSchema {
    pub name: String,
    pub version: u32,
    pub objects: BTreeMap<String, ObjectSchema>,
    pub tables: BTreeMap<String, TableSchema>,
    #[serde(default)]
    pub events: BTreeMap<String, EventSchema>,
    pub behaviors: BTreeMap<String, BehaviorSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaHistoryEntry {
    pub version: u32,
    pub name: String,
    pub current: bool,
    pub object_count: usize,
    pub table_count: usize,
    pub event_count: usize,
    pub behavior_count: usize,
}

impl SchemaHistoryEntry {
    fn from_schema(schema: &DatabaseSchema, current: bool) -> Self {
        Self {
            version: schema.version,
            name: schema.name.clone(),
            current,
            object_count: schema.objects.len(),
            table_count: schema.tables.len(),
            event_count: schema.events.len(),
            behavior_count: schema.behaviors.len(),
        }
    }
}

impl DatabaseSchema {
    pub fn default_nextdb() -> Self {
        let mut objects = BTreeMap::new();
        objects.insert(
            "Object".to_string(),
            ObjectSchema {
                fields: fields([
                    (
                        "id",
                        FieldType::Id {
                            entity: "Object".into(),
                        },
                    ),
                    ("path", FieldType::String),
                    ("contentType", FieldType::String),
                    ("byteSize", FieldType::Int64),
                    ("sha256", FieldType::String),
                    ("createdAtMs", FieldType::TimeMs),
                ]),
            },
        );

        let mut nested = BTreeMap::new();
        let mut message_fields = fields([
            (
                "id",
                FieldType::Id {
                    entity: "Message".into(),
                },
            ),
            (
                "roomId",
                FieldType::Id {
                    entity: "Room".into(),
                },
            ),
            (
                "senderId",
                FieldType::Id {
                    entity: "User".into(),
                },
            ),
            ("body", FieldType::Text { inline_until: 8192 }),
            (
                "attachments",
                FieldType::List {
                    item: Box::new(FieldType::ObjectRef {
                        object: "Object".into(),
                    }),
                },
            ),
            ("createdAtMs", FieldType::TimeMs),
            ("path", FieldType::String),
        ]);
        message_fields.insert("lsn".to_string(), FieldSchema::optional(FieldType::Int64));

        nested.insert(
            "messages".to_string(),
            NestedTableSchema {
                storage: StorageClass::ChatLog {
                    bucket: "day(createdAtMs)".to_string(),
                    order: vec!["desc(createdAtMs)".to_string(), "id".to_string()],
                    live_window: 5_000,
                },
                fields: message_fields,
                read_visibility: ReadVisibilityPolicy::default(),
                indexes: indexes([(
                    "bySender",
                    IndexSchema {
                        fields: vec!["senderId".to_string()],
                        unique: false,
                    },
                )]),
            },
        );

        let mut tables = BTreeMap::new();
        tables.insert(
            "rooms".to_string(),
            TableSchema {
                storage: StorageClass::ActorPartition,
                fields: fields([
                    (
                        "id",
                        FieldType::Id {
                            entity: "Room".into(),
                        },
                    ),
                    ("title", FieldType::String),
                ]),
                nested,
                read_visibility: ReadVisibilityPolicy::default(),
                indexes: indexes([(
                    "byTitle",
                    IndexSchema {
                        fields: vec!["title".to_string()],
                        unique: false,
                    },
                )]),
            },
        );

        let mut behaviors = BTreeMap::new();
        let mut echo_send_fields = fields([
            (
                "roomId",
                FieldType::Id {
                    entity: "Room".into(),
                },
            ),
            ("body", FieldType::String),
        ]);
        echo_send_fields.insert(
            "scheduleReminder".to_string(),
            FieldSchema::optional(FieldType::String),
        );
        echo_send_fields.insert(
            "scheduleReminderDueAtMs".to_string(),
            FieldSchema::optional(FieldType::String),
        );
        let echo_mutations = fields([(
            "echo.send",
            FieldType::Object {
                fields: echo_send_fields,
            },
        )]);
        behaviors.insert(
            "echo".to_string(),
            BehaviorSchema {
                mutations: echo_mutations.clone(),
            },
        );
        behaviors.insert(
            "echo-ts".to_string(),
            BehaviorSchema {
                mutations: echo_mutations,
            },
        );

        let mut events = BTreeMap::new();
        let realtime_member_type = realtime_member_type();
        events.insert(
            "notification.created".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([("text", FieldType::String)]),
                }),
            },
        );
        events.insert(
            "presence.ping".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([("at", FieldType::TimeMs)]),
                }),
            },
        );
        events.insert(
            "realtime.channel.signal".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        (
                            "fromUserId",
                            FieldType::Id {
                                entity: "User".to_string(),
                            },
                        ),
                        (
                            "toUserId",
                            FieldType::Id {
                                entity: "User".to_string(),
                            },
                        ),
                        ("kind", FieldType::String),
                        ("payload", FieldType::Json),
                        ("sequence", FieldType::Int64),
                        ("timestampMs", FieldType::TimeMs),
                    ]),
                }),
            },
        );
        events.insert(
            "realtime.channel.event".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        (
                            "fromUserId",
                            FieldType::Id {
                                entity: "User".to_string(),
                            },
                        ),
                        ("kind", FieldType::String),
                        ("payload", FieldType::Json),
                        ("sequence", FieldType::Int64),
                        ("timestampMs", FieldType::TimeMs),
                    ]),
                }),
            },
        );
        events.insert(
            "realtime.channel.state".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        (
                            "fromUserId",
                            FieldType::Id {
                                entity: "User".to_string(),
                            },
                        ),
                        (
                            "state",
                            FieldType::Object {
                                fields: fields([
                                    (
                                        "channelId",
                                        FieldType::Id {
                                            entity: "RealtimeChannel".to_string(),
                                        },
                                    ),
                                    ("version", FieldType::Int64),
                                    ("state", FieldType::Json),
                                    ("updatedAtMs", FieldType::TimeMs),
                                ]),
                            },
                        ),
                        ("sequence", FieldType::Int64),
                        ("timestampMs", FieldType::TimeMs),
                    ]),
                }),
            },
        );
        events.insert(
            "realtime.channel.memberJoined".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        ("member", realtime_member_type.clone()),
                    ]),
                }),
            },
        );
        events.insert(
            "realtime.channel.memberLeft".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        (
                            "members",
                            FieldType::List {
                                item: Box::new(realtime_member_type.clone()),
                            },
                        ),
                    ]),
                }),
            },
        );
        events.insert(
            "realtime.channel.memberUpdated".to_string(),
            EventSchema {
                payload: FieldSchema::required(FieldType::Object {
                    fields: fields([
                        (
                            "channelId",
                            FieldType::Id {
                                entity: "RealtimeChannel".to_string(),
                            },
                        ),
                        ("member", realtime_member_type),
                        ("sequence", FieldType::Int64),
                        ("timestampMs", FieldType::TimeMs),
                    ]),
                }),
            },
        );

        Self {
            name: "nextdb".to_string(),
            version: 1,
            objects,
            tables,
            events,
            behaviors,
        }
    }

    pub fn storage_policy_report(&self) -> SchemaStoragePolicyReport {
        let mut entries = Vec::new();
        for (table_name, table) in &self.tables {
            entries.push(StoragePolicyEntry {
                path: format!("tables.{table_name}"),
                storage: table.storage.clone(),
                physical_role: match &table.storage {
                    StorageClass::ActorPartition
                    | StorageClass::Resident
                    | StorageClass::Lru { .. } => "actor".to_string(),
                    StorageClass::Disk => "disk".to_string(),
                    StorageClass::ChatLog { .. } => "chatLog".to_string(),
                    StorageClass::Object => "object".to_string(),
                },
            });
            for (nested_name, nested) in &table.nested {
                entries.push(StoragePolicyEntry {
                    path: format!("tables.{table_name}.nested.{nested_name}"),
                    storage: nested.storage.clone(),
                    physical_role: match &nested.storage {
                        StorageClass::ActorPartition
                        | StorageClass::Resident
                        | StorageClass::Lru { .. } => "actor".to_string(),
                        StorageClass::Disk => "disk".to_string(),
                        StorageClass::ChatLog { .. } => "chatLog".to_string(),
                        StorageClass::Object => "object".to_string(),
                    },
                });
            }
        }

        SchemaStoragePolicyReport { entries }
    }

    pub fn message_live_window(&self) -> Option<usize> {
        match &self.tables.get("rooms")?.nested.get("messages")?.storage {
            StorageClass::ChatLog { live_window, .. } => Some(*live_window),
            _ => None,
        }
    }

    pub fn room_lru_max_items(&self) -> Option<usize> {
        match &self.tables.get("rooms")?.storage {
            StorageClass::Lru { max_items } => Some(*max_items),
            _ => None,
        }
    }

    pub fn validate_message_draft(&self, draft: &MessageDraft) -> Result<()> {
        let messages = self
            .tables
            .get("rooms")
            .and_then(|table| table.nested.get("messages"))
            .ok_or_else(|| anyhow::anyhow!("schema missing rooms.messages nested table"))?;
        validate_message_draft_fields("rooms.messages", &messages.fields, draft)
    }

    pub fn validate_table_record(&self, table_name: &str, value: &Value) -> Result<()> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("schema missing table {table_name}"))?;
        validate_object_fields(&format!("tables.{table_name}"), &table.fields, value)
    }

    pub fn validate_nested_table_record(
        &self,
        table_name: &str,
        nested_name: &str,
        value: &Value,
    ) -> Result<()> {
        let nested = self
            .tables
            .get(table_name)
            .and_then(|table| table.nested.get(nested_name))
            .ok_or_else(|| {
                anyhow::anyhow!("schema missing nested table {table_name}.{nested_name}")
            })?;
        validate_object_fields(
            &format!("tables.{table_name}.nested.{nested_name}"),
            &nested.fields,
            value,
        )
    }

    pub fn record_table_accepts_volatile(&self, table_name: &str) -> Result<bool> {
        let storage = if let Some(table) = self.tables.get(table_name) {
            &table.storage
        } else if let Some((parent_table, nested_table)) = table_name.split_once('.') {
            &self
                .tables
                .get(parent_table)
                .and_then(|table| table.nested.get(nested_table))
                .ok_or_else(|| anyhow::anyhow!("schema missing table {table_name}"))?
                .storage
        } else {
            bail!("schema missing table {table_name}");
        };
        Ok(matches!(
            storage,
            StorageClass::ActorPartition | StorageClass::Resident | StorageClass::Lru { .. }
        ))
    }

    pub fn validation_report(&self) -> SchemaValidationReport {
        let mut errors = Vec::new();

        if self.name.trim().is_empty() {
            errors.push("schema.name is required".to_string());
        }
        if !self.objects.contains_key("Object") {
            errors.push("objects.Object is required for ObjectRef fields".to_string());
        }
        for (object_name, object) in &self.objects {
            validate_field_map(
                &format!("objects.{object_name}.fields"),
                &object.fields,
                self,
                &mut errors,
            );
        }
        if let Some(messages) = self
            .tables
            .get("rooms")
            .and_then(|table| table.nested.get("messages"))
        {
            for field in [
                "id",
                "roomId",
                "senderId",
                "body",
                "attachments",
                "createdAtMs",
                "path",
            ] {
                if !messages.fields.contains_key(field) {
                    errors.push(format!(
                        "tables.rooms.nested.messages.fields.{field} is required"
                    ));
                }
            }
        }
        for (behavior_name, behavior) in &self.behaviors {
            if behavior.mutations.is_empty() {
                errors.push(format!(
                    "behaviors.{behavior_name}.mutations must not be empty"
                ));
            }
            validate_field_map(
                &format!("behaviors.{behavior_name}.mutations"),
                &behavior.mutations,
                self,
                &mut errors,
            );
        }
        for (event_name, event) in &self.events {
            if event_name.trim().is_empty() {
                errors.push("events contains an empty event name".to_string());
            }
            validate_field_schema(
                &format!("events.{event_name}.payload.type"),
                &event.payload.field_type,
                self,
                &mut errors,
            );
        }
        for (table_name, table) in &self.tables {
            validate_field_map(
                &format!("tables.{table_name}.fields"),
                &table.fields,
                self,
                &mut errors,
            );
            validate_storage_class(
                &format!("tables.{table_name}.storage"),
                &table.fields,
                &table.storage,
                &mut errors,
            );
            validate_read_visibility_policy(
                &format!("tables.{table_name}.readVisibility"),
                &table.fields,
                &table.read_visibility,
                &mut errors,
            );
            for (index_name, index) in &table.indexes {
                if index.fields.is_empty() {
                    errors.push(format!(
                        "tables.{table_name}.indexes.{index_name}.fields must not be empty"
                    ));
                }
                for field in &index.fields {
                    if !table.fields.contains_key(field) {
                        errors.push(format!(
                            "tables.{table_name}.indexes.{index_name}.fields references missing field {field}"
                        ));
                    }
                }
            }
            for (nested_name, nested) in &table.nested {
                validate_field_map(
                    &format!("tables.{table_name}.nested.{nested_name}.fields"),
                    &nested.fields,
                    self,
                    &mut errors,
                );
                validate_storage_class(
                    &format!("tables.{table_name}.nested.{nested_name}.storage"),
                    &nested.fields,
                    &nested.storage,
                    &mut errors,
                );
                validate_read_visibility_policy(
                    &format!("tables.{table_name}.nested.{nested_name}.readVisibility"),
                    &nested.fields,
                    &nested.read_visibility,
                    &mut errors,
                );
                for (index_name, index) in &nested.indexes {
                    if index.fields.is_empty() {
                        errors.push(format!(
                            "tables.{table_name}.nested.{nested_name}.indexes.{index_name}.fields must not be empty"
                        ));
                    }
                    for field in &index.fields {
                        if !nested.fields.contains_key(field) {
                            errors.push(format!(
                                "tables.{table_name}.nested.{nested_name}.indexes.{index_name}.fields references missing field {field}"
                            ));
                        }
                    }
                }
            }
        }

        SchemaValidationReport {
            ok: errors.is_empty(),
            errors,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaStoragePolicyReport {
    pub entries: Vec<StoragePolicyEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoragePolicyEntry {
    pub path: String,
    pub storage: StorageClass,
    pub physical_role: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaValidationReport {
    pub ok: bool,
    pub errors: Vec<String>,
}

impl SchemaValidationReport {
    pub fn into_result(self) -> Result<()> {
        if self.ok {
            Ok(())
        } else {
            bail!("invalid schema: {}", self.errors.join("; "))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaMigrationPlan {
    pub from_version: u32,
    pub to_version: u32,
    pub compatible: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub requires_replay_rebuild: bool,
    #[serde(default)]
    pub replay_safe_breaking_changes: Vec<String>,
    #[serde(default)]
    pub unsafe_breaking_changes: Vec<String>,
    #[serde(default)]
    pub projection_rebuild_required: bool,
    #[serde(default)]
    pub projection_rebuild_reasons: Vec<String>,
}

impl SchemaMigrationPlan {
    pub fn between(current: &DatabaseSchema, candidate: &DatabaseSchema) -> Self {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut replay_safe_breaking_changes = Vec::new();
        let mut unsafe_breaking_changes = Vec::new();
        let mut projection_rebuild_reasons = Vec::new();

        if candidate.version < current.version {
            push_unsafe_schema_change(
                format!(
                    "schema version cannot decrease from {} to {}",
                    current.version, candidate.version
                ),
                &mut errors,
                &mut unsafe_breaking_changes,
            );
        }
        if candidate.version == current.version && candidate != current {
            warnings.push(format!(
                "schema content changed without version bump at version {}",
                current.version
            ));
        }

        for (table_name, table) in &current.tables {
            let Some(candidate_table) = candidate.tables.get(table_name) else {
                push_replay_safe_schema_change(
                    format!("table {table_name} cannot be removed"),
                    &mut errors,
                    &mut replay_safe_breaking_changes,
                );
                continue;
            };
            check_field_changes(
                &format!("tables.{table_name}.fields"),
                &table.fields,
                &candidate_table.fields,
                &mut errors,
                &mut replay_safe_breaking_changes,
                &mut unsafe_breaking_changes,
                &mut projection_rebuild_reasons,
            );
            check_record_projection_shape_changes(
                &format!("tables.{table_name}"),
                &table.storage,
                &candidate_table.storage,
                &table.indexes,
                &candidate_table.indexes,
                &mut projection_rebuild_reasons,
            );
            for (nested_name, nested) in &table.nested {
                let Some(candidate_nested) = candidate_table.nested.get(nested_name) else {
                    push_replay_safe_schema_change(
                        format!("nested table {table_name}.{nested_name} cannot be removed"),
                        &mut errors,
                        &mut replay_safe_breaking_changes,
                    );
                    continue;
                };
                check_field_changes(
                    &format!("tables.{table_name}.nested.{nested_name}.fields"),
                    &nested.fields,
                    &candidate_nested.fields,
                    &mut errors,
                    &mut replay_safe_breaking_changes,
                    &mut unsafe_breaking_changes,
                    &mut projection_rebuild_reasons,
                );
                check_record_projection_shape_changes(
                    &format!("tables.{table_name}.nested.{nested_name}"),
                    &nested.storage,
                    &candidate_nested.storage,
                    &nested.indexes,
                    &candidate_nested.indexes,
                    &mut projection_rebuild_reasons,
                );
            }
        }

        for (object_name, object) in &current.objects {
            let Some(candidate_object) = candidate.objects.get(object_name) else {
                push_replay_safe_schema_change(
                    format!("object {object_name} cannot be removed"),
                    &mut errors,
                    &mut replay_safe_breaking_changes,
                );
                continue;
            };
            check_field_changes(
                &format!("objects.{object_name}.fields"),
                &object.fields,
                &candidate_object.fields,
                &mut errors,
                &mut replay_safe_breaking_changes,
                &mut unsafe_breaking_changes,
                &mut projection_rebuild_reasons,
            );
        }

        for (event_name, event) in &current.events {
            let Some(candidate_event) = candidate.events.get(event_name) else {
                push_replay_safe_schema_change(
                    format!("event {event_name} cannot be removed"),
                    &mut errors,
                    &mut replay_safe_breaking_changes,
                );
                continue;
            };
            check_field_shape_change(
                &format!("events.{event_name}.payload"),
                &event.payload,
                &candidate_event.payload,
                &mut errors,
                &mut unsafe_breaking_changes,
            );
        }

        for (behavior_name, behavior) in &current.behaviors {
            let Some(candidate_behavior) = candidate.behaviors.get(behavior_name) else {
                push_replay_safe_schema_change(
                    format!("behavior {behavior_name} cannot be removed"),
                    &mut errors,
                    &mut replay_safe_breaking_changes,
                );
                continue;
            };
            check_behavior_mutation_changes(
                &format!("behaviors.{behavior_name}.mutations"),
                &behavior.mutations,
                &candidate_behavior.mutations,
                &mut errors,
                &mut unsafe_breaking_changes,
            );
        }

        Self {
            from_version: current.version,
            to_version: candidate.version,
            compatible: errors.is_empty(),
            errors,
            warnings,
            requires_replay_rebuild: !replay_safe_breaking_changes.is_empty(),
            replay_safe_breaking_changes,
            unsafe_breaking_changes,
            projection_rebuild_required: !projection_rebuild_reasons.is_empty(),
            projection_rebuild_reasons,
        }
    }

    pub fn can_replay_rebuild(&self) -> bool {
        !self.compatible && self.requires_replay_rebuild && self.unsafe_breaking_changes.is_empty()
    }

    pub fn into_result(self) -> Result<()> {
        if self.compatible {
            Ok(())
        } else {
            bail!("incompatible schema migration: {}", self.errors.join("; "))
        }
    }
}

fn push_replay_safe_schema_change(
    message: String,
    errors: &mut Vec<String>,
    replay_safe_breaking_changes: &mut Vec<String>,
) {
    errors.push(message.clone());
    replay_safe_breaking_changes.push(message);
}

fn push_unsafe_schema_change(
    message: String,
    errors: &mut Vec<String>,
    unsafe_breaking_changes: &mut Vec<String>,
) {
    errors.push(message.clone());
    unsafe_breaking_changes.push(message);
}

fn check_field_changes(
    path: &str,
    current: &BTreeMap<String, FieldSchema>,
    candidate: &BTreeMap<String, FieldSchema>,
    errors: &mut Vec<String>,
    replay_safe_breaking_changes: &mut Vec<String>,
    unsafe_breaking_changes: &mut Vec<String>,
    projection_rebuild_reasons: &mut Vec<String>,
) {
    for (field_name, field) in current {
        let Some(candidate_field) = candidate.get(field_name) else {
            push_replay_safe_schema_change(
                format!("{path}.{field_name} cannot be removed"),
                errors,
                replay_safe_breaking_changes,
            );
            push_projection_rebuild_reason(
                format!("{path}.{field_name} removed"),
                projection_rebuild_reasons,
            );
            continue;
        };
        check_field_shape_change(
            &format!("{path}.{field_name}"),
            field,
            candidate_field,
            errors,
            unsafe_breaking_changes,
        );
    }
}

fn check_behavior_mutation_changes(
    path: &str,
    current: &BTreeMap<String, FieldSchema>,
    candidate: &BTreeMap<String, FieldSchema>,
    errors: &mut Vec<String>,
    unsafe_breaking_changes: &mut Vec<String>,
) {
    for (mutation_name, mutation) in current {
        let Some(candidate_mutation) = candidate.get(mutation_name) else {
            push_unsafe_schema_change(
                format!("{path}.{mutation_name} cannot be removed"),
                errors,
                unsafe_breaking_changes,
            );
            continue;
        };
        check_field_shape_change(
            &format!("{path}.{mutation_name}"),
            mutation,
            candidate_mutation,
            errors,
            unsafe_breaking_changes,
        );
    }
}

fn check_field_shape_change(
    path: &str,
    current: &FieldSchema,
    candidate: &FieldSchema,
    errors: &mut Vec<String>,
    unsafe_breaking_changes: &mut Vec<String>,
) {
    check_field_type_shape_change(
        path,
        &current.field_type,
        &candidate.field_type,
        errors,
        unsafe_breaking_changes,
    );
    if current.optional != candidate.optional {
        push_unsafe_schema_change(
            format!("{path} optional flag cannot change"),
            errors,
            unsafe_breaking_changes,
        );
    }
}

fn check_field_type_shape_change(
    path: &str,
    current: &FieldType,
    candidate: &FieldType,
    errors: &mut Vec<String>,
    unsafe_breaking_changes: &mut Vec<String>,
) {
    match (current, candidate) {
        (
            FieldType::Object { fields },
            FieldType::Object {
                fields: candidate_fields,
            },
        ) => {
            for (field_name, field) in fields {
                let Some(candidate_field) = candidate_fields.get(field_name) else {
                    push_unsafe_schema_change(
                        format!("{path}.fields.{field_name} cannot be removed"),
                        errors,
                        unsafe_breaking_changes,
                    );
                    continue;
                };
                check_field_shape_change(
                    &format!("{path}.fields.{field_name}"),
                    field,
                    candidate_field,
                    errors,
                    unsafe_breaking_changes,
                );
            }
            for (field_name, field) in candidate_fields {
                if !fields.contains_key(field_name) && !field.optional {
                    push_unsafe_schema_change(
                        format!("{path}.fields.{field_name} required field cannot be added"),
                        errors,
                        unsafe_breaking_changes,
                    );
                }
            }
        }
        _ if current == candidate => {}
        _ => push_unsafe_schema_change(
            format!("{path} type cannot change"),
            errors,
            unsafe_breaking_changes,
        ),
    }
}

fn check_record_projection_shape_changes(
    path: &str,
    current_storage: &StorageClass,
    candidate_storage: &StorageClass,
    current_indexes: &BTreeMap<String, IndexSchema>,
    candidate_indexes: &BTreeMap<String, IndexSchema>,
    projection_rebuild_reasons: &mut Vec<String>,
) {
    if current_storage != candidate_storage {
        push_projection_rebuild_reason(
            format!("{path}.storage changed"),
            projection_rebuild_reasons,
        );
    }
    if current_indexes != candidate_indexes {
        push_projection_rebuild_reason(
            format!("{path}.indexes changed"),
            projection_rebuild_reasons,
        );
    }
}

fn push_projection_rebuild_reason(reason: String, projection_rebuild_reasons: &mut Vec<String>) {
    if !projection_rebuild_reasons.contains(&reason) {
        projection_rebuild_reasons.push(reason);
    }
}

fn fields<const N: usize>(entries: [(&str, FieldType); N]) -> BTreeMap<String, FieldSchema> {
    entries
        .into_iter()
        .map(|(name, field_type)| (name.to_string(), FieldSchema::required(field_type)))
        .collect()
}

fn indexes<const N: usize>(entries: [(&str, IndexSchema); N]) -> BTreeMap<String, IndexSchema> {
    entries
        .into_iter()
        .map(|(name, index)| (name.to_string(), index))
        .collect()
}

fn realtime_member_type() -> FieldType {
    let mut fields = fields([
        (
            "userId",
            FieldType::Id {
                entity: "User".to_string(),
            },
        ),
        ("metadata", FieldType::Json),
        ("joinedAtMs", FieldType::TimeMs),
        ("updatedAtMs", FieldType::TimeMs),
    ]);
    fields.insert(
        "sessionId".to_string(),
        FieldSchema::optional(FieldType::String),
    );
    FieldType::Object { fields }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectSchema {
    pub fields: BTreeMap<String, FieldSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub storage: StorageClass,
    pub fields: BTreeMap<String, FieldSchema>,
    #[serde(default)]
    pub nested: BTreeMap<String, NestedTableSchema>,
    #[serde(default, skip_serializing_if = "ReadVisibilityPolicy::is_public")]
    pub read_visibility: ReadVisibilityPolicy,
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSchema {
    pub fields: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NestedTableSchema {
    pub storage: StorageClass,
    pub fields: BTreeMap<String, FieldSchema>,
    #[serde(default, skip_serializing_if = "ReadVisibilityPolicy::is_public")]
    pub read_visibility: ReadVisibilityPolicy,
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexSchema>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadVisibilityPolicy {
    #[serde(default)]
    pub all: Vec<ReadVisibilityRule>,
}

impl ReadVisibilityPolicy {
    pub fn is_public(&self) -> bool {
        self.all.is_empty()
    }

    pub fn allows_value_for_user(&self, value: &Value, user_id: Option<&str>) -> bool {
        if self.is_public() {
            return true;
        }
        let Some(user_id) = user_id else {
            return false;
        };
        self.all.iter().all(|rule| rule.allows(value, user_id))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ReadVisibilityRule {
    FieldEqualsUserId { field: String },
}

impl ReadVisibilityRule {
    fn field(&self) -> &str {
        match self {
            Self::FieldEqualsUserId { field } => field,
        }
    }

    fn allows(&self, value: &Value, user_id: &str) -> bool {
        match self {
            Self::FieldEqualsUserId { field } => value
                .as_object()
                .and_then(|object| object.get(field))
                .and_then(Value::as_str)
                .is_some_and(|value| value == user_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviorSchema {
    pub mutations: BTreeMap<String, FieldSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSchema {
    pub payload: FieldSchema,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSchema {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(default)]
    pub optional: bool,
}

impl FieldSchema {
    pub fn required(field_type: FieldType) -> Self {
        Self {
            field_type,
            optional: false,
        }
    }

    pub fn optional(field_type: FieldType) -> Self {
        Self {
            field_type,
            optional: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FieldType {
    String,
    Text {
        inline_until: usize,
    },
    Int64,
    TimeMs,
    Boolean,
    Id {
        entity: String,
    },
    ObjectRef {
        object: String,
    },
    List {
        item: Box<FieldType>,
    },
    Object {
        fields: BTreeMap<String, FieldSchema>,
    },
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StorageClass {
    ActorPartition,
    Resident,
    Lru {
        max_items: usize,
    },
    Disk,
    ChatLog {
        bucket: String,
        order: Vec<String>,
        live_window: usize,
    },
    Object,
}

fn generate_typescript(schema: &DatabaseSchema) -> String {
    let mut out = String::new();
    out.push_str("// Generated by NextDB schema registry. Do not edit by hand.\n\n");
    out.push_str("export type Id<T extends string> = string & { readonly __entity: T }\n\n");

    for (name, object) in &schema.objects {
        out.push_str(&format!("export interface {} {{\n", pascal(name)));
        push_fields(&mut out, &object.fields);
        out.push_str("}\n\n");
    }

    for (table_name, table) in &schema.tables {
        out.push_str(&format!("export interface {} {{\n", pascal(table_name)));
        push_fields(&mut out, &table.fields);
        out.push_str("}\n\n");

        for (nested_name, nested) in &table.nested {
            out.push_str(&format!(
                "export interface {}{} {{\n",
                pascal(table_name),
                pascal(nested_name)
            ));
            push_fields(&mut out, &nested.fields);
            out.push_str("}\n\n");
        }
    }

    for (behavior_name, behavior) in &schema.behaviors {
        let prefix = pascal_identifier(behavior_name);
        for (mutation, field) in &behavior.mutations {
            out.push_str(&format!(
                "export type {}{}Input = {}\n\n",
                prefix,
                pascal_identifier(mutation),
                ts_type(&field.field_type)
            ));
        }
    }
    for (event_name, event) in &schema.events {
        out.push_str(&format!(
            "export type {}EventPayload = {}\n\n",
            pascal_identifier(event_name),
            ts_type(&event.payload.field_type)
        ));
    }

    push_typed_client_bindings(&mut out, schema);

    out
}

fn push_typed_client_bindings(out: &mut String, schema: &DatabaseSchema) {
    out.push_str("export interface NextDbTables {\n");
    for table_name in schema.tables.keys() {
        out.push_str(&format!(
            "  {}: {}\n",
            ts_property_name(table_name),
            pascal(table_name)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbObjects {\n");
    for object_name in schema.objects.keys() {
        out.push_str(&format!(
            "  {}: {}\n",
            ts_property_name(object_name),
            pascal(object_name)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbNestedTables {\n");
    for (table_name, table) in &schema.tables {
        out.push_str(&format!("  {}: {{\n", ts_property_name(table_name)));
        for nested_name in table.nested.keys() {
            out.push_str(&format!(
                "    {}: {}{}\n",
                ts_property_name(nested_name),
                pascal(table_name),
                pascal(nested_name)
            ));
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbBehaviors {\n");
    for (behavior_name, behavior) in &schema.behaviors {
        out.push_str(&format!("  {}: {{\n", ts_property_name(behavior_name)));
        let prefix = pascal_identifier(behavior_name);
        for mutation in behavior.mutations.keys() {
            out.push_str(&format!(
                "    {}: {}{}Input\n",
                ts_property_name(mutation),
                prefix,
                pascal_identifier(mutation)
            ));
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbEvents {\n");
    for event_name in schema.events.keys() {
        out.push_str(&format!(
            "  {}: {}EventPayload\n",
            ts_property_name(event_name),
            pascal_identifier(event_name)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbTableIndexes {\n");
    for (table_name, table) in &schema.tables {
        out.push_str(&format!(
            "  {}: {}\n",
            ts_property_name(table_name),
            ts_string_union(table.indexes.keys())
        ));
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbTableIndexValues {\n");
    for (table_name, table) in &schema.tables {
        out.push_str(&format!("  {}: {{\n", ts_property_name(table_name)));
        for (index_name, index) in &table.indexes {
            out.push_str(&format!(
                "    {}: {}\n",
                ts_property_name(index_name),
                ts_index_value_type(index, &table.fields)
            ));
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbNestedTableIndexes {\n");
    for (table_name, table) in &schema.tables {
        out.push_str(&format!("  {}: {{\n", ts_property_name(table_name)));
        for (nested_name, nested) in &table.nested {
            out.push_str(&format!(
                "    {}: {}\n",
                ts_property_name(nested_name),
                ts_string_union(nested.indexes.keys())
            ));
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");

    out.push_str("export interface NextDbNestedTableIndexValues {\n");
    for (table_name, table) in &schema.tables {
        out.push_str(&format!("  {}: {{\n", ts_property_name(table_name)));
        for (nested_name, nested) in &table.nested {
            out.push_str(&format!("    {}: {{\n", ts_property_name(nested_name)));
            for (index_name, index) in &nested.indexes {
                out.push_str(&format!(
                    "      {}: {}\n",
                    ts_property_name(index_name),
                    ts_index_value_type(index, &nested.fields)
                ));
            }
            out.push_str("    }\n");
        }
        out.push_str("  }\n");
    }
    out.push_str("}\n\n");

    out.push_str(&format!(
        "export const NEXTDB_SCHEMA_VERSION = {} as const\n\n",
        schema.version
    ));

    out.push_str(
        r#"export type NextDbDurability = "strict" | "relaxed"

export type NextDbConnectionTransport = "webSocket" | "webTransport" | "custom"

export interface NextDbRuntimeDrainState {
  draining: boolean
  reason?: string
  updatedAtMs?: number
}

export interface NextDbRuntimeWriteState {
  inFlight: number
  lastStartedAtMs?: number
  lastFinishedAtMs?: number
}

export interface NextDbConnectionLayerCapabilities {
  protocol: string
  frameEncoding: string
  connectPath: string
  supportedTransports: NextDbConnectionTransport[]
  defaultTransport: NextDbConnectionTransport
  webSocket: { supported: boolean; connectPath?: string | null }
  webTransport: { supported: boolean; connectPath?: string | null }
  custom: { supported: boolean; connectPath?: string | null }
}

export interface NextDbHealth {
  ok: boolean
  runtimeId: string
  draining: boolean
  acceptingWrites: boolean
  runtimeDrain: NextDbRuntimeDrainState
  runtimeWrites: NextDbRuntimeWriteState
  actorKernel: NextDbActorKernelStatus
  currentLsn: number
  nodeId: string
  connectionCount: number
  connectedUsers: number
  connectionLayer: NextDbConnectionLayerCapabilities
  [key: string]: unknown
}

export type NextDbRecordHotStorageClass =
  | { kind: "actorPartition" }
  | { kind: "resident" }
  | { kind: "lru"; maxItems: number }

export interface NextDbRecordHotTableStatus {
  table: string
  storage: NextDbRecordHotStorageClass
  maxItems: number | null
  records: number
  volatileRecords: number
}

export interface NextDbRecordHotCacheStatus {
  tables: NextDbRecordHotTableStatus[]
  tableCount: number
  recordCount: number
  volatileRecords: number
  durableIdleTtlMs: number
  durableIdleLastSweepAtMs?: number | null
  durableIdleLastEvicted: number
  durableIdleTotalEvicted: number
}

export interface NextDbActorIdleMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastEvicted: number
  totalEvicted: number
}

export interface NextDbActorSplitMaintenanceStatus {
  lastSweepAtMs?: number | null
  lastProcessed: number
  totalProcessed: number
}

export interface NextDbRuntimeRecordActivationOptions<T extends NextDbTableName = NextDbTableName> {
  table: T
  parentKey?: NextDbTableKey<T>
  nested?: NextDbNestedTableName<T>
  key?: NextDbTableKey<T> | string
  keys?: Array<NextDbTableKey<T> | string>
  afterKey?: NextDbTableKey<T> | string
  order?: "key" | "schema"
  limit?: number
}

export interface NextDbRuntimeRecordActivationResponse<T extends NextDbTableName = NextDbTableName> {
  table: T | string
  parentKey?: NextDbTableKey<T>
  nested?: NextDbNestedTableName<T>
  requested: number
  found: number
  activated: number
  evicted: number
  actorScope?: NextDbScopeRowsActivationResult | null
  actorScopes: NextDbScopeRowsActivationResult[]
  before: NextDbRecordHotCacheStatus
  after: NextDbRecordHotCacheStatus
}

export interface NextDbRuntimeRoomActivationOptions<R extends NextDbRoomId = NextDbRoomId> {
  roomId: R
  limit?: number
}

export interface NextDbRuntimeRoomActivationResponse<R extends NextDbRoomId = NextDbRoomId> {
  roomId: R
  requested: number
  found: number
  activated: boolean
  evicted: boolean
  beforeRoomCount: number
  afterRoomCount: number
  source: "live" | "chatLog" | "missing"
}

export interface NextDbRuntimeRoomStatus<R extends NextDbRoomId = NextDbRoomId> {
  roomId: R
  messages: number
  oldestLsn?: number
  newestLsn?: number
  lastAccessedMs: number
}

export type NextDbActorKind = "room" | "scope" | "table" | "view" | "aggregate"

export interface NextDbActorId {
  kind: NextDbActorKind
  key: string
}

export interface NextDbActorKernelStatus {
  totalActors: number
  roomActors: number
  kernelActors: number
  scopeRows: number
  scopeBytes: number
  scopeSubscriptionRefCount: number
  subscribedScopes: number
  lingeringScopes: number
  l1ScopeActors: number
  l3ScopeActors: number
  tableScopes: number
  tablePendingSplits: number
  kindCounts: Record<string, number>
  oldestAccessedMs?: number | null
  newestAccessedMs?: number | null
}

export interface NextDbScopeRowsActivationResult {
  actorId: NextDbActorId
  tableActorId: NextDbActorId
  shardIndex: number
  created: boolean
  requested: number
  inserted: number
  updated: number
  rows: number
  bytes: number
  tableScopes: number
  tablePendingSplits: number
  scopeSplitPending: boolean
  scopeSplitRows: number
  scopeSplitBytes: number
  turnCount: number
  lastAccessedMs: number
}

export interface NextDbRuntimeActorActivationOptions {
  kind: NextDbActorKind
  key: string
}

export interface NextDbRuntimeActorActivationResponse {
  actorId: NextDbActorId
  shardIndex: number
  activated: boolean
  turnCount: number
  lastAccessedMs: number
  before: NextDbActorKernelStatus
  after: NextDbActorKernelStatus
}

export interface NextDbRuntimeActivationStatusResponse {
  rooms: Array<NextDbRuntimeRoomStatus>
  roomCount: number
  actorKernel: NextDbActorKernelStatus
  maxHotRooms: number
  hotWindow: number
  hotRoomIdleTtlMs: number
  hotRoomMaintenanceIntervalMs: number
  hotRoomIdleMaintenance: NextDbActorIdleMaintenanceStatus
  actorSplitMaintenanceIntervalMs: number
  actorSplitMaintenanceLimit: number
  actorSplitMaintenance: NextDbActorSplitMaintenanceStatus
  recordHotMaintenanceIntervalMs: number
  recordHotCache: NextDbRecordHotCacheStatus
}

export interface NextDbReadinessCheck {
  name: string
  ok: boolean
  detail: string
}

export interface NextDbReadiness {
  ok: boolean
  readReady: boolean
  writeReady: boolean
  realtimeReady: boolean
  acceptingWrites: boolean
  draining: boolean
  runtimeDrain: NextDbRuntimeDrainState
  runtimeWrites: NextDbRuntimeWriteState
  currentLsn: number
  runtimeId: string
  nodeId: string
  walShardCount: number
  localWritableShards: number
  checkedAtMs: number
  checks: NextDbReadinessCheck[]
}

export type NextDbTableName = keyof NextDbTables & string
export type NextDbTableRecord<T extends NextDbTableName> = NextDbTables[T]
export type NextDbTableKey<T extends NextDbTableName> = NextDbTables[T] extends { id: infer K extends string }
  ? K
  : string
export type NextDbObjectName = keyof NextDbObjects & string
export type NextDbObjectMetadata<O extends NextDbObjectName> = NextDbObjects[O]
export type NextDbObjectId<O extends NextDbObjectName> = NextDbObjects[O] extends { id: infer K extends string }
  ? K
  : string
export type NextDbNestedTableName<T extends NextDbTableName> = keyof NextDbNestedTables[T] & string
export type NextDbNestedRecord<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
> = NextDbNestedTables[T][N]
export type NextDbNestedKey<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
> = NextDbNestedTables[T][N] extends { id: infer K extends string } ? K : string
export type NextDbTableIndexName<T extends NextDbTableName> = NextDbTableIndexes[T] & string
export type NextDbNestedIndexName<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
> = N extends keyof NextDbNestedTableIndexes[T] ? NextDbNestedTableIndexes[T][N] & string : never
export type NextDbTableIndexValue<
  T extends NextDbTableName,
  I extends NextDbTableIndexName<T>,
> = I extends keyof NextDbTableIndexValues[T] ? NextDbTableIndexValues[T][I] : never
export type NextDbNestedIndexValue<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
  I extends NextDbNestedIndexName<T, N>,
> = N extends keyof NextDbNestedTableIndexValues[T]
  ? I extends keyof NextDbNestedTableIndexValues[T][N]
    ? NextDbNestedTableIndexValues[T][N][I]
    : never
  : never
export type NextDbBehaviorName = keyof NextDbBehaviors & string
export type NextDbBehaviorMutationName<B extends NextDbBehaviorName> = keyof NextDbBehaviors[B] & string
export type NextDbBehaviorInput<
  B extends NextDbBehaviorName,
  M extends NextDbBehaviorMutationName<B>,
> = NextDbBehaviors[B][M]

export interface NextDbUpsertOptions {
  durability?: NextDbDurability
  expectedLsn?: number
  clientMutationId?: string
}

export interface NextDbDeleteOptions {
  durability?: NextDbDurability
  expectedLsn?: number
  clientMutationId?: string
}

export interface NextDbRecordTransactionOptions {
  durability?: Exclude<NextDbDurability, "volatile">
  clientMutationId?: string
}

export interface NextDbUpsertManyRecordItem<T, K extends string = string> {
  key: K
  value: T
  expectedLsn?: number
}

export interface NextDbSubscriptionOptions {
  catchUp?: boolean
  catchUpLimit?: number
}

export type NextDbCacheSnapshotSource = "mutation" | "realtime" | "sync" | "offline" | "cacheInvalidation" | "manual" | "cache"

export interface NextDbWatchOptions extends NextDbSubscriptionOptions {
  limit?: number
  immediate?: boolean
}

export interface NextDbClientCacheProfile {
  version: number
  leaseTtlMs: number
  maxObjects: number
  maxObjectBytes: number
  maxRoomMessages: number
  maxUserEvents: number
  maxRecordsPerTable: number
  maxNestedPartitions: number
  maxPendingWrites: number
  maxPendingWriteBytes: number
  offlineWrites: boolean
}

export interface NextDbCacheStats {
  totalObjects: number
  totalObjectBytes: number
  totalObjectCachedBytes: number
  totalObjectRangeChunks: number
  totalMessages: number
  totalUserEvents: number
  totalUserProfiles: number
  totalRecords: number
  rooms: Record<string, number>
  users: Record<string, number>
  tables: Record<string, number>
  nestedTables: Record<string, Record<string, number>>
}

export interface NextDbObjectCacheCoverage {
  objects: number
  byteSize: number
  cachedByteSize: number
  rangeChunks: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRoomCacheCoverage {
  messages: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbUserCacheCoverage {
  events: number
  profile: boolean
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRecordCacheCoverage {
  records: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbNestedRecordCacheCoverage {
  records: number
  cursor: number
  pendingWrites: number
  activeSubscription: boolean
  persistentSubscription: boolean
  storedSubscription: boolean
}

export interface NextDbRealtimeChannelCacheCoverage {
  stateVersion?: number
  stateUpdatedAtMs?: number
  members: number
  membersUpdatedAtMs?: number
  recentEvents: number
  latestEventSequence?: number
  latestEventTimestampMs?: number
  recentSignals: number
  latestSignalSequence?: number
  latestSignalTimestampMs?: number
  activeSubscription: boolean
}

export interface NextDbCacheCoverage {
  globalCursor: number
  objects: NextDbObjectCacheCoverage
  rooms: Record<string, NextDbRoomCacheCoverage>
  users: Record<string, NextDbUserCacheCoverage>
  tables: Record<string, NextDbRecordCacheCoverage>
  nestedTables: Record<string, Record<string, NextDbNestedRecordCacheCoverage>>
  realtimeChannels: Record<string, NextDbRealtimeChannelCacheCoverage>
}

export interface NextDbPendingWriteSummary {
  id: string
  type: string
  createdAtMs: number
  attempts: number
  lastError?: string
  clientMutationId?: string
  [key: string]: unknown
}

export interface NextDbPendingWriteStats {
  total: number
  byType: Record<string, number>
  estimatedBytes: number
  objectPutBytes: number
  failed: number
  totalAttempts: number
  oldestCreatedAtMs?: number
  newestCreatedAtMs?: number
  maxWrites: number
  maxBytes: number
  overMaxWrites: boolean
  overMaxBytes: boolean
}

export interface NextDbPendingWriteQueueStatus {
  stats: NextDbPendingWriteStats
  writes: NextDbPendingWriteSummary[]
  autoFlush: {
    enabled: boolean
    intervalMs: number
    limit: number
    retryOnStart: boolean
    scheduled: boolean
    inFlight: boolean
  }
}

export interface NextDbPendingWritesSnapshot {
  queue: NextDbPendingWriteQueueStatus
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbDiscardPendingWriteOptions {
  removeOptimistic?: boolean
}

export interface NextDbDiscardPendingWriteResponse {
  id: string
  discarded: boolean
  removedOptimistic: boolean
  write?: NextDbPendingWriteSummary
}

export interface NextDbResetPendingWriteResponse {
  id: string
  reset: boolean
  write?: NextDbPendingWriteSummary
}

export interface NextDbFlushPendingWritesResult {
  attempted: number
  committed: number
  remaining: number
  errors: Array<{ id: string; error: string; retryable: boolean }>
}

export interface NextDbCacheScope {
  kind: "memory" | "indexedDb" | "custom"
  namespace: string
  name?: string
  endpoint: string
  userId?: NextDbUserId
}

export type NextDbStoredSubscriptionKind = "room" | "table" | "nestedTable" | "query" | "userEvents" | "objects"

export type NextDbStoredSubscription =
  | {
      id: string
      kind: "room"
      roomId: NextDbRoomId
      options: NextDbSubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "table"
      table: NextDbTableName
      options: NextDbSubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "nestedTable"
      table: NextDbTableName
      parentKey: string
      nested: string
      options: NextDbSubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "query"
      query: unknown
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "userEvents"
      userId: NextDbUserId
      options: NextDbSubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }
  | {
      id: string
      kind: "objects"
      options: NextDbSubscriptionOptions
      createdAtMs: number
      updatedAtMs: number
    }

export interface NextDbCacheLease {
  clientId: string
  sessionId?: string
  issuedAtMs: number
  expiresAtMs: number
  profileVersion: number
}

export type NextDbClientCacheInvalidationScope = "all" | "object" | "room" | "user" | "table" | "nestedTable"

export interface NextDbClientCacheInvalidationEntry {
  id: string
  generation: number
  scope: NextDbClientCacheInvalidationScope
  key?: string
  table?: string
  parentKey?: string
  nested?: string
  minValidLsn: number
  reason: string
  createdAtMs: number
}

export interface NextDbClientCacheProfileResponse {
  runtimeId: string
  profile: NextDbClientCacheProfile
  lease: NextDbCacheLease
  invalidations: NextDbClientCacheInvalidationEntry[]
  currentLsn: number
  schemaVersion: number
  resetRequired: boolean
}

export interface NextDbLocalDataStatus {
  endpoint: string
  initialEndpoint: string
  cacheScope: NextDbCacheScope
  configuredRealtimeTransportKind: "websocket" | "webtransport" | "jsonl" | "custom"
  configuredConnectionTransport: NextDbConnectionTransport
  realtimeTransportKind: "websocket" | "webtransport" | "jsonl" | "custom"
  connectionTransport: NextDbConnectionTransport
  transportState: "connecting" | "open" | "closed" | "idle"
  manuallyClosed: boolean
  lastSeenLsn: number
  objectSeenLsn: number
  roomSeenLsn: Record<string, number>
  userSeenLsn: Record<string, number>
  tableSeenLsn: Record<string, number>
  nestedTableSeenLsn: Record<string, number>
  cache: NextDbCacheStats
  coverage: NextDbCacheCoverage
  pendingWrites: NextDbPendingWriteStats
  storedSubscriptions: NextDbStoredSubscription[]
  activeSubscriptions: {
    rooms: string[]
    tables: string[]
    nestedTables: string[]
    queries: string[]
    realtimeChannels: string[]
    userEvents: boolean
    objects: boolean
  }
  persistentSubscriptions: {
    rooms: string[]
    tables: string[]
    nestedTables: string[]
    queries: string[]
    realtimeChannels: string[]
    userEvents: boolean
    objects: boolean
  }
  realtimeChannelStates: Record<string, { version: number; updatedAtMs: number }>
  realtimeChannelMembers: Record<string, { memberCount: number; updatedAtMs?: number }>
  realtimeChannelEvents: Record<string, { eventCount: number; latestSequence?: number; latestTimestampMs?: number }>
  realtimeChannelSignals: Record<string, { signalCount: number; latestSequence?: number; latestTimestampMs?: number }>
  connectionSessions: { sessionCount: number; userCount: number; updatedAtMs?: number }
  cacheMetadata?: unknown
  cacheProfile?: NextDbClientCacheProfile
}

export interface NextDbLocalDataStatusSnapshot {
  status: NextDbLocalDataStatus
  pendingQueue: NextDbPendingWriteQueueStatus
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbLocalCacheProfileTrimReport {
  objects: number
  roomMessages: Record<string, number>
  userEvents: Record<string, number>
  records: Record<string, number>
  nestedRecords: Record<string, Record<string, number>>
  nestedPartitions: Record<string, number>
  total: number
}

export interface NextDbLocalCacheProfileEnforcementResult {
  profile: NextDbClientCacheProfile
  before: NextDbCacheStats
  after: NextDbCacheStats
  removed: NextDbLocalCacheProfileTrimReport
}

export interface NextDbEnforceLocalCacheProfileOptions {
  profile?: NextDbClientCacheProfile
  refreshLease?: boolean
}

export interface NextDbConnectionSession {
  sessionId: string
  userId?: string
  transport: NextDbConnectionTransport
  metadata: unknown
  connectedAtMs: number
  lastSeenAtMs: number
  subscribedRooms: string[]
  subscribedTables: string[]
  subscribedNestedTables: string[]
  subscribedQueries: string[]
  subscribedQueryTables: Record<string, number>
  subscribedUserEvents: boolean
  subscribedObjects: boolean
}

export interface NextDbListConnectionsOptions {
  userId?: NextDbUserId
  transport?: NextDbConnectionTransport
}

export interface NextDbWatchConnectionsOptions extends NextDbListConnectionsOptions {
  immediate?: boolean
}

export interface NextDbConnectionUserSummary {
  userId: string
  sessionCount: number
  sessionIds: string[]
  transports: Record<NextDbConnectionTransport, number>
  subscribedRooms: string[]
  subscribedTables: string[]
  subscribedNestedTables: string[]
  subscribedQueries: string[]
  subscribedQueryTables: Record<string, number>
  userEventSessions: number
  objectSessions: number
  lastSeenAtMs: number
}

export interface NextDbConnectionListResponse {
  sessions: NextDbConnectionSession[]
  total: number
  users: number
  transports: Record<NextDbConnectionTransport, number>
  userSummaries: NextDbConnectionUserSummary[]
}

export interface NextDbConnectionListSnapshotView {
  connections?: NextDbConnectionListResponse
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export type NextDbConnectionEventType =
  | "connected"
  | "disconnected"
  | "subscriptionsUpdated"
  | "metadataUpdated"
  | "disconnectRequested"

export interface NextDbConnectionEvent {
  eventType: NextDbConnectionEventType
  timestampMs: number
  session?: NextDbConnectionSession
  userId?: string
  sessionId?: string
  reason?: string
  targetedSessionIds: string[]
}

export interface NextDbRecord<T> {
  table: string
  key: string
  value: T
  updatedAtMs: number
  lsn: number
  path: string
}

export type NextDbAuditTraceKind =
  | "room"
  | "user"
  | "object"
  | "record"
  | "nestedRecord"
  | "path"
  | "clientMutation"

export interface NextDbAuditPageOptions {
  afterLsn?: number
  limit?: number
}

export type NextDbAuditRecordTraceOptions = {
  [T in NextDbTableName]: {
    kind: "record"
    table: T
    recordKey?: NextDbTableKey<T>
    id?: NextDbTableKey<T>
  } & NextDbAuditPageOptions
}[NextDbTableName]

export type NextDbAuditNestedRecordTraceOptionsForTable<T extends NextDbTableName> = {
  [N in NextDbNestedTableName<T>]: {
    kind: "nestedRecord"
    table: T
    parentKey: NextDbTableKey<T>
    nested: N
    nestedKey?: NextDbNestedKey<T, N>
    id?: NextDbNestedKey<T, N>
  } & NextDbAuditPageOptions
}[NextDbNestedTableName<T>]

export type NextDbAuditNestedRecordTraceOptions = {
  [T in NextDbTableName]: NextDbAuditNestedRecordTraceOptionsForTable<T>
}[NextDbTableName]

export type NextDbAuditTraceOptions =
  | ({ kind: "room"; id: NextDbRoomId } & NextDbAuditPageOptions)
  | ({ kind: "user"; id: NextDbUserId } & NextDbAuditPageOptions)
  | ({ kind: "object"; id: NextDbObjectId<NextDbObjectName> } & NextDbAuditPageOptions)
  | NextDbAuditRecordTraceOptions
  | NextDbAuditNestedRecordTraceOptions
  | ({ kind: "path"; path?: string; id?: string } & NextDbAuditPageOptions)
  | ({ kind: "clientMutation"; clientMutationId?: string; id?: string } & NextDbAuditPageOptions)

export interface NextDbAuditTraceTarget {
  kind: NextDbAuditTraceKind
  id: string
  table?: string
  recordKey?: string
  parentKey?: string
  nested?: string
  nestedKey?: string
  path?: string
  clientMutationId?: string
}

export interface NextDbAuditTraceResponse {
  target: NextDbAuditTraceTarget
  records: unknown[]
  nextAfterLsn: number
  hasMore: boolean
}

export interface NextDbAuditReplayPointOptions {
  atLsn?: number
}

export type NextDbAuditRecordReplayOptions = {
  [T in NextDbTableName]: {
    kind: "record"
    table: T
    recordKey?: NextDbTableKey<T>
    id?: NextDbTableKey<T>
  } & NextDbAuditReplayPointOptions
}[NextDbTableName]

export type NextDbAuditNestedRecordReplayOptionsForTable<T extends NextDbTableName> = {
  [N in NextDbNestedTableName<T>]: {
    kind: "nestedRecord"
    table: T
    parentKey: NextDbTableKey<T>
    nested: N
    nestedKey?: NextDbNestedKey<T, N>
    id?: NextDbNestedKey<T, N>
  } & NextDbAuditReplayPointOptions
}[NextDbNestedTableName<T>]

export type NextDbAuditNestedRecordReplayOptions = {
  [T in NextDbTableName]: NextDbAuditNestedRecordReplayOptionsForTable<T>
}[NextDbTableName]

export type NextDbAuditReplayOptions =
  | ({ kind: "user"; id: NextDbUserId } & NextDbAuditReplayPointOptions)
  | ({ kind: "object"; id: NextDbObjectId<NextDbObjectName> } & NextDbAuditReplayPointOptions)
  | NextDbAuditRecordReplayOptions
  | NextDbAuditNestedRecordReplayOptions

export type NextDbAuditReplayStatus = "exists" | "deleted" | "missing"

export interface NextDbAuditReplayDelete {
  table?: string
  key?: string
  objectId?: string
  path: string
  deletedAtMs: number
  force?: boolean
}

export interface NextDbAuditReplayBaseResponse {
  target: NextDbAuditTraceTarget
  atLsn: number
  status: NextDbAuditReplayStatus
  sourceLsn?: number
  delete?: NextDbAuditReplayDelete
}

export type NextDbAuditReplayResponseForOptions<O extends NextDbAuditReplayOptions> =
  O extends { kind: "record"; table: infer T }
    ? T extends NextDbTableName
      ? NextDbAuditReplayBaseResponse & { record?: NextDbRecord<NextDbTables[T]> }
      : NextDbAuditReplayBaseResponse & { record?: NextDbRecord<unknown> }
    : O extends { kind: "nestedRecord"; table: infer T; nested: infer N }
      ? T extends NextDbTableName
        ? N extends NextDbNestedTableName<T>
          ? NextDbAuditReplayBaseResponse & { record?: NextDbRecord<NextDbNestedTables[T][N]> }
          : NextDbAuditReplayBaseResponse & { record?: NextDbRecord<unknown> }
        : NextDbAuditReplayBaseResponse & { record?: NextDbRecord<unknown> }
      : O extends { kind: "user" }
        ? NextDbAuditReplayBaseResponse & { user?: NextDbUserProfileRecord }
        : O extends { kind: "object" }
          ? NextDbAuditReplayBaseResponse & { object?: NextDbObjects[NextDbObjectName] }
          : NextDbAuditReplayBaseResponse

export interface NextDbDeleteRecordResponse {
  table: string
  key: string
  deleted: boolean
  lsn: number
  deletedAtMs?: number
  path: string
}

export interface NextDbListRecordsResponse<T> {
  table: string
  records: Array<NextDbRecord<T>>
  nextAfterKey?: string
  nextCursor?: string
  hasMore: boolean
}

export type NextDbRoomId = NextDbTableKey<"rooms">
export type NextDbRoomRecord = NextDbTables["rooms"]
export type NextDbRoomMessage = NextDbNestedTables["rooms"]["messages"]
export type NextDbCommittedRoomMessage = NextDbRoomMessage & { lsn: number }

export interface NextDbMessagesResponse<M = NextDbCommittedRoomMessage> {
  roomId: NextDbRoomId
  source: "live" | "chatLog" | "cache"
  messages: M[]
}

export interface NextDbSendMessageOptions {
  durability?: NextDbDurability
  attachments?: Array<NextDbObjectId<NextDbObjectName>>
  clientMutationId?: string
}

export interface NextDbSendMessagesItem {
  body: string
  attachments?: Array<NextDbObjectId<NextDbObjectName>>
  clientMutationId?: string
}

export interface NextDbSendMessagesOptions {
  durability?: NextDbDurability
}

export type NextDbRoomDeliveryEvent<M = NextDbCommittedRoomMessage> =
  | {
      type: "messageCreated"
      roomId: NextDbRoomId
      message: M
    }
  | {
      type: "volatileRoomEvent"
      roomId: NextDbRoomId
      name: string
      payload: unknown
    }

export interface NextDbRoomMessagesSnapshot<M = NextDbCommittedRoomMessage> {
  roomId: NextDbRoomId
  messages: M[]
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbRoomMessagesBinding<M = NextDbCommittedRoomMessage> {
  latest(limitOrOptions?: number | (NextDbFreshnessOptions & { limit?: number })): Promise<NextDbMessagesResponse<M>>
  before(beforeLsn: number, limitOrOptions?: number | (NextDbFreshnessOptions & { limit?: number })): Promise<NextDbMessagesResponse<M>>
  cached(options?: NextDbListCachedRoomMessagesOptions): Promise<NextDbMessagesResponse<M>>
  subscribe(listener: (event: NextDbRoomDeliveryEvent<M>) => void, options?: NextDbSubscriptionOptions): () => void
  watchLatest(listener: (snapshot: NextDbRoomMessagesSnapshot<M>) => void, options?: NextDbWatchOptions): () => void
  sync(options?: { limit?: number; maxPages?: number }): Promise<NextDbSyncUntilCaughtUpResponse>
  activateRuntime(
    options?: Omit<NextDbRuntimeRecordActivationOptions<"rooms">, "table" | "parentKey" | "nested" | "key" | "keys" | "afterKey"> & {
      key?: NextDbNestedKey<"rooms", "messages">
      keys?: Array<NextDbNestedKey<"rooms", "messages">>
      afterKey?: NextDbNestedKey<"rooms", "messages">
    },
  ): Promise<NextDbRuntimeRecordActivationResponse<"rooms">>
  send(body: string, optionsOrDurability?: NextDbSendMessageOptions | NextDbDurability): Promise<M>
  sendMany(messages: Array<string | NextDbSendMessagesItem>, optionsOrDurability?: NextDbSendMessagesOptions | NextDbDurability): Promise<M[]>
}

export interface NextDbRoomBinding<
  R = NextDbRoomRecord,
  M = NextDbCommittedRoomMessage,
> {
  readonly roomId: NextDbRoomId
  messages: NextDbRoomMessagesBinding<M>
  cache: {
    clear(): Promise<void>
    trim(keepLatest: number): Promise<number>
  }
  publishVolatile<E extends string>(
    name: E,
    payload: E extends NextDbEventName ? NextDbEventPayload<E> : unknown,
  ): Promise<NextDbVolatilePublishResponse>
}

export type NextDbObjectBody = Blob | ArrayBuffer | Uint8Array | string

export type NextDbReadConsistency = "local" | "quorum" | "all"
export type NextDbRecordReadConsistency = "eventual" | "read-your-writes" | "strong"

export interface NextDbFreshnessOptions {
  minLsn?: number
  timeoutMs?: number
  consistency?: NextDbReadConsistency
  recordConsistency?: NextDbRecordReadConsistency
}

export type NextDbPredicateTermForField<T, F extends keyof T & string> =
  | {
      field: F
      op: "eq" | "ne"
      value: T[F]
    }
  | (T[F] extends string | number
      ? {
          field: F
          op: "lt" | "lte" | "gt" | "gte"
          value: T[F]
        }
      : never)
  | (T[F] extends string
      ? {
          field: F
          op: "contains" | "startsWith"
          value: string
        }
      : never)
  | (T[F] extends readonly (infer Item)[]
      ? {
          field: F
          op: "contains"
          value: Item
        }
      : never)
  | {
      field: F
      op: "exists"
      value?: boolean
    }

export type NextDbPredicateTerm<T> = {
  [F in keyof T & string]: NextDbPredicateTermForField<T, F>
}[keyof T & string]

export interface NextDbPredicate<T> {
  all: Array<NextDbPredicateTerm<T>>
}

export interface NextDbPageReadOptions<T = unknown> extends NextDbFreshnessOptions {
  limit?: number
  afterKey?: string
  predicate?: NextDbPredicate<T>
}

export interface NextDbListCachedRecordsOptions {
  limit?: number
  afterKey?: string
}

export interface NextDbListCachedNestedRecordsOptions extends NextDbListCachedRecordsOptions {
  order?: "key" | "schema"
  afterCursor?: string
}

export interface NextDbPutObjectOptions<K extends string = string> {
  contentType?: string
  objectId?: K
  clientMutationId?: string
}

export interface NextDbListObjectsOptions extends NextDbFreshnessOptions {
  limit?: number
  afterId?: string
}

export interface NextDbListCachedObjectsOptions {
  limit?: number
  afterId?: string
}

export interface NextDbListObjectsResponse<O, K extends string = string> {
  objects: O[]
  nextAfterId?: K
  hasMore: boolean
}

export interface NextDbObjectBodyRangeOptions extends NextDbFreshnessOptions {
  start?: number
  end?: number
  suffixLength?: number
}

export interface NextDbObjectBodyRangeResponse {
  body: Blob
  contentRange: string
  start: number
  end: number
  byteSize: number
  contentType: string
}

export interface NextDbDeleteObjectOptions {
  force?: boolean
  clientMutationId?: string
}

export interface NextDbDeleteObjectResponse<K extends string = string> {
  objectId: K
  deleted: boolean
  lsn: number
  deletedAtMs?: number
  path: string
}

export interface NextDbObjectReferences<K extends string = string> {
  objectId: K
  refCount: number
  sources: string[]
}

export type NextDbObjectEvent<O, K extends string = string> =
  | {
      type: "objectCommitted"
      object: O
    }
  | {
      type: "objectDeleted"
      objectId: K
      deletedAtMs?: number
      lsn: number
      path?: string
    }

export interface NextDbObjectListSnapshot<O, K extends string = string> extends NextDbListObjectsResponse<O, K> {
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbObjectWatchOptions extends NextDbWatchOptions {
  includeBody?: boolean
}

export interface NextDbObjectSnapshot<O, K extends string = string> {
  objectId: K
  metadata?: O
  cachedBodyAvailable: boolean
  cachedBody?: Blob
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export type NextDbEventName = keyof NextDbEvents & string
export type NextDbEventPayload<E extends NextDbEventName> = NextDbEvents[E]

export type NextDbUserId = Id<"User">
export type NextDbRealtimeChannelId = Id<"RealtimeChannel">

export interface NextDbUserProfileRecord {
  userId: NextDbUserId
  displayName?: string
  metadata: unknown
  createdAtMs: number
  updatedAtMs: number
  lsn: number
  path: string
}

export interface NextDbListUsersOptions extends NextDbFreshnessOptions {
  limit?: number
  afterUserId?: NextDbUserId
}

export interface NextDbListCachedUsersOptions {
  limit?: number
  afterUserId?: NextDbUserId
}

export interface NextDbListUsersResponse {
  users: NextDbUserProfileRecord[]
  nextAfterUserId?: NextDbUserId
  hasMore: boolean
}

export interface NextDbVolatilePublishResponse {
  delivered: number
}

export type NextDbUserEventRecord<E extends NextDbEventName = NextDbEventName> = {
  [N in E]: {
    id: string
    userId: NextDbUserId
    name: N
    payload: NextDbEventPayload<N>
    createdAtMs: number
    lsn: number
    path: string
  }
}[E]

export interface NextDbUserEventsListOptions extends NextDbFreshnessOptions {
  limit?: number
  beforeLsn?: number
  sync?: boolean
}

export interface NextDbListCachedRoomMessagesOptions {
  limit?: number
  beforeLsn?: number
}

export interface NextDbListCachedUserEventsOptions {
  limit?: number
  beforeLsn?: number
}

export interface NextDbPublishUserEventOptions {
  durability?: NextDbDurability
  clientMutationId?: string
}

export interface NextDbUserEventsSnapshot<E extends NextDbEventName = NextDbEventName> {
  userId: NextDbUserId
  events: Array<NextDbUserEventRecord<E>>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export type NextDbUserDeliveryEvent<E extends NextDbEventName = NextDbEventName> =
  | {
      type: "userEvent"
      userId: NextDbUserId
      event: NextDbUserEventRecord<E>
    }
  | {
      type: "volatileUserEvent"
      userId: NextDbUserId
      name: E
      payload: NextDbEventPayload<E>
    }
  | {
      type: "userUpserted"
      userId: NextDbUserId
      user: unknown
    }

export interface NextDbRealtimeMember<M = unknown> {
  userId: NextDbUserId
  sessionId?: string
  metadata: M
  joinedAtMs: number
  updatedAtMs: number
}

export interface NextDbRealtimeJoinResponse<C extends string = string, M = unknown> {
  channelId: C
  member: NextDbRealtimeMember<M>
  members: Array<NextDbRealtimeMember>
}

export interface NextDbRealtimeLeaveResponse<C extends string = string> {
  channelId: C
  removed: boolean
  members: Array<NextDbRealtimeMember>
}

export interface NextDbRealtimeMembersResponse<C extends string = string, M = unknown> {
  channelId: C
  members: Array<NextDbRealtimeMember<M>>
}

export interface NextDbRealtimeMembersSnapshotView<C extends string = string, M = unknown> {
  channelId: C
  snapshot?: NextDbRealtimeMembersResponse<C, M>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbRealtimePresenceUpdateResponse<C extends string = string, M = unknown> {
  channelId: C
  member: NextDbRealtimeMember<M>
  members: Array<NextDbRealtimeMember>
  sequence: number
  delivered: number
}

export interface NextDbRealtimeSignalResponse {
  channelId: string
  sequence: number
  timestampMs: number
  delivered: boolean
  deliveredSessions: number
}

export interface NextDbRealtimeBroadcastResponse {
  channelId: string
  sequence: number
  delivered: number
}

export type NextDbRealtimeSignalEventName = Extract<NextDbEventName, "realtime.channel.signal">
export type NextDbRealtimeChannelEventName = Extract<NextDbEventName, "realtime.channel.event">
export type NextDbRealtimeChannelStateEventName = Extract<NextDbEventName, "realtime.channel.state">
export type NextDbRealtimeMemberJoinedEventName = Extract<NextDbEventName, "realtime.channel.memberJoined">
export type NextDbRealtimeMemberLeftEventName = Extract<NextDbEventName, "realtime.channel.memberLeft">
export type NextDbRealtimeMemberUpdatedEventName = Extract<NextDbEventName, "realtime.channel.memberUpdated">

export type NextDbRealtimeSignalPayload = [NextDbRealtimeSignalEventName] extends [never]
  ? { channelId: NextDbRealtimeChannelId; fromUserId: NextDbUserId; toUserId: NextDbUserId; kind: string; payload: unknown; sequence: number; timestampMs: number }
  : NextDbEventPayload<NextDbRealtimeSignalEventName>

export type NextDbRealtimeChannelEventPayload = [NextDbRealtimeChannelEventName] extends [never]
  ? { channelId: NextDbRealtimeChannelId; fromUserId: NextDbUserId; kind: string; payload: unknown; sequence: number; timestampMs: number }
  : NextDbEventPayload<NextDbRealtimeChannelEventName>

export type NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventPayload extends { kind: infer K extends string }
  ? K
  : string

export type NextDbRealtimeSignalKind = NextDbRealtimeSignalPayload extends { kind: infer K extends string }
  ? K
  : string

export type NextDbRealtimePayloadOfKind<P, K extends string> = Extract<P, { kind: K }> extends never
  ? P & { kind: K }
  : Extract<P, { kind: K }>

export type NextDbRealtimeChannelEventOfKind<
  K extends NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventKind,
> = NextDbRealtimePayloadOfKind<NextDbRealtimeChannelEventPayload, K>

export type NextDbRealtimeSignalOfKind<
  K extends NextDbRealtimeSignalKind = NextDbRealtimeSignalKind,
> = NextDbRealtimePayloadOfKind<NextDbRealtimeSignalPayload, K>

export interface NextDbRealtimeChannelEventsOptions<
  K extends NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventKind,
> {
  limit?: number
  kind?: K
}

export interface NextDbRealtimeChannelEventsSnapshotView<
  C extends string = string,
  K extends NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventKind,
> {
  channelId: C
  events: Array<NextDbRealtimeChannelEventOfKind<K>>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbRealtimeChannelSignalsOptions<
  K extends NextDbRealtimeSignalKind = NextDbRealtimeSignalKind,
> {
  limit?: number
  kind?: K
}

export interface NextDbRealtimeChannelSignalsSnapshotView<
  C extends string = string,
  K extends NextDbRealtimeSignalKind = NextDbRealtimeSignalKind,
> {
  channelId: C
  signals: Array<NextDbRealtimeSignalOfKind<K>>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbRealtimeChannelStateSnapshot<S = unknown, C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  channelId: C
  version: number
  state: S
  updatedAtMs: number
}

export interface NextDbRealtimeChannelStatePayload<S = unknown, C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  channelId: C
  fromUserId: NextDbUserId
  state: NextDbRealtimeChannelStateSnapshot<S, C>
  sequence: number
  timestampMs: number
}

export interface NextDbRealtimeChannelStateResponse<S = unknown, C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  channelId: C
  state: NextDbRealtimeChannelStateSnapshot<S, C>
}

export interface NextDbRealtimeChannelStateUpdateResponse<S = unknown, C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  channelId: C
  state: NextDbRealtimeChannelStateSnapshot<S, C>
  sequence: number
  delivered: number
}

export interface NextDbRealtimeChannelStateSnapshotView<S = unknown, C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  channelId: C
  snapshot?: NextDbRealtimeChannelStateSnapshot<S, C>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export type NextDbRealtimeMemberJoinedPayload = [NextDbRealtimeMemberJoinedEventName] extends [never]
  ? { channelId: string; member: NextDbRealtimeMember }
  : NextDbEventPayload<NextDbRealtimeMemberJoinedEventName>

export type NextDbRealtimeMemberLeftPayload = [NextDbRealtimeMemberLeftEventName] extends [never]
  ? { channelId: string; members: NextDbRealtimeMember[] }
  : NextDbEventPayload<NextDbRealtimeMemberLeftEventName>

export type NextDbRealtimeMemberUpdatedPayload = [NextDbRealtimeMemberUpdatedEventName] extends [never]
  ? { channelId: string; member: NextDbRealtimeMember; sequence: number; timestampMs: number }
  : NextDbEventPayload<NextDbRealtimeMemberUpdatedEventName>

export type NextDbRealtimeBinaryBody = Blob | ArrayBuffer | Uint8Array | string

export interface NextDbRealtimeBinaryFrameOptions {
  contentType?: string
  codec?: string
  timestampMs?: number
  metadata?: unknown
  includeSelf?: boolean
}

export interface NextDbRealtimeBinaryFramePayload {
  dataBase64: string
  byteLength: number
  contentType?: string
  codec?: string
  timestampMs: number
  metadata?: unknown
}

export interface NextDbRealtimeChannelBinding<C extends NextDbRealtimeChannelId = NextDbRealtimeChannelId> {
  join<M = unknown>(metadata?: M): Promise<NextDbRealtimeJoinResponse<C, M>>
  leave(): Promise<NextDbRealtimeLeaveResponse<C>>
  updatePresence<M = unknown>(metadata: M): Promise<NextDbRealtimePresenceUpdateResponse<C, M>>
  members<M = unknown>(): Promise<NextDbRealtimeMembersResponse<C, M>>
  cachedMembers<M = unknown>(): NextDbRealtimeMembersResponse<C, M> | undefined
  watchMembers<M = unknown>(
    listener: (snapshot: NextDbRealtimeMembersSnapshotView<C, M>) => void,
    options?: NextDbWatchOptions,
  ): () => void
  state<S = unknown>(): Promise<NextDbRealtimeChannelStateResponse<S, C>>
  cachedState<S = unknown>(): NextDbRealtimeChannelStateSnapshot<S, C> | undefined
  watchState<S = unknown>(
    listener: (snapshot: NextDbRealtimeChannelStateSnapshotView<S, C>) => void,
    options?: NextDbWatchOptions,
  ): () => void
  cachedRecentEvents<K extends NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventKind>(
    options?: NextDbRealtimeChannelEventsOptions<K>,
  ): Array<NextDbRealtimeChannelEventOfKind<K>>
  watchRecentEvents<K extends NextDbRealtimeChannelEventKind = NextDbRealtimeChannelEventKind>(
    listener: (snapshot: NextDbRealtimeChannelEventsSnapshotView<C, K>) => void,
    options?: NextDbWatchOptions & NextDbRealtimeChannelEventsOptions<K>,
  ): () => void
  cachedRecentSignals<K extends NextDbRealtimeSignalKind = NextDbRealtimeSignalKind>(
    options?: NextDbRealtimeChannelSignalsOptions<K>,
  ): Array<NextDbRealtimeSignalOfKind<K>>
  watchRecentSignals<K extends NextDbRealtimeSignalKind = NextDbRealtimeSignalKind>(
    listener: (snapshot: NextDbRealtimeChannelSignalsSnapshotView<C, K>) => void,
    options?: NextDbWatchOptions & NextDbRealtimeChannelSignalsOptions<K>,
  ): () => void
  updateState<S = unknown>(
    state: S,
    options?: { expectedVersion?: number },
  ): Promise<NextDbRealtimeChannelStateUpdateResponse<S, C>>
  signal(
    toUserId: NextDbUserId,
    kind: string,
    payload: unknown,
  ): Promise<NextDbRealtimeSignalResponse>
  broadcast(
    kind: string,
    payload: unknown,
    options?: { includeSelf?: boolean },
  ): Promise<NextDbRealtimeBroadcastResponse>
  sendGameInput(payload: unknown, options?: { includeSelf?: boolean }): Promise<NextDbRealtimeBroadcastResponse>
  sendGameInputFrame(body: NextDbRealtimeBinaryBody, options?: NextDbRealtimeBinaryFrameOptions): Promise<NextDbRealtimeBroadcastResponse>
  sendStatePatch(payload: unknown, options?: { includeSelf?: boolean }): Promise<NextDbRealtimeBroadcastResponse>
  sendVoice(payload: unknown, options?: { includeSelf?: boolean }): Promise<NextDbRealtimeBroadcastResponse>
  sendVoiceFrame(body: NextDbRealtimeBinaryBody, options?: NextDbRealtimeBinaryFrameOptions): Promise<NextDbRealtimeBroadcastResponse>
  sendVideo(payload: unknown, options?: { includeSelf?: boolean }): Promise<NextDbRealtimeBroadcastResponse>
  sendVideoFrame(body: NextDbRealtimeBinaryBody, options?: NextDbRealtimeBinaryFrameOptions): Promise<NextDbRealtimeBroadcastResponse>
  onEvent(listener: (event: NextDbRealtimeChannelEventPayload) => void): () => void
  onEventKind<K extends NextDbRealtimeChannelEventKind>(
    kind: K,
    listener: (event: NextDbRealtimeChannelEventOfKind<K>) => void,
  ): () => void
  onGameInput(listener: (event: NextDbRealtimeChannelEventPayload) => void): () => void
  onStatePatch(listener: (event: NextDbRealtimeChannelEventPayload) => void): () => void
  onState<S = unknown>(listener: (event: NextDbRealtimeChannelStatePayload<S, C>) => void): () => void
  onSignal(listener: (signal: NextDbRealtimeSignalPayload) => void): () => void
  onSignalKind<K extends NextDbRealtimeSignalKind>(
    kind: K,
    listener: (signal: NextDbRealtimeSignalOfKind<K>) => void,
  ): () => void
  onMemberJoined(listener: (event: NextDbRealtimeMemberJoinedPayload) => void): () => void
  onMemberLeft(listener: (event: NextDbRealtimeMemberLeftPayload) => void): () => void
  onMemberUpdated(listener: (event: NextDbRealtimeMemberUpdatedPayload) => void): () => void
}

export interface NextDbIndexQueryCommonOptions {
  limit?: number
  afterKey?: string
  afterCursor?: string
}

export type NextDbIndexTupleValue = readonly [unknown, ...unknown[]]

export type NextDbIndexExactOptions<V> = V extends NextDbIndexTupleValue
  ? {
      values: V
      value?: never
    }
  : {
      value: V
      values?: never
    } | {
      values: [V]
      value?: never
    }

export type NextDbIndexRangeOptions<V> = V extends NextDbIndexTupleValue
  ? {
      lowerValues?: V
      upperValues?: V
      lower?: never
      upper?: never
    }
  : {
      lower?: V
      upper?: V
      lowerValues?: never
      upperValues?: never
    } | {
      lowerValues?: [V]
      upperValues?: [V]
      lower?: never
      upper?: never
    }

export type NextDbQueryByIndexOptions<V = unknown, T = unknown> = NextDbFreshnessOptions & NextDbIndexQueryCommonOptions
  & { predicate?: NextDbPredicate<T> }
  & (NextDbIndexExactOptions<V> | NextDbIndexRangeOptions<V>)

export type NextDbTableIndexQueryOptions<
  T extends NextDbTableName,
  I extends NextDbTableIndexName<T>,
> = NextDbQueryByIndexOptions<NextDbTableIndexValue<T, I>, NextDbTables[T]>

export type NextDbNestedIndexQueryOptions<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
  I extends NextDbNestedIndexName<T, N>,
> = NextDbQueryByIndexOptions<NextDbNestedIndexValue<T, N, I>, NextDbNestedTables[T][N]>

export interface NextDbTableRecordsSnapshot<T> {
  table: string
  records: Array<NextDbRecord<T>>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export interface NextDbRecordSnapshot<T, K extends string = string> {
  table: string
  key: K
  record?: NextDbRecord<T>
  source: NextDbCacheSnapshotSource
  change?: unknown
}

export type NextDbTableEvent<T> =
  | {
      type: "recordUpserted"
      table: string
      key: string
      record: NextDbRecord<T>
    }
  | {
      type: "recordDeleted"
      table: string
      key: string
      deletedAtMs: number
      lsn: number
      path: string
    }

export interface NextDbSyncUntilCaughtUpResponse {
  events: unknown[]
  nextAfterLsn: number
  currentLsn: number
  hasMore: boolean
  pages: number
}

export interface NextDbNestedSchemaOrderListOptions<T = unknown> extends NextDbFreshnessOptions {
  limit?: number
  afterKey?: string
  afterCursor?: string
  predicate?: NextDbPredicate<T>
}

export interface NextDbRecordLiveQueryResult<T> {
  queryId: string
  response: NextDbListRecordsResponse<T>
  currentLsn: number
  resultId: string
}

export interface NextDbRecordLiveQueryOptions<T> extends NextDbPageReadOptions<T> {
  queryId?: string
  indexName?: string
  value?: unknown
  values?: unknown[]
  lower?: unknown
  upper?: unknown
  lowerValues?: unknown[]
  upperValues?: unknown[]
  afterCursor?: string
  order?: "key" | "schema"
  resultId?: string
  persistent?: boolean
}

export type NextDbNestedTransactionOperation<T, K extends string = string> =
  | {
      type: "upsert"
      key: K
      value: T
      expectedLsn?: number
    }
  | {
      type: "delete"
      key: K
      expectedLsn?: number
    }

export type NextDbTableLocalTransactionOperation<T, K extends string = string> =
  | {
      type: "upsert"
      key: K
      value: T
      expectedLsn?: number
    }
  | {
      type: "delete"
      key: K
      expectedLsn?: number
    }

export type NextDbTableTransactionOperationFor<T extends NextDbTableName> =
  | {
      type: "upsert"
      table: T
      key: NextDbTableKey<T>
      value: NextDbTables[T]
      expectedLsn?: number
    }
  | {
      type: "delete"
      table: T
      key: NextDbTableKey<T>
      expectedLsn?: number
    }

export type NextDbNestedTransactionOperationFor<
  T extends NextDbTableName,
  N extends NextDbNestedTableName<T>,
> =
  | {
      type: "nestedUpsert"
      table: T
      parentKey: NextDbTableKey<T>
      nested: N
      nestedKey: NextDbNestedKey<T, N>
      value: NextDbNestedTables[T][N]
      expectedLsn?: number
    }
  | {
      type: "nestedDelete"
      table: T
      parentKey: NextDbTableKey<T>
      nested: N
      nestedKey: NextDbNestedKey<T, N>
      expectedLsn?: number
    }

export type NextDbNestedRecordTransactionOperationForTable<T extends NextDbTableName> = {
  [N in NextDbNestedTableName<T>]: NextDbNestedTransactionOperationFor<T, N>
}[NextDbNestedTableName<T>]

export type NextDbRecordTransactionOperation =
  | {
      [T in NextDbTableName]: NextDbTableTransactionOperationFor<T>
    }[NextDbTableName]
  | {
      [T in NextDbTableName]: NextDbNestedRecordTransactionOperationForTable<T>
    }[NextDbTableName]

export type NextDbRecordTransactionOperationValue<O extends NextDbRecordTransactionOperation> =
  O extends { type: "upsert"; table: infer T }
    ? T extends NextDbTableName
      ? NextDbTables[T]
      : never
    : O extends { type: "nestedUpsert"; table: infer T; nested: infer N }
      ? T extends NextDbTableName
        ? N extends NextDbNestedTableName<T>
          ? NextDbNestedTables[T][N]
          : never
        : never
      : never

export type NextDbRecordTransactionOperationResult<T> =
  | {
      type: "recordUpserted"
      record: NextDbRecord<T>
    }
  | {
      type: "recordDeleted"
      table: string
      key: string
      deletedAtMs: number
      lsn: number
      path: string
    }

export interface NextDbRecordTransactionResponse<T> {
  lsn: number
  operations: Array<NextDbRecordTransactionOperationResult<T>>
}

export interface NextDbRecordBatchResponse<T> extends NextDbRecordTransactionResponse<T> {
  transactionCount: number
}

export type NextDbBehaviorRecordRead = {
  [T in NextDbTableName]: {
    table: T
    key: NextDbTableKey<T>
  }
}[NextDbTableName]

export type NextDbBehaviorNestedRecordReadForTable<T extends NextDbTableName> = {
  [N in NextDbNestedTableName<T>]: {
    table: T
    parentKey: NextDbTableKey<T>
    nested: N
    nestedKey: NextDbNestedKey<T, N>
  }
}[NextDbNestedTableName<T>]

export type NextDbBehaviorNestedRecordRead = {
  [T in NextDbTableName]: NextDbBehaviorNestedRecordReadForTable<T>
}[NextDbTableName]

export type NextDbBehaviorObjectRead = {
  [O in NextDbObjectName]: {
    object?: O
    objectId: NextDbObjectId<O>
  }
}[NextDbObjectName]

export interface NextDbBehaviorReadPlan {
  records?: NextDbBehaviorRecordRead[]
  nestedRecords?: NextDbBehaviorNestedRecordRead[]
  latestMessages?: Array<{ roomId: NextDbRoomId; limit?: number }>
  objects?: NextDbBehaviorObjectRead[]
  objectBodies?: NextDbBehaviorObjectRead[]
  realtimeChannelMembers?: Array<{ channelId: NextDbRealtimeChannelId }>
  realtimeChannelStates?: Array<{ channelId: NextDbRealtimeChannelId }>
  connectionSessions?: Array<{ userId?: NextDbUserId; sessionId?: string; transport?: NextDbConnectionTransport }>
  auditTraces?: NextDbAuditTraceOptions[]
  auditReplays?: NextDbAuditReplayOptions[]
}

export interface NextDbBehaviorInvokeRequest<
  B extends NextDbBehaviorName,
  M extends NextDbBehaviorMutationName<B>,
> {
  behavior: B
  mutation: M
  userId?: NextDbUserId
  input: NextDbBehaviorInput<B, M>
  read?: NextDbBehaviorReadPlan
  context?: unknown
}

export interface NextDbBehaviorInvokeResponse {
  output: {
    commands: unknown[]
    result: unknown
  }
  metadata: {
    behavior: string
    behaviorVersion: string
    epoch: number
  }
  committed: Array<{ type: string; [key: string]: unknown }>
}

export interface NextDbTableBinding<
  TN extends NextDbTableName,
  T,
  K extends string = string,
  I extends NextDbTableIndexName<TN> = NextDbTableIndexName<TN>,
> {
  upsert(
    key: K,
    value: T,
    optionsOrDurability?: NextDbUpsertOptions | NextDbDurability,
  ): Promise<NextDbRecord<T>>
  upsertMany(
    records: Array<NextDbUpsertManyRecordItem<T, K>>,
    options?: NextDbRecordTransactionOptions,
  ): Promise<Array<NextDbRecord<T>>>
  delete(
    key: K,
    optionsOrDurability?: NextDbDeleteOptions | NextDbDurability,
  ): Promise<NextDbDeleteRecordResponse>
  get(key: K, options?: NextDbFreshnessOptions): Promise<NextDbRecord<T>>
  list(limitOrOptions?: number | NextDbPageReadOptions<T>, afterKey?: string): Promise<NextDbListRecordsResponse<T>>
  index<IN extends I>(indexName: IN, options: NextDbTableIndexQueryOptions<TN, IN>): Promise<NextDbListRecordsResponse<T>>
  transaction(
    operations: Array<NextDbTableLocalTransactionOperation<T, K>>,
    options?: NextDbRecordTransactionOptions,
  ): Promise<NextDbRecordTransactionResponse<T>>
  activateRuntime(
    options?: Omit<NextDbRuntimeRecordActivationOptions<TN>, "table" | "parentKey" | "nested">,
  ): Promise<NextDbRuntimeRecordActivationResponse<TN>>
  evictRuntime(
    options?: Omit<NextDbRuntimeRecordActivationOptions<TN>, "table" | "parentKey" | "nested">,
  ): Promise<NextDbRuntimeRecordActivationResponse<TN>>
  sync(options?: { limit?: number; maxPages?: number }): Promise<NextDbSyncUntilCaughtUpResponse>
  subscribe(listener: (event: NextDbTableEvent<T>) => void, options?: NextDbSubscriptionOptions): () => void
  subscribeQuery(
    listener: (event: NextDbRecordLiveQueryResult<T>) => void,
    options?: Omit<NextDbRecordLiveQueryOptions<T>, "indexName" | "value" | "values" | "lower" | "upper" | "lowerValues" | "upperValues" | "order">,
  ): () => void
  subscribeQuery<IN extends I>(
    listener: (event: NextDbRecordLiveQueryResult<T>) => void,
    options: NextDbTableIndexQueryOptions<TN, IN> & {
      queryId?: string
      indexName: IN
      resultId?: string
      persistent?: boolean
    },
  ): () => void
  watchList(listener: (snapshot: NextDbTableRecordsSnapshot<T>) => void, options?: NextDbWatchOptions): () => void
  watch(key: K, listener: (snapshot: NextDbRecordSnapshot<T, K>) => void, options?: NextDbWatchOptions): () => void
  cache: {
    get(key: K): Promise<NextDbRecord<T> | undefined>
    list(options?: NextDbListCachedRecordsOptions): Promise<NextDbListRecordsResponse<T>>
    clear(): Promise<number>
  }
}

export interface NextDbNestedTableBinding<
  TN extends NextDbTableName,
  NN extends NextDbNestedTableName<TN>,
  T,
  K extends string = string,
  I extends NextDbNestedIndexName<TN, NN> = NextDbNestedIndexName<TN, NN>,
> {
  upsert(
    key: K,
    value: T,
    optionsOrDurability?: NextDbUpsertOptions | NextDbDurability,
  ): Promise<NextDbRecord<T>>
  upsertMany(
    records: Array<NextDbUpsertManyRecordItem<T, K>>,
    options?: NextDbRecordTransactionOptions,
  ): Promise<Array<NextDbRecord<T>>>
  delete(
    key: K,
    optionsOrDurability?: NextDbDeleteOptions | NextDbDurability,
  ): Promise<NextDbDeleteRecordResponse>
  get(key: K, options?: NextDbFreshnessOptions): Promise<NextDbRecord<T>>
  list(limitOrOptions?: number | NextDbPageReadOptions<T>, afterKey?: string): Promise<NextDbListRecordsResponse<T>>
  listBySchemaOrder(
    limitOrOptions?: number | NextDbNestedSchemaOrderListOptions<T>,
    afterKey?: string,
  ): Promise<NextDbListRecordsResponse<T>>
  index<IN extends I>(indexName: IN, options: NextDbNestedIndexQueryOptions<TN, NN, IN>): Promise<NextDbListRecordsResponse<T>>
  transaction(
    operations: Array<NextDbNestedTransactionOperation<T, K>>,
    options?: NextDbRecordTransactionOptions,
  ): Promise<NextDbRecordTransactionResponse<T>>
  activateRuntime(
    options?: Omit<NextDbRuntimeRecordActivationOptions<TN>, "table" | "parentKey" | "nested" | "key" | "keys" | "afterKey"> & {
      key?: K
      keys?: K[]
      afterKey?: K
    },
  ): Promise<NextDbRuntimeRecordActivationResponse<TN>>
  evictRuntime(
    options?: Omit<NextDbRuntimeRecordActivationOptions<TN>, "table" | "parentKey" | "nested" | "key" | "keys" | "afterKey"> & {
      key?: K
      keys?: K[]
      afterKey?: K
    },
  ): Promise<NextDbRuntimeRecordActivationResponse<TN>>
  sync(options?: { limit?: number; maxPages?: number }): Promise<NextDbSyncUntilCaughtUpResponse>
  subscribe(listener: (event: NextDbTableEvent<T>) => void, options?: NextDbSubscriptionOptions): () => void
  subscribeQuery(
    listener: (event: NextDbRecordLiveQueryResult<T>) => void,
    options?: Omit<NextDbRecordLiveQueryOptions<T>, "indexName" | "value" | "values" | "lower" | "upper" | "lowerValues" | "upperValues">,
  ): () => void
  subscribeQuery<IN extends I>(
    listener: (event: NextDbRecordLiveQueryResult<T>) => void,
    options: NextDbNestedIndexQueryOptions<TN, NN, IN> & {
      queryId?: string
      indexName: IN
      order?: "key" | "schema"
      resultId?: string
      persistent?: boolean
    },
  ): () => void
  watchList(listener: (snapshot: NextDbTableRecordsSnapshot<T>) => void, options?: NextDbWatchOptions): () => void
  watch(key: K, listener: (snapshot: NextDbRecordSnapshot<T, K>) => void, options?: NextDbWatchOptions): () => void
  cache: {
    get(key: K): Promise<NextDbRecord<T> | undefined>
    list(options?: NextDbListCachedNestedRecordsOptions): Promise<NextDbListRecordsResponse<T>>
    listBySchemaOrder(options?: Omit<NextDbListCachedNestedRecordsOptions, "order">): Promise<NextDbListRecordsResponse<T>>
    clear(): Promise<number>
  }
}

export interface NextDbObjectStoreBinding<
  O,
  K extends string = string,
> {
  put(
    body: NextDbObjectBody,
    contentTypeOrOptions?: string | NextDbPutObjectOptions<K>,
  ): Promise<O>
  getMetadata(objectId: K, options?: NextDbFreshnessOptions): Promise<O>
  getCachedMetadata(objectId: K): Promise<O | undefined>
  getBody(objectId: K, options?: NextDbFreshnessOptions): Promise<Blob>
  getCachedBody(objectId: K): Promise<Blob | undefined>
  getBodyRange(objectId: K, options: NextDbObjectBodyRangeOptions): Promise<NextDbObjectBodyRangeResponse>
  getReferences(objectId: K): Promise<NextDbObjectReferences<K>>
  delete(objectId: K, options?: NextDbDeleteObjectOptions): Promise<NextDbDeleteObjectResponse<K>>
  list(options?: NextDbListObjectsOptions): Promise<NextDbListObjectsResponse<O, K>>
  listCached(options?: NextDbListCachedObjectsOptions): Promise<NextDbListObjectsResponse<O, K>>
  sync(options?: { limit?: number; maxPages?: number }): Promise<NextDbSyncUntilCaughtUpResponse>
  subscribe(listener: (event: NextDbObjectEvent<O, K>) => void, options?: NextDbSubscriptionOptions): () => void
  watchList(listener: (snapshot: NextDbObjectListSnapshot<O, K>) => void, options?: NextDbWatchOptions): () => void
  watch(objectId: K, listener: (snapshot: NextDbObjectSnapshot<O, K>) => void, options?: NextDbObjectWatchOptions): () => void
}

export interface NextDbTypedClient {
  withSchemaVersion(schemaVersion: typeof NEXTDB_SCHEMA_VERSION): NextDbTypedClient
  health(): Promise<NextDbHealth>
  readiness(): Promise<NextDbReadiness>
  metrics(): Promise<string>
  cacheStats(): Promise<NextDbCacheStats>
  cacheCoverage(): Promise<NextDbCacheCoverage>
  localDataStatus(): Promise<NextDbLocalDataStatus>
  watchLocalDataStatus(
    listener: (snapshot: NextDbLocalDataStatusSnapshot) => void,
    options?: Pick<NextDbWatchOptions, "limit" | "immediate">,
  ): () => void
  pendingWriteQueueStatus(limit?: number): Promise<NextDbPendingWriteQueueStatus>
  watchPendingWrites(
    listener: (snapshot: NextDbPendingWritesSnapshot) => void,
    options?: Pick<NextDbWatchOptions, "limit" | "immediate">,
  ): () => void
  pendingWriteStats(): Promise<NextDbPendingWriteStats>
  clearPendingWrites(): Promise<number>
  discardPendingWrite(id: string, options?: NextDbDiscardPendingWriteOptions): Promise<NextDbDiscardPendingWriteResponse>
  resetPendingWrite(id: string): Promise<NextDbResetPendingWriteResponse>
  flushPendingWrites(limit?: number): Promise<NextDbFlushPendingWritesResult>
  listStoredSubscriptions(): Promise<NextDbStoredSubscription[]>
  restoreSubscriptions(): Promise<NextDbStoredSubscription[]>
  clearStoredSubscriptions(): Promise<number>
  refreshCacheLease(): Promise<NextDbClientCacheProfileResponse>
  clearCache(): Promise<number>
  enforceLocalCacheProfile(options?: NextDbEnforceLocalCacheProfileOptions): Promise<NextDbLocalCacheProfileEnforcementResult>
  runtimeActivationStatus(): Promise<NextDbRuntimeActivationStatusResponse>
  activateRuntimeRecords<T extends NextDbTableName>(
    options: NextDbRuntimeRecordActivationOptions<T>,
  ): Promise<NextDbRuntimeRecordActivationResponse<T>>
  activateRuntimeActor(
    options: NextDbRuntimeActorActivationOptions,
  ): Promise<NextDbRuntimeActorActivationResponse>
  evictRuntimeRecords<T extends NextDbTableName>(
    options: NextDbRuntimeRecordActivationOptions<T>,
  ): Promise<NextDbRuntimeRecordActivationResponse<T>>
  activateRuntimeRoom<R extends NextDbRoomId>(
    options: NextDbRuntimeRoomActivationOptions<R>,
  ): Promise<NextDbRuntimeRoomActivationResponse<R>>
  evictRuntimeRoom<R extends NextDbRoomId>(
    options: NextDbRuntimeRoomActivationOptions<R>,
  ): Promise<NextDbRuntimeRoomActivationResponse<R>>
  getCachedUser(userId?: NextDbUserId): Promise<NextDbUserProfileRecord | undefined>
  getUser(userId?: NextDbUserId, options?: NextDbFreshnessOptions): Promise<NextDbUserProfileRecord>
  upsertUser(
    userId?: NextDbUserId,
    profile?: { displayName?: string; metadata?: unknown; clientMutationId?: string },
  ): Promise<NextDbUserProfileRecord>
  listUsers(options?: NextDbListUsersOptions): Promise<NextDbListUsersResponse>
  listConnections(userIdOrOptions?: NextDbUserId | NextDbListConnectionsOptions): Promise<NextDbConnectionListResponse>
  cachedConnections(options?: NextDbListConnectionsOptions): NextDbConnectionListResponse | undefined
  watchConnections(listener: (snapshot: NextDbConnectionListSnapshotView) => void, options?: NextDbWatchConnectionsOptions): () => void
  onConnectionEvent(listener: (event: NextDbConnectionEvent) => void): () => void
  updateConnectionMetadata(metadata?: unknown): void
  listCachedUsers(options?: NextDbListCachedUsersOptions): Promise<NextDbListUsersResponse>
  listCachedRoomMessages(roomId: NextDbRoomId, options?: NextDbListCachedRoomMessagesOptions): Promise<NextDbMessagesResponse<NextDbCommittedRoomMessage>>
  listCachedUserEvents<E extends NextDbEventName = NextDbEventName>(
    userId: NextDbUserId,
    options?: NextDbListCachedUserEventsOptions,
  ): Promise<Array<NextDbUserEventRecord<E>>>
  listCachedCurrentUserEvents<E extends NextDbEventName = NextDbEventName>(
    options?: NextDbListCachedUserEventsOptions,
  ): Promise<Array<NextDbUserEventRecord<E>>>
  listCachedObjects<O extends NextDbObjectName = NextDbObjectName>(
    options?: NextDbListCachedObjectsOptions,
  ): Promise<NextDbListObjectsResponse<NextDbObjects[O], NextDbObjectId<O>>>
  getCachedObjectMetadata<O extends NextDbObjectName = NextDbObjectName>(
    objectId: NextDbObjectId<O>,
  ): Promise<NextDbObjects[O] | undefined>
  getCachedObjectBody<O extends NextDbObjectName = NextDbObjectName>(
    objectId: NextDbObjectId<O>,
  ): Promise<Blob | undefined>
  getCachedRecord<T extends NextDbTableName>(
    table: T,
    key: NextDbTableKey<T>,
  ): Promise<NextDbRecord<NextDbTables[T]> | undefined>
  listCachedRecords<T extends NextDbTableName>(
    table: T,
    options?: NextDbListCachedRecordsOptions,
  ): Promise<NextDbListRecordsResponse<NextDbTables[T]>>
  getCachedNestedRecord<T extends NextDbTableName, N extends NextDbNestedTableName<T>>(
    table: T,
    parentKey: NextDbTableKey<T>,
    nested: N,
    nestedKey: NextDbNestedKey<T, N>,
  ): Promise<NextDbRecord<NextDbNestedTables[T][N]> | undefined>
  listCachedNestedRecords<T extends NextDbTableName, N extends NextDbNestedTableName<T>>(
    table: T,
    parentKey: NextDbTableKey<T>,
    nested: N,
    options?: NextDbListCachedNestedRecordsOptions,
  ): Promise<NextDbListRecordsResponse<NextDbNestedTables[T][N]>>
  clearNestedTableCache<T extends NextDbTableName, N extends NextDbNestedTableName<T>>(
    table: T,
    parentKey: NextDbTableKey<T>,
    nested: N,
  ): Promise<number>
  clearUserProfileCache(userId?: NextDbUserId): Promise<number>
  clearUserEventCache(userId?: NextDbUserId): Promise<number>
  clearUserCache(userId?: NextDbUserId): Promise<number>
  room(roomId: NextDbRoomId): NextDbRoomBinding<NextDbRoomRecord, NextDbCommittedRoomMessage>
  table<T extends NextDbTableName>(
    table: T,
  ): NextDbTableBinding<T, NextDbTables[T], NextDbTableKey<T>, NextDbTableIndexName<T>>
  objectStore<O extends NextDbObjectName>(
    object: O,
  ): NextDbObjectStoreBinding<NextDbObjects[O], NextDbObjectId<O>>
  publishUserEvent<E extends NextDbEventName>(
    userId: NextDbUserId,
    name: E,
    payload: NextDbEventPayload<E>,
    optionsOrDurability?: NextDbPublishUserEventOptions | NextDbDurability,
  ): Promise<NextDbUserEventRecord<E>>
  publishUserVolatile<E extends NextDbEventName>(
    userId: NextDbUserId,
    name: E,
    payload: NextDbEventPayload<E>,
  ): Promise<NextDbVolatilePublishResponse>
  listUserEvents<E extends NextDbEventName = NextDbEventName>(
    userId: NextDbUserId,
    options?: NextDbUserEventsListOptions,
  ): Promise<Array<NextDbUserEventRecord<E>>>
  listCurrentUserEvents<E extends NextDbEventName = NextDbEventName>(
    options?: NextDbUserEventsListOptions,
  ): Promise<Array<NextDbUserEventRecord<E>>>
  recordTransaction<O extends NextDbRecordTransactionOperation>(
    operations: O[],
    options?: NextDbRecordTransactionOptions,
  ): Promise<NextDbRecordTransactionResponse<NextDbRecordTransactionOperationValue<O>>>
  recordBatch<O extends NextDbRecordTransactionOperation>(
    operations: O[],
    options?: NextDbRecordTransactionOptions,
  ): Promise<NextDbRecordBatchResponse<NextDbRecordTransactionOperationValue<O>>>
  watchCurrentUserEvents<E extends NextDbEventName = NextDbEventName>(
    listener: (snapshot: NextDbUserEventsSnapshot<E>) => void,
    options?: NextDbWatchOptions,
  ): () => void
  onUserEvent<E extends NextDbEventName = NextDbEventName>(
    listener: (event: NextDbUserDeliveryEvent<E>) => void,
  ): () => void
  realtimeChannel<C extends NextDbRealtimeChannelId>(
    channelId: C,
  ): NextDbRealtimeChannelBinding<C>
  nestedTable<T extends NextDbTableName, N extends NextDbNestedTableName<T>>(
    table: T,
    parentKey: NextDbTableKey<T>,
    nested: N,
  ): NextDbNestedTableBinding<T, N, NextDbNestedTables[T][N], NextDbNestedKey<T, N>, NextDbNestedIndexName<T, N>>
  traceEntity(options: NextDbAuditTraceOptions): Promise<NextDbAuditTraceResponse>
  replayEntity<O extends NextDbAuditReplayOptions>(options: O): Promise<NextDbAuditReplayResponseForOptions<O>>
  invokeBehavior<B extends NextDbBehaviorName, M extends NextDbBehaviorMutationName<B>>(
    request: NextDbBehaviorInvokeRequest<B, M>,
  ): Promise<NextDbBehaviorInvokeResponse>
}

export function typedNextDb(client: unknown): NextDbTypedClient {
  const maybeClient = client as { withSchemaVersion?: (schemaVersion: number) => unknown }
  if (maybeClient && typeof maybeClient.withSchemaVersion === "function") {
    return maybeClient.withSchemaVersion(NEXTDB_SCHEMA_VERSION) as NextDbTypedClient
  }
  return client as NextDbTypedClient
}

"#,
    );
}

fn push_fields(out: &mut String, fields: &BTreeMap<String, FieldSchema>) {
    for (name, field) in fields {
        let optional = if field.optional { "?" } else { "" };
        out.push_str(&format!(
            "  {}{}: {}\n",
            ts_property_name(name),
            optional,
            ts_type(&field.field_type)
        ));
    }
}

fn ts_index_value_type(index: &IndexSchema, fields: &BTreeMap<String, FieldSchema>) -> String {
    let types: Vec<String> = index
        .fields
        .iter()
        .map(|field| {
            fields
                .get(field)
                .map(|schema| ts_type(&schema.field_type))
                .unwrap_or_else(|| "unknown".to_string())
        })
        .collect();
    match types.as_slice() {
        [] => "never".to_string(),
        [single] => single.clone(),
        _ => format!("[{}]", types.join(", ")),
    }
}

fn ts_type(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String | FieldType::Text { .. } => "string".to_string(),
        FieldType::Int64 | FieldType::TimeMs => "number".to_string(),
        FieldType::Boolean => "boolean".to_string(),
        FieldType::Id { entity } => format!("Id<\"{}\">", entity),
        FieldType::ObjectRef { object } => pascal(object),
        FieldType::List { item } => format!("{}[]", ts_type(item)),
        FieldType::Json => "unknown".to_string(),
        FieldType::Object { fields } => {
            let mut out = String::from("{\n");
            for (name, field) in fields {
                let optional = if field.optional { "?" } else { "" };
                out.push_str(&format!(
                    "  {}{}: {}\n",
                    ts_property_name(name),
                    optional,
                    ts_type(&field.field_type)
                ));
            }
            out.push('}');
            out
        }
    }
}

fn pascal(input: &str) -> String {
    let singular = input.strip_suffix('s').unwrap_or(input);
    pascal_identifier(singular)
}

fn pascal_identifier(input: &str) -> String {
    input
        .split(|char: char| !char.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn ts_property_name(input: &str) -> String {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return "\"\"".to_string();
    };
    if !(first == '_' || first == '$' || first.is_ascii_alphabetic()) {
        return ts_json_string(input);
    }
    if chars.all(|char| char == '_' || char == '$' || char.is_ascii_alphanumeric()) {
        input.to_string()
    } else {
        ts_json_string(input)
    }
}

fn ts_string_union<'a>(values: impl Iterator<Item = &'a String>) -> String {
    let values: Vec<String> = values.map(|value| ts_json_string(value)).collect();
    if values.is_empty() {
        "never".to_string()
    } else {
        values.join(" | ")
    }
}

fn ts_json_string(input: &str) -> String {
    serde_json::to_string(input).unwrap_or_else(|_| format!("{input:?}"))
}

fn validate_object_fields(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    value: &Value,
) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{path} must be an object"))?;
    for (name, field) in fields {
        match object.get(name) {
            Some(value) => validate_field(&format!("{path}.{name}"), field, value)?,
            None if field.optional => {}
            None => bail!("{path}.{name} is required"),
        }
    }
    Ok(())
}

fn validate_message_draft_fields(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    draft: &MessageDraft,
) -> Result<()> {
    for (name, field) in fields {
        let field_path = format!("{path}.{name}");
        match name.as_str() {
            "id" => validate_string_value(&field_path, field, &draft.id)?,
            "clientMutationId" => validate_optional_string_value(
                &field_path,
                field,
                draft.client_mutation_id.as_deref(),
            )?,
            "roomId" => validate_string_value(&field_path, field, &draft.room_id)?,
            "senderId" => validate_string_value(&field_path, field, &draft.sender_id)?,
            "body" => validate_string_value(&field_path, field, &draft.body)?,
            "attachments" => {
                validate_object_ref_list_value(&field_path, field, &draft.attachments)?
            }
            "createdAtMs" => validate_u64_value(&field_path, field, draft.created_at_ms)?,
            "path" => validate_string_value(&field_path, field, &draft.path)?,
            "lsn" if field.optional => {}
            "lsn" => bail!("{field_path} is required"),
            _ if field.optional => {}
            _ => bail!("{field_path} is required"),
        }
    }
    Ok(())
}

fn validate_optional_string_value(
    path: &str,
    field: &FieldSchema,
    value: Option<&str>,
) -> Result<()> {
    match value {
        Some(value) => validate_string_value(path, field, value),
        None if field.optional => Ok(()),
        None => bail!("{path} is required"),
    }
}

fn validate_string_value(path: &str, field: &FieldSchema, value: &str) -> Result<()> {
    match &field.field_type {
        FieldType::String | FieldType::Json => {}
        FieldType::Text { inline_until } => {
            if value.len() > *inline_until {
                bail!("{path} must be at most {inline_until} bytes");
            }
        }
        FieldType::Id { .. } => {
            if value.trim().is_empty() {
                bail!("{path} must be a non-empty id string");
            }
        }
        _ => validate_field(path, field, &Value::String(value.to_string()))?,
    }
    Ok(())
}

fn validate_u64_value(path: &str, field: &FieldSchema, value: u64) -> Result<()> {
    match &field.field_type {
        FieldType::Int64 | FieldType::TimeMs | FieldType::Json => Ok(()),
        _ => validate_field(path, field, &Value::Number(value.into())),
    }
}

fn validate_object_ref_list_value(
    path: &str,
    field: &FieldSchema,
    refs: &[ObjectRef],
) -> Result<()> {
    match &field.field_type {
        FieldType::List { item } if matches!(item.as_ref(), FieldType::ObjectRef { .. }) => {
            for (index, object_ref) in refs.iter().enumerate() {
                validate_object_ref_value(&format!("{path}[{index}]"), object_ref)?;
            }
            Ok(())
        }
        FieldType::Json => Ok(()),
        _ => {
            let value = serde_json::to_value(refs)?;
            validate_field(path, field, &value)
        }
    }
}

fn validate_object_ref_value(path: &str, object_ref: &ObjectRef) -> Result<()> {
    for (field, value) in [
        ("id", object_ref.id.as_str()),
        ("path", object_ref.path.as_str()),
        ("contentType", object_ref.content_type.as_str()),
        ("sha256", object_ref.sha256.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("{path}.{field} must be a non-empty string");
        }
    }
    Ok(())
}

fn validate_field(path: &str, field: &FieldSchema, value: &Value) -> Result<()> {
    if value.is_null() {
        if field.optional {
            return Ok(());
        }
        bail!("{path} cannot be null");
    }

    match &field.field_type {
        FieldType::String => {
            if !value.is_string() {
                bail!("{path} must be a string");
            }
        }
        FieldType::Text { inline_until } => {
            let Some(text) = value.as_str() else {
                bail!("{path} must be a string");
            };
            if text.len() > *inline_until {
                bail!("{path} must be at most {inline_until} bytes");
            }
        }
        FieldType::Int64 | FieldType::TimeMs => {
            if !is_json_integer(value) {
                bail!("{path} must be an integer");
            }
        }
        FieldType::Boolean => {
            if !value.is_boolean() {
                bail!("{path} must be a boolean");
            }
        }
        FieldType::Id { .. } => {
            if value.as_str().is_none_or(|value| value.trim().is_empty()) {
                bail!("{path} must be a non-empty id string");
            }
        }
        FieldType::ObjectRef { object: _ } => {
            validate_object_ref(path, value)?;
        }
        FieldType::List { item } => {
            let values = value
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("{path} must be an array"))?;
            for (index, value) in values.iter().enumerate() {
                validate_field(
                    &format!("{path}[{index}]"),
                    &FieldSchema {
                        field_type: *item.clone(),
                        optional: false,
                    },
                    value,
                )?;
            }
        }
        FieldType::Object { fields } => validate_object_fields(path, fields, value)?,
        FieldType::Json => {}
    }

    Ok(())
}

fn validate_object_ref(path: &str, value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{path} must be an object ref"))?;
    for key in ["id", "path", "contentType", "sha256"] {
        if object
            .get(key)
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        {
            bail!("{path}.{key} must be a non-empty string");
        }
    }
    if !object.get("byteSize").is_some_and(Value::is_u64) {
        bail!("{path}.byteSize must be a non-negative integer");
    }
    Ok(())
}

fn is_json_integer(value: &Value) -> bool {
    value.is_i64() || value.is_u64()
}

fn validate_field_map(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    schema: &DatabaseSchema,
    errors: &mut Vec<String>,
) {
    if fields.is_empty() {
        errors.push(format!("{path} must not be empty"));
    }
    for (field_name, field) in fields {
        if field_name.trim().is_empty() {
            errors.push(format!("{path} contains an empty field name"));
        }
        validate_field_schema(
            &format!("{path}.{field_name}.type"),
            &field.field_type,
            schema,
            errors,
        );
    }
}

fn validate_field_schema(
    path: &str,
    field_type: &FieldType,
    schema: &DatabaseSchema,
    errors: &mut Vec<String>,
) {
    match field_type {
        FieldType::Text { inline_until } => {
            if *inline_until == 0 {
                errors.push(format!("{path}.inlineUntil must be greater than 0"));
            }
        }
        FieldType::Id { entity } => {
            if entity.trim().is_empty() {
                errors.push(format!("{path}.entity must not be empty"));
            }
        }
        FieldType::ObjectRef { object } => {
            if object.trim().is_empty() {
                errors.push(format!("{path}.object must not be empty"));
            } else if !schema.objects.contains_key(object) {
                errors.push(format!("{path}.object references missing object {object}"));
            }
        }
        FieldType::List { item } => {
            validate_field_schema(&format!("{path}.item"), item, schema, errors);
        }
        FieldType::Object { fields } => {
            validate_field_map(&format!("{path}.fields"), fields, schema, errors);
        }
        FieldType::String
        | FieldType::Int64
        | FieldType::TimeMs
        | FieldType::Boolean
        | FieldType::Json => {}
    }
}

fn validate_storage_class(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    storage: &StorageClass,
    errors: &mut Vec<String>,
) {
    match storage {
        StorageClass::Lru { max_items } => {
            if *max_items == 0 {
                errors.push(format!("{path}.maxItems must be greater than 0"));
            }
        }
        StorageClass::ChatLog {
            bucket,
            order,
            live_window,
        } => {
            if *live_window == 0 {
                errors.push(format!("{path}.liveWindow must be greater than 0"));
            }
            if order.is_empty() {
                errors.push(format!("{path}.order must not be empty"));
            }
            match parse_chat_log_bucket_field(bucket) {
                Ok(field) => {
                    if !fields.contains_key(field) {
                        errors.push(format!("{path}.bucket references missing field {field}"));
                    }
                }
                Err(message) => errors.push(format!("{path}.bucket {message}")),
            }
            for (index, term) in order.iter().enumerate() {
                match parse_chat_log_order_field(term) {
                    Ok(field) => {
                        if !fields.contains_key(field) {
                            errors.push(format!(
                                "{path}.order[{index}] references missing field {field}"
                            ));
                        }
                    }
                    Err(message) => errors.push(format!("{path}.order[{index}] {message}")),
                }
            }
        }
        StorageClass::ActorPartition
        | StorageClass::Resident
        | StorageClass::Disk
        | StorageClass::Object => {}
    }
}

fn validate_read_visibility_policy(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    policy: &ReadVisibilityPolicy,
    errors: &mut Vec<String>,
) {
    if policy.all.len() > 8 {
        errors.push(format!("{path}.all supports at most 8 rules"));
    }
    for (index, rule) in policy.all.iter().enumerate() {
        let field = rule.field();
        let rule_path = format!("{path}.all[{index}]");
        if field.is_empty()
            || field.len() > 64
            || !field
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            errors.push(format!("{rule_path}.field is invalid"));
            continue;
        }
        let Some(field_schema) = fields.get(field) else {
            errors.push(format!(
                "{rule_path}.field references missing field {field}"
            ));
            continue;
        };
        match rule {
            ReadVisibilityRule::FieldEqualsUserId { .. } => match &field_schema.field_type {
                FieldType::Id { entity } if entity == "User" => {}
                FieldType::String => {}
                _ => errors.push(format!(
                    "{rule_path}.field must reference a User id or string field"
                )),
            },
        }
    }
}

fn parse_chat_log_bucket_field(bucket: &str) -> std::result::Result<&str, &'static str> {
    let Some(field) = bucket
        .strip_prefix("day(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err("must be day(field)");
    };
    parse_storage_field_name(field)
}

fn parse_chat_log_order_field(term: &str) -> std::result::Result<&str, &'static str> {
    if let Some(field) = term
        .strip_prefix("desc(")
        .and_then(|value| value.strip_suffix(')'))
    {
        return parse_storage_field_name(field);
    }
    parse_storage_field_name(term)
}

fn parse_storage_field_name(field: &str) -> std::result::Result<&str, &'static str> {
    if field.trim().is_empty() {
        return Err("must reference a field");
    }
    if field != field.trim() {
        return Err("must not contain surrounding whitespace");
    }
    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_messages_storage(schema: &mut DatabaseSchema) -> &mut StorageClass {
        &mut schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .storage
    }

    fn remove_message_field(schema: &mut DatabaseSchema, field: &str) {
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .fields
            .remove(field);
    }

    fn message_draft(body: &str) -> MessageDraft {
        MessageDraft {
            id: "message-a".to_string(),
            client_mutation_id: Some("mutation-a".to_string()),
            room_id: "room-a".to_string(),
            sender_id: "user-a".to_string(),
            body: body.to_string(),
            attachments: Vec::new(),
            created_at_ms: 1,
            path: "rooms/room-a/messages/message-a".to_string(),
        }
    }

    #[test]
    fn default_schema_is_valid() {
        let report = DatabaseSchema::default_nextdb().validation_report();
        assert!(report.ok, "{:?}", report.errors);
    }

    #[test]
    fn chat_log_order_must_not_be_empty() {
        let mut schema = DatabaseSchema::default_nextdb();
        if let StorageClass::ChatLog { order, .. } = default_messages_storage(&mut schema) {
            order.clear();
        }

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(
                |error| error == "tables.rooms.nested.messages.storage.order must not be empty"
            ),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn chat_log_order_fields_must_exist() {
        let mut schema = DatabaseSchema::default_nextdb();
        remove_message_field(&mut schema, "createdAtMs");

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "tables.rooms.nested.messages.storage.order[0] references missing field createdAtMs"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn chat_log_bucket_field_must_exist() {
        let mut schema = DatabaseSchema::default_nextdb();
        if let StorageClass::ChatLog { bucket, .. } = default_messages_storage(&mut schema) {
            *bucket = "day(deletedAtMs)".to_string();
        }

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error == "tables.rooms.nested.messages.storage.bucket references missing field deletedAtMs"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn chat_log_live_window_must_be_positive() {
        let mut schema = DatabaseSchema::default_nextdb();
        if let StorageClass::ChatLog { live_window, .. } = default_messages_storage(&mut schema) {
            *live_window = 0;
        }

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "tables.rooms.nested.messages.storage.liveWindow must be greater than 0"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn nested_index_fields_must_exist() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .indexes
            .insert(
                "byMissing".to_string(),
                IndexSchema {
                    fields: vec!["missingSender".to_string()],
                    unique: false,
                },
            );

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "tables.rooms.nested.messages.indexes.byMissing.fields references missing field missingSender"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn object_ref_fields_must_reference_declared_objects() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema.objects.remove("Object");

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "tables.rooms.nested.messages.fields.attachments.type.item.object references missing object Object"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn id_entity_must_not_be_empty() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .fields
            .insert(
                "ownerId".to_string(),
                FieldSchema::required(FieldType::Id {
                    entity: " ".to_string(),
                }),
            );

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error == "tables.rooms.fields.ownerId.type.entity must not be empty"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn text_inline_until_must_be_positive() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .fields
            .insert(
                "summary".to_string(),
                FieldSchema::required(FieldType::Text { inline_until: 0 }),
            );

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "tables.rooms.nested.messages.fields.summary.type.inlineUntil must be greater than 0"),
            "{:?}",
            report.errors
        );
    }

    #[test]
    fn text_inline_until_is_enforced_at_runtime() {
        let fields = fields([("body", FieldType::Text { inline_until: 4 })]);
        let result =
            validate_object_fields("test", &fields, &serde_json::json!({ "body": "12345" }));

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "test.body must be at most 4 bytes"
        );
    }

    #[test]
    fn message_draft_fast_validation_accepts_default_schema() {
        let schema = DatabaseSchema::default_nextdb();
        schema
            .validate_message_draft(&message_draft("hello"))
            .expect("message draft should validate");
    }

    #[test]
    fn message_draft_fast_validation_enforces_text_inline_until() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .fields
            .insert(
                "body".to_string(),
                FieldSchema::required(FieldType::Text { inline_until: 4 }),
            );

        let result = schema.validate_message_draft(&message_draft("12345"));

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "rooms.messages.body must be at most 4 bytes"
        );
    }

    #[test]
    fn message_draft_fast_validation_rejects_unknown_required_fields() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .fields
            .insert(
                "extra".to_string(),
                FieldSchema::required(FieldType::String),
            );

        let result = schema.validate_message_draft(&message_draft("hello"));

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "rooms.messages.extra is required"
        );
    }

    #[test]
    fn numeric_schema_fields_reject_floats_at_runtime() {
        let fields = fields([
            ("count", FieldType::Int64),
            ("createdAtMs", FieldType::TimeMs),
        ]);
        let result = validate_object_fields(
            "test",
            &fields,
            &serde_json::json!({ "count": 1.5, "createdAtMs": 10 }),
        );

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "test.count must be an integer"
        );
    }

    #[test]
    fn object_ref_byte_size_rejects_floats_at_runtime() {
        let fields = fields([(
            "attachment",
            FieldType::ObjectRef {
                object: "Object".to_string(),
            },
        )]);
        let result = validate_object_fields(
            "test",
            &fields,
            &serde_json::json!({
                "attachment": {
                    "id": "object-1",
                    "path": "objects/object-1",
                    "contentType": "text/plain",
                    "byteSize": 1.5,
                    "sha256": "abc"
                }
            }),
        );

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "test.attachment.byteSize must be a non-negative integer"
        );
    }

    #[test]
    fn declared_event_payloads_are_validated_at_runtime() {
        let schema = SchemaRegistry {
            path: PathBuf::from("test"),
            schema: Arc::new(RwLock::new(DatabaseSchema::default_nextdb())),
        };

        let result = schema.validate_event_payload("notification.created", &serde_json::json!({}));

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "events.notification.created.payload.text is required"
        );
    }

    #[test]
    fn undeclared_event_payloads_remain_json_passthrough() {
        let schema = SchemaRegistry {
            path: PathBuf::from("test"),
            schema: Arc::new(RwLock::new(DatabaseSchema::default_nextdb())),
        };

        schema
            .validate_event_payload("custom.event", &serde_json::json!({ "any": ["json"] }))
            .expect("undeclared event should pass");
    }

    #[test]
    fn realtime_channel_state_event_payload_is_schema_checked() {
        let schema = SchemaRegistry {
            path: PathBuf::from("test"),
            schema: Arc::new(RwLock::new(DatabaseSchema::default_nextdb())),
        };

        schema
            .validate_event_payload(
                "realtime.channel.state",
                &serde_json::json!({
                    "channelId": "call",
                    "fromUserId": "alice",
                    "state": {
                        "channelId": "call",
                        "version": 1,
                        "state": { "phase": "lobby" },
                        "updatedAtMs": 1_000,
                    },
                    "sequence": 1,
                    "timestampMs": 1_000,
                }),
            )
            .expect("valid realtime channel state event should pass");

        let result = schema.validate_event_payload(
            "realtime.channel.state",
            &serde_json::json!({
                "channelId": "call",
                "fromUserId": "alice",
                "state": {
                    "channelId": "call",
                    "state": { "phase": "lobby" },
                    "updatedAtMs": 1_000,
                },
                "sequence": 1,
                "timestampMs": 1_000,
            }),
        );
        assert!(result.is_err());
    }

    #[test]
    fn generated_typescript_binds_index_value_and_range_types() {
        let typescript = generate_typescript(&DatabaseSchema::default_nextdb());

        assert!(typescript.contains("export interface NextDbTableIndexValues"));
        assert!(typescript.contains("byTitle: string"));
        assert!(typescript.contains("export interface NextDbNestedTableIndexValues"));
        assert!(typescript.contains("bySender: Id<\"User\">"));
        assert!(typescript.contains("lower?: V"));
        assert!(typescript.contains("upper?: V"));
        assert!(typescript.contains("afterCursor?: string"));
        assert!(typescript.contains("options: NextDbTableIndexQueryOptions<TN, IN>"));
        assert!(typescript.contains("options: NextDbNestedIndexQueryOptions<TN, NN, IN>"));
        assert!(typescript.contains("export interface NextDbRecordSnapshot"));
        assert!(
            typescript
                .contains("watch(key: K, listener: (snapshot: NextDbRecordSnapshot<T, K>) => void")
        );
        assert!(typescript.contains("export interface NextDbObjects"));
        assert!(typescript.contains("Object: Object"));
        assert!(typescript.contains("export type NextDbObjectId"));
        assert!(typescript.contains("export interface NextDbObjectStoreBinding"));
        assert!(typescript.contains("objectStore<O extends NextDbObjectName>"));
        assert!(typescript.contains("export interface NextDbObjectSnapshot"));
        assert!(typescript.contains(
            "watch(objectId: K, listener: (snapshot: NextDbObjectSnapshot<O, K>) => void"
        ));
        assert!(
            typescript
                .contains("watchList(listener: (snapshot: NextDbObjectListSnapshot<O, K>) => void")
        );
        assert!(typescript.contains("export interface NextDbClientCacheProfile"));
        assert!(typescript.contains("export interface NextDbHealth"));
        assert!(typescript.contains("export interface NextDbRuntimeRecordActivationOptions"));
        assert!(typescript.contains("export interface NextDbRuntimeActivationStatusResponse"));
        assert!(typescript.contains("export interface NextDbActorKernelStatus"));
        assert!(
            typescript.contains(
                "runtimeActivationStatus(): Promise<NextDbRuntimeActivationStatusResponse>"
            )
        );
        assert!(typescript.contains("activateRuntimeRecords<T extends NextDbTableName>"));
        assert!(typescript.contains("activateRuntimeActor("));
        assert!(typescript.contains("evictRuntimeRecords<T extends NextDbTableName>"));
        assert!(typescript.contains("export interface NextDbRuntimeRoomActivationOptions"));
        assert!(typescript.contains("activateRuntimeRoom<R extends NextDbRoomId>"));
        assert!(typescript.contains("evictRuntimeRoom<R extends NextDbRoomId>"));
        assert!(typescript.contains("export interface NextDbReadiness"));
        assert!(typescript.contains("export interface NextDbCacheCoverage"));
        assert!(typescript.contains("export interface NextDbLocalDataStatus"));
        assert!(typescript.contains("export interface NextDbPendingWriteQueueStatus"));
        assert!(typescript.contains("export interface NextDbLocalDataStatusSnapshot"));
        assert!(typescript.contains("export interface NextDbRealtimeChannelCacheCoverage"));
        assert!(typescript.contains("export interface NextDbClientCacheProfileResponse"));
        assert!(typescript.contains("export type NextDbStoredSubscription"));
        assert!(
            typescript
                .contains("realtimeChannels: Record<string, NextDbRealtimeChannelCacheCoverage>")
        );
        assert!(typescript.contains("cacheCoverage(): Promise<NextDbCacheCoverage>"));
        assert!(typescript.contains("health(): Promise<NextDbHealth>"));
        assert!(typescript.contains("readiness(): Promise<NextDbReadiness>"));
        assert!(typescript.contains("metrics(): Promise<string>"));
        assert!(typescript.contains("localDataStatus(): Promise<NextDbLocalDataStatus>"));
        assert!(typescript.contains("pendingWriteStats(): Promise<NextDbPendingWriteStats>"));
        assert!(typescript.contains(
            "flushPendingWrites(limit?: number): Promise<NextDbFlushPendingWritesResult>"
        ));
        assert!(
            typescript.contains("listStoredSubscriptions(): Promise<NextDbStoredSubscription[]>")
        );
        assert!(
            typescript.contains("refreshCacheLease(): Promise<NextDbClientCacheProfileResponse>")
        );
        assert!(typescript.contains("clearCache(): Promise<number>"));
        assert!(typescript.contains(
            "pendingWriteQueueStatus(limit?: number): Promise<NextDbPendingWriteQueueStatus>"
        ));
        assert!(
            typescript.contains(
                "watchLocalDataStatus(\n    listener: (snapshot: NextDbLocalDataStatusSnapshot) => void,"
            )
        );
        assert!(typescript.contains(
            "watchPendingWrites(\n    listener: (snapshot: NextDbPendingWritesSnapshot) => void,"
        ));
        assert!(typescript.contains("export interface NextDbLocalCacheProfileEnforcementResult"));
        assert!(
            typescript.contains(
                "enforceLocalCacheProfile(options?: NextDbEnforceLocalCacheProfileOptions)"
            )
        );
        assert!(typescript.contains("export type NextDbEventName"));
        assert!(typescript.contains("export type NextDbUserEventRecord"));
        assert!(
            typescript.contains("export type NextDbRealtimeChannelId = Id<\"RealtimeChannel\">")
        );
        assert!(typescript.contains("publishUserEvent<E extends NextDbEventName>"));
        assert!(typescript.contains("realtimeChannel<C extends NextDbRealtimeChannelId>"));
        assert!(typescript.contains("export interface NextDbRealtimeChannelBinding"));
        assert!(typescript.contains("export interface NextDbRealtimeBinaryFramePayload"));
        assert!(typescript.contains("export interface NextDbRealtimeMember"));
        assert!(typescript.contains("export interface NextDbRealtimePresenceUpdateResponse"));
        assert!(typescript.contains("export interface NextDbRealtimeMembersSnapshotView"));
        assert!(typescript.contains("export interface NextDbRealtimeChannelStateSnapshot"));
        assert!(typescript.contains("export interface NextDbRealtimeChannelEventsSnapshotView"));
        assert!(typescript.contains("export interface NextDbRealtimeChannelSignalsSnapshotView"));
        assert!(typescript.contains("updatePresence<M = unknown>"));
        assert!(typescript.contains("watchMembers<M = unknown>"));
        assert!(typescript.contains("onMemberUpdated"));
        assert!(typescript.contains("cachedRecentEvents<K extends NextDbRealtimeChannelEventKind"));
        assert!(typescript.contains("watchRecentEvents<K extends NextDbRealtimeChannelEventKind"));
        assert!(typescript.contains("cachedRecentSignals<K extends NextDbRealtimeSignalKind"));
        assert!(typescript.contains("watchRecentSignals<K extends NextDbRealtimeSignalKind"));
        assert!(typescript.contains("onEventKind<K extends NextDbRealtimeChannelEventKind"));
        assert!(typescript.contains("onSignalKind<K extends NextDbRealtimeSignalKind"));
        assert!(typescript.contains("updateState<S = unknown>"));
        assert!(typescript.contains("sendVoiceFrame(body: NextDbRealtimeBinaryBody"));
        assert!(typescript.contains("sendVideoFrame(body: NextDbRealtimeBinaryBody"));
        assert!(typescript.contains("sendGameInputFrame(body: NextDbRealtimeBinaryBody"));
        assert!(typescript.contains("onState<S = unknown>"));
        assert!(typescript.contains("export type NextDbRecordTransactionOperation"));
        assert!(typescript.contains("export type NextDbTableLocalTransactionOperation"));
        assert!(
            typescript.contains("recordTransaction<O extends NextDbRecordTransactionOperation>")
        );
        assert!(typescript.contains("export interface NextDbRecordBatchResponse"));
        assert!(typescript.contains("recordBatch<O extends NextDbRecordTransactionOperation>"));
        assert!(
            typescript.contains("operations: Array<NextDbTableLocalTransactionOperation<T, K>>")
        );
        assert!(typescript.contains("export type NextDbBehaviorRecordRead"));
        assert!(typescript.contains("export type NextDbBehaviorNestedRecordRead"));
        assert!(
            typescript.contains("latestMessages?: Array<{ roomId: NextDbRoomId; limit?: number }>")
        );
        assert!(typescript.contains("objects?: NextDbBehaviorObjectRead[]"));
        assert!(typescript.contains("objectBodies?: NextDbBehaviorObjectRead[]"));
        assert!(
            typescript
                .contains("realtimeChannelMembers?: Array<{ channelId: NextDbRealtimeChannelId }>")
        );
        assert!(
            typescript
                .contains("realtimeChannelStates?: Array<{ channelId: NextDbRealtimeChannelId }>")
        );
        assert!(typescript.contains("connectionSessions?: Array<{ userId?: NextDbUserId; sessionId?: string; transport?: NextDbConnectionTransport }>"));
        assert!(typescript.contains("auditTraces?: NextDbAuditTraceOptions[]"));
        assert!(typescript.contains("auditReplays?: NextDbAuditReplayOptions[]"));
    }

    #[test]
    fn event_schema_removals_are_structured_replay_safe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate.events.remove("notification.created");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["event notification.created cannot be removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"event notification.created cannot be removed".to_string())
        );
    }

    #[test]
    fn object_schema_removals_are_structured_replay_safe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut with_unused_object = current.clone();
        with_unused_object.objects.insert(
            "Avatar".to_string(),
            ObjectSchema {
                fields: fields([
                    (
                        "id",
                        FieldType::Id {
                            entity: "Object".to_string(),
                        },
                    ),
                    ("label", FieldType::String),
                ]),
            },
        );
        let mut candidate = with_unused_object.clone();
        candidate.objects.remove("Avatar");

        let plan = SchemaMigrationPlan::between(&with_unused_object, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["object Avatar cannot be removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"object Avatar cannot be removed".to_string())
        );
    }

    #[test]
    fn event_payload_shape_changes_are_structured_unsafe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate
            .events
            .get_mut("notification.created")
            .unwrap()
            .payload
            .field_type = FieldType::Json;

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(!plan.can_replay_rebuild());
        assert_eq!(
            plan.unsafe_breaking_changes,
            vec!["events.notification.created.payload type cannot change".to_string()]
        );
        assert!(plan.replay_safe_breaking_changes.is_empty());
    }

    #[test]
    fn behavior_mutation_changes_are_structured_unsafe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate
            .behaviors
            .get_mut("echo")
            .unwrap()
            .mutations
            .remove("echo.send");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(!plan.can_replay_rebuild());
        assert_eq!(
            plan.unsafe_breaking_changes,
            vec!["behaviors.echo.mutations.echo.send cannot be removed".to_string()]
        );
        assert!(plan.replay_safe_breaking_changes.is_empty());
    }

    #[test]
    fn behavior_schema_removals_are_structured_replay_safe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate.behaviors.remove("echo-ts");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["behavior echo-ts cannot be removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"behavior echo-ts cannot be removed".to_string())
        );
    }

    #[test]
    fn nested_table_removals_are_structured_replay_safe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate
            .tables
            .get_mut("rooms")
            .unwrap()
            .nested
            .remove("messages");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["nested table rooms.messages cannot be removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"nested table rooms.messages cannot be removed".to_string())
        );
    }

    #[test]
    fn table_removals_are_structured_replay_safe_migration_changes() {
        let mut current = DatabaseSchema::default_nextdb();
        current.tables.insert(
            "auditLogs".to_string(),
            TableSchema {
                storage: StorageClass::Disk,
                fields: fields([
                    (
                        "id",
                        FieldType::Id {
                            entity: "AuditLog".to_string(),
                        },
                    ),
                    ("message", FieldType::String),
                ]),
                nested: BTreeMap::new(),
                read_visibility: ReadVisibilityPolicy::default(),
                indexes: BTreeMap::new(),
            },
        );
        let mut candidate = current.clone();
        candidate.tables.remove("auditLogs");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["table auditLogs cannot be removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"table auditLogs cannot be removed".to_string())
        );
    }

    #[test]
    fn field_removals_are_structured_replay_safe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate
            .tables
            .get_mut("rooms")
            .unwrap()
            .fields
            .remove("title");

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(plan.requires_replay_rebuild);
        assert!(plan.can_replay_rebuild());
        assert!(plan.projection_rebuild_required);
        assert_eq!(
            plan.replay_safe_breaking_changes,
            vec!["tables.rooms.fields.title cannot be removed".to_string()]
        );
        assert_eq!(
            plan.projection_rebuild_reasons,
            vec!["tables.rooms.fields.title removed".to_string()]
        );
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(
            plan.errors
                .contains(&"tables.rooms.fields.title cannot be removed".to_string())
        );
    }

    #[test]
    fn field_shape_changes_are_structured_unsafe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate
            .tables
            .get_mut("rooms")
            .unwrap()
            .fields
            .get_mut("title")
            .unwrap()
            .field_type = FieldType::Text { inline_until: 128 };

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(!plan.requires_replay_rebuild);
        assert!(!plan.can_replay_rebuild());
        assert!(!plan.projection_rebuild_required);
        assert!(plan.replay_safe_breaking_changes.is_empty());
        assert_eq!(
            plan.unsafe_breaking_changes,
            vec!["tables.rooms.fields.title type cannot change".to_string()]
        );
        assert!(
            plan.errors
                .contains(&"tables.rooms.fields.title type cannot change".to_string())
        );
    }

    #[test]
    fn nested_object_optional_field_additions_are_compatible_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        let FieldType::Object { fields } = &mut candidate
            .behaviors
            .get_mut("echo")
            .unwrap()
            .mutations
            .get_mut("echo.send")
            .unwrap()
            .field_type
        else {
            panic!("echo.send must be an object input");
        };
        fields.insert(
            "attachment".to_string(),
            FieldSchema::optional(FieldType::ObjectRef {
                object: "Object".to_string(),
            }),
        );

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(plan.compatible, "{:?}", plan.errors);
        assert!(plan.unsafe_breaking_changes.is_empty());
        assert!(plan.replay_safe_breaking_changes.is_empty());
    }

    #[test]
    fn nested_object_shape_changes_are_structured_unsafe_migration_changes() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        let FieldType::Object { fields } = &mut candidate
            .behaviors
            .get_mut("echo")
            .unwrap()
            .mutations
            .get_mut("echo.send")
            .unwrap()
            .field_type
        else {
            panic!("echo.send must be an object input");
        };
        fields.get_mut("body").unwrap().field_type = FieldType::Text { inline_until: 128 };

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(!plan.compatible);
        assert!(!plan.can_replay_rebuild());
        assert_eq!(
            plan.unsafe_breaking_changes,
            vec!["behaviors.echo.mutations.echo.send.fields.body type cannot change".to_string()]
        );
        assert!(plan.replay_safe_breaking_changes.is_empty());
    }

    #[test]
    fn index_and_storage_changes_are_structured_projection_rebuild_reasons() {
        let current = DatabaseSchema::default_nextdb();
        let mut candidate = current.clone();
        candidate.tables.get_mut("rooms").unwrap().indexes.insert(
            "byOwner".to_string(),
            IndexSchema {
                fields: vec!["ownerId".to_string()],
                unique: false,
            },
        );
        candidate
            .tables
            .get_mut("rooms")
            .unwrap()
            .nested
            .get_mut("messages")
            .unwrap()
            .storage = StorageClass::Lru { max_items: 128 };

        let plan = SchemaMigrationPlan::between(&current, &candidate);

        assert!(plan.compatible);
        assert!(!plan.requires_replay_rebuild);
        assert!(plan.projection_rebuild_required);
        assert_eq!(
            plan.projection_rebuild_reasons,
            vec![
                "tables.rooms.indexes changed".to_string(),
                "tables.rooms.nested.messages.storage changed".to_string(),
            ]
        );
        assert!(plan.replay_safe_breaking_changes.is_empty());
        assert!(plan.unsafe_breaking_changes.is_empty());
    }

    #[test]
    fn schema_registry_recovers_poisoned_lock() {
        let registry = SchemaRegistry {
            path: PathBuf::from("test"),
            schema: Arc::new(RwLock::new(DatabaseSchema::default_nextdb())),
        };

        let poisoned = std::panic::catch_unwind({
            let schema = Arc::clone(&registry.schema);
            move || {
                let _guard = schema.write().expect("schema write lock");
                panic!("poison schema lock");
            }
        });
        assert!(poisoned.is_err());

        assert_eq!(registry.version(), DatabaseSchema::default_nextdb().version);
        let mut next = registry.schema();
        next.version += 1;
        registry.apply(next.clone());
        assert_eq!(registry.version(), next.version);
    }

    #[test]
    fn nested_object_fields_are_validated_recursively() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .behaviors
            .get_mut("echo")
            .expect("echo behavior")
            .mutations
            .insert(
                "echo.attach".to_string(),
                FieldSchema::required(FieldType::Object {
                    fields: fields([(
                        "file",
                        FieldType::ObjectRef {
                            object: "MissingObject".to_string(),
                        },
                    )]),
                }),
            );

        let report = schema.validation_report();
        assert!(!report.ok);
        assert!(
            report.errors.iter().any(|error| error
                == "behaviors.echo.mutations.echo.attach.type.fields.file.type.object references missing object MissingObject"),
            "{:?}",
            report.errors
        );
    }

    #[tokio::test]
    async fn persist_candidate_rewrites_schema_file_atomically() {
        let root =
            std::env::temp_dir().join(format!("nextdb-schema-persist-{}", uuid::Uuid::now_v7()));
        let path = root.join("schema").join("nextdb.schema.json");
        let registry = SchemaRegistry::load(path.clone()).await.unwrap();
        let mut candidate = registry.schema();
        candidate.version += 1;
        candidate
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .fields
            .insert(
                "topic".to_string(),
                FieldSchema::optional(FieldType::String),
            );

        registry.persist_candidate(&candidate).await.unwrap();

        let bytes = fs::read(&path).await.unwrap();
        let persisted: DatabaseSchema = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(persisted.version, candidate.version);
        assert!(persisted.tables["rooms"].fields.contains_key("topic"));
        assert!(!path.with_file_name(".nextdb.schema.json.tmp").exists());
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn persist_candidate_keeps_schema_version_history() {
        let root =
            std::env::temp_dir().join(format!("nextdb-schema-history-{}", uuid::Uuid::now_v7()));
        let path = root.join("schema").join("nextdb.schema.json");
        let registry = SchemaRegistry::load(path.clone()).await.unwrap();
        let current = registry.schema();
        let mut candidate = current.clone();
        candidate.version += 1;
        candidate
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .fields
            .insert(
                "topic".to_string(),
                FieldSchema::optional(FieldType::String),
            );

        registry.persist_candidate(&candidate).await.unwrap();
        registry.apply(candidate.clone());

        let history = registry.history().await.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|entry| (entry.version, entry.current))
                .collect::<Vec<_>>(),
            vec![(current.version, false), (candidate.version, true)]
        );
        let old_schema = registry
            .schema_version(current.version)
            .await
            .unwrap()
            .expect("old schema");
        let new_schema = registry
            .schema_version(candidate.version)
            .await
            .unwrap()
            .expect("new schema");
        assert!(!old_schema.tables["rooms"].fields.contains_key("topic"));
        assert!(new_schema.tables["rooms"].fields.contains_key("topic"));
        assert!(
            path.parent()
                .unwrap()
                .join("history")
                .join("v1.json")
                .exists()
        );
        let _ = fs::remove_dir_all(root).await;
    }
}
