use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
};

use crate::{
    api::error::ApiError,
    model::{DbRecord, Message},
    schema::{DatabaseSchema, FieldSchema, FieldType},
};

#[derive(Clone)]
pub struct ObjectRefIndex {
    path: PathBuf,
    state: Arc<RwLock<RefState>>,
    write_lock: Arc<Mutex<()>>,
}

impl ObjectRefIndex {
    #[cfg(test)]
    pub async fn load(path: PathBuf, messages: &[Message], records: &[DbRecord]) -> Result<Self> {
        let state = RefState::from_messages_and_records(messages, records);
        Self::load_from_state(path, state).await
    }

    pub async fn load_for_schema(
        path: PathBuf,
        messages: &[Message],
        records: &[DbRecord],
        schema: &DatabaseSchema,
    ) -> Result<Self> {
        let state = RefState::from_messages_and_records_for_schema(messages, records, schema)?;
        Self::load_from_state(path, state).await
    }

    async fn load_from_state(path: PathBuf, state: RefState) -> Result<Self> {
        let index = Self {
            path,
            state: Arc::new(RwLock::new(state)),
            write_lock: Arc::new(Mutex::new(())),
        };
        index.persist().await?;
        Ok(index)
    }

    pub async fn retain_message(&self, message: &Message) -> Result<()> {
        if message.attachments.is_empty() {
            return Ok(());
        }
        let changed = {
            let mut state = self.state.write().await;
            state.retain_message(message)
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn retain_messages<'a, I>(&self, messages: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a Message>,
    {
        let mut retained = false;
        {
            let mut state = self.state.write().await;
            for message in messages {
                if message.attachments.is_empty() {
                    continue;
                }
                state.retain_message(message);
                retained = true;
            }
        }
        if retained {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn retain_record_for_schema(
        &self,
        schema: &DatabaseSchema,
        record: &DbRecord,
    ) -> Result<()> {
        let changed = {
            let mut state = self.state.write().await;
            state.retain_record_for_schema(schema, record)?
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn remove_record(&self, path: &str) -> Result<()> {
        let changed = {
            let mut state = self.state.write().await;
            state.remove_source(path)
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn apply_record_changes<'a, R, P>(
        &self,
        removed_paths: P,
        retained_records: R,
    ) -> Result<()>
    where
        R: IntoIterator<Item = &'a DbRecord>,
        P: IntoIterator<Item = &'a str>,
    {
        let changed = {
            let mut changed = false;
            let mut state = self.state.write().await;
            for path in removed_paths {
                changed |= state.remove_source(path);
            }
            for record in retained_records {
                changed |= state.retain_record(record);
            }
            changed
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn apply_record_changes_for_schema<'a, R, P>(
        &self,
        schema: &DatabaseSchema,
        removed_paths: P,
        retained_records: R,
    ) -> Result<()>
    where
        R: IntoIterator<Item = &'a DbRecord>,
        P: IntoIterator<Item = &'a str>,
    {
        let changed = {
            let mut changed = false;
            let mut state = self.state.write().await;
            for path in removed_paths {
                changed |= state.remove_source(path);
            }
            for record in retained_records {
                changed |= state.retain_record_for_schema(schema, record)?;
            }
            changed
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    pub async fn rebuild_for_schema(
        &self,
        messages: &[Message],
        records: &[DbRecord],
        schema: &DatabaseSchema,
    ) -> Result<RefState> {
        let rebuilt = RefState::from_messages_and_records_for_schema(messages, records, schema)?;
        self.replace_with(rebuilt).await
    }

    pub(crate) async fn replace_with(&self, rebuilt: RefState) -> Result<RefState> {
        {
            let mut state = self.state.write().await;
            *state = rebuilt.clone();
        }
        self.persist().await?;
        Ok(rebuilt)
    }

    pub async fn references_for(&self, object_id: &str) -> ObjectReferences {
        let state = self.state.read().await;
        let sources = state.refs.get(object_id).cloned().unwrap_or_default();
        ObjectReferences {
            object_id: object_id.to_string(),
            object_exists: false,
            dangling: !sources.is_empty(),
            ref_count: sources.len(),
            sources: sources.into_iter().collect(),
        }
    }

    pub async fn referenced_ids(&self) -> BTreeSet<String> {
        self.state.read().await.refs.keys().cloned().collect()
    }

    async fn persist(&self) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let state = self.state.read().await;
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec(&*state)?).await?;
        fs::rename(tmp, &self.path).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefState {
    pub refs: BTreeMap<String, BTreeSet<String>>,
}

impl RefState {
    pub fn from_messages(messages: &[Message]) -> Self {
        let mut state = Self::default();
        for message in messages {
            state.retain_message(message);
        }
        state
    }

    #[cfg(test)]
    pub fn from_messages_and_records(messages: &[Message], records: &[DbRecord]) -> Self {
        let mut state = Self::from_messages(messages);
        for record in records {
            state.retain_record(record);
        }
        state
    }

    pub fn from_messages_and_records_for_schema(
        messages: &[Message],
        records: &[DbRecord],
        schema: &DatabaseSchema,
    ) -> Result<Self> {
        let mut state = Self::from_messages(messages);
        for record in records {
            state.retain_record_for_schema(schema, record)?;
        }
        Ok(state)
    }

    pub fn retain_message(&mut self, message: &Message) -> bool {
        let mut changed = false;
        for attachment in &message.attachments {
            changed |= self
                .refs
                .entry(attachment.id.clone())
                .or_default()
                .insert(message.path.clone());
        }
        changed
    }

    #[cfg(test)]
    pub fn retain_record(&mut self, record: &DbRecord) -> bool {
        let mut changed = self.remove_source(&record.path);
        changed |= retain_object_refs_from_value(&record.value, &record.path, &mut self.refs);
        changed
    }

    pub fn retain_record_for_schema(
        &mut self,
        schema: &DatabaseSchema,
        record: &DbRecord,
    ) -> Result<bool> {
        let mut changed = self.remove_source(&record.path);
        for object_id in declared_record_object_ref_ids(schema, record)? {
            changed |= self
                .refs
                .entry(object_id)
                .or_default()
                .insert(record.path.clone());
        }
        Ok(changed)
    }

    fn remove_source(&mut self, source: &str) -> bool {
        let mut changed = false;
        let mut empty = Vec::new();
        for (object_id, sources) in &mut self.refs {
            changed |= sources.remove(source);
            if sources.is_empty() {
                empty.push(object_id.clone());
            }
        }
        for object_id in empty {
            self.refs.remove(&object_id);
        }
        changed
    }
}

fn declared_record_object_ref_ids(
    schema: &DatabaseSchema,
    record: &DbRecord,
) -> Result<Vec<String>> {
    let refs = if let Some((table, nested)) = record.table.split_once('.') {
        let Some(table_schema) = schema.tables.get(table) else {
            return Ok(Vec::new());
        };
        let Some(nested_schema) = table_schema.nested.get(nested) else {
            return Ok(Vec::new());
        };
        collect_declared_object_refs(
            &format!("{} in tables.{table}.nested.{nested}", record.path),
            &nested_schema.fields,
            &record.value,
        )
    } else {
        let Some(table_schema) = schema.tables.get(&record.table) else {
            return Ok(Vec::new());
        };
        collect_declared_object_refs(
            &format!("{} in tables.{}", record.path, record.table),
            &table_schema.fields,
            &record.value,
        )
    }
    .map_err(|err| anyhow::anyhow!(err.message))?;
    Ok(refs.into_iter().map(|object_ref| object_ref.id).collect())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectReferences {
    pub object_id: String,
    pub object_exists: bool,
    pub dangling: bool,
    pub ref_count: usize,
    pub sources: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DeclaredObjectRef {
    pub(crate) path: String,
    pub(crate) id: String,
    pub(crate) object_path: String,
    pub(crate) content_type: String,
    pub(crate) byte_size: u64,
    pub(crate) sha256: String,
}

pub(crate) fn collect_declared_object_refs(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    value: &serde_json::Value,
) -> Result<Vec<DeclaredObjectRef>, ApiError> {
    let mut refs = Vec::new();
    collect_declared_object_refs_from_fields(path, fields, value, &mut refs)?;
    Ok(refs)
}

fn collect_declared_object_refs_from_fields(
    path: &str,
    fields: &BTreeMap<String, FieldSchema>,
    value: &serde_json::Value,
    refs: &mut Vec<DeclaredObjectRef>,
) -> Result<(), ApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request(format!("{path} must be an object")))?;
    for (name, field) in fields {
        let Some(value) = object.get(name) else {
            continue;
        };
        collect_declared_object_refs_from_type(
            &format!("{path}.{name}"),
            &field.field_type,
            value,
            refs,
        )?;
    }
    Ok(())
}

pub(crate) fn collect_declared_object_refs_from_type(
    path: &str,
    field_type: &FieldType,
    value: &serde_json::Value,
    refs: &mut Vec<DeclaredObjectRef>,
) -> Result<(), ApiError> {
    if value.is_null() {
        return Ok(());
    }
    match field_type {
        FieldType::ObjectRef { .. } => {
            let object = value
                .as_object()
                .ok_or_else(|| ApiError::bad_request(format!("{path} must be an object ref")))?;
            let id = object_ref_string_field(path, object, "id")?;
            refs.push(DeclaredObjectRef {
                path: path.to_string(),
                id,
                object_path: object_ref_string_field(path, object, "path")?,
                content_type: object_ref_string_field(path, object, "contentType")?,
                byte_size: object
                    .get("byteSize")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "{path}.byteSize must be a non-negative integer"
                        ))
                    })?,
                sha256: object_ref_string_field(path, object, "sha256")?,
            });
        }
        FieldType::List { item } => {
            let values = value
                .as_array()
                .ok_or_else(|| ApiError::bad_request(format!("{path} must be an array")))?;
            for (index, value) in values.iter().enumerate() {
                collect_declared_object_refs_from_type(
                    &format!("{path}[{index}]"),
                    item,
                    value,
                    refs,
                )?;
            }
        }
        FieldType::Object { fields } => {
            collect_declared_object_refs_from_fields(path, fields, value, refs)?;
        }
        FieldType::String
        | FieldType::Text { .. }
        | FieldType::Int64
        | FieldType::TimeMs
        | FieldType::Boolean
        | FieldType::Id { .. }
        | FieldType::Json => {}
    }
    Ok(())
}

fn object_ref_string_field(
    path: &str,
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<String, ApiError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::bad_request(format!("{path}.{field} must be a non-empty string")))
}

impl ObjectReferences {
    pub fn with_object_exists(mut self, object_exists: bool) -> Self {
        self.object_exists = object_exists;
        self.dangling = !object_exists && self.ref_count > 0;
        self
    }
}

#[cfg(test)]
fn retain_object_refs_from_value(
    value: &serde_json::Value,
    source: &str,
    refs: &mut BTreeMap<String, BTreeSet<String>>,
) -> bool {
    match value {
        serde_json::Value::Object(object) if looks_like_object_ref(object) => {
            let mut changed = false;
            if let Some(id) = object.get("id").and_then(serde_json::Value::as_str) {
                changed |= refs
                    .entry(id.to_string())
                    .or_default()
                    .insert(source.to_string());
            }
            for value in object.values() {
                changed |= retain_object_refs_from_value(value, source, refs);
            }
            changed
        }
        serde_json::Value::Object(object) => {
            let mut changed = false;
            for value in object.values() {
                changed |= retain_object_refs_from_value(value, source, refs);
            }
            changed
        }
        serde_json::Value::Array(values) => {
            let mut changed = false;
            for value in values {
                changed |= retain_object_refs_from_value(value, source, refs);
            }
            changed
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => false,
    }
}

#[cfg(test)]
fn looks_like_object_ref(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    ["id", "path", "contentType", "sha256"].iter().all(|key| {
        object
            .get(*key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    }) && object
        .get("byteSize")
        .is_some_and(serde_json::Value::is_number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "nextdb-object-refs-{name}-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    fn record(key: &str, object_id: &str) -> DbRecord {
        DbRecord {
            table: "rooms".to_string(),
            key: key.to_string(),
            value: json!({
                "id": key,
                "title": key,
                "asset": {
                    "id": object_id,
                    "path": format!("objects/{object_id}"),
                    "contentType": "text/plain",
                    "byteSize": 7,
                    "sha256": format!("sha-{object_id}")
                }
            }),
            updated_at_ms: 1,
            lsn: 1,
            path: format!("tables/rooms/{key}"),
        }
    }

    fn plain_record(key: &str) -> DbRecord {
        DbRecord {
            table: "rooms".to_string(),
            key: key.to_string(),
            value: json!({
                "id": key,
                "title": key
            }),
            updated_at_ms: 1,
            lsn: 1,
            path: format!("tables/rooms/{key}"),
        }
    }

    fn object_ref(object_id: &str) -> crate::model::ObjectRef {
        crate::model::ObjectRef {
            id: object_id.to_string(),
            path: format!("objects/{object_id}"),
            content_type: "text/plain".to_string(),
            byte_size: 7,
            sha256: format!("sha-{object_id}"),
        }
    }

    fn message(id: &str, attachments: Vec<crate::model::ObjectRef>) -> Message {
        Message {
            id: id.to_string(),
            room_id: "room-a".to_string(),
            sender_id: "user-a".to_string(),
            body: id.to_string(),
            attachments,
            created_at_ms: 1,
            lsn: 1,
            path: format!("rooms/room-a/messages/{id}"),
        }
    }

    #[tokio::test]
    async fn apply_record_changes_removes_then_retains_and_persists() {
        let path = test_path("apply-record-changes");
        let old_record = record("room-a", "object-old");
        let new_record = record("room-a", "object-new");
        let refs = ObjectRefIndex::load(path.clone(), &[], std::slice::from_ref(&old_record))
            .await
            .expect("load refs");

        refs.apply_record_changes([old_record.path.as_str()], [&new_record])
            .await
            .expect("apply record changes");

        assert!(refs.references_for("object-old").await.sources.is_empty());
        assert_eq!(
            refs.references_for("object-new").await.sources,
            vec![new_record.path.clone()]
        );

        let bytes = fs::read(&path).await.expect("read persisted refs");
        let persisted = serde_json::from_slice::<RefState>(&bytes).expect("parse persisted refs");
        assert_eq!(
            persisted
                .refs
                .get("object-new")
                .cloned()
                .unwrap_or_default(),
            BTreeSet::from([new_record.path.clone()])
        );
        assert!(!persisted.refs.contains_key("object-old"));

        if path.exists() {
            let _ = fs::remove_file(path).await;
        }
    }

    #[tokio::test]
    async fn apply_record_changes_skips_persist_when_refs_do_not_change() {
        let path = test_path("apply-record-changes-noop");
        let refs = ObjectRefIndex::load(path.clone(), &[], &[])
            .await
            .expect("load refs");
        let before = fs::metadata(&path)
            .await
            .expect("read refs metadata before")
            .modified()
            .expect("read refs mtime before");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        refs.apply_record_changes(["tables/rooms/missing"], [&plain_record("room-a")])
            .await
            .expect("apply no-op record changes");

        assert!(refs.referenced_ids().await.is_empty());
        let after = fs::metadata(&path)
            .await
            .expect("read refs metadata after")
            .modified()
            .expect("read refs mtime after");
        assert_eq!(after, before);

        if path.exists() {
            let _ = fs::remove_file(path).await;
        }
    }

    #[tokio::test]
    async fn retain_messages_batches_attachment_refs_and_persists() {
        let path = test_path("retain-messages");
        let refs = ObjectRefIndex::load(path.clone(), &[], &[])
            .await
            .expect("load refs");
        let first = message("m1", vec![object_ref("object-a")]);
        let second = message("m2", vec![object_ref("object-a"), object_ref("object-b")]);
        let empty = message("m3", Vec::new());

        refs.retain_messages([&first, &second, &empty])
            .await
            .expect("retain messages");

        assert_eq!(
            refs.references_for("object-a").await.sources,
            vec![first.path.clone(), second.path.clone()]
        );
        assert_eq!(
            refs.references_for("object-b").await.sources,
            vec![second.path.clone()]
        );
        assert!(
            refs.references_for("object-missing")
                .await
                .sources
                .is_empty()
        );

        let bytes = fs::read(&path).await.expect("read persisted refs");
        let persisted = serde_json::from_slice::<RefState>(&bytes).expect("parse persisted refs");
        assert_eq!(
            persisted.refs.get("object-a").cloned().unwrap_or_default(),
            BTreeSet::from([first.path.clone(), second.path.clone()])
        );
        assert_eq!(
            persisted.refs.get("object-b").cloned().unwrap_or_default(),
            BTreeSet::from([second.path.clone()])
        );

        if path.exists() {
            let _ = fs::remove_file(path).await;
        }
    }

    #[tokio::test]
    async fn retain_message_skips_empty_attachments_without_persisting() {
        let path = test_path("retain-empty-message");
        let refs = ObjectRefIndex::load(path.clone(), &[], &[])
            .await
            .expect("load refs");
        let before = fs::metadata(&path)
            .await
            .expect("read refs metadata before")
            .modified()
            .expect("read refs mtime before");

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        refs.retain_message(&message("empty", Vec::new()))
            .await
            .expect("retain empty message");

        assert!(refs.referenced_ids().await.is_empty());
        let after = fs::metadata(&path)
            .await
            .expect("read refs metadata after")
            .modified()
            .expect("read refs mtime after");
        assert_eq!(after, before);

        if path.exists() {
            let _ = fs::remove_file(path).await;
        }
    }
}
