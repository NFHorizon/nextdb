use std::{
    cmp::Ordering,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    fs,
    sync::{Mutex, OwnedMutexGuard},
};

use crate::{
    model::{BinaryJsonValue, DbRecord},
    record_projection::{
        IndexRangeProjectionQuery, OrderedProjectionEntry, RecordProjectionKv,
        encode_projected_record,
    },
    schema::IndexSchema,
    util::{decode_hex, hex_lower},
};

const INDEX_RANGE_CURSOR_MAGIC: &[u8; 8] = b"NDBCUR01";

#[derive(Debug, Deserialize, Serialize)]
struct BinaryIndexRangeCursor {
    values: Vec<BinaryJsonValue>,
    key: String,
}

pub enum RecordStoreMutation<'a> {
    Upsert {
        record: &'a DbRecord,
        indexes: &'a BTreeMap<String, IndexSchema>,
        order: Option<Vec<RecordOrderTerm>>,
    },
    Delete {
        table: &'a str,
        key: &'a str,
    },
}

#[derive(Debug, Clone)]
pub struct RecordOrderTerm {
    pub field: String,
    pub direction: RecordOrderDirection,
}

pub struct OrderedDbRecord {
    pub record: DbRecord,
    pub cursor: String,
}

pub struct IndexedDbRecord {
    pub record: DbRecord,
    pub cursor: String,
}

pub struct StagedRecordProjectionRebuild {
    store: RecordStore,
    temp_root: PathBuf,
    backup_root: PathBuf,
    record_count: usize,
    _guard: OwnedMutexGuard<()>,
}

impl StagedRecordProjectionRebuild {
    pub async fn commit(self) -> Result<usize> {
        self.store.close_projection().await;

        let had_existing_root = self.store.root.exists();
        if had_existing_root {
            fs::rename(&self.store.root, &self.backup_root).await?;
        }
        if let Err(err) = fs::rename(&self.temp_root, &self.store.root).await {
            if had_existing_root && self.backup_root.exists() {
                let _ = fs::rename(&self.backup_root, &self.store.root).await;
            }
            if self.temp_root.exists() {
                let _ = fs::remove_dir_all(&self.temp_root).await;
            }
            return Err(err.into());
        }
        self.store.reset_projection().await?;
        Ok(self.record_count)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordProjectionStatus {
    pub records: usize,
    pub key_order_entries: usize,
    pub recent_entries: usize,
    pub index_entries: usize,
    pub partition_entries: usize,
    pub order_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOrderDirection {
    Asc,
    Desc,
}

#[derive(Clone)]
pub struct RecordStore {
    root: PathBuf,
    bootstrap_lock: Arc<Mutex<()>>,
    projection: Arc<Mutex<Option<Arc<RecordProjectionKv>>>>,
}

impl RecordStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            bootstrap_lock: Arc::new(Mutex::new(())),
            projection: Arc::new(Mutex::new(None)),
        }
    }

    async fn projection(&self) -> Result<Arc<RecordProjectionKv>> {
        let mut projection = self.projection.lock().await;
        if let Some(projection) = projection.as_ref() {
            return Ok(projection.clone());
        }

        let opened = Arc::new(
            RecordProjectionKv::open(self.projection_store_dir())
                .with_context(|| "open fjall record projection")?,
        );
        *projection = Some(opened.clone());
        Ok(opened)
    }

    async fn reset_projection(&self) -> Result<()> {
        let opened = Arc::new(
            RecordProjectionKv::open(self.projection_store_dir())
                .with_context(|| "reopen fjall record projection")?,
        );
        *self.projection.lock().await = Some(opened);
        Ok(())
    }

    async fn close_projection(&self) {
        *self.projection.lock().await = None;
    }

    async fn ensure_projection_bootstrapped(&self) -> Result<()> {
        if self.projection_bootstrap_marker().exists() {
            return Ok(());
        }

        let _guard = self.bootstrap_lock.lock().await;
        if self.projection_bootstrap_marker().exists() {
            return Ok(());
        }

        let projection = self.projection().await?;
        projection.import_legacy_records(&self.root).await?;
        fs::write(self.projection_bootstrap_marker(), b"ok\n").await?;
        Ok(())
    }

    pub(crate) fn is_projection_bootstrapped(&self) -> bool {
        self.projection_bootstrap_marker().exists()
    }

    pub async fn upsert_with_indexes_and_order(
        &self,
        record: &DbRecord,
        indexes: &BTreeMap<String, IndexSchema>,
        order: Option<&[RecordOrderTerm]>,
    ) -> Result<()> {
        self.ensure_projection_bootstrapped().await?;
        self.upsert_with_indexes(record, indexes, order).await
    }

    pub async fn get(&self, table: &str, key: &str) -> Result<Option<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection().await?.get(table, key)
    }

    pub async fn delete(&self, table: &str, key: &str) -> Result<bool> {
        self.ensure_projection_bootstrapped().await?;
        self.delete_from_projection(table, key).await
    }

    pub async fn apply_transaction(&self, operations: &[RecordStoreMutation<'_>]) -> Result<()> {
        self.ensure_projection_bootstrapped().await?;
        for operation in operations {
            match operation {
                RecordStoreMutation::Upsert {
                    record,
                    indexes,
                    order,
                } => {
                    self.upsert_with_indexes(record, indexes, order.as_deref())
                        .await?;
                }
                RecordStoreMutation::Delete { table, key } => {
                    self.delete_from_projection(table, key).await?;
                }
            }
        }
        Ok(())
    }

    async fn upsert_with_indexes(
        &self,
        record: &DbRecord,
        indexes: &BTreeMap<String, IndexSchema>,
        order: Option<&[RecordOrderTerm]>,
    ) -> Result<()> {
        let record_bytes = encode_projected_record(record)?;
        let projection = self.projection().await?;
        let existed = projection.get(&record.table, &record.key)?.is_some();
        if existed {
            projection.remove_order_entries(&record.table, &record.key)?;
            projection.remove_index_entries(&record.table, &record.key)?;
        }
        self.apply_prepared_projection_upsert(record, indexes, order, &record_bytes)
            .await
    }

    async fn apply_prepared_projection_upsert(
        &self,
        record: &DbRecord,
        indexes: &BTreeMap<String, IndexSchema>,
        order: Option<&[RecordOrderTerm]>,
        record_bytes: &[u8],
    ) -> Result<()> {
        let projection = self.projection().await?;
        projection.put_record_bytes(record, record_bytes)?;
        if let Some(order) = order {
            projection.put_order_entry(
                record,
                &order_id(order),
                &order_record_cursor(record, order),
                record_bytes,
            )?;
        }
        for (index_name, index) in indexes {
            let values = record_index_values(record, index)?;
            projection.put_index_entry(record, index_name, &values, index.unique, record_bytes)?;
        }
        Ok(())
    }

    async fn delete_from_projection(&self, table: &str, key: &str) -> Result<bool> {
        let projection = self.projection().await?;
        let existed = projection.get(table, key)?.is_some();
        projection.remove_record(table, key)?;
        projection.remove_order_entries(table, key)?;
        projection.remove_index_entries(table, key)?;
        Ok(existed)
    }

    pub async fn list(
        &self,
        table: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection().await?.list(table, after_key, limit)
    }

    pub async fn list_recent(&self, table: &str, limit: Option<usize>) -> Result<Vec<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection().await?.list_recent(table, limit)
    }

    pub async fn list_by_key_prefix(
        &self,
        table: &str,
        key_prefix: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        if let Some(parent_key) = partition_parent_from_prefix(key_prefix) {
            return self
                .list_partition(table, parent_key, after_key, limit)
                .await;
        }

        self.ensure_projection_bootstrapped().await?;
        self.projection()
            .await?
            .list_by_key_prefix(table, key_prefix, after_key, limit)
    }

    pub async fn list_by_key_prefix_ordered(
        &self,
        table: &str,
        key_prefix: &str,
        order: &[RecordOrderTerm],
        after_key: Option<&str>,
        after_cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<OrderedDbRecord>> {
        let Some(parent_key) = partition_parent_from_prefix(key_prefix) else {
            bail!("ordered key-prefix lists require a parent partition prefix");
        };
        if after_cursor.is_some_and(|cursor| !ensure_safe_order_cursor(cursor)) {
            bail!("invalid ordered cursor");
        }
        let logical_after_key = after_key.map(|key| {
            if key.starts_with(key_prefix) {
                key.to_string()
            } else {
                format!("{key_prefix}{key}")
            }
        });
        self.list_ordered_partition(
            table,
            parent_key,
            order,
            logical_after_key.as_deref(),
            after_cursor,
            limit,
        )
        .await
    }

    async fn list_partition(
        &self,
        table: &str,
        parent_key: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection()
            .await?
            .list_partition(table, parent_key, after_key, limit)
    }

    async fn list_ordered_partition(
        &self,
        table: &str,
        parent_key: &str,
        order: &[RecordOrderTerm],
        after_key: Option<&str>,
        after_cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<OrderedDbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        let projection = self.projection().await?;
        let order_id = order_id(order);
        Ok(projection
            .list_ordered_partition(table, parent_key, &order_id, after_key, after_cursor, limit)?
            .into_iter()
            .map(ordered_projection_entry)
            .collect())
    }

    pub async fn query_index(
        &self,
        table: &str,
        index_name: &str,
        values: &[Value],
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection()
            .await?
            .query_index(table, index_name, values, None, after_key, limit)
    }

    pub async fn query_index_by_key_prefix(
        &self,
        table: &str,
        index_name: &str,
        values: &[Value],
        key_prefix: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        self.projection().await?.query_index(
            table,
            index_name,
            values,
            Some(key_prefix),
            after_key,
            limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_index_range(
        &self,
        table: &str,
        index_name: &str,
        lower: Option<&[Value]>,
        upper: Option<&[Value]>,
        key_prefix: Option<&str>,
        after_cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<IndexedDbRecord>> {
        self.ensure_projection_bootstrapped().await?;
        let after_cursor = match after_cursor {
            Some(cursor) => Some(parse_index_range_cursor(cursor)?),
            None => None,
        };
        self.projection()
            .await?
            .query_index_range(IndexRangeProjectionQuery {
                table,
                index_name,
                lower,
                upper,
                key_prefix,
                after_cursor: after_cursor
                    .as_ref()
                    .map(|(values, key)| (values.as_slice(), key.as_str())),
                limit,
            })?
            .into_iter()
            .map(|entry| {
                let cursor = index_range_cursor(&entry.values, &entry.record.key)?;
                Ok(IndexedDbRecord {
                    record: entry.record,
                    cursor,
                })
            })
            .collect()
    }

    pub async fn force_rebuild_from_records_with_indexes(
        &self,
        records: &[DbRecord],
        indexes_by_table: &BTreeMap<String, BTreeMap<String, IndexSchema>>,
        orders_by_table: &BTreeMap<String, Vec<RecordOrderTerm>>,
    ) -> Result<usize> {
        self.stage_rebuild_from_records_with_indexes(records, indexes_by_table, orders_by_table)
            .await?
            .commit()
            .await
    }

    pub async fn stage_rebuild_from_records_with_indexes(
        &self,
        records: &[DbRecord],
        indexes_by_table: &BTreeMap<String, BTreeMap<String, IndexSchema>>,
        orders_by_table: &BTreeMap<String, Vec<RecordOrderTerm>>,
    ) -> Result<StagedRecordProjectionRebuild> {
        let guard = self.bootstrap_lock.clone().lock_owned().await;
        let parent = self
            .root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent).await?;
        let suffix = rebuild_suffix();
        let temp_root = parent.join(format!(
            ".{}.rebuild-{suffix}",
            self.root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("records")
        ));
        let backup_root = parent.join(format!(
            ".{}.backup-{suffix}",
            self.root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("records")
        ));
        for path in [&temp_root, &backup_root] {
            if path.exists() {
                fs::remove_dir_all(path).await?;
            }
        }

        let temp_store = RecordStore::new(temp_root.clone());
        let build_result = build_projection_root(
            &temp_store,
            &temp_root,
            records,
            indexes_by_table,
            orders_by_table,
        )
        .await;
        temp_store.close_projection().await;
        if let Err(err) = build_result {
            if temp_root.exists() {
                let _ = fs::remove_dir_all(&temp_root).await;
            }
            return Err(err);
        }
        Ok(StagedRecordProjectionRebuild {
            store: self.clone(),
            temp_root,
            backup_root,
            record_count: records.len(),
            _guard: guard,
        })
    }

    pub async fn validate_rebuild_from_records_with_indexes(
        &self,
        records: &[DbRecord],
        indexes_by_table: &BTreeMap<String, BTreeMap<String, IndexSchema>>,
        orders_by_table: &BTreeMap<String, Vec<RecordOrderTerm>>,
    ) -> Result<usize> {
        let parent = self
            .root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent).await?;
        let suffix = rebuild_suffix();
        let temp_root = parent.join(format!(
            ".{}.validate-{suffix}",
            self.root
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("records")
        ));
        if temp_root.exists() {
            fs::remove_dir_all(&temp_root).await?;
        }
        let temp_store = RecordStore::new(temp_root.clone());
        let build_result = build_projection_root(
            &temp_store,
            &temp_root,
            records,
            indexes_by_table,
            orders_by_table,
        )
        .await;
        temp_store.close_projection().await;
        if temp_root.exists() {
            let _ = fs::remove_dir_all(&temp_root).await;
        }
        build_result?;
        Ok(records.len())
    }

    pub async fn projection_status(&self) -> Result<RecordProjectionStatus> {
        self.ensure_projection_bootstrapped().await?;
        let status = self.projection().await?.status()?;
        Ok(RecordProjectionStatus {
            records: status.records,
            key_order_entries: status.key_order_entries,
            recent_entries: status.recent_entries,
            index_entries: status.index_entries,
            partition_entries: status.partition_entries,
            order_entries: status.order_entries,
        })
    }

    #[cfg(test)]
    fn table_dir(&self, table: &str) -> PathBuf {
        self.root.join(safe_component(table))
    }

    fn projection_store_dir(&self) -> PathBuf {
        self.root.join("_fjall")
    }

    fn projection_bootstrap_marker(&self) -> PathBuf {
        self.root.join("_fjall.records.bootstrapped")
    }

    #[cfg(test)]
    fn record_path(&self, table: &str, key: &str) -> PathBuf {
        self.table_dir(table)
            .join(format!("{}.json", hex_lower(key.as_bytes())))
    }

    #[cfg(test)]
    fn index_table_dir(&self, table: &str) -> PathBuf {
        self.root.join("_indexes").join(safe_component(table))
    }

    #[cfg(test)]
    fn key_order_dir(&self, table: &str) -> PathBuf {
        self.root.join("_key_order").join(safe_component(table))
    }

    #[cfg(test)]
    fn recent_dir(&self, table: &str) -> PathBuf {
        self.root.join("_recent").join(safe_component(table))
    }

    #[cfg(test)]
    fn partition_dir(&self, table: &str, parent_key: &str) -> PathBuf {
        self.root
            .join("_partitions")
            .join(safe_component(table))
            .join(safe_component(parent_key))
    }

    #[cfg(test)]
    fn order_table_parent_dir(&self, table: &str, parent_key: &str) -> PathBuf {
        self.root
            .join("_orders")
            .join(safe_component(table))
            .join(safe_component(parent_key))
    }

    #[cfg(test)]
    fn order_dir(&self, table: &str, parent_key: &str, order: &[RecordOrderTerm]) -> PathBuf {
        self.order_table_parent_dir(table, parent_key)
            .join(order_id(order))
    }
}

async fn build_projection_root(
    store: &RecordStore,
    root: &Path,
    records: &[DbRecord],
    indexes_by_table: &BTreeMap<String, BTreeMap<String, IndexSchema>>,
    orders_by_table: &BTreeMap<String, Vec<RecordOrderTerm>>,
) -> Result<()> {
    fs::create_dir_all(root).await?;
    for record in records {
        let indexes = indexes_by_table
            .get(&record.table)
            .cloned()
            .unwrap_or_default();
        let order = orders_by_table.get(&record.table).map(Vec::as_slice);
        store
            .upsert_with_indexes_and_order(record, &indexes, order)
            .await?;
    }
    fs::write(store.projection_bootstrap_marker(), b"ok\n").await?;
    fs::write(root.join(".rebuilt-from-wal"), b"ok\n").await?;
    Ok(())
}

pub fn parse_record_order_terms(order: &[String]) -> Result<Vec<RecordOrderTerm>> {
    order
        .iter()
        .map(|term| parse_record_order_term(term))
        .collect()
}

fn parse_record_order_term(term: &str) -> Result<RecordOrderTerm> {
    if let Some(field) = term
        .strip_prefix("desc(")
        .and_then(|value| value.strip_suffix(')'))
    {
        if field.trim().is_empty() {
            bail!("invalid schema order term");
        }
        return Ok(RecordOrderTerm {
            field: field.to_string(),
            direction: RecordOrderDirection::Desc,
        });
    }
    if term.trim().is_empty() {
        bail!("invalid schema order term");
    }
    Ok(RecordOrderTerm {
        field: term.to_string(),
        direction: RecordOrderDirection::Asc,
    })
}

fn order_id(order: &[RecordOrderTerm]) -> String {
    let mut encoded = String::new();
    for term in order {
        encoded.push_str(match term.direction {
            RecordOrderDirection::Asc => "asc:",
            RecordOrderDirection::Desc => "desc:",
        });
        encoded.push_str(&term.field);
        encoded.push('\n');
    }
    hex_lower(encoded.as_bytes())
}

fn ordered_projection_entry(entry: OrderedProjectionEntry) -> OrderedDbRecord {
    OrderedDbRecord {
        record: entry.record,
        cursor: entry.cursor,
    }
}

pub fn index_range_cursor(values: &[Value], key: &str) -> Result<String> {
    let cursor = BinaryIndexRangeCursor {
        values: values
            .iter()
            .map(BinaryJsonValue::from_json)
            .collect::<Result<Vec<_>>>()?,
        key: key.to_string(),
    };
    let payload = postcard::to_allocvec(&cursor)?;
    let mut encoded = Vec::with_capacity(INDEX_RANGE_CURSOR_MAGIC.len() + payload.len());
    encoded.extend_from_slice(INDEX_RANGE_CURSOR_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(hex_lower(&encoded))
}

pub fn parse_index_range_cursor(cursor: &str) -> Result<(Vec<Value>, String)> {
    let bytes = decode_hex(cursor)?;
    if let Some(payload) = bytes.strip_prefix(INDEX_RANGE_CURSOR_MAGIC) {
        let cursor: BinaryIndexRangeCursor =
            postcard::from_bytes(payload).context("parse postcard index range cursor")?;
        let values = cursor
            .values
            .into_iter()
            .map(BinaryJsonValue::into_json)
            .collect::<Result<Vec<_>>>()?;
        return Ok((values, cursor.key));
    }
    serde_json::from_slice(&bytes).context("parse legacy JSON index range cursor")
}

pub fn compare_index_values(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let ordering = compare_index_value(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn compare_index_value(left: &Value, right: &Value) -> Ordering {
    index_value_rank(left)
        .cmp(&index_value_rank(right))
        .then_with(|| match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
            (Value::Number(left), Value::Number(right)) => compare_json_numbers(left, right),
            (Value::String(left), Value::String(right)) => left.cmp(right),
            _ => left.to_string().cmp(&right.to_string()),
        })
}

fn index_value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) | Value::Object(_) => 4,
    }
}

fn compare_json_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn rebuild_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

pub fn order_record_cursor(record: &DbRecord, order: &[RecordOrderTerm]) -> String {
    format!(
        "{}__{}",
        order_record_file_stem(record, order),
        hex_lower(record.key.as_bytes())
    )
}

fn order_record_file_stem(record: &DbRecord, order: &[RecordOrderTerm]) -> String {
    let mut parts = Vec::with_capacity(order.len());
    for term in order {
        parts.push(order_value_component(
            record.value.get(&term.field),
            term.direction,
        ));
    }
    parts.join("_")
}

fn order_value_component(value: Option<&Value>, direction: RecordOrderDirection) -> String {
    let asc = match value {
        Some(Value::Number(number)) => {
            if let Some(value) = number.as_u64() {
                format!("2u{:020}", value)
            } else if let Some(value) = number.as_i64() {
                let normalized = (value as i128) - (i64::MIN as i128);
                format!("2i{:020}", normalized)
            } else if let Some(value) = number.as_f64() {
                format!("2f{:024}", value)
            } else {
                "0".to_string()
            }
        }
        Some(Value::String(value)) => format!("3s{}", hex_lower(value.as_bytes())),
        Some(Value::Bool(value)) => format!("1b{}", if *value { 1 } else { 0 }),
        Some(Value::Null) | None => "0".to_string(),
        Some(value) => format!("4j{}", hex_lower(value.to_string().as_bytes())),
    };
    match direction {
        RecordOrderDirection::Asc => asc,
        RecordOrderDirection::Desc => invert_sort_component(&asc),
    }
}

fn invert_sort_component(value: &str) -> String {
    let inverted = value
        .as_bytes()
        .iter()
        .map(|byte| 255_u8.saturating_sub(*byte))
        .collect::<Vec<_>>();
    hex_lower(&inverted)
}

fn partition_parent_from_prefix(key_prefix: &str) -> Option<&str> {
    key_prefix
        .strip_suffix(':')
        .filter(|parent| !parent.is_empty())
}

fn ensure_safe_order_cursor(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '_' | '-'))
}

pub fn ensure_safe_record_component(value: &str) -> bool {
    !value.trim().is_empty()
        && Path::new(value).components().count() == 1
        && value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | ':' | '.'))
}

#[cfg(test)]
fn safe_component(value: &str) -> String {
    hex_lower(value.as_bytes())
}

pub fn record_index_values(record: &DbRecord, index: &IndexSchema) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(index.fields.len());
    for field in &index.fields {
        let value = record
            .value
            .get(field)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("indexed field {field} is missing"))?;
        match value {
            Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
                values.push(value)
            }
            _ => bail!("indexed field {field} must be scalar"),
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("nextdb-record-store-{name}-{}", rebuild_suffix()))
    }

    fn record(key: &str, title: &str, lsn: u64) -> DbRecord {
        DbRecord {
            table: "rooms".to_string(),
            key: key.to_string(),
            value: json!({ "id": key, "title": title }),
            updated_at_ms: lsn,
            lsn,
            path: format!("tables/rooms/{key}"),
        }
    }

    fn nested_record(parent_key: &str, nested_key: &str, created_at_ms: u64) -> DbRecord {
        let key = format!("{parent_key}:{nested_key}");
        DbRecord {
            table: "rooms.messages".to_string(),
            key,
            value: json!({
                "id": nested_key,
                "roomId": parent_key,
                "senderId": "user-1",
                "body": "hello",
                "attachments": [],
                "createdAtMs": created_at_ms,
                "path": format!("rooms/{parent_key}/messages/{nested_key}")
            }),
            updated_at_ms: created_at_ms,
            lsn: created_at_ms,
            path: format!("tables/rooms/{parent_key}/messages/{nested_key}"),
        }
    }

    #[test]
    fn index_range_cursor_uses_postcard_and_reads_legacy_json() {
        let values = vec![json!("Beta"), json!(42), json!({ "nested": true })];
        let cursor = index_range_cursor(&values, "room-1").expect("encode cursor");
        let bytes = decode_hex(&cursor).expect("decode cursor hex");
        assert!(bytes.starts_with(INDEX_RANGE_CURSOR_MAGIC));
        assert!(serde_json::from_slice::<(Vec<Value>, String)>(&bytes).is_err());
        assert_eq!(
            parse_index_range_cursor(&cursor).expect("decode postcard cursor"),
            (values.clone(), "room-1".to_string())
        );

        let legacy = hex_lower(&serde_json::to_vec(&(values.clone(), "room-2")).unwrap());
        assert_eq!(
            parse_index_range_cursor(&legacy).expect("decode legacy cursor"),
            (values, "room-2".to_string())
        );
    }

    #[tokio::test]
    async fn failed_atomic_rebuild_preserves_existing_projection() {
        let root = test_root("atomic-failure");
        let store = RecordStore::new(root.clone());
        let records = vec![record("a", "same", 1), record("b", "same", 2)];
        let no_indexes = BTreeMap::new();
        let no_orders = BTreeMap::new();
        store
            .force_rebuild_from_records_with_indexes(&records, &no_indexes, &no_orders)
            .await
            .expect("initial rebuild");
        assert!(!store.key_order_dir("rooms").exists());
        assert!(!store.recent_dir("rooms").exists());

        let mut room_indexes = BTreeMap::new();
        room_indexes.insert(
            "byTitle".to_string(),
            IndexSchema {
                fields: vec!["title".to_string()],
                unique: true,
            },
        );
        let mut indexes = BTreeMap::new();
        indexes.insert("rooms".to_string(), room_indexes);

        let err = store
            .force_rebuild_from_records_with_indexes(&records, &indexes, &no_orders)
            .await
            .expect_err("unique rebuild should fail");

        assert!(err.to_string().contains("unique index violation"));
        assert_eq!(store.list("rooms", None, Some(10)).await.unwrap().len(), 2);
        let status = store.projection_status().await.unwrap();
        assert_eq!(status.records, 2);
        assert_eq!(status.key_order_entries, 2);
        assert_eq!(status.recent_entries, 2);
        assert_eq!(status.index_entries, 0);

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn successful_rebuild_keeps_previous_projection_for_rollback() {
        let root = test_root("rollback-backup");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();
        let no_orders = BTreeMap::new();
        let initial_records = vec![record("a", "Alpha", 1), record("b", "Beta", 2)];
        let rebuilt_records = vec![record("c", "Gamma", 3)];

        store
            .force_rebuild_from_records_with_indexes(&initial_records, &no_indexes, &no_orders)
            .await
            .expect("initial rebuild");
        store
            .force_rebuild_from_records_with_indexes(&rebuilt_records, &no_indexes, &no_orders)
            .await
            .expect("second rebuild");

        assert_eq!(
            store
                .list("rooms", None, Some(10))
                .await
                .expect("list rebuilt projection")
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );

        let parent = root.parent().expect("test root parent");
        let backup_prefix = format!(
            ".{}.backup-",
            root.file_name()
                .and_then(|name| name.to_str())
                .expect("test root file name")
        );
        let mut backups = Vec::new();
        let mut entries = fs::read_dir(parent).await.expect("read parent dir");
        while let Some(entry) = entries.next_entry().await.expect("read backup entry") {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&backup_prefix))
            {
                backups.push(entry.path());
            }
        }
        assert_eq!(backups.len(), 1);

        let rollback_store = RecordStore::new(backups[0].clone());
        assert!(rollback_store.is_projection_bootstrapped());
        assert_eq!(
            rollback_store
                .list("rooms", None, Some(10))
                .await
                .expect("list rollback projection")
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
        for backup in backups {
            if backup.exists() {
                let _ = fs::remove_dir_all(backup).await;
            }
        }
    }

    #[tokio::test]
    async fn staged_rebuild_replaces_projection_only_on_commit() {
        let root = test_root("staged-commit");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();
        let no_orders = BTreeMap::new();
        let initial_records = vec![record("a", "Alpha", 1)];
        let rebuilt_records = vec![record("b", "Beta", 2)];

        store
            .force_rebuild_from_records_with_indexes(&initial_records, &no_indexes, &no_orders)
            .await
            .expect("initial rebuild");
        let staged = store
            .stage_rebuild_from_records_with_indexes(&rebuilt_records, &no_indexes, &no_orders)
            .await
            .expect("stage rebuild");

        assert_eq!(
            store
                .list("rooms", None, Some(10))
                .await
                .expect("list before commit")
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a"]
        );

        staged.commit().await.expect("commit staged rebuild");
        assert_eq!(
            store
                .list("rooms", None, Some(10))
                .await
                .expect("list after commit")
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn top_level_list_uses_fjall_key_order_projection() {
        let root = test_root("key-order");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        store
            .upsert_with_indexes_and_order(&record("c", "Gamma", 3), &no_indexes, None)
            .await
            .expect("upsert c");
        store
            .upsert_with_indexes_and_order(&record("a", "Alpha", 1), &no_indexes, None)
            .await
            .expect("upsert a");
        store
            .upsert_with_indexes_and_order(&record("b", "Beta", 2), &no_indexes, None)
            .await
            .expect("upsert b");

        let first_page = store
            .list("rooms", None, Some(2))
            .await
            .expect("list first page");
        assert_eq!(
            first_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(store.projection_store_dir().exists());
        assert!(store.projection_bootstrap_marker().exists());
        assert!(!store.key_order_dir("rooms").join(".complete").exists());
        let status = store.projection_status().await.expect("projection status");
        assert_eq!(status.records, 3);
        assert_eq!(status.key_order_entries, 3);
        assert_eq!(status.recent_entries, 3);

        let second_page = store
            .list("rooms", Some("b"), Some(2))
            .await
            .expect("list second page");
        assert_eq!(
            second_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );

        store.delete("rooms", "b").await.expect("delete b");
        store
            .upsert_with_indexes_and_order(&record("d", "Delta", 4), &no_indexes, None)
            .await
            .expect("upsert d");
        let maintained = store
            .list("rooms", None, Some(10))
            .await
            .expect("list maintained projection");
        assert_eq!(
            maintained
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c", "d"]
        );
        let status = store
            .projection_status()
            .await
            .expect("maintained projection status");
        assert_eq!(status.key_order_entries, 3);
        assert_eq!(status.recent_entries, 3);
        assert!(!store.key_order_dir("rooms").join(".complete").exists());

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn base_records_are_fjall_authoritative_without_json_files() {
        let root = test_root("base-record-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        store
            .upsert_with_indexes_and_order(&record("a", "Alpha", 1), &no_indexes, None)
            .await
            .expect("upsert a");
        store
            .upsert_with_indexes_and_order(&record("b", "Beta", 2), &no_indexes, None)
            .await
            .expect("upsert b");

        assert!(!store.record_path("rooms", "a").exists());
        assert!(!store.record_path("rooms", "b").exists());
        assert!(!store.table_dir("rooms").exists());

        let fetched = store
            .get("rooms", "a")
            .await
            .expect("get a")
            .expect("record a");
        assert_eq!(fetched.value["title"], "Alpha");

        let listed = store
            .list("rooms", None, Some(10))
            .await
            .expect("list rooms");
        assert_eq!(
            listed
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let deleted = store.delete("rooms", "a").await.expect("delete a");
        assert!(deleted);
        assert!(!store.record_path("rooms", "a").exists());
        assert!(
            store
                .get("rooms", "a")
                .await
                .expect("get deleted a")
                .is_none()
        );

        let status = store.projection_status().await.expect("projection status");
        assert_eq!(status.records, 1);
        assert_eq!(status.key_order_entries, 1);
        assert_eq!(status.recent_entries, 1);

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn projection_status_reports_fjall_after_legacy_projection_removal() {
        let root = test_root("fjall-status");
        let store = RecordStore::new(root.clone());
        let order = parse_record_order_terms(&["desc(createdAtMs)".to_string(), "id".to_string()])
            .expect("parse order");
        let mut room_indexes = BTreeMap::new();
        room_indexes.insert(
            "byTitle".to_string(),
            IndexSchema {
                fields: vec!["title".to_string()],
                unique: false,
            },
        );
        let indexes = BTreeMap::from([("rooms".to_string(), room_indexes)]);

        for record in [record("a", "Alpha", 1), record("b", "Beta", 2)] {
            store
                .upsert_with_indexes_and_order(
                    &record,
                    indexes.get("rooms").expect("rooms indexes"),
                    None,
                )
                .await
                .expect("upsert indexed record");
        }
        let no_indexes = BTreeMap::new();
        for record in [
            nested_record("room-a", "m1", 3),
            nested_record("room-a", "m2", 4),
            nested_record("room-a", "m3", 5),
        ] {
            store
                .upsert_with_indexes_and_order(&record, &no_indexes, Some(&order))
                .await
                .expect("upsert ordered nested record");
        }

        for path in [
            store.table_dir("rooms"),
            store.table_dir("rooms.messages"),
            store.root.join("_key_order"),
            store.root.join("_recent"),
            store.root.join("_indexes"),
            store.root.join("_partitions"),
            store.root.join("_orders"),
        ] {
            assert!(!path.exists());
        }

        let status = store
            .projection_status()
            .await
            .expect("fjall projection status");
        assert_eq!(status.records, 5);
        assert_eq!(status.key_order_entries, 5);
        assert_eq!(status.recent_entries, 5);
        assert_eq!(status.index_entries, 2);
        assert_eq!(status.partition_entries, 3);
        assert_eq!(status.order_entries, 3);

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn fjall_projection_bootstraps_from_legacy_json_records() {
        let root = test_root("legacy-fjall-bootstrap");
        let store = RecordStore::new(root.clone());
        let legacy_a = record("a", "Alpha", 1);
        let legacy_b = record("b", "Beta", 2);
        fs::create_dir_all(store.table_dir("rooms"))
            .await
            .expect("create legacy table dir");
        fs::write(
            store.record_path("rooms", "b"),
            serde_json::to_vec(&legacy_b).expect("encode b"),
        )
        .await
        .expect("write legacy b");
        fs::write(
            store.record_path("rooms", "a"),
            serde_json::to_vec(&legacy_a).expect("encode a"),
        )
        .await
        .expect("write legacy a");

        let listed = store
            .list("rooms", None, Some(10))
            .await
            .expect("bootstrap and list");
        assert_eq!(
            listed
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(store.projection_bootstrap_marker().exists());

        fs::remove_dir_all(store.table_dir("rooms"))
            .await
            .expect("remove legacy json table");
        let listed_after_legacy_removal = store
            .list("rooms", None, Some(10))
            .await
            .expect("list from fjall after legacy removal");
        assert_eq!(
            listed_after_legacy_removal
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let legacy_c = record("c", "Gamma", 3);
        fs::create_dir_all(store.table_dir("rooms"))
            .await
            .expect("recreate legacy table dir");
        fs::write(
            store.record_path("rooms", "c"),
            serde_json::to_vec(&legacy_c).expect("encode c"),
        )
        .await
        .expect("write post-bootstrap legacy c");
        assert!(
            store
                .get("rooms", "c")
                .await
                .expect("get post-bootstrap legacy c")
                .is_none()
        );
        let listed_after_post_bootstrap_legacy_write = store
            .list("rooms", None, Some(10))
            .await
            .expect("list ignores post-bootstrap legacy write");
        assert_eq!(
            listed_after_post_bootstrap_legacy_write
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn nested_partition_uses_fjall_projection() {
        let root = test_root("partition-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        store
            .upsert_with_indexes_and_order(&nested_record("room-a", "m3", 3), &no_indexes, None)
            .await
            .expect("upsert m3");
        store
            .upsert_with_indexes_and_order(&nested_record("room-a", "m1", 1), &no_indexes, None)
            .await
            .expect("upsert m1");
        store
            .upsert_with_indexes_and_order(&nested_record("room-a", "m2", 2), &no_indexes, None)
            .await
            .expect("upsert m2");

        let first_page = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(2))
            .await
            .expect("list partition first page");
        assert_eq!(
            first_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m2"]
        );
        assert!(
            !store
                .partition_dir("rooms.messages", "room-a")
                .join(".complete")
                .exists()
        );

        assert!(!store.partition_dir("rooms.messages", "room-a").exists());

        let second_page = store
            .list_by_key_prefix("rooms.messages", "room-a:", Some("room-a:m2"), Some(2))
            .await
            .expect("list partition second page");
        assert_eq!(
            second_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m3"]
        );

        store
            .delete("rooms.messages", "room-a:m2")
            .await
            .expect("delete m2");
        store
            .upsert_with_indexes_and_order(&nested_record("room-a", "m4", 4), &no_indexes, None)
            .await
            .expect("upsert m4");
        assert!(!store.partition_dir("rooms.messages", "room-a").exists());

        let maintained = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .await
            .expect("list maintained partition");
        assert_eq!(
            maintained
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m3", "room-a:m4"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn batch_transaction_maintains_partition_projection_after_upserts() {
        let root = test_root("partition-batch-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        let m1 = nested_record("room-a", "m1", 1);
        store
            .upsert_with_indexes_and_order(&m1, &no_indexes, None)
            .await
            .expect("upsert m1");
        let first_page = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .await
            .expect("materialize partition");
        assert_eq!(
            first_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1"]
        );
        assert!(
            !store
                .partition_dir("rooms.messages", "room-a")
                .join(".complete")
                .exists()
        );

        let m2 = nested_record("room-a", "m2", 2);
        let m3 = nested_record("room-a", "m3", 3);
        store
            .apply_transaction(&[
                RecordStoreMutation::Upsert {
                    record: &m2,
                    indexes: &no_indexes,
                    order: None,
                },
                RecordStoreMutation::Upsert {
                    record: &m3,
                    indexes: &no_indexes,
                    order: None,
                },
            ])
            .await
            .expect("batch upsert partition records");
        assert!(!store.partition_dir("rooms.messages", "room-a").exists());

        let listed = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .await
            .expect("list batch-maintained partition");
        assert_eq!(
            listed
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m2", "room-a:m3"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn batch_transaction_maintains_partition_projection_after_delete_and_upsert() {
        let root = test_root("partition-batch-delete-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        let records = vec![
            nested_record("room-a", "m1", 1),
            nested_record("room-a", "m2", 2),
            nested_record("room-a", "m3", 3),
        ];
        for record in &records {
            store
                .upsert_with_indexes_and_order(record, &no_indexes, None)
                .await
                .expect("seed partition record");
        }
        store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .await
            .expect("list seeded partition");

        let m4 = nested_record("room-a", "m4", 4);
        store
            .apply_transaction(&[
                RecordStoreMutation::Delete {
                    table: "rooms.messages",
                    key: "room-a:m2",
                },
                RecordStoreMutation::Upsert {
                    record: &m4,
                    indexes: &no_indexes,
                    order: None,
                },
            ])
            .await
            .expect("batch delete and upsert partition records");
        assert!(!store.partition_dir("rooms.messages", "room-a").exists());

        let listed = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .await
            .expect("list batch delete-maintained partition");
        assert_eq!(
            listed
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m3", "room-a:m4"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn batch_transaction_maintains_index_projection_after_deletes() {
        let root = test_root("index-batch-delete-fjall");
        let store = RecordStore::new(root.clone());
        let records = vec![
            record("a", "Alpha", 1),
            record("b", "Beta", 2),
            record("c", "Beta", 3),
        ];
        let mut room_indexes = BTreeMap::new();
        room_indexes.insert(
            "byTitle".to_string(),
            IndexSchema {
                fields: vec!["title".to_string()],
                unique: false,
            },
        );
        let indexes = BTreeMap::from([("rooms".to_string(), room_indexes)]);
        let no_orders = BTreeMap::new();
        store
            .force_rebuild_from_records_with_indexes(&records, &indexes, &no_orders)
            .await
            .expect("rebuild indexed records");
        let beta_before = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, Some(10))
            .await
            .expect("query beta index before delete");
        assert_eq!(
            beta_before
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        store
            .apply_transaction(&[
                RecordStoreMutation::Delete {
                    table: "rooms",
                    key: "b",
                },
                RecordStoreMutation::Delete {
                    table: "rooms",
                    key: "c",
                },
            ])
            .await
            .expect("batch delete indexed records");

        let beta_after = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, Some(10))
            .await
            .expect("query beta index after delete");
        assert!(beta_after.is_empty());
        let status = store
            .projection_status()
            .await
            .expect("projection status after index deletes");
        assert_eq!(status.index_entries, 1);

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn batch_transaction_maintains_order_projection_after_deletes() {
        let root = test_root("order-batch-delete-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();
        let order = parse_record_order_terms(&["desc(createdAtMs)".to_string(), "id".to_string()])
            .expect("parse order");
        let records = vec![
            nested_record("room-a", "m1", 1),
            nested_record("room-a", "m2", 2),
            nested_record("room-a", "m3", 3),
        ];
        for record in &records {
            store
                .upsert_with_indexes_and_order(record, &no_indexes, Some(&order))
                .await
                .expect("seed ordered record");
        }
        let before = store
            .list_ordered_partition("rooms.messages", "room-a", &order, None, None, Some(10))
            .await
            .expect("materialize ordered partition");
        assert_eq!(
            before
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m3", "room-a:m2", "room-a:m1"]
        );
        let order_dir = store.order_dir("rooms.messages", "room-a", &order);
        assert!(!order_dir.exists());

        store
            .apply_transaction(&[
                RecordStoreMutation::Delete {
                    table: "rooms.messages",
                    key: "room-a:m2",
                },
                RecordStoreMutation::Delete {
                    table: "rooms.messages",
                    key: "room-a:m3",
                },
            ])
            .await
            .expect("batch delete ordered records");
        assert!(!order_dir.exists());

        let after = store
            .list_ordered_partition("rooms.messages", "room-a", &order, None, None, Some(10))
            .await
            .expect("list batch-deleted ordered partition");
        assert_eq!(
            after
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn ordered_partition_uses_fjall_projection() {
        let root = test_root("order-fjall");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();
        let order = parse_record_order_terms(&["desc(createdAtMs)".to_string(), "id".to_string()])
            .expect("parse order");
        let records = vec![
            nested_record("room-a", "m3", 3),
            nested_record("room-a", "m1", 1),
            nested_record("room-a", "m2", 2),
        ];

        for record in &records {
            store
                .upsert_with_indexes_and_order(record, &no_indexes, Some(&order))
                .await
                .expect("upsert ordered record");
        }

        let first_page = store
            .list_ordered_partition("rooms.messages", "room-a", &order, None, None, Some(2))
            .await
            .expect("list ordered first page");
        assert_eq!(
            first_page
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m3", "room-a:m2"]
        );
        let order_dir = store.order_dir("rooms.messages", "room-a", &order);
        assert!(!order_dir.exists());

        let second_page = store
            .list_ordered_partition(
                "rooms.messages",
                "room-a",
                &order,
                None,
                Some(&first_page.last().expect("cursor").cursor),
                Some(2),
            )
            .await
            .expect("list ordered second page");
        assert_eq!(
            second_page
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1"]
        );

        store
            .delete("rooms.messages", "room-a:m2")
            .await
            .expect("delete ordered m2");
        let m4 = nested_record("room-a", "m4", 4);
        store
            .upsert_with_indexes_and_order(&m4, &no_indexes, Some(&order))
            .await
            .expect("upsert ordered m4");
        assert!(!order_dir.exists());

        let maintained = store
            .list_ordered_partition("rooms.messages", "room-a", &order, None, None, Some(10))
            .await
            .expect("list maintained ordered partition");
        assert_eq!(
            maintained
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m4", "room-a:m3", "room-a:m1"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn list_recent_uses_fjall_projection() {
        let root = test_root("recent");
        let store = RecordStore::new(root.clone());
        let no_indexes = BTreeMap::new();

        store
            .upsert_with_indexes_and_order(&record("a", "Alpha", 1), &no_indexes, None)
            .await
            .expect("upsert a");
        store
            .upsert_with_indexes_and_order(&record("b", "Beta", 3), &no_indexes, None)
            .await
            .expect("upsert b");
        store
            .upsert_with_indexes_and_order(&record("c", "Gamma", 2), &no_indexes, None)
            .await
            .expect("upsert c");

        let recent = store
            .list_recent("rooms", Some(2))
            .await
            .expect("list recent");
        assert_eq!(
            recent
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert!(!store.recent_dir("rooms").exists());

        store
            .upsert_with_indexes_and_order(&record("a", "Alpha Updated", 4), &no_indexes, None)
            .await
            .expect("update a");
        store.delete("rooms", "b").await.expect("delete b");
        assert!(!store.recent_dir("rooms").exists());
        let maintained = store
            .list_recent("rooms", Some(10))
            .await
            .expect("list maintained recent");
        assert_eq!(
            maintained
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn index_range_query_orders_by_index_value_and_paginates_by_cursor() {
        let root = test_root("index-range");
        let store = RecordStore::new(root.clone());
        let records = vec![
            record("a", "Alpha", 1),
            record("b", "Beta", 2),
            record("c", "Delta", 3),
            record("d", "Gamma", 4),
        ];
        let mut room_indexes = BTreeMap::new();
        room_indexes.insert(
            "byTitle".to_string(),
            IndexSchema {
                fields: vec!["title".to_string()],
                unique: false,
            },
        );
        let mut indexes = BTreeMap::new();
        indexes.insert("rooms".to_string(), room_indexes);
        let no_orders = BTreeMap::new();
        store
            .force_rebuild_from_records_with_indexes(&records, &indexes, &no_orders)
            .await
            .expect("rebuild indexes");

        let page = store
            .query_index_range(
                "rooms",
                "byTitle",
                Some(&[json!("Beta")]),
                Some(&[json!("Gamma")]),
                None,
                None,
                Some(2),
            )
            .await
            .expect("range query");

        assert_eq!(
            page.iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        let next = store
            .query_index_range(
                "rooms",
                "byTitle",
                Some(&[json!("Beta")]),
                Some(&[json!("Gamma")]),
                None,
                Some(&page.last().expect("cursor").cursor),
                Some(2),
            )
            .await
            .expect("second range page");

        assert_eq!(
            next.iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["d"]
        );

        let beta_exact = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, Some(10))
            .await
            .expect("exact index query");
        assert_eq!(
            beta_exact
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
        if store.index_table_dir("rooms").exists() {
            fs::remove_dir_all(store.index_table_dir("rooms"))
                .await
                .expect("remove legacy index projection");
        }
        let beta_without_legacy = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, Some(10))
            .await
            .expect("exact index query without legacy files");
        assert_eq!(
            beta_without_legacy
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
        let range_without_legacy = store
            .query_index_range(
                "rooms",
                "byTitle",
                Some(&[json!("Beta")]),
                Some(&[json!("Gamma")]),
                None,
                None,
                Some(10),
            )
            .await
            .expect("range query without legacy files");
        assert_eq!(
            range_without_legacy
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "d"]
        );

        store
            .upsert_with_indexes_and_order(
                &record("b", "Omega", 5),
                indexes.get("rooms").expect("room indexes"),
                None,
            )
            .await
            .expect("update indexed record");
        let beta_after_update = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, Some(10))
            .await
            .expect("exact index query after update");
        assert!(beta_after_update.is_empty());

        let omega_exact = store
            .query_index("rooms", "byTitle", &[json!("Omega")], None, Some(10))
            .await
            .expect("new exact index query");
        assert_eq!(
            omega_exact
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn ordered_partition_uses_bounded_file_names_for_long_nested_keys() {
        let root = test_root("ordered-long-key");
        let store = RecordStore::new(root.clone());
        let parent_key = "room-with-a-long-but-valid-actor-partition-key-1234567890";
        let nested_key = "message-with-a-long-but-valid-clustering-key-1234567890";
        let order = parse_record_order_terms(&["desc(createdAtMs)".to_string(), "id".to_string()])
            .expect("parse order");
        let record = nested_record(parent_key, nested_key, 1_782_556_232_626);

        store
            .upsert_with_indexes_and_order(&record, &BTreeMap::new(), Some(&order))
            .await
            .expect("upsert ordered record");

        let page = store
            .list_ordered_partition("rooms.messages", parent_key, &order, None, None, Some(10))
            .await
            .expect("list ordered partition");

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].record.key, record.key);
        assert!(page[0].cursor.contains(&hex_lower(record.key.as_bytes())));
        assert!(
            !store
                .order_dir("rooms.messages", parent_key, &order)
                .exists()
        );

        store
            .delete("rooms.messages", &record.key)
            .await
            .expect("delete ordered record");
        let empty = store
            .list_ordered_partition("rooms.messages", parent_key, &order, None, None, Some(10))
            .await
            .expect("list empty ordered partition");
        assert!(empty.is_empty());

        if root.exists() {
            let _ = fs::remove_dir_all(root).await;
        }
    }
}
