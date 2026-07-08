use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{fs, sync::mpsc, sync::watch};
use tracing::error;

use crate::{
    model::{BinaryJsonValue, DbRecord},
    record_hot::RecordHotCache,
    record_store::{RecordOrderTerm, RecordStore, RecordStoreMutation},
    schema::IndexSchema,
    util::{decode_hex, hex_lower, normalize_limit},
};

const RECORDS_KEYSPACE: &str = "records";
const INDEXES_KEYSPACE: &str = "record_indexes";
const INDEX_REFS_KEYSPACE: &str = "record_index_refs";
const PARTITIONS_KEYSPACE: &str = "record_partitions";
const RECENT_KEYSPACE: &str = "record_recent";
const RECENT_REFS_KEYSPACE: &str = "record_recent_refs";
const ORDERS_KEYSPACE: &str = "record_orders";
const ORDER_REFS_KEYSPACE: &str = "record_order_refs";
const RECORD_PROJECTION_APPLIER_QUEUE: usize = 65_536;
const PROJECTED_RECORD_MAGIC: &[u8; 8] = b"NDBREC01";
const INDEX_VALUES_MAGIC: &[u8; 8] = b"NDBIDX01";

pub struct IndexProjectionEntry {
    pub values: Vec<Value>,
    pub record: DbRecord,
}

pub struct OrderedProjectionEntry {
    pub record: DbRecord,
    pub cursor: String,
}

pub struct IndexRangeProjectionQuery<'a> {
    pub table: &'a str,
    pub index_name: &'a str,
    pub lower: Option<&'a [Value]>,
    pub upper: Option<&'a [Value]>,
    pub key_prefix: Option<&'a str>,
    pub after_cursor: Option<(&'a [Value], &'a str)>,
    pub limit: Option<usize>,
}

pub struct RecordProjectionKv {
    db: Database,
    records: Keyspace,
    indexes: Keyspace,
    index_refs: Keyspace,
    partitions: Keyspace,
    recent: Keyspace,
    recent_refs: Keyspace,
    orders: Keyspace,
    order_refs: Keyspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordProjectionKvStatus {
    pub records: usize,
    pub key_order_entries: usize,
    pub recent_entries: usize,
    pub index_entries: usize,
    pub partition_entries: usize,
    pub order_entries: usize,
}

#[derive(Clone)]
pub(crate) struct RecordProjectionApplier {
    tx: mpsc::Sender<RecordProjectionJob>,
    #[cfg_attr(not(test), allow(dead_code))]
    applied_lsn: watch::Receiver<u64>,
}

enum RecordProjectionJob {
    Upsert {
        lsn: u64,
        record: DbRecord,
        indexes: std::collections::BTreeMap<String, IndexSchema>,
        order: Option<Vec<RecordOrderTerm>>,
    },
    Delete {
        lsn: u64,
        table: String,
        key: String,
    },
    Transaction {
        lsn: u64,
        mutations: Vec<RecordProjectionMutation>,
    },
}

pub(crate) enum RecordProjectionMutation {
    Upsert {
        record: DbRecord,
        indexes: std::collections::BTreeMap<String, IndexSchema>,
        order: Option<Vec<RecordOrderTerm>>,
    },
    Delete {
        table: String,
        key: String,
    },
}

impl RecordProjectionApplier {
    pub(crate) fn spawn(records: RecordStore, record_hot: RecordHotCache) -> Self {
        let (tx, mut rx) = mpsc::channel(RECORD_PROJECTION_APPLIER_QUEUE);
        let (applied_tx, applied_lsn) = watch::channel(0);
        tokio::spawn(async move {
            let mut highest_applied_lsn = 0_u64;
            while let Some(job) = rx.recv().await {
                match job {
                    RecordProjectionJob::Upsert {
                        lsn,
                        record,
                        indexes,
                        order,
                    } => {
                        if let Err(err) = records
                            .upsert_with_indexes_and_order(&record, &indexes, order.as_deref())
                            .await
                        {
                            error!(
                                error = %err,
                                lsn,
                                table = %record.table,
                                key = %record.key,
                                "record projection applier failed"
                            );
                            continue;
                        }
                        highest_applied_lsn = highest_applied_lsn.max(lsn);
                        applied_tx.send_replace(highest_applied_lsn);
                    }
                    RecordProjectionJob::Delete { lsn, table, key } => {
                        if let Err(err) = records.delete(&table, &key).await {
                            error!(
                                error = %err,
                                lsn,
                                table = %table,
                                key = %key,
                                "record projection applier failed"
                            );
                            continue;
                        }
                        record_hot
                            .clear_durable_delete_tombstone(&table, &key, lsn)
                            .await;
                        highest_applied_lsn = highest_applied_lsn.max(lsn);
                        applied_tx.send_replace(highest_applied_lsn);
                    }
                    RecordProjectionJob::Transaction { lsn, mutations } => {
                        let store_operations = mutations
                            .iter()
                            .map(|mutation| match mutation {
                                RecordProjectionMutation::Upsert {
                                    record,
                                    indexes,
                                    order,
                                } => RecordStoreMutation::Upsert {
                                    record,
                                    indexes,
                                    order: order.clone(),
                                },
                                RecordProjectionMutation::Delete { table, key } => {
                                    RecordStoreMutation::Delete { table, key }
                                }
                            })
                            .collect::<Vec<_>>();
                        if let Err(err) = records.apply_transaction(&store_operations).await {
                            error!(
                                error = %err,
                                lsn,
                                "record projection applier failed"
                            );
                            continue;
                        }
                        for mutation in mutations {
                            if let RecordProjectionMutation::Delete { table, key } = mutation {
                                record_hot
                                    .clear_durable_delete_tombstone(&table, &key, lsn)
                                    .await;
                            }
                        }
                        highest_applied_lsn = highest_applied_lsn.max(lsn);
                        applied_tx.send_replace(highest_applied_lsn);
                    }
                }
            }
        });
        Self { tx, applied_lsn }
    }

    #[cfg(test)]
    pub(crate) fn paused_for_test() -> Self {
        let (tx, rx) = mpsc::channel(RECORD_PROJECTION_APPLIER_QUEUE);
        let (applied_tx, applied_lsn) = watch::channel(0);
        tokio::spawn(async move {
            let _rx = rx;
            let _applied_tx = applied_tx;
            std::future::pending::<()>().await;
        });
        Self { tx, applied_lsn }
    }

    pub(crate) async fn enqueue_upsert(
        &self,
        lsn: u64,
        record: DbRecord,
        indexes: std::collections::BTreeMap<String, IndexSchema>,
        order: Option<Vec<RecordOrderTerm>>,
    ) -> Result<()> {
        self.tx
            .send(RecordProjectionJob::Upsert {
                lsn,
                record,
                indexes,
                order,
            })
            .await
            .context("record projection applier stopped")
    }

    pub(crate) async fn enqueue_transaction(
        &self,
        lsn: u64,
        mutations: Vec<RecordProjectionMutation>,
    ) -> Result<()> {
        self.tx
            .send(RecordProjectionJob::Transaction { lsn, mutations })
            .await
            .context("record projection applier stopped")
    }

    pub(crate) async fn enqueue_delete(&self, lsn: u64, table: String, key: String) -> Result<()> {
        self.tx
            .send(RecordProjectionJob::Delete { lsn, table, key })
            .await
            .context("record projection applier stopped")
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn wait_for_lsn(&self, lsn: u64) -> Result<()> {
        let mut applied_lsn = self.applied_lsn.clone();
        while *applied_lsn.borrow_and_update() < lsn {
            applied_lsn
                .changed()
                .await
                .context("record projection applier stopped")?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct IndexRef {
    index_name: String,
    values: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BinaryIndexValues(Vec<BinaryJsonValue>);

#[derive(Debug, Deserialize, Serialize)]
struct ProjectedRecordEnvelope {
    table: String,
    key: String,
    value: BinaryJsonValue,
    updated_at_ms: u64,
    lsn: u64,
    path: String,
}

pub(crate) fn encode_projected_record(record: &DbRecord) -> Result<Vec<u8>> {
    let envelope = ProjectedRecordEnvelope {
        table: record.table.clone(),
        key: record.key.clone(),
        value: BinaryJsonValue::from_json(&record.value)?,
        updated_at_ms: record.updated_at_ms,
        lsn: record.lsn,
        path: record.path.clone(),
    };
    let postcard = postcard::to_allocvec(&envelope)?;
    let mut encoded = Vec::with_capacity(PROJECTED_RECORD_MAGIC.len() + postcard.len());
    encoded.extend_from_slice(PROJECTED_RECORD_MAGIC);
    encoded.extend_from_slice(&postcard);
    Ok(encoded)
}

fn decode_projected_record(bytes: &[u8]) -> Result<DbRecord> {
    if !bytes.starts_with(PROJECTED_RECORD_MAGIC) {
        return serde_json::from_slice(bytes).context("parse legacy JSON record projection");
    }
    let envelope: ProjectedRecordEnvelope =
        postcard::from_bytes(&bytes[PROJECTED_RECORD_MAGIC.len()..])
            .context("parse postcard record projection envelope")?;
    Ok(DbRecord {
        table: envelope.table,
        key: envelope.key,
        value: envelope
            .value
            .into_json()
            .context("decode record projection value")?,
        updated_at_ms: envelope.updated_at_ms,
        lsn: envelope.lsn,
        path: envelope.path,
    })
}

impl RecordProjectionKv {
    pub fn open(path: PathBuf) -> Result<Self> {
        let db = Database::builder(path).open()?;
        let records = db.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)?;
        let indexes = db.keyspace(INDEXES_KEYSPACE, KeyspaceCreateOptions::default)?;
        let index_refs = db.keyspace(INDEX_REFS_KEYSPACE, KeyspaceCreateOptions::default)?;
        let partitions = db.keyspace(PARTITIONS_KEYSPACE, KeyspaceCreateOptions::default)?;
        let recent = db.keyspace(RECENT_KEYSPACE, KeyspaceCreateOptions::default)?;
        let recent_refs = db.keyspace(RECENT_REFS_KEYSPACE, KeyspaceCreateOptions::default)?;
        let orders = db.keyspace(ORDERS_KEYSPACE, KeyspaceCreateOptions::default)?;
        let order_refs = db.keyspace(ORDER_REFS_KEYSPACE, KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            records,
            indexes,
            index_refs,
            partitions,
            recent,
            recent_refs,
            orders,
            order_refs,
        })
    }

    pub fn put_record_bytes(&self, record: &DbRecord, record_bytes: &[u8]) -> Result<()> {
        let recent_ref_key = recent_ref_key(&record.table, &record.key);
        let previous_recent_key = self.recent_refs.get(&recent_ref_key)?;
        let recent_key = recent_key(record);

        let mut batch = self.db.batch();
        batch.insert(
            &self.records,
            record_key(&record.table, &record.key),
            record_bytes.to_vec(),
        );
        if let Some(previous_recent_key) = previous_recent_key {
            batch.remove(&self.recent, previous_recent_key.as_ref().to_vec());
        }
        batch.insert(&self.recent, recent_key.clone(), record_bytes.to_vec());
        batch.insert(&self.recent_refs, recent_ref_key, recent_key);
        if let Some((parent_key, nested_key)) = split_nested_key(&record.key) {
            batch.insert(
                &self.partitions,
                partition_key(&record.table, parent_key, nested_key),
                record_bytes.to_vec(),
            );
        }
        batch.commit()?;
        Ok(())
    }

    pub fn remove_record(&self, table: &str, key: &str) -> Result<()> {
        let recent_ref_key = recent_ref_key(table, key);
        let previous_recent_key = self.recent_refs.get(&recent_ref_key)?;
        let order_removals = self.order_removals(table, key)?;

        let mut batch = self.db.batch();
        batch.remove(&self.records, record_key(table, key));
        if let Some(previous_recent_key) = previous_recent_key {
            batch.remove(&self.recent, previous_recent_key.as_ref().to_vec());
            batch.remove(&self.recent_refs, recent_ref_key);
        }
        if let Some((parent_key, nested_key)) = split_nested_key(key) {
            batch.remove(
                &self.partitions,
                partition_key(table, parent_key, nested_key),
            );
        }
        for (reference_key, order_key) in order_removals {
            batch.remove(&self.order_refs, reference_key);
            batch.remove(&self.orders, order_key);
        }
        batch.commit()?;
        Ok(())
    }

    pub fn get(&self, table: &str, key: &str) -> Result<Option<DbRecord>> {
        let Some(bytes) = self.records.get(record_key(table, key))? else {
            return Ok(None);
        };
        decode_projected_record(bytes.as_ref())
            .map(Some)
            .with_context(|| format!("decode fjall record projection for {table}/{key}"))
    }

    pub fn list(
        &self,
        table: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        self.list_by_key_prefix(table, "", after_key, limit)
    }

    pub fn list_by_key_prefix(
        &self,
        table: &str,
        key_prefix: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        let limit = normalize_limit(limit);
        let prefix = record_prefix(table, key_prefix);
        let mut records = Vec::new();
        for guard in self.records.prefix(prefix) {
            let bytes = guard.value()?;
            let record = decode_projected_record(bytes.as_ref())
                .with_context(|| format!("decode fjall record projection for table {table}"))?;
            if after_key.is_some_and(|after| record.key.as_str() <= after) {
                continue;
            }
            records.push(record);
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn list_partition(
        &self,
        table: &str,
        parent_key: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        let limit = normalize_limit(limit);
        let prefix = partition_prefix(table, parent_key);
        let after_nested_key = partition_after_nested_key(parent_key, after_key);
        let mut records = Vec::new();
        for guard in self.partitions.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            let Some(nested_key) = parse_partition_key(table, parent_key, key.as_ref()) else {
                continue;
            };
            if after_nested_key
                .as_deref()
                .is_some_and(|after| nested_key.as_str() <= after)
            {
                continue;
            }
            let record = decode_projected_record(value.as_ref()).with_context(|| {
                format!("decode fjall partition projection for {table}/{parent_key}")
            })?;
            records.push(record);
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn list_recent(&self, table: &str, limit: Option<usize>) -> Result<Vec<DbRecord>> {
        let limit = normalize_limit(limit);
        let prefix = recent_prefix(table);
        let mut records = Vec::new();
        for guard in self.recent.prefix(prefix) {
            let bytes = guard.value()?;
            records
                .push(decode_projected_record(bytes.as_ref()).with_context(|| {
                    format!("decode fjall recent projection for table {table}")
                })?);
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn put_order_entry(
        &self,
        record: &DbRecord,
        order_id: &str,
        cursor: &str,
        record_bytes: &[u8],
    ) -> Result<()> {
        let Some((parent_key, _nested_key)) = split_nested_key(&record.key) else {
            return Ok(());
        };
        let order_ref_key = order_ref_key(&record.table, &record.key, order_id);
        let previous_order_key = self.order_refs.get(&order_ref_key)?;
        let order_key = order_key(&record.table, parent_key, order_id, cursor, &record.key);

        let mut batch = self.db.batch();
        if let Some(previous_order_key) = previous_order_key {
            batch.remove(&self.orders, previous_order_key.as_ref().to_vec());
        }
        batch.insert(&self.orders, order_key.clone(), record_bytes.to_vec());
        batch.insert(&self.order_refs, order_ref_key, order_key);
        batch.commit()?;
        Ok(())
    }

    pub fn remove_order_entries(&self, table: &str, key: &str) -> Result<()> {
        let removals = self.order_removals(table, key)?;
        if removals.is_empty() {
            return Ok(());
        }
        let mut batch = self.db.batch();
        for (reference_key, order_key) in removals {
            batch.remove(&self.order_refs, reference_key);
            batch.remove(&self.orders, order_key);
        }
        batch.commit()?;
        Ok(())
    }

    pub fn list_ordered_partition(
        &self,
        table: &str,
        parent_key: &str,
        order_id: &str,
        after_key: Option<&str>,
        after_cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<OrderedProjectionEntry>> {
        let limit = normalize_limit(limit);
        let prefix = order_prefix(table, parent_key, order_id);
        let mut entries = Vec::new();
        let mut after_seen = after_key.is_none() || after_cursor.is_some();
        for guard in self.orders.prefix(prefix) {
            let (key, value) = guard.into_inner()?;
            let Some((cursor, record_key)) =
                parse_order_key(table, parent_key, order_id, key.as_ref())
            else {
                continue;
            };
            if after_cursor.is_some_and(|after_cursor| cursor.as_str() <= after_cursor) {
                continue;
            }
            if !after_seen {
                after_seen = Some(record_key.as_str()) == after_key;
                continue;
            }
            let record = decode_projected_record(value.as_ref()).with_context(|| {
                format!("decode fjall ordered projection for {table}/{parent_key}")
            })?;
            entries.push(OrderedProjectionEntry { record, cursor });
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }

    pub fn put_index_entry(
        &self,
        record: &DbRecord,
        index_name: &str,
        values: &[Value],
        unique: bool,
        record_bytes: &[u8],
    ) -> Result<()> {
        if unique {
            for guard in self
                .indexes
                .prefix(index_prefix(&record.table, index_name, values)?)
            {
                let bytes = guard.value()?;
                let existing = decode_projected_record(bytes.as_ref())?;
                if existing.key != record.key {
                    bail!(
                        "unique index violation on {}.{} for record {}",
                        record.table,
                        index_name,
                        record.key
                    );
                }
            }
        }

        let mut batch = self.db.batch();
        batch.insert(
            &self.indexes,
            index_key(&record.table, index_name, values, &record.key)?,
            record_bytes.to_vec(),
        );
        batch.insert(
            &self.index_refs,
            index_ref_key(&record.table, &record.key, index_name, values)?,
            [],
        );
        batch.commit()?;
        Ok(())
    }

    pub fn remove_index_entries(&self, table: &str, key: &str) -> Result<()> {
        let mut removals = Vec::new();
        for guard in self.index_refs.prefix(index_ref_prefix(table, key)) {
            let reference_key = guard.key()?;
            let Some(index_ref) = parse_index_ref_key(reference_key.as_ref())? else {
                continue;
            };
            removals.push((
                reference_key.to_vec(),
                index_key(table, &index_ref.index_name, &index_ref.values, key)?,
            ));
        }
        if removals.is_empty() {
            return Ok(());
        }

        let mut batch = self.db.batch();
        for (reference_key, index_key) in removals {
            batch.remove(&self.index_refs, reference_key);
            batch.remove(&self.indexes, index_key);
        }
        batch.commit()?;
        Ok(())
    }

    pub fn query_index(
        &self,
        table: &str,
        index_name: &str,
        values: &[Value],
        key_prefix: Option<&str>,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DbRecord>> {
        let limit = normalize_limit(limit);
        let prefix = match key_prefix {
            Some(key_prefix) => index_key_prefix(table, index_name, values, key_prefix)?,
            None => index_prefix(table, index_name, values)?,
        };
        let mut records = Vec::new();
        for guard in self.indexes.prefix(prefix) {
            let bytes = guard.value()?;
            let record = decode_projected_record(bytes.as_ref())?;
            if after_key.is_some_and(|after| record.key.as_str() <= after) {
                continue;
            }
            records.push(record);
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn query_index_range(
        &self,
        query: IndexRangeProjectionQuery<'_>,
    ) -> Result<Vec<IndexProjectionEntry>> {
        let limit = normalize_limit(query.limit);
        let mut entries = Vec::new();
        for guard in self
            .indexes
            .prefix(index_table_prefix(query.table, query.index_name))
        {
            let (key, value) = guard.into_inner()?;
            let Some((values, record_key)) =
                parse_index_key(query.table, query.index_name, key.as_ref())?
            else {
                continue;
            };
            if query
                .lower
                .is_some_and(|lower| compare_index_values(&values, lower) == Ordering::Less)
            {
                continue;
            }
            if query
                .upper
                .is_some_and(|upper| compare_index_values(&values, upper) == Ordering::Greater)
            {
                continue;
            }
            if query
                .key_prefix
                .is_some_and(|prefix| !record_key.starts_with(prefix))
            {
                continue;
            }
            let record = decode_projected_record(value.as_ref())?;
            entries.push(IndexProjectionEntry { values, record });
        }
        entries.sort_by(|left, right| {
            compare_index_values(&left.values, &right.values)
                .then_with(|| left.record.key.cmp(&right.record.key))
        });

        Ok(entries
            .into_iter()
            .filter(|entry| match query.after_cursor {
                Some((after_values, after_key)) => {
                    compare_index_values(&entry.values, after_values)
                        .then_with(|| entry.record.key.as_str().cmp(after_key))
                        == Ordering::Greater
                }
                None => true,
            })
            .take(limit)
            .collect())
    }

    pub fn status(&self) -> Result<RecordProjectionKvStatus> {
        let records = self.records.len()?;
        Ok(RecordProjectionKvStatus {
            records,
            key_order_entries: records,
            recent_entries: self.recent.len()?,
            index_entries: self.indexes.len()?,
            partition_entries: self.partitions.len()?,
            order_entries: self.orders.len()?,
        })
    }

    pub async fn import_legacy_records(&self, root: &Path) -> Result<usize> {
        if !root.exists() {
            return Ok(0);
        }

        let mut imported = 0;
        let mut tables = fs::read_dir(root).await?;
        while let Some(table_entry) = tables.next_entry().await? {
            if !table_entry.file_type().await?.is_dir() {
                continue;
            }
            if table_entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with('_'))
            {
                continue;
            }

            let mut entries = fs::read_dir(table_entry.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_file()
                    || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
                {
                    continue;
                }
                let bytes = fs::read(entry.path()).await?;
                let record = serde_json::from_slice::<DbRecord>(&bytes)?;
                let record_bytes = encode_projected_record(&record)?;
                self.put_record_bytes(&record, &record_bytes)?;
                imported += 1;
            }
        }
        Ok(imported)
    }

    fn order_removals(&self, table: &str, key: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut removals = Vec::new();
        for guard in self.order_refs.prefix(order_ref_prefix(table, key)) {
            let (reference_key, order_key) = guard.into_inner()?;
            removals.push((reference_key.to_vec(), order_key.to_vec()));
        }
        Ok(removals)
    }
}

fn index_table_prefix(table: &str, index_name: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + index_name.len() + 2);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(index_name.as_bytes());
    bytes.push(0);
    bytes
}

fn index_prefix(table: &str, index_name: &str, values: &[Value]) -> Result<Vec<u8>> {
    let encoded_values = encode_index_values_component(values)?;
    let mut bytes = Vec::with_capacity(table.len() + index_name.len() + encoded_values.len() + 3);
    bytes.extend_from_slice(&index_table_prefix(table, index_name));
    bytes.extend_from_slice(encoded_values.as_bytes());
    bytes.push(0);
    Ok(bytes)
}

fn index_key_prefix(
    table: &str,
    index_name: &str,
    values: &[Value],
    key_prefix: &str,
) -> Result<Vec<u8>> {
    let mut bytes = index_prefix(table, index_name, values)?;
    bytes.extend_from_slice(key_prefix.as_bytes());
    Ok(bytes)
}

fn index_key(table: &str, index_name: &str, values: &[Value], key: &str) -> Result<Vec<u8>> {
    let mut bytes = index_prefix(table, index_name, values)?;
    bytes.extend_from_slice(key.as_bytes());
    Ok(bytes)
}

fn parse_index_key(
    table: &str,
    index_name: &str,
    key: &[u8],
) -> Result<Option<(Vec<Value>, String)>> {
    let prefix = index_table_prefix(table, index_name);
    let Some(rest) = key.strip_prefix(prefix.as_slice()) else {
        return Ok(None);
    };
    let Some(separator) = rest.iter().position(|byte| *byte == 0) else {
        return Ok(None);
    };
    let values = decode_index_values_component(&rest[..separator])?;
    let record_key = String::from_utf8_lossy(&rest[separator + 1..]).to_string();
    Ok(Some((values, record_key)))
}

fn index_ref_prefix(table: &str, key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + key.len() + 2);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes
}

fn index_ref_key(table: &str, key: &str, index_name: &str, values: &[Value]) -> Result<Vec<u8>> {
    let encoded_values = encode_index_values_component(values)?;
    let mut bytes =
        Vec::with_capacity(table.len() + key.len() + index_name.len() + encoded_values.len() + 3);
    bytes.extend_from_slice(&index_ref_prefix(table, key));
    bytes.extend_from_slice(index_name.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(encoded_values.as_bytes());
    Ok(bytes)
}

fn parse_index_ref_key(key: &[u8]) -> Result<Option<IndexRef>> {
    let parts = key.split(|byte| *byte == 0).collect::<Vec<_>>();
    if parts.len() != 4 {
        return Ok(None);
    }
    Ok(Some(IndexRef {
        index_name: String::from_utf8_lossy(parts[2]).to_string(),
        values: decode_index_values_component(parts[3])?,
    }))
}

fn encode_index_values_component(values: &[Value]) -> Result<String> {
    let values = BinaryIndexValues(
        values
            .iter()
            .map(BinaryJsonValue::from_json)
            .collect::<Result<Vec<_>>>()?,
    );
    let payload = postcard::to_allocvec(&values)?;
    let mut encoded = Vec::with_capacity(INDEX_VALUES_MAGIC.len() + payload.len());
    encoded.extend_from_slice(INDEX_VALUES_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(hex_lower(&encoded))
}

fn decode_index_values_component(component: &[u8]) -> Result<Vec<Value>> {
    if let Ok(component_text) = std::str::from_utf8(component)
        && let Ok(bytes) = decode_hex(component_text)
        && let Some(payload) = bytes.strip_prefix(INDEX_VALUES_MAGIC)
    {
        let values: BinaryIndexValues =
            postcard::from_bytes(payload).context("parse postcard index values")?;
        return values
            .0
            .into_iter()
            .map(BinaryJsonValue::into_json)
            .collect();
    }
    serde_json::from_slice(component).context("parse legacy JSON index values")
}

fn compare_index_values(left: &[Value], right: &[Value]) -> Ordering {
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

fn record_key(table: &str, key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + key.len() + 1);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

fn record_prefix(table: &str, key_prefix: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + key_prefix.len() + 1);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key_prefix.as_bytes());
    bytes
}

fn recent_key(record: &DbRecord) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(record.table.len() + record.key.len() + 44);
    bytes.extend_from_slice(record.table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(format!("{:020}", u64::MAX.saturating_sub(record.lsn)).as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(
        format!("{:020}", u64::MAX.saturating_sub(record.updated_at_ms)).as_bytes(),
    );
    bytes.push(0);
    bytes.extend_from_slice(record.key.as_bytes());
    bytes
}

fn recent_prefix(table: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + 1);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes
}

fn recent_ref_key(table: &str, key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + key.len() + 1);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

fn order_prefix(table: &str, parent_key: &str, order_id: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + parent_key.len() + order_id.len() + 3);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(parent_key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(order_id.as_bytes());
    bytes.push(0);
    bytes
}

fn order_key(table: &str, parent_key: &str, order_id: &str, cursor: &str, key: &str) -> Vec<u8> {
    let mut bytes = order_prefix(table, parent_key, order_id);
    bytes.extend_from_slice(cursor.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

fn parse_order_key(
    table: &str,
    parent_key: &str,
    order_id: &str,
    key: &[u8],
) -> Option<(String, String)> {
    let prefix = order_prefix(table, parent_key, order_id);
    let rest = key.strip_prefix(prefix.as_slice())?;
    let separator = rest.iter().position(|byte| *byte == 0)?;
    Some((
        String::from_utf8_lossy(&rest[..separator]).to_string(),
        String::from_utf8_lossy(&rest[separator + 1..]).to_string(),
    ))
}

fn order_ref_prefix(table: &str, key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + key.len() + 2);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes
}

fn order_ref_key(table: &str, key: &str, order_id: &str) -> Vec<u8> {
    let mut bytes = order_ref_prefix(table, key);
    bytes.extend_from_slice(order_id.as_bytes());
    bytes
}

fn partition_key(table: &str, parent_key: &str, nested_key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + parent_key.len() + nested_key.len() + 2);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(parent_key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(nested_key.as_bytes());
    bytes
}

fn partition_prefix(table: &str, parent_key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(table.len() + parent_key.len() + 2);
    bytes.extend_from_slice(table.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(parent_key.as_bytes());
    bytes.push(0);
    bytes
}

fn parse_partition_key(table: &str, parent_key: &str, key: &[u8]) -> Option<String> {
    let prefix = partition_prefix(table, parent_key);
    let rest = key.strip_prefix(prefix.as_slice())?;
    Some(String::from_utf8_lossy(rest).to_string())
}

fn partition_after_nested_key(parent_key: &str, after_key: Option<&str>) -> Option<String> {
    let after_key = after_key?;
    let prefix = format!("{parent_key}:");
    Some(
        after_key
            .strip_prefix(&prefix)
            .unwrap_or(after_key)
            .to_string(),
    )
}

fn split_nested_key(key: &str) -> Option<(&str, &str)> {
    let (parent_key, nested_key) = key.split_once(':')?;
    if parent_key.is_empty() || nested_key.is_empty() {
        return None;
    }
    Some((parent_key, nested_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(table: &str, key: &str, lsn: u64) -> DbRecord {
        DbRecord {
            table: table.to_string(),
            key: key.to_string(),
            value: json!({ "id": key }),
            updated_at_ms: lsn,
            lsn,
            path: format!("tables/{table}/{key}"),
        }
    }

    #[test]
    fn projected_record_encoding_uses_postcard_envelope_and_reads_legacy_json() {
        let record = record("rooms", "a", 1);

        let encoded = encode_projected_record(&record).expect("encode projected record");

        assert!(encoded.starts_with(PROJECTED_RECORD_MAGIC));
        assert!(serde_json::from_slice::<DbRecord>(&encoded).is_err());
        assert_eq!(
            decode_projected_record(&encoded)
                .expect("decode projected record")
                .key,
            "a"
        );

        let legacy = serde_json::to_vec(&record).expect("encode legacy record");
        assert_eq!(
            decode_projected_record(&legacy)
                .expect("decode legacy projected record")
                .key,
            "a"
        );
    }

    #[test]
    fn index_value_components_use_postcard_hex_and_read_legacy_json() {
        let values = vec![json!("Beta"), json!(42), json!({ "nested": true })];

        let component = encode_index_values_component(&values).expect("encode index values");
        assert!(!component.as_bytes().contains(&0));
        let decoded_bytes = decode_hex(&component).expect("decode hex component");
        assert!(decoded_bytes.starts_with(INDEX_VALUES_MAGIC));
        assert!(serde_json::from_slice::<Vec<Value>>(&decoded_bytes).is_err());
        assert_eq!(
            decode_index_values_component(component.as_bytes()).expect("decode postcard values"),
            values
        );

        let legacy = serde_json::to_vec(&values).expect("encode legacy values");
        assert_eq!(
            decode_index_values_component(&legacy).expect("decode legacy values"),
            values
        );
    }

    #[test]
    fn records_are_listed_by_table_and_key_order() {
        let root =
            std::env::temp_dir().join(format!("nextdb-record-projection-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");

        for record in [
            record("rooms", "c", 3),
            record("rooms", "a", 1),
            record("rooms", "b", 2),
            record("users", "a", 4),
        ] {
            let bytes = serde_json::to_vec(&record).expect("encode record");
            store.put_record_bytes(&record, &bytes).expect("put record");
        }

        let records = store.list("rooms", Some("a"), Some(10)).expect("list");
        assert_eq!(
            records
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn records_are_listed_by_key_prefix() {
        let root = std::env::temp_dir().join(format!(
            "nextdb-record-projection-prefix-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");

        for record in [
            record("rooms.messages", "room-a:m2", 2),
            record("rooms.messages", "room-b:m1", 3),
            record("rooms.messages", "room-a:m1", 1),
        ] {
            let bytes = serde_json::to_vec(&record).expect("encode record");
            store.put_record_bytes(&record, &bytes).expect("put record");
        }

        let records = store
            .list_by_key_prefix("rooms.messages", "room-a:", None, Some(10))
            .expect("list prefix");
        assert_eq!(
            records
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m2"]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn partitions_are_listed_by_nested_key_order() {
        let root = std::env::temp_dir().join(format!(
            "nextdb-record-projection-partition-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");

        for record in [
            record("rooms.messages", "room-a:m3", 3),
            record("rooms.messages", "room-b:m1", 4),
            record("rooms.messages", "room-a:m1", 1),
            record("rooms.messages", "room-a:m2", 2),
        ] {
            let bytes = serde_json::to_vec(&record).expect("encode record");
            store.put_record_bytes(&record, &bytes).expect("put record");
        }

        let first_page = store
            .list_partition("rooms.messages", "room-a", None, Some(2))
            .expect("list partition first page");
        assert_eq!(
            first_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m2"]
        );

        let second_page = store
            .list_partition("rooms.messages", "room-a", Some("room-a:m2"), Some(2))
            .expect("list partition second page");
        assert_eq!(
            second_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m3"]
        );

        store
            .remove_record("rooms.messages", "room-a:m2")
            .expect("remove partition record");
        let after_delete = store
            .list_partition("rooms.messages", "room-a", None, Some(10))
            .expect("list partition after delete");
        assert_eq!(
            after_delete
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m3"]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn recent_records_are_listed_by_lsn_and_replace_old_versions() {
        let root = std::env::temp_dir().join(format!(
            "nextdb-record-projection-recent-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");

        for record in [
            record("rooms", "a", 1),
            record("rooms", "b", 3),
            record("rooms", "c", 2),
            record("users", "u1", 10),
        ] {
            let bytes = serde_json::to_vec(&record).expect("encode record");
            store.put_record_bytes(&record, &bytes).expect("put record");
        }

        let first_page = store
            .list_recent("rooms", Some(2))
            .expect("list recent first page");
        assert_eq!(
            first_page
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        let updated = record("rooms", "a", 4);
        let updated_bytes = serde_json::to_vec(&updated).expect("encode updated record");
        store
            .put_record_bytes(&updated, &updated_bytes)
            .expect("update record");
        store.remove_record("rooms", "b").expect("remove record");

        let after_update = store
            .list_recent("rooms", Some(10))
            .expect("list recent after update");
        assert_eq!(
            after_update
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ordered_partitions_are_listed_by_cursor_and_replace_old_versions() {
        let root = std::env::temp_dir().join(format!(
            "nextdb-record-projection-order-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");
        let order_id = "created-at-desc";

        for (record, cursor) in [
            (record("rooms.messages", "room-a:m3", 3), "001"),
            (record("rooms.messages", "room-b:m1", 4), "001"),
            (record("rooms.messages", "room-a:m1", 1), "003"),
            (record("rooms.messages", "room-a:m2", 2), "002"),
        ] {
            let bytes = serde_json::to_vec(&record).expect("encode record");
            store
                .put_order_entry(&record, order_id, cursor, &bytes)
                .expect("put order entry");
        }

        let first_page = store
            .list_ordered_partition("rooms.messages", "room-a", order_id, None, None, Some(2))
            .expect("list ordered first page");
        assert_eq!(
            first_page
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m3", "room-a:m2"]
        );

        let second_page = store
            .list_ordered_partition(
                "rooms.messages",
                "room-a",
                order_id,
                None,
                Some(&first_page.last().expect("cursor").cursor),
                Some(2),
            )
            .expect("list ordered second page");
        assert_eq!(
            second_page
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1"]
        );

        let updated = record("rooms.messages", "room-a:m1", 5);
        let updated_bytes = serde_json::to_vec(&updated).expect("encode updated");
        store
            .put_order_entry(&updated, order_id, "000", &updated_bytes)
            .expect("update order entry");
        store
            .remove_record("rooms.messages", "room-a:m2")
            .expect("remove ordered record");
        let after_update = store
            .list_ordered_partition("rooms.messages", "room-a", order_id, None, None, Some(10))
            .expect("list ordered after update");
        assert_eq!(
            after_update
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m3"]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn secondary_indexes_support_exact_range_update_and_unique_checks() {
        let root = std::env::temp_dir().join(format!(
            "nextdb-record-projection-index-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = RecordProjectionKv::open(root.clone()).expect("open projection");
        let records = [
            record("rooms", "a", 1),
            record("rooms", "b", 2),
            record("rooms", "c", 3),
        ];
        let values = [json!("Alpha"), json!("Beta"), json!("Gamma")];
        for (record, value) in records.iter().zip(values.iter()) {
            let bytes = serde_json::to_vec(record).expect("encode record");
            store.put_record_bytes(record, &bytes).expect("put record");
            store
                .put_index_entry(
                    record,
                    "byTitle",
                    std::slice::from_ref(value),
                    false,
                    &bytes,
                )
                .expect("put index");
        }

        let exact = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, None, Some(10))
            .expect("exact query");
        assert_eq!(
            exact
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );

        let range = store
            .query_index_range(IndexRangeProjectionQuery {
                table: "rooms",
                index_name: "byTitle",
                lower: Some(&[json!("Beta")]),
                upper: Some(&[json!("Gamma")]),
                key_prefix: None,
                after_cursor: None,
                limit: Some(10),
            })
            .expect("range query");
        assert_eq!(
            range
                .iter()
                .map(|entry| entry.record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        store
            .remove_index_entries("rooms", "b")
            .expect("remove b indexes");
        let exact_after_remove = store
            .query_index("rooms", "byTitle", &[json!("Beta")], None, None, Some(10))
            .expect("exact query after remove");
        assert!(exact_after_remove.is_empty());

        let duplicate = record("rooms", "duplicate", 4);
        let duplicate_bytes = serde_json::to_vec(&duplicate).expect("encode duplicate");
        let err = store
            .put_index_entry(
                &duplicate,
                "byTitle",
                &[json!("Gamma")],
                true,
                &duplicate_bytes,
            )
            .expect_err("unique index should reject existing value");
        assert!(err.to_string().contains("unique index violation"));

        let _ = std::fs::remove_dir_all(root);
    }
}
