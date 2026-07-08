use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    model::DbRecord,
    schema::{DatabaseSchema, StorageClass},
    util::{normalize_limit, now_ms},
};

#[derive(Clone, Default)]
pub struct RecordHotCache {
    state: Arc<RwLock<RecordHotState>>,
}

#[derive(Default)]
struct RecordHotState {
    tables: BTreeMap<String, HotTableState>,
    access_seq: u64,
    durable_idle_ttl_ms: u64,
    durable_idle_last_sweep_at_ms: Option<u64>,
    durable_idle_last_evicted: usize,
    durable_idle_total_evicted: usize,
    get_total: u64,
    get_hit_total: u64,
    get_miss_total: u64,
    list_total: u64,
    list_records_total: u64,
    hydrate_durable_total: u64,
    hydrate_durable_skipped_volatile_total: u64,
    upsert_total: u64,
    delete_total: u64,
    evict_total: u64,
    lru_evicted_total: u64,
}

#[derive(Clone)]
struct HotTableState {
    storage: StorageClass,
    max_items: Option<usize>,
    records: BTreeMap<String, DbRecord>,
    durable_delete_tombstones: BTreeMap<String, u64>,
    volatile_records: usize,
    volatile_generation: u64,
    volatile_key_prefixes: BTreeMap<String, usize>,
    volatile_key_prefix_generations: BTreeMap<String, u64>,
    access: BTreeMap<String, u64>,
    last_accessed_ms: BTreeMap<String, u64>,
    counters: RecordHotCounters,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotCounters {
    pub get_total: u64,
    pub get_hit_total: u64,
    pub get_miss_total: u64,
    pub list_total: u64,
    pub list_records_total: u64,
    pub hydrate_durable_total: u64,
    pub hydrate_durable_skipped_volatile_total: u64,
    pub upsert_total: u64,
    pub delete_total: u64,
    pub evict_total: u64,
    pub lru_evicted_total: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotCacheStatus {
    pub tables: Vec<RecordHotTableStatus>,
    pub table_count: usize,
    pub record_count: usize,
    pub volatile_records: usize,
    pub durable_idle_ttl_ms: u64,
    pub durable_idle_last_sweep_at_ms: Option<u64>,
    pub durable_idle_last_evicted: usize,
    pub durable_idle_total_evicted: usize,
    pub get_total: u64,
    pub get_hit_total: u64,
    pub get_miss_total: u64,
    pub list_total: u64,
    pub list_records_total: u64,
    pub hydrate_durable_total: u64,
    pub hydrate_durable_skipped_volatile_total: u64,
    pub upsert_total: u64,
    pub delete_total: u64,
    pub evict_total: u64,
    pub lru_evicted_total: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotTableStatus {
    pub table: String,
    pub storage: StorageClass,
    pub max_items: Option<usize>,
    pub records: usize,
    pub durable_delete_tombstones: usize,
    pub volatile_records: usize,
    #[serde(flatten)]
    pub counters: RecordHotCounters,
}

#[derive(Debug, Clone, Default)]
pub struct RecordHotKeyOrderOverlay {
    pub records: Vec<DbRecord>,
    pub shadow_keys: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotSnapshot {
    #[serde(default)]
    pub access_seq: u64,
    #[serde(default)]
    pub tables: BTreeMap<String, RecordHotTableSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotTableSnapshot {
    #[serde(default)]
    pub records: BTreeMap<String, RecordHotRecordSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordHotRecordSnapshot {
    pub record: DbRecord,
    #[serde(default)]
    pub access: u64,
    #[serde(default)]
    pub last_accessed_ms: u64,
}

impl RecordHotSnapshot {
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn record_count(&self) -> usize {
        self.tables.values().map(|table| table.records.len()).sum()
    }
}

impl RecordHotCache {
    pub fn from_schema_and_records(
        schema: &DatabaseSchema,
        records: &[DbRecord],
        durable_idle_ttl_ms: u64,
    ) -> Self {
        Self::from_schema_snapshot_and_records(schema, None, 0, records, durable_idle_ttl_ms)
    }

    pub fn from_schema_snapshot_and_records(
        schema: &DatabaseSchema,
        snapshot: Option<&RecordHotSnapshot>,
        snapshot_lsn: u64,
        records: &[DbRecord],
        durable_idle_ttl_ms: u64,
    ) -> Self {
        let mut state = RecordHotState {
            tables: hot_tables_for_schema(schema)
                .into_iter()
                .map(|(table, (storage, max_items))| {
                    (
                        table,
                        HotTableState {
                            storage,
                            max_items,
                            records: BTreeMap::new(),
                            durable_delete_tombstones: BTreeMap::new(),
                            volatile_records: 0,
                            volatile_generation: 0,
                            volatile_key_prefixes: BTreeMap::new(),
                            volatile_key_prefix_generations: BTreeMap::new(),
                            access: BTreeMap::new(),
                            last_accessed_ms: BTreeMap::new(),
                            counters: RecordHotCounters::default(),
                        },
                    )
                })
                .collect(),
            access_seq: 0,
            durable_idle_ttl_ms,
            durable_idle_last_sweep_at_ms: None,
            durable_idle_last_evicted: 0,
            durable_idle_total_evicted: 0,
            get_total: 0,
            get_hit_total: 0,
            get_miss_total: 0,
            list_total: 0,
            list_records_total: 0,
            hydrate_durable_total: 0,
            hydrate_durable_skipped_volatile_total: 0,
            upsert_total: 0,
            delete_total: 0,
            evict_total: 0,
            lru_evicted_total: 0,
        };
        let current_records = records
            .iter()
            .map(|record| ((record.table.as_str(), record.key.as_str()), record))
            .collect::<BTreeMap<_, _>>();
        if let Some(snapshot) = snapshot {
            state.access_seq = snapshot.access_seq;
            for (table, snapshot_table) in &snapshot.tables {
                if !state.tables.contains_key(table) {
                    continue;
                }
                for (key, entry) in &snapshot_table.records {
                    if !is_durable_record(&entry.record) {
                        continue;
                    }
                    let current = current_records
                        .get(&(entry.record.table.as_str(), entry.record.key.as_str()));
                    if !current.is_some_and(|current| {
                        current.lsn == entry.record.lsn
                            && current.path == entry.record.path
                            && current.table == entry.record.table
                            && current.key == entry.record.key
                    }) {
                        continue;
                    }
                    state.insert_with_access(
                        table,
                        key,
                        entry.record.clone(),
                        entry.access,
                        entry.last_accessed_ms,
                    );
                }
                state.evict_lru(table);
            }
        }

        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.lsn
                .cmp(&right.lsn)
                .then_with(|| left.table.cmp(&right.table))
                .then_with(|| left.key.cmp(&right.key))
        });
        for record in sorted {
            let Some(table_state) = state.tables.get(&record.table) else {
                continue;
            };
            if snapshot.is_some()
                && matches!(table_state.storage, StorageClass::Lru { .. })
                && record.lsn <= snapshot_lsn
                && !table_state.records.contains_key(&record.key)
            {
                continue;
            }
            let _ = state.upsert_internal(record, false, true);
        }
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn reconfigure(
        &self,
        schema: &DatabaseSchema,
        records: &[DbRecord],
        durable_idle_ttl_ms: u64,
    ) {
        let replacement = Self::from_schema_and_records(schema, records, durable_idle_ttl_ms);
        let replacement_state = replacement.state.read().await;
        let mut state = self.state.write().await;
        state.tables = replacement_state.tables.clone();
        state.access_seq = replacement_state.access_seq;
        state.durable_idle_ttl_ms = replacement_state.durable_idle_ttl_ms;
        state.durable_idle_last_sweep_at_ms = None;
        state.durable_idle_last_evicted = 0;
        state.durable_idle_total_evicted = 0;
        state.get_total = 0;
        state.get_hit_total = 0;
        state.get_miss_total = 0;
        state.list_total = 0;
        state.list_records_total = 0;
        state.hydrate_durable_total = 0;
        state.hydrate_durable_skipped_volatile_total = 0;
        state.upsert_total = 0;
        state.delete_total = 0;
        state.evict_total = 0;
        state.lru_evicted_total = 0;
    }

    pub async fn status(&self) -> RecordHotCacheStatus {
        let state = self.state.read().await;
        let tables = state
            .tables
            .iter()
            .map(|(table, table_state)| RecordHotTableStatus {
                table: table.clone(),
                storage: table_state.storage.clone(),
                max_items: table_state.max_items,
                records: table_state.records.len(),
                durable_delete_tombstones: table_state.durable_delete_tombstones.len(),
                volatile_records: table_state.volatile_records,
                counters: table_state.counters.clone(),
            })
            .collect::<Vec<_>>();
        RecordHotCacheStatus {
            table_count: tables.len(),
            record_count: tables.iter().map(|table| table.records).sum(),
            volatile_records: tables.iter().map(|table| table.volatile_records).sum(),
            durable_idle_ttl_ms: state.durable_idle_ttl_ms,
            durable_idle_last_sweep_at_ms: state.durable_idle_last_sweep_at_ms,
            durable_idle_last_evicted: state.durable_idle_last_evicted,
            durable_idle_total_evicted: state.durable_idle_total_evicted,
            get_total: state.get_total,
            get_hit_total: state.get_hit_total,
            get_miss_total: state.get_miss_total,
            list_total: state.list_total,
            list_records_total: state.list_records_total,
            hydrate_durable_total: state.hydrate_durable_total,
            hydrate_durable_skipped_volatile_total: state.hydrate_durable_skipped_volatile_total,
            upsert_total: state.upsert_total,
            delete_total: state.delete_total,
            evict_total: state.evict_total,
            lru_evicted_total: state.lru_evicted_total,
            tables,
        }
    }

    pub async fn evict_idle_durable_records(&self) -> usize {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records()
    }

    pub async fn snapshot(&self) -> RecordHotSnapshot {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        RecordHotSnapshot {
            access_seq: state.access_seq,
            tables: state
                .tables
                .iter()
                .filter_map(|(table, table_state)| {
                    let records = table_state
                        .records
                        .iter()
                        .filter(|(_, record)| is_durable_record(record))
                        .map(|(key, record)| {
                            (
                                key.clone(),
                                RecordHotRecordSnapshot {
                                    record: record.clone(),
                                    access: table_state.access.get(key).copied().unwrap_or(0),
                                    last_accessed_ms: table_state
                                        .last_accessed_ms
                                        .get(key)
                                        .copied()
                                        .unwrap_or(0),
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    if records.is_empty() {
                        None
                    } else {
                        Some((table.clone(), RecordHotTableSnapshot { records }))
                    }
                })
                .collect(),
        }
    }

    pub async fn is_lru_table(&self, table: &str) -> bool {
        let state = self.state.read().await;
        state
            .tables
            .get(table)
            .is_some_and(|table_state| matches!(table_state.storage, StorageClass::Lru { .. }))
    }

    pub async fn is_windowed_table(&self, table: &str) -> bool {
        let state = self.state.read().await;
        state
            .tables
            .get(table)
            .is_some_and(|table_state| table_state.max_items.is_some())
    }

    pub async fn is_hot_table(&self, table: &str) -> bool {
        let state = self.state.read().await;
        state.tables.contains_key(table)
    }

    #[cfg(test)]
    pub async fn has_volatile_overlay_in_scope(
        &self,
        table: &str,
        key_prefix: Option<&str>,
    ) -> bool {
        let state = self.state.read().await;
        state.tables.get(table).is_some_and(|table_state| {
            if let Some(key_prefix) = key_prefix {
                return table_state
                    .volatile_key_prefixes
                    .get(key_prefix)
                    .is_some_and(|count| *count > 0);
            }
            table_state.volatile_records > 0
        })
    }

    pub async fn volatile_generation_in_scope(&self, table: &str, key_prefix: Option<&str>) -> u64 {
        let state = self.state.read().await;
        state
            .tables
            .get(table)
            .map(|table_state| {
                if let Some(key_prefix) = key_prefix {
                    return table_state
                        .volatile_key_prefix_generations
                        .get(key_prefix)
                        .copied()
                        .unwrap_or(0);
                }
                table_state.volatile_generation
            })
            .unwrap_or(0)
    }

    pub async fn get(&self, table: &str, key: &str) -> Option<Option<DbRecord>> {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        state.get(table, key)
    }

    pub async fn list(
        &self,
        table: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Option<Vec<DbRecord>> {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        state.list(table, after_key, limit)
    }

    pub async fn list_by_key_prefix(
        &self,
        table: &str,
        key_prefix: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Option<Vec<DbRecord>> {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        state.list_by_key_prefix(table, key_prefix, after_key, limit)
    }

    pub async fn scan_key_order(
        &self,
        table: &str,
        key_prefix: Option<&str>,
        after_key: Option<&str>,
    ) -> Option<Vec<DbRecord>> {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        state.scan_key_order(table, key_prefix, after_key)
    }

    pub async fn scan_key_order_overlay(
        &self,
        table: &str,
        key_prefix: Option<&str>,
        after_key: Option<&str>,
    ) -> Option<RecordHotKeyOrderOverlay> {
        let mut state = self.state.write().await;
        state.evict_idle_durable_records();
        state.scan_key_order_overlay(table, key_prefix, after_key)
    }

    pub async fn durable_delete_lsn(&self, table: &str, key: &str) -> Option<Option<u64>> {
        let state = self.state.read().await;
        let table_state = state.tables.get(table)?;
        Some(table_state.durable_delete_tombstones.get(key).copied())
    }

    pub async fn upsert(&self, record: &DbRecord) {
        let mut state = self.state.write().await;
        state.upsert(record.clone());
    }

    pub async fn upsert_many<'a>(&self, records: impl IntoIterator<Item = &'a DbRecord>) {
        let mut state = self.state.write().await;
        let mut touched_tables = BTreeSet::new();
        for record in records {
            if let Some(table) = state.upsert_internal(record.clone(), true, false) {
                touched_tables.insert(table);
            }
        }
        for table in touched_tables {
            state.evict_lru(&table);
        }
    }

    pub async fn hydrate_durable_many<'a>(&self, records: impl IntoIterator<Item = &'a DbRecord>) {
        let mut state = self.state.write().await;
        let mut touched_tables = BTreeSet::new();
        for record in records {
            if let Some(table) = state.hydrate_durable(record, false) {
                touched_tables.insert(table);
            }
        }
        for table in touched_tables {
            state.evict_lru(&table);
        }
    }

    pub async fn delete(&self, table: &str, key: &str) {
        let mut state = self.state.write().await;
        state.delete(table, key);
    }

    pub async fn delete_durable(&self, table: &str, key: &str, lsn: u64) {
        let mut state = self.state.write().await;
        state.delete_durable(table, key, lsn);
    }

    pub async fn clear_durable_delete_tombstone(&self, table: &str, key: &str, lsn: u64) {
        let mut state = self.state.write().await;
        state.clear_durable_delete_tombstone(table, key, lsn);
    }

    pub async fn evict_many<'a>(
        &self,
        table: &str,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> usize {
        let mut state = self.state.write().await;
        let mut evicted = 0;
        for key in keys {
            if state.evict(table, key) {
                evicted += 1;
            }
        }
        evicted
    }
}

impl RecordHotState {
    fn get(&mut self, table: &str, key: &str) -> Option<Option<DbRecord>> {
        if !self.tables.contains_key(table) {
            return None;
        }
        self.get_total = self.get_total.saturating_add(1);
        let record = {
            let table_state = self.tables.get_mut(table)?;
            table_state.counters.get_total = table_state.counters.get_total.saturating_add(1);
            let record = table_state.records.get(key).cloned();
            if record.is_some() {
                table_state.counters.get_hit_total =
                    table_state.counters.get_hit_total.saturating_add(1);
            } else {
                table_state.counters.get_miss_total =
                    table_state.counters.get_miss_total.saturating_add(1);
            }
            record
        };
        if record.is_some() {
            self.get_hit_total = self.get_hit_total.saturating_add(1);
            self.touch(table, key);
        } else {
            self.get_miss_total = self.get_miss_total.saturating_add(1);
        }
        Some(record)
    }

    fn list(
        &mut self,
        table: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Option<Vec<DbRecord>> {
        let table_state = self.tables.get(table)?;
        let limit = normalize_limit(limit);
        let records = key_order_records(table_state, after_key)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        self.note_list(table, records.len());
        self.touch_many(table, records.iter().map(|record| record.key.as_str()));
        Some(records)
    }

    fn list_by_key_prefix(
        &mut self,
        table: &str,
        key_prefix: &str,
        after_key: Option<&str>,
        limit: Option<usize>,
    ) -> Option<Vec<DbRecord>> {
        let table_state = self.tables.get(table)?;
        let limit = normalize_limit(limit);
        let records = key_prefix_records(table_state, key_prefix, after_key)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        self.note_list(table, records.len());
        self.touch_many(table, records.iter().map(|record| record.key.as_str()));
        Some(records)
    }

    fn scan_key_order(
        &mut self,
        table: &str,
        key_prefix: Option<&str>,
        after_key: Option<&str>,
    ) -> Option<Vec<DbRecord>> {
        let table_state = self.tables.get(table)?;
        let records = match key_prefix {
            Some(prefix) => key_prefix_records(table_state, prefix, after_key)
                .cloned()
                .collect::<Vec<_>>(),
            None => key_order_records(table_state, after_key)
                .cloned()
                .collect::<Vec<_>>(),
        };
        self.note_list(table, records.len());
        self.touch_many(table, records.iter().map(|record| record.key.as_str()));
        Some(records)
    }

    fn scan_key_order_overlay(
        &mut self,
        table: &str,
        key_prefix: Option<&str>,
        after_key: Option<&str>,
    ) -> Option<RecordHotKeyOrderOverlay> {
        let table_state = self.tables.get(table)?;
        let records = match key_prefix {
            Some(prefix) => key_prefix_records(table_state, prefix, after_key)
                .cloned()
                .collect::<Vec<_>>(),
            None => key_order_records(table_state, after_key)
                .cloned()
                .collect::<Vec<_>>(),
        };
        let mut shadow_keys = records
            .iter()
            .map(|record| record.key.clone())
            .collect::<BTreeSet<_>>();
        shadow_keys.extend(tombstone_keys(table_state, key_prefix, after_key));
        self.note_list(table, records.len());
        self.touch_many(table, records.iter().map(|record| record.key.as_str()));
        Some(RecordHotKeyOrderOverlay {
            records,
            shadow_keys,
        })
    }

    fn upsert(&mut self, record: DbRecord) {
        let _ = self.upsert_internal(record, true, true);
    }

    fn upsert_internal(
        &mut self,
        record: DbRecord,
        count_upsert: bool,
        evict_lru: bool,
    ) -> Option<String> {
        let table = record.table.clone();
        let key = record.key.clone();
        let table_state = self.tables.get_mut(&table)?;
        if count_upsert {
            self.upsert_total = self.upsert_total.saturating_add(1);
            table_state.counters.upsert_total = table_state.counters.upsert_total.saturating_add(1);
        }
        if is_durable_record(&record) {
            let should_clear = table_state
                .durable_delete_tombstones
                .get(&key)
                .is_none_or(|deleted_lsn| *deleted_lsn <= record.lsn);
            if should_clear {
                table_state.durable_delete_tombstones.remove(&key);
            }
        }
        let old_is_volatile = table_state
            .records
            .get(&key)
            .is_some_and(|existing| !is_durable_record(existing));
        let new_is_volatile = !is_durable_record(&record);
        table_state.records.insert(key.clone(), record);
        update_volatile_record_state(table_state, &key, old_is_volatile, new_is_volatile);
        self.touch(&table, &key);
        if evict_lru {
            self.evict_lru(&table);
        }
        Some(table)
    }

    fn hydrate_durable(&mut self, record: &DbRecord, evict_lru: bool) -> Option<String> {
        if !is_durable_record(record) {
            return None;
        }
        let current_is_volatile = self
            .tables
            .get(&record.table)
            .and_then(|table_state| table_state.records.get(&record.key))
            .is_some_and(|existing| !is_durable_record(existing));
        if current_is_volatile {
            self.hydrate_durable_skipped_volatile_total = self
                .hydrate_durable_skipped_volatile_total
                .saturating_add(1);
            if let Some(table_state) = self.tables.get_mut(&record.table) {
                table_state.counters.hydrate_durable_skipped_volatile_total = table_state
                    .counters
                    .hydrate_durable_skipped_volatile_total
                    .saturating_add(1);
            }
            return None;
        }
        if let Some(table_state) = self.tables.get_mut(&record.table) {
            self.hydrate_durable_total = self.hydrate_durable_total.saturating_add(1);
            table_state.counters.hydrate_durable_total =
                table_state.counters.hydrate_durable_total.saturating_add(1);
        }
        self.upsert_internal(record.clone(), false, evict_lru)
    }

    fn delete(&mut self, table: &str, key: &str) {
        let Some(table_state) = self.tables.get_mut(table) else {
            return;
        };
        if let Some(removed) = table_state.records.remove(key) {
            self.delete_total = self.delete_total.saturating_add(1);
            table_state.counters.delete_total = table_state.counters.delete_total.saturating_add(1);
            update_volatile_record_state(table_state, key, !is_durable_record(&removed), false);
            table_state.access.remove(key);
            table_state.last_accessed_ms.remove(key);
        }
    }

    fn delete_durable(&mut self, table: &str, key: &str, lsn: u64) {
        let Some(table_state) = self.tables.get_mut(table) else {
            return;
        };
        if table_state
            .records
            .get(key)
            .is_some_and(|record| is_durable_record(record) && record.lsn > lsn)
        {
            return;
        }
        if let Some(removed) = table_state.records.remove(key) {
            update_volatile_record_state(table_state, key, !is_durable_record(&removed), false);
        }
        let tombstone = table_state
            .durable_delete_tombstones
            .entry(key.to_string())
            .or_insert(0);
        *tombstone = (*tombstone).max(lsn);
        table_state.access.remove(key);
        table_state.last_accessed_ms.remove(key);
        self.delete_total = self.delete_total.saturating_add(1);
        table_state.counters.delete_total = table_state.counters.delete_total.saturating_add(1);
    }

    fn clear_durable_delete_tombstone(&mut self, table: &str, key: &str, lsn: u64) {
        let Some(table_state) = self.tables.get_mut(table) else {
            return;
        };
        if table_state
            .durable_delete_tombstones
            .get(key)
            .is_some_and(|deleted_lsn| *deleted_lsn <= lsn)
        {
            table_state.durable_delete_tombstones.remove(key);
        }
    }

    fn evict(&mut self, table: &str, key: &str) -> bool {
        let Some(table_state) = self.tables.get_mut(table) else {
            return false;
        };
        let removed = table_state.records.remove(key);
        let existed = removed.is_some();
        if let Some(removed) = removed.as_ref() {
            update_volatile_record_state(table_state, key, !is_durable_record(removed), false);
        }
        table_state.access.remove(key);
        table_state.last_accessed_ms.remove(key);
        if existed {
            self.evict_total = self.evict_total.saturating_add(1);
            table_state.counters.evict_total = table_state.counters.evict_total.saturating_add(1);
        }
        existed
    }

    fn note_list(&mut self, table: &str, records: usize) {
        self.list_total = self.list_total.saturating_add(1);
        self.list_records_total = self.list_records_total.saturating_add(records as u64);
        if let Some(table_state) = self.tables.get_mut(table) {
            table_state.counters.list_total = table_state.counters.list_total.saturating_add(1);
            table_state.counters.list_records_total = table_state
                .counters
                .list_records_total
                .saturating_add(records as u64);
        }
    }

    fn insert_with_access(
        &mut self,
        table: &str,
        key: &str,
        record: DbRecord,
        access: u64,
        last_accessed_ms: u64,
    ) {
        let Some(table_state) = self.tables.get_mut(table) else {
            return;
        };
        let old_is_volatile = table_state
            .records
            .get(key)
            .is_some_and(|existing| !is_durable_record(existing));
        let new_is_volatile = !is_durable_record(&record);
        table_state.records.insert(key.to_string(), record);
        if !new_is_volatile {
            table_state.durable_delete_tombstones.remove(key);
        }
        update_volatile_record_state(table_state, key, old_is_volatile, new_is_volatile);
        table_state.access.insert(key.to_string(), access);
        table_state.last_accessed_ms.insert(
            key.to_string(),
            if last_accessed_ms == 0 {
                now_ms()
            } else {
                last_accessed_ms
            },
        );
        self.access_seq = self.access_seq.max(access);
    }

    fn touch(&mut self, table: &str, key: &str) {
        self.touch_many(table, [key]);
    }

    fn touch_many<'a>(&mut self, table: &str, keys: impl IntoIterator<Item = &'a str>) {
        let Some(table_state) = self.tables.get_mut(table) else {
            return;
        };
        let now = now_ms();
        for key in keys {
            if !table_state.records.contains_key(key) {
                continue;
            }
            self.access_seq = self.access_seq.saturating_add(1);
            table_state.access.insert(key.to_string(), self.access_seq);
            table_state.last_accessed_ms.insert(key.to_string(), now);
        }
    }

    fn evict_lru(&mut self, table: &str) {
        let mut evicted = 0_u64;
        {
            let Some(table_state) = self.tables.get_mut(table) else {
                return;
            };
            let Some(max_items) = table_state.max_items else {
                return;
            };
            let overflow = table_state.records.len().saturating_sub(max_items);
            if overflow == 0 {
                return;
            }
            let victims = if overflow == 1 {
                table_state
                    .records
                    .keys()
                    .min_by_key(|key| table_state.access.get(*key).copied().unwrap_or(0))
                    .cloned()
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                let mut candidates = table_state
                    .records
                    .keys()
                    .map(|key| {
                        (
                            table_state.access.get(key).copied().unwrap_or(0),
                            key.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                candidates
                    .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
                candidates
                    .into_iter()
                    .take(overflow)
                    .map(|(_, key)| key)
                    .collect::<Vec<_>>()
            };
            for victim in victims {
                if let Some(removed) = table_state.records.remove(&victim) {
                    update_volatile_record_state(
                        table_state,
                        &victim,
                        !is_durable_record(&removed),
                        false,
                    );
                }
                table_state.access.remove(&victim);
                table_state.last_accessed_ms.remove(&victim);
                table_state.counters.lru_evicted_total =
                    table_state.counters.lru_evicted_total.saturating_add(1);
                evicted += 1;
            }
        }
        if evicted > 0 {
            self.lru_evicted_total = self.lru_evicted_total.saturating_add(evicted);
        }
    }

    fn evict_idle_durable_records(&mut self) -> usize {
        if self.durable_idle_ttl_ms == 0 {
            return 0;
        }
        let now = now_ms();
        let mut evicted = 0;
        for table_state in self.tables.values_mut() {
            let victims = table_state
                .records
                .iter()
                .filter(|(_, record)| is_durable_record(record))
                .filter_map(|(key, _)| {
                    let last_accessed_ms = table_state
                        .last_accessed_ms
                        .get(key)
                        .copied()
                        .unwrap_or_default();
                    (last_accessed_ms > 0
                        && now.saturating_sub(last_accessed_ms) > self.durable_idle_ttl_ms)
                        .then_some(key.clone())
                })
                .collect::<Vec<_>>();
            for victim in victims {
                if let Some(removed) = table_state.records.remove(&victim) {
                    update_volatile_record_state(
                        table_state,
                        &victim,
                        !is_durable_record(&removed),
                        false,
                    );
                }
                table_state.access.remove(&victim);
                table_state.last_accessed_ms.remove(&victim);
                evicted += 1;
            }
        }
        self.durable_idle_last_sweep_at_ms = Some(now);
        self.durable_idle_last_evicted = evicted;
        self.durable_idle_total_evicted = self.durable_idle_total_evicted.saturating_add(evicted);
        evicted
    }
}

fn hot_tables_for_schema(
    schema: &DatabaseSchema,
) -> BTreeMap<String, (StorageClass, Option<usize>)> {
    let mut tables = BTreeMap::new();
    for (table_name, table) in &schema.tables {
        if let Some((storage, max_items)) = hot_storage_policy(&table.storage) {
            tables.insert(table_name.clone(), (storage, max_items));
        }
        for (nested_name, nested) in &table.nested {
            if let Some((storage, max_items)) = hot_storage_policy(&nested.storage) {
                tables.insert(format!("{table_name}.{nested_name}"), (storage, max_items));
            }
        }
    }
    tables
}

fn hot_storage_policy(storage: &StorageClass) -> Option<(StorageClass, Option<usize>)> {
    match storage {
        StorageClass::Resident | StorageClass::ActorPartition => Some((storage.clone(), None)),
        StorageClass::Lru { max_items } => Some((storage.clone(), Some(*max_items))),
        StorageClass::ChatLog { live_window, .. } => Some((storage.clone(), Some(*live_window))),
        StorageClass::Disk | StorageClass::Object => None,
    }
}

fn is_durable_record(record: &DbRecord) -> bool {
    record.lsn > 0 && !record.path.starts_with("volatile/")
}

fn update_volatile_record_state(
    table_state: &mut HotTableState,
    key: &str,
    old_is_volatile: bool,
    new_is_volatile: bool,
) {
    if old_is_volatile || new_is_volatile {
        bump_volatile_generation(table_state, key);
    }
    match (old_is_volatile, new_is_volatile) {
        (false, true) => {
            table_state.volatile_records = table_state.volatile_records.saturating_add(1);
            adjust_volatile_key_prefixes(table_state, key, 1);
        }
        (true, false) => {
            table_state.volatile_records = table_state.volatile_records.saturating_sub(1);
            adjust_volatile_key_prefixes(table_state, key, -1);
        }
        _ => {}
    }
}

fn bump_volatile_generation(table_state: &mut HotTableState, key: &str) {
    table_state.volatile_generation = table_state.volatile_generation.saturating_add(1);
    for prefix in volatile_key_prefixes(key) {
        let generation = table_state
            .volatile_key_prefix_generations
            .entry(prefix)
            .or_insert(0);
        *generation = generation.saturating_add(1);
    }
}

fn adjust_volatile_key_prefixes(table_state: &mut HotTableState, key: &str, delta: isize) {
    for prefix in volatile_key_prefixes(key) {
        if delta > 0 {
            let count = table_state.volatile_key_prefixes.entry(prefix).or_insert(0);
            *count = count.saturating_add(delta as usize);
            continue;
        }
        let Some(count) = table_state.volatile_key_prefixes.get_mut(&prefix) else {
            continue;
        };
        *count = count.saturating_sub(delta.unsigned_abs());
        if *count == 0 {
            table_state.volatile_key_prefixes.remove(&prefix);
        }
    }
}

fn volatile_key_prefixes(key: &str) -> impl Iterator<Item = String> + '_ {
    key.match_indices(':')
        .map(|(index, _)| key[..=index].to_string())
}

fn key_order_records<'a>(
    table_state: &'a HotTableState,
    after_key: Option<&'a str>,
) -> impl Iterator<Item = &'a DbRecord> {
    let start = after_key.unwrap_or_default();
    table_state
        .records
        .range(start.to_string()..)
        .map(|(_, record)| record)
        .filter(move |record| match after_key {
            Some(after) => record.key.as_str() > after,
            None => true,
        })
}

fn key_prefix_records<'a>(
    table_state: &'a HotTableState,
    key_prefix: &'a str,
    after_key: Option<&'a str>,
) -> impl Iterator<Item = &'a DbRecord> {
    let start = match after_key {
        Some(after) if after > key_prefix => after,
        _ => key_prefix,
    };
    table_state
        .records
        .range(start.to_string()..)
        .map(|(_, record)| record)
        .take_while(move |record| record.key.starts_with(key_prefix))
        .filter(move |record| match after_key {
            Some(after) => record.key.as_str() > after,
            None => true,
        })
}

fn tombstone_keys<'a>(
    table_state: &'a HotTableState,
    key_prefix: Option<&'a str>,
    after_key: Option<&'a str>,
) -> impl Iterator<Item = String> + 'a {
    let start = match (key_prefix, after_key) {
        (_, Some(after)) => after,
        (Some(prefix), None) => prefix,
        (None, None) => "",
    };
    table_state
        .durable_delete_tombstones
        .range(start.to_string()..)
        .map(|(key, _)| key)
        .take_while(move |key| key_prefix.is_none_or(|prefix| key.starts_with(prefix)))
        .filter(move |key| after_key.is_none_or(|after| key.as_str() > after))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldSchema, FieldType, TableSchema};

    fn record(table: &str, key: &str, lsn: u64) -> DbRecord {
        DbRecord {
            table: table.to_string(),
            key: key.to_string(),
            value: serde_json::json!({ "id": key }),
            updated_at_ms: lsn,
            lsn,
            path: format!("tables/{table}/{key}"),
        }
    }

    fn volatile_record(table: &str, key: &str) -> DbRecord {
        DbRecord {
            table: table.to_string(),
            key: key.to_string(),
            value: serde_json::json!({ "id": key }),
            updated_at_ms: 0,
            lsn: 0,
            path: format!("volatile/tables/{table}/{key}"),
        }
    }

    fn schema_with_lru(max_items: usize) -> DatabaseSchema {
        let mut tables = BTreeMap::new();
        tables.insert(
            "rooms".to_string(),
            TableSchema {
                storage: StorageClass::Lru { max_items },
                fields: BTreeMap::from([(
                    "id".to_string(),
                    FieldSchema::required(FieldType::Id {
                        entity: "Room".to_string(),
                    }),
                )]),
                nested: BTreeMap::new(),
                read_visibility: Default::default(),
                indexes: BTreeMap::new(),
            },
        );
        DatabaseSchema {
            name: "test".to_string(),
            version: 1,
            objects: BTreeMap::new(),
            tables,
            events: BTreeMap::new(),
            behaviors: BTreeMap::new(),
        }
    }

    fn schema_with_chat_log_window(max_items: usize) -> DatabaseSchema {
        let mut schema = DatabaseSchema::default_nextdb();
        let storage = &mut schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .storage;
        let StorageClass::ChatLog { live_window, .. } = storage else {
            panic!("default messages table should use chatLog storage");
        };
        *live_window = max_items;
        schema
    }

    #[tokio::test]
    async fn lru_table_evicts_oldest_record_but_keeps_disk_truth_external() {
        let cache = RecordHotCache::from_schema_and_records(
            &schema_with_lru(2),
            &[record("rooms", "a", 1), record("rooms", "b", 2)],
            0,
        );
        assert!(cache.get("rooms", "a").await.flatten().is_some());

        cache.upsert(&record("rooms", "c", 3)).await;

        assert!(cache.get("rooms", "a").await.flatten().is_some());
        assert!(cache.get("rooms", "b").await.flatten().is_none());
        assert!(cache.get("rooms", "c").await.flatten().is_some());
    }

    #[tokio::test]
    async fn lru_table_batches_upserts_before_evicting_overflow() {
        let cache = RecordHotCache::from_schema_and_records(&schema_with_lru(2), &[], 0);

        cache
            .upsert_many(
                [
                    record("rooms", "a", 1),
                    record("rooms", "b", 2),
                    record("rooms", "c", 3),
                    record("rooms", "d", 4),
                ]
                .iter(),
            )
            .await;

        assert!(cache.get("rooms", "a").await.flatten().is_none());
        assert!(cache.get("rooms", "b").await.flatten().is_none());
        assert!(cache.get("rooms", "c").await.flatten().is_some());
        assert!(cache.get("rooms", "d").await.flatten().is_some());
        let status = cache.status().await;
        assert_eq!(status.record_count, 2);
        assert_eq!(status.lru_evicted_total, 2);
    }

    #[tokio::test]
    async fn disk_tables_are_not_served_by_hot_cache() {
        let mut schema = schema_with_lru(2);
        schema.tables.get_mut("rooms").unwrap().storage = StorageClass::Disk;
        let cache = RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 0);

        assert!(cache.get("rooms", "a").await.is_none());
        assert!(cache.list("rooms", None, Some(10)).await.is_none());
    }

    #[tokio::test]
    async fn chat_log_tables_use_live_window_as_hot_capacity() {
        let cache = RecordHotCache::from_schema_and_records(
            &schema_with_chat_log_window(2),
            &[
                record("rooms.messages", "room-a:m1", 1),
                record("rooms.messages", "room-a:m2", 2),
                record("rooms.messages", "room-a:m3", 3),
            ],
            0,
        );

        assert!(cache.is_hot_table("rooms.messages").await);
        assert!(cache.is_windowed_table("rooms.messages").await);
        assert!(!cache.is_lru_table("rooms.messages").await);
        assert!(
            cache
                .get("rooms.messages", "room-a:m1")
                .await
                .flatten()
                .is_none()
        );
        assert!(
            cache
                .get("rooms.messages", "room-a:m2")
                .await
                .flatten()
                .is_some()
        );
        assert!(
            cache
                .get("rooms.messages", "room-a:m3")
                .await
                .flatten()
                .is_some()
        );

        let status = cache.status().await;
        let messages = status
            .tables
            .iter()
            .find(|table| table.table == "rooms.messages")
            .expect("rooms.messages hot table");
        assert_eq!(messages.max_items, Some(2));
        assert!(matches!(messages.storage, StorageClass::ChatLog { .. }));
    }

    #[tokio::test]
    async fn snapshot_restores_lru_hot_set_without_loading_cold_rows() {
        let schema = schema_with_lru(2);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[record("rooms", "a", 1), record("rooms", "b", 2)],
            0,
        );
        assert!(cache.get("rooms", "a").await.flatten().is_some());
        cache.upsert(&record("rooms", "c", 3)).await;
        let snapshot = cache.snapshot().await;

        let restored = RecordHotCache::from_schema_snapshot_and_records(
            &schema,
            Some(&snapshot),
            3,
            &[
                record("rooms", "a", 1),
                record("rooms", "b", 2),
                record("rooms", "c", 3),
            ],
            0,
        );

        assert!(restored.get("rooms", "a").await.flatten().is_some());
        assert!(restored.get("rooms", "b").await.flatten().is_none());
        assert!(restored.get("rooms", "c").await.flatten().is_some());
    }

    #[tokio::test]
    async fn snapshot_restore_applies_durable_rows_after_snapshot() {
        let schema = schema_with_lru(2);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[record("rooms", "a", 1), record("rooms", "b", 2)],
            0,
        );
        assert!(cache.get("rooms", "a").await.flatten().is_some());
        cache.upsert(&record("rooms", "c", 3)).await;
        let snapshot = cache.snapshot().await;

        let restored = RecordHotCache::from_schema_snapshot_and_records(
            &schema,
            Some(&snapshot),
            3,
            &[
                record("rooms", "a", 1),
                record("rooms", "b", 2),
                record("rooms", "c", 3),
                record("rooms", "d", 4),
            ],
            0,
        );

        assert!(restored.get("rooms", "a").await.flatten().is_none());
        assert!(restored.get("rooms", "b").await.flatten().is_none());
        assert!(restored.get("rooms", "c").await.flatten().is_some());
        assert!(restored.get("rooms", "d").await.flatten().is_some());
    }

    #[tokio::test]
    async fn snapshot_skips_volatile_records() {
        let schema = schema_with_lru(2);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 0);
        cache.upsert(&volatile_record("rooms", "a")).await;

        let snapshot = cache.snapshot().await;

        assert_eq!(snapshot.record_count(), 0);
    }

    #[tokio::test]
    async fn durable_hydration_keeps_current_volatile_record() {
        let schema = schema_with_lru(2);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[], 0);
        cache.upsert(&volatile_record("rooms", "a")).await;

        cache
            .hydrate_durable_many([record("rooms", "a", 1), record("rooms", "b", 2)].iter())
            .await;

        let current_a = cache.get("rooms", "a").await.flatten().unwrap();
        assert_eq!(current_a.lsn, 0);
        assert!(current_a.path.starts_with("volatile/"));
        assert_eq!(cache.get("rooms", "b").await.flatten().unwrap().lsn, 2);
    }

    #[tokio::test]
    async fn volatile_overlay_detection_tracks_current_hot_records() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 0);

        assert!(!cache.has_volatile_overlay_in_scope("rooms", None).await);
        assert!(!cache.has_volatile_overlay_in_scope("missing", None).await);
        assert_eq!(cache.status().await.volatile_records, 0);

        cache.upsert(&volatile_record("rooms", "volatile")).await;
        assert!(cache.has_volatile_overlay_in_scope("rooms", None).await);
        assert_eq!(cache.status().await.volatile_records, 1);

        cache.upsert(&record("rooms", "volatile", 2)).await;
        assert!(!cache.has_volatile_overlay_in_scope("rooms", None).await);
        assert_eq!(cache.status().await.volatile_records, 0);

        cache.upsert(&volatile_record("rooms", "volatile")).await;
        assert!(cache.has_volatile_overlay_in_scope("rooms", None).await);

        cache.delete("rooms", "volatile").await;
        assert!(!cache.has_volatile_overlay_in_scope("rooms", None).await);
        let status = cache.status().await;
        assert_eq!(status.volatile_records, 0);
        let table = status
            .tables
            .iter()
            .find(|candidate| candidate.table == "rooms")
            .unwrap();
        assert_eq!(table.volatile_records, 0);
    }

    #[tokio::test]
    async fn volatile_overlay_detection_can_be_scoped_by_key_prefix() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[], 0);
        assert_eq!(cache.volatile_generation_in_scope("rooms", None).await, 0);
        assert_eq!(
            cache
                .volatile_generation_in_scope("rooms", Some("room-a:"))
                .await,
            0
        );

        cache
            .upsert(&volatile_record("rooms", "room-a:message-1"))
            .await;
        let first_table_generation = cache.volatile_generation_in_scope("rooms", None).await;
        let first_prefix_generation = cache
            .volatile_generation_in_scope("rooms", Some("room-a:"))
            .await;
        assert!(first_table_generation > 0);
        assert!(first_prefix_generation > 0);
        assert!(cache.has_volatile_overlay_in_scope("rooms", None).await);
        assert!(
            cache
                .has_volatile_overlay_in_scope("rooms", Some("room-a:"))
                .await
        );
        assert!(
            !cache
                .has_volatile_overlay_in_scope("rooms", Some("room-b:"))
                .await
        );
        assert_eq!(
            cache
                .volatile_generation_in_scope("rooms", Some("room-b:"))
                .await,
            0
        );

        cache
            .upsert(&volatile_record("rooms", "room-a:message-1"))
            .await;
        assert!(cache.volatile_generation_in_scope("rooms", None).await > first_table_generation);
        assert!(
            cache
                .volatile_generation_in_scope("rooms", Some("room-a:"))
                .await
                > first_prefix_generation
        );

        cache
            .upsert(&volatile_record("rooms", "org:room:message-1"))
            .await;
        assert!(
            cache
                .has_volatile_overlay_in_scope("rooms", Some("org:"))
                .await
        );
        assert!(
            cache
                .has_volatile_overlay_in_scope("rooms", Some("org:room:"))
                .await
        );

        cache.delete("rooms", "room-a:message-1").await;
        assert!(
            !cache
                .has_volatile_overlay_in_scope("rooms", Some("room-a:"))
                .await
        );
        assert!(
            cache
                .volatile_generation_in_scope("rooms", Some("room-a:"))
                .await
                > first_prefix_generation
        );
        assert!(cache.has_volatile_overlay_in_scope("rooms", None).await);

        cache
            .upsert(&record("rooms", "org:room:message-1", 7))
            .await;
        assert!(!cache.has_volatile_overlay_in_scope("rooms", None).await);
        assert!(
            !cache
                .has_volatile_overlay_in_scope("rooms", Some("org:room:"))
                .await
        );
    }

    #[tokio::test]
    async fn key_prefix_reads_preserve_partition_order_and_after_key() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[
                record("rooms", "room-a:m1", 1),
                record("rooms", "room-a:m2", 2),
                record("rooms", "room-b:m1", 3),
            ],
            0,
        );

        let page = cache
            .list_by_key_prefix("rooms", "room-a:", None, Some(10))
            .await
            .unwrap();
        assert_eq!(
            page.iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m2"]
        );

        let after = cache
            .list_by_key_prefix("rooms", "room-a:", Some("room-a:m1"), Some(10))
            .await
            .unwrap();
        assert_eq!(
            after
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m2"]
        );

        let scan = cache
            .scan_key_order("rooms", Some("room-a:"), Some("room-a:m1"))
            .await
            .unwrap();
        assert_eq!(
            scan.iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m2"]
        );
    }

    #[tokio::test]
    async fn key_order_reads_preserve_order_and_after_key() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[
                record("rooms", "a", 1),
                record("rooms", "b", 2),
                record("rooms", "c", 3),
            ],
            0,
        );

        let page = cache.list("rooms", Some("a"), Some(10)).await.unwrap();
        assert_eq!(
            page.iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );

        let limited = cache.list("rooms", Some("a"), Some(1)).await.unwrap();
        assert_eq!(
            limited
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );

        let scan = cache
            .scan_key_order("rooms", None, Some("b"))
            .await
            .unwrap();
        assert_eq!(
            scan.iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );
    }

    #[tokio::test]
    async fn durable_delete_tombstone_shadows_key_order_overlay_until_cleared() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[
                record("rooms", "room-a:m1", 1),
                record("rooms", "room-a:m2", 2),
                record("rooms", "room-a:m3", 3),
            ],
            0,
        );

        cache.delete_durable("rooms", "room-a:m2", 4).await;

        assert!(
            cache
                .durable_delete_lsn("rooms", "room-a:m2")
                .await
                .flatten()
                .is_some()
        );
        assert!(cache.get("rooms", "room-a:m2").await.flatten().is_none());
        let overlay = cache
            .scan_key_order_overlay("rooms", Some("room-a:"), None)
            .await
            .expect("hot overlay");
        assert_eq!(
            overlay
                .records
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:m1", "room-a:m3"]
        );
        assert!(overlay.shadow_keys.contains("room-a:m2"));

        cache
            .clear_durable_delete_tombstone("rooms", "room-a:m2", 3)
            .await;
        assert!(
            cache
                .durable_delete_lsn("rooms", "room-a:m2")
                .await
                .flatten()
                .is_some()
        );
        cache
            .clear_durable_delete_tombstone("rooms", "room-a:m2", 4)
            .await;
        assert!(
            cache
                .durable_delete_lsn("rooms", "room-a:m2")
                .await
                .flatten()
                .is_none()
        );
    }

    #[tokio::test]
    async fn durable_upsert_clears_older_delete_tombstone() {
        let schema = schema_with_lru(10);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[], 0);

        cache.delete_durable("rooms", "a", 4).await;
        cache.upsert(&record("rooms", "a", 5)).await;

        assert!(
            cache
                .durable_delete_lsn("rooms", "a")
                .await
                .flatten()
                .is_none()
        );
        assert_eq!(cache.get("rooms", "a").await.flatten().unwrap().lsn, 5);
    }

    #[tokio::test]
    async fn key_order_reads_touch_returned_records_as_lru_recent() {
        let schema = schema_with_lru(3);
        let cache = RecordHotCache::from_schema_and_records(
            &schema,
            &[
                record("rooms", "a", 1),
                record("rooms", "b", 2),
                record("rooms", "c", 3),
            ],
            0,
        );

        let page = cache.list("rooms", None, Some(2)).await.unwrap();
        assert_eq!(
            page.iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        cache.upsert(&record("rooms", "d", 4)).await;

        assert!(cache.get("rooms", "a").await.flatten().is_some());
        assert!(cache.get("rooms", "b").await.flatten().is_some());
        assert!(cache.get("rooms", "c").await.flatten().is_none());
        assert!(cache.get("rooms", "d").await.flatten().is_some());
    }

    #[tokio::test]
    async fn durable_idle_ttl_evicts_only_durable_records() {
        let schema = schema_with_lru(10);
        let cache =
            RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 50);
        cache.upsert(&volatile_record("rooms", "volatile")).await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(cache.get("rooms", "a").await.flatten().is_none());
        let volatile = cache.get("rooms", "volatile").await.flatten().unwrap();
        assert_eq!(volatile.lsn, 0);
        assert_eq!(cache.status().await.record_count, 1);
    }

    #[tokio::test]
    async fn status_observes_without_evicting_idle_records() {
        let schema = schema_with_lru(10);
        let cache =
            RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 50);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let before = cache.status().await;
        assert_eq!(before.record_count, 1);
        assert_eq!(before.durable_idle_last_sweep_at_ms, None);

        assert_eq!(cache.evict_idle_durable_records().await, 1);
        let after = cache.status().await;
        assert_eq!(after.record_count, 0);
        assert_eq!(after.durable_idle_last_evicted, 1);
        assert_eq!(after.durable_idle_total_evicted, 1);
        assert!(after.durable_idle_last_sweep_at_ms.is_some());
    }

    #[tokio::test]
    async fn status_reports_runtime_hot_cache_counters() {
        let schema = schema_with_lru(2);
        let cache = RecordHotCache::from_schema_and_records(&schema, &[record("rooms", "a", 1)], 0);
        let initial = cache.status().await;
        assert_eq!(initial.upsert_total, 0);
        assert_eq!(initial.hydrate_durable_total, 0);

        assert!(cache.get("rooms", "a").await.flatten().is_some());
        assert!(cache.get("rooms", "missing").await.flatten().is_none());
        assert_eq!(cache.list("rooms", None, Some(10)).await.unwrap().len(), 1);
        cache
            .hydrate_durable_many([record("rooms", "b", 2)].iter())
            .await;
        cache.upsert(&record("rooms", "c", 3)).await;
        assert_eq!(cache.evict_many("rooms", ["c"].iter().copied()).await, 1);

        let status = cache.status().await;
        assert_eq!(status.get_total, 2);
        assert_eq!(status.get_hit_total, 1);
        assert_eq!(status.get_miss_total, 1);
        assert_eq!(status.list_total, 1);
        assert_eq!(status.list_records_total, 1);
        assert_eq!(status.hydrate_durable_total, 1);
        assert_eq!(status.upsert_total, 1);
        assert_eq!(status.evict_total, 1);
        assert_eq!(status.lru_evicted_total, 1);
        let table = status
            .tables
            .iter()
            .find(|candidate| candidate.table == "rooms")
            .unwrap();
        assert_eq!(table.counters.get_total, status.get_total);
        assert_eq!(table.counters.get_hit_total, status.get_hit_total);
        assert_eq!(table.counters.get_miss_total, status.get_miss_total);
        assert_eq!(table.counters.list_total, status.list_total);
        assert_eq!(table.counters.list_records_total, status.list_records_total);
        assert_eq!(
            table.counters.hydrate_durable_total,
            status.hydrate_durable_total
        );
        assert_eq!(table.counters.upsert_total, status.upsert_total);
        assert_eq!(table.counters.evict_total, status.evict_total);
        assert_eq!(table.counters.lru_evicted_total, status.lru_evicted_total);
    }
}
