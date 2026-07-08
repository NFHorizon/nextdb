use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use anyhow::Result;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::{model::DeliveryEvent, realtime::RealtimeMember, record_store::RecordStore};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregateCountSnapshot {
    pub(crate) table: String,
    pub(crate) count: usize,
    pub(crate) current_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregateCountUpdate {
    pub(crate) table: String,
    pub(crate) count: usize,
    pub(crate) lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregateSumSnapshot {
    pub(crate) table: String,
    pub(crate) field: String,
    pub(crate) sum: f64,
    pub(crate) current_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregateSumUpdate {
    pub(crate) table: String,
    pub(crate) field: String,
    pub(crate) sum: f64,
    pub(crate) lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregatePresenceSnapshot {
    pub(crate) channel_id: String,
    pub(crate) member_count: usize,
    pub(crate) user_count: usize,
    pub(crate) current_lsn: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AggregatePresenceUpdate {
    pub(crate) channel_id: String,
    pub(crate) member_count: usize,
    pub(crate) user_count: usize,
    pub(crate) current_lsn: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum AggregateUpdate {
    Count(AggregateCountUpdate),
    Sum(AggregateSumUpdate),
    Presence(AggregatePresenceUpdate),
}

#[derive(Clone)]
pub(crate) struct AggregateRegistry {
    inner: Arc<RwLock<AggregateInner>>,
    updates: broadcast::Sender<AggregateUpdate>,
}

#[derive(Default)]
struct AggregateInner {
    table_keys: BTreeMap<String, BTreeSet<String>>,
    hydrated_tables: BTreeSet<String>,
    sum_values: BTreeMap<AggregateSumKey, BTreeMap<String, f64>>,
    hydrated_sums: BTreeSet<AggregateSumKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AggregateSumKey {
    pub(crate) table: String,
    pub(crate) field: String,
}

impl AggregateSumKey {
    pub(crate) fn new(table: String, field: String) -> Self {
        Self { table, field }
    }
}

impl Default for AggregateRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(AggregateInner::default())),
            updates: broadcast::channel(4096).0,
        }
    }
}

impl AggregateRegistry {
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<AggregateUpdate> {
        self.updates.subscribe()
    }

    pub(crate) async fn table_count_snapshot(
        &self,
        records: &RecordStore,
        table: &str,
        current_lsn: u64,
    ) -> Result<AggregateCountSnapshot> {
        if let Some(count) = self.table_count_if_hydrated(table) {
            return Ok(AggregateCountSnapshot {
                table: table.to_string(),
                count,
                current_lsn,
            });
        }

        let mut after_key = None::<String>;
        let mut keys = BTreeSet::new();
        loop {
            let page = records.list(table, after_key.as_deref(), Some(500)).await?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                keys.insert(record.key.clone());
            }
            after_key = page.last().map(|record| record.key.clone());
            if page.len() < 500 {
                break;
            }
        }
        let count = {
            let Ok(mut inner) = self.inner.write() else {
                return Ok(AggregateCountSnapshot {
                    table: table.to_string(),
                    count: keys.len(),
                    current_lsn,
                });
            };
            let table_keys = inner.table_keys.entry(table.to_string()).or_default();
            table_keys.extend(keys);
            let count = table_keys.len();
            inner.hydrated_tables.insert(table.to_string());
            count
        };

        Ok(AggregateCountSnapshot {
            table: table.to_string(),
            count,
            current_lsn,
        })
    }

    pub(crate) async fn table_sum_snapshot(
        &self,
        records: &RecordStore,
        table: &str,
        field: &str,
        current_lsn: u64,
    ) -> Result<AggregateSumSnapshot> {
        let key = AggregateSumKey::new(table.to_string(), field.to_string());
        if let Some(sum) = self.table_sum_if_hydrated(&key) {
            return Ok(AggregateSumSnapshot {
                table: table.to_string(),
                field: field.to_string(),
                sum,
                current_lsn,
            });
        }

        let mut after_key = None::<String>;
        let mut values = BTreeMap::new();
        loop {
            let page = records.list(table, after_key.as_deref(), Some(500)).await?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                if let Some(value) = numeric_field_value(&record.value, field) {
                    values.insert(record.key.clone(), value);
                }
            }
            after_key = page.last().map(|record| record.key.clone());
            if page.len() < 500 {
                break;
            }
        }
        let sum = {
            let Ok(mut inner) = self.inner.write() else {
                return Ok(AggregateSumSnapshot {
                    table: table.to_string(),
                    field: field.to_string(),
                    sum: values.values().sum(),
                    current_lsn,
                });
            };
            let sum_values = inner.sum_values.entry(key.clone()).or_default();
            *sum_values = values;
            let sum = sum_values.values().sum();
            inner.hydrated_sums.insert(key);
            sum
        };

        Ok(AggregateSumSnapshot {
            table: table.to_string(),
            field: field.to_string(),
            sum,
            current_lsn,
        })
    }

    pub(crate) fn channel_presence_snapshot(
        &self,
        channel_id: &str,
        members: &[RealtimeMember],
        current_lsn: u64,
        updated_at_ms: u64,
    ) -> AggregatePresenceSnapshot {
        let (member_count, user_count) = presence_counts(members);
        AggregatePresenceSnapshot {
            channel_id: channel_id.to_string(),
            member_count,
            user_count,
            current_lsn,
            updated_at_ms,
        }
    }

    pub(crate) fn publish_presence_update(
        &self,
        channel_id: &str,
        members: &[RealtimeMember],
        current_lsn: u64,
        updated_at_ms: u64,
    ) {
        let (member_count, user_count) = presence_counts(members);
        let _ = self
            .updates
            .send(AggregateUpdate::Presence(AggregatePresenceUpdate {
                channel_id: channel_id.to_string(),
                member_count,
                user_count,
                current_lsn,
                updated_at_ms,
            }));
    }

    pub(crate) fn apply_delivery_events(&self, events: &[DeliveryEvent]) {
        let updates = self.collect_updates(events);
        for update in updates {
            let _ = self.updates.send(update);
        }
    }

    fn table_count_if_hydrated(&self, table: &str) -> Option<usize> {
        let Ok(inner) = self.inner.read() else {
            return None;
        };
        inner
            .hydrated_tables
            .contains(table)
            .then(|| inner.table_keys.get(table).map_or(0, BTreeSet::len))
    }

    fn table_sum_if_hydrated(&self, key: &AggregateSumKey) -> Option<f64> {
        let Ok(inner) = self.inner.read() else {
            return None;
        };
        inner.hydrated_sums.contains(key).then(|| {
            inner
                .sum_values
                .get(key)
                .map_or(0.0, |values| values.values().sum())
        })
    }

    fn collect_updates(&self, events: &[DeliveryEvent]) -> Vec<AggregateUpdate> {
        let Ok(mut inner) = self.inner.write() else {
            return Vec::new();
        };
        let mut updates = Vec::new();
        for event in events {
            match event {
                DeliveryEvent::RecordUpserted { table, key, record } => {
                    let table_keys = inner.table_keys.entry(table.clone()).or_default();
                    if table_keys.insert(key.clone()) {
                        updates.push(AggregateUpdate::Count(AggregateCountUpdate {
                            table: table.clone(),
                            count: table_keys.len(),
                            lsn: record.lsn,
                        }));
                    }
                    collect_sum_upsert_updates(&mut inner, &mut updates, table, key, record);
                }
                DeliveryEvent::RecordDeleted {
                    table, key, lsn, ..
                } => {
                    let table_keys = inner.table_keys.entry(table.clone()).or_default();
                    if table_keys.remove(key) {
                        updates.push(AggregateUpdate::Count(AggregateCountUpdate {
                            table: table.clone(),
                            count: table_keys.len(),
                            lsn: *lsn,
                        }));
                    }
                    collect_sum_delete_updates(&mut inner, &mut updates, table, key, *lsn);
                }
                _ => {}
            }
        }
        updates
    }
}

fn collect_sum_upsert_updates(
    inner: &mut AggregateInner,
    updates: &mut Vec<AggregateUpdate>,
    table: &str,
    key: &str,
    record: &crate::model::DbRecord,
) {
    let hydrated = inner
        .hydrated_sums
        .iter()
        .filter(|sum_key| sum_key.table == table)
        .cloned()
        .collect::<Vec<_>>();
    for sum_key in hydrated {
        let next_value = numeric_field_value(&record.value, &sum_key.field);
        let values = inner.sum_values.entry(sum_key.clone()).or_default();
        let previous = match next_value {
            Some(value) => values.insert(key.to_string(), value),
            None => values.remove(key),
        };
        if previous != next_value {
            updates.push(AggregateUpdate::Sum(AggregateSumUpdate {
                table: sum_key.table.clone(),
                field: sum_key.field.clone(),
                sum: values.values().sum(),
                lsn: record.lsn,
            }));
        }
    }
}

fn collect_sum_delete_updates(
    inner: &mut AggregateInner,
    updates: &mut Vec<AggregateUpdate>,
    table: &str,
    key: &str,
    lsn: u64,
) {
    let hydrated = inner
        .hydrated_sums
        .iter()
        .filter(|sum_key| sum_key.table == table)
        .cloned()
        .collect::<Vec<_>>();
    for sum_key in hydrated {
        let values = inner.sum_values.entry(sum_key.clone()).or_default();
        if values.remove(key).is_some() {
            updates.push(AggregateUpdate::Sum(AggregateSumUpdate {
                table: sum_key.table.clone(),
                field: sum_key.field.clone(),
                sum: values.values().sum(),
                lsn,
            }));
        }
    }
}

fn numeric_field_value(value: &serde_json::Value, field: &str) -> Option<f64> {
    value.get(field).and_then(serde_json::Value::as_f64)
}

fn presence_counts(members: &[RealtimeMember]) -> (usize, usize) {
    let users = members
        .iter()
        .map(|member| member.user_id.as_str())
        .collect::<BTreeSet<_>>();
    (members.len(), users.len())
}
