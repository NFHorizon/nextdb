use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    AppState,
    aggregate::AggregateSumKey,
    api::frames::{NestedTableSubscription, TableSubscription},
    api::records::{
        ListRecordsQuery, ListRecordsResponse, QueryRecordsByIndexQuery, RecordPredicate,
        RecordQueryDiff, RecordQueryRemovedRecord, execute_record_index_query,
        execute_record_list_query, parse_index_prefix_values, parse_index_query_values,
        record_matches_index_value_prefix, record_matches_index_values, record_matches_predicate,
    },
    config::RuntimeLimits,
    model::{DbRecord, DeliveryEvent},
    schema::{DatabaseSchema, IndexSchema},
    util::Sha256Writer,
};

pub(crate) const LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES: usize = 256;

#[derive(Default)]
pub(crate) struct RealtimeConnectionState {
    pub(crate) subscribed_rooms: BTreeSet<String>,
    pub(crate) subscribed_tables: BTreeSet<String>,
    pub(crate) subscribed_table_ranges: BTreeSet<TableSubscription>,
    pub(crate) subscribed_nested_tables: BTreeSet<NestedTableSubscription>,
    pub(crate) record_subscription_router: RecordSubscriptionRouter,
    pub(crate) subscribed_queries: BTreeMap<String, RecordQuerySubscription>,
    pub(crate) subscribed_query_ids: BTreeSet<String>,
    pub(crate) subscribed_query_ids_by_table: BTreeMap<String, BTreeSet<String>>,
    pub(crate) subscribed_objects: bool,
    pub(crate) subscribed_user_events: bool,
    pub(crate) subscribed_connection_events: bool,
    pub(crate) subscribed_aggregate_counts: BTreeSet<String>,
    pub(crate) subscribed_aggregate_sums: BTreeSet<AggregateSumKey>,
    pub(crate) subscribed_aggregate_presence: BTreeSet<String>,
}

impl RealtimeConnectionState {
    pub(crate) fn add_table_subscription(&mut self, subscription: TableSubscription) -> bool {
        if subscription.is_full_table() {
            return self.subscribed_tables.insert(subscription.table);
        }
        let inserted = self.subscribed_table_ranges.insert(subscription.clone());
        if inserted && !subscription.has_index_prefix() {
            self.record_subscription_router
                .insert_table_subscription(&subscription);
        }
        inserted
    }

    pub(crate) fn remove_table_subscription(&mut self, subscription: &TableSubscription) -> bool {
        if subscription.is_full_table() {
            return self.subscribed_tables.remove(&subscription.table);
        }
        let removed = self.subscribed_table_ranges.remove(subscription);
        if removed && !subscription.has_index_prefix() {
            self.record_subscription_router
                .remove_table_subscription(subscription);
        }
        removed
    }

    pub(crate) fn add_nested_table_subscription(
        &mut self,
        subscription: NestedTableSubscription,
    ) -> bool {
        let inserted = self.subscribed_nested_tables.insert(subscription.clone());
        if inserted {
            self.record_subscription_router
                .insert_nested_table_subscription(&subscription);
        }
        inserted
    }

    pub(crate) fn remove_nested_table_subscription(
        &mut self,
        subscription: &NestedTableSubscription,
    ) -> bool {
        let removed = self.subscribed_nested_tables.remove(subscription);
        if removed {
            self.record_subscription_router
                .remove_nested_table_subscription(subscription);
        }
        removed
    }

    pub(crate) fn add_query_subscription(
        &mut self,
        query_id: String,
        subscription: RecordQuerySubscription,
    ) {
        self.remove_query_subscription(&query_id);
        self.subscribed_query_ids.insert(query_id.clone());
        self.subscribed_query_ids_by_table
            .entry(subscription.subscribed_table.clone())
            .or_default()
            .insert(query_id.clone());
        self.subscribed_queries.insert(query_id, subscription);
    }

    pub(crate) fn remove_query_subscription(&mut self, query_id: &str) -> bool {
        self.remove_query_subscription_entry(query_id).is_some()
            || self.subscribed_query_ids.remove(query_id)
    }

    pub(crate) fn remove_query_subscription_entry(
        &mut self,
        query_id: &str,
    ) -> Option<RecordQuerySubscription> {
        let subscription = self.subscribed_queries.remove(query_id)?;
        self.subscribed_query_ids.remove(query_id);
        if let Some(query_ids) = self
            .subscribed_query_ids_by_table
            .get_mut(&subscription.subscribed_table)
        {
            query_ids.remove(query_id);
            if query_ids.is_empty() {
                self.subscribed_query_ids_by_table
                    .remove(&subscription.subscribed_table);
            }
        }
        Some(subscription)
    }

    pub(crate) fn take_query_subscription(
        &mut self,
        query_id: &str,
    ) -> Option<RecordQuerySubscription> {
        self.subscribed_queries.remove(query_id)
    }

    pub(crate) fn put_query_subscription(
        &mut self,
        query_id: String,
        subscription: RecordQuerySubscription,
    ) {
        self.subscribed_queries.insert(query_id, subscription);
    }

    pub(crate) fn affected_query_ids_for_event<F>(
        &self,
        schema_version: u32,
        event: &DeliveryEvent,
        matches_event: F,
    ) -> Vec<String>
    where
        F: Fn(u32, &RecordQuerySubscription, &DeliveryEvent) -> bool,
    {
        let Some(table) = event.table() else {
            return Vec::new();
        };
        let Some(query_ids) = self.subscribed_query_ids_by_table.get(table) else {
            return Vec::new();
        };
        query_ids
            .iter()
            .filter_map(|query_id| {
                let subscription = self.subscribed_queries.get(query_id)?;
                if matches_event(schema_version, subscription, event) {
                    Some(query_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn subscribed_query_table_counts(&self) -> BTreeMap<String, usize> {
        self.subscribed_query_ids_by_table
            .iter()
            .map(|(table, query_ids)| (table.clone(), query_ids.len()))
            .collect()
    }

    pub(crate) fn projected_query_count(&self, query_id: &str) -> usize {
        self.subscribed_query_ids.len() + usize::from(!self.subscribed_query_ids.contains(query_id))
    }

    pub(crate) fn query_subscription_limit_error(
        &self,
        query_id: &str,
        subscribed_table: &str,
        limits: &RuntimeLimits,
    ) -> Option<String> {
        let total_after = self.projected_query_count(query_id);
        if limits.max_live_queries_per_connection > 0
            && total_after > limits.max_live_queries_per_connection
        {
            return Some(format!(
                "live query subscription limit exceeded: maxLiveQueriesPerConnection={} current={} requested={}",
                limits.max_live_queries_per_connection,
                self.subscribed_query_ids.len(),
                total_after
            ));
        }

        let existing_table = self
            .subscribed_queries
            .get(query_id)
            .map(|subscription| subscription.subscribed_table.as_str());
        let table_count = self
            .subscribed_query_ids_by_table
            .get(subscribed_table)
            .map(BTreeSet::len)
            .unwrap_or_default();
        let table_after = table_count + usize::from(existing_table != Some(subscribed_table));
        if limits.max_live_queries_per_table_per_connection > 0
            && table_after > limits.max_live_queries_per_table_per_connection
        {
            return Some(format!(
                "live query subscription table limit exceeded: table={subscribed_table} maxLiveQueriesPerTablePerConnection={} current={} requested={}",
                limits.max_live_queries_per_table_per_connection, table_count, table_after
            ));
        }

        None
    }
}

#[derive(Clone)]
pub(crate) struct LiveQueryMetrics {
    subscribed_total: Arc<AtomicU64>,
    unsubscribed_total: Arc<AtomicU64>,
    event_batches_total: Arc<AtomicU64>,
    batched_events_total: Arc<AtomicU64>,
    refresh_candidates_total: Arc<AtomicU64>,
    refresh_total: Arc<AtomicU64>,
    query_executions_total: Arc<AtomicU64>,
    result_frames_total: Arc<AtomicU64>,
    diff_frames_total: Arc<AtomicU64>,
    unchanged_total: Arc<AtomicU64>,
    evaluation_cache_hits_total: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LiveQueryMetricsStatus {
    pub(crate) current: usize,
    pub(crate) event_batch_max: usize,
    pub(crate) subscribed_total: u64,
    pub(crate) unsubscribed_total: u64,
    pub(crate) event_batches_total: u64,
    pub(crate) batched_events_total: u64,
    pub(crate) refresh_candidates_total: u64,
    pub(crate) refresh_total: u64,
    pub(crate) query_executions_total: u64,
    pub(crate) result_frames_total: u64,
    pub(crate) diff_frames_total: u64,
    pub(crate) unchanged_total: u64,
    pub(crate) evaluation_cache_hits_total: u64,
    pub(crate) errors_total: u64,
}

impl LiveQueryMetrics {
    pub(crate) fn new() -> Self {
        Self {
            subscribed_total: Arc::new(AtomicU64::new(0)),
            unsubscribed_total: Arc::new(AtomicU64::new(0)),
            event_batches_total: Arc::new(AtomicU64::new(0)),
            batched_events_total: Arc::new(AtomicU64::new(0)),
            refresh_candidates_total: Arc::new(AtomicU64::new(0)),
            refresh_total: Arc::new(AtomicU64::new(0)),
            query_executions_total: Arc::new(AtomicU64::new(0)),
            result_frames_total: Arc::new(AtomicU64::new(0)),
            diff_frames_total: Arc::new(AtomicU64::new(0)),
            unchanged_total: Arc::new(AtomicU64::new(0)),
            evaluation_cache_hits_total: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn note_subscribed(&self) {
        self.subscribed_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_unsubscribed(&self) {
        self.unsubscribed_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_event_batch(&self, event_count: usize) {
        self.event_batches_total.fetch_add(1, Ordering::AcqRel);
        self.batched_events_total
            .fetch_add(event_count as u64, Ordering::AcqRel);
    }

    pub(crate) fn note_refresh_candidates(&self, count: usize) {
        self.refresh_candidates_total
            .fetch_add(count as u64, Ordering::AcqRel);
    }

    pub(crate) fn note_refresh(&self) {
        self.refresh_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_query_execution(&self) {
        self.query_executions_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_result_frame(&self) {
        self.result_frames_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_diff_frame(&self) {
        self.diff_frames_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_unchanged(&self) {
        self.unchanged_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_evaluation_cache_hit(&self) {
        self.evaluation_cache_hits_total
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_error(&self) {
        self.errors_total.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn snapshot(
        &self,
        current: usize,
        event_batch_max: usize,
    ) -> LiveQueryMetricsStatus {
        LiveQueryMetricsStatus {
            current,
            event_batch_max,
            subscribed_total: self.subscribed_total.load(Ordering::Acquire),
            unsubscribed_total: self.unsubscribed_total.load(Ordering::Acquire),
            event_batches_total: self.event_batches_total.load(Ordering::Acquire),
            batched_events_total: self.batched_events_total.load(Ordering::Acquire),
            refresh_candidates_total: self.refresh_candidates_total.load(Ordering::Acquire),
            refresh_total: self.refresh_total.load(Ordering::Acquire),
            query_executions_total: self.query_executions_total.load(Ordering::Acquire),
            result_frames_total: self.result_frames_total.load(Ordering::Acquire),
            diff_frames_total: self.diff_frames_total.load(Ordering::Acquire),
            unchanged_total: self.unchanged_total.load(Ordering::Acquire),
            evaluation_cache_hits_total: self.evaluation_cache_hits_total.load(Ordering::Acquire),
            errors_total: self.errors_total.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordKeyRange {
    lower_inclusive: Option<String>,
    upper_exclusive: Option<String>,
}

impl RecordKeyRange {
    pub(crate) fn new(lower_inclusive: Option<String>, upper_exclusive: Option<String>) -> Self {
        Self {
            lower_inclusive,
            upper_exclusive,
        }
    }
}

#[derive(Default)]
pub(crate) struct RecordSubscriptionRouter {
    key_prefixes_by_table: BTreeMap<String, BTreeSet<String>>,
    key_ranges_by_table: BTreeMap<String, TableKeyRangeIndex>,
}

impl RecordSubscriptionRouter {
    pub(crate) fn insert_table_subscription(&mut self, subscription: &TableSubscription) {
        self.insert_key_range(
            subscription.table.clone(),
            RecordKeyRange::new(
                subscription.lower_key.clone(),
                subscription.upper_key.clone(),
            ),
        );
    }

    pub(crate) fn remove_table_subscription(&mut self, subscription: &TableSubscription) {
        self.remove_key_range(
            &subscription.table,
            &RecordKeyRange::new(
                subscription.lower_key.clone(),
                subscription.upper_key.clone(),
            ),
        );
    }

    pub(crate) fn insert_nested_table_subscription(
        &mut self,
        subscription: &NestedTableSubscription,
    ) {
        self.key_prefixes_by_table
            .entry(subscription.logical_table())
            .or_default()
            .insert(subscription.key_prefix());
    }

    pub(crate) fn remove_nested_table_subscription(
        &mut self,
        subscription: &NestedTableSubscription,
    ) {
        let logical_table = subscription.logical_table();
        let key_prefix = subscription.key_prefix();
        let Some(prefixes) = self.key_prefixes_by_table.get_mut(&logical_table) else {
            return;
        };
        prefixes.remove(&key_prefix);
        if prefixes.is_empty() {
            self.key_prefixes_by_table.remove(&logical_table);
        }
    }

    pub(crate) fn insert_key_range(&mut self, table: String, range: RecordKeyRange) {
        self.key_ranges_by_table
            .entry(table)
            .or_default()
            .insert(range);
    }

    pub(crate) fn remove_key_range(&mut self, table: &str, range: &RecordKeyRange) {
        let Some(index) = self.key_ranges_by_table.get_mut(table) else {
            return;
        };
        index.remove(range);
        if index.is_empty() {
            self.key_ranges_by_table.remove(table);
        }
    }

    pub(crate) fn matches(&self, table: &str, key: &str) -> bool {
        self.matches_key_prefix(table, key) || self.matches_key_range(table, key)
    }

    fn matches_key_prefix(&self, table: &str, key: &str) -> bool {
        self.key_prefixes_by_table
            .get(table)
            .is_some_and(|prefixes| {
                prefixes
                    .range(..=key.to_string())
                    .next_back()
                    .is_some_and(|prefix| key.starts_with(prefix))
            })
    }

    fn matches_key_range(&self, table: &str, key: &str) -> bool {
        self.key_ranges_by_table
            .get(table)
            .is_some_and(|ranges| ranges.matches(key))
    }
}

#[derive(Default)]
struct TableKeyRangeIndex {
    unbounded_lower_uppers: BTreeSet<Option<String>>,
    uppers_by_lower: BTreeMap<String, BTreeSet<Option<String>>>,
    unbounded_lower_best_upper: Option<Option<String>>,
    prefix_best_upper_by_lower: BTreeMap<String, Option<String>>,
}

impl TableKeyRangeIndex {
    fn insert(&mut self, range: RecordKeyRange) {
        match range.lower_inclusive {
            Some(lower) => {
                self.uppers_by_lower
                    .entry(lower)
                    .or_default()
                    .insert(range.upper_exclusive);
            }
            None => {
                self.unbounded_lower_uppers.insert(range.upper_exclusive);
            }
        }
        self.rebuild_match_cache();
    }

    fn remove(&mut self, range: &RecordKeyRange) {
        match range.lower_inclusive.as_deref() {
            Some(lower) => {
                let Some(uppers) = self.uppers_by_lower.get_mut(lower) else {
                    return;
                };
                uppers.remove(&range.upper_exclusive);
                if uppers.is_empty() {
                    self.uppers_by_lower.remove(lower);
                }
            }
            None => {
                self.unbounded_lower_uppers.remove(&range.upper_exclusive);
            }
        }
        self.rebuild_match_cache();
    }

    fn matches(&self, key: &str) -> bool {
        best_upper_matches_key(self.unbounded_lower_best_upper.as_ref(), key)
            || self
                .prefix_best_upper_by_lower
                .range(..=key.to_string())
                .next_back()
                .is_some_and(|(_, upper)| best_upper_matches_key(Some(upper), key))
    }

    fn is_empty(&self) -> bool {
        self.unbounded_lower_uppers.is_empty() && self.uppers_by_lower.is_empty()
    }

    fn rebuild_match_cache(&mut self) {
        self.unbounded_lower_best_upper = best_upper_for_set(&self.unbounded_lower_uppers);
        self.prefix_best_upper_by_lower.clear();
        let mut prefix_best_upper = None;
        for (lower, uppers) in &self.uppers_by_lower {
            prefix_best_upper = wider_upper(prefix_best_upper, best_upper_for_set(uppers));
            if let Some(best_upper) = prefix_best_upper.clone() {
                self.prefix_best_upper_by_lower
                    .insert(lower.clone(), best_upper);
            }
        }
    }
}

fn best_upper_for_set(uppers: &BTreeSet<Option<String>>) -> Option<Option<String>> {
    if uppers.is_empty() {
        None
    } else if uppers.contains(&None) {
        Some(None)
    } else {
        Some(uppers.iter().next_back().cloned().flatten())
    }
}

fn wider_upper(
    current: Option<Option<String>>,
    candidate: Option<Option<String>>,
) -> Option<Option<String>> {
    match (current, candidate) {
        (Some(None), _) | (_, Some(None)) => Some(None),
        (None, candidate) => candidate,
        (current, None) => current,
        (Some(Some(current)), Some(Some(candidate))) => Some(Some(current.max(candidate))),
    }
}

fn best_upper_matches_key(best_upper: Option<&Option<String>>, key: &str) -> bool {
    best_upper.is_some_and(|upper| upper.as_deref().is_none_or(|upper| key < upper))
}

pub(crate) fn record_matches_table_filters(
    table: &str,
    key: &str,
    table_filter: &HashSet<String>,
    table_range_filter: &BTreeSet<TableSubscription>,
    nested_table_filter: &BTreeSet<NestedTableSubscription>,
) -> bool {
    table_filter.contains(table)
        || table_range_filter
            .iter()
            .any(|subscription| subscription.matches(table, key))
        || nested_table_filter.iter().any(|subscription| {
            subscription.logical_table() == table && key.starts_with(&subscription.key_prefix())
        })
}

pub(crate) fn record_matches_table_router(
    table: &str,
    key: &str,
    table_filter: &HashSet<String>,
    record_subscription_router: &RecordSubscriptionRouter,
) -> bool {
    table_filter.contains(table) || record_subscription_router.matches(table, key)
}

pub(crate) fn record_matches_table_subscription(
    table: &str,
    key: &str,
    record: Option<&DbRecord>,
    schema: &DatabaseSchema,
    subscription: &TableSubscription,
) -> bool {
    if subscription.table != table {
        return false;
    }
    if !subscription.has_index_prefix() {
        return subscription.matches(table, key);
    }
    let Some(record) = record else {
        return false;
    };
    let Some(index_name) = subscription.index_name.as_deref() else {
        return false;
    };
    let Some(index_values) = subscription.index_values.as_deref() else {
        return false;
    };
    let Some(index) = table_subscription_index_schema(schema, table, index_name) else {
        return false;
    };
    let Ok(values) = parse_index_prefix_values(index_values, index) else {
        return false;
    };
    record_matches_index_value_prefix(record, index, &values)
}

fn table_subscription_index_schema<'a>(
    schema: &'a DatabaseSchema,
    table: &str,
    index_name: &str,
) -> Option<&'a IndexSchema> {
    match table.split_once('.') {
        Some((parent, nested)) => schema
            .tables
            .get(parent)
            .and_then(|table| table.nested.get(nested))
            .and_then(|nested| nested.indexes.get(index_name)),
        None => schema
            .tables
            .get(table)
            .and_then(|table| table.indexes.get(index_name)),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecordQuerySubscription {
    pub(crate) query_id: String,
    pub(crate) table: String,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) subscribed_table: String,
    pub(crate) parent_key_prefix: Option<String>,
    pub(crate) index_name: Option<String>,
    pub(crate) index_query: QueryRecordsByIndexQuery,
    pub(crate) impact_filter: RecordQueryImpactFilter,
    pub(crate) schema_version: u32,
    pub(crate) after_key: Option<String>,
    pub(crate) after_cursor: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) order: Option<String>,
    pub(crate) predicate: Option<RecordPredicate>,
    pub(crate) last_result_id: Option<String>,
    pub(crate) last_response: Option<ListRecordsResponse>,
    pub(crate) last_response_keys: HashSet<String>,
    pub(crate) retained_scope_keys: BTreeSet<String>,
    pub(crate) diff: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RecordQueryPlanKey {
    pub(crate) table: String,
    pub(crate) parent_key: Option<String>,
    pub(crate) nested: Option<String>,
    pub(crate) index_name: Option<String>,
    pub(crate) index_query: Option<String>,
    pub(crate) after_key: Option<String>,
    pub(crate) after_cursor: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) order: Option<String>,
    pub(crate) predicate: Option<String>,
}

impl RecordQuerySubscription {
    pub(crate) fn plan_key(&self) -> RecordQueryPlanKey {
        RecordQueryPlanKey {
            table: self.table.clone(),
            parent_key: self.parent_key.clone(),
            nested: self.nested.clone(),
            index_name: self.index_name.clone(),
            index_query: self
                .index_name
                .as_ref()
                .map(|_| serde_json::to_string(&self.index_query).unwrap_or_default()),
            after_key: self.after_key.clone(),
            after_cursor: self.after_cursor.clone(),
            limit: self.limit,
            order: self.order.clone(),
            predicate: self
                .predicate
                .as_ref()
                .map(|predicate| serde_json::to_string(predicate).unwrap_or_default()),
        }
    }

    pub(crate) fn apply_snapshot(&mut self, snapshot: RecordQuerySnapshot) {
        self.last_result_id = Some(snapshot.result_id);
        self.last_response_keys = snapshot
            .response
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect();
        self.last_response = if self.diff {
            Some(snapshot.response)
        } else {
            None
        };
    }

    pub(crate) fn last_response_contains_key(&self, key: &str) -> bool {
        self.last_response_keys.contains(key)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RecordQueryImpactFilter {
    AllUpserts,
    Predicate {
        predicate: RecordPredicate,
    },
    ExactIndex {
        index: IndexSchema,
        values: Vec<serde_json::Value>,
        predicate: Option<RecordPredicate>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RecordQueryDeletedHint {
    pub(crate) lsn: u64,
    pub(crate) deleted_at_ms: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RecordQueryDeletedHints {
    hints: HashMap<(String, String), RecordQueryDeletedHint>,
}

impl RecordQueryDeletedHints {
    pub(crate) fn add_event(&mut self, event: &DeliveryEvent) {
        let DeliveryEvent::RecordDeleted {
            table,
            key,
            lsn,
            deleted_at_ms,
            ..
        } = event
        else {
            return;
        };
        self.hints.insert(
            (table.clone(), key.clone()),
            RecordQueryDeletedHint {
                lsn: *lsn,
                deleted_at_ms: *deleted_at_ms,
            },
        );
    }

    pub(crate) fn get(&self, table: &str, key: &str) -> Option<&RecordQueryDeletedHint> {
        self.hints.get(&(table.to_string(), key.to_string()))
    }

    pub(crate) fn as_ref(&self) -> Option<&Self> {
        if self.hints.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecordQueryEvaluation {
    pub(crate) response: ListRecordsResponse,
    pub(crate) result_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveQueryEvaluationCacheKey {
    pub(crate) token: LiveQueryEvaluationCacheToken,
    pub(crate) plan_key: RecordQueryPlanKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveQueryEvaluationCacheToken {
    pub(crate) lsn: u64,
    pub(crate) volatile_generation: u64,
}

#[derive(Debug, Default)]
pub(crate) struct LiveQueryEvaluationCache {
    pub(crate) entries: BTreeMap<LiveQueryEvaluationCacheKey, RecordQueryEvaluation>,
}

impl LiveQueryEvaluationCache {
    pub(crate) fn get(&self, key: &LiveQueryEvaluationCacheKey) -> Option<RecordQueryEvaluation> {
        self.entries.get(key).cloned()
    }

    pub(crate) fn insert(
        &mut self,
        key: LiveQueryEvaluationCacheKey,
        evaluation: RecordQueryEvaluation,
    ) {
        self.entries.insert(key, evaluation);
        while self.entries.len() > LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES {
            let Some(oldest_key) = self.entries.keys().next().cloned() else {
                break;
            };
            self.entries.remove(&oldest_key);
        }
    }
}

#[derive(Debug)]
pub(crate) struct RecordQuerySnapshot {
    pub(crate) result_id: String,
    pub(crate) response: ListRecordsResponse,
}

pub(crate) fn affected_live_query_refresh_batch<'a, I>(
    connection_state: &RealtimeConnectionState,
    schema_version: u32,
    events: I,
) -> (BTreeSet<String>, usize, RecordQueryDeletedHints)
where
    I: IntoIterator<Item = &'a DeliveryEvent>,
{
    let mut refresh_candidates = 0usize;
    let mut affected_query_ids = BTreeSet::new();
    let mut deleted_hints = RecordQueryDeletedHints::default();
    for event in events {
        let event_query_ids = connection_state.affected_query_ids_for_event(
            schema_version,
            event,
            record_query_matches_event,
        );
        refresh_candidates += event_query_ids.len();
        affected_query_ids.extend(event_query_ids);
        deleted_hints.add_event(event);
    }
    (affected_query_ids, refresh_candidates, deleted_hints)
}

pub(crate) fn record_event_batch_cache_lsn<'a, I>(events: I, current_lsn: u64) -> Option<u64>
where
    I: IntoIterator<Item = &'a DeliveryEvent>,
{
    let mut saw_record_event = false;
    let mut max_event_lsn = 0_u64;
    for event in events {
        match event {
            DeliveryEvent::RecordUpserted { record, .. } if record.lsn > 0 => {
                saw_record_event = true;
                max_event_lsn = max_event_lsn.max(record.lsn);
            }
            DeliveryEvent::RecordDeleted { lsn, .. } if *lsn > 0 => {
                saw_record_event = true;
                max_event_lsn = max_event_lsn.max(*lsn);
            }
            DeliveryEvent::RecordUpserted { .. } | DeliveryEvent::RecordDeleted { .. } => {
                saw_record_event = true;
            }
            DeliveryEvent::MessageCreated { .. }
            | DeliveryEvent::VolatileRoomEvent { .. }
            | DeliveryEvent::VolatileUserEvent { .. }
            | DeliveryEvent::UserEvent { .. }
            | DeliveryEvent::UserUpserted { .. }
            | DeliveryEvent::ObjectCommitted { .. }
            | DeliveryEvent::ObjectDeleted { .. } => {}
        }
    }
    (saw_record_event && current_lsn >= max_event_lsn).then_some(current_lsn)
}

pub(crate) async fn live_query_cache_token_with_lsn(
    state: &AppState,
    subscription: &RecordQuerySubscription,
    lsn: Option<u64>,
) -> Option<LiveQueryEvaluationCacheToken> {
    let lsn = lsn?;
    let volatile_generation = state
        .record_hot
        .volatile_generation_in_scope(
            &subscription.subscribed_table,
            subscription.parent_key_prefix.as_deref(),
        )
        .await;
    Some(LiveQueryEvaluationCacheToken {
        lsn,
        volatile_generation,
    })
}

pub(crate) async fn cached_record_query_evaluation(
    state: &AppState,
    subscription: &RecordQuerySubscription,
    cache_token: Option<LiveQueryEvaluationCacheToken>,
    plan_key: &RecordQueryPlanKey,
) -> std::result::Result<RecordQueryEvaluation, String> {
    let Some(token) = cache_token else {
        return execute_record_query_evaluation(state, subscription).await;
    };
    let cache_key = LiveQueryEvaluationCacheKey {
        token,
        plan_key: plan_key.clone(),
    };
    if let Some(evaluation) = state
        .live_query_evaluation_cache
        .lock()
        .await
        .get(&cache_key)
    {
        state.live_query_metrics.note_evaluation_cache_hit();
        return Ok(evaluation);
    }
    let evaluation = execute_record_query_evaluation(state, subscription).await?;
    state
        .live_query_evaluation_cache
        .lock()
        .await
        .insert(cache_key, evaluation.clone());
    Ok(evaluation)
}

pub(crate) async fn execute_record_query_evaluation(
    state: &AppState,
    subscription: &RecordQuerySubscription,
) -> std::result::Result<RecordQueryEvaluation, String> {
    state.live_query_metrics.note_query_execution();
    let response = if let Some(index_name) = subscription.index_name.clone() {
        execute_record_index_query(
            state,
            subscription.table.clone(),
            subscription.parent_key.clone(),
            subscription.nested.clone(),
            index_name,
            subscription.index_query.clone(),
        )
        .await
    } else {
        execute_record_list_query(
            state,
            subscription.table.clone(),
            subscription.parent_key.clone(),
            subscription.nested.clone(),
            ListRecordsQuery {
                consistency: Default::default(),
                after_key: subscription.after_key.clone(),
                after_cursor: subscription.after_cursor.clone(),
                limit: subscription.limit,
                order: subscription.order.clone(),
                shard: None,
                predicate: subscription.predicate.clone(),
            },
        )
        .await
    }
    .map_err(|err| err.message)?;
    let result_id = record_query_result_id(&response);
    Ok(RecordQueryEvaluation {
        response,
        result_id,
    })
}

pub(crate) fn record_query_diff(
    previous: &ListRecordsResponse,
    next: &ListRecordsResponse,
    deleted_hints: Option<&RecordQueryDeletedHints>,
) -> RecordQueryDiff {
    let mut previous_by_key = HashMap::with_capacity(previous.records.len());
    for record in &previous.records {
        previous_by_key.insert(record.key.as_str(), record);
    }
    let mut next_keys = HashSet::with_capacity(next.records.len());
    for record in &next.records {
        next_keys.insert(record.key.as_str());
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    for record in &next.records {
        match previous_by_key.get(record.key.as_str()) {
            None => added.push(record.clone()),
            Some(previous_record) if record_query_record_changed(previous_record, record) => {
                updated.push(record.clone());
            }
            Some(_) => {}
        }
    }

    let removed = previous
        .records
        .iter()
        .filter(|record| !next_keys.contains(record.key.as_str()))
        .map(|record| {
            let matching_deleted_hint =
                deleted_hints.and_then(|hints| hints.get(&record.table, &record.key));
            RecordQueryRemovedRecord {
                table: record.table.clone(),
                key: record.key.clone(),
                path: record.path.clone(),
                deleted: matching_deleted_hint.is_some(),
                lsn: matching_deleted_hint.map(|hint| hint.lsn),
                deleted_at_ms: matching_deleted_hint.map(|hint| hint.deleted_at_ms),
            }
        })
        .collect();

    RecordQueryDiff {
        table: next.table.clone(),
        added,
        updated,
        removed,
        keys: next
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect(),
        next_after_key: next.next_after_key.clone(),
        next_cursor: next.next_cursor.clone(),
        has_more: next.has_more,
    }
}

pub(crate) fn record_query_result_id(response: &ListRecordsResponse) -> String {
    let mut hasher = Sha256::new();
    hasher.update(response.table.as_bytes());
    hasher.update([0]);
    hasher.update(if response.has_more { [1] } else { [0] });
    hasher.update([0]);
    if let Some(next_after_key) = response.next_after_key.as_deref() {
        hasher.update(next_after_key.as_bytes());
    }
    hasher.update([0]);
    if let Some(next_cursor) = response.next_cursor.as_deref() {
        hasher.update(next_cursor.as_bytes());
    }
    hasher.update([0]);
    for record in &response.records {
        hasher.update(record.table.as_bytes());
        hasher.update([0]);
        hasher.update(record.key.as_bytes());
        hasher.update([0]);
        hasher.update(record.lsn.to_le_bytes());
        hasher.update([0]);
        hasher.update(record.updated_at_ms.to_le_bytes());
        hasher.update([0]);
        hasher.update(record.path.as_bytes());
        hasher.update([0]);
        hash_json_value(&mut hasher, &record.value);
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn hash_json_value(hasher: &mut Sha256, value: &serde_json::Value) {
    let mut writer = Sha256Writer { hasher };
    let _ = serde_json::to_writer(&mut writer, value);
}

fn record_query_record_changed(previous: &DbRecord, next: &DbRecord) -> bool {
    previous.lsn != next.lsn
        || previous.updated_at_ms != next.updated_at_ms
        || previous.path != next.path
        || previous.value != next.value
}

pub(crate) fn record_query_matches_event(
    schema_version: u32,
    subscription: &RecordQuerySubscription,
    event: &DeliveryEvent,
) -> bool {
    let Some(event_table) = event.table() else {
        return false;
    };
    if event_table != subscription.subscribed_table {
        return false;
    }
    if let Some(prefix) = subscription.parent_key_prefix.as_deref() {
        match event {
            DeliveryEvent::RecordUpserted { key, .. }
            | DeliveryEvent::RecordDeleted { key, .. } => {
                if !key.starts_with(prefix) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    match event {
        DeliveryEvent::RecordUpserted { key, record, .. } => {
            record_query_upsert_may_change_result(schema_version, subscription, key, record)
        }
        _ => true,
    }
}

fn record_query_upsert_may_change_result(
    schema_version: u32,
    subscription: &RecordQuerySubscription,
    key: &str,
    record: &DbRecord,
) -> bool {
    if subscription.last_response_contains_key(key) {
        return true;
    }

    if schema_version != subscription.schema_version {
        return true;
    }

    match &subscription.impact_filter {
        RecordQueryImpactFilter::AllUpserts => true,
        RecordQueryImpactFilter::Predicate { predicate } => {
            record_matches_predicate(record, Some(predicate))
        }
        RecordQueryImpactFilter::ExactIndex {
            index,
            values,
            predicate,
        } => {
            record_matches_predicate(record, predicate.as_ref())
                && record_matches_index_values(record, index, values)
        }
    }
}

pub(crate) fn record_query_impact_filter(
    schema: &DatabaseSchema,
    table: &str,
    nested: Option<&str>,
    index_name: Option<&str>,
    index_query: &QueryRecordsByIndexQuery,
    predicate: Option<&RecordPredicate>,
) -> RecordQueryImpactFilter {
    let predicate = index_name
        .and(index_query.predicate.as_ref())
        .or(predicate)
        .cloned();
    let Some(index_name) = index_name else {
        return predicate
            .map(|predicate| RecordQueryImpactFilter::Predicate { predicate })
            .unwrap_or(RecordQueryImpactFilter::AllUpserts);
    };
    if index_query.is_range_query() {
        return predicate
            .map(|predicate| RecordQueryImpactFilter::Predicate { predicate })
            .unwrap_or(RecordQueryImpactFilter::AllUpserts);
    }
    let Some(index) = record_query_index_schema(schema, table, nested, index_name) else {
        return predicate
            .map(|predicate| RecordQueryImpactFilter::Predicate { predicate })
            .unwrap_or(RecordQueryImpactFilter::AllUpserts);
    };
    let Ok(values) = parse_index_query_values(index_query, index) else {
        return predicate
            .map(|predicate| RecordQueryImpactFilter::Predicate { predicate })
            .unwrap_or(RecordQueryImpactFilter::AllUpserts);
    };
    RecordQueryImpactFilter::ExactIndex {
        index: index.clone(),
        values,
        predicate,
    }
}

fn record_query_index_schema<'a>(
    schema: &'a DatabaseSchema,
    table: &str,
    nested: Option<&str>,
    index_name: &str,
) -> Option<&'a IndexSchema> {
    match nested {
        Some(nested) => schema
            .tables
            .get(table)
            .and_then(|table| table.nested.get(nested))
            .and_then(|nested| nested.indexes.get(index_name)),
        None => schema
            .tables
            .get(table)
            .and_then(|table| table.indexes.get(index_name)),
    }
}

pub(crate) fn is_valid_query_id(query_id: &str) -> bool {
    !query_id.is_empty()
        && query_id.len() <= 128
        && query_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_subscription_router_matches_logical_table_prefixes() {
        let mut router = RecordSubscriptionRouter::default();
        router.insert_nested_table_subscription(&NestedTableSubscription::new(
            "rooms".to_string(),
            "room-a".to_string(),
            "messages".to_string(),
        ));
        router.insert_nested_table_subscription(&NestedTableSubscription::new(
            "rooms".to_string(),
            "room-b".to_string(),
            "messages".to_string(),
        ));

        assert!(router.matches("rooms.messages", "room-a:msg-1"));
        assert!(router.matches("rooms.messages", "room-b:msg-1"));
        assert!(!router.matches("rooms.messages", "room-c:msg-1"));
        assert!(!router.matches("rooms.presence", "room-a:user-a"));
    }

    #[test]
    fn record_subscription_router_matches_key_ranges() {
        let mut router = RecordSubscriptionRouter::default();
        let range = RecordKeyRange::new(Some("msg-010".to_string()), Some("msg-020".to_string()));

        router.insert_key_range("messages".to_string(), range.clone());
        router.insert_key_range("messages".to_string(), range.clone());

        assert!(!router.matches("messages", "msg-009"));
        assert!(router.matches("messages", "msg-010"));
        assert!(router.matches("messages", "msg-019"));
        assert!(!router.matches("messages", "msg-020"));
        assert!(!router.matches("other", "msg-010"));

        router.remove_key_range("messages", &range);
        assert!(!router.matches("messages", "msg-010"));
    }

    #[test]
    fn record_subscription_router_matches_unbounded_and_overlapping_key_ranges() {
        let mut router = RecordSubscriptionRouter::default();
        let early_range = RecordKeyRange::new(None, Some("msg-005".to_string()));
        let tail_range = RecordKeyRange::new(Some("msg-090".to_string()), None);

        router.insert_key_range("messages".to_string(), early_range.clone());
        router.insert_key_range("messages".to_string(), tail_range.clone());
        router.insert_key_range(
            "messages".to_string(),
            RecordKeyRange::new(Some("msg-010".to_string()), Some("msg-030".to_string())),
        );
        router.insert_key_range(
            "messages".to_string(),
            RecordKeyRange::new(Some("msg-020".to_string()), Some("msg-040".to_string())),
        );

        assert!(router.matches("messages", "msg-001"));
        assert!(!router.matches("messages", "msg-005"));
        assert!(router.matches("messages", "msg-015"));
        assert!(router.matches("messages", "msg-035"));
        assert!(!router.matches("messages", "msg-050"));
        assert!(router.matches("messages", "msg-090"));
        assert!(router.matches("messages", "msg-999"));

        router.remove_key_range("messages", &early_range);
        router.remove_key_range("messages", &tail_range);
        assert!(!router.matches("messages", "msg-001"));
        assert!(!router.matches("messages", "msg-090"));
    }

    #[test]
    fn record_subscription_router_rebuilds_range_cache_after_removal() {
        let mut router = RecordSubscriptionRouter::default();
        let wide_range =
            RecordKeyRange::new(Some("msg-010".to_string()), Some("msg-100".to_string()));
        let narrow_range =
            RecordKeyRange::new(Some("msg-020".to_string()), Some("msg-030".to_string()));

        router.insert_key_range("messages".to_string(), wide_range.clone());
        router.insert_key_range("messages".to_string(), narrow_range.clone());

        assert!(router.matches("messages", "msg-090"));

        router.remove_key_range("messages", &wide_range);
        assert!(!router.matches("messages", "msg-090"));
        assert!(router.matches("messages", "msg-025"));

        router.remove_key_range("messages", &narrow_range);
        assert!(!router.matches("messages", "msg-025"));
    }

    #[test]
    fn realtime_connection_state_updates_nested_subscription_router() {
        let subscription = NestedTableSubscription::new(
            "rooms".to_string(),
            "room-a".to_string(),
            "messages".to_string(),
        );
        let mut state = RealtimeConnectionState::default();

        assert!(state.add_nested_table_subscription(subscription.clone()));
        assert!(!state.add_nested_table_subscription(subscription.clone()));
        assert!(
            state
                .record_subscription_router
                .matches("rooms.messages", "room-a:msg-1")
        );

        assert!(state.remove_nested_table_subscription(&subscription));
        assert!(!state.remove_nested_table_subscription(&subscription));
        assert!(
            !state
                .record_subscription_router
                .matches("rooms.messages", "room-a:msg-1")
        );
    }
}
