use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, RwLock},
};

use tokio::sync::mpsc;

use crate::{
    api::frames::{NestedTableSubscription, TableSubscription},
    api::records::parse_index_prefix_values,
    model::{DbRecord, DeliveryEvent, DeliveryEventBatch, SharedDeliveryEventBatch},
    realtime::{EncodedServerFrame, encode_delivery_events_frame},
    schema::{DatabaseSchema, IndexSchema},
    util::shard_index,
};

const FANOUT_BUCKET_COUNT: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct RoutedRealtimeEventBatch {
    events: SharedDeliveryEventBatch,
    preencoded_events_frame: Option<EncodedServerFrame>,
}

impl RoutedRealtimeEventBatch {
    pub(crate) fn new(
        events: SharedDeliveryEventBatch,
        preencoded_events_frame: Option<EncodedServerFrame>,
    ) -> Self {
        Self {
            events,
            preencoded_events_frame,
        }
    }

    pub(crate) fn preencoded_events_frame(&self) -> Option<EncodedServerFrame> {
        self.preencoded_events_frame.clone()
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.events.extend(other.events);
        self.preencoded_events_frame = None;
    }

    pub(crate) fn refresh_preencoded_events_frame(&mut self) {
        self.preencoded_events_frame = preencode_shared_events(&self.events);
    }
}

impl Deref for RoutedRealtimeEventBatch {
    type Target = [Arc<DeliveryEvent>];

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

#[derive(Clone, Default)]
pub(crate) struct RealtimeFanoutRegistry {
    inner: Arc<RwLock<RealtimeFanoutInner>>,
}

#[derive(Default)]
struct RealtimeFanoutInner {
    sessions: BTreeMap<String, RealtimeFanoutSession>,
    rooms: BTreeMap<String, BTreeSet<String>>,
    full_tables: BTreeMap<String, BTreeSet<String>>,
    range_tables: BTreeMap<String, RangeFanoutIndex>,
    index_prefix_tables: BTreeMap<String, IndexPrefixFanoutIndex>,
    nested_tables: BTreeMap<String, PrefixFanoutIndex>,
    query_tables: BTreeMap<String, BTreeSet<String>>,
    user_events: BTreeMap<String, BTreeSet<String>>,
    objects: BTreeSet<String>,
}

struct RealtimeFanoutSession {
    user_id: Option<String>,
    sender: mpsc::UnboundedSender<RoutedRealtimeEventBatch>,
    subscriptions: RealtimeFanoutSubscriptions,
}

#[derive(Default)]
struct RealtimeFanoutSubscriptions {
    rooms: BTreeSet<String>,
    full_tables: BTreeSet<String>,
    table_ranges: BTreeSet<TableSubscription>,
    index_prefix_tables: BTreeSet<IndexPrefixFanoutSubscription>,
    nested_tables: BTreeSet<NestedTableSubscription>,
    query_tables: BTreeSet<String>,
    user_events: bool,
    objects: bool,
}

impl RealtimeFanoutRegistry {
    pub(crate) fn register(
        &self,
        session_id: String,
        user_id: Option<String>,
        sender: mpsc::UnboundedSender<RoutedRealtimeEventBatch>,
    ) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.remove_session(&session_id);
        inner.sessions.insert(
            session_id,
            RealtimeFanoutSession {
                user_id,
                sender,
                subscriptions: RealtimeFanoutSubscriptions::default(),
            },
        );
    }

    pub(crate) fn unregister(&self, session_id: &str) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.remove_session(session_id);
    }

    #[cfg(test)]
    pub(crate) fn update_record_subscriptions(
        &self,
        session_id: &str,
        rooms: &BTreeSet<String>,
        full_tables: &BTreeSet<String>,
        table_ranges: &BTreeSet<TableSubscription>,
        nested_tables: &BTreeSet<NestedTableSubscription>,
    ) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.replace_record_subscriptions(
            session_id,
            rooms.clone(),
            full_tables.clone(),
            table_ranges.clone(),
            BTreeSet::new(),
            nested_tables.clone(),
        );
    }

    pub(crate) fn update_record_subscriptions_with_schema(
        &self,
        session_id: &str,
        rooms: &BTreeSet<String>,
        full_tables: &BTreeSet<String>,
        table_ranges: &BTreeSet<TableSubscription>,
        nested_tables: &BTreeSet<NestedTableSubscription>,
        schema: &DatabaseSchema,
    ) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.replace_record_subscriptions(
            session_id,
            rooms.clone(),
            full_tables.clone(),
            table_ranges.clone(),
            index_prefix_subscriptions(table_ranges, schema),
            nested_tables.clone(),
        );
    }

    pub(crate) fn update_query_subscriptions(
        &self,
        session_id: &str,
        query_tables: &BTreeSet<String>,
    ) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.replace_query_subscriptions(session_id, query_tables.clone());
    }

    pub(crate) fn update_user_event_subscription(&self, session_id: &str, subscribed: bool) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.replace_user_event_subscription(session_id, subscribed);
    }

    pub(crate) fn update_object_subscription(&self, session_id: &str, subscribed: bool) {
        let Ok(mut inner) = self.inner.write() else {
            return;
        };
        inner.replace_object_subscription(session_id, subscribed);
    }

    pub(crate) fn publish(&self, events: DeliveryEventBatch) -> bool {
        if events.is_empty() {
            return true;
        }
        let deliveries = {
            let Ok(inner) = self.inner.read() else {
                return false;
            };
            if inner.sessions.is_empty() {
                return false;
            }
            inner.deliveries_for_events(&events)
        };
        for (sender, batch) in deliveries {
            let _ = sender.send(batch);
        }
        true
    }
}

impl RealtimeFanoutInner {
    fn remove_session(&mut self, session_id: &str) {
        let Some(session) = self.sessions.remove(session_id) else {
            return;
        };
        self.remove_record_indexes(session_id, &session.subscriptions);
        self.remove_query_indexes(session_id, &session.subscriptions);
        self.remove_user_event_index(
            session_id,
            session.user_id.as_deref(),
            &session.subscriptions,
        );
        self.remove_object_index(session_id, &session.subscriptions);
    }

    fn replace_record_subscriptions(
        &mut self,
        session_id: &str,
        rooms: BTreeSet<String>,
        full_tables: BTreeSet<String>,
        table_ranges: BTreeSet<TableSubscription>,
        index_prefix_tables: BTreeSet<IndexPrefixFanoutSubscription>,
        nested_tables: BTreeSet<NestedTableSubscription>,
    ) {
        let Some(previous) =
            self.sessions
                .get(session_id)
                .map(|session| RealtimeFanoutSubscriptions {
                    rooms: session.subscriptions.rooms.clone(),
                    full_tables: session.subscriptions.full_tables.clone(),
                    table_ranges: session.subscriptions.table_ranges.clone(),
                    index_prefix_tables: session.subscriptions.index_prefix_tables.clone(),
                    nested_tables: session.subscriptions.nested_tables.clone(),
                    ..RealtimeFanoutSubscriptions::default()
                })
        else {
            return;
        };
        self.remove_record_indexes(session_id, &previous);
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        session.subscriptions.rooms = rooms;
        session.subscriptions.full_tables = full_tables;
        session.subscriptions.table_ranges = table_ranges;
        session.subscriptions.index_prefix_tables = index_prefix_tables;
        session.subscriptions.nested_tables = nested_tables;
        let subscriptions = RealtimeFanoutSubscriptions {
            rooms: session.subscriptions.rooms.clone(),
            full_tables: session.subscriptions.full_tables.clone(),
            table_ranges: session.subscriptions.table_ranges.clone(),
            index_prefix_tables: session.subscriptions.index_prefix_tables.clone(),
            nested_tables: session.subscriptions.nested_tables.clone(),
            ..RealtimeFanoutSubscriptions::default()
        };
        self.add_record_indexes(session_id, &subscriptions);
    }

    fn replace_query_subscriptions(&mut self, session_id: &str, query_tables: BTreeSet<String>) {
        let Some(previous) =
            self.sessions
                .get(session_id)
                .map(|session| RealtimeFanoutSubscriptions {
                    query_tables: session.subscriptions.query_tables.clone(),
                    ..RealtimeFanoutSubscriptions::default()
                })
        else {
            return;
        };
        self.remove_query_indexes(session_id, &previous);
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        session.subscriptions.query_tables = query_tables;
        let subscriptions = RealtimeFanoutSubscriptions {
            query_tables: session.subscriptions.query_tables.clone(),
            ..RealtimeFanoutSubscriptions::default()
        };
        self.add_query_indexes(session_id, &subscriptions);
    }

    fn replace_user_event_subscription(&mut self, session_id: &str, subscribed: bool) {
        let Some((user_id, previous)) = self.sessions.get(session_id).map(|session| {
            (
                session.user_id.clone(),
                RealtimeFanoutSubscriptions {
                    user_events: session.subscriptions.user_events,
                    ..RealtimeFanoutSubscriptions::default()
                },
            )
        }) else {
            return;
        };
        self.remove_user_event_index(session_id, user_id.as_deref(), &previous);
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        session.subscriptions.user_events = subscribed;
        let subscriptions = RealtimeFanoutSubscriptions {
            user_events: subscribed,
            ..RealtimeFanoutSubscriptions::default()
        };
        self.add_user_event_index(session_id, user_id.as_deref(), &subscriptions);
    }

    fn replace_object_subscription(&mut self, session_id: &str, subscribed: bool) {
        let Some(previous) =
            self.sessions
                .get(session_id)
                .map(|session| RealtimeFanoutSubscriptions {
                    objects: session.subscriptions.objects,
                    ..RealtimeFanoutSubscriptions::default()
                })
        else {
            return;
        };
        self.remove_object_index(session_id, &previous);
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        session.subscriptions.objects = subscribed;
        let subscriptions = RealtimeFanoutSubscriptions {
            objects: subscribed,
            ..RealtimeFanoutSubscriptions::default()
        };
        self.add_object_index(session_id, &subscriptions);
    }

    fn deliveries_for_events(
        &self,
        events: &[DeliveryEvent],
    ) -> Vec<(
        mpsc::UnboundedSender<RoutedRealtimeEventBatch>,
        RoutedRealtimeEventBatch,
    )> {
        let shared_events = events.iter().cloned().map(Arc::new).collect::<Vec<_>>();
        let mut batches = BTreeMap::<String, (SharedDeliveryEventBatch, Vec<usize>)>::new();
        for (event_index, event) in shared_events.iter().enumerate() {
            for session_id in self.candidate_session_ids(event) {
                let (batch, indices) = batches
                    .entry(session_id)
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                batch.push(Arc::clone(event));
                indices.push(event_index);
            }
        }
        let mut encoded_by_indices = BTreeMap::<Vec<usize>, Option<EncodedServerFrame>>::new();
        batches
            .into_iter()
            .filter_map(|(session_id, (batch, indices))| {
                let preencoded_events_frame = encoded_by_indices
                    .entry(indices)
                    .or_insert_with(|| preencode_shared_events(&batch))
                    .clone();
                self.sessions.get(&session_id).map(|session| {
                    (
                        session.sender.clone(),
                        RoutedRealtimeEventBatch::new(batch, preencoded_events_frame),
                    )
                })
            })
            .collect()
    }

    fn candidate_session_ids(&self, event: &DeliveryEvent) -> BTreeSet<String> {
        let mut candidates = BTreeSet::new();
        if event.is_object_event() {
            candidates.extend(self.objects.iter().cloned());
        }
        if let Some(room_id) = event.room_id()
            && let Some(session_ids) = self.rooms.get(room_id)
        {
            candidates.extend(session_ids.iter().cloned());
        }
        if let Some((table, key)) = event.table().zip(event.record_key()) {
            if let Some(session_ids) = self.full_tables.get(table) {
                candidates.extend(session_ids.iter().cloned());
            }
            if let Some(index) = self.range_tables.get(table) {
                candidates.extend(index.matching_session_ids(key));
            }
            if let Some(index) = self.index_prefix_tables.get(table) {
                candidates.extend(index.matching_session_ids(event));
            }
            if let Some(index) = self.nested_tables.get(table) {
                candidates.extend(index.matching_session_ids(key));
            }
            if let Some(session_ids) = self.query_tables.get(table) {
                candidates.extend(session_ids.iter().cloned());
            }
        }
        if let Some(user_id) = event.user_id()
            && let Some(session_ids) = self.user_events.get(user_id)
        {
            match event.target_session_ids() {
                Some(target_session_ids) => {
                    candidates.extend(session_ids.intersection(target_session_ids).cloned());
                }
                None => {
                    candidates.extend(session_ids.iter().cloned());
                }
            }
        }
        candidates
    }

    fn add_record_indexes(
        &mut self,
        session_id: &str,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        add_values(&mut self.rooms, session_id, &subscriptions.rooms);
        add_values(
            &mut self.full_tables,
            session_id,
            &subscriptions.full_tables,
        );
        for subscription in subscriptions
            .table_ranges
            .iter()
            .filter(|subscription| !subscription.has_index_prefix())
        {
            self.range_tables
                .entry(subscription.table.clone())
                .or_default()
                .insert(subscription, session_id);
        }
        for subscription in &subscriptions.index_prefix_tables {
            self.index_prefix_tables
                .entry(subscription.table.clone())
                .or_default()
                .insert(subscription, session_id);
        }
        for subscription in &subscriptions.nested_tables {
            self.nested_tables
                .entry(subscription.logical_table())
                .or_default()
                .insert(&subscription.key_prefix(), session_id);
        }
    }

    fn remove_record_indexes(
        &mut self,
        session_id: &str,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        remove_values(&mut self.rooms, session_id, &subscriptions.rooms);
        remove_values(
            &mut self.full_tables,
            session_id,
            &subscriptions.full_tables,
        );
        for subscription in subscriptions
            .table_ranges
            .iter()
            .filter(|subscription| !subscription.has_index_prefix())
        {
            let Some(index) = self.range_tables.get_mut(&subscription.table) else {
                continue;
            };
            index.remove(subscription, session_id);
            if index.is_empty() {
                self.range_tables.remove(&subscription.table);
            }
        }
        for subscription in &subscriptions.index_prefix_tables {
            let Some(index) = self.index_prefix_tables.get_mut(&subscription.table) else {
                continue;
            };
            index.remove(subscription, session_id);
            if index.is_empty() {
                self.index_prefix_tables.remove(&subscription.table);
            }
        }
        for subscription in &subscriptions.nested_tables {
            let logical_table = subscription.logical_table();
            let Some(index) = self.nested_tables.get_mut(&logical_table) else {
                continue;
            };
            index.remove(&subscription.key_prefix(), session_id);
            if index.is_empty() {
                self.nested_tables.remove(&logical_table);
            }
        }
    }

    fn add_query_indexes(&mut self, session_id: &str, subscriptions: &RealtimeFanoutSubscriptions) {
        add_values(
            &mut self.query_tables,
            session_id,
            &subscriptions.query_tables,
        );
    }

    fn remove_query_indexes(
        &mut self,
        session_id: &str,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        remove_values(
            &mut self.query_tables,
            session_id,
            &subscriptions.query_tables,
        );
    }

    fn add_user_event_index(
        &mut self,
        session_id: &str,
        user_id: Option<&str>,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        if subscriptions.user_events
            && let Some(user_id) = user_id
        {
            self.user_events
                .entry(user_id.to_string())
                .or_default()
                .insert(session_id.to_string());
        }
    }

    fn remove_user_event_index(
        &mut self,
        session_id: &str,
        user_id: Option<&str>,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        if subscriptions.user_events
            && let Some(user_id) = user_id
        {
            remove_index_value(&mut self.user_events, user_id, session_id);
        }
    }

    fn add_object_index(&mut self, session_id: &str, subscriptions: &RealtimeFanoutSubscriptions) {
        if subscriptions.objects {
            self.objects.insert(session_id.to_string());
        }
    }

    fn remove_object_index(
        &mut self,
        session_id: &str,
        subscriptions: &RealtimeFanoutSubscriptions,
    ) {
        if subscriptions.objects {
            self.objects.remove(session_id);
        }
    }
}

fn preencode_shared_events(events: &[Arc<DeliveryEvent>]) -> Option<EncodedServerFrame> {
    let event_refs = events.iter().map(Arc::as_ref).collect::<Vec<_>>();
    encode_delivery_events_frame(&event_refs).ok().flatten()
}

fn add_values(
    index: &mut BTreeMap<String, BTreeSet<String>>,
    session_id: &str,
    values: &BTreeSet<String>,
) {
    for value in values {
        index
            .entry(value.clone())
            .or_default()
            .insert(session_id.to_string());
    }
}

fn remove_values(
    index: &mut BTreeMap<String, BTreeSet<String>>,
    session_id: &str,
    values: &BTreeSet<String>,
) {
    for value in values {
        remove_index_value(index, value, session_id);
    }
}

fn remove_index_value(
    index: &mut BTreeMap<String, BTreeSet<String>>,
    value: &str,
    session_id: &str,
) {
    let Some(session_ids) = index.get_mut(value) else {
        return;
    };
    session_ids.remove(session_id);
    if session_ids.is_empty() {
        index.remove(value);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IndexPrefixFanoutSubscription {
    table: String,
    fields: Vec<String>,
    values_json: String,
}

#[derive(Default)]
struct IndexPrefixFanoutIndex {
    subscriptions: BTreeMap<IndexPrefixFanoutSubscription, BTreeSet<String>>,
}

impl IndexPrefixFanoutIndex {
    fn insert(&mut self, subscription: &IndexPrefixFanoutSubscription, session_id: &str) {
        self.subscriptions
            .entry(subscription.clone())
            .or_default()
            .insert(session_id.to_string());
    }

    fn remove(&mut self, subscription: &IndexPrefixFanoutSubscription, session_id: &str) {
        let Some(session_ids) = self.subscriptions.get_mut(subscription) else {
            return;
        };
        session_ids.remove(session_id);
        if session_ids.is_empty() {
            self.subscriptions.remove(subscription);
        }
    }

    fn matching_session_ids(&self, event: &DeliveryEvent) -> BTreeSet<String> {
        let Some((table, record)) = event_record_for_index_prefix(event) else {
            return BTreeSet::new();
        };
        let mut session_ids = BTreeSet::new();
        for (subscription, subscription_session_ids) in &self.subscriptions {
            if subscription.table == table && subscription.matches(record) {
                session_ids.extend(subscription_session_ids.iter().cloned());
            }
        }
        session_ids
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl IndexPrefixFanoutSubscription {
    fn matches(&self, record: &DbRecord) -> bool {
        let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(&self.values_json) else {
            return false;
        };
        if values.len() > self.fields.len() {
            return false;
        }
        self.fields
            .iter()
            .zip(values.iter())
            .all(|(field, expected)| value_at_field_path(&record.value, field) == Some(expected))
    }
}

fn index_prefix_subscriptions(
    table_ranges: &BTreeSet<TableSubscription>,
    schema: &DatabaseSchema,
) -> BTreeSet<IndexPrefixFanoutSubscription> {
    table_ranges
        .iter()
        .filter_map(|subscription| index_prefix_subscription(subscription, schema))
        .collect()
}

fn index_prefix_subscription(
    subscription: &TableSubscription,
    schema: &DatabaseSchema,
) -> Option<IndexPrefixFanoutSubscription> {
    let index_name = subscription.index_name.as_deref()?;
    let values_json = subscription.index_values.as_deref()?;
    let index = table_subscription_index_schema(schema, &subscription.table, index_name)?;
    parse_index_prefix_values(values_json, index).ok()?;
    Some(IndexPrefixFanoutSubscription {
        table: subscription.table.clone(),
        fields: index.fields.clone(),
        values_json: values_json.to_string(),
    })
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

fn event_record_for_index_prefix(event: &DeliveryEvent) -> Option<(&str, &DbRecord)> {
    match event {
        DeliveryEvent::RecordUpserted { table, record, .. } => Some((table, record)),
        DeliveryEvent::RecordDeleted {
            table,
            previous_record: Some(previous_record),
            ..
        } => Some((table, previous_record)),
        _ => None,
    }
}

fn value_at_field_path<'a>(
    mut value: &'a serde_json::Value,
    field_path: &str,
) -> Option<&'a serde_json::Value> {
    for component in field_path.split('.') {
        value = value.as_object()?.get(component)?;
    }
    Some(value)
}

#[derive(Default)]
struct RangeFanoutIndex {
    fallback: RangeFanoutBucketIndex,
    buckets: BTreeMap<u8, RangeFanoutBucketIndex>,
}

impl RangeFanoutIndex {
    fn insert(&mut self, subscription: &TableSubscription, session_id: &str) {
        match range_fanout_buckets(subscription) {
            FanoutBucketSelection::Empty => {}
            FanoutBucketSelection::Fallback => self.fallback.insert(subscription, session_id),
            FanoutBucketSelection::Buckets(buckets) => {
                for bucket in buckets {
                    self.buckets
                        .entry(bucket)
                        .or_default()
                        .insert(subscription, session_id);
                }
            }
        }
    }

    fn remove(&mut self, subscription: &TableSubscription, session_id: &str) {
        match range_fanout_buckets(subscription) {
            FanoutBucketSelection::Empty => {}
            FanoutBucketSelection::Fallback => self.fallback.remove(subscription, session_id),
            FanoutBucketSelection::Buckets(buckets) => {
                for bucket in buckets {
                    let Some(index) = self.buckets.get_mut(&bucket) else {
                        continue;
                    };
                    index.remove(subscription, session_id);
                    if index.is_empty() {
                        self.buckets.remove(&bucket);
                    }
                }
            }
        }
    }

    fn matching_session_ids(&self, key: &str) -> BTreeSet<String> {
        let mut session_ids = self.fallback.matching_session_ids(key);
        if let Some(index) = self.buckets.get(&range_fanout_bucket(key)) {
            session_ids.extend(index.matching_session_ids(key));
        }
        session_ids
    }

    fn is_empty(&self) -> bool {
        self.fallback.is_empty() && self.buckets.is_empty()
    }
}

#[derive(Default)]
struct RangeFanoutBucketIndex {
    unbounded_lower_uppers: BTreeMap<Option<String>, BTreeSet<String>>,
    uppers_by_lower: BTreeMap<String, BTreeMap<Option<String>, BTreeSet<String>>>,
    unbounded_lower_best_upper: Option<Option<String>>,
    prefix_best_upper_by_lower: BTreeMap<String, Option<String>>,
}

impl RangeFanoutBucketIndex {
    fn insert(&mut self, subscription: &TableSubscription, session_id: &str) {
        match subscription.lower_key.as_deref() {
            Some(lower) => {
                self.uppers_by_lower
                    .entry(lower.to_string())
                    .or_default()
                    .entry(subscription.upper_key.clone())
                    .or_default()
                    .insert(session_id.to_string());
            }
            None => {
                self.unbounded_lower_uppers
                    .entry(subscription.upper_key.clone())
                    .or_default()
                    .insert(session_id.to_string());
            }
        }
        self.rebuild_match_cache();
    }

    fn remove(&mut self, subscription: &TableSubscription, session_id: &str) {
        match subscription.lower_key.as_deref() {
            Some(lower) => {
                let Some(uppers) = self.uppers_by_lower.get_mut(lower) else {
                    return;
                };
                remove_upper_session(uppers, &subscription.upper_key, session_id);
                if uppers.is_empty() {
                    self.uppers_by_lower.remove(lower);
                }
            }
            None => {
                remove_upper_session(
                    &mut self.unbounded_lower_uppers,
                    &subscription.upper_key,
                    session_id,
                );
            }
        }
        self.rebuild_match_cache();
    }

    fn matching_session_ids(&self, key: &str) -> BTreeSet<String> {
        let mut session_ids = BTreeSet::new();
        if range_upper_matches_key(self.unbounded_lower_best_upper.as_ref(), key) {
            collect_matching_upper_sessions(&self.unbounded_lower_uppers, key, &mut session_ids);
        }
        let key_owned = key.to_string();
        let mut next_lower = self.uppers_by_lower.range(..=key_owned).next_back();
        while let Some((lower, uppers)) = next_lower {
            collect_matching_upper_sessions(uppers, key, &mut session_ids);
            let previous_lower = self
                .uppers_by_lower
                .range(..lower.clone())
                .next_back()
                .map(|(previous_lower, _)| previous_lower.clone());
            let Some(previous_lower) = previous_lower else {
                break;
            };
            if !range_upper_matches_key(self.prefix_best_upper_by_lower.get(&previous_lower), key) {
                break;
            }
            next_lower = self.uppers_by_lower.get_key_value(&previous_lower);
        }
        session_ids
    }

    fn is_empty(&self) -> bool {
        self.unbounded_lower_uppers.is_empty() && self.uppers_by_lower.is_empty()
    }

    fn rebuild_match_cache(&mut self) {
        self.unbounded_lower_best_upper = best_upper_for_session_map(&self.unbounded_lower_uppers);
        self.prefix_best_upper_by_lower.clear();
        let mut prefix_best_upper = None;
        for (lower, uppers) in &self.uppers_by_lower {
            prefix_best_upper =
                wider_range_upper(prefix_best_upper, best_upper_for_session_map(uppers));
            if let Some(best_upper) = prefix_best_upper.clone() {
                self.prefix_best_upper_by_lower
                    .insert(lower.clone(), best_upper);
            }
        }
    }
}

#[derive(Default)]
struct PrefixFanoutIndex {
    buckets: BTreeMap<u8, PrefixFanoutBucketIndex>,
}

impl PrefixFanoutIndex {
    fn insert(&mut self, prefix: &str, session_id: &str) {
        self.buckets
            .entry(prefix_fanout_bucket(prefix))
            .or_default()
            .insert(prefix, session_id);
    }

    fn remove(&mut self, prefix: &str, session_id: &str) {
        let bucket = prefix_fanout_bucket(prefix);
        let Some(index) = self.buckets.get_mut(&bucket) else {
            return;
        };
        index.remove(prefix, session_id);
        if index.is_empty() {
            self.buckets.remove(&bucket);
        }
    }

    fn matching_session_ids(&self, key: &str) -> BTreeSet<String> {
        let mut session_ids = BTreeSet::new();
        self.collect_prefix_matches("", &mut session_ids);
        for (index, _) in key.char_indices().skip(1) {
            self.collect_prefix_matches(&key[..index], &mut session_ids);
        }
        self.collect_prefix_matches(key, &mut session_ids);
        session_ids
    }

    fn collect_prefix_matches(&self, prefix: &str, session_ids: &mut BTreeSet<String>) {
        if let Some(index) = self.buckets.get(&prefix_fanout_bucket(prefix)) {
            index.collect_prefix_matches(prefix, session_ids);
        }
    }

    fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[derive(Default)]
struct PrefixFanoutBucketIndex {
    session_ids_by_prefix: BTreeMap<String, BTreeSet<String>>,
}

impl PrefixFanoutBucketIndex {
    fn insert(&mut self, prefix: &str, session_id: &str) {
        self.session_ids_by_prefix
            .entry(prefix.to_string())
            .or_default()
            .insert(session_id.to_string());
    }

    fn remove(&mut self, prefix: &str, session_id: &str) {
        remove_index_value(&mut self.session_ids_by_prefix, prefix, session_id);
    }

    fn collect_prefix_matches(&self, prefix: &str, session_ids: &mut BTreeSet<String>) {
        if let Some(prefix_session_ids) = self.session_ids_by_prefix.get(prefix) {
            session_ids.extend(prefix_session_ids.iter().cloned());
        }
    }

    fn is_empty(&self) -> bool {
        self.session_ids_by_prefix.is_empty()
    }
}

enum FanoutBucketSelection {
    Empty,
    Fallback,
    Buckets(Vec<u8>),
}

fn range_fanout_buckets(subscription: &TableSubscription) -> FanoutBucketSelection {
    if let (Some(lower), Some(upper)) = (
        subscription.lower_key.as_deref(),
        subscription.upper_key.as_deref(),
    ) && lower >= upper
    {
        return FanoutBucketSelection::Empty;
    }

    let lower_bucket = subscription
        .lower_key
        .as_deref()
        .map(range_fanout_bucket)
        .unwrap_or(0);
    let upper_bucket = subscription
        .upper_key
        .as_deref()
        .map(range_fanout_bucket)
        .unwrap_or(u8::MAX);
    if lower_bucket > upper_bucket {
        return FanoutBucketSelection::Fallback;
    }
    if lower_bucket == 0 && upper_bucket == u8::MAX {
        return FanoutBucketSelection::Fallback;
    }
    FanoutBucketSelection::Buckets((lower_bucket..=upper_bucket).collect())
}

fn range_fanout_bucket(key: &str) -> u8 {
    key.as_bytes().first().copied().unwrap_or(0)
}

fn prefix_fanout_bucket(prefix: &str) -> u8 {
    shard_index(prefix, FANOUT_BUCKET_COUNT) as u8
}

fn remove_upper_session(
    uppers: &mut BTreeMap<Option<String>, BTreeSet<String>>,
    upper: &Option<String>,
    session_id: &str,
) {
    let Some(session_ids) = uppers.get_mut(upper) else {
        return;
    };
    session_ids.remove(session_id);
    if session_ids.is_empty() {
        uppers.remove(upper);
    }
}

fn collect_matching_upper_sessions(
    uppers: &BTreeMap<Option<String>, BTreeSet<String>>,
    key: &str,
    session_ids: &mut BTreeSet<String>,
) {
    for (upper, upper_session_ids) in uppers {
        if upper.as_deref().is_none_or(|upper| key < upper) {
            session_ids.extend(upper_session_ids.iter().cloned());
        }
    }
}

fn best_upper_for_session_map(
    uppers: &BTreeMap<Option<String>, BTreeSet<String>>,
) -> Option<Option<String>> {
    if uppers.is_empty() {
        None
    } else if uppers.contains_key(&None) {
        Some(None)
    } else {
        uppers.keys().next_back().cloned()
    }
}

fn wider_range_upper(
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

fn range_upper_matches_key(best_upper: Option<&Option<String>>, key: &str) -> bool {
    best_upper.is_some_and(|upper| upper.as_deref().is_none_or(|upper| key < upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{DbRecord, DeliveryEvent},
        schema::DatabaseSchema,
    };

    #[test]
    fn fanout_routes_by_room_table_range_and_nested_prefix() {
        let fanout = RealtimeFanoutRegistry::default();
        let (room_tx, mut room_rx) = mpsc::unbounded_channel();
        let (range_tx, mut range_rx) = mpsc::unbounded_channel();
        let (nested_tx, mut nested_rx) = mpsc::unbounded_channel();
        fanout.register("room-session".to_string(), None, room_tx);
        fanout.register("range-session".to_string(), None, range_tx);
        fanout.register("nested-session".to_string(), None, nested_tx);
        fanout.update_record_subscriptions(
            "room-session",
            &BTreeSet::from(["room-a".to_string()]),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        fanout.update_record_subscriptions(
            "range-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([TableSubscription::new(
                "messages".to_string(),
                Some("msg-010".to_string()),
                Some("msg-020".to_string()),
            )]),
            &BTreeSet::new(),
        );
        fanout.update_record_subscriptions(
            "nested-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([NestedTableSubscription::new(
                "rooms".to_string(),
                "room-a".to_string(),
                "messages".to_string(),
            )]),
        );

        assert!(fanout.publish(vec![
            room_event("room-a"),
            record_event("messages", "msg-015"),
            record_event("rooms.messages", "room-a:msg-1"),
            record_event("messages", "msg-020"),
        ]));

        assert_eq!(room_rx.try_recv().unwrap().len(), 1);
        assert_eq!(range_rx.try_recv().unwrap().len(), 1);
        assert_eq!(nested_rx.try_recv().unwrap().len(), 1);
    }

    #[test]
    fn fanout_routes_query_and_user_event_subscriptions() {
        let fanout = RealtimeFanoutRegistry::default();
        let (query_tx, mut query_rx) = mpsc::unbounded_channel();
        let (user_tx, mut user_rx) = mpsc::unbounded_channel();
        let (other_tx, mut other_rx) = mpsc::unbounded_channel();
        fanout.register("query-session".to_string(), None, query_tx);
        fanout.register(
            "user-session".to_string(),
            Some("alice".to_string()),
            user_tx,
        );
        fanout.register(
            "other-session".to_string(),
            Some("bob".to_string()),
            other_tx,
        );
        fanout.update_query_subscriptions("query-session", &BTreeSet::from(["rooms".to_string()]));
        fanout.update_user_event_subscription("user-session", true);
        fanout.update_user_event_subscription("other-session", true);

        assert!(fanout.publish(vec![
            record_event("rooms", "room-a"),
            DeliveryEvent::UserEvent {
                user_id: "alice".to_string(),
                event: crate::model::UserEvent {
                    id: "event-a".to_string(),
                    user_id: "alice".to_string(),
                    name: "notice".to_string(),
                    payload: serde_json::json!({}),
                    created_at_ms: 1,
                    path: "users/alice/events/event-a".to_string(),
                    lsn: 1,
                },
            },
        ]));

        assert_eq!(query_rx.try_recv().unwrap().len(), 1);
        assert_eq!(user_rx.try_recv().unwrap().len(), 1);
        assert!(other_rx.try_recv().is_err());
    }

    #[test]
    fn fanout_shares_event_instances_across_matching_sessions() {
        let fanout = RealtimeFanoutRegistry::default();
        let (left_tx, mut left_rx) = mpsc::unbounded_channel();
        let (right_tx, mut right_rx) = mpsc::unbounded_channel();
        fanout.register("left-session".to_string(), None, left_tx);
        fanout.register("right-session".to_string(), None, right_tx);
        for session_id in ["left-session", "right-session"] {
            fanout.update_record_subscriptions(
                session_id,
                &BTreeSet::new(),
                &BTreeSet::from(["rooms".to_string()]),
                &BTreeSet::new(),
                &BTreeSet::new(),
            );
        }

        assert!(fanout.publish(vec![record_event("rooms", "room-a")]));

        let left = left_rx.try_recv().unwrap();
        let right = right_rx.try_recv().unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        assert!(Arc::ptr_eq(&left[0], &right[0]));
    }

    #[test]
    fn fanout_shares_preencoded_event_frames_across_identical_batches() {
        let fanout = RealtimeFanoutRegistry::default();
        let (left_tx, mut left_rx) = mpsc::unbounded_channel();
        let (right_tx, mut right_rx) = mpsc::unbounded_channel();
        fanout.register("left-session".to_string(), None, left_tx);
        fanout.register("right-session".to_string(), None, right_tx);
        for session_id in ["left-session", "right-session"] {
            fanout.update_record_subscriptions(
                session_id,
                &BTreeSet::new(),
                &BTreeSet::from(["rooms".to_string()]),
                &BTreeSet::new(),
                &BTreeSet::new(),
            );
        }

        assert!(fanout.publish(vec![record_event("rooms", "room-a")]));

        let left_frame = left_rx
            .try_recv()
            .unwrap()
            .preencoded_events_frame()
            .expect("left preencoded frame");
        let right_frame = right_rx
            .try_recv()
            .unwrap()
            .preencoded_events_frame()
            .expect("right preencoded frame");
        assert_eq!(left_frame.json(), right_frame.json());
        assert_eq!(left_frame.json().as_ptr(), right_frame.json().as_ptr());
    }

    #[test]
    fn fanout_range_index_excludes_same_table_non_matches() {
        let fanout = RealtimeFanoutRegistry::default();
        let (matching_tx, mut matching_rx) = mpsc::unbounded_channel();
        let (early_tx, mut early_rx) = mpsc::unbounded_channel();
        let (tail_tx, mut tail_rx) = mpsc::unbounded_channel();
        fanout.register("matching-session".to_string(), None, matching_tx);
        fanout.register("early-session".to_string(), None, early_tx);
        fanout.register("tail-session".to_string(), None, tail_tx);
        fanout.update_record_subscriptions(
            "matching-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([TableSubscription::new(
                "messages".to_string(),
                Some("msg-010".to_string()),
                Some("msg-020".to_string()),
            )]),
            &BTreeSet::new(),
        );
        fanout.update_record_subscriptions(
            "early-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([TableSubscription::new(
                "messages".to_string(),
                None,
                Some("msg-005".to_string()),
            )]),
            &BTreeSet::new(),
        );
        fanout.update_record_subscriptions(
            "tail-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([TableSubscription::new(
                "messages".to_string(),
                Some("msg-090".to_string()),
                None,
            )]),
            &BTreeSet::new(),
        );

        assert!(fanout.publish(vec![record_event("messages", "msg-015")]));

        assert_eq!(matching_rx.try_recv().unwrap().len(), 1);
        assert!(early_rx.try_recv().is_err());
        assert!(tail_rx.try_recv().is_err());
    }

    #[test]
    fn fanout_index_prefix_excludes_same_table_non_matches() {
        let fanout = RealtimeFanoutRegistry::default();
        let schema = DatabaseSchema::default_nextdb();
        let (matching_tx, mut matching_rx) = mpsc::unbounded_channel();
        fanout.register("matching-session".to_string(), None, matching_tx);
        fanout.update_record_subscriptions_with_schema(
            "matching-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([TableSubscription::new("rooms".to_string(), None, None)
                .with_index_prefix(
                    Some("byTitle".to_string()),
                    Some(r#"["target"]"#.to_string()),
                )]),
            &BTreeSet::new(),
            &schema,
        );

        assert!(fanout.publish(vec![record_event_with_value(
            "rooms",
            "room-other",
            serde_json::json!({ "id": "room-other", "title": "other" }),
        )]));
        assert!(matching_rx.try_recv().is_err());

        assert!(fanout.publish(vec![record_event_with_value(
            "rooms",
            "room-target",
            serde_json::json!({ "id": "room-target", "title": "target" }),
        )]));
        assert_eq!(matching_rx.try_recv().unwrap().len(), 1);
    }

    #[test]
    fn fanout_range_index_rebuilds_after_wide_range_removal() {
        let mut index = RangeFanoutIndex::default();
        let wide_range = TableSubscription::new(
            "messages".to_string(),
            Some("msg-010".to_string()),
            Some("msg-100".to_string()),
        );
        let narrow_range = TableSubscription::new(
            "messages".to_string(),
            Some("msg-020".to_string()),
            Some("msg-030".to_string()),
        );

        index.insert(&wide_range, "wide-session");
        index.insert(&narrow_range, "narrow-session");
        assert_eq!(
            index.matching_session_ids("msg-090"),
            BTreeSet::from(["wide-session".to_string()])
        );

        index.remove(&wide_range, "wide-session");
        assert!(index.matching_session_ids("msg-090").is_empty());
        assert_eq!(
            index.matching_session_ids("msg-025"),
            BTreeSet::from(["narrow-session".to_string()])
        );
    }

    #[test]
    fn fanout_range_index_buckets_bounded_ranges_and_cleans_empty_buckets() {
        let mut index = RangeFanoutIndex::default();
        let narrow_range = TableSubscription::new(
            "messages".to_string(),
            Some("msg-020".to_string()),
            Some("msg-030".to_string()),
        );
        let fallback_range = TableSubscription::new("messages".to_string(), None, None);

        index.insert(&narrow_range, "narrow-session");
        assert!(index.fallback.is_empty());
        assert_eq!(index.buckets.len(), 1);
        assert_eq!(
            index.matching_session_ids("msg-025"),
            BTreeSet::from(["narrow-session".to_string()])
        );
        assert!(index.matching_session_ids("room-025").is_empty());

        index.insert(&fallback_range, "fallback-session");
        assert_eq!(
            index.matching_session_ids("room-025"),
            BTreeSet::from(["fallback-session".to_string()])
        );

        index.remove(&narrow_range, "narrow-session");
        assert!(index.buckets.is_empty());
        assert_eq!(
            index.matching_session_ids("msg-025"),
            BTreeSet::from(["fallback-session".to_string()])
        );

        index.remove(&fallback_range, "fallback-session");
        assert!(index.is_empty());
    }

    #[test]
    fn fanout_prefix_index_excludes_same_table_non_matches() {
        let fanout = RealtimeFanoutRegistry::default();
        let (matching_tx, mut matching_rx) = mpsc::unbounded_channel();
        let (other_tx, mut other_rx) = mpsc::unbounded_channel();
        fanout.register("matching-session".to_string(), None, matching_tx);
        fanout.register("other-session".to_string(), None, other_tx);
        fanout.update_record_subscriptions(
            "matching-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([NestedTableSubscription::new(
                "rooms".to_string(),
                "room-a".to_string(),
                "messages".to_string(),
            )]),
        );
        fanout.update_record_subscriptions(
            "other-session",
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::from([NestedTableSubscription::new(
                "rooms".to_string(),
                "room-b".to_string(),
                "messages".to_string(),
            )]),
        );

        assert!(fanout.publish(vec![record_event("rooms.messages", "room-a:msg-1")]));

        assert_eq!(matching_rx.try_recv().unwrap().len(), 1);
        assert!(other_rx.try_recv().is_err());
    }

    #[test]
    fn prefix_index_matches_only_actual_key_prefixes() {
        let mut index = PrefixFanoutIndex::default();
        for suffix in 0..128 {
            index.insert(&format!("room-{suffix:03}:"), "non-prefix-session");
        }
        index.insert("room-target:", "target-session");
        index.insert("room-target:thread:", "thread-session");
        index.insert("房间:", "unicode-session");

        assert_eq!(
            index.matching_session_ids("room-target:thread:msg-1"),
            BTreeSet::from(["target-session".to_string(), "thread-session".to_string()])
        );
        assert_eq!(
            index.matching_session_ids("房间:消息-1"),
            BTreeSet::from(["unicode-session".to_string()])
        );
        assert!(index.matching_session_ids("room-999:msg-1").is_empty());
    }

    #[test]
    fn prefix_index_buckets_prefixes_and_removes_empty_buckets() {
        let mut index = PrefixFanoutIndex::default();
        index.insert("room-a:", "room-session");
        index.insert("room-a:thread:", "thread-session");

        assert_eq!(
            index.matching_session_ids("room-a:thread:msg-1"),
            BTreeSet::from(["room-session".to_string(), "thread-session".to_string()])
        );
        assert!(index.matching_session_ids("room-b:msg-1").is_empty());

        index.remove("room-a:thread:", "thread-session");
        assert_eq!(
            index.matching_session_ids("room-a:thread:msg-1"),
            BTreeSet::from(["room-session".to_string()])
        );
        index.remove("room-a:", "room-session");
        assert!(index.is_empty());
    }

    fn room_event(room_id: &str) -> DeliveryEvent {
        DeliveryEvent::VolatileRoomEvent {
            room_id: room_id.to_string(),
            name: "tick".to_string(),
            payload: serde_json::json!({}),
        }
    }

    fn record_event(table: &str, key: &str) -> DeliveryEvent {
        record_event_with_value(table, key, serde_json::json!({}))
    }

    fn record_event_with_value(table: &str, key: &str, value: serde_json::Value) -> DeliveryEvent {
        DeliveryEvent::RecordUpserted {
            table: table.to_string(),
            key: key.to_string(),
            record: DbRecord {
                table: table.to_string(),
                key: key.to_string(),
                value,
                updated_at_ms: 1,
                path: format!("tables/{table}/{key}"),
                lsn: 1,
            },
        }
    }
}
