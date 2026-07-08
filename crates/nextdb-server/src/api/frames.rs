use serde::{Deserialize, Serialize};

use crate::{
    aggregate::{
        AggregateCountSnapshot, AggregateCountUpdate, AggregatePresenceSnapshot,
        AggregatePresenceUpdate, AggregateSumSnapshot, AggregateSumUpdate,
    },
    api::{
        connections::ConnectionEvent,
        records::{
            ListRecordsResponse, RecordPredicate, RecordQueryDiff,
            deserialize_optional_record_predicate, nested_record_prefix, nested_record_table,
        },
    },
    cache_control::ClientCacheInvalidationEntry,
    connection::ConnectionSession,
    model::DeliveryEvent,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum ClientFrame {
    SubscribeRoom {
        room_id: String,
        after_lsn: Option<u64>,
        catch_up_limit: Option<usize>,
    },
    UnsubscribeRoom {
        room_id: String,
    },
    SubscribeTable {
        table: String,
        lower_key: Option<String>,
        upper_key: Option<String>,
        index_name: Option<String>,
        index_values: Option<String>,
        snapshot_limit: Option<usize>,
        after_lsn: Option<u64>,
        catch_up_limit: Option<usize>,
    },
    UnsubscribeTable {
        table: String,
        lower_key: Option<String>,
        upper_key: Option<String>,
        index_name: Option<String>,
        index_values: Option<String>,
    },
    SubscribeNestedTable {
        table: String,
        parent_key: String,
        nested: String,
        snapshot_limit: Option<usize>,
        after_lsn: Option<u64>,
        catch_up_limit: Option<usize>,
    },
    UnsubscribeNestedTable {
        table: String,
        parent_key: String,
        nested: String,
    },
    SubscribeQuery {
        query_id: String,
        table: String,
        parent_key: Option<String>,
        nested: Option<String>,
        index_name: Option<String>,
        value: Option<String>,
        values: Option<String>,
        lower: Option<String>,
        upper: Option<String>,
        lower_values: Option<String>,
        upper_values: Option<String>,
        after_key: Option<String>,
        after_cursor: Option<String>,
        limit: Option<usize>,
        order: Option<String>,
        #[serde(default, deserialize_with = "deserialize_optional_record_predicate")]
        predicate: Option<RecordPredicate>,
        result_id: Option<String>,
        #[serde(default)]
        diff: bool,
    },
    UnsubscribeQuery {
        query_id: String,
    },
    SubscribeUserEvents {
        after_lsn: Option<u64>,
        catch_up_limit: Option<usize>,
    },
    UnsubscribeUserEvents,
    SubscribeObjects {
        after_lsn: Option<u64>,
        catch_up_limit: Option<usize>,
    },
    UnsubscribeObjects,
    UpdateConnectionMetadata {
        #[serde(default)]
        metadata: serde_json::Value,
    },
    SubscribeConnectionEvents,
    UnsubscribeConnectionEvents,
    SubscribeAggregateCount {
        table: String,
    },
    UnsubscribeAggregateCount {
        table: String,
    },
    SubscribeAggregateSum {
        table: String,
        field: String,
    },
    UnsubscribeAggregateSum {
        table: String,
        field: String,
    },
    SubscribeAggregatePresence {
        channel_id: String,
    },
    UnsubscribeAggregatePresence {
        channel_id: String,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NestedTableSubscription {
    pub(crate) table: String,
    pub(crate) parent_key: String,
    pub(crate) nested: String,
}

impl NestedTableSubscription {
    pub(crate) fn new(table: String, parent_key: String, nested: String) -> Self {
        Self {
            table,
            parent_key,
            nested,
        }
    }

    pub(crate) fn logical_table(&self) -> String {
        nested_record_table(&self.table, &self.nested)
    }

    pub(crate) fn key_prefix(&self) -> String {
        nested_record_prefix(&self.parent_key)
    }

    pub(crate) fn label(&self) -> String {
        format!("{}/{}/{}", self.table, self.parent_key, self.nested)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableSubscription {
    pub(crate) table: String,
    pub(crate) lower_key: Option<String>,
    pub(crate) upper_key: Option<String>,
    pub(crate) index_name: Option<String>,
    pub(crate) index_values: Option<String>,
}

impl TableSubscription {
    pub(crate) fn new(table: String, lower_key: Option<String>, upper_key: Option<String>) -> Self {
        Self {
            table,
            lower_key,
            upper_key,
            index_name: None,
            index_values: None,
        }
    }

    pub(crate) fn with_index_prefix(
        mut self,
        index_name: Option<String>,
        index_values: Option<String>,
    ) -> Self {
        self.index_name = index_name;
        self.index_values = index_values;
        self
    }

    pub(crate) fn is_full_table(&self) -> bool {
        self.lower_key.is_none() && self.upper_key.is_none() && self.index_name.is_none()
    }

    pub(crate) fn has_index_prefix(&self) -> bool {
        self.index_name.is_some()
    }

    pub(crate) fn matches(&self, table: &str, key: &str) -> bool {
        self.table == table
            && !self.has_index_prefix()
            && self.lower_key.as_deref().is_none_or(|lower| key >= lower)
            && self.upper_key.as_deref().is_none_or(|upper| key < upper)
    }

    pub(crate) fn label(&self) -> String {
        if let Some(index_name) = self.index_name.as_deref() {
            return format!(
                "{}@{}={}",
                self.table,
                index_name,
                self.index_values.as_deref().unwrap_or("[]")
            );
        }
        match (self.lower_key.as_deref(), self.upper_key.as_deref()) {
            (None, None) => self.table.clone(),
            (lower, upper) => format!(
                "{}[{}..{})",
                self.table,
                lower.unwrap_or(""),
                upper.unwrap_or("")
            ),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum ServerFrame {
    Hello {
        user_id: Option<String>,
        session_id: String,
    },
    Subscribed {
        room_id: String,
    },
    Unsubscribed {
        room_id: String,
    },
    TableSubscribed {
        table: String,
    },
    TableSnapshot {
        table: String,
        lower_key: Option<String>,
        upper_key: Option<String>,
        index_name: Option<String>,
        index_values: Option<String>,
        response: ListRecordsResponse,
        current_lsn: u64,
    },
    NestedTableSnapshot {
        table: String,
        parent_key: String,
        nested: String,
        response: ListRecordsResponse,
        current_lsn: u64,
    },
    TableUnsubscribed {
        table: String,
    },
    QuerySubscribed {
        query_id: String,
    },
    QueryUnsubscribed {
        query_id: String,
    },
    QueryResult {
        query_id: String,
        response: ListRecordsResponse,
        current_lsn: u64,
        result_id: String,
    },
    QueryDiff {
        query_id: String,
        diff: RecordQueryDiff,
        current_lsn: u64,
        result_id: String,
    },
    QueryUnchanged {
        query_id: String,
        result_id: String,
        current_lsn: u64,
    },
    ObjectsSubscribed,
    UserEventsUnsubscribed,
    ObjectsUnsubscribed,
    ConnectionMetadataUpdated {
        session: ConnectionSession,
    },
    CacheInvalidated {
        invalidation: ClientCacheInvalidationEntry,
    },
    ConnectionEventsSubscribed,
    ConnectionEventsUnsubscribed,
    AggregateCountSubscribed {
        snapshot: AggregateCountSnapshot,
    },
    AggregateCountUnsubscribed {
        table: String,
    },
    AggregateCountUpdated {
        update: AggregateCountUpdate,
    },
    AggregateSumSubscribed {
        snapshot: AggregateSumSnapshot,
    },
    AggregateSumUnsubscribed {
        table: String,
        field: String,
    },
    AggregateSumUpdated {
        update: AggregateSumUpdate,
    },
    AggregatePresenceSubscribed {
        snapshot: AggregatePresenceSnapshot,
    },
    AggregatePresenceUnsubscribed {
        channel_id: String,
    },
    AggregatePresenceUpdated {
        update: AggregatePresenceUpdate,
    },
    ConnectionEvent {
        event: ConnectionEvent,
    },
    ConnectionClosing {
        reason: String,
    },
    Event {
        event: DeliveryEvent,
    },
    #[allow(dead_code)]
    Events {
        events: Vec<DeliveryEvent>,
    },
    SubscriptionCatchUp {
        rooms: Vec<String>,
        users: Vec<String>,
        tables: Vec<String>,
        nested_tables: Vec<NestedTableSubscription>,
        objects: bool,
        next_after_lsn: u64,
        current_lsn: u64,
        has_more: bool,
    },
    Error {
        message: String,
    },
}
