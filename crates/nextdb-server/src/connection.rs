use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::util::now_ms;

#[derive(Clone, Default)]
pub struct ConnectionRegistry {
    sessions: Arc<RwLock<BTreeMap<String, ConnectionSession>>>,
}

impl ConnectionRegistry {
    pub async fn register(
        &self,
        session_id: String,
        user_id: Option<String>,
        transport: ConnectionTransport,
        metadata: serde_json::Value,
    ) -> ConnectionSession {
        let now = now_ms();
        let session = ConnectionSession {
            session_id: session_id.clone(),
            user_id,
            transport,
            metadata,
            connected_at_ms: now,
            last_seen_at_ms: now,
            subscribed_rooms: Vec::new(),
            subscribed_tables: Vec::new(),
            subscribed_nested_tables: Vec::new(),
            subscribed_queries: Vec::new(),
            subscribed_query_tables: BTreeMap::new(),
            subscribed_user_events: false,
            subscribed_objects: false,
        };
        self.sessions
            .write()
            .await
            .insert(session_id, session.clone());
        session
    }

    pub async fn unregister(&self, session_id: &str) -> Option<ConnectionSession> {
        self.sessions.write().await.remove(session_id)
    }

    pub async fn update_metadata(
        &self,
        session_id: &str,
        metadata: serde_json::Value,
    ) -> Option<ConnectionSession> {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
            session.metadata = metadata;
            return Some(session.clone());
        }
        None
    }

    pub async fn touch(&self, session_id: &str) {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
        }
    }

    pub async fn update_subscriptions(
        &self,
        session_id: &str,
        rooms: &BTreeSet<String>,
        tables: &BTreeSet<String>,
        nested_tables: &BTreeSet<String>,
    ) -> Option<ConnectionSession> {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
            session.subscribed_rooms = rooms.iter().cloned().collect();
            session.subscribed_tables = tables.iter().cloned().collect();
            session.subscribed_nested_tables = nested_tables.iter().cloned().collect();
            return Some(session.clone());
        }
        None
    }

    pub async fn update_query_subscriptions(
        &self,
        session_id: &str,
        queries: &BTreeSet<String>,
        query_tables: &BTreeMap<String, usize>,
    ) -> Option<ConnectionSession> {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
            session.subscribed_queries = queries.iter().cloned().collect();
            session.subscribed_query_tables = query_tables.clone();
            return Some(session.clone());
        }
        None
    }

    pub async fn update_user_event_subscription(
        &self,
        session_id: &str,
        subscribed: bool,
    ) -> Option<ConnectionSession> {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
            session.subscribed_user_events = subscribed;
            return Some(session.clone());
        }
        None
    }

    pub async fn update_object_subscription(
        &self,
        session_id: &str,
        subscribed: bool,
    ) -> Option<ConnectionSession> {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_seen_at_ms = now_ms();
            session.subscribed_objects = subscribed;
            return Some(session.clone());
        }
        None
    }

    pub async fn list(
        &self,
        user_id: Option<&str>,
        transport: Option<ConnectionTransport>,
    ) -> Vec<ConnectionSession> {
        self.sessions
            .read()
            .await
            .values()
            .filter(|session| {
                user_id.is_none_or(|user_id| session.user_id.as_deref() == Some(user_id))
                    && transport.is_none_or(|transport| session.transport == transport)
            })
            .cloned()
            .collect()
    }

    pub async fn count(&self) -> usize {
        self.sessions.read().await.len()
    }

    pub async fn user_count(&self) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter_map(|session| session.user_id.as_deref())
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub async fn active_user_sessions(&self) -> BTreeSet<(String, String)> {
        self.sessions
            .read()
            .await
            .values()
            .filter_map(|session| {
                session
                    .user_id
                    .as_ref()
                    .map(|user_id| (user_id.clone(), session.session_id.clone()))
            })
            .collect()
    }

    pub async fn count_user(&self, user_id: &str) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter(|session| session.user_id.as_deref() == Some(user_id))
            .count()
    }

    pub async fn count_user_query_subscriptions_excluding(
        &self,
        user_id: &str,
        excluded_session_id: &str,
    ) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter(|session| {
                session.user_id.as_deref() == Some(user_id)
                    && session.session_id != excluded_session_id
            })
            .map(|session| session.subscribed_queries.len())
            .sum()
    }

    pub async fn total_query_subscriptions(&self) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .map(|session| session.subscribed_queries.len())
            .sum()
    }

    pub async fn count_user_sessions_in(
        &self,
        user_id: &str,
        session_ids: &BTreeSet<String>,
    ) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter(|session| {
                session.user_id.as_deref() == Some(user_id)
                    && session_ids.contains(&session.session_id)
            })
            .count()
    }

    pub async fn has_user_session(&self, user_id: &str, session_id: &str) -> bool {
        self.sessions
            .read()
            .await
            .get(session_id)
            .is_some_and(|session| session.user_id.as_deref() == Some(user_id))
    }

    pub async fn count_room_subscribers(&self, room_id: &str) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter(|session| session.subscribed_rooms.iter().any(|room| room == room_id))
            .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionTransport {
    WebSocket,
    WebTransport,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSession {
    pub session_id: String,
    pub user_id: Option<String>,
    pub transport: ConnectionTransport,
    pub metadata: serde_json::Value,
    pub connected_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub subscribed_rooms: Vec<String>,
    pub subscribed_tables: Vec<String>,
    pub subscribed_nested_tables: Vec<String>,
    pub subscribed_queries: Vec<String>,
    pub subscribed_query_tables: BTreeMap<String, usize>,
    pub subscribed_user_events: bool,
    pub subscribed_objects: bool,
}
