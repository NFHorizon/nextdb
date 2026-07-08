use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    convert::Infallible,
    sync::{Arc, atomic::Ordering},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    AppState, actor,
    aggregate::{AggregateSumKey, AggregateUpdate},
    api::{
        auth::{ensure_user_token_authorized, token_matches_request},
        error::ApiError,
        frames::{ClientFrame, NestedTableSubscription, ServerFrame, TableSubscription},
        guards::ensure_json_value_limit,
        realtime::publish_realtime_member_left,
        records::{
            ListRecordsQuery, ListRecordsResponse, QueryRecordsByIndexQuery, RecordPredicate,
            RecordPredicateOp, RecordPredicateTerm, RecordReadConsistency,
            RecordReadConsistencyQuery, execute_record_index_query, execute_record_list_query,
            list_records_from_live_or_disk, nested_record_prefix, nested_record_table,
            parse_index_prefix_values, resolve_record_read_consistency, validate_nested_table_path,
            validate_table_path,
        },
        runtime::{RuntimeRoomActivationRequest, activate_runtime_room_internal},
        sync::sync_events_from_wal_records,
        users::ConnectQuery,
    },
    connection::{ConnectionSession, ConnectionTransport},
    live_query::{
        LiveQueryEvaluationCacheToken, RealtimeConnectionState, RecordQueryDeletedHints,
        RecordQueryEvaluation, RecordQueryPlanKey, RecordQuerySnapshot, RecordQuerySubscription,
        affected_live_query_refresh_batch, cached_record_query_evaluation, is_valid_query_id,
        live_query_cache_token_with_lsn, record_event_batch_cache_lsn, record_matches_table_router,
        record_matches_table_subscription, record_query_diff, record_query_impact_filter,
    },
    model::{DbRecord, DeliveryEvent, DeliveryEventBatch},
    realtime::{
        BodyJsonLineFrameSource, ChannelJsonLineFrameSink, EncodedServerFrame, RealtimeFrameRead,
        RealtimeFrameSink, RealtimeFrameSource, encode_delivery_events_frame,
        encode_server_frame_to_encoded, send_server_frame,
    },
    realtime_fanout::RoutedRealtimeEventBatch,
    schema::{DatabaseSchema, ReadVisibilityPolicy},
    util::{normalize_limit, now_ms},
    wal::read_records_from_wal_paths_after_lsn,
};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Query, Request, State, WebSocketUpgrade, ws::WebSocket},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionListQuery {
    pub(crate) user_id: Option<String>,
    pub(crate) transport: Option<ConnectionTransport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionListResponse {
    pub(crate) sessions: Vec<ConnectionSession>,
    pub(crate) total: usize,
    pub(crate) users: usize,
    pub(crate) transports: ConnectionTransportCounts,
    pub(crate) user_summaries: Vec<ConnectionUserSummary>,
}

pub(crate) async fn list_connections(
    State(state): State<AppState>,
    Query(query): Query<ConnectionListQuery>,
) -> Json<ConnectionListResponse> {
    let user_id = query
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let sessions = state.connections.list(user_id, query.transport).await;
    let transports = connection_transport_counts(&sessions);
    Json(ConnectionListResponse {
        total: sessions.len(),
        users: sessions
            .iter()
            .filter_map(|session| session.user_id.as_deref())
            .collect::<BTreeSet<_>>()
            .len(),
        transports,
        user_summaries: connection_user_summaries(&sessions),
        sessions,
    })
}

pub(crate) fn parse_connection_metadata(
    raw: Option<&str>,
    state: &AppState,
) -> Result<serde_json::Value, ApiError> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(serde_json::json!({}));
    };
    let metadata = serde_json::from_str(raw)
        .map_err(|err| ApiError::bad_request(format!("invalid connection metadata: {err}")))?;
    ensure_json_value_limit(
        "connection metadata",
        &metadata,
        state.limits.max_user_event_bytes,
    )?;
    Ok(metadata)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionDisconnectRequest {
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionDisconnectResponse {
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) reason: String,
    pub(crate) targeted: usize,
    pub(crate) targeted_session_ids: Vec<String>,
}

pub(crate) async fn disconnect_connections(
    State(state): State<AppState>,
    Json(request): Json<ConnectionDisconnectRequest>,
) -> Result<Json<ConnectionDisconnectResponse>, ApiError> {
    request_connection_disconnect(&state, request)
        .await
        .map(Json)
}

pub(crate) async fn request_connection_disconnect(
    state: &AppState,
    request: ConnectionDisconnectRequest,
) -> Result<ConnectionDisconnectResponse, ApiError> {
    let user_id = request
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let session_id = request
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if user_id.is_none() && session_id.is_none() {
        return Err(ApiError::bad_request(
            "userId or sessionId is required to disconnect connections",
        ));
    }
    let reason = request
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("admin disconnect")
        .to_string();

    let mut targeted_session_ids = state
        .connections
        .list(user_id.as_deref(), None)
        .await
        .into_iter()
        .filter(|session| {
            session_id
                .as_deref()
                .is_none_or(|target_session_id| session.session_id == target_session_id)
        })
        .map(|session| session.session_id)
        .collect::<Vec<_>>();
    targeted_session_ids.sort();

    let message = ConnectionControlMessage {
        user_id: user_id.clone(),
        session_id: session_id.clone(),
        reason: reason.clone(),
    };
    let _ = state.connection_controls.send(message);
    publish_connection_event(
        state,
        ConnectionEvent {
            event_type: ConnectionEventType::DisconnectRequested,
            timestamp_ms: now_ms(),
            session: None,
            user_id: user_id.clone(),
            session_id: session_id.clone(),
            reason: Some(reason.clone()),
            targeted_session_ids: targeted_session_ids.clone(),
        },
    );

    Ok(ConnectionDisconnectResponse {
        user_id,
        session_id,
        reason,
        targeted: targeted_session_ids.len(),
        targeted_session_ids,
    })
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionEvent {
    pub(crate) event_type: ConnectionEventType,
    pub(crate) timestamp_ms: u64,
    pub(crate) session: Option<ConnectionSession>,
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) targeted_session_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ConnectionEventType {
    Connected,
    Disconnected,
    SubscriptionsUpdated,
    MetadataUpdated,
    DisconnectRequested,
}

pub(crate) fn connection_session_event(
    event_type: ConnectionEventType,
    session: ConnectionSession,
) -> ConnectionEvent {
    ConnectionEvent {
        event_type,
        timestamp_ms: now_ms(),
        user_id: session.user_id.clone(),
        session_id: Some(session.session_id.clone()),
        session: Some(session),
        reason: None,
        targeted_session_ids: Vec::new(),
    }
}

pub(crate) fn publish_connection_event(state: &AppState, event: ConnectionEvent) {
    let _ = state.connection_events.send(event);
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionControlMessage {
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) reason: String,
}

impl ConnectionControlMessage {
    pub(crate) fn matches(&self, user_id: Option<&str>, session_id: &str) -> bool {
        self.session_id
            .as_deref()
            .is_none_or(|target_session_id| target_session_id == session_id)
            && self
                .user_id
                .as_deref()
                .is_none_or(|target_user_id| user_id == Some(target_user_id))
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionTransportCounts {
    pub(crate) web_socket: usize,
    pub(crate) web_transport: usize,
    pub(crate) custom: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionUserSummary {
    pub(crate) user_id: String,
    pub(crate) session_count: usize,
    pub(crate) session_ids: Vec<String>,
    pub(crate) transports: ConnectionTransportCounts,
    pub(crate) subscribed_rooms: Vec<String>,
    pub(crate) subscribed_tables: Vec<String>,
    pub(crate) subscribed_nested_tables: Vec<String>,
    pub(crate) subscribed_queries: Vec<String>,
    pub(crate) subscribed_query_tables: BTreeMap<String, usize>,
    pub(crate) user_event_sessions: usize,
    pub(crate) object_sessions: usize,
    pub(crate) last_seen_at_ms: u64,
}

fn connection_transport_counts(sessions: &[ConnectionSession]) -> ConnectionTransportCounts {
    let mut counts = ConnectionTransportCounts::default();
    for session in sessions {
        match session.transport {
            ConnectionTransport::WebSocket => counts.web_socket += 1,
            ConnectionTransport::WebTransport => counts.web_transport += 1,
            ConnectionTransport::Custom => counts.custom += 1,
        }
    }
    counts
}

fn connection_user_summaries(sessions: &[ConnectionSession]) -> Vec<ConnectionUserSummary> {
    #[derive(Default)]
    struct Accumulator {
        session_ids: Vec<String>,
        sessions: Vec<ConnectionSession>,
        rooms: BTreeSet<String>,
        tables: BTreeSet<String>,
        nested_tables: BTreeSet<String>,
        queries: BTreeSet<String>,
        query_tables: BTreeMap<String, usize>,
        user_event_sessions: usize,
        object_sessions: usize,
        last_seen_at_ms: u64,
    }

    let mut by_user = BTreeMap::<String, Accumulator>::new();
    for session in sessions {
        let Some(user_id) = session.user_id.clone() else {
            continue;
        };
        let entry = by_user.entry(user_id).or_default();
        entry.session_ids.push(session.session_id.clone());
        entry.sessions.push(session.clone());
        entry.rooms.extend(session.subscribed_rooms.iter().cloned());
        entry
            .tables
            .extend(session.subscribed_tables.iter().cloned());
        entry
            .nested_tables
            .extend(session.subscribed_nested_tables.iter().cloned());
        entry
            .queries
            .extend(session.subscribed_queries.iter().cloned());
        for (table, count) in &session.subscribed_query_tables {
            *entry.query_tables.entry(table.clone()).or_default() += count;
        }
        if session.subscribed_user_events {
            entry.user_event_sessions += 1;
        }
        if session.subscribed_objects {
            entry.object_sessions += 1;
        }
        entry.last_seen_at_ms = entry.last_seen_at_ms.max(session.last_seen_at_ms);
    }

    by_user
        .into_iter()
        .map(|(user_id, mut entry)| {
            entry.session_ids.sort();
            ConnectionUserSummary {
                user_id,
                session_count: entry.sessions.len(),
                transports: connection_transport_counts(&entry.sessions),
                session_ids: entry.session_ids,
                subscribed_rooms: entry.rooms.into_iter().collect(),
                subscribed_tables: entry.tables.into_iter().collect(),
                subscribed_nested_tables: entry.nested_tables.into_iter().collect(),
                subscribed_queries: entry.queries.into_iter().collect(),
                subscribed_query_tables: entry.query_tables,
                user_event_sessions: entry.user_event_sessions,
                object_sessions: entry.object_sessions,
                last_seen_at_ms: entry.last_seen_at_ms,
            }
        })
        .collect()
}

pub(crate) async fn connect_ws(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConnectQuery>,
    uri: Uri,
    ws: WebSocketUpgrade,
) -> Response {
    let initial_metadata = match parse_connection_metadata(query.metadata.as_deref(), &state) {
        Ok(metadata) => metadata,
        Err(error) => return error.into_response(),
    };
    let admin_connection = token_matches_request(
        &headers,
        &uri,
        state.admin_token.as_deref(),
        &["x-nextdb-admin-token"],
    );
    if let Some(user_id) = query.user_id.as_deref()
        && let Err(error) = ensure_user_token_authorized(&state, &headers, &uri, user_id)
    {
        return error.into_response();
    }
    let drain = state.runtime_drain.read().await.clone();
    if drain.draining {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "node is draining; retry another replica",
                "draining": true,
                "reason": drain.reason,
                "updatedAtMs": drain.updated_at_ms,
            })),
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| {
        websocket_loop(state, query, initial_metadata, admin_connection, socket)
    })
}

pub(crate) async fn connect_jsonl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConnectQuery>,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    let initial_metadata = match parse_connection_metadata(query.metadata.as_deref(), &state) {
        Ok(metadata) => metadata,
        Err(error) => return error.into_response(),
    };
    let admin_connection = token_matches_request(
        &headers,
        &uri,
        state.admin_token.as_deref(),
        &["x-nextdb-admin-token"],
    );
    if let Some(user_id) = query.user_id.as_deref()
        && let Err(error) = ensure_user_token_authorized(&state, &headers, &uri, user_id)
    {
        return error.into_response();
    }
    let drain = state.runtime_drain.read().await.clone();
    if drain.draining {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "node is draining; retry another replica",
                "draining": true,
                "reason": drain.reason,
                "updatedAtMs": drain.updated_at_ms,
            })),
        )
            .into_response();
    }

    let session_id = query
        .session_id
        .unwrap_or_else(|| format!("session_{}", Uuid::now_v7()));
    let user_id = query.user_id;
    let transport = query.transport.unwrap_or(ConnectionTransport::Custom);
    let source = BodyJsonLineFrameSource::new(request.into_body().into_data_stream());
    let (sender, receiver) = mpsc::channel::<Result<Bytes, Infallible>>(1024);
    let sink = ChannelJsonLineFrameSink::new(sender);
    tokio::spawn(async move {
        realtime_connection_loop(
            state,
            session_id,
            user_id,
            transport,
            initial_metadata,
            admin_connection,
            sink,
            source,
        )
        .await;
    });

    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|chunk| (chunk, receiver))
    });
    (
        [
            (header::CONTENT_TYPE, "application/x-ndjson"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn user_query_subscription_limit_error(
    state: &AppState,
    connection_state: &RealtimeConnectionState,
    session_id: &str,
    user_id: Option<&str>,
    query_id: &str,
) -> Option<String> {
    let max_live_queries_per_user = state.limits.max_live_queries_per_user;
    if max_live_queries_per_user == 0 {
        return None;
    }
    let user_id = user_id?;
    let other_session_queries = state
        .connections
        .count_user_query_subscriptions_excluding(user_id, session_id)
        .await;
    let current_session_after = connection_state.projected_query_count(query_id);
    let total_after = other_session_queries + current_session_after;
    if total_after <= max_live_queries_per_user {
        return None;
    }

    Some(format!(
        "live query subscription user limit exceeded: userId={user_id} maxLiveQueriesPerUser={max_live_queries_per_user} otherSessions={other_session_queries} currentSessionRequested={current_session_after} requested={total_after}"
    ))
}

async fn websocket_loop(
    state: AppState,
    query: ConnectQuery,
    initial_metadata: serde_json::Value,
    admin_connection: bool,
    socket: WebSocket,
) {
    let session_id = query
        .session_id
        .unwrap_or_else(|| format!("session_{}", Uuid::now_v7()));
    let user_id = query.user_id;
    let transport = query.transport.unwrap_or(ConnectionTransport::WebSocket);
    let (sender, receiver) = socket.split();
    realtime_connection_loop(
        state,
        session_id,
        user_id,
        transport,
        initial_metadata,
        admin_connection,
        sender,
        receiver,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn realtime_connection_loop<S, R>(
    state: AppState,
    session_id: String,
    user_id: Option<String>,
    transport: ConnectionTransport,
    initial_metadata: serde_json::Value,
    admin_connection: bool,
    mut sender: S,
    mut receiver: R,
) where
    S: RealtimeFrameSink + Send,
    R: RealtimeFrameSource + Send,
{
    let mut events = state.events.subscribe();
    let mut cache_invalidations = state.cache_invalidations.subscribe();
    let mut connection_controls = state.connection_controls.subscribe();
    let mut connection_events = state.connection_events.subscribe();
    let mut aggregate_updates = state.aggregates.subscribe();
    let (targeted_event_tx, mut targeted_events) = mpsc::unbounded_channel();
    let mut connection_state = RealtimeConnectionState::default();

    let hello = ServerFrame::Hello {
        user_id: user_id.clone(),
        session_id: session_id.clone(),
    };
    if send_server_frame(&mut sender, &hello).await.is_err() {
        return;
    }
    let registered = state
        .connections
        .register(
            session_id.clone(),
            user_id.clone(),
            transport,
            initial_metadata,
        )
        .await;
    publish_connection_event(
        &state,
        connection_session_event(ConnectionEventType::Connected, registered),
    );
    state
        .realtime_fanout
        .register(session_id.clone(), user_id.clone(), targeted_event_tx);

    loop {
        tokio::select! {
            frame_read = receiver.next_client_frame() => {
                match frame_read {
                    Ok(RealtimeFrameRead::Frame(frame)) => {
                        state.connections.touch(&session_id).await;
                        if !handle_client_frame(
                            &state,
                            &mut sender,
                            &mut connection_state,
                            &session_id,
                            user_id.as_deref(),
                            admin_connection,
                            *frame,
                        )
                    .await
                    {
                        break;
                    }
                },
                Ok(RealtimeFrameRead::Invalid { message }) => {
                        state.connections.touch(&session_id).await;
                        let frame = ServerFrame::Error {
                            message,
                        };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Ok(RealtimeFrameRead::Ignored) => {}
                    Ok(RealtimeFrameRead::Closed) => break,
                    Err(err) => {
                        warn!(?err, "realtime frame receive failed");
                        break;
                    }
                }
            },
            event = targeted_events.recv() => {
                let Some(first_event) = event else {
                    break;
                };
                let event_batch = drain_targeted_realtime_event_batch(
                    &mut targeted_events,
                    first_event,
                    state.realtime_event_batch_max,
                );
                if !process_realtime_event_batch(
                    &state,
                    &mut sender,
                    &mut connection_state,
                    user_id.as_deref(),
                    &session_id,
                    event_batch,
                    None,
                )
                .await
                {
                    break;
                }
            },
            event = events.recv() => {
                match event {
                    Ok(first_event) => {
                        let (event_batch, lagged) = drain_realtime_event_batch(
                            &mut events,
                            first_event,
                            state.realtime_event_batch_max,
                        );
                        if !process_realtime_event_batch(
                            &state,
                            &mut sender,
                            &mut connection_state,
                            user_id.as_deref(),
                            &session_id,
                            share_delivery_event_batch(event_batch),
                            lagged,
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let frame = ServerFrame::Error {
                            message: format!("connection lagged and skipped {skipped} realtime events; resubscribe with latest LSN"),
                        };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            },
            invalidation = cache_invalidations.recv() => {
                match invalidation {
                    Ok(invalidation) => {
                        let frame = ServerFrame::CacheInvalidated { invalidation };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                        state.connections.touch(&session_id).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let frame = ServerFrame::Error {
                            message: format!("connection lagged and skipped {skipped} cache invalidation events; refresh cache profile"),
                        };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            },
            control = connection_controls.recv() => {
                match control {
                    Ok(control) => {
                        if control.matches(user_id.as_deref(), &session_id) {
                            let frame = ServerFrame::ConnectionClosing {
                                reason: control.reason,
                            };
                            let _ = send_server_frame(&mut sender, &frame).await;
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            },
            connection_event = connection_events.recv(), if connection_state.subscribed_connection_events => {
                match connection_event {
                    Ok(event) => {
                        let frame = ServerFrame::ConnectionEvent { event };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let frame = ServerFrame::Error {
                            message: format!("connection event stream lagged and skipped {skipped} events; refresh /v1/admin/connections"),
                        };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            },
            aggregate_update = aggregate_updates.recv(), if !connection_state.subscribed_aggregate_counts.is_empty() || !connection_state.subscribed_aggregate_sums.is_empty() || !connection_state.subscribed_aggregate_presence.is_empty() => {
                match aggregate_update {
                    Ok(update) => {
                        match update {
                            AggregateUpdate::Count(update) => {
                                if connection_state.subscribed_aggregate_counts.contains(&update.table) {
                                    let frame = ServerFrame::AggregateCountUpdated { update };
                                    if send_server_frame(&mut sender, &frame).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            AggregateUpdate::Sum(update) => {
                                let key = AggregateSumKey::new(update.table.clone(), update.field.clone());
                                if connection_state.subscribed_aggregate_sums.contains(&key) {
                                    let frame = ServerFrame::AggregateSumUpdated { update };
                                    if send_server_frame(&mut sender, &frame).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            AggregateUpdate::Presence(update) => {
                                if connection_state.subscribed_aggregate_presence.contains(&update.channel_id) {
                                    let frame = ServerFrame::AggregatePresenceUpdated { update };
                                    if send_server_frame(&mut sender, &frame).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let frame = ServerFrame::Error {
                            message: format!("aggregate count stream lagged and skipped {skipped} updates; resubscribe aggregate counts"),
                        };
                        if send_server_frame(&mut sender, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            },
        }
    }
    state.realtime_fanout.unregister(&session_id);
    release_table_subscription_scopes(
        &state,
        &connection_state.subscribed_tables,
        &connection_state.subscribed_table_ranges,
    )
    .await;
    release_nested_table_scopes(&state, &connection_state.subscribed_nested_tables).await;
    release_query_scope_residency_all(&state, &mut connection_state).await;
    if let Some(session) = state.connections.unregister(&session_id).await {
        publish_connection_event(
            &state,
            connection_session_event(ConnectionEventType::Disconnected, session),
        );
    }
    if let Some(user_id) = user_id.as_deref() {
        let leaves = state.realtime.leave_session(user_id, &session_id).await;
        for leave in leaves {
            state.aggregates.publish_presence_update(
                &leave.channel_id,
                &leave.remaining,
                state.current_lsn.load(Ordering::Acquire),
                now_ms(),
            );
            publish_realtime_member_left(&state, &leave).await;
        }
    }
}

async fn process_realtime_event_batch<S>(
    state: &AppState,
    sender: &mut S,
    connection_state: &mut RealtimeConnectionState,
    user_id: Option<&str>,
    session_id: &str,
    event_batch: RoutedRealtimeEventBatch,
    lagged: Option<u64>,
) -> bool
where
    S: RealtimeFrameSink + Send,
{
    struct PendingQueryUpdate {
        query_id: String,
        subscription: RecordQuerySubscription,
        snapshot: RecordQuerySnapshot,
        sent_diff: bool,
    }

    state.live_query_metrics.note_event_batch(event_batch.len());
    let mut touched = false;
    let mut frames = Vec::<EncodedServerFrame>::new();
    let mut pending_query_updates = Vec::<PendingQueryUpdate>::new();
    let schema = state.schema.schema();
    let schema_version = schema.version;
    let (affected_query_ids, refresh_candidates, deleted_hints) = affected_live_query_refresh_batch(
        connection_state,
        schema_version,
        event_batch.iter().map(Arc::as_ref),
    );
    let mut deliverable_events = Vec::new();
    for event in event_batch.iter() {
        if should_deliver_event(event, &schema, connection_state, user_id, session_id) {
            touched = true;
            deliverable_events.push(event.as_ref());
        }
    }
    if !deliverable_events.is_empty() {
        if deliverable_events.len() == event_batch.len()
            && let Some(frame) = event_batch.preencoded_events_frame()
        {
            frames.push(frame);
        } else if let Ok(Some(frame)) = encode_delivery_events_frame(&deliverable_events) {
            frames.push(frame);
        } else {
            return false;
        }
    }
    state
        .live_query_metrics
        .note_refresh_candidates(refresh_candidates);
    let global_cache_lsn = record_event_batch_cache_lsn(
        event_batch.iter().map(Arc::as_ref),
        state.current_lsn.load(Ordering::Acquire),
    );
    let mut query_evaluation_cache =
        BTreeMap::<RecordQueryPlanKey, std::result::Result<RecordQueryEvaluation, String>>::new();
    for query_id in affected_query_ids {
        let Some(subscription) = connection_state.take_query_subscription(&query_id) else {
            continue;
        };
        state.live_query_metrics.note_refresh();
        let plan_key = subscription.plan_key();
        let evaluation = if let Some(evaluation) = query_evaluation_cache.get(&plan_key).cloned() {
            evaluation
        } else {
            let cache_token =
                live_query_cache_token_with_lsn(state, &subscription, global_cache_lsn).await;
            let evaluation =
                cached_record_query_evaluation(state, &subscription, cache_token, &plan_key).await;
            query_evaluation_cache.insert(plan_key, evaluation.clone());
            evaluation
        };
        match evaluation {
            Ok(evaluation) => match prepare_record_query_evaluation(
                state,
                &subscription,
                false,
                deleted_hints.as_ref(),
                &evaluation,
            ) {
                Ok(Some(prepared)) => {
                    touched = true;
                    let Ok(frame) = encode_server_frame_to_encoded(&prepared.frame) else {
                        connection_state.put_query_subscription(query_id, subscription);
                        for update in pending_query_updates {
                            connection_state
                                .put_query_subscription(update.query_id, update.subscription);
                        }
                        return false;
                    };
                    frames.push(frame);
                    pending_query_updates.push(PendingQueryUpdate {
                        query_id,
                        subscription,
                        snapshot: prepared.snapshot,
                        sent_diff: prepared.sent_diff,
                    });
                }
                Ok(None) => {
                    connection_state.put_query_subscription(query_id, subscription);
                }
                Err(err) => {
                    touched = true;
                    state.live_query_metrics.note_error();
                    connection_state.put_query_subscription(query_id, subscription);
                    let frame = ServerFrame::Error {
                        message: format!("query refresh failed: {err}"),
                    };
                    if let Ok(frame) = encode_server_frame_to_encoded(&frame) {
                        frames.push(frame);
                    } else {
                        for update in pending_query_updates {
                            connection_state
                                .put_query_subscription(update.query_id, update.subscription);
                        }
                        return false;
                    }
                }
            },
            Err(err) => {
                touched = true;
                state.live_query_metrics.note_error();
                connection_state.put_query_subscription(query_id, subscription);
                let frame = ServerFrame::Error {
                    message: format!("query refresh failed: {err}"),
                };
                if let Ok(frame) = encode_server_frame_to_encoded(&frame) {
                    frames.push(frame);
                } else {
                    for update in pending_query_updates {
                        connection_state
                            .put_query_subscription(update.query_id, update.subscription);
                    }
                    return false;
                }
            }
        }
    }
    if let Some(skipped) = lagged {
        let frame = ServerFrame::Error {
            message: format!(
                "connection lagged and skipped {skipped} realtime events; resubscribe with latest LSN"
            ),
        };
        if let Ok(frame) = encode_server_frame_to_encoded(&frame) {
            frames.push(frame);
        } else {
            for update in pending_query_updates {
                connection_state.put_query_subscription(update.query_id, update.subscription);
            }
            return false;
        }
    }
    if sender.send_encoded_frames(&frames).await.is_err() {
        for update in pending_query_updates {
            connection_state.put_query_subscription(update.query_id, update.subscription);
        }
        return false;
    }
    for mut update in pending_query_updates {
        let next_scope_keys =
            query_scope_keys_for_response(&update.subscription, &update.snapshot.response);
        update.subscription.apply_snapshot(update.snapshot);
        sync_query_scope_residency(state, &mut update.subscription, next_scope_keys).await;
        if update.sent_diff {
            state.live_query_metrics.note_diff_frame();
        } else {
            state.live_query_metrics.note_result_frame();
        }
        connection_state.put_query_subscription(update.query_id, update.subscription);
    }
    if touched {
        state.connections.touch(session_id).await;
    }
    true
}

pub(crate) fn drain_targeted_realtime_event_batch(
    events: &mut mpsc::UnboundedReceiver<RoutedRealtimeEventBatch>,
    first_events: RoutedRealtimeEventBatch,
    max_events: usize,
) -> RoutedRealtimeEventBatch {
    let max_events = max_events.max(1);
    let mut batch = first_events;
    let mut combined = false;
    while batch.len() < max_events {
        match events.try_recv() {
            Ok(events) => {
                batch.extend(events);
                combined = true;
            }
            Err(mpsc::error::TryRecvError::Empty)
            | Err(mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    if combined {
        batch.refresh_preencoded_events_frame();
    }
    batch
}

fn share_delivery_event_batch(events: DeliveryEventBatch) -> RoutedRealtimeEventBatch {
    RoutedRealtimeEventBatch::new(events.into_iter().map(Arc::new).collect(), None)
}

pub(crate) fn drain_realtime_event_batch(
    events: &mut broadcast::Receiver<DeliveryEventBatch>,
    first_events: DeliveryEventBatch,
    max_events: usize,
) -> (Vec<DeliveryEvent>, Option<u64>) {
    let max_events = max_events.max(1);
    let mut batch = Vec::with_capacity(max_events.min(16));
    batch.extend(first_events);
    let mut lagged = None;
    while batch.len() < max_events {
        match events.try_recv() {
            Ok(events) => batch.extend(events),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => break,
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                lagged = Some(skipped);
                break;
            }
        }
    }
    (batch, lagged)
}

#[allow(clippy::collapsible_if)]
pub(crate) async fn handle_client_frame(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    connection_state: &mut RealtimeConnectionState,
    session_id: &str,
    user_id: Option<&str>,
    admin_connection: bool,
    frame: ClientFrame,
) -> bool {
    match frame {
        ClientFrame::SubscribeRoom {
            room_id,
            after_lsn,
            catch_up_limit,
        } => {
            connection_state.subscribed_rooms.insert(room_id.clone());
            if let Err(error) = activate_runtime_room_internal(
                state,
                RuntimeRoomActivationRequest {
                    room_id: room_id.clone(),
                    limit: catch_up_limit,
                },
            )
            .await
            {
                let frame = ServerFrame::Error {
                    message: error.message,
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
            }
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::Subscribed {
                room_id: room_id.clone(),
            };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
            if let Some(after_lsn) = after_lsn {
                if send_subscription_catch_up(
                    state,
                    sender,
                    vec![room_id],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    false,
                    user_id,
                    after_lsn,
                    catch_up_limit,
                )
                .await
                .is_err()
                {
                    return false;
                }
            }
        }
        ClientFrame::UnsubscribeRoom { room_id } => {
            connection_state.subscribed_rooms.remove(&room_id);
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::Unsubscribed { room_id };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeTable {
            table,
            lower_key,
            upper_key,
            index_name,
            index_values,
            snapshot_limit,
            after_lsn,
            catch_up_limit,
        } => {
            let subscription = TableSubscription::new(table, lower_key, upper_key)
                .with_index_prefix(index_name, index_values);
            if let Err(error) = validate_table_subscription(&subscription, &state.schema.schema()) {
                let frame = ServerFrame::Error {
                    message: error.message,
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            let inserted = connection_state.add_table_subscription(subscription.clone());
            if inserted {
                retain_table_subscription_scopes(state, &subscription).await;
            }
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::TableSubscribed {
                table: subscription.table.clone(),
            };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
            if let Some(snapshot_limit) = snapshot_limit
                && send_table_subscription_snapshot(
                    state,
                    sender,
                    &subscription,
                    snapshot_limit,
                    user_id,
                )
                .await
                .is_err()
            {
                return false;
            }
            if let Some(after_lsn) = after_lsn {
                if send_subscription_catch_up(
                    state,
                    sender,
                    Vec::new(),
                    Vec::new(),
                    if subscription.is_full_table() {
                        vec![subscription.table.clone()]
                    } else {
                        Vec::new()
                    },
                    if subscription.is_full_table() {
                        Vec::new()
                    } else {
                        vec![subscription.clone()]
                    },
                    Vec::new(),
                    false,
                    user_id,
                    after_lsn,
                    catch_up_limit,
                )
                .await
                .is_err()
                {
                    return false;
                }
            }
        }
        ClientFrame::UnsubscribeTable {
            table,
            lower_key,
            upper_key,
            index_name,
            index_values,
        } => {
            let subscription = TableSubscription::new(table, lower_key, upper_key)
                .with_index_prefix(index_name, index_values);
            let removed = connection_state.remove_table_subscription(&subscription);
            if removed {
                release_table_subscription_scope(state, &subscription).await;
            }
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::TableUnsubscribed {
                table: subscription.table,
            };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeNestedTable {
            table,
            parent_key,
            nested,
            snapshot_limit,
            after_lsn,
            catch_up_limit,
        } => {
            let subscription = NestedTableSubscription::new(table, parent_key, nested);
            if let Err(error) = validate_nested_table_path(
                &subscription.table,
                &subscription.parent_key,
                &subscription.nested,
                state,
            ) {
                let frame = ServerFrame::Error {
                    message: error.message,
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            let inserted = connection_state.add_nested_table_subscription(subscription.clone());
            if inserted {
                retain_nested_table_scope(state, &subscription).await;
            }
            if subscription.table == "rooms" && subscription.nested == "messages" {
                if let Err(error) = activate_runtime_room_internal(
                    state,
                    RuntimeRoomActivationRequest {
                        room_id: subscription.parent_key.clone(),
                        limit: catch_up_limit,
                    },
                )
                .await
                {
                    let frame = ServerFrame::Error {
                        message: error.message,
                    };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
            }
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::TableSubscribed {
                table: subscription.logical_table(),
            };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
            if let Some(snapshot_limit) = snapshot_limit
                && send_nested_table_subscription_snapshot(
                    state,
                    sender,
                    &subscription,
                    snapshot_limit,
                    user_id,
                )
                .await
                .is_err()
            {
                return false;
            }
            if let Some(after_lsn) = after_lsn {
                if send_subscription_catch_up(
                    state,
                    sender,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    vec![subscription],
                    false,
                    user_id,
                    after_lsn,
                    catch_up_limit,
                )
                .await
                .is_err()
                {
                    return false;
                }
            }
        }
        ClientFrame::UnsubscribeNestedTable {
            table,
            parent_key,
            nested,
        } => {
            let subscription = NestedTableSubscription::new(table, parent_key, nested);
            let removed = connection_state.remove_nested_table_subscription(&subscription);
            if removed {
                release_nested_table_scope(state, &subscription).await;
            }
            update_connection_subscriptions(
                state,
                session_id,
                &connection_state.subscribed_rooms,
                &connection_state.subscribed_tables,
                &connection_state.subscribed_table_ranges,
                &connection_state.subscribed_nested_tables,
            )
            .await;
            let frame = ServerFrame::TableUnsubscribed {
                table: subscription.logical_table(),
            };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeQuery {
            query_id,
            table,
            parent_key,
            nested,
            index_name,
            value,
            values,
            lower,
            upper,
            lower_values,
            upper_values,
            after_key,
            after_cursor,
            limit,
            order,
            predicate,
            result_id,
            diff,
        } => {
            if !is_valid_query_id(&query_id) {
                let frame = ServerFrame::Error {
                    message: "invalid queryId".to_string(),
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            let subscribed_table = nested
                .as_ref()
                .map(|nested| nested_record_table(&table, nested))
                .unwrap_or_else(|| table.clone());
            let parent_key_prefix = parent_key.as_deref().map(nested_record_prefix);
            if let Some(message) = connection_state.query_subscription_limit_error(
                &query_id,
                &subscribed_table,
                &state.limits,
            ) {
                let frame = ServerFrame::Error { message };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            if let Some(message) = user_query_subscription_limit_error(
                state,
                connection_state,
                session_id,
                user_id,
                &query_id,
            )
            .await
            {
                let frame = ServerFrame::Error { message };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            let index_query = QueryRecordsByIndexQuery {
                consistency: RecordReadConsistencyQuery::default(),
                value,
                values,
                lower,
                upper,
                lower_values,
                upper_values,
                after_key: after_key.clone(),
                after_cursor: after_cursor.clone(),
                limit,
                shard: None,
                predicate: predicate.clone(),
            };
            let schema = state.schema.schema();
            let impact_filter = record_query_impact_filter(
                &schema,
                &table,
                nested.as_deref(),
                index_name.as_deref(),
                &index_query,
                predicate.as_ref(),
            );
            let mut subscription = RecordQuerySubscription {
                query_id: query_id.clone(),
                table,
                parent_key,
                nested,
                subscribed_table,
                parent_key_prefix,
                index_name,
                index_query,
                impact_filter,
                schema_version: schema.version,
                after_key,
                after_cursor,
                limit,
                order,
                predicate,
                last_result_id: result_id,
                last_response: None,
                last_response_keys: HashSet::new(),
                retained_scope_keys: BTreeSet::new(),
                diff,
            };
            let force_initial = subscription.last_result_id.is_none();
            let initial_cache_token = live_query_cache_token_with_lsn(
                state,
                &subscription,
                Some(state.current_lsn.load(Ordering::Acquire)),
            )
            .await;
            match send_record_query_result(
                state,
                sender,
                &subscription,
                force_initial,
                None,
                initial_cache_token,
            )
            .await
            {
                Ok(Some(snapshot)) => {
                    let next_scope_keys =
                        query_scope_keys_for_response(&subscription, &snapshot.response);
                    subscription.apply_snapshot(snapshot);
                    sync_query_scope_residency(state, &mut subscription, next_scope_keys).await;
                    replace_query_subscription(
                        state,
                        connection_state,
                        query_id.clone(),
                        subscription,
                    )
                    .await;
                    state.live_query_metrics.note_subscribed();
                    update_connection_query_subscriptions(state, session_id, connection_state)
                        .await;
                    let frame = ServerFrame::QuerySubscribed { query_id };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
                Ok(None) => {
                    let result_id = subscription.last_result_id.clone().unwrap_or_default();
                    replace_query_subscription(
                        state,
                        connection_state,
                        query_id.clone(),
                        subscription,
                    )
                    .await;
                    state.live_query_metrics.note_subscribed();
                    update_connection_query_subscriptions(state, session_id, connection_state)
                        .await;
                    let frame = ServerFrame::QueryUnchanged {
                        query_id: query_id.clone(),
                        result_id,
                        current_lsn: state.current_lsn.load(Ordering::Acquire),
                    };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                    let frame = ServerFrame::QuerySubscribed { query_id };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
                Err(err) => {
                    state.live_query_metrics.note_error();
                    let frame = ServerFrame::Error {
                        message: format!("query subscription failed: {err}"),
                    };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
            }
        }
        ClientFrame::UnsubscribeQuery { query_id } => {
            if let Some(mut subscription) =
                connection_state.remove_query_subscription_entry(&query_id)
            {
                release_query_scope_residency(state, &mut subscription).await;
                state.live_query_metrics.note_unsubscribed();
            }
            update_connection_query_subscriptions(state, session_id, connection_state).await;
            let frame = ServerFrame::QueryUnsubscribed { query_id };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeUserEvents {
            after_lsn,
            catch_up_limit,
        } => {
            let Some(user_id) = user_id else {
                let frame = ServerFrame::Error {
                    message: "subscribeUserEvents requires userId".to_string(),
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            };
            connection_state.subscribed_user_events = true;
            state
                .realtime_fanout
                .update_user_event_subscription(session_id, true);
            if let Some(session) = state
                .connections
                .update_user_event_subscription(session_id, true)
                .await
            {
                publish_connection_event(
                    state,
                    connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
                );
            }
            if let Some(after_lsn) = after_lsn {
                if send_subscription_catch_up(
                    state,
                    sender,
                    Vec::new(),
                    vec![user_id.to_string()],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    false,
                    Some(user_id),
                    after_lsn,
                    catch_up_limit,
                )
                .await
                .is_err()
                {
                    return false;
                }
            }
        }
        ClientFrame::UnsubscribeUserEvents => {
            connection_state.subscribed_user_events = false;
            state
                .realtime_fanout
                .update_user_event_subscription(session_id, false);
            if let Some(session) = state
                .connections
                .update_user_event_subscription(session_id, false)
                .await
            {
                publish_connection_event(
                    state,
                    connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
                );
            }
            let frame = ServerFrame::UserEventsUnsubscribed;
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeObjects {
            after_lsn,
            catch_up_limit,
        } => {
            connection_state.subscribed_objects = true;
            state
                .realtime_fanout
                .update_object_subscription(session_id, true);
            if let Some(session) = state
                .connections
                .update_object_subscription(session_id, true)
                .await
            {
                publish_connection_event(
                    state,
                    connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
                );
            }
            let frame = ServerFrame::ObjectsSubscribed;
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
            if let Some(after_lsn) = after_lsn {
                if send_subscription_catch_up(
                    state,
                    sender,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    true,
                    user_id,
                    after_lsn,
                    catch_up_limit,
                )
                .await
                .is_err()
                {
                    return false;
                }
            }
        }
        ClientFrame::UnsubscribeObjects => {
            connection_state.subscribed_objects = false;
            state
                .realtime_fanout
                .update_object_subscription(session_id, false);
            if let Some(session) = state
                .connections
                .update_object_subscription(session_id, false)
                .await
            {
                publish_connection_event(
                    state,
                    connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
                );
            }
            let frame = ServerFrame::ObjectsUnsubscribed;
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::UpdateConnectionMetadata { metadata } => {
            if let Err(error) = ensure_json_value_limit(
                "connection metadata",
                &metadata,
                state.limits.max_user_event_bytes,
            ) {
                let frame = ServerFrame::Error {
                    message: error.message,
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            if let Some(session) = state
                .connections
                .update_metadata(session_id, metadata)
                .await
            {
                publish_connection_event(
                    state,
                    connection_session_event(ConnectionEventType::MetadataUpdated, session.clone()),
                );
                let frame = ServerFrame::ConnectionMetadataUpdated { session };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
            }
        }
        ClientFrame::SubscribeConnectionEvents => {
            if !admin_connection {
                let frame = ServerFrame::Error {
                    message: "subscribeConnectionEvents requires admin token".to_string(),
                };
                if send_server_frame(sender, &frame).await.is_err() {
                    return false;
                }
                return true;
            }
            connection_state.subscribed_connection_events = true;
            let frame = ServerFrame::ConnectionEventsSubscribed;
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::UnsubscribeConnectionEvents => {
            connection_state.subscribed_connection_events = false;
            let frame = ServerFrame::ConnectionEventsUnsubscribed;
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeAggregateCount { table } => {
            connection_state
                .subscribed_aggregate_counts
                .insert(table.clone());
            match state
                .aggregates
                .table_count_snapshot(
                    &state.records,
                    &table,
                    state.current_lsn.load(Ordering::Acquire),
                )
                .await
            {
                Ok(snapshot) => {
                    let frame = ServerFrame::AggregateCountSubscribed { snapshot };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
                Err(err) => {
                    connection_state.subscribed_aggregate_counts.remove(&table);
                    let frame = ServerFrame::Error {
                        message: format!("aggregate count subscription failed: {err}"),
                    };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
            }
        }
        ClientFrame::UnsubscribeAggregateCount { table } => {
            connection_state.subscribed_aggregate_counts.remove(&table);
            let frame = ServerFrame::AggregateCountUnsubscribed { table };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeAggregateSum { table, field } => {
            let key = AggregateSumKey::new(table.clone(), field.clone());
            connection_state
                .subscribed_aggregate_sums
                .insert(key.clone());
            match state
                .aggregates
                .table_sum_snapshot(
                    &state.records,
                    &table,
                    &field,
                    state.current_lsn.load(Ordering::Acquire),
                )
                .await
            {
                Ok(snapshot) => {
                    let frame = ServerFrame::AggregateSumSubscribed { snapshot };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
                Err(err) => {
                    connection_state.subscribed_aggregate_sums.remove(&key);
                    let frame = ServerFrame::Error {
                        message: format!("aggregate sum subscription failed: {err}"),
                    };
                    if send_server_frame(sender, &frame).await.is_err() {
                        return false;
                    }
                }
            }
        }
        ClientFrame::UnsubscribeAggregateSum { table, field } => {
            connection_state
                .subscribed_aggregate_sums
                .remove(&AggregateSumKey::new(table.clone(), field.clone()));
            let frame = ServerFrame::AggregateSumUnsubscribed { table, field };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::SubscribeAggregatePresence { channel_id } => {
            connection_state
                .subscribed_aggregate_presence
                .insert(channel_id.clone());
            let members = state.realtime.members(&channel_id).await;
            let snapshot = state.aggregates.channel_presence_snapshot(
                &channel_id,
                &members,
                state.current_lsn.load(Ordering::Acquire),
                now_ms(),
            );
            let frame = ServerFrame::AggregatePresenceSubscribed { snapshot };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
        ClientFrame::UnsubscribeAggregatePresence { channel_id } => {
            connection_state
                .subscribed_aggregate_presence
                .remove(&channel_id);
            let frame = ServerFrame::AggregatePresenceUnsubscribed { channel_id };
            if send_server_frame(sender, &frame).await.is_err() {
                return false;
            }
        }
    }
    true
}

async fn update_connection_subscriptions(
    state: &AppState,
    session_id: &str,
    subscribed_rooms: &BTreeSet<String>,
    subscribed_tables: &BTreeSet<String>,
    subscribed_table_ranges: &BTreeSet<TableSubscription>,
    subscribed_nested_tables: &BTreeSet<NestedTableSubscription>,
) {
    state
        .realtime_fanout
        .update_record_subscriptions_with_schema(
            session_id,
            subscribed_rooms,
            subscribed_tables,
            subscribed_table_ranges,
            subscribed_nested_tables,
            &state.schema.schema(),
        );
    let table_labels = subscribed_tables
        .iter()
        .cloned()
        .chain(subscribed_table_ranges.iter().map(TableSubscription::label))
        .collect::<BTreeSet<_>>();
    let nested_labels = subscribed_nested_tables
        .iter()
        .map(NestedTableSubscription::label)
        .collect::<BTreeSet<_>>();
    if let Some(session) = state
        .connections
        .update_subscriptions(session_id, subscribed_rooms, &table_labels, &nested_labels)
        .await
    {
        publish_connection_event(
            state,
            connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
        );
    }
}

async fn update_connection_query_subscriptions(
    state: &AppState,
    session_id: &str,
    connection_state: &RealtimeConnectionState,
) {
    let query_table_counts = connection_state.subscribed_query_table_counts();
    let query_tables = query_table_counts.keys().cloned().collect::<BTreeSet<_>>();
    state
        .realtime_fanout
        .update_query_subscriptions(session_id, &query_tables);
    if let Some(session) = state
        .connections
        .update_query_subscriptions(
            session_id,
            &connection_state.subscribed_query_ids,
            &query_table_counts,
        )
        .await
    {
        publish_connection_event(
            state,
            connection_session_event(ConnectionEventType::SubscriptionsUpdated, session),
        );
    }
}

async fn retain_table_subscription_scopes(state: &AppState, subscription: &TableSubscription) {
    let table_key = actor::record_actor_table_key(&subscription.table);
    for scope_key in table_subscription_scope_keys(subscription) {
        state
            .actors
            .retain_scope_subscription(table_key.clone(), scope_key)
            .await;
    }
}

async fn release_table_subscription_scope(state: &AppState, subscription: &TableSubscription) {
    let table_key = actor::record_actor_table_key(&subscription.table);
    for scope_key in table_subscription_scope_keys(subscription) {
        state
            .actors
            .release_scope_subscription(table_key.clone(), scope_key, scope_residency_linger_ms())
            .await;
    }
}

async fn release_table_subscription_scopes(
    state: &AppState,
    subscribed_tables: &BTreeSet<String>,
    subscribed_table_ranges: &BTreeSet<TableSubscription>,
) {
    for table in subscribed_tables {
        release_table_subscription_scope(state, &TableSubscription::new(table.clone(), None, None))
            .await;
    }
    for subscription in subscribed_table_ranges {
        release_table_subscription_scope(state, subscription).await;
    }
}

fn table_subscription_scope_keys(subscription: &TableSubscription) -> Vec<String> {
    if subscription.has_index_prefix() {
        return Vec::new();
    }
    (0..actor::RECORD_ACTOR_SCOPE_BUCKET_COUNT)
        .map(|bucket| actor::record_actor_scope_bucket_key(&subscription.table, bucket))
        .collect()
}

async fn retain_nested_table_scope(state: &AppState, subscription: &NestedTableSubscription) {
    let logical_table = subscription.logical_table();
    let table_key = actor::record_actor_table_key(&logical_table);
    let scope_key = nested_table_scope_key(&logical_table, &subscription.parent_key);
    state
        .actors
        .retain_scope_subscription(table_key, scope_key)
        .await;
}

async fn release_nested_table_scope(state: &AppState, subscription: &NestedTableSubscription) {
    let logical_table = subscription.logical_table();
    let table_key = actor::record_actor_table_key(&logical_table);
    let scope_key = nested_table_scope_key(&logical_table, &subscription.parent_key);
    state
        .actors
        .release_scope_subscription(table_key, scope_key, scope_residency_linger_ms())
        .await;
}

async fn release_nested_table_scopes(
    state: &AppState,
    subscriptions: &BTreeSet<NestedTableSubscription>,
) {
    for subscription in subscriptions {
        release_nested_table_scope(state, subscription).await;
    }
}

async fn sync_query_scope_residency(
    state: &AppState,
    subscription: &mut RecordQuerySubscription,
    next_scope_keys: BTreeSet<String>,
) {
    let table_key = actor::record_actor_table_key(&subscription.subscribed_table);
    for scope_key in subscription
        .retained_scope_keys
        .difference(&next_scope_keys)
    {
        state
            .actors
            .release_scope_subscription(
                table_key.clone(),
                scope_key.clone(),
                scope_residency_linger_ms(),
            )
            .await;
    }
    for scope_key in next_scope_keys.difference(&subscription.retained_scope_keys) {
        state
            .actors
            .retain_scope_subscription(table_key.clone(), scope_key.clone())
            .await;
    }
    subscription.retained_scope_keys = next_scope_keys;
}

async fn release_query_scope_residency(
    state: &AppState,
    subscription: &mut RecordQuerySubscription,
) {
    if subscription.retained_scope_keys.is_empty() {
        return;
    }
    let table_key = actor::record_actor_table_key(&subscription.subscribed_table);
    let retained_scope_keys = std::mem::take(&mut subscription.retained_scope_keys);
    for scope_key in retained_scope_keys {
        state
            .actors
            .release_scope_subscription(table_key.clone(), scope_key, scope_residency_linger_ms())
            .await;
    }
}

async fn release_query_scope_residency_all(
    state: &AppState,
    connection_state: &mut RealtimeConnectionState,
) {
    for subscription in connection_state.subscribed_queries.values_mut() {
        release_query_scope_residency(state, subscription).await;
    }
}

async fn replace_query_subscription(
    state: &AppState,
    connection_state: &mut RealtimeConnectionState,
    query_id: String,
    subscription: RecordQuerySubscription,
) {
    if let Some(mut previous) = connection_state.remove_query_subscription_entry(&query_id) {
        release_query_scope_residency(state, &mut previous).await;
    }
    connection_state.add_query_subscription(query_id, subscription);
}

fn query_scope_keys_for_response(
    subscription: &RecordQuerySubscription,
    response: &ListRecordsResponse,
) -> BTreeSet<String> {
    let mut scope_keys = BTreeSet::new();
    if let Some(parent_key) = subscription.parent_key.as_deref()
        && subscription.nested.is_some()
    {
        scope_keys.insert(nested_table_scope_key(
            &subscription.subscribed_table,
            parent_key,
        ));
    }
    for record in &response.records {
        scope_keys.insert(actor::record_actor_scope_key(
            &subscription.subscribed_table,
            &record.key,
        ));
    }
    scope_keys
}

fn nested_table_scope_key(logical_table: &str, parent_key: &str) -> String {
    actor::record_actor_scope_key(logical_table, &format!("{parent_key}:"))
}

fn scope_residency_linger_ms() -> u64 {
    std::env::var("NEXTDB_SCOPE_RESIDENCY_LINGER_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30_000)
}

fn subscription_catch_up_tables(
    tables: Vec<String>,
    table_ranges: &[TableSubscription],
    nested_tables: &[NestedTableSubscription],
) -> Vec<String> {
    let mut table_set = tables.into_iter().collect::<BTreeSet<_>>();
    for table_range in table_ranges {
        table_set.insert(table_range.table.clone());
    }
    for nested_table in nested_tables {
        table_set.insert(nested_table.logical_table());
    }
    table_set.into_iter().collect()
}

async fn send_table_subscription_snapshot(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    subscription: &TableSubscription,
    limit: usize,
    user_id: Option<&str>,
) -> Result<()> {
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let response = match execute_table_subscription_snapshot(
        state,
        subscription,
        normalize_limit(Some(limit)),
        current_lsn,
    )
    .await
    {
        Ok(response) => filter_visible_table_snapshot(response, &state.schema.schema(), user_id),
        Err(error) => {
            let frame = ServerFrame::Error {
                message: format!("table snapshot failed: {}", error.message),
            };
            return send_server_frame(sender, &frame).await;
        }
    };
    let frame = ServerFrame::TableSnapshot {
        table: subscription.table.clone(),
        lower_key: subscription.lower_key.clone(),
        upper_key: subscription.upper_key.clone(),
        index_name: subscription.index_name.clone(),
        index_values: subscription.index_values.clone(),
        response,
        current_lsn,
    };
    send_server_frame(sender, &frame).await
}

async fn send_nested_table_subscription_snapshot(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    subscription: &NestedTableSubscription,
    limit: usize,
    user_id: Option<&str>,
) -> Result<()> {
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let response = match execute_record_list_query(
        state,
        subscription.table.clone(),
        Some(subscription.parent_key.clone()),
        Some(subscription.nested.clone()),
        ListRecordsQuery {
            consistency: RecordReadConsistencyQuery {
                consistency: Some(RecordReadConsistency::ReadYourWrites),
                min_lsn: Some(current_lsn),
            },
            after_key: None,
            after_cursor: None,
            limit: Some(normalize_limit(Some(limit))),
            order: None,
            shard: None,
            predicate: None,
        },
    )
    .await
    {
        Ok(response) => filter_visible_table_snapshot(response, &state.schema.schema(), user_id),
        Err(error) => {
            let frame = ServerFrame::Error {
                message: format!("nested table snapshot failed: {}", error.message),
            };
            return send_server_frame(sender, &frame).await;
        }
    };
    let frame = ServerFrame::NestedTableSnapshot {
        table: subscription.table.clone(),
        parent_key: subscription.parent_key.clone(),
        nested: subscription.nested.clone(),
        response,
        current_lsn,
    };
    send_server_frame(sender, &frame).await
}

async fn execute_table_subscription_snapshot(
    state: &AppState,
    subscription: &TableSubscription,
    limit: usize,
    current_lsn: u64,
) -> Result<ListRecordsResponse, ApiError> {
    if subscription.is_full_table() {
        return execute_record_list_query(
            state,
            subscription.table.clone(),
            None,
            None,
            ListRecordsQuery {
                consistency: RecordReadConsistencyQuery {
                    consistency: Some(RecordReadConsistency::ReadYourWrites),
                    min_lsn: Some(current_lsn),
                },
                after_key: None,
                after_cursor: None,
                limit: Some(limit),
                order: None,
                shard: None,
                predicate: None,
            },
        )
        .await;
    }

    if subscription.has_index_prefix() {
        return execute_table_subscription_index_prefix_snapshot(
            state,
            subscription,
            limit,
            current_lsn,
        )
        .await;
    }

    validate_table_path(&subscription.table, state)?;
    resolve_record_read_consistency(
        state,
        &RecordReadConsistencyQuery {
            consistency: Some(RecordReadConsistency::ReadYourWrites),
            min_lsn: Some(current_lsn),
        },
    )
    .await?;
    let (records, has_more) =
        list_table_subscription_range_records(state, subscription, limit).await?;
    let next_after_key = records.last().map(|record| record.key.clone());
    Ok(ListRecordsResponse {
        table: subscription.table.clone(),
        records,
        next_after_key,
        next_cursor: None,
        has_more,
    })
}

async fn list_table_subscription_range_records(
    state: &AppState,
    subscription: &TableSubscription,
    limit: usize,
) -> Result<(Vec<DbRecord>, bool), ApiError> {
    list_table_subscription_filtered_range_records(state, subscription, limit, |_| true).await
}

async fn list_table_subscription_filtered_range_records(
    state: &AppState,
    subscription: &TableSubscription,
    limit: usize,
    matches: impl Fn(&DbRecord) -> bool,
) -> Result<(Vec<DbRecord>, bool), ApiError> {
    let mut records = Vec::with_capacity(limit);
    let mut after_key: Option<String> = None;

    loop {
        let batch =
            list_records_from_live_or_disk(state, &subscription.table, after_key.as_deref(), 500)
                .await?;
        if batch.is_empty() {
            return Ok((records, false));
        }
        for record in batch {
            after_key = Some(record.key.clone());
            if subscription
                .lower_key
                .as_deref()
                .is_some_and(|lower| record.key.as_str() < lower)
            {
                continue;
            }
            if subscription
                .upper_key
                .as_deref()
                .is_some_and(|upper| record.key.as_str() >= upper)
            {
                return Ok((records, false));
            }
            if matches(&record) {
                if records.len() >= limit {
                    return Ok((records, true));
                }
                records.push(record);
            }
        }
    }
}

async fn execute_table_subscription_index_prefix_snapshot(
    state: &AppState,
    subscription: &TableSubscription,
    limit: usize,
    current_lsn: u64,
) -> Result<ListRecordsResponse, ApiError> {
    validate_table_path(&subscription.table, state)?;
    let index_name = subscription
        .index_name
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("indexName is required"))?;
    let index_values = subscription
        .index_values
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("indexValues is required"))?;
    let schema = state.schema.schema();
    let index = table_subscription_index_schema(&schema, &subscription.table, index_name)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "table subscription index {index_name} is not declared on {}",
                subscription.table
            ))
        })?;
    let values = parse_index_prefix_values(index_values, index)?;
    if subscription.lower_key.is_some() || subscription.upper_key.is_some() {
        resolve_record_read_consistency(
            state,
            &RecordReadConsistencyQuery {
                consistency: Some(RecordReadConsistency::ReadYourWrites),
                min_lsn: Some(current_lsn),
            },
        )
        .await?;
        let (records, has_more) =
            list_table_subscription_filtered_range_records(state, subscription, limit, |record| {
                record_matches_table_subscription(
                    &subscription.table,
                    &record.key,
                    Some(record),
                    &schema,
                    subscription,
                )
            })
            .await?;
        let next_after_key = records.last().map(|record| record.key.clone());
        return Ok(ListRecordsResponse {
            table: subscription.table.clone(),
            records,
            next_after_key,
            next_cursor: None,
            has_more,
        });
    }
    if values.len() == index.fields.len() {
        return execute_record_index_query(
            state,
            subscription.table.clone(),
            None,
            None,
            index_name.to_string(),
            QueryRecordsByIndexQuery {
                consistency: RecordReadConsistencyQuery {
                    consistency: Some(RecordReadConsistency::ReadYourWrites),
                    min_lsn: Some(current_lsn),
                },
                value: None,
                values: Some(index_values.to_string()),
                lower: None,
                upper: None,
                lower_values: None,
                upper_values: None,
                after_key: None,
                after_cursor: None,
                limit: Some(limit),
                shard: None,
                predicate: None,
            },
        )
        .await;
    }
    execute_record_list_query(
        state,
        subscription.table.clone(),
        None,
        None,
        ListRecordsQuery {
            consistency: RecordReadConsistencyQuery {
                consistency: Some(RecordReadConsistency::ReadYourWrites),
                min_lsn: Some(current_lsn),
            },
            after_key: None,
            after_cursor: None,
            limit: Some(limit),
            order: None,
            shard: None,
            predicate: Some(index_prefix_predicate(index, &values)),
        },
    )
    .await
}

fn index_prefix_predicate(
    index: &crate::schema::IndexSchema,
    values: &[serde_json::Value],
) -> RecordPredicate {
    RecordPredicate {
        all: index
            .fields
            .iter()
            .zip(values.iter())
            .map(|(field, value)| RecordPredicateTerm {
                field: field.clone(),
                op: RecordPredicateOp::Eq,
                value: Some(value.clone()),
            })
            .collect(),
    }
}

fn filter_visible_table_snapshot(
    mut response: ListRecordsResponse,
    schema: &DatabaseSchema,
    user_id: Option<&str>,
) -> ListRecordsResponse {
    let policy = record_read_visibility_policy(schema, &response.table);
    response
        .records
        .retain(|record| record_visible_to_user(&response.table, record, policy, user_id));
    response
}

#[allow(clippy::too_many_arguments)]
async fn send_subscription_catch_up(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    rooms: Vec<String>,
    users: Vec<String>,
    tables: Vec<String>,
    table_ranges: Vec<TableSubscription>,
    nested_tables: Vec<NestedTableSubscription>,
    objects: bool,
    user_id: Option<&str>,
    after_lsn: u64,
    limit: Option<usize>,
) -> Result<()> {
    let records = match read_records_from_wal_paths_after_lsn(&state.wal_paths, after_lsn) {
        Ok(records) => records,
        Err(err) => {
            let frame = ServerFrame::Error {
                message: format!("subscription catch-up failed: {err:#}"),
            };
            return send_server_frame(sender, &frame).await;
        }
    };
    let room_filter = rooms.iter().cloned().collect::<HashSet<_>>();
    let user_filter = users.iter().cloned().collect::<HashSet<_>>();
    let table_filter = tables.iter().cloned().collect::<HashSet<_>>();
    let table_range_filter = table_ranges.iter().cloned().collect::<BTreeSet<_>>();
    let nested_table_filter = nested_tables.iter().cloned().collect::<BTreeSet<_>>();
    let schema = state.schema.schema();
    let page = sync_events_from_wal_records(
        records,
        after_lsn,
        &room_filter,
        &user_filter,
        &table_filter,
        &table_range_filter,
        &nested_table_filter,
        Some(&schema),
        objects,
        normalize_limit(limit),
    );

    for event in filter_visible_catch_up_events(page.events, &schema, user_id) {
        let frame = ServerFrame::Event { event };
        send_server_frame(sender, &frame).await?;
    }

    let frame = ServerFrame::SubscriptionCatchUp {
        rooms,
        users,
        tables: subscription_catch_up_tables(tables, &table_ranges, &nested_tables),
        nested_tables,
        objects,
        next_after_lsn: page.next_after_lsn,
        current_lsn: state.current_lsn.load(Ordering::Acquire),
        has_more: page.has_more,
    };
    send_server_frame(sender, &frame).await
}

fn filter_visible_catch_up_events(
    events: Vec<DeliveryEvent>,
    schema: &DatabaseSchema,
    user_id: Option<&str>,
) -> Vec<DeliveryEvent> {
    events
        .into_iter()
        .filter(|event| record_event_visible_to_user(event, schema, user_id))
        .collect()
}

fn validate_table_subscription(
    subscription: &TableSubscription,
    schema: &DatabaseSchema,
) -> std::result::Result<(), ApiError> {
    let Some(index_name) = subscription.index_name.as_deref() else {
        if subscription.index_values.is_some() {
            return Err(ApiError::bad_request(
                "indexValues requires indexName for table subscriptions",
            ));
        }
        return Ok(());
    };
    let Some(index_values) = subscription.index_values.as_deref() else {
        return Err(ApiError::bad_request(
            "indexName requires indexValues for table subscriptions",
        ));
    };
    let Some(index) = table_subscription_index_schema(schema, &subscription.table, index_name)
    else {
        return Err(ApiError::bad_request(format!(
            "table subscription index {index_name} is not declared on {}",
            subscription.table
        )));
    };
    parse_index_prefix_values(index_values, index)?;
    Ok(())
}

fn table_subscription_index_schema<'a>(
    schema: &'a DatabaseSchema,
    table: &str,
    index_name: &str,
) -> Option<&'a crate::schema::IndexSchema> {
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

async fn send_record_query_result(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    subscription: &RecordQuerySubscription,
    force: bool,
    deleted_hints: Option<&RecordQueryDeletedHints>,
    cache_token: Option<LiveQueryEvaluationCacheToken>,
) -> std::result::Result<Option<RecordQuerySnapshot>, String> {
    let plan_key = subscription.plan_key();
    let evaluation =
        cached_record_query_evaluation(state, subscription, cache_token, &plan_key).await?;
    send_record_query_evaluation(
        state,
        sender,
        subscription,
        force,
        deleted_hints,
        &evaluation,
    )
    .await
}

async fn send_record_query_evaluation(
    state: &AppState,
    sender: &mut (impl RealtimeFrameSink + ?Sized),
    subscription: &RecordQuerySubscription,
    force: bool,
    deleted_hints: Option<&RecordQueryDeletedHints>,
    evaluation: &RecordQueryEvaluation,
) -> std::result::Result<Option<RecordQuerySnapshot>, String> {
    let Some(prepared) =
        prepare_record_query_evaluation(state, subscription, force, deleted_hints, evaluation)?
    else {
        return Ok(None);
    };
    send_server_frame(sender, &prepared.frame)
        .await
        .map_err(|err| err.to_string())?;
    if prepared.sent_diff {
        state.live_query_metrics.note_diff_frame();
    } else {
        state.live_query_metrics.note_result_frame();
    }
    Ok(Some(prepared.snapshot))
}

struct PreparedRecordQueryEvaluation {
    frame: ServerFrame,
    snapshot: RecordQuerySnapshot,
    sent_diff: bool,
}

fn prepare_record_query_evaluation(
    state: &AppState,
    subscription: &RecordQuerySubscription,
    force: bool,
    deleted_hints: Option<&RecordQueryDeletedHints>,
    evaluation: &RecordQueryEvaluation,
) -> std::result::Result<Option<PreparedRecordQueryEvaluation>, String> {
    let response = &evaluation.response;
    let result_id = &evaluation.result_id;
    if !force && subscription.last_result_id.as_deref() == Some(result_id.as_str()) {
        state.live_query_metrics.note_unchanged();
        return Ok(None);
    }
    ensure_live_query_result_size(
        subscription,
        response.records.len(),
        state.limits.max_live_query_result_rows,
    )?;
    let current_lsn = state.current_lsn.load(Ordering::Acquire);
    let mut sent_diff = false;
    let frame = if subscription.diff && !force {
        match subscription.last_response.as_ref() {
            Some(previous) => {
                sent_diff = true;
                ServerFrame::QueryDiff {
                    query_id: subscription.query_id.clone(),
                    diff: record_query_diff(previous, response, deleted_hints),
                    current_lsn,
                    result_id: result_id.clone(),
                }
            }
            None => ServerFrame::QueryResult {
                query_id: subscription.query_id.clone(),
                response: response.clone(),
                current_lsn,
                result_id: result_id.clone(),
            },
        }
    } else {
        ServerFrame::QueryResult {
            query_id: subscription.query_id.clone(),
            response: response.clone(),
            current_lsn,
            result_id: result_id.clone(),
        }
    };
    let snapshot = RecordQuerySnapshot {
        result_id: result_id.clone(),
        response: response.clone(),
    };
    Ok(Some(PreparedRecordQueryEvaluation {
        frame,
        snapshot,
        sent_diff,
    }))
}

fn ensure_live_query_result_size(
    subscription: &RecordQuerySubscription,
    row_count: usize,
    max_rows: usize,
) -> std::result::Result<(), String> {
    if max_rows == 0 || row_count <= max_rows {
        return Ok(());
    }
    Err(format!(
        "live query result too large: queryId={} table={} rows={} maxLiveQueryResultRows={}; narrow the query or lower limit",
        subscription.query_id, subscription.subscribed_table, row_count, max_rows
    ))
}

fn should_deliver_event(
    event: &DeliveryEvent,
    schema: &DatabaseSchema,
    connection_state: &RealtimeConnectionState,
    user_id: Option<&str>,
    session_id: &str,
) -> bool {
    if connection_state.subscribed_objects && event.is_object_event() {
        return true;
    }

    if event
        .room_id()
        .is_some_and(|room_id| connection_state.subscribed_rooms.contains(room_id))
    {
        return true;
    }

    let record_visible = record_event_visible_to_user(event, schema, user_id);

    if event
        .table()
        .is_some_and(|table| connection_state.subscribed_tables.contains(table) && record_visible)
    {
        return true;
    }

    if let Some((table, key)) = event.table().zip(event.record_key())
        && record_visible
        && record_matches_table_router(
            table,
            key,
            &HashSet::new(),
            &connection_state.record_subscription_router,
        )
    {
        return true;
    }

    if record_visible
        && connection_state
            .subscribed_table_ranges
            .iter()
            .any(|subscription| event_matches_table_subscription(event, schema, subscription))
    {
        return true;
    }

    let Some(target_user_id) = event.user_id() else {
        return false;
    };
    if !connection_state.subscribed_user_events {
        return false;
    }
    if Some(target_user_id) != user_id {
        return false;
    }

    event
        .target_session_ids()
        .is_none_or(|target_session_ids| target_session_ids.contains(session_id))
}

fn event_matches_table_subscription(
    event: &DeliveryEvent,
    schema: &DatabaseSchema,
    subscription: &TableSubscription,
) -> bool {
    match event {
        DeliveryEvent::RecordUpserted { table, key, record } => {
            record_matches_table_subscription(table, key, Some(record), schema, subscription)
        }
        DeliveryEvent::RecordDeleted {
            table,
            key,
            previous_record,
            ..
        } => record_matches_table_subscription(
            table,
            key,
            previous_record.as_ref(),
            schema,
            subscription,
        ),
        _ => false,
    }
}

fn record_event_visible_to_user(
    event: &DeliveryEvent,
    schema: &DatabaseSchema,
    user_id: Option<&str>,
) -> bool {
    match event {
        DeliveryEvent::RecordUpserted { table, record, .. } => record_visible_to_user(
            table,
            record,
            record_read_visibility_policy(schema, table),
            user_id,
        ),
        DeliveryEvent::RecordDeleted {
            table,
            previous_record,
            ..
        } => {
            let Some(policy) = record_read_visibility_policy(schema, table) else {
                return true;
            };
            if policy.is_public() {
                return true;
            }
            previous_record
                .as_ref()
                .is_some_and(|record| record_visible_to_user(table, record, Some(policy), user_id))
        }
        _ => true,
    }
}

fn record_visible_to_user(
    _table: &str,
    record: &DbRecord,
    policy: Option<&ReadVisibilityPolicy>,
    user_id: Option<&str>,
) -> bool {
    policy.is_none_or(|policy| policy.allows_value_for_user(&record.value, user_id))
}

fn record_read_visibility_policy<'a>(
    schema: &'a DatabaseSchema,
    table: &str,
) -> Option<&'a ReadVisibilityPolicy> {
    if let Some((parent, nested)) = table.split_once('.') {
        return schema
            .tables
            .get(parent)
            .and_then(|table| table.nested.get(nested))
            .map(|nested| &nested.read_visibility);
    }
    schema.tables.get(table).map(|table| &table.read_visibility)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        live_query::RecordQueryImpactFilter,
        schema::{
            FieldSchema, FieldType, IndexSchema, ReadVisibilityRule, StorageClass, TableSchema,
        },
    };

    #[test]
    fn table_subscription_applies_read_visibility_on_record_upserts() {
        let schema = schema_with_private_docs();
        let event = DeliveryEvent::RecordUpserted {
            table: "privateDocs".to_string(),
            key: "doc-a".to_string(),
            record: private_doc_record("doc-a", "user-a"),
        };
        let connection_state = connection_state_with_tables(["privateDocs"]);

        assert!(should_deliver_event(
            &event,
            &schema,
            &connection_state,
            Some("user-a"),
            "session-a",
        ));
        assert!(!should_deliver_event(
            &event,
            &schema,
            &connection_state,
            Some("user-b"),
            "session-b",
        ));
        assert!(!should_deliver_event(
            &event,
            &schema,
            &connection_state,
            None,
            "anonymous",
        ));
    }

    #[test]
    fn table_range_subscription_filters_record_keys() {
        let schema = DatabaseSchema::default_nextdb();
        let mut connection_state = RealtimeConnectionState::default();
        connection_state.add_table_subscription(TableSubscription::new(
            "rooms".to_string(),
            Some("room-010".to_string()),
            Some("room-020".to_string()),
        ));

        let visible_event = DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-010".to_string(),
            record: record("rooms", "room-010", 1),
        };
        let before_event = DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-009".to_string(),
            record: record("rooms", "room-009", 1),
        };
        let after_event = DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-020".to_string(),
            record: record("rooms", "room-020", 1),
        };

        assert!(should_deliver_event(
            &visible_event,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
        assert!(!should_deliver_event(
            &before_event,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
        assert!(!should_deliver_event(
            &after_event,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
    }

    #[test]
    fn table_index_prefix_subscription_filters_record_values() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .indexes
            .insert(
                "byTitleScore".to_string(),
                IndexSchema {
                    fields: vec!["title".to_string(), "score".to_string()],
                    unique: false,
                },
            );
        let mut connection_state = RealtimeConnectionState::default();
        connection_state.add_table_subscription(
            TableSubscription::new("rooms".to_string(), None, None).with_index_prefix(
                Some("byTitleScore".to_string()),
                Some(r#"["target"]"#.to_string()),
            ),
        );

        let mut visible_record = record("rooms", "room-a", 1);
        visible_record.value = serde_json::json!({ "id": "room-a", "title": "target", "score": 1 });
        let visible_event = DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: visible_record.clone(),
        };
        let mut hidden_record = record("rooms", "room-b", 2);
        hidden_record.value = serde_json::json!({ "id": "room-b", "title": "other", "score": 1 });
        let hidden_event = DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-b".to_string(),
            record: hidden_record,
        };
        let visible_delete = DeliveryEvent::RecordDeleted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            deleted_at_ms: 3,
            lsn: 3,
            path: "tables/rooms/room-a".to_string(),
            previous_record: Some(visible_record),
        };
        let unknown_delete = DeliveryEvent::RecordDeleted {
            table: "rooms".to_string(),
            key: "room-c".to_string(),
            deleted_at_ms: 4,
            lsn: 4,
            path: "tables/rooms/room-c".to_string(),
            previous_record: None,
        };

        assert!(should_deliver_event(
            &visible_event,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
        assert!(!should_deliver_event(
            &hidden_event,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
        assert!(should_deliver_event(
            &visible_delete,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
        assert!(!should_deliver_event(
            &unknown_delete,
            &schema,
            &connection_state,
            None,
            "session-a",
        ));
    }

    #[test]
    fn protected_record_deletes_use_previous_record_for_read_visibility() {
        let schema = schema_with_private_docs();
        let visible_event = DeliveryEvent::RecordDeleted {
            table: "privateDocs".to_string(),
            key: "doc-a".to_string(),
            deleted_at_ms: 1,
            lsn: 2,
            path: "tables/privateDocs/doc-a".to_string(),
            previous_record: Some(private_doc_record("doc-a", "user-a")),
        };
        let connection_state = connection_state_with_tables(["privateDocs"]);

        assert!(should_deliver_event(
            &visible_event,
            &schema,
            &connection_state,
            Some("user-a"),
            "session-a",
        ));
        assert!(!should_deliver_event(
            &visible_event,
            &schema,
            &connection_state,
            Some("user-b"),
            "session-b",
        ));

        let opaque_event = DeliveryEvent::RecordDeleted {
            table: "privateDocs".to_string(),
            key: "doc-a".to_string(),
            deleted_at_ms: 1,
            lsn: 2,
            path: "tables/privateDocs/doc-a".to_string(),
            previous_record: None,
        };
        assert!(!should_deliver_event(
            &opaque_event,
            &schema,
            &connection_state,
            Some("user-a"),
            "session-a",
        ));
    }

    #[test]
    fn nested_subscription_applies_read_visibility_on_record_upserts() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .read_visibility
            .all
            .push(ReadVisibilityRule::FieldEqualsUserId {
                field: "senderId".to_string(),
            });
        let event = DeliveryEvent::RecordUpserted {
            table: "rooms.messages".to_string(),
            key: "room-a:msg-a".to_string(),
            record: DbRecord {
                table: "rooms.messages".to_string(),
                key: "room-a:msg-a".to_string(),
                value: serde_json::json!({
                    "id": "msg-a",
                    "roomId": "room-a",
                    "senderId": "user-a",
                    "body": "hello",
                    "attachments": [],
                    "createdAtMs": 1,
                    "path": "tables/rooms/room-a/messages/msg-a",
                }),
                updated_at_ms: 1,
                lsn: 1,
                path: "tables/rooms/room-a/messages/msg-a".to_string(),
            },
        };
        let mut connection_state = RealtimeConnectionState::default();
        connection_state.add_nested_table_subscription(NestedTableSubscription::new(
            "rooms".to_string(),
            "room-a".to_string(),
            "messages".to_string(),
        ));

        assert!(should_deliver_event(
            &event,
            &schema,
            &connection_state,
            Some("user-a"),
            "session-a",
        ));
        assert!(!should_deliver_event(
            &event,
            &schema,
            &connection_state,
            Some("user-b"),
            "session-b",
        ));
    }

    #[test]
    fn catch_up_events_apply_read_visibility() {
        let schema = schema_with_private_docs();
        let events = vec![
            DeliveryEvent::RecordUpserted {
                table: "privateDocs".to_string(),
                key: "doc-a".to_string(),
                record: private_doc_record("doc-a", "user-a"),
            },
            DeliveryEvent::RecordUpserted {
                table: "privateDocs".to_string(),
                key: "doc-b".to_string(),
                record: private_doc_record("doc-b", "user-b"),
            },
            DeliveryEvent::RecordDeleted {
                table: "privateDocs".to_string(),
                key: "doc-a".to_string(),
                deleted_at_ms: 2,
                lsn: 3,
                path: "tables/privateDocs/doc-a".to_string(),
                previous_record: Some(private_doc_record("doc-a", "user-a")),
            },
            DeliveryEvent::RecordDeleted {
                table: "privateDocs".to_string(),
                key: "doc-c".to_string(),
                deleted_at_ms: 2,
                lsn: 3,
                path: "tables/privateDocs/doc-c".to_string(),
                previous_record: None,
            },
            DeliveryEvent::RecordUpserted {
                table: "rooms".to_string(),
                key: "room-a".to_string(),
                record: record("rooms", "room-a", 4),
            },
        ];

        let visible = filter_visible_catch_up_events(events, &schema, Some("user-a"));

        assert_eq!(
            visible
                .iter()
                .filter_map(DeliveryEvent::record_key)
                .collect::<Vec<_>>(),
            vec!["doc-a", "doc-a", "room-a"]
        );
    }

    #[test]
    fn table_snapshot_applies_read_visibility() {
        let schema = schema_with_private_docs();
        let response = ListRecordsResponse {
            table: "privateDocs".to_string(),
            records: vec![
                private_doc_record("doc-a", "user-a"),
                private_doc_record("doc-b", "user-b"),
            ],
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        };

        let visible = filter_visible_table_snapshot(response, &schema, Some("user-a"));

        assert_eq!(
            visible
                .records
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["doc-a"]
        );
    }

    #[test]
    fn nested_table_snapshot_applies_read_visibility() {
        let mut schema = DatabaseSchema::default_nextdb();
        schema
            .tables
            .get_mut("rooms")
            .expect("rooms table")
            .nested
            .get_mut("messages")
            .expect("messages nested table")
            .read_visibility
            .all
            .push(ReadVisibilityRule::FieldEqualsUserId {
                field: "senderId".to_string(),
            });
        let response = ListRecordsResponse {
            table: "rooms.messages".to_string(),
            records: vec![
                message_record("room-a", "msg-a", "user-a"),
                message_record("room-a", "msg-b", "user-b"),
            ],
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        };

        let visible = filter_visible_table_snapshot(response, &schema, Some("user-a"));

        assert_eq!(
            visible
                .records
                .iter()
                .map(|record| record.key.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a:msg-a"]
        );
    }

    #[test]
    fn query_scope_keys_include_result_record_buckets() {
        let subscription = query_subscription("rooms", None, None);
        let response = ListRecordsResponse {
            table: "rooms".to_string(),
            records: vec![record("rooms", "room-a", 1), record("rooms", "room-b", 2)],
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        };

        let scope_keys = query_scope_keys_for_response(&subscription, &response);

        assert_eq!(
            scope_keys,
            ["room-a", "room-b"]
                .into_iter()
                .map(|key| actor::record_actor_scope_key("rooms", key))
                .collect()
        );
    }

    #[test]
    fn nested_query_scope_keys_retain_empty_parent_scope() {
        let subscription = query_subscription(
            "rooms.messages",
            Some("room-a".to_string()),
            Some("messages".to_string()),
        );
        let response = ListRecordsResponse {
            table: "rooms.messages".to_string(),
            records: Vec::new(),
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        };

        let scope_keys = query_scope_keys_for_response(&subscription, &response);

        assert_eq!(
            scope_keys,
            [actor::record_actor_scope_key("rooms.messages", "room-a:")]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn table_subscription_scope_keys_cover_top_level_hash_buckets() {
        let subscription = TableSubscription::new("rooms".to_string(), None, None);

        let scope_keys = table_subscription_scope_keys(&subscription);

        assert_eq!(scope_keys.len(), actor::RECORD_ACTOR_SCOPE_BUCKET_COUNT);
        assert_eq!(
            scope_keys.first().map(String::as_str),
            Some("table:rooms/bucket:00")
        );
        assert_eq!(
            scope_keys.last().map(String::as_str),
            Some("table:rooms/bucket:ff")
        );
    }

    #[test]
    fn table_range_subscription_scope_keys_cover_top_level_hash_buckets() {
        let subscription = TableSubscription::new(
            "rooms".to_string(),
            Some("room-010".to_string()),
            Some("room-020".to_string()),
        );

        let scope_keys = table_subscription_scope_keys(&subscription);

        assert_eq!(scope_keys.len(), actor::RECORD_ACTOR_SCOPE_BUCKET_COUNT);
    }

    #[test]
    fn table_index_prefix_subscription_scope_keys_defer_to_result_residency() {
        let subscription = TableSubscription::new("rooms".to_string(), None, None)
            .with_index_prefix(
                Some("byTitle".to_string()),
                Some(r#"["target"]"#.to_string()),
            );

        let scope_keys = table_subscription_scope_keys(&subscription);

        assert!(scope_keys.is_empty());
    }

    #[test]
    fn live_query_result_size_guard_rejects_oversized_pages() {
        let subscription = query_subscription("rooms", None, None);

        assert!(ensure_live_query_result_size(&subscription, 500, 0).is_ok());
        assert!(ensure_live_query_result_size(&subscription, 250, 250).is_ok());
        let error = ensure_live_query_result_size(&subscription, 251, 250)
            .expect_err("oversized live query page should be rejected");

        assert!(error.contains("maxLiveQueryResultRows=250"));
        assert!(error.contains("queryId=query-a"));
    }

    fn query_subscription(
        subscribed_table: &str,
        parent_key: Option<String>,
        nested: Option<String>,
    ) -> RecordQuerySubscription {
        RecordQuerySubscription {
            query_id: "query-a".to_string(),
            table: subscribed_table
                .split_once('.')
                .map(|(table, _)| table)
                .unwrap_or(subscribed_table)
                .to_string(),
            parent_key: parent_key.clone(),
            nested,
            subscribed_table: subscribed_table.to_string(),
            parent_key_prefix: parent_key.as_deref().map(nested_record_prefix),
            index_name: None,
            index_query: QueryRecordsByIndexQuery {
                consistency: RecordReadConsistencyQuery::default(),
                value: None,
                values: None,
                lower: None,
                upper: None,
                lower_values: None,
                upper_values: None,
                after_key: None,
                after_cursor: None,
                limit: Some(20),
                shard: None,
                predicate: None,
            },
            impact_filter: RecordQueryImpactFilter::AllUpserts,
            schema_version: DatabaseSchema::default_nextdb().version,
            after_key: None,
            after_cursor: None,
            limit: Some(20),
            order: None,
            predicate: None,
            last_result_id: None,
            last_response: None,
            last_response_keys: HashSet::new(),
            retained_scope_keys: BTreeSet::new(),
            diff: false,
        }
    }

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

    fn schema_with_private_docs() -> DatabaseSchema {
        let mut schema = DatabaseSchema::default_nextdb();
        schema.tables.insert(
            "privateDocs".to_string(),
            TableSchema {
                storage: StorageClass::Disk,
                fields: BTreeMap::from([
                    (
                        "id".to_string(),
                        FieldSchema::required(FieldType::Id {
                            entity: "PrivateDoc".to_string(),
                        }),
                    ),
                    (
                        "ownerId".to_string(),
                        FieldSchema::required(FieldType::Id {
                            entity: "User".to_string(),
                        }),
                    ),
                ]),
                nested: BTreeMap::new(),
                read_visibility: crate::schema::ReadVisibilityPolicy {
                    all: vec![ReadVisibilityRule::FieldEqualsUserId {
                        field: "ownerId".to_string(),
                    }],
                },
                indexes: BTreeMap::new(),
            },
        );
        schema
    }

    fn private_doc_record(key: &str, owner_id: &str) -> DbRecord {
        DbRecord {
            table: "privateDocs".to_string(),
            key: key.to_string(),
            value: serde_json::json!({
                "id": key,
                "ownerId": owner_id,
            }),
            updated_at_ms: 1,
            lsn: 1,
            path: format!("tables/privateDocs/{key}"),
        }
    }

    fn message_record(room_id: &str, message_id: &str, sender_id: &str) -> DbRecord {
        let key = format!("{room_id}:{message_id}");
        DbRecord {
            table: "rooms.messages".to_string(),
            key: key.clone(),
            value: serde_json::json!({
                "id": message_id,
                "roomId": room_id,
                "senderId": sender_id,
                "body": "hello",
                "attachments": [],
                "createdAtMs": 1,
                "path": format!("tables/rooms/{room_id}/messages/{message_id}"),
            }),
            updated_at_ms: 1,
            lsn: 1,
            path: format!("tables/rooms/{room_id}/messages/{message_id}"),
        }
    }

    fn connection_state_with_tables<const N: usize>(tables: [&str; N]) -> RealtimeConnectionState {
        let mut state = RealtimeConnectionState::default();
        state
            .subscribed_tables
            .extend(tables.into_iter().map(str::to_string));
        state
    }
}
