use super::*;
use crate::record_store::{index_range_cursor, order_record_cursor};
use crate::{
    api::connections::handle_client_frame,
    api::frames::{ClientFrame, ServerFrame, TableSubscription},
    api::records::{
        ListRecordsResponse, QueryRecordsByIndexQuery, RecordPredicate, RecordReadConsistency,
        RecordReadConsistencyQuery, apply_record_transaction_operations,
        disk_window_for_hot_overlay, merge_index_range_records_with_hot_entries,
        merge_key_order_records, merge_key_order_records_matching_with_shadow_keys,
        merge_ordered_records_matching_with_shadow_keys, nested_record_key, nested_record_prefix,
        nested_record_table, record_key_set, resolve_record_read_consistency,
        split_matching_hot_records, split_matching_ordered_hot_records,
    },
    api::schema::SchemaReplayApplyPhase,
    api::sync::sync_events_from_wal_records,
    live_query::{
        LiveQueryEvaluationCacheKey, LiveQueryEvaluationCacheToken, RealtimeConnectionState,
        RecordQueryDeletedHints, RecordQueryEvaluation, RecordQueryImpactFilter,
        RecordQuerySnapshot, RecordQuerySubscription, affected_live_query_refresh_batch,
        hash_json_value, record_event_batch_cache_lsn, record_query_diff,
        record_query_impact_filter, record_query_matches_event,
    },
    model::{ClientMutationRecord, DbRecordDraft, DbRecordMutationDraft, DeliveryEvent},
    realtime::{
        EncodedServerFrame, RealtimeFrameRead, RealtimeFrameSink, RealtimeFrameSource,
        send_server_frame,
    },
    record_projection::RecordProjectionMutation,
    record_store::{IndexedDbRecord, OrderedDbRecord, RecordOrderTerm},
    schema::IndexSchema,
};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use tokio::io::BufReader;

struct MemoryFrameSink {
    frames: Vec<String>,
}

impl RealtimeFrameSink for MemoryFrameSink {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a ServerFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.frames.push(serde_json::to_string(frame)?);
            Ok(())
        })
    }

    fn send_encoded_frames<'a>(
        &'a mut self,
        frames: &'a [EncodedServerFrame],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            for frame in frames {
                self.frames
                    .push(String::from_utf8(frame.json().to_vec()).context("utf8 server frame")?);
            }
            Ok(())
        })
    }
}

struct MemoryFrameSource {
    reads: VecDeque<Result<RealtimeFrameRead>>,
}

impl RealtimeFrameSource for MemoryFrameSource {
    fn next_client_frame<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RealtimeFrameRead>> + Send + 'a>> {
        Box::pin(async move {
            self.reads
                .pop_front()
                .unwrap_or(Ok(RealtimeFrameRead::Closed))
        })
    }
}

fn test_temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "nextdb-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn test_app_state(root: PathBuf) -> AppState {
    let schema = SchemaRegistry::load(root.join("schema").join("nextdb.schema.json"))
        .await
        .expect("load test schema");
    let active_schema = schema.schema();
    let record_store = RecordStore::new(root.join("records"));
    let record_hot = RecordHotCache::from_schema_and_records(&active_schema, &[], 0);
    let record_projection_applier =
        RecordProjectionApplier::spawn(record_store.clone(), record_hot.clone());
    let object_refs = ObjectRefIndex::load(root.join("objects").join("refs.json"), &[], &[])
        .await
        .expect("load object refs");
    let behavior_root = root.join("behaviors");
    let behaviors = BehaviorRuntime::load_checked(behavior_root.clone(), |_| Ok(()))
        .await
        .expect("load behavior runtime");
    let export_backup_policy = ExportBackupPolicy::default();
    let (events, _) = broadcast::channel(64);
    let (cache_invalidations, _) = broadcast::channel(64);

    AppState {
        runtime_id: "test-runtime".to_string(),
        actors: ActorRuntime::new(HashMap::new(), 100, 100, 0),
        wal_shards: Vec::new(),
        wal_paths: Vec::new(),
        current_lsn: Arc::new(AtomicU64::new(0)),
        next_lsn: Arc::new(AtomicU64::new(0)),
        last_snapshot_lsn: Arc::new(AtomicU64::new(0)),
        last_compaction_lsn: Arc::new(AtomicU64::new(0)),
        checkpoint_in_flight: Arc::new(AtomicBool::new(false)),
        checkpoint_every_lsn: 0,
        auto_compact_wal: false,
        limits: RuntimeLimits::from_env(),
        admin_token: None,
        client_token: None,
        client_user_tokens: BTreeMap::new(),
        wal_replication_token: None,
        explicit_wal_remote_replica_urls: Some(Vec::new()),
        explicit_object_remote_replica_urls: Some(Vec::new()),
        object_replication_token: None,
        object_gc_grace_ms: 0,
        export_root: root.join("exports"),
        export_backup_runs: Arc::new(RwLock::new(Vec::new())),
        export_backup_runs_path: root.join("exports").join("backup-runs.json"),
        export_backup_policy: Arc::new(RwLock::new(export_backup_policy.clone())),
        export_backup_policy_path: root.join("exports").join("backup-policy.json"),
        export_backup_controller: Arc::new(RwLock::new(ExportBackupControllerState {
            enabled: false,
            interval_ms: export_backup_policy.interval_ms,
            last_run_at_ms: None,
            last_run_id: None,
            last_error: None,
        })),
        object_repair_controller: Arc::new(RwLock::new(ObjectRepairControllerState {
            enabled: false,
            interval_ms: 0,
            last_run_at_ms: None,
            last_shards: Vec::new(),
            last_objects_sent: 0,
            last_repaired_replicas: 0,
            last_satisfied: true,
            last_error: None,
        })),
        chat_log: ChatLog::new(root.join("chat-log")),
        records: record_store,
        record_projection_applier,
        record_hot,
        record_hot_durable_idle_ttl_ms: 0,
        record_hot_maintenance_interval_ms: 0,
        record_hot_prewarm_limit: 0,
        record_hot_prewarm: Arc::new(RwLock::new(RecordHotPrewarmStatus::default())),
        projection_rebuild_status: Arc::new(RwLock::new(ProjectionRebuildStatus::default())),
        hot_room_maintenance_interval_ms: 0,
        actor_scope_residency_maintenance_interval_ms: 0,
        actor_scope_residency_maintenance_limit: 256,
        actor_split_maintenance_interval_ms: 0,
        actor_split_maintenance_limit: 64,
        actor_reminder_maintenance_interval_ms: 0,
        actor_reminder_maintenance_limit: 64,
        realtime_maintenance_interval_ms: 0,
        realtime_event_batch_max: 1,
        objects: ObjectStore::new(root.join("objects")),
        object_refs,
        users: UserProjection::default(),
        client_mutations: Arc::new(StdRwLock::new(BTreeMap::new())),
        host_http_requests: Arc::new(StdRwLock::new(BTreeMap::new())),
        actor_reminders: Arc::new(StdRwLock::new(BTreeMap::new())),
        snapshots: SnapshotStore::new(root.join("snapshots").join("actors.json")),
        behaviors,
        behavior_root,
        schema,
        schema_apply_lock: Arc::new(Mutex::new(())),
        schema_replay_apply_status: Arc::new(RwLock::new(SchemaReplayApplyStatus::default())),
        schema_replay_apply_status_path: root.join("schema").join("schema-replay-status.json"),
        schema_proposals: Arc::new(RwLock::new(BTreeMap::new())),
        schema_proposals_path: root.join("schema").join("proposals.json"),
        cache_control: Arc::new(RwLock::new(ClientCacheControl::default_with_env())),
        cache_control_path: root.join("cache").join("control.json"),
        connections: ConnectionRegistry::default(),
        connection_controls: broadcast::channel(64).0,
        connection_events: broadcast::channel(64).0,
        realtime: RealtimeChannels::default(),
        realtime_fanout: crate::realtime_fanout::RealtimeFanoutRegistry::default(),
        aggregates: crate::aggregate::AggregateRegistry::default(),
        cluster: ClusterConfig::from_env(1),
        topology_overrides: Arc::new(RwLock::new(BTreeMap::new())),
        topology_overrides_path: root.join("cluster").join("topology-overrides.json"),
        topology_log_path: root.join("cluster").join("topology-log.jsonl"),
        topology_proposals: Arc::new(RwLock::new(BTreeMap::new())),
        topology_proposals_path: root.join("cluster").join("topology-proposals.json"),
        topology_lease: Arc::new(RwLock::new(TopologyLease {
            current_term: 0,
            holder_node_id: None,
            proposal_id: None,
            expires_at_ms: None,
        })),
        topology_lease_path: root.join("cluster").join("topology-lease.json"),
        topology_lease_ms: 0,
        shard_controls: Arc::new(RwLock::new(BTreeMap::new())),
        runtime_drain: Arc::new(RwLock::new(RuntimeDrainState {
            draining: false,
            reason: None,
            updated_at_ms: None,
        })),
        runtime_writes: RuntimeWriteTracker::new(),
        live_query_metrics: LiveQueryMetrics::new(),
        live_query_evaluation_cache: Arc::new(Mutex::new(LiveQueryEvaluationCache::default())),
        runtime_write_gate: Arc::new(RwLock::new(())),
        handoff_workflows: Arc::new(RwLock::new(BTreeMap::new())),
        handoff_workflows_path: root.join("cluster").join("handoff-workflows.json"),
        handoff_controller: Arc::new(RwLock::new(HandoffControllerState {
            enabled: false,
            interval_ms: 0,
            last_run_at_ms: None,
            last_workflow_id: None,
            last_applied_workflow_id: None,
            last_error: None,
        })),
        failover_controller: Arc::new(RwLock::new(FailoverControllerState {
            enabled: false,
            interval_ms: 0,
            last_run_at_ms: None,
            last_shard: None,
            last_proposal_id: None,
            last_committed_proposal_id: None,
            last_error: None,
        })),
        wal_repair_controller: Arc::new(RwLock::new(WalRepairControllerState {
            enabled: false,
            interval_ms: 0,
            last_run_at_ms: None,
            last_shards: Vec::new(),
            last_records_sent: 0,
            last_repaired_replicas: 0,
            last_satisfied: true,
            last_error: None,
        })),
        peer_health: Arc::new(RwLock::new(PeerHealthMonitorState {
            enabled: false,
            interval_ms: 0,
            last_run_at_ms: None,
            peers: BTreeMap::new(),
        })),
        startup_recovery: StartupRecoveryReport {
            snapshot_loaded: false,
            snapshot_lsn: 0,
            snapshot_schema_version: None,
            snapshot_room_count: 0,
            snapshot_record_hot_table_count: 0,
            snapshot_record_hot_record_count: 0,
            schema_wal_recovery: SchemaWalRecoveryReport {
                recovered: false,
                latest_lsn: None,
                latest_version: None,
                history_versions: Vec::new(),
            },
            wal_restores: Vec::new(),
            wal_replay: Vec::new(),
            wal_records_scanned: 0,
            wal_records_after_snapshot: 0,
            highest_lsn: 0,
            rebuilt_messages: 0,
            rebuilt_records: 0,
            rebuilt_object_refs: 0,
        },
        events,
        cache_invalidations,
    }
}

#[tokio::test]
async fn schema_replay_cancel_marks_running_status_cancelled() {
    let root = test_temp_root("schema-replay-cancel");
    let state = test_app_state(root.clone()).await;
    let schema = state.schema.schema();
    {
        let mut status = state.schema_replay_apply_status.write().await;
        *status = SchemaReplayApplyStatus {
            phase: SchemaReplayApplyPhase::Running,
            run_id: Some("cancel-test-run".to_string()),
            resumed_from_run_id: None,
            target_version: Some(schema.version),
            expected_version: Some(schema.version.saturating_sub(1)),
            schema: Some(schema),
            allow_breaking_replay: true,
            replay_rebuild: true,
            projection_rebuild: true,
            resume_eligible: false,
            resume_reason: None,
            started_at_ms: Some(1),
            finished_at_ms: None,
            schema_audit_lsn: None,
            projection_status: None,
            error: None,
        };
    }

    let axum::Json(cancelled) =
        crate::api::schema::cancel_schema_replay_apply(axum::extract::State(state.clone()))
            .await
            .expect("cancel schema replay");
    assert_eq!(cancelled.phase, SchemaReplayApplyPhase::Cancelled);
    assert!(cancelled.finished_at_ms.is_some());
    assert_eq!(
        cancelled.error.as_deref(),
        Some("cancelled by operator before SchemaApplied commit")
    );

    let persisted: SchemaReplayApplyStatus = serde_json::from_slice(
        &tokio::fs::read(root.join("schema").join("schema-replay-status.json"))
            .await
            .expect("read persisted status"),
    )
    .expect("parse persisted status");
    assert_eq!(persisted.phase, SchemaReplayApplyPhase::Cancelled);

    let err = crate::api::schema::cancel_schema_replay_apply(axum::extract::State(state))
        .await
        .expect_err("second cancel should conflict");
    assert_eq!(
        err.message,
        "schema replay cancel requires a running replay status"
    );
}

#[tokio::test]
async fn realtime_frame_sink_accepts_non_websocket_sender() {
    let mut sink = MemoryFrameSink { frames: Vec::new() };
    send_server_frame(
        &mut sink,
        &ServerFrame::Hello {
            user_id: Some("alice".to_string()),
            session_id: "session-a".to_string(),
        },
    )
    .await
    .unwrap();

    assert_eq!(sink.frames.len(), 1);
    let frame: serde_json::Value = serde_json::from_str(&sink.frames[0]).unwrap();
    assert_eq!(frame["type"], "hello");
    assert_eq!(frame["userId"], "alice");
    assert_eq!(frame["sessionId"], "session-a");
}

#[tokio::test]
async fn realtime_frame_source_accepts_non_websocket_receiver() {
    let mut source = MemoryFrameSource {
        reads: VecDeque::from([
            Ok(RealtimeFrameRead::Frame(Box::new(
                ClientFrame::SubscribeObjects {
                    after_lsn: Some(42),
                    catch_up_limit: Some(8),
                },
            ))),
            Ok(RealtimeFrameRead::Closed),
        ]),
    };

    match source.next_client_frame().await.unwrap() {
        RealtimeFrameRead::Frame(frame) => match *frame {
            ClientFrame::SubscribeObjects {
                after_lsn,
                catch_up_limit,
            } => {
                assert_eq!(after_lsn, Some(42));
                assert_eq!(catch_up_limit, Some(8));
            }
            _ => panic!("unexpected frame read"),
        },
        _ => panic!("unexpected frame read"),
    }
    assert!(matches!(
        source.next_client_frame().await.unwrap(),
        RealtimeFrameRead::Closed
    ));
}

#[tokio::test]
async fn aggregate_count_hydrates_projection_and_tracks_record_events() {
    let root = test_temp_root("aggregate-count");
    let state = test_app_state(root.clone()).await;
    state
        .records
        .upsert_with_indexes_and_order(&room_record("room-a", "Alpha", 1), &BTreeMap::new(), None)
        .await
        .expect("upsert room-a");
    state
        .records
        .upsert_with_indexes_and_order(&room_record("room-b", "Beta", 2), &BTreeMap::new(), None)
        .await
        .expect("upsert room-b");

    let snapshot = state
        .aggregates
        .table_count_snapshot(&state.records, "rooms", 2)
        .await
        .expect("hydrate aggregate count");
    assert_eq!(snapshot.count, 2);

    let mut updates = state.aggregates.subscribe();
    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-c".to_string(),
            record: room_record("room-c", "Gamma", 3),
        }]);
    let update = updates.recv().await.expect("aggregate upsert update");
    let crate::aggregate::AggregateUpdate::Count(update) = update else {
        panic!("expected count update");
    };
    assert_eq!(update.table, "rooms");
    assert_eq!(update.count, 3);
    assert_eq!(update.lsn, 3);

    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-c".to_string(),
            record: room_record("room-c", "Gamma 2", 4),
        }]);
    assert!(updates.try_recv().is_err());

    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordDeleted {
            table: "rooms".to_string(),
            key: "room-b".to_string(),
            deleted_at_ms: 5,
            lsn: 5,
            path: "tables/rooms/room-b".to_string(),
            previous_record: None,
        }]);
    let update = updates.recv().await.expect("aggregate delete update");
    let crate::aggregate::AggregateUpdate::Count(update) = update else {
        panic!("expected count update");
    };
    assert_eq!(update.table, "rooms");
    assert_eq!(update.count, 2);
    assert_eq!(update.lsn, 5);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn aggregate_sum_hydrates_projection_and_tracks_record_events() {
    let root = test_temp_root("aggregate-sum");
    let state = test_app_state(root.clone()).await;
    state
        .records
        .upsert_with_indexes_and_order(
            &room_record_with_score("room-a", "Alpha", 10.5, 1),
            &BTreeMap::new(),
            None,
        )
        .await
        .expect("upsert room-a");
    state
        .records
        .upsert_with_indexes_and_order(
            &room_record_with_score("room-b", "Beta", 4.5, 2),
            &BTreeMap::new(),
            None,
        )
        .await
        .expect("upsert room-b");

    let snapshot = state
        .aggregates
        .table_sum_snapshot(&state.records, "rooms", "score", 2)
        .await
        .expect("hydrate aggregate sum");
    assert_eq!(snapshot.table, "rooms");
    assert_eq!(snapshot.field, "score");
    assert_eq!(snapshot.sum, 15.0);

    let mut updates = state.aggregates.subscribe();
    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-c".to_string(),
            record: room_record_with_score("room-c", "Gamma", 2.0, 3),
        }]);
    let update = next_sum_update(&mut updates).await;
    assert_eq!(update.table, "rooms");
    assert_eq!(update.field, "score");
    assert_eq!(update.sum, 17.0);
    assert_eq!(update.lsn, 3);

    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-c".to_string(),
            record: room_record("room-c", "Gamma no score", 4),
        }]);
    let update = next_sum_update(&mut updates).await;
    assert_eq!(update.sum, 15.0);
    assert_eq!(update.lsn, 4);

    state
        .aggregates
        .apply_delivery_events(&[DeliveryEvent::RecordDeleted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            deleted_at_ms: 5,
            lsn: 5,
            path: "tables/rooms/room-a".to_string(),
            previous_record: None,
        }]);
    let update = next_sum_update(&mut updates).await;
    assert_eq!(update.sum, 4.5);
    assert_eq!(update.lsn, 5);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn aggregate_count_subscription_returns_snapshot_frame() {
    let root = test_temp_root("aggregate-count-subscribe");
    let state = test_app_state(root.clone()).await;
    state
        .records
        .upsert_with_indexes_and_order(&room_record("room-a", "Alpha", 7), &BTreeMap::new(), None)
        .await
        .expect("upsert room-a");
    state.current_lsn.store(7, Ordering::Release);

    let mut sink = MemoryFrameSink { frames: Vec::new() };
    let mut connection_state = RealtimeConnectionState::default();
    let keep_open = handle_client_frame(
        &state,
        &mut sink,
        &mut connection_state,
        "session-a",
        None,
        false,
        ClientFrame::SubscribeAggregateCount {
            table: "rooms".to_string(),
        },
    )
    .await;
    assert!(keep_open);
    assert!(
        connection_state
            .subscribed_aggregate_counts
            .contains("rooms")
    );
    assert_eq!(sink.frames.len(), 1);
    let frame: serde_json::Value = serde_json::from_str(&sink.frames[0]).expect("frame json");
    assert_eq!(frame["type"], "aggregateCountSubscribed");
    assert_eq!(frame["snapshot"]["table"], "rooms");
    assert_eq!(frame["snapshot"]["count"], 1);
    assert_eq!(frame["snapshot"]["currentLsn"], 7);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn aggregate_sum_subscription_returns_snapshot_frame() {
    let root = test_temp_root("aggregate-sum-subscribe");
    let state = test_app_state(root.clone()).await;
    state
        .records
        .upsert_with_indexes_and_order(
            &room_record_with_score("room-a", "Alpha", 7.25, 7),
            &BTreeMap::new(),
            None,
        )
        .await
        .expect("upsert room-a");
    state.current_lsn.store(7, Ordering::Release);

    let mut sink = MemoryFrameSink { frames: Vec::new() };
    let mut connection_state = RealtimeConnectionState::default();
    let keep_open = handle_client_frame(
        &state,
        &mut sink,
        &mut connection_state,
        "session-a",
        None,
        false,
        ClientFrame::SubscribeAggregateSum {
            table: "rooms".to_string(),
            field: "score".to_string(),
        },
    )
    .await;
    assert!(keep_open);
    assert!(connection_state.subscribed_aggregate_sums.contains(
        &crate::aggregate::AggregateSumKey::new("rooms".to_string(), "score".to_string())
    ));
    assert_eq!(sink.frames.len(), 1);
    let frame: serde_json::Value = serde_json::from_str(&sink.frames[0]).expect("frame json");
    assert_eq!(frame["type"], "aggregateSumSubscribed");
    assert_eq!(frame["snapshot"]["table"], "rooms");
    assert_eq!(frame["snapshot"]["field"], "score");
    assert_eq!(frame["snapshot"]["sum"], 7.25);
    assert_eq!(frame["snapshot"]["currentLsn"], 7);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn aggregate_presence_snapshot_and_update_track_channel_members() {
    let state = test_app_state(test_temp_root("aggregate-presence"));
    let state = state.await;
    let members = vec![
        realtime_member("alice", Some("session-a")),
        realtime_member("alice", Some("session-b")),
        realtime_member("bob", Some("session-c")),
    ];
    let snapshot = state
        .aggregates
        .channel_presence_snapshot("lobby", &members, 9, 10);
    assert_eq!(snapshot.channel_id, "lobby");
    assert_eq!(snapshot.member_count, 3);
    assert_eq!(snapshot.user_count, 2);
    assert_eq!(snapshot.current_lsn, 9);
    assert_eq!(snapshot.updated_at_ms, 10);

    let mut updates = state.aggregates.subscribe();
    state
        .aggregates
        .publish_presence_update("lobby", &members[..2], 11, 12);
    let update = next_presence_update(&mut updates).await;
    assert_eq!(update.channel_id, "lobby");
    assert_eq!(update.member_count, 2);
    assert_eq!(update.user_count, 1);
    assert_eq!(update.current_lsn, 11);
    assert_eq!(update.updated_at_ms, 12);
}

#[tokio::test]
async fn aggregate_presence_subscription_returns_snapshot_frame() {
    let root = test_temp_root("aggregate-presence-subscribe");
    let state = test_app_state(root.clone()).await;
    state.current_lsn.store(15, Ordering::Release);
    state
        .realtime
        .join(
            "lobby".to_string(),
            "alice".to_string(),
            Some("session-a".to_string()),
            serde_json::json!({"role": "host"}),
        )
        .await;
    state
        .realtime
        .join(
            "lobby".to_string(),
            "alice".to_string(),
            Some("session-b".to_string()),
            serde_json::json!({"role": "viewer"}),
        )
        .await;

    let mut sink = MemoryFrameSink { frames: Vec::new() };
    let mut connection_state = RealtimeConnectionState::default();
    let keep_open = handle_client_frame(
        &state,
        &mut sink,
        &mut connection_state,
        "session-a",
        Some("alice"),
        false,
        ClientFrame::SubscribeAggregatePresence {
            channel_id: "lobby".to_string(),
        },
    )
    .await;
    assert!(keep_open);
    assert!(
        connection_state
            .subscribed_aggregate_presence
            .contains("lobby")
    );
    assert_eq!(sink.frames.len(), 1);
    let frame: serde_json::Value = serde_json::from_str(&sink.frames[0]).expect("frame json");
    assert_eq!(frame["type"], "aggregatePresenceSubscribed");
    assert_eq!(frame["snapshot"]["channelId"], "lobby");
    assert_eq!(frame["snapshot"]["memberCount"], 2);
    assert_eq!(frame["snapshot"]["userCount"], 1);
    assert_eq!(frame["snapshot"]["currentLsn"], 15);
    assert!(frame["snapshot"]["updatedAtMs"].as_u64().is_some());

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn json_line_frame_source_decodes_client_frames() {
    let input = concat!(
        "{\"type\":\"subscribeObjects\",\"afterLsn\":42,\"catchUpLimit\":8}\n",
        "\n",
        "not-json\n"
    );
    let mut source = JsonLineFrameSource::new(BufReader::new(input.as_bytes()));

    match source.next_client_frame().await.unwrap() {
        RealtimeFrameRead::Frame(frame) => match *frame {
            ClientFrame::SubscribeObjects {
                after_lsn,
                catch_up_limit,
            } => {
                assert_eq!(after_lsn, Some(42));
                assert_eq!(catch_up_limit, Some(8));
            }
            _ => panic!("unexpected first JSONL frame"),
        },
        _ => panic!("unexpected first JSONL frame"),
    }
    assert!(matches!(
        source.next_client_frame().await.unwrap(),
        RealtimeFrameRead::Ignored
    ));
    match source.next_client_frame().await.unwrap() {
        RealtimeFrameRead::Invalid { message } => {
            assert!(message.starts_with("invalid frame:"));
        }
        _ => panic!("unexpected invalid JSONL frame"),
    }
    assert!(matches!(
        source.next_client_frame().await.unwrap(),
        RealtimeFrameRead::Closed
    ));
}

#[tokio::test]
async fn json_line_frame_sink_writes_server_frames() {
    let mut output = Vec::new();
    {
        let mut sink = JsonLineFrameSink::new(&mut output);
        send_server_frame(&mut sink, &ServerFrame::ObjectsSubscribed)
            .await
            .unwrap();
        send_server_frame(&mut sink, &ServerFrame::ConnectionEventsUnsubscribed)
            .await
            .unwrap();
    }

    assert_eq!(
        String::from_utf8(output).unwrap(),
        "{\"type\":\"objectsSubscribed\"}\n{\"type\":\"connectionEventsUnsubscribed\"}\n"
    );
}

#[test]
fn realtime_frame_codec_round_trips_json_contract() {
    let decoded = decode_client_frame(
        r#"{"type":"subscribeRoom","roomId":"room-a","afterLsn":12,"catchUpLimit":4}"#,
    )
    .unwrap();
    match decoded {
        ClientFrame::SubscribeRoom {
            room_id,
            after_lsn,
            catch_up_limit,
        } => {
            assert_eq!(room_id, "room-a");
            assert_eq!(after_lsn, Some(12));
            assert_eq!(catch_up_limit, Some(4));
        }
        other => panic!("unexpected frame: {other:?}"),
    }

    let line = encode_server_frame_json_line(&ServerFrame::Subscribed {
        room_id: "room-a".to_string(),
    })
    .unwrap();
    assert_eq!(
        line,
        r#"{"type":"subscribed","roomId":"room-a"}"#.to_string() + "\n"
    );

    let line = encode_server_frame_json_line(&ServerFrame::Events {
        events: vec![DeliveryEvent::MessageCreated {
            room_id: "room-a".to_string(),
            message: Message {
                id: "message-a".to_string(),
                room_id: "room-a".to_string(),
                sender_id: "user-a".to_string(),
                body: "hello".to_string(),
                attachments: Vec::new(),
                created_at_ms: 1,
                lsn: 2,
                path: "rooms/room-a/messages/message-a".to_string(),
            },
        }],
    })
    .unwrap();
    let frame: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(frame["type"], "events");
    assert_eq!(frame["events"][0]["type"], "messageCreated");
    assert_eq!(frame["events"][0]["message"]["lsn"], 2);
}

#[test]
fn realtime_event_drain_expands_internal_batches() {
    let event = |id: &str, lsn: u64| DeliveryEvent::MessageCreated {
        room_id: "room-a".to_string(),
        message: Message {
            id: id.to_string(),
            room_id: "room-a".to_string(),
            sender_id: "user-a".to_string(),
            body: id.to_string(),
            attachments: Vec::new(),
            created_at_ms: lsn,
            lsn,
            path: format!("rooms/room-a/messages/{id}"),
        },
    };
    let (tx, mut rx) = broadcast::channel::<DeliveryEventBatch>(8);
    tx.send(vec![event("m3", 3)]).unwrap();

    let (events, lagged) =
        drain_realtime_event_batch(&mut rx, vec![event("m1", 1), event("m2", 2)], 8);

    assert_eq!(lagged, None);
    assert_eq!(
        events
            .iter()
            .filter_map(|event| event.room_id())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-a", "room-a"]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| match event {
                DeliveryEvent::MessageCreated { message, .. } => message.lsn,
                _ => 0,
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn realtime_connection_state_indexes_query_subscriptions_by_table() {
    let mut state = RealtimeConnectionState::default();
    state.add_query_subscription(
        "rooms-query".to_string(),
        indexed_room_query_subscription("Target", Vec::new()),
    );
    state.add_query_subscription(
        "rooms-other-query".to_string(),
        indexed_room_query_subscription("Other", Vec::new()),
    );
    state.add_query_subscription(
        "messages-query".to_string(),
        nested_message_query_subscription("room-a"),
    );
    assert_eq!(
        state.subscribed_query_table_counts(),
        BTreeMap::from([("rooms".to_string(), 2), ("rooms.messages".to_string(), 1)])
    );

    let schema_version = DatabaseSchema::default_nextdb().version;
    assert_eq!(
        state.affected_query_ids_for_event(
            schema_version,
            &DeliveryEvent::RecordUpserted {
                table: "rooms".to_string(),
                key: "room-a".to_string(),
                record: room_record("room-a", "Target", 1),
            },
            record_query_matches_event,
        ),
        vec!["rooms-query".to_string()]
    );
    assert_eq!(
        state.affected_query_ids_for_event(
            schema_version,
            &DeliveryEvent::RecordUpserted {
                table: "rooms.messages".to_string(),
                key: nested_record_key("room-a", "message-a"),
                record: message_record("room-a", "message-a", 2),
            },
            record_query_matches_event,
        ),
        vec!["messages-query".to_string()]
    );

    state.remove_query_subscription("rooms-query");
    assert!(
        state
            .affected_query_ids_for_event(
                schema_version,
                &DeliveryEvent::RecordUpserted {
                    table: "rooms".to_string(),
                    key: "room-a".to_string(),
                    record: room_record("room-a", "Target", 3),
                },
                record_query_matches_event,
            )
            .is_empty()
    );
    assert!(state.subscribed_query_ids_by_table.contains_key("rooms"));
    state.remove_query_subscription("rooms-other-query");
    assert!(!state.subscribed_query_ids_by_table.contains_key("rooms"));
}

#[test]
fn realtime_connection_state_take_put_preserves_query_indexes() {
    let mut state = RealtimeConnectionState::default();
    state.add_query_subscription(
        "rooms-query".to_string(),
        indexed_room_query_subscription("Target", Vec::new()),
    );
    let schema_version = DatabaseSchema::default_nextdb().version;
    let event = DeliveryEvent::RecordUpserted {
        table: "rooms".to_string(),
        key: "room-a".to_string(),
        record: room_record("room-a", "Target", 1),
    };
    assert_eq!(
        state.affected_query_ids_for_event(schema_version, &event, record_query_matches_event),
        vec!["rooms-query".to_string()]
    );

    let subscription = state.take_query_subscription("rooms-query").unwrap();
    assert!(
        state
            .affected_query_ids_for_event(schema_version, &event, record_query_matches_event)
            .is_empty()
    );
    assert!(state.subscribed_query_ids.contains("rooms-query"));
    assert!(state.subscribed_query_ids_by_table.contains_key("rooms"));

    state.put_query_subscription("rooms-query".to_string(), subscription);
    assert_eq!(
        state.affected_query_ids_for_event(schema_version, &event, record_query_matches_event),
        vec!["rooms-query".to_string()]
    );
}

#[test]
fn realtime_connection_state_replaces_query_subscription_index() {
    let mut state = RealtimeConnectionState::default();
    state.add_query_subscription(
        "replace-query".to_string(),
        indexed_room_query_subscription("Target", Vec::new()),
    );
    state.add_query_subscription(
        "replace-query".to_string(),
        nested_message_query_subscription("room-a"),
    );

    assert!(!state.subscribed_query_ids_by_table.contains_key("rooms"));
    assert_eq!(
        state.subscribed_query_table_counts(),
        BTreeMap::from([("rooms.messages".to_string(), 1)])
    );
}

#[test]
fn realtime_connection_state_reports_query_subscription_limit_errors() {
    let mut state = RealtimeConnectionState::default();
    state.add_query_subscription(
        "rooms-query-1".to_string(),
        indexed_room_query_subscription("Target", Vec::new()),
    );
    state.add_query_subscription(
        "rooms-query-2".to_string(),
        indexed_room_query_subscription("Other", Vec::new()),
    );
    let per_table_limits = RuntimeLimits {
        max_object_bytes: 0,
        max_message_bytes: 0,
        max_user_event_bytes: 0,
        max_record_value_bytes: 0,
        max_live_queries_per_connection: 0,
        max_live_queries_per_table_per_connection: 2,
        max_live_queries_per_user: 0,
        max_live_query_result_rows: 250,
    };
    assert!(
        state
            .query_subscription_limit_error("rooms-query-3", "rooms", &per_table_limits)
            .is_some_and(|message| message.contains("maxLiveQueriesPerTablePerConnection=2"))
    );
    assert!(
        state
            .query_subscription_limit_error("rooms-query-1", "rooms", &per_table_limits)
            .is_none()
    );
    assert!(
        state
            .query_subscription_limit_error("messages-query-1", "rooms.messages", &per_table_limits)
            .is_none()
    );

    let per_connection_limits = RuntimeLimits {
        max_live_queries_per_connection: 2,
        max_live_queries_per_table_per_connection: 0,
        ..per_table_limits
    };
    assert!(
        state
            .query_subscription_limit_error(
                "messages-query-1",
                "rooms.messages",
                &per_connection_limits
            )
            .is_some_and(|message| message.contains("maxLiveQueriesPerConnection=2"))
    );
}

#[test]
fn record_query_diff_reports_added_updated_removed_and_keys() {
    let previous = ListRecordsResponse {
        table: "rooms".to_string(),
        records: vec![
            room_record("room-a", "Old", 1),
            room_record("room-b", "Deleted", 2),
        ],
        next_after_key: Some("room-b".to_string()),
        next_cursor: None,
        has_more: false,
    };
    let next = ListRecordsResponse {
        table: "rooms".to_string(),
        records: vec![
            room_record("room-a", "New", 3),
            room_record("room-c", "Added", 4),
        ],
        next_after_key: Some("room-c".to_string()),
        next_cursor: Some("cursor-c".to_string()),
        has_more: true,
    };

    let mut deleted_hints = RecordQueryDeletedHints::default();
    deleted_hints.add_event(&DeliveryEvent::RecordDeleted {
        table: "rooms".to_string(),
        key: "room-b".to_string(),
        deleted_at_ms: 6,
        lsn: 5,
        path: "tables/rooms/room-b".to_string(),
        previous_record: None,
    });

    let diff = record_query_diff(&previous, &next, deleted_hints.as_ref());

    assert_eq!(
        diff.added
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-c"]
    );
    assert_eq!(
        diff.updated
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a"]
    );
    assert_eq!(diff.removed.len(), 1);
    assert_eq!(diff.removed[0].key, "room-b");
    assert!(diff.removed[0].deleted);
    assert_eq!(diff.removed[0].lsn, Some(5));
    assert_eq!(diff.removed[0].deleted_at_ms, Some(6));
    assert_eq!(diff.keys, vec!["room-a".to_string(), "room-c".to_string()]);
    assert_eq!(diff.next_after_key, Some("room-c".to_string()));
    assert_eq!(diff.next_cursor, Some("cursor-c".to_string()));
    assert!(diff.has_more);
}

#[test]
fn record_query_diff_reports_multiple_deleted_hints() {
    let previous = ListRecordsResponse {
        table: "rooms".to_string(),
        records: vec![
            room_record("room-a", "Deleted A", 1),
            room_record("room-b", "Deleted B", 2),
        ],
        next_after_key: None,
        next_cursor: None,
        has_more: false,
    };
    let next = ListRecordsResponse {
        table: "rooms".to_string(),
        records: Vec::new(),
        next_after_key: None,
        next_cursor: None,
        has_more: false,
    };
    let mut deleted_hints = RecordQueryDeletedHints::default();
    deleted_hints.add_event(&DeliveryEvent::RecordDeleted {
        table: "rooms".to_string(),
        key: "room-a".to_string(),
        deleted_at_ms: 10,
        lsn: 11,
        path: "tables/rooms/room-a".to_string(),
        previous_record: None,
    });
    deleted_hints.add_event(&DeliveryEvent::RecordDeleted {
        table: "rooms".to_string(),
        key: "room-b".to_string(),
        deleted_at_ms: 20,
        lsn: 21,
        path: "tables/rooms/room-b".to_string(),
        previous_record: None,
    });

    let diff = record_query_diff(&previous, &next, deleted_hints.as_ref());

    assert_eq!(diff.removed.len(), 2);
    assert!(diff.removed.iter().all(|record| record.deleted));
    assert_eq!(diff.removed[0].lsn, Some(11));
    assert_eq!(diff.removed[0].deleted_at_ms, Some(10));
    assert_eq!(diff.removed[1].lsn, Some(21));
    assert_eq!(diff.removed[1].deleted_at_ms, Some(20));
}

#[test]
fn merge_key_order_records_preserves_sorted_order_and_prefers_hot_records() {
    let disk_records = vec![
        room_record("room-a", "Disk A", 1),
        room_record("room-b", "Disk B", 2),
        room_record("room-d", "Disk D", 4),
    ];
    let hot_records = vec![
        room_record("room-b", "Hot B", 20),
        room_record("room-c", "Hot C", 30),
    ];

    let merged = merge_key_order_records(disk_records, hot_records, 4);

    assert_eq!(
        merged
            .iter()
            .map(|record| (record.key.as_str(), record.value["title"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        vec![
            ("room-a", "Disk A"),
            ("room-b", "Hot B"),
            ("room-c", "Hot C"),
            ("room-d", "Disk D")
        ]
    );
}

#[tokio::test]
async fn replicated_record_upsert_uses_projection_applier() {
    let root = test_temp_root("replicated-record-upsert");
    let state = test_app_state(root.clone()).await;
    let wal_record = mutation_wal_record(
        7,
        WalPayload::RecordUpserted {
            record: DbRecordDraft {
                table: "rooms".to_string(),
                key: "room-replicated".to_string(),
                value: serde_json::json!({ "id": "room-replicated", "title": "Replicated" }),
                updated_at_ms: 7,
                path: "tables/rooms/room-replicated".to_string(),
                client_mutation_id: None,
            },
        },
    );

    let projection_lsn = apply_replicated_wal_record(&state, wal_record, None)
        .await
        .expect("apply replicated WAL record");

    assert_eq!(projection_lsn, Some(7));
    assert_eq!(
        state
            .record_hot
            .get("rooms", "room-replicated")
            .await
            .flatten()
            .expect("hot replicated record")
            .value["title"],
        "Replicated"
    );

    wait_for_replicated_record_projection(&state, projection_lsn)
        .await
        .expect("projection catches up");
    assert_eq!(
        state
            .records
            .get("rooms", "room-replicated")
            .await
            .expect("read projected record")
            .expect("projected replicated record")
            .value["title"],
        "Replicated"
    );

    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn record_projection_applier_applies_upsert_after_enqueue() {
    let root = std::env::temp_dir().join(format!(
        "nextdb-record-projection-applier-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = RecordStore::new(root.clone());
    let record_hot =
        RecordHotCache::from_schema_and_records(&DatabaseSchema::default_nextdb(), &[], 0);
    let applier = RecordProjectionApplier::spawn(store.clone(), record_hot);
    let record = room_record("room-applier", "Async Projection", 42);

    assert!(
        store
            .get("rooms", "room-applier")
            .await
            .expect("read before projection")
            .is_none()
    );

    applier
        .enqueue_upsert(record.lsn, record.clone(), BTreeMap::new(), None)
        .await
        .expect("enqueue projection upsert");
    applier
        .wait_for_lsn(record.lsn)
        .await
        .expect("projection applier catches up");

    let stored = store
        .get("rooms", "room-applier")
        .await
        .expect("read projected record")
        .expect("projected record exists");
    assert_eq!(stored.value["title"], "Async Projection");

    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn record_apply_returns_before_projection_applier_catches_up() {
    let root = test_temp_root("record-apply-paused-projection");
    let mut state = test_app_state(root.clone()).await;
    state.record_projection_applier = RecordProjectionApplier::paused_for_test();
    let lsn = 77;
    let record = DbRecordDraft {
        table: "rooms".to_string(),
        key: "room-paused-projection".to_string(),
        value: serde_json::json!({
            "id": "room-paused-projection",
            "title": "Visible From Hot State",
        }),
        updated_at_ms: lsn,
        path: "tables/rooms/room-paused-projection".to_string(),
        client_mutation_id: None,
    };

    let responses = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        apply_record_transaction_operations(
            &state,
            vec![DbRecordMutationDraft::Upsert { record }],
            lsn,
            false,
        ),
    )
    .await
    .expect("record apply must not wait for the projection applier")
    .expect("apply record transaction operations");

    assert_eq!(responses.len(), 1);
    assert_eq!(
        state
            .record_hot
            .get("rooms", "room-paused-projection")
            .await
            .flatten()
            .expect("hot record is visible before projection catches up")
            .value["title"],
        "Visible From Hot State"
    );
    assert!(
        state
            .records
            .get("rooms", "room-paused-projection")
            .await
            .expect("read cold projection")
            .is_none()
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            state.record_projection_applier.wait_for_lsn(lsn)
        )
        .await
        .is_err()
    );

    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn eventual_record_read_consistency_does_not_wait_for_projection() {
    let root = test_temp_root("eventual-record-read-consistency");
    let mut state = test_app_state(root.clone()).await;
    state.record_projection_applier = RecordProjectionApplier::paused_for_test();
    state.current_lsn.store(99, Ordering::Release);

    tokio::time::timeout(
        std::time::Duration::from_millis(20),
        resolve_record_read_consistency(
            &state,
            &RecordReadConsistencyQuery {
                consistency: Some(RecordReadConsistency::Eventual),
                min_lsn: Some(99),
            },
        ),
    )
    .await
    .expect("eventual consistency must not wait")
    .expect("eventual consistency succeeds");

    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn read_your_writes_record_consistency_requires_min_lsn() {
    let root = test_temp_root("ryw-record-consistency-requires-min-lsn");
    let state = test_app_state(root.clone()).await;

    let error = resolve_record_read_consistency(
        &state,
        &RecordReadConsistencyQuery {
            consistency: Some(RecordReadConsistency::ReadYourWrites),
            min_lsn: None,
        },
    )
    .await
    .expect_err("read-your-writes without minLsn should fail");

    assert!(error.message.contains("requires minLsn"));
    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn read_your_writes_record_consistency_waits_for_projection() {
    let root = test_temp_root("ryw-record-consistency-waits");
    let mut state = test_app_state(root.clone()).await;
    state.record_projection_applier = RecordProjectionApplier::paused_for_test();
    state.current_lsn.store(99, Ordering::Release);

    let error = resolve_record_read_consistency(
        &state,
        &RecordReadConsistencyQuery {
            consistency: Some(RecordReadConsistency::ReadYourWrites),
            min_lsn: Some(99),
        },
    )
    .await
    .expect_err("read-your-writes should wait for projection and time out");

    assert!(error.message.contains("did not catch up to LSN 99"));
    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn record_projection_applier_applies_delete_after_enqueue() {
    let root = std::env::temp_dir().join(format!(
        "nextdb-record-projection-delete-applier-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = RecordStore::new(root.clone());
    let record_hot =
        RecordHotCache::from_schema_and_records(&DatabaseSchema::default_nextdb(), &[], 0);
    let applier = RecordProjectionApplier::spawn(store.clone(), record_hot.clone());
    let record = room_record("room-delete-applier", "Async Delete Projection", 41);
    store
        .upsert_with_indexes_and_order(&record, &BTreeMap::new(), None)
        .await
        .expect("seed projected record");
    record_hot
        .delete_durable("rooms", "room-delete-applier", 42)
        .await;

    assert!(
        record_hot
            .durable_delete_lsn("rooms", "room-delete-applier")
            .await
            .flatten()
            .is_some()
    );

    applier
        .enqueue_delete(42, "rooms".to_string(), "room-delete-applier".to_string())
        .await
        .expect("enqueue projection delete");
    applier
        .wait_for_lsn(42)
        .await
        .expect("projection applier catches up");

    assert!(
        store
            .get("rooms", "room-delete-applier")
            .await
            .expect("read projected record")
            .is_none()
    );
    assert!(
        record_hot
            .durable_delete_lsn("rooms", "room-delete-applier")
            .await
            .flatten()
            .is_none()
    );

    let _ = fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn record_projection_applier_applies_transaction_after_enqueue() {
    let root = std::env::temp_dir().join(format!(
        "nextdb-record-projection-transaction-applier-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = RecordStore::new(root.clone());
    let record_hot =
        RecordHotCache::from_schema_and_records(&DatabaseSchema::default_nextdb(), &[], 0);
    let applier = RecordProjectionApplier::spawn(store.clone(), record_hot.clone());
    let deleted = room_record("room-transaction-delete", "Delete Me", 1);
    let inserted = room_record("room-transaction-insert", "Insert Me", 9);
    store
        .upsert_with_indexes_and_order(&deleted, &BTreeMap::new(), None)
        .await
        .expect("seed projected record");
    record_hot.delete_durable("rooms", &deleted.key, 9).await;

    applier
        .enqueue_transaction(
            9,
            vec![
                RecordProjectionMutation::Delete {
                    table: "rooms".to_string(),
                    key: deleted.key.clone(),
                },
                RecordProjectionMutation::Upsert {
                    record: inserted.clone(),
                    indexes: BTreeMap::new(),
                    order: None,
                },
            ],
        )
        .await
        .expect("enqueue projection transaction");
    applier
        .wait_for_lsn(9)
        .await
        .expect("projection applier catches up");

    assert!(
        store
            .get("rooms", &deleted.key)
            .await
            .expect("read deleted record")
            .is_none()
    );
    assert_eq!(
        store
            .get("rooms", &inserted.key)
            .await
            .expect("read inserted record")
            .expect("inserted record exists")
            .value["title"],
        "Insert Me"
    );
    assert!(
        record_hot
            .durable_delete_lsn("rooms", &deleted.key)
            .await
            .flatten()
            .is_none()
    );

    let _ = fs::remove_dir_all(root).await;
}

#[test]
fn merge_key_order_records_stops_at_limit() {
    let disk_records = vec![
        room_record("room-a", "Disk A", 1),
        room_record("room-b", "Disk B", 2),
        room_record("room-d", "Disk D", 4),
    ];
    let hot_records = vec![
        room_record("room-b", "Hot B", 20),
        room_record("room-c", "Hot C", 30),
    ];

    let merged = merge_key_order_records(disk_records, hot_records, 3);

    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c"]
    );
    assert_eq!(merged[1].value["title"], "Hot B");
}

fn merge_key_order_records_matching<F>(
    disk_records: Vec<DbRecord>,
    hot_records: Vec<DbRecord>,
    limit: usize,
    matches: F,
) -> (Vec<DbRecord>, bool)
where
    F: FnMut(&DbRecord) -> bool,
{
    let shadow_keys = record_key_set(&hot_records);
    merge_key_order_records_matching_with_shadow_keys(
        disk_records,
        hot_records,
        &shadow_keys,
        limit,
        matches,
    )
}

#[test]
fn merge_key_order_records_matching_uses_hot_record_as_current_state() {
    let disk_records = vec![
        room_record("room-b", "Target", 2),
        room_record("room-c", "Target", 3),
    ];
    let hot_records = vec![
        room_record("room-a", "Target", 10),
        room_record("room-b", "Other", 20),
    ];

    let (merged, has_more) =
        merge_key_order_records_matching(disk_records, hot_records, 10, |record| {
            record.value["title"] == "Target"
        });

    assert!(!has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-c"]
    );
}

#[test]
fn merge_key_order_records_matching_shadow_keys_hide_stale_disk_rows() {
    let disk_records = vec![
        room_record("room-a", "Target", 1),
        room_record("room-b", "Target", 2),
        room_record("room-c", "Target", 3),
    ];
    let hot_records = vec![room_record("room-a", "Target", 10)];
    let shadow_keys = BTreeSet::from(["room-a".to_string(), "room-b".to_string()]);

    let (merged, has_more) = merge_key_order_records_matching_with_shadow_keys(
        disk_records,
        hot_records,
        &shadow_keys,
        10,
        |record| record.value["title"] == "Target",
    );

    assert!(!has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-c"]
    );
}

#[test]
fn split_matching_hot_records_preserves_shadow_keys_for_nonmatches() {
    let hot_records = vec![
        room_record("room-a", "Target", 10),
        room_record("room-b", "Other", 20),
    ];

    let (matching, shadow_keys) =
        split_matching_hot_records(hot_records, |record| record.value["title"] == "Target");

    assert_eq!(
        matching
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a"]
    );
    assert_eq!(
        shadow_keys.into_iter().collect::<Vec<_>>(),
        vec!["room-a".to_string(), "room-b".to_string()]
    );
}

#[test]
fn merge_key_order_records_matching_reports_has_more_after_limit() {
    let disk_records = vec![
        room_record("room-a", "Target", 1),
        room_record("room-c", "Target", 3),
        room_record("room-e", "Target", 5),
    ];
    let hot_records = vec![
        room_record("room-b", "Target", 2),
        room_record("room-d", "Target", 4),
    ];

    let (merged, has_more) =
        merge_key_order_records_matching(disk_records, hot_records, 3, |record| {
            record.value["title"] == "Target"
        });

    assert!(has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c"]
    );
}

#[test]
fn disk_window_for_hot_overlay_keeps_room_for_shadowed_disk_rows() {
    assert_eq!(disk_window_for_hot_overlay(20, 0), 21);
    assert_eq!(disk_window_for_hot_overlay(20, 3), 24);
}

fn merge_ordered_records_matching<F>(
    disk_records: Vec<OrderedDbRecord>,
    hot_records: Vec<OrderedDbRecord>,
    limit: usize,
    matches: F,
) -> (Vec<OrderedDbRecord>, bool)
where
    F: FnMut(&DbRecord) -> bool,
{
    let shadow_keys = hot_records
        .iter()
        .map(|record| record.record.key.clone())
        .collect::<BTreeSet<_>>();
    merge_ordered_records_matching_with_shadow_keys(
        disk_records,
        hot_records,
        &shadow_keys,
        limit,
        matches,
    )
}

#[test]
fn merge_ordered_records_prefers_hot_current_state_for_duplicate_keys() {
    let order = parse_record_order_terms(&["title".to_string()]).unwrap();
    let disk_records = vec![
        ordered_room_record("room-a", "A", 1, &order),
        ordered_room_record("room-b", "B", 2, &order),
    ];
    let hot_records = vec![
        ordered_room_record("room-b", "C", 20, &order),
        ordered_room_record("room-c", "D", 30, &order),
    ];

    let (merged, has_more) =
        merge_ordered_records_matching(disk_records, hot_records, 10, |_| true);

    assert!(!has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| (
                record.record.key.as_str(),
                record.record.value["title"].as_str().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("room-a", "A"), ("room-b", "C"), ("room-c", "D")]
    );
}

#[test]
fn merge_ordered_records_interleaves_disk_and_hot_by_cursor() {
    let order = parse_record_order_terms(&["title".to_string()]).unwrap();
    let disk_records = vec![
        ordered_room_record("room-a", "A", 1, &order),
        ordered_room_record("room-c", "C", 3, &order),
    ];
    let hot_records = vec![
        ordered_room_record("room-b", "B", 2, &order),
        ordered_room_record("room-d", "D", 4, &order),
    ];

    let (merged, has_more) =
        merge_ordered_records_matching(disk_records, hot_records, 10, |_| true);

    assert!(!has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c", "room-d"]
    );
}

#[test]
fn merge_ordered_records_shadow_keys_hide_stale_disk_rows_outside_hot_page() {
    let order = parse_record_order_terms(&["title".to_string()]).unwrap();
    let disk_records = vec![
        ordered_room_record("room-a", "A", 1, &order),
        ordered_room_record("room-b", "B", 2, &order),
        ordered_room_record("room-c", "C", 3, &order),
    ];
    let hot_records = Vec::new();
    let shadow_keys = BTreeSet::from(["room-b".to_string()]);

    let (merged, has_more) = merge_ordered_records_matching_with_shadow_keys(
        disk_records,
        hot_records,
        &shadow_keys,
        10,
        |_| true,
    );

    assert!(!has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-c"]
    );
}

#[test]
fn split_matching_ordered_hot_records_preserves_order_and_filters_nonmatches() {
    let order = parse_record_order_terms(&["title".to_string()]).unwrap();
    let hot_records = vec![
        ordered_room_record("room-a", "A", 1, &order),
        ordered_room_record("room-b", "B", 2, &order),
        ordered_room_record("room-c", "C", 3, &order),
    ];

    let matching =
        split_matching_ordered_hot_records(hot_records, |record| record.value["title"] != "B");

    assert_eq!(
        matching
            .iter()
            .map(|record| record.record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-c"]
    );
}

#[test]
fn merge_ordered_records_reports_has_more_after_limit() {
    let order = parse_record_order_terms(&["title".to_string()]).unwrap();
    let disk_records = vec![
        ordered_room_record("room-a", "A", 1, &order),
        ordered_room_record("room-c", "C", 3, &order),
        ordered_room_record("room-e", "E", 5, &order),
    ];
    let hot_records = vec![ordered_room_record("room-b", "B", 2, &order)];

    let (merged, has_more) = merge_ordered_records_matching(disk_records, hot_records, 3, |_| true);

    assert!(has_more);
    assert_eq!(
        merged
            .iter()
            .map(|record| record.record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c"]
    );
}

#[test]
fn merge_index_range_records_prefers_hot_current_state_for_duplicate_keys() {
    let index = IndexSchema {
        fields: vec!["title".to_string()],
        unique: false,
    };
    let hot_record = room_record("room-a", "Target", 10);
    let hot_values = vec![serde_json::json!("Target")];
    let hot_cursor = index_range_cursor(&hot_values, &hot_record.key).unwrap();
    let hot_entries = vec![(hot_values, hot_record.key.clone(), hot_cursor, hot_record)];
    let hot_keys = BTreeSet::from(["room-a".to_string(), "room-b".to_string()]);
    let disk_records = vec![
        indexed_room_record("room-b", "Target", 2),
        indexed_room_record("room-c", "Target", 3),
    ];

    let (merged, next_cursor, has_more) = merge_index_range_records_with_hot_entries(
        disk_records,
        hot_keys,
        hot_entries,
        &index,
        10,
        |_| true,
    )
    .unwrap();

    assert!(!has_more);
    assert!(next_cursor.is_none());
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-c"]
    );
}

#[test]
fn merge_index_range_records_interleaves_hot_and_disk_entries() {
    let index = IndexSchema {
        fields: vec!["title".to_string()],
        unique: false,
    };
    let disk_records = vec![
        indexed_room_record("room-a", "A", 1),
        indexed_room_record("room-c", "C", 3),
    ];
    let hot_b = room_record("room-b", "B", 2);
    let hot_d = room_record("room-d", "D", 4);
    let hot_b_values = vec![serde_json::json!("B")];
    let hot_d_values = vec![serde_json::json!("D")];
    let hot_entries = vec![
        (
            hot_d_values.clone(),
            hot_d.key.clone(),
            index_range_cursor(&hot_d_values, &hot_d.key).unwrap(),
            hot_d,
        ),
        (
            hot_b_values.clone(),
            hot_b.key.clone(),
            index_range_cursor(&hot_b_values, &hot_b.key).unwrap(),
            hot_b,
        ),
    ];

    let (merged, next_cursor, has_more) = merge_index_range_records_with_hot_entries(
        disk_records,
        BTreeSet::from(["room-b".to_string(), "room-d".to_string()]),
        hot_entries,
        &index,
        10,
        |_| true,
    )
    .unwrap();

    assert!(!has_more);
    assert!(next_cursor.is_none());
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c", "room-d"]
    );
}

#[test]
fn merge_index_range_records_reports_has_more_after_limit() {
    let index = IndexSchema {
        fields: vec!["title".to_string()],
        unique: false,
    };
    let disk_records = vec![
        indexed_room_record("room-a", "Target", 1),
        indexed_room_record("room-c", "Target", 3),
        indexed_room_record("room-e", "Target", 5),
    ];
    let hot_record = room_record("room-b", "Target", 2);
    let hot_values = vec![serde_json::json!("Target")];
    let hot_cursor = index_range_cursor(&hot_values, &hot_record.key).unwrap();
    let hot_entries = vec![(hot_values, hot_record.key.clone(), hot_cursor, hot_record)];

    let (merged, next_cursor, has_more) = merge_index_range_records_with_hot_entries(
        disk_records,
        BTreeSet::from(["room-b".to_string()]),
        hot_entries,
        &index,
        3,
        |_| true,
    )
    .unwrap();

    assert!(has_more);
    assert!(next_cursor.is_some());
    assert_eq!(
        merged
            .iter()
            .map(|record| record.key.as_str())
            .collect::<Vec<_>>(),
        vec!["room-a", "room-b", "room-c"]
    );
}

#[test]
fn live_query_refresh_batch_deduplicates_query_ids() {
    let schema = DatabaseSchema::default_nextdb();
    let mut state = RealtimeConnectionState::default();
    state.add_query_subscription(
        "rooms-target".to_string(),
        indexed_room_query_subscription("Target", vec![room_record("room-a", "Target", 1)]),
    );
    let events = vec![
        DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: room_record("room-a", "Other", 2),
        },
        DeliveryEvent::RecordDeleted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            deleted_at_ms: 30,
            lsn: 31,
            path: "tables/rooms/room-a".to_string(),
            previous_record: None,
        },
    ];

    let (affected_query_ids, refresh_candidates, deleted_hints) =
        affected_live_query_refresh_batch(&state, schema.version, &events);

    assert_eq!(refresh_candidates, 2);
    assert_eq!(
        affected_query_ids.into_iter().collect::<Vec<_>>(),
        vec!["rooms-target".to_string()]
    );
    let hint = deleted_hints.get("rooms", "room-a").unwrap();
    assert_eq!(hint.lsn, 31);
    assert_eq!(hint.deleted_at_ms, 30);
}

#[test]
fn live_query_plan_key_groups_identical_query_shapes() {
    let mut first = indexed_room_query_subscription("Target", Vec::new());
    first.query_id = "rooms-target-result".to_string();
    first.diff = false;
    first.last_result_id = Some("sha256:first".to_string());

    let mut second = indexed_room_query_subscription("Target", Vec::new());
    second.query_id = "rooms-target-diff".to_string();
    second.diff = true;
    second.last_result_id = Some("sha256:second".to_string());

    assert_eq!(first.plan_key(), second.plan_key());

    second.index_query.value = Some("Other".to_string());
    assert_ne!(first.plan_key(), second.plan_key());

    second.index_query.value = Some("Target".to_string());
    second.index_query.after_cursor = Some("cursor".to_string());
    assert_ne!(first.plan_key(), second.plan_key());
}

#[test]
fn record_event_batch_cache_lsn_accepts_volatile_record_events() {
    assert_eq!(
        record_event_batch_cache_lsn(
            &[
                DeliveryEvent::RecordUpserted {
                    table: "rooms".to_string(),
                    key: "room-a".to_string(),
                    record: room_record("room-a", "Target", 7),
                },
                DeliveryEvent::RecordDeleted {
                    table: "rooms".to_string(),
                    key: "room-b".to_string(),
                    deleted_at_ms: 8,
                    lsn: 9,
                    path: "tables/rooms/room-b".to_string(),
                    previous_record: None,
                },
            ],
            10,
        ),
        Some(10)
    );
    assert_eq!(
        record_event_batch_cache_lsn(
            &[DeliveryEvent::RecordUpserted {
                table: "rooms".to_string(),
                key: "room-a".to_string(),
                record: room_record("room-a", "Volatile", 0),
            }],
            10,
        ),
        Some(10)
    );
    assert_eq!(
        record_event_batch_cache_lsn(
            &[DeliveryEvent::VolatileRoomEvent {
                room_id: "room-a".to_string(),
                name: "typing".to_string(),
                payload: serde_json::json!({ "user": "alice" }),
            }],
            10,
        ),
        None
    );
}

#[test]
fn live_query_evaluation_cache_prunes_oldest_entries() {
    let subscription = indexed_room_query_subscription("Target", Vec::new());
    let mut cache = LiveQueryEvaluationCache::default();
    for lsn in 1..=(live_query::LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES as u64 + 1) {
        cache.insert(
            LiveQueryEvaluationCacheKey {
                token: LiveQueryEvaluationCacheToken {
                    lsn,
                    volatile_generation: lsn * 2,
                },
                plan_key: subscription.plan_key(),
            },
            RecordQueryEvaluation {
                response: ListRecordsResponse {
                    table: "rooms".to_string(),
                    records: vec![room_record(&format!("room-{lsn}"), "Target", lsn)],
                    next_after_key: None,
                    next_cursor: None,
                    has_more: false,
                },
                result_id: format!("sha256:{lsn}"),
            },
        );
    }

    assert_eq!(
        cache.entries.len(),
        live_query::LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES
    );
    assert!(
        cache
            .get(&LiveQueryEvaluationCacheKey {
                token: LiveQueryEvaluationCacheToken {
                    lsn: 1,
                    volatile_generation: 2,
                },
                plan_key: subscription.plan_key(),
            })
            .is_none()
    );
    assert!(
        cache
            .get(&LiveQueryEvaluationCacheKey {
                token: LiveQueryEvaluationCacheToken {
                    lsn: live_query::LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES as u64 + 1,
                    volatile_generation: (live_query::LIVE_QUERY_EVALUATION_CACHE_MAX_ENTRIES
                        as u64
                        + 1)
                        * 2,
                },
                plan_key: subscription.plan_key(),
            })
            .is_some()
    );
}

#[test]
fn live_query_evaluation_cache_distinguishes_volatile_generation() {
    let subscription = indexed_room_query_subscription("Target", Vec::new());
    let mut cache = LiveQueryEvaluationCache::default();
    cache.insert(
        LiveQueryEvaluationCacheKey {
            token: LiveQueryEvaluationCacheToken {
                lsn: 10,
                volatile_generation: 1,
            },
            plan_key: subscription.plan_key(),
        },
        RecordQueryEvaluation {
            response: ListRecordsResponse {
                table: "rooms".to_string(),
                records: vec![room_record("room-a", "Target", 10)],
                next_after_key: None,
                next_cursor: None,
                has_more: false,
            },
            result_id: "sha256:generation-1".to_string(),
        },
    );

    assert!(
        cache
            .get(&LiveQueryEvaluationCacheKey {
                token: LiveQueryEvaluationCacheToken {
                    lsn: 10,
                    volatile_generation: 1,
                },
                plan_key: subscription.plan_key(),
            })
            .is_some()
    );
    assert!(
        cache
            .get(&LiveQueryEvaluationCacheKey {
                token: LiveQueryEvaluationCacheToken {
                    lsn: 10,
                    volatile_generation: 2,
                },
                plan_key: subscription.plan_key(),
            })
            .is_none()
    );
}

#[test]
fn record_query_subscription_snapshot_rebuilds_page_key_set() {
    let mut subscription = indexed_room_query_subscription("Target", Vec::new());
    assert!(!subscription.last_response_contains_key("room-a"));

    subscription.apply_snapshot(RecordQuerySnapshot {
        result_id: "sha256:one".to_string(),
        response: ListRecordsResponse {
            table: "rooms".to_string(),
            records: vec![room_record("room-a", "Target", 1)],
            next_after_key: Some("room-a".to_string()),
            next_cursor: None,
            has_more: false,
        },
    });
    assert!(subscription.last_response_contains_key("room-a"));

    subscription.apply_snapshot(RecordQuerySnapshot {
        result_id: "sha256:two".to_string(),
        response: ListRecordsResponse {
            table: "rooms".to_string(),
            records: vec![room_record("room-b", "Target", 2)],
            next_after_key: Some("room-b".to_string()),
            next_cursor: None,
            has_more: false,
        },
    });
    assert!(!subscription.last_response_contains_key("room-a"));
    assert!(subscription.last_response_contains_key("room-b"));
    assert_eq!(subscription.last_result_id.as_deref(), Some("sha256:two"));
}

#[test]
fn non_diff_query_snapshot_keeps_key_set_without_full_response() {
    let mut subscription = indexed_room_query_subscription("Target", Vec::new());
    subscription.diff = false;

    subscription.apply_snapshot(RecordQuerySnapshot {
        result_id: "sha256:one".to_string(),
        response: ListRecordsResponse {
            table: "rooms".to_string(),
            records: vec![room_record("room-a", "Target", 1)],
            next_after_key: Some("room-a".to_string()),
            next_cursor: None,
            has_more: false,
        },
    });

    assert_eq!(subscription.last_result_id.as_deref(), Some("sha256:one"));
    assert!(subscription.last_response_contains_key("room-a"));
    assert!(subscription.last_response.is_none());
}

#[test]
fn hash_json_value_matches_allocating_json_encoding() {
    let value = serde_json::json!({
        "id": "room-a",
        "title": "Streaming Hash",
        "count": 42,
        "tags": ["a", "b"],
    });
    let encoded = serde_json::to_vec(&value).unwrap();
    let expected = hex_lower(&Sha256::digest(&encoded));

    let mut hasher = Sha256::new();
    hash_json_value(&mut hasher, &value);

    assert_eq!(hex_lower(&hasher.finalize()), expected);
}

#[test]
fn exact_index_query_skips_non_matching_upsert_refresh() {
    let schema = DatabaseSchema::default_nextdb();
    let subscription = indexed_room_query_subscription("Target", Vec::new());

    assert!(!record_query_matches_event(
        schema.version,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: room_record("room-a", "Other", 1),
        }
    ));
    assert!(record_query_matches_event(
        schema.version,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-b".to_string(),
            record: room_record("room-b", "Target", 2),
        }
    ));
}

#[test]
fn exact_index_query_refreshes_when_previous_page_key_changes_out() {
    let schema = DatabaseSchema::default_nextdb();
    let subscription =
        indexed_room_query_subscription("Target", vec![room_record("room-a", "Target", 1)]);

    assert!(record_query_matches_event(
        schema.version,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: room_record("room-a", "Other", 2),
        }
    ));
}

#[test]
fn exact_index_query_refreshes_after_schema_version_change() {
    let schema = DatabaseSchema::default_nextdb();
    let subscription = indexed_room_query_subscription("Target", Vec::new());

    assert!(record_query_matches_event(
        schema.version + 1,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: room_record("room-a", "Other", 1),
        }
    ));
}

#[test]
fn predicate_query_skips_non_matching_upsert_refresh() {
    let schema = DatabaseSchema::default_nextdb();
    let predicate = RecordPredicate {
        all: vec![RecordPredicateTerm {
            field: "title".to_string(),
            op: RecordPredicateOp::Eq,
            value: Some(serde_json::json!("Target")),
        }],
    };
    let index_query = QueryRecordsByIndexQuery {
        consistency: Default::default(),
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
    };
    let subscription = RecordQuerySubscription {
        query_id: "rooms-target".to_string(),
        table: "rooms".to_string(),
        parent_key: None,
        nested: None,
        subscribed_table: "rooms".to_string(),
        parent_key_prefix: None,
        index_name: None,
        index_query,
        impact_filter: RecordQueryImpactFilter::Predicate {
            predicate: predicate.clone(),
        },
        schema_version: schema.version,
        after_key: None,
        after_cursor: None,
        limit: Some(20),
        order: None,
        predicate: Some(predicate),
        last_result_id: None,
        last_response: Some(ListRecordsResponse {
            table: "rooms".to_string(),
            records: Vec::new(),
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        }),
        last_response_keys: HashSet::new(),
        retained_scope_keys: BTreeSet::new(),
        diff: true,
    };

    assert!(!record_query_matches_event(
        schema.version,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-a".to_string(),
            record: room_record("room-a", "Other", 1),
        }
    ));
    assert!(record_query_matches_event(
        schema.version,
        &subscription,
        &DeliveryEvent::RecordUpserted {
            table: "rooms".to_string(),
            key: "room-b".to_string(),
            record: room_record("room-b", "Target", 2),
        }
    ));
}

#[test]
fn client_mutation_index_restores_committed_responses_from_wal() {
    let records = vec![
        mutation_wal_record(
            11,
            WalPayload::MessageCreated {
                message: MessageDraft {
                    id: "message-a".to_string(),
                    client_mutation_id: Some("mutation-message".to_string()),
                    room_id: "room-a".to_string(),
                    sender_id: "user-a".to_string(),
                    body: "hello".to_string(),
                    attachments: Vec::new(),
                    created_at_ms: 11,
                    path: "rooms/room-a/messages/message-a".to_string(),
                },
            },
        ),
        mutation_wal_record(
            12,
            WalPayload::RecordTransactionCommitted {
                operations: vec![DbRecordMutationDraft::Upsert {
                    record: DbRecordDraft {
                        table: "rooms".to_string(),
                        key: "room-a".to_string(),
                        value: serde_json::json!({"id": "room-a", "title": "Room A"}),
                        updated_at_ms: 12,
                        path: "tables/rooms/room-a".to_string(),
                        client_mutation_id: None,
                    },
                }],
                client_mutation_id: Some("mutation-transaction".to_string()),
            },
        ),
        mutation_wal_record(
            13,
            WalPayload::ClientMutationRecorded {
                client_mutation_id: "mutation-delete-noop".to_string(),
                record: ClientMutationRecord::RecordDeleteNoop {
                    table: "rooms".to_string(),
                    key: "missing".to_string(),
                    path: "tables/rooms/missing".to_string(),
                },
            },
        ),
    ];

    let index = client_mutation_index_from_wal_records(&records);

    match index.get("mutation-message") {
        Some(CommittedMutation::MessageCreated { message }) => {
            assert_eq!(message.id, "message-a");
            assert_eq!(message.lsn, 11);
        }
        _ => panic!("missing message mutation"),
    }
    match index.get("mutation-transaction") {
        Some(CommittedMutation::RecordTransactionCommitted { response }) => {
            assert_eq!(response.lsn, 12);
            assert_eq!(response.operations.len(), 1);
        }
        _ => panic!("missing transaction mutation"),
    }
    match index.get("mutation-delete-noop") {
        Some(CommittedMutation::RecordDeleted { response }) => {
            assert!(!response.deleted);
            assert_eq!(response.lsn, 13);
            assert_eq!(response.path, "tables/rooms/missing");
        }
        _ => panic!("missing delete noop mutation"),
    }
}

fn indexed_room_query_subscription(
    title: &str,
    previous_records: Vec<DbRecord>,
) -> RecordQuerySubscription {
    let schema = DatabaseSchema::default_nextdb();
    let index_query = QueryRecordsByIndexQuery {
        consistency: Default::default(),
        value: Some(title.to_string()),
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
    };
    let index_name = Some("byTitle".to_string());
    let impact_filter = record_query_impact_filter(
        &schema,
        "rooms",
        None,
        index_name.as_deref(),
        &index_query,
        None,
    );
    let last_response_keys = previous_records
        .iter()
        .map(|record| record.key.clone())
        .collect();
    RecordQuerySubscription {
        query_id: "rooms-by-title".to_string(),
        table: "rooms".to_string(),
        parent_key: None,
        nested: None,
        subscribed_table: "rooms".to_string(),
        parent_key_prefix: None,
        index_name,
        index_query,
        impact_filter,
        schema_version: schema.version,
        after_key: None,
        after_cursor: None,
        limit: Some(20),
        order: None,
        predicate: None,
        last_result_id: None,
        last_response: Some(ListRecordsResponse {
            table: "rooms".to_string(),
            records: previous_records,
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        }),
        last_response_keys,
        retained_scope_keys: BTreeSet::new(),
        diff: true,
    }
}

fn nested_message_query_subscription(room_id: &str) -> RecordQuerySubscription {
    let schema = DatabaseSchema::default_nextdb();
    let table = "rooms".to_string();
    let nested = Some("messages".to_string());
    let logical_table = nested_record_table(&table, nested.as_deref().unwrap());
    let index_query = QueryRecordsByIndexQuery {
        consistency: Default::default(),
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
    };
    RecordQuerySubscription {
        query_id: "messages-query".to_string(),
        table,
        parent_key: Some(room_id.to_string()),
        nested,
        subscribed_table: logical_table,
        parent_key_prefix: Some(nested_record_prefix(room_id)),
        index_name: None,
        index_query,
        impact_filter: RecordQueryImpactFilter::AllUpserts,
        schema_version: schema.version,
        after_key: None,
        after_cursor: None,
        limit: Some(20),
        order: None,
        predicate: None,
        last_result_id: None,
        last_response: Some(ListRecordsResponse {
            table: "rooms.messages".to_string(),
            records: Vec::new(),
            next_after_key: None,
            next_cursor: None,
            has_more: false,
        }),
        last_response_keys: HashSet::new(),
        retained_scope_keys: BTreeSet::new(),
        diff: true,
    }
}

fn room_record(key: &str, title: &str, lsn: u64) -> DbRecord {
    DbRecord {
        table: "rooms".to_string(),
        key: key.to_string(),
        value: serde_json::json!({
            "id": key,
            "title": title,
        }),
        updated_at_ms: lsn,
        lsn,
        path: format!("tables/rooms/{key}"),
    }
}

fn room_record_with_score(key: &str, title: &str, score: f64, lsn: u64) -> DbRecord {
    DbRecord {
        table: "rooms".to_string(),
        key: key.to_string(),
        value: serde_json::json!({
            "id": key,
            "title": title,
            "score": score,
        }),
        updated_at_ms: lsn,
        lsn,
        path: format!("tables/rooms/{key}"),
    }
}

fn realtime_member(user_id: &str, session_id: Option<&str>) -> crate::realtime::RealtimeMember {
    crate::realtime::RealtimeMember {
        user_id: user_id.to_string(),
        session_id: session_id.map(str::to_string),
        metadata: serde_json::json!({}),
        joined_at_ms: 1,
        updated_at_ms: 1,
    }
}

async fn next_sum_update(
    updates: &mut tokio::sync::broadcast::Receiver<crate::aggregate::AggregateUpdate>,
) -> crate::aggregate::AggregateSumUpdate {
    loop {
        match updates.recv().await.expect("aggregate update") {
            crate::aggregate::AggregateUpdate::Sum(update) => return update,
            crate::aggregate::AggregateUpdate::Count(_)
            | crate::aggregate::AggregateUpdate::Presence(_) => {}
        }
    }
}

async fn next_presence_update(
    updates: &mut tokio::sync::broadcast::Receiver<crate::aggregate::AggregateUpdate>,
) -> crate::aggregate::AggregatePresenceUpdate {
    loop {
        match updates.recv().await.expect("aggregate update") {
            crate::aggregate::AggregateUpdate::Presence(update) => return update,
            crate::aggregate::AggregateUpdate::Count(_)
            | crate::aggregate::AggregateUpdate::Sum(_) => {}
        }
    }
}

fn mutation_wal_record(lsn: u64, payload: WalPayload) -> WalRecord {
    WalRecord {
        lsn,
        shard: 0,
        shard_epoch: 1,
        owner_node_id: "test".to_string(),
        timestamp_ms: lsn,
        schema_version: 1,
        durability: Durability::Strict,
        payload,
        checksum: None,
    }
}

fn indexed_room_record(key: &str, title: &str, lsn: u64) -> IndexedDbRecord {
    let values = vec![serde_json::json!(title)];
    IndexedDbRecord {
        record: room_record(key, title, lsn),
        cursor: index_range_cursor(&values, key).unwrap(),
    }
}

fn ordered_room_record(
    key: &str,
    title: &str,
    lsn: u64,
    order: &[RecordOrderTerm],
) -> OrderedDbRecord {
    let record = room_record(key, title, lsn);
    let cursor = order_record_cursor(&record, order);
    OrderedDbRecord { record, cursor }
}

fn message_record(room_id: &str, key: &str, lsn: u64) -> DbRecord {
    let logical_key = nested_record_key(room_id, key);
    DbRecord {
        table: "rooms.messages".to_string(),
        key: logical_key.clone(),
        value: serde_json::json!({
            "id": key,
            "roomId": room_id,
            "senderId": "alice",
            "text": "hello",
            "createdAt": lsn,
        }),
        updated_at_ms: lsn,
        lsn,
        path: format!("tables/rooms/{room_id}/messages/{key}"),
    }
}

#[test]
fn scoped_user_sync_excludes_other_event_categories() {
    let user_filter = HashSet::from(["alice".to_string()]);
    let page = sync_events_from_wal_records(
        sample_sync_records(),
        0,
        &HashSet::new(),
        &user_filter,
        &HashSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        false,
        100,
    );

    let event_types = page
        .events
        .iter()
        .map(delivery_event_type)
        .collect::<Vec<_>>();
    assert_eq!(event_types, vec!["userUpserted", "userEvent"]);
}

#[test]
fn object_sync_filter_excludes_non_object_events() {
    let page = sync_events_from_wal_records(
        sample_sync_records(),
        0,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        true,
        100,
    );

    let event_types = page
        .events
        .iter()
        .map(delivery_event_type)
        .collect::<Vec<_>>();
    assert_eq!(event_types, vec!["objectCommitted", "objectDeleted"]);
}

#[test]
fn table_range_sync_filters_record_keys() {
    let table_ranges = BTreeSet::from([TableSubscription::new(
        "rooms".to_string(),
        Some("room-010".to_string()),
        Some("room-020".to_string()),
    )]);
    let records = vec![
        sample_record(
            1,
            WalPayload::RecordUpserted {
                record: DbRecordDraft {
                    table: "rooms".to_string(),
                    key: "room-009".to_string(),
                    value: serde_json::json!({ "id": "room-009" }),
                    updated_at_ms: 1,
                    path: "tables/rooms/room-009".to_string(),
                    client_mutation_id: None,
                },
            },
        ),
        sample_record(
            2,
            WalPayload::RecordUpserted {
                record: DbRecordDraft {
                    table: "rooms".to_string(),
                    key: "room-010".to_string(),
                    value: serde_json::json!({ "id": "room-010" }),
                    updated_at_ms: 2,
                    path: "tables/rooms/room-010".to_string(),
                    client_mutation_id: None,
                },
            },
        ),
        sample_record(
            3,
            WalPayload::RecordUpserted {
                record: DbRecordDraft {
                    table: "rooms".to_string(),
                    key: "room-020".to_string(),
                    value: serde_json::json!({ "id": "room-020" }),
                    updated_at_ms: 3,
                    path: "tables/rooms/room-020".to_string(),
                    client_mutation_id: None,
                },
            },
        ),
    ];

    let page = sync_events_from_wal_records(
        records,
        0,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &table_ranges,
        &BTreeSet::new(),
        None,
        false,
        100,
    );

    assert_eq!(
        page.events
            .iter()
            .filter_map(DeliveryEvent::record_key)
            .collect::<Vec<_>>(),
        vec!["room-010"]
    );
}

#[test]
fn unfiltered_sync_includes_all_durable_event_categories() {
    let page = sync_events_from_wal_records(
        sample_sync_records(),
        0,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        false,
        100,
    );

    let event_types = page
        .events
        .iter()
        .map(delivery_event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        event_types,
        vec![
            "messageCreated",
            "userUpserted",
            "userEvent",
            "recordUpserted",
            "objectCommitted",
            "objectDeleted",
        ]
    );
}

fn sample_sync_records() -> Vec<WalRecord> {
    vec![
        sample_record(
            1,
            WalPayload::MessageCreated {
                message: MessageDraft {
                    id: "msg-1".to_string(),
                    client_mutation_id: None,
                    room_id: "general".to_string(),
                    sender_id: "alice".to_string(),
                    body: "hello".to_string(),
                    attachments: Vec::new(),
                    created_at_ms: 1,
                    path: "rooms/general/messages/msg-1".to_string(),
                },
            },
        ),
        sample_record(
            2,
            WalPayload::UserUpserted {
                user: UserProfileDraft {
                    user_id: "alice".to_string(),
                    client_mutation_id: None,
                    display_name: Some("Alice".to_string()),
                    metadata: serde_json::json!({ "role": "tester" }),
                    created_at_ms: 2,
                    updated_at_ms: 2,
                    path: "users/alice".to_string(),
                },
            },
        ),
        sample_record(
            3,
            WalPayload::UserEventPublished {
                event: UserEventDraft {
                    id: "event-1".to_string(),
                    client_mutation_id: None,
                    user_id: "alice".to_string(),
                    name: "notification.created".to_string(),
                    payload: serde_json::json!({ "text": "hello" }),
                    created_at_ms: 3,
                    path: "users/alice/events/event-1".to_string(),
                },
            },
        ),
        sample_record(
            4,
            WalPayload::RecordUpserted {
                record: DbRecordDraft {
                    table: "rooms".to_string(),
                    key: "general".to_string(),
                    value: serde_json::json!({ "id": "general" }),
                    updated_at_ms: 4,
                    path: "tables/rooms/general".to_string(),
                    client_mutation_id: None,
                },
            },
        ),
        sample_record(
            5,
            WalPayload::ObjectCommitted {
                object: ObjectMetadata {
                    id: "object-1".to_string(),
                    path: "objects/object-1".to_string(),
                    content_type: "text/plain".to_string(),
                    byte_size: 5,
                    sha256: "sha".to_string(),
                    created_at_ms: 5,
                },
                client_mutation_id: None,
            },
        ),
        sample_record(
            6,
            WalPayload::ObjectDeleted {
                object_id: "object-1".to_string(),
                deleted_at_ms: 6,
                path: "objects/object-1".to_string(),
                force: false,
                client_mutation_id: None,
            },
        ),
    ]
}

fn sample_record(lsn: u64, payload: WalPayload) -> WalRecord {
    WalRecord {
        lsn,
        shard: 0,
        shard_epoch: 1,
        owner_node_id: "node-a".to_string(),
        timestamp_ms: lsn,
        schema_version: 1,
        durability: Durability::Strict,
        payload,
        checksum: None,
    }
}

fn delivery_event_type(event: &DeliveryEvent) -> &'static str {
    match event {
        DeliveryEvent::MessageCreated { .. } => "messageCreated",
        DeliveryEvent::VolatileRoomEvent { .. } => "volatileRoomEvent",
        DeliveryEvent::VolatileUserEvent { .. } => "volatileUserEvent",
        DeliveryEvent::UserEvent { .. } => "userEvent",
        DeliveryEvent::UserUpserted { .. } => "userUpserted",
        DeliveryEvent::RecordUpserted { .. } => "recordUpserted",
        DeliveryEvent::RecordDeleted { .. } => "recordDeleted",
        DeliveryEvent::ObjectCommitted { .. } => "objectCommitted",
        DeliveryEvent::ObjectDeleted { .. } => "objectDeleted",
    }
}
